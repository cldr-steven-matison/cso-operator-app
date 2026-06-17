import asyncio

import httpx
from aiokafka.admin import AIOKafkaAdminClient
from fastapi import APIRouter, Request

from config import settings
from services import nifi as nifi_svc

router = APIRouter()


async def _ping(client: httpx.AsyncClient, url: str) -> dict:
    try:
        r = await client.get(url, timeout=5.0)
        return {"ok": r.status_code < 400, "status": r.status_code}
    except Exception as e:
        return {"ok": False, "error": str(e)}


async def _ping_vllm(client: httpx.AsyncClient) -> dict:
    """Validate vLLM is reachable AND that VLLM_MODEL is one of the loaded
    models. A reachable server with a misnamed model would otherwise pass
    health and silently 404 every chat completion."""
    try:
        r = await client.get(f"{settings.VLLM_URL}/v1/models", timeout=5.0)
    except Exception as e:
        return {"ok": False, "error": str(e)}

    if r.status_code >= 400:
        return {"ok": False, "status": r.status_code}

    try:
        loaded = [m.get("id") for m in r.json().get("data", [])]
    except Exception as e:
        return {"ok": False, "status": r.status_code, "error": f"parse: {e!r}"}

    if settings.VLLM_MODEL not in loaded:
        return {
            "ok": False,
            "status": r.status_code,
            "error": (
                f"configured VLLM_MODEL={settings.VLLM_MODEL!r} is not loaded; "
                f"server reports {loaded}"
            ),
            "configured": settings.VLLM_MODEL,
            "loaded": loaded,
        }

    return {
        "ok": True,
        "status": r.status_code,
        "configured": settings.VLLM_MODEL,
        "loaded": loaded,
    }


async def _ping_nifi(client: httpx.AsyncClient) -> dict:
    try:
        await nifi_svc._get(client, "/system-diagnostics")
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


async def _ping_kafka() -> dict:
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.KAFKA_BOOTSTRAP)
    try:
        await admin.start()
        topics = await admin.list_topics()
        return {"ok": True, "topics": len(topics)}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        try:
            await admin.close()
        except Exception:
            pass


@router.get("/health")
async def health(request: Request):
    client: httpx.AsyncClient = request.app.state.http

    vllm, qdrant, embed, whisper, nifi, kafka = await asyncio.gather(
        _ping_vllm(client),
        _ping(client, f"{settings.QDRANT_URL}/collections"),
        _ping(client, f"{settings.EMBED_URL}/health"),
        _ping(client, f"{settings.WHISPER_URL}/docs"),
        _ping_nifi(client),
        _ping_kafka(),
    )

    services = {
        "vllm": vllm,
        "qdrant": qdrant,
        "embedding": embed,
        "whisper": whisper,
        "nifi": nifi,
        "kafka": kafka,
    }
    return {"ok": all(s["ok"] for s in services.values()), "services": services}
