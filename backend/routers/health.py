import asyncio

import httpx
from aiokafka.admin import AIOKafkaAdminClient
from fastapi import APIRouter, Request

from config import settings
from services import nifi as nifi_svc

router = APIRouter()

_enabled_modules = [m.strip() for m in settings.MODULES.split(",") if m.strip()]


def _module_active(*names: str) -> bool:
    return any(n in _enabled_modules for n in names) or "all" in _enabled_modules


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


async def _ping_efm(client: httpx.AsyncClient) -> dict:
    return await _ping(client, f"{settings.EFM_URL}/efm/api/agent-classes")


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
    """Only pings services owned by a module actually baked into this image
    (settings.MODULES) — an EFM-less deploy shouldn't burn a request (and show
    a permanently red dot) probing an EFM agent-manager that was never installed."""
    client: httpx.AsyncClient = request.app.state.http
    rag_or_streamers = _module_active("rag", "streamers")

    checks = {}
    if rag_or_streamers:
        checks["vllm"] = _ping_vllm(client)
        checks["nifi"] = _ping_nifi(client)
        checks["kafka"] = _ping_kafka()
    if _module_active("rag"):
        checks["qdrant"] = _ping(client, f"{settings.QDRANT_URL}/collections")
        checks["embedding"] = _ping(client, f"{settings.EMBED_URL}/health")
    if _module_active("streamers"):
        checks["whisper"] = _ping(client, f"{settings.WHISPER_URL}/docs")
    if _module_active("efm"):
        checks["efm"] = _ping_efm(client)

    names = list(checks.keys())
    results = await asyncio.gather(*checks.values())
    services = dict(zip(names, results))
    return {"ok": all(s["ok"] for s in services.values()), "services": services}
