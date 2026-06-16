import asyncio

import httpx
from aiokafka.admin import AIOKafkaAdminClient
from fastapi import APIRouter, Request

from config import settings

router = APIRouter()


async def _ping(client: httpx.AsyncClient, url: str) -> dict:
    try:
        r = await client.get(url, timeout=5.0)
        return {"ok": r.status_code < 500, "status": r.status_code}
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
        _ping(client, f"{settings.VLLM_URL}/v1/models"),
        _ping(client, f"{settings.QDRANT_URL}/collections"),
        _ping(client, f"{settings.EMBED_URL}/health"),
        _ping(client, f"{settings.WHISPER_URL}/docs"),
        _ping(client, f"{settings.NIFI_URL}/nifi-api/system-diagnostics"),
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
