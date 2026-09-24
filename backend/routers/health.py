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


async def _ping_kb(client: httpx.AsyncClient) -> dict:
    """The BrainShare KB on kb-do: its MCP endpoint answers 400/405 to a bare
    authenticated GET and 302s to the main site without the bearer, so
    'reachable and the bearer is accepted' is any answer that is not a
    redirect, a 401/403 or a server error."""
    if not settings.KB_URL:
        return {"ok": False, "error": "not configured (KB_URL)"}
    try:
        r = await client.get(settings.KB_URL, timeout=5.0, follow_redirects=False,
                             headers={"Authorization": f"Bearer {settings.KB_BEARER}",
                                      "Accept": "application/json, text/event-stream"})
    except Exception as e:
        return {"ok": False, "error": str(e)}
    ok = r.status_code < 500 and r.status_code not in (301, 302, 401, 403)
    return {"ok": ok, "status": r.status_code}


async def _ping_streamer_kb(client: httpx.AsyncClient) -> dict:
    """The Streamer KB is served by the Spark card door (BRAIN_CARD_URL /kb).
    Unset means the door is down or not wired on this box: a red light, not a
    missing one, because the KB is expected to come back (#304)."""
    if not settings.BRAIN_CARD_URL:
        return {"ok": False, "error": "not configured (BRAIN_CARD_URL)"}
    return await _ping(client, f"{settings.BRAIN_CARD_URL}/kb")


_HOME_STALE_S = 180  # flow_state arrives every 60 s; three misses is "link down"


def _home_link() -> tuple[dict, dict]:
    """The bridge light and home's own vllm/whisper lights as it last pushed
    them (flow_state, #382). A stale push turns every one of them red."""
    from services import streamers as streamers_svc
    services, age = streamers_svc.home_services()
    if age is None:
        link = {"ok": False, "error": "no push from home yet"}
    elif age > _HOME_STALE_S:
        link = {"ok": False, "error": f"last push {int(age)}s ago"}
    else:
        link = {"ok": True, "age_s": int(age)}
    relayed = {}
    for name in ("vllm", "whisper"):
        s = dict(services.get(name) or {"ok": False, "error": "not in home's push"})
        if not link["ok"]:
            s = {**s, "ok": False, "error": link["error"]}
        relayed[name] = s
    return link, relayed


async def home_state_services(client: httpx.AsyncClient) -> dict:
    """What home ships to the surface with its flow state: the two services
    the surface reports on but cannot reach."""
    vllm, whisper = await asyncio.gather(
        _ping_vllm(client), _ping(client, f"{settings.WHISPER_URL}/docs"))
    return {"vllm": vllm, "whisper": whisper}


@router.get("/health")
async def health(request: Request):
    """Only pings services owned by a module actually baked into this image
    (settings.MODULES) — an EFM-less deploy shouldn't burn a request (and show
    a permanently red dot) probing an EFM agent-manager that was never installed.
    The streamers-do surface (ROLE=surface) has no Kafka and cannot reach
    home's services: it shows the bridge link, home's vllm/whisper as pushed,
    nifi-do, the kb-do KB and the Streamer KB (Steven, 2026-09-24)."""
    client: httpx.AsyncClient = request.app.state.http
    rag_or_streamers = _module_active("rag", "streamers")
    surface = settings.ROLE == "surface"

    checks = {}
    if rag_or_streamers:
        if not surface:
            checks["vllm"] = _ping_vllm(client)
            checks["kafka"] = _ping_kafka()
        checks["nifi"] = _ping_nifi(client)
    if _module_active("rag"):
        checks["qdrant"] = _ping(client, f"{settings.QDRANT_URL}/collections")
        checks["embedding"] = _ping(client, f"{settings.EMBED_URL}/health")
    if _module_active("streamers"):
        if not surface:
            checks["whisper"] = _ping(client, f"{settings.WHISPER_URL}/docs")
        else:
            checks["kb"] = _ping_kb(client)
        checks["streamer_kb"] = _ping_streamer_kb(client)
    if _module_active("efm"):
        checks["efm"] = _ping_efm(client)

    names = list(checks.keys())
    results = await asyncio.gather(*checks.values())
    services = dict(zip(names, results))
    if surface and _module_active("streamers"):
        link, relayed = _home_link()
        services = {"home": link, **relayed, **services}
    return {"ok": all(s["ok"] for s in services.values()), "services": services}
