import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from config import settings
from routers import efm, health, ingest, k8s, kafka, nifi, qdrant, query

_enabled_modules = [m.strip() for m in settings.MODULES.split(",") if m.strip()]
# streamers-do (#382): no cluster-internal services exist on the droplet.
_surface = settings.ROLE == "surface"

_oauth_refresh_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _oauth_refresh_task
    # verify carries the mTLS client cert as an SSLContext when NIFI_CLIENT_CERT/KEY are set
    app.state.http = httpx.AsyncClient(verify=settings.nifi_verify, timeout=30.0)
    # Small pool — this is a read-only, low-frequency admin query (agent-classes/agents
    # polled every 15s by one page), not app traffic.
    app.state.efm_db = None if _surface else await asyncpg.create_pool(
        host=settings.EFM_DB_HOST,
        port=settings.EFM_DB_PORT,
        database=settings.EFM_DB_NAME,
        user=settings.EFM_DB_USER,
        password=settings.EFM_DB_PASSWORD,
        min_size=0,
        max_size=2,
    )
    if "streamers" in _enabled_modules and _surface:
        from services import clip_store, roster_store, streamers as streamers_service
        await roster_store.start(streamers_service.roster_seed())
        await clip_store.start()
    elif "streamers" in _enabled_modules:
        from services import roster_store, streamers as streamers_service
        # Roster/catalog from Postgres (#275): opens its pool, seeds a fresh DB from
        # the hardcoded constants, loads the in-process cache. Degrades to the
        # constants on any failure rather than blocking startup.
        await roster_store.start(streamers_service.roster_seed())
        _oauth_refresh_task = asyncio.create_task(streamers_service.start_oauth_refresh_scheduler(app.state.http))
        # No chat_activity aggregator or overlay chat relay here any more (#382):
        # the aggregator's only producer (home TwitchChatListener) was retired with
        # Legacy Flows, and the overlay relay now runs as nifi-do's OverlayChatRelay
        # PG (#393/#394). The overlay SSE router stays mounted until StarlinkAI's
        # OBS source is repointed.
    yield
    if _oauth_refresh_task is not None:
        from services import streamers as streamers_service
        await streamers_service.stop_oauth_refresh_scheduler()
    if "streamers" in _enabled_modules:
        from services import roster_store
        await roster_store.stop()
        if _surface:
            from services import clip_store
            await clip_store.stop()
    if app.state.efm_db is not None:
        await app.state.efm_db.close()
    await app.state.http.aclose()


app = FastAPI(title="CSO Operator App", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_base_routers = (health.router,) if _surface else (
    health.router, query.router, nifi.router, qdrant.router, kafka.router, ingest.router, k8s.router, efm.router)
for r in _base_routers:
    app.include_router(r, prefix="/api")

if "streamers" in _enabled_modules:
    from routers import streamers as _streamers_router
    app.include_router(_streamers_router.router, prefix="/api")
    if not _surface:  # the OBS overlay relay reads home Kafka + IRC; stays home
        from routers import overlay as _overlay_router
        app.include_router(_overlay_router.router, prefix="/api")


@app.get("/api")
async def api_root():
    return {"name": "cso-operator-app", "ok": True}


# Serve the built frontend from /app/static when it exists (production image).
_static = Path(__file__).parent / "static"
if _static.is_dir():
    app.mount("/", StaticFiles(directory=_static, html=True), name="static")


