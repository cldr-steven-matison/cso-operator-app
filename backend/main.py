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

_chat_activity_task: asyncio.Task | None = None
_oauth_refresh_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _chat_activity_task, _oauth_refresh_task
    # verify carries the mTLS client cert as an SSLContext when NIFI_CLIENT_CERT/KEY are set
    app.state.http = httpx.AsyncClient(verify=settings.nifi_verify, timeout=30.0)
    # Small pool — this is a read-only, low-frequency admin query (agent-classes/agents
    # polled every 15s by one page), not app traffic.
    app.state.efm_db = await asyncpg.create_pool(
        host=settings.EFM_DB_HOST,
        port=settings.EFM_DB_PORT,
        database=settings.EFM_DB_NAME,
        user=settings.EFM_DB_USER,
        password=settings.EFM_DB_PASSWORD,
        min_size=0,
        max_size=2,
    )
    if "streamers" in _enabled_modules:
        from services import chat_activity, streamers as streamers_service
        _chat_activity_task = asyncio.create_task(chat_activity.start_aggregator())
        _oauth_refresh_task = asyncio.create_task(streamers_service.start_oauth_refresh_scheduler(app.state.http))
    yield
    if _chat_activity_task is not None:
        from services import chat_activity
        await chat_activity.stop_aggregator()
    if _oauth_refresh_task is not None:
        from services import streamers as streamers_service
        await streamers_service.stop_oauth_refresh_scheduler()
    await app.state.efm_db.close()
    await app.state.http.aclose()


app = FastAPI(title="CSO Operator App", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

for r in (health.router, query.router, nifi.router, qdrant.router, kafka.router, ingest.router, k8s.router, efm.router):
    app.include_router(r, prefix="/api")

if "streamers" in _enabled_modules:
    from routers import streamers as _streamers_router
    app.include_router(_streamers_router.router, prefix="/api")


@app.get("/api")
async def api_root():
    return {"name": "cso-operator-app", "ok": True}


# Serve the built frontend from /app/static when it exists (production image).
_static = Path(__file__).parent / "static"
if _static.is_dir():
    app.mount("/", StaticFiles(directory=_static, html=True), name="static")


