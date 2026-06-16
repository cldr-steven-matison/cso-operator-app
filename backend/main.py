from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import settings
from routers import health


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.http = httpx.AsyncClient(verify=settings.NIFI_VERIFY_TLS, timeout=30.0)
    yield
    await app.state.http.aclose()


app = FastAPI(title="CSO Operator App", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router, prefix="/api")


@app.get("/api")
async def root():
    return {"name": "cso-operator-app", "ok": True}
