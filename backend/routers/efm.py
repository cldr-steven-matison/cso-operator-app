import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
import httpx

from config import settings

router = APIRouter(prefix="/efm")

# Demo catalog. Two layouts:
#   - In the image: Dockerfile flattens backend/ to /app/ and copies samples/
#     to /app/samples/, so /app/routers/efm.py → ../samples/efm-demos.json.
#   - Local dev: backend/routers/efm.py → ../../samples/efm-demos.json.
# Read at request time so catalog edits hot-reload without a backend restart.
_HERE = Path(__file__).resolve().parent
_DEMO_CANDIDATES = (
    _HERE.parent / "samples" / "efm-demos.json",          # image: /app/samples
    _HERE.parent.parent / "samples" / "efm-demos.json",   # local: repo/samples
)


def _demos_file() -> Path | None:
    for p in _DEMO_CANDIDATES:
        if p.is_file():
            return p
    return None

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _efm_get(http: httpx.AsyncClient, path: str, *, raise_on_error: bool = True, timeout: float = 10.0):
    """GET {EFM_URL}{path}. Raises HTTPException(502) on network/HTTP errors."""
    try:
        r = await http.get(f"{settings.EFM_URL}{path}", timeout=timeout)
        r.raise_for_status()
        return r.json()
    except httpx.RequestError as e:
        if raise_on_error:
            raise HTTPException(status_code=502, detail=f"EFM unreachable: {e}")
        return None
    except httpx.HTTPStatusError as e:
        if raise_on_error:
            raise HTTPException(status_code=502, detail=f"EFM error {e.response.status_code}")
        return None


async def _fetch_agents(http: httpx.AsyncClient, pool) -> list[dict]:
    """
    Real agent registry, read directly from EFM's own Postgres — replaces the old
    operations/events discovery heuristic entirely.

    EFM v2.3.1 has no REST "list agents" endpoint, so the previous approach
    reconstructed candidate agent IDs from recent operations + events, then verified
    each via /efm/api/monitor/agents/{id}. That broke in two ways (found 2026-07-18):
    the operations table has no automatic retention (a single agent's reconnect-loop
    piled up ~11.8k rows in under a day and made that endpoint hang), and DESCRIBE
    operations only fire on agent connect/reconnect, not on routine heartbeats — so
    even a healthy, fully-online agent could go "undiscoverable" for long stretches.
    The `agent`/`device` tables are EFM's actual source of truth for identity/class/IP
    and don't have either problem.

    BUT: the DB's `agent.last_seen` column turns out to have the exact same "only
    updates on connect/reconnect" behavior as the operations table (confirmed
    2026-07-18) — it can lag true liveness by hours for a genuinely-online agent,
    which made the frontend's freshness-based status dot show red/dead for agents
    that were actually fine. The per-agent REST monitor endpoint tracks real-time
    state and updates within seconds, so use it to enrich lastSeen — it's cheap now
    that we already know the IDs from the DB and don't need to discover them.
    """
    rows = await pool.fetch(
        """
        SELECT a.id, a.agent_class, a.agent_state, a.last_seen, d.ip_address
        FROM agent a
        LEFT JOIN device d ON a.device_id = d.id
        """
    )
    db_agents = [dict(r) for r in rows]

    async def fetch_live(agent_id: str) -> dict | None:
        return await _efm_get(http, f"/efm/api/monitor/agents/{agent_id}", raise_on_error=False)

    live_results = await asyncio.gather(*[fetch_live(a["id"]) for a in db_agents])
    live_by_id = {
        r["identifier"]: r for r in live_results if r and isinstance(r, dict) and r.get("identifier")
    }

    for a in db_agents:
        live = live_by_id.get(a["id"])
        live_last_seen_ms = live.get("lastSeen") if live else None
        if live_last_seen_ms:
            a["last_seen"] = datetime.fromtimestamp(live_last_seen_ms / 1000, tz=timezone.utc)
        elif a["last_seen"]:
            a["last_seen"] = a["last_seen"].replace(tzinfo=timezone.utc)

    return db_agents


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/agent-classes")
async def get_agent_classes(request: Request):
    """Return agent classes with live (ONLINE) agent counts."""
    http: httpx.AsyncClient = request.app.state.http

    classes_data = await _efm_get(http, "/efm/api/agent-classes")
    agents = await _fetch_agents(http, request.app.state.efm_db)

    classes_list = classes_data if isinstance(classes_data, list) else []

    counts: dict[str, int] = {}
    for agent in agents:
        if agent["agent_state"] == "ONLINE":
            cls = agent["agent_class"] or ""
            if cls:
                counts[cls] = counts.get(cls, 0) + 1

    return [
        {"name": cls.get("name", ""), "agentCount": counts.get(cls.get("name", ""), 0)}
        for cls in classes_list
        if cls.get("name")
    ]


@router.get("/agents")
async def get_agents(request: Request):
    """Return all known agents (any state) with resolved ListenHTTP endpoint URL."""
    http: httpx.AsyncClient = request.app.state.http
    agents = await _fetch_agents(http, request.app.state.efm_db)

    result = []
    for a in agents:
        last_seen = a["last_seen"].isoformat() if a["last_seen"] else None

        # Stored as the agent's reported IP from its heartbeat. Use it for all
        # classes — for KubernetesPod this is the pod IP, for LAN devices it's
        # the LAN IP (or 127.0.0.1 if the agent reports loopback).
        ip = a["ip_address"] or ""
        endpoint_url = f"http://{ip}:8080/contentListener" if ip and ip != "127.0.0.1" else ""

        result.append({
            "identifier": a["id"],
            "className": a["agent_class"] or "",
            "lastSeen": last_seen,
            "status": {"state": a["agent_state"] or ""},
            "endpointUrl": endpoint_url,
        })

    return result


@router.get("/demos")
async def get_demos():
    """Return the repo-local demo catalog.

    Each entry: {name, agentClass, contentType, payload, kafkaTopic, expect:{topic,withinSec,match?}}.
    Empty list if the catalog file is missing.
    """
    path = _demos_file()
    if path is None:
        return []
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as e:
        raise HTTPException(status_code=500, detail=f"Failed to read demo catalog: {e}")


class SendRequest(BaseModel):
    endpoint_url: str
    payload: str
    content_type: str


@router.post("/send")
async def send_to_agent(body: SendRequest, request: Request):
    """POST a payload to a MiNiFi agent's ListenHTTP contentListener."""
    if not body.endpoint_url:
        raise HTTPException(status_code=400, detail="endpoint_url must not be empty")

    http: httpx.AsyncClient = request.app.state.http

    try:
        r = await http.post(
            body.endpoint_url,
            content=body.payload,
            headers={"Content-Type": body.content_type},
            timeout=10.0,
        )
        return {
            "ok": r.status_code < 400,
            "status_code": r.status_code,
            "body_preview": r.text[:500],
        }
    except httpx.RequestError as e:
        return {
            "ok": False,
            "status_code": 0,
            "body_preview": f"Connection error: {e}",
        }
