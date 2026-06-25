import asyncio

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
import httpx

from config import settings

router = APIRouter(prefix="/efm")

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _efm_get(http: httpx.AsyncClient, path: str, *, raise_on_error: bool = True):
    """GET {EFM_URL}{path}. Raises HTTPException(502) on network/HTTP errors."""
    try:
        r = await http.get(f"{settings.EFM_URL}{path}", timeout=10.0)
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


async def _discover_agents(http: httpx.AsyncClient) -> list[dict]:
    """
    EFM v2.3.1 has no /agents list endpoint.
    Strategy: collect candidate agent IDs from operations (targetAgentId) and
    agent-sourced events (eventSource.type=="Agent"), then verify each via
    /efm/api/monitor/agents/{id} concurrently.
    """
    ops_data, events_data = await asyncio.gather(
        _efm_get(http, "/efm/api/operations?pageSize=500", raise_on_error=False),
        _efm_get(http, "/efm/api/events?pageSize=200", raise_on_error=False),
    )

    candidate_ids: set[str] = set()

    # Operations: each has targetAgentId directly.
    if ops_data:
        ops_list = ops_data if isinstance(ops_data, list) else ops_data.get("elements", [])
        for op in ops_list:
            aid = op.get("targetAgentId", "")
            if aid:
                candidate_ids.add(aid)

    # Events: supplement with agent-sourced event source IDs.
    if events_data:
        events_list = events_data if isinstance(events_data, list) else events_data.get("elements", [])
        for event in events_list:
            src = event.get("eventSource") or {}
            if src.get("type") == "Agent":
                aid = src.get("id", "")
                if aid:
                    candidate_ids.add(aid)

    if not candidate_ids:
        return []

    # Verify each candidate via monitor endpoint; discard invalid/missing ones.
    async def fetch_one(agent_id: str) -> dict | None:
        return await _efm_get(http, f"/efm/api/monitor/agents/{agent_id}", raise_on_error=False)

    results = await asyncio.gather(*[fetch_one(aid) for aid in candidate_ids])

    return [d for d in results if d and isinstance(d, dict) and "identifier" in d]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/agent-classes")
async def get_agent_classes(request: Request):
    """Return agent classes with live agent counts."""
    http: httpx.AsyncClient = request.app.state.http

    classes_data, agents = await asyncio.gather(
        _efm_get(http, "/efm/api/agent-classes"),
        _discover_agents(http),
    )

    classes_list = classes_data if isinstance(classes_data, list) else []

    counts: dict[str, int] = {}
    for agent in agents:
        cls = agent.get("agentClass", "")
        if cls:
            counts[cls] = counts.get(cls, 0) + 1

    return [
        {"name": cls.get("name", ""), "agentCount": counts.get(cls.get("name", ""), 0)}
        for cls in classes_list
        if cls.get("name")
    ]


@router.get("/agents")
async def get_agents(request: Request):
    """Return active agents with resolved ListenHTTP endpoint URL."""
    from datetime import datetime, timezone

    http: httpx.AsyncClient = request.app.state.http
    agents = await _discover_agents(http)

    result = []
    for a in agents:
        class_name = a.get("agentClass", "")

        last_seen_ms = a.get("lastSeen")
        last_seen = (
            datetime.fromtimestamp(last_seen_ms / 1000, tz=timezone.utc).isoformat()
            if last_seen_ms else None
        )

        # EFM's monitor data includes the agent's reported IP from its heartbeat.
        # Use it for all classes — for KubernetesPod this is the pod IP,
        # for LAN devices it's the LAN IP (or 127.0.0.1 if the agent reports loopback).
        ip = (a.get("deviceInfo") or {}).get("networkInfo", {}).get("ipAddress", "")
        endpoint_url = f"http://{ip}:8080/contentListener" if ip and ip != "127.0.0.1" else ""

        result.append({
            "identifier": a.get("identifier", ""),
            "className": class_name,
            "lastSeen": last_seen,
            "status": {"state": a.get("state", "")},
            "endpointUrl": endpoint_url,
        })

    return result


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
