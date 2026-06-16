"""NiFi REST helpers.

Process groups of interest are looked up by name under the root group at
request time. State changes use PUT /flow/process-groups/{id}. Auth: if
NIFI_USERNAME/PASSWORD are set, fetch a Bearer token via /access/token
and cache it; refresh on 401.
"""

import asyncio

import httpx

from config import settings

PG_NAMES = ("IngestDataToStream", "StreamToWhisper", "StreamTovLLM")

_token: str | None = None
_token_lock = asyncio.Lock()


async def _fetch_token(client: httpx.AsyncClient) -> str:
    if not (settings.NIFI_USERNAME and settings.NIFI_PASSWORD):
        return ""
    r = await client.post(
        f"{settings.NIFI_URL}/nifi-api/access/token",
        data={
            "username": settings.NIFI_USERNAME,
            "password": settings.NIFI_PASSWORD,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        cookies={},
    )
    r.raise_for_status()
    # Drop cookies the token endpoint sets — NiFi will treat any future
    # session cookie + Bearer combination as cookie-auth and demand CSRF.
    client.cookies.clear()
    return r.text.strip()


async def _headers(client: httpx.AsyncClient, force_refresh: bool = False) -> dict:
    global _token
    async with _token_lock:
        if _token is None or force_refresh:
            _token = await _fetch_token(client)
    return {"Authorization": f"Bearer {_token}"} if _token else {}


async def _get(client: httpx.AsyncClient, path: str) -> dict:
    url = f"{settings.NIFI_URL}/nifi-api{path}"
    r = await client.get(url, headers=await _headers(client), cookies={})
    if r.status_code == 401:
        client.cookies.clear()
        r = await client.get(url, headers=await _headers(client, force_refresh=True), cookies={})
    if r.status_code >= 400:
        raise httpx.HTTPStatusError(
            f"{r.status_code} from {path}: {r.text[:500]}", request=r.request, response=r
        )
    return r.json()


async def _put(client: httpx.AsyncClient, path: str, body: dict) -> dict:
    url = f"{settings.NIFI_URL}/nifi-api{path}"
    r = await client.put(url, headers=await _headers(client), json=body, cookies={})
    if r.status_code == 401:
        client.cookies.clear()
        r = await client.put(url, headers=await _headers(client, force_refresh=True), json=body, cookies={})
    if r.status_code >= 400:
        raise httpx.HTTPStatusError(
            f"{r.status_code} from {path}: {r.text[:500]}", request=r.request, response=r
        )
    return r.json()


async def _children(client: httpx.AsyncClient, group_id: str) -> list[dict]:
    data = await _get(client, f"/process-groups/{group_id}/process-groups")
    return data.get("processGroups", [])


def _pg_info(pg: dict) -> dict:
    return {
        "id": pg["id"],
        "version": pg.get("revision", {}).get("version", 0),
    }


async def resolve_groups(
    client: httpx.AsyncClient, max_depth: int = 4
) -> dict[str, dict]:
    """Walk process groups under root and return {name: info} for known PGs."""
    out: dict[str, dict] = {}
    queue: list[tuple[str, int]] = [("root", 0)]
    visited: set[str] = set()
    while queue:
        gid, depth = queue.pop(0)
        if gid in visited or depth > max_depth:
            continue
        visited.add(gid)
        for pg in await _children(client, gid):
            name = pg.get("component", {}).get("name")
            if name in PG_NAMES and name not in out:
                out[name] = _pg_info(pg)
            queue.append((pg["id"], depth + 1))
        if len(out) == len(PG_NAMES):
            break
    return out


async def pg_state(client: httpx.AsyncClient, pg_id: str) -> str:
    """Compute a PG's state from its processors' states.

    NiFi's PG-level aggregate counts come back null in our setup, but
    /flow/process-groups/{id} reliably returns processor-level state.
    Returns one of: "RUNNING" | "STOPPED" | "INVALID" | "DISABLED" | "EMPTY".
    """
    data = await _get(client, f"/flow/process-groups/{pg_id}")
    procs = data.get("processGroupFlow", {}).get("flow", {}).get("processors", [])
    counts = {"RUNNING": 0, "STOPPED": 0, "INVALID": 0, "DISABLED": 0}
    for p in procs:
        s = p.get("component", {}).get("state") or p.get("status", {}).get(
            "aggregateSnapshot", {}
        ).get("runStatus")
        if s in counts:
            counts[s] += 1
    if counts["RUNNING"]:
        return "RUNNING"
    if counts["STOPPED"]:
        return "STOPPED"
    if counts["INVALID"]:
        return "INVALID"
    if counts["DISABLED"]:
        return "DISABLED"
    return "EMPTY"


async def state(client: httpx.AsyncClient) -> dict:
    """Return {name: {id, version, state}} for all known PGs."""
    groups = await resolve_groups(client)
    results = await asyncio.gather(
        *(pg_state(client, pg["id"]) for pg in groups.values()),
        return_exceptions=True,
    )
    out: dict[str, dict] = {}
    for (name, pg), st in zip(groups.items(), results):
        out[name] = {**pg, "state": st if isinstance(st, str) else "UNKNOWN"}
    return out


async def set_state(client: httpx.AsyncClient, name: str, running: bool) -> dict:
    groups = await resolve_groups(client)
    if name not in groups:
        raise ValueError(f"Process group '{name}' not found")
    pg = groups[name]
    body = {
        "id": pg["id"],
        "state": "RUNNING" if running else "STOPPED",
        "disconnectedNodeAcknowledged": False,
    }
    return await _put(client, f"/flow/process-groups/{pg['id']}", body)
