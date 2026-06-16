"""NiFi REST helpers.

Process groups of interest are looked up by name under the root group at
request time. State changes use PUT /flow/process-groups/{id}. Auth: if
NIFI_USERNAME/PASSWORD are set, fetch a Bearer token via /access/token
and cache it; refresh on 401.
"""

import asyncio

import httpx

from config import settings

PG_NAMES = ("IngestToStream", "IngestDataToStream", "StreamToWhisper", "StreamTovLLM")

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
    )
    r.raise_for_status()
    return r.text.strip()


async def _headers(client: httpx.AsyncClient, force_refresh: bool = False) -> dict:
    global _token
    async with _token_lock:
        if _token is None or force_refresh:
            _token = await _fetch_token(client)
    return {"Authorization": f"Bearer {_token}"} if _token else {}


async def _get(client: httpx.AsyncClient, path: str) -> dict:
    url = f"{settings.NIFI_URL}/nifi-api{path}"
    r = await client.get(url, headers=await _headers(client))
    if r.status_code == 401:
        r = await client.get(url, headers=await _headers(client, force_refresh=True))
    r.raise_for_status()
    return r.json()


async def _put(client: httpx.AsyncClient, path: str, body: dict) -> dict:
    url = f"{settings.NIFI_URL}/nifi-api{path}"
    r = await client.put(url, headers=await _headers(client), json=body)
    if r.status_code == 401:
        r = await client.put(url, headers=await _headers(client, force_refresh=True), json=body)
    r.raise_for_status()
    return r.json()


async def list_root_groups(client: httpx.AsyncClient) -> list[dict]:
    data = await _get(client, "/process-groups/root/process-groups")
    return data.get("processGroups", [])


async def resolve_groups(client: httpx.AsyncClient) -> dict[str, dict]:
    """Return {name: {"id": str, "version": int, "state": str}} for known PGs."""
    out: dict[str, dict] = {}
    for pg in await list_root_groups(client):
        name = pg.get("component", {}).get("name") or pg.get("status", {}).get("name")
        if name in PG_NAMES:
            out[name] = {
                "id": pg["id"],
                "version": pg.get("revision", {}).get("version", 0),
                "state": pg.get("status", {}).get("aggregateSnapshot", {}).get("runStatus")
                or pg.get("component", {}).get("state"),
            }
    return out


async def state(client: httpx.AsyncClient) -> dict:
    return await resolve_groups(client)


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
