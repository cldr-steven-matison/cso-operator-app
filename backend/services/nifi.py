"""NiFi REST helpers.

Process groups of interest are looked up by name under the root group at
startup (or on demand). State changes use PUT /flow/process-groups/{id}.
"""

import httpx

from config import settings

PG_NAMES = ("IngestToStream", "IngestDataToStream", "StreamToWhisper", "StreamTovLLM")


async def list_root_groups(client: httpx.AsyncClient) -> list[dict]:
    r = await client.get(
        f"{settings.NIFI_URL}/nifi-api/process-groups/root/process-groups"
    )
    r.raise_for_status()
    return r.json().get("processGroups", [])


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
    r = await client.put(
        f"{settings.NIFI_URL}/nifi-api/flow/process-groups/{pg['id']}",
        json=body,
    )
    r.raise_for_status()
    return r.json()
