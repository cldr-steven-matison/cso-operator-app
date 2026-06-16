import httpx

from config import settings


def _coll_url() -> str:
    return f"{settings.QDRANT_URL}/collections/{settings.QDRANT_COLLECTION}"


async def search(client: httpx.AsyncClient, vector: list[float], limit: int) -> list[dict]:
    r = await client.post(
        f"{_coll_url()}/points/search",
        json={"vector": vector, "limit": limit, "with_payload": True},
    )
    r.raise_for_status()
    return r.json().get("result", [])


async def stats(client: httpx.AsyncClient) -> dict:
    r = await client.get(_coll_url())
    if r.status_code == 404:
        return {"exists": False}
    r.raise_for_status()
    body = r.json().get("result", {})
    return {
        "exists": True,
        "points_count": body.get("points_count"),
        "vectors_count": body.get("vectors_count"),
        "segments_count": body.get("segments_count"),
        "status": body.get("status"),
    }


async def recreate(client: httpx.AsyncClient) -> dict:
    await client.delete(_coll_url())
    r = await client.put(
        _coll_url(),
        json={"vectors": {"size": settings.EMBED_DIM, "distance": "Cosine"}},
    )
    r.raise_for_status()
    return r.json()
