import httpx

from config import settings


async def embed(client: httpx.AsyncClient, text: str) -> list[float]:
    r = await client.post(
        f"{settings.EMBED_URL}/embed",
        json={"inputs": text},
        headers={"Content-Type": "application/json"},
    )
    r.raise_for_status()
    data = r.json()
    # TEI returns [[...]] for a single input
    return data[0] if isinstance(data, list) and data and isinstance(data[0], list) else data
