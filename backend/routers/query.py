from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from config import settings
from services import embedding, qdrant, vllm

router = APIRouter()


class QueryIn(BaseModel):
    question: str
    top_k: int | None = None
    max_tokens: int | None = None


@router.post("/query")
async def query(payload: QueryIn, request: Request):
    client = request.app.state.http
    top_k = payload.top_k or settings.RAG_TOP_K
    max_tokens = payload.max_tokens or settings.RAG_MAX_TOKENS

    vec = await embedding.embed(client, payload.question)
    hits = await qdrant.search(client, vec, top_k)
    chunks = [h.get("payload", {}).get("text", "") for h in hits]

    messages = vllm.build_messages(payload.question, chunks)

    async def stream():
        # Send sources first as a single SSE event, then forward vLLM's stream.
        import json
        sources = [
            {"id": h.get("id"), "score": h.get("score"), "payload": h.get("payload")}
            for h in hits
        ]
        yield f"event: sources\ndata: {json.dumps(sources)}\n\n".encode()
        async for chunk in vllm.chat_stream(client, messages, max_tokens):
            yield chunk

    return StreamingResponse(stream(), media_type="text/event-stream")
