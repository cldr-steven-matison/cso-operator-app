import json
from typing import AsyncIterator

import httpx

from config import settings


# Per-chunk context cap, mirroring the working blog query-rag-5.py.
# Keeps the prompt small enough that vLLM doesn't reject on context length
# and the response stays focused.
CONTEXT_CHUNK_CHAR_CAP = 500


def build_messages(question: str, context_chunks: list[str]) -> list[dict]:
    # Drop empty chunks and any chunk that looks like a leaked vector
    # (Qdrant payload misconfiguration), as the blog script does.
    cleaned: list[str] = []
    for raw in context_chunks:
        text = str(raw or "").strip()
        if not text:
            continue
        if text.startswith("["):
            continue
        cleaned.append(text[:CONTEXT_CHUNK_CHAR_CAP])

    context = "\n\n---\n\n".join(cleaned) if cleaned else "No context available."
    return [
        {"role": "system", "content": "Briefly answer using this context."},
        {"role": "user", "content": f"Context: {context}\n\nQuestion: {question}"},
    ]


async def chat_stream(
    client: httpx.AsyncClient, messages: list[dict], max_tokens: int
) -> AsyncIterator[bytes]:
    """Call vLLM exactly like the blog's working curl/python does — a single
    non-streaming POST — then re-emit the result as SSE so the frontend's
    EventSource parser doesn't change.

    The blog's request shape:
        POST /v1/chat/completions
        {"model": ..., "messages": [...], "max_tokens": N}
    No `stream: true`. We surface non-200 responses as a visible error event
    instead of silently dropping them.
    """
    payload = {
        "model": settings.VLLM_MODEL,
        "messages": messages,
        "max_tokens": max_tokens,
    }

    try:
        r = await client.post(
            f"{settings.VLLM_URL}/v1/chat/completions",
            json=payload,
            timeout=120.0,
        )
    except Exception as e:  # network / DNS / timeout
        err = {"error": f"vllm request failed: {e!r}"}
        yield f"event: error\ndata: {json.dumps(err)}\n\n".encode()
        yield b"data: [DONE]\n\n"
        return

    if r.status_code != 200:
        err = {"error": f"vllm {r.status_code}", "body": r.text[:1000]}
        yield f"event: error\ndata: {json.dumps(err)}\n\n".encode()
        yield b"data: [DONE]\n\n"
        return

    try:
        body = r.json()
        content = body["choices"][0]["message"]["content"]
    except Exception as e:
        err = {"error": f"vllm response parse failed: {e!r}", "body": r.text[:1000]}
        yield f"event: error\ndata: {json.dumps(err)}\n\n".encode()
        yield b"data: [DONE]\n\n"
        return

    # Re-emit as a single OpenAI-style SSE delta so the existing frontend
    # code path (`obj.choices[0].delta.content`) renders it unchanged.
    delta = {"choices": [{"delta": {"content": content}}]}
    yield f"data: {json.dumps(delta)}\n\n".encode()
    yield b"data: [DONE]\n\n"
