from typing import AsyncIterator

import httpx

from config import settings


def build_messages(question: str, context_chunks: list[str]) -> list[dict]:
    context = "\n\n---\n\n".join(context_chunks) if context_chunks else "No context available."
    return [
        {"role": "system", "content": "Answer concisely using the provided context."},
        {"role": "user", "content": f"Context:\n{context}\n\nQuestion: {question}"},
    ]


async def chat_stream(
    client: httpx.AsyncClient, messages: list[dict], max_tokens: int
) -> AsyncIterator[bytes]:
    """Pass-through SSE stream from vLLM's OpenAI-compatible endpoint."""
    payload = {
        "model": settings.VLLM_MODEL,
        "messages": messages,
        "max_tokens": max_tokens,
        "stream": True,
    }
    async with client.stream(
        "POST",
        f"{settings.VLLM_URL}/v1/chat/completions",
        json=payload,
        timeout=120.0,
    ) as r:
        async for line in r.aiter_lines():
            if line:
                yield f"{line}\n\n".encode()
