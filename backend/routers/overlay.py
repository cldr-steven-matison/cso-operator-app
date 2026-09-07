"""Overlay chat relay endpoints (#300).

- POST /api/overlay/relay {"channel": <login|"me"|"off"|null>}
    Repoint the left-side chat column at another streamer's chat (raids /
    collabs), or back to @tunastarlink's own chat. Called by
    TwitchChatListenerProcessor's !c overlay dispatch (the same
    InvokeHTTP-to-endpoint shape as !load), broadcaster/mod-gated in NiFi.

- GET /api/overlay/chat/stream
    SSE feed the OBS Browser Source (overlays/tunastarlink/overlay.html)
    consumes. Same StreamingResponse/text-event-stream shape as
    /api/streamers/chat-activity/{login}/tail.
"""
import asyncio
import json

from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from services import overlay_relay

router = APIRouter(prefix="/overlay")


class RelayTarget(BaseModel):
    channel: "str | None" = None


@router.post("/relay")
async def set_relay(body: RelayTarget):
    """Swap the relay target. Returns the resolved login now being relayed
    (own chat when the request asked for me/off/own/null)."""
    target = await overlay_relay.set_target(body.channel)
    return {"ok": True, "channel": target}


@router.get("/relay")
async def get_relay():
    """Current relay target — for the overlay/backend to confirm state."""
    return {"channel": overlay_relay.current_target()}


@router.get("/chat/stream")
async def chat_stream():
    """SSE stream of relayed chat messages for the OBS Browser Source. Each
    event's data is one message object {user,color,badges,text,ts,channel};
    a periodic comment heartbeat keeps the connection warm through quiet chat."""
    async def stream():
        q = overlay_relay.subscribe()
        try:
            # Prime the connection so EventSource fires onopen immediately even
            # before the first chat line arrives.
            yield b": connected\n\n"
            while True:
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=15.0)
                except asyncio.TimeoutError:
                    yield b": keepalive\n\n"
                    continue
                yield f"data: {json.dumps(msg)}\n\n".encode("utf-8")
        finally:
            overlay_relay.unsubscribe(q)

    return StreamingResponse(stream(), media_type="text/event-stream")
