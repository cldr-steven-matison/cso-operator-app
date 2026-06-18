import json

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from config import settings
from services import kafka as kafka_svc

router = APIRouter(prefix="/kafka")


@router.get("/topics")
async def topics():
    try:
        return await kafka_svc.topic_stats()
    except Exception as e:
        return {"error": str(e), "topics": []}


@router.get("/all-topics")
async def all_topics():
    try:
        return await kafka_svc.list_all_topics()
    except Exception as e:
        return {"error": str(e), "topics": []}


@router.get("/peek/{topic}")
async def peek(topic: str, limit: int = 10):
    try:
        limit = max(1, min(100, int(limit)))
        return await kafka_svc.peek(topic, limit)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.get("/tail/{topic}")
async def tail(topic: str):
    if topic not in (settings.TOPIC_AUDIO, settings.TOPIC_DOCS):
        raise HTTPException(status_code=400, detail="Unknown topic")

    async def stream():
        async for msg in kafka_svc.tail(topic):
            yield f"data: {json.dumps(msg)}\n\n".encode()

    return StreamingResponse(stream(), media_type="text/event-stream")
