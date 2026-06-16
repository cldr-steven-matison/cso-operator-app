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


@router.get("/tail/{topic}")
async def tail(topic: str):
    if topic not in (settings.TOPIC_AUDIO, settings.TOPIC_DOCS):
        raise HTTPException(status_code=400, detail="Unknown topic")

    async def stream():
        async for msg in kafka_svc.tail(topic):
            yield f"data: {json.dumps(msg)}\n\n".encode()

    return StreamingResponse(stream(), media_type="text/event-stream")
