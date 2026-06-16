"""Ingest uploads.

Two delivery modes:
1. NiFi ListenHTTP (preferred): if NIFI_INGEST_DOC_URL / NIFI_INGEST_AUDIO_URL
   is set, POST the file body straight to that processor.
2. Direct Kafka (fallback): publish raw bytes to TOPIC_DOCS / TOPIC_AUDIO.
   The downstream NiFi flows (StreamTovLLM / StreamToWhisper) consume from
   those topics, so the file still lands in NiFi — just at the consumer
   end of the flow instead of through the IngestDocs/IngestData ingress.
"""

from fastapi import APIRouter, File, Request, UploadFile

from config import settings
from services import kafka as kafka_svc

router = APIRouter(prefix="/ingest")


async def _deliver(client, target_url: str, topic: str, file: UploadFile) -> dict:
    body = await file.read()
    filename = (file.filename or "upload.bin").encode()
    if target_url:
        r = await client.post(
            target_url,
            content=body,
            headers={
                "Content-Type": file.content_type or "application/octet-stream",
                "X-Filename": file.filename or "upload.bin",
            },
        )
        return {
            "delivery": "nifi-listenhttp",
            "status": r.status_code,
            "bytes": len(body),
            "filename": file.filename,
        }
    meta = await kafka_svc.produce(
        topic, body, headers=[("filename", filename)]
    )
    return {
        "delivery": "kafka",
        "topic": meta["topic"],
        "partition": meta["partition"],
        "offset": meta["offset"],
        "bytes": len(body),
        "filename": file.filename,
    }


@router.post("/doc")
async def ingest_doc(request: Request, file: UploadFile = File(...)):
    return await _deliver(
        request.app.state.http,
        settings.NIFI_INGEST_DOC_URL,
        settings.TOPIC_DOCS,
        file,
    )


@router.post("/audio")
async def ingest_audio(request: Request, file: UploadFile = File(...)):
    return await _deliver(
        request.app.state.http,
        settings.NIFI_INGEST_AUDIO_URL,
        settings.TOPIC_AUDIO,
        file,
    )
