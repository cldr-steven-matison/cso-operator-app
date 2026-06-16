"""Forward uploads to the NiFi ListenHTTP processors at the head of
IngestToStream and IngestDataToStream. URLs are configured via
NIFI_INGEST_DOC_URL / NIFI_INGEST_AUDIO_URL once the processors are wired.
"""

from fastapi import APIRouter, File, HTTPException, Request, UploadFile

from config import settings

router = APIRouter(prefix="/ingest")


async def _forward(client, target_url: str, file: UploadFile) -> dict:
    if not target_url:
        raise HTTPException(
            status_code=503,
            detail="Ingest target not configured. Set NIFI_INGEST_*_URL after wiring ListenHTTP.",
        )
    body = await file.read()
    r = await client.post(
        target_url,
        content=body,
        headers={
            "Content-Type": file.content_type or "application/octet-stream",
            "X-Filename": file.filename or "upload.bin",
        },
    )
    return {"status": r.status_code, "bytes": len(body), "filename": file.filename}


@router.post("/doc")
async def ingest_doc(request: Request, file: UploadFile = File(...)):
    return await _forward(request.app.state.http, settings.NIFI_INGEST_DOC_URL, file)


@router.post("/audio")
async def ingest_audio(request: Request, file: UploadFile = File(...)):
    return await _forward(request.app.state.http, settings.NIFI_INGEST_AUDIO_URL, file)
