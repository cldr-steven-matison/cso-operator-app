"""Ingest uploads.

The NiFi `IngestDataToStream` flow exposes a single ListenHTTP at the head
and uses RouteOnAttribute on the request `Content-Type` / mime type to
branch docs (→ `new_documents`) vs audio (→ `new_audio`).

Backend behavior:
- POST /api/ingest forwards the upload body to NIFI_INGEST_URL with the
  correct Content-Type so NiFi can route it.
- GET /api/sample-audio streams the blog's reference WAV through the
  backend so the browser doesn't hit upstream CORS.
"""

import mimetypes

import httpx
from fastapi import APIRouter, File, HTTPException, Request, UploadFile
from fastapi.responses import StreamingResponse

from config import settings

router = APIRouter()


def _resolve_content_type(file: UploadFile) -> str:
    if file.content_type and file.content_type != "application/octet-stream":
        return file.content_type
    if file.filename:
        guessed, _ = mimetypes.guess_type(file.filename)
        if guessed:
            return guessed
    return "application/octet-stream"


@router.post("/ingest")
async def ingest(request: Request, file: UploadFile = File(...)):
    """Forward the upload to NiFi's ListenHTTP. The flow's RouteOnAttribute
    decides docs vs audio from the Content-Type, so we just pass the right
    header through."""
    body = await file.read()
    if not body:
        raise HTTPException(status_code=400, detail="empty upload")

    filename = file.filename or "upload.bin"
    content_type = _resolve_content_type(file)

    client: httpx.AsyncClient = request.app.state.http
    try:
        r = await client.post(
            settings.NIFI_INGEST_URL,
            content=body,
            headers={
                "Content-Type": content_type,
                "X-Filename": filename,
            },
            timeout=60.0,
        )
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"nifi ingest failed: {e!r}")

    return {
        "url": settings.NIFI_INGEST_URL,
        "status": r.status_code,
        "ok": 200 <= r.status_code < 300,
        "response": r.text[:500],
        "content_type": content_type,
        "filename": filename,
        "bytes": len(body),
    }


@router.get("/sample-audio")
async def sample_audio(request: Request):
    """Proxy the blog's reference WAV. Browser fetch hits CORS otherwise."""
    client: httpx.AsyncClient = request.app.state.http
    upstream = settings.SAMPLE_AUDIO_URL

    async def stream():
        async with client.stream("GET", upstream, timeout=60.0) as r:
            if r.status_code != 200:
                raise HTTPException(status_code=502, detail=f"sample fetch {r.status_code}")
            async for chunk in r.aiter_bytes():
                yield chunk

    filename = upstream.rsplit("/", 1)[-1] or "sample.wav"
    return StreamingResponse(
        stream(),
        media_type="audio/wav",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
