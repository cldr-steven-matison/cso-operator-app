from fastapi import APIRouter, Request

from services import qdrant as qdrant_svc

router = APIRouter(prefix="/qdrant")


@router.get("/stats")
async def stats(request: Request):
    try:
        return await qdrant_svc.stats(request.app.state.http)
    except Exception as e:
        return {"exists": False, "error": str(e)}


@router.post("/recreate")
async def recreate(request: Request):
    try:
        return await qdrant_svc.recreate(request.app.state.http)
    except Exception as e:
        return {"ok": False, "error": str(e)}
