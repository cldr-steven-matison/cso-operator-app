from fastapi import APIRouter, Request

from services import qdrant as qdrant_svc

router = APIRouter(prefix="/qdrant")


@router.get("/stats")
async def stats(request: Request):
    return await qdrant_svc.stats(request.app.state.http)


@router.post("/recreate")
async def recreate(request: Request):
    return await qdrant_svc.recreate(request.app.state.http)
