from fastapi import APIRouter, HTTPException, Request

from services import nifi

router = APIRouter(prefix="/nifi")


@router.get("/state")
async def state(request: Request):
    return await nifi.state(request.app.state.http)


@router.post("/{name}/start")
async def start(name: str, request: Request):
    try:
        return await nifi.set_state(request.app.state.http, name, running=True)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/{name}/stop")
async def stop(name: str, request: Request):
    try:
        return await nifi.set_state(request.app.state.http, name, running=False)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
