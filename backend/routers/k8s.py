from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException

from services import k8s as k8s_svc

router = APIRouter(prefix="/k8s")

# Mutations are restricted to the namespaces we drive — same scope as the
# RoleBindings, so a request outside the set is rejected before it hits the
# kube API and bounces back as a 403.
ALLOWED_NS = {"cld-streaming", "cfm-streaming", "default"}


@router.get("/operators")
async def operators():
    try:
        return await k8s_svc.list_operators()
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.get("/pods")
async def pods():
    try:
        return await k8s_svc.pod_summary()
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.post("/deploy/{ns}/{name}/restart")
async def restart(ns: str, name: str):
    if ns not in ALLOWED_NS:
        raise HTTPException(status_code=400, detail=f"namespace {ns} not allowed")
    try:
        return await k8s_svc.restart_deployment(ns, name, datetime.now(timezone.utc))
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.delete("/pod/{ns}/{name}")
async def delete_pod(ns: str, name: str):
    if ns not in ALLOWED_NS:
        raise HTTPException(status_code=400, detail=f"namespace {ns} not allowed")
    try:
        return await k8s_svc.delete_pod(ns, name)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))
