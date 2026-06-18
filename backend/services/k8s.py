"""Kubernetes view + light mutations for the operator control plane.

Loads in-cluster config when running as a pod, falls back to ~/.kube/config
for Mac/Windows dev (matching how `mac-dev.sh` is run from the host).

Scope is intentionally narrow:
  - List the three Cloudera operators we drive (CSM/Strimzi, CSA/Flink, CFM)
    by their well-known operator deployments + the CRD groups they own.
  - Summarize pods in `cld-streaming`, `cfm-streaming`, `default`.
  - Rollout-restart a deployment (kubectl-style annotation patch).
  - Delete a single pod.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Iterable

from kubernetes_asyncio import client, config
from kubernetes_asyncio.client.api_client import ApiClient
from kubernetes_asyncio.client.exceptions import ApiException

WATCHED_NS: tuple[str, ...] = ("cld-streaming", "cfm-streaming", "default")

# (display_name, deployment_name, namespace, crd_group_suffixes)
OPERATORS: list[tuple[str, str, str, tuple[str, ...]]] = [
    ("CSM (Strimzi)", "strimzi-cluster-operator", "cld-streaming",
     ("kafka.strimzi.io", "core.strimzi.io")),
    ("CSA (Flink)", "flink-kubernetes-operator", "cld-streaming",
     ("flink.apache.org",)),
    ("CFM (NiFi)", "cfm-operator", "cfm-streaming",
     ("cfm.cloudera.com",)),
]

_IMAGE_TAG_RE = re.compile(r":([^:/]+)$")


async def _api_client() -> ApiClient:
    """Return a configured ApiClient; caller is responsible for closing it."""
    try:
        config.load_incluster_config()
    except config.ConfigException:
        await config.load_kube_config()
    return ApiClient()


def _image_version(image: str | None) -> str:
    if not image:
        return ""
    m = _IMAGE_TAG_RE.search(image)
    return m.group(1) if m else ""


def _ready_replicas(dep) -> tuple[int, int]:
    """(ready, desired) — desired falls back to spec.replicas."""
    status = dep.status
    spec = dep.spec
    desired = spec.replicas if spec and spec.replicas is not None else 0
    ready = status.ready_replicas if status and status.ready_replicas else 0
    return ready, desired


async def list_operators() -> list[dict]:
    api = await _api_client()
    apps = client.AppsV1Api(api)
    ext = client.ApiextensionsV1Api(api)
    try:
        crds = await ext.list_custom_resource_definition()
        crd_groups: dict[str, int] = {}
        for c in crds.items:
            g = c.spec.group
            crd_groups[g] = crd_groups.get(g, 0) + 1

        out: list[dict] = []
        for display, name, ns, groups in OPERATORS:
            entry: dict = {
                "name": display,
                "deployment": name,
                "namespace": ns,
                "installed": False,
                "ready": 0,
                "replicas": 0,
                "image": "",
                "version": "",
                "crd_groups": list(groups),
                "crds_present": sum(crd_groups.get(g, 0) for g in groups),
            }
            try:
                dep = await apps.read_namespaced_deployment(name=name, namespace=ns)
            except ApiException as e:
                if e.status == 404:
                    out.append(entry)
                    continue
                entry["error"] = f"{e.status} {e.reason}"
                out.append(entry)
                continue
            ready, desired = _ready_replicas(dep)
            containers = dep.spec.template.spec.containers if dep.spec else []
            image = containers[0].image if containers else ""
            labels = dep.metadata.labels or {}
            version = (
                labels.get("app.kubernetes.io/version")
                or labels.get("version")
                or _image_version(image)
            )
            entry.update({
                "installed": True,
                "ready": ready,
                "replicas": desired,
                "image": image,
                "version": version,
            })
            out.append(entry)
        return out
    finally:
        await api.close()


def _pod_ready(pod) -> tuple[int, int]:
    statuses = pod.status.container_statuses or []
    total = len(statuses)
    ready = sum(1 for s in statuses if s.ready)
    return ready, total


def _pod_restarts(pod) -> int:
    statuses = pod.status.container_statuses or []
    return sum(s.restart_count or 0 for s in statuses)


def _age_seconds(start) -> int:
    if not start:
        return 0
    now = datetime.now(timezone.utc)
    return max(0, int((now - start).total_seconds()))


async def pod_summary(namespaces: Iterable[str] = WATCHED_NS) -> list[dict]:
    api = await _api_client()
    core = client.CoreV1Api(api)
    try:
        out: list[dict] = []
        for ns in namespaces:
            try:
                pods = await core.list_namespaced_pod(namespace=ns)
            except ApiException as e:
                out.append({"ns": ns, "error": f"{e.status} {e.reason}",
                            "total": 0, "running": 0, "pending": 0,
                            "failed": 0, "succeeded": 0, "pods": []})
                continue
            counts = {"Running": 0, "Pending": 0, "Failed": 0, "Succeeded": 0}
            pod_list: list[dict] = []
            for p in pods.items:
                phase = p.status.phase or "Unknown"
                counts[phase] = counts.get(phase, 0) + 1
                ready, total = _pod_ready(p)
                owner = (p.metadata.owner_references or [None])[0]
                pod_list.append({
                    "name": p.metadata.name,
                    "phase": phase,
                    "ready": ready,
                    "containers": total,
                    "restarts": _pod_restarts(p),
                    "age_seconds": _age_seconds(p.status.start_time),
                    "node": p.spec.node_name or "",
                    "owner_kind": owner.kind if owner else "",
                    "owner_name": owner.name if owner else "",
                })
            pod_list.sort(key=lambda x: x["name"])
            out.append({
                "ns": ns,
                "total": len(pods.items),
                "running": counts.get("Running", 0),
                "pending": counts.get("Pending", 0),
                "failed": counts.get("Failed", 0),
                "succeeded": counts.get("Succeeded", 0),
                "pods": pod_list,
            })
        return out
    finally:
        await api.close()


async def restart_deployment(ns: str, name: str, when: datetime) -> dict:
    """Mirror `kubectl rollout restart deploy/<name>` — patch the pod template
    annotations so the controller rolls a new ReplicaSet."""
    api = await _api_client()
    apps = client.AppsV1Api(api)
    try:
        body = {
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "kubectl.kubernetes.io/restartedAt": when.isoformat(),
                        }
                    }
                }
            }
        }
        dep = await apps.patch_namespaced_deployment(
            name=name, namespace=ns, body=body
        )
        return {"name": dep.metadata.name, "namespace": dep.metadata.namespace,
                "restartedAt": when.isoformat()}
    finally:
        await api.close()


async def delete_pod(ns: str, name: str) -> dict:
    api = await _api_client()
    core = client.CoreV1Api(api)
    try:
        await core.delete_namespaced_pod(name=name, namespace=ns)
        return {"name": name, "namespace": ns, "deleted": True}
    finally:
        await api.close()
