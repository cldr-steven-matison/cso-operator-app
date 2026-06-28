#!/usr/bin/env python3
"""
setup-streamers-flows.py — Create the StreamersApp NiFi process groups via REST API.

Run from inside the app pod:
  kubectl exec deploy/cso-operator-app -- python3 /app/scripts/setup-streamers-flows.py

Or locally with port-forward to NiFi (NIFI_URL=https://localhost:8443):
  NIFI_URL=https://localhost:8443 NIFI_USERNAME=admin NIFI_PASSWORD=admin12345678 python3 scripts/setup-streamers-flows.py

Creates:
  StreamersApp (parent PG at root)
  ├── FetchClips  — GenerateFlowFile (15min) → InvokeHTTP → /api/streamers/fetch-clips
  ├── ProcessClips — ConsumeKafka(new_clips) → InvokeHTTP → /api/streamers/process-clip → PublishKafka(processed_clips)
  └── PublishClip  — HandleHttpRequest(:9001) → InvokeHTTP → /api/streamers/publish → HandleHttpResponse
"""

import json
import os
import sys
import uuid

import httpx

NIFI_URL = os.environ.get("NIFI_URL", "https://mynifi-web.cfm-streaming.svc.cluster.local:8443")
NIFI_USERNAME = os.environ.get("NIFI_USERNAME", "admin")
NIFI_PASSWORD = os.environ.get("NIFI_PASSWORD", "admin12345678")
APP_URL = os.environ.get("APP_URL", "http://cso-operator-app.default.svc.cluster.local:8000")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "my-cluster-kafka-bootstrap.cld-streaming.svc:9092")

NAR_KAFKA = {"group": "org.apache.nifi", "artifact": "nifi-kafka-2-6-nar", "version": "1.28.1.2.3.17.0-9"}
NAR_STD   = {"group": "org.apache.nifi", "artifact": "nifi-standard-nar",  "version": "1.28.1.2.3.17.0-9"}

_token = ""

client = httpx.Client(verify=False, timeout=30.0)


def get_token() -> str:
    global _token
    if _token:
        return _token
    r = client.post(
        f"{NIFI_URL}/nifi-api/access/token",
        data={"username": NIFI_USERNAME, "password": NIFI_PASSWORD},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    r.raise_for_status()
    client.cookies.clear()
    _token = r.text.strip()
    return _token


def headers() -> dict:
    return {"Authorization": f"Bearer {get_token()}"}


def nifi_get(path: str) -> dict:
    r = client.get(f"{NIFI_URL}/nifi-api{path}", headers=headers())
    if r.status_code == 401:
        global _token; _token = ""
        r = client.get(f"{NIFI_URL}/nifi-api{path}", headers=headers())
    r.raise_for_status()
    return r.json()


def nifi_post(path: str, body: dict) -> dict:
    r = client.post(f"{NIFI_URL}/nifi-api{path}", headers=headers(), json=body)
    if r.status_code == 401:
        global _token; _token = ""
        r = client.post(f"{NIFI_URL}/nifi-api{path}", headers=headers(), json=body)
    r.raise_for_status()
    return r.json()


def get_root_pg_id() -> str:
    data = nifi_get("/process-groups/root")
    return data["id"]


def find_pg_by_name(parent_id: str, name: str) -> dict | None:
    data = nifi_get(f"/process-groups/{parent_id}/process-groups")
    for pg in data.get("processGroups", []):
        if pg.get("component", {}).get("name") == name:
            return pg
    return None


def create_pg(parent_id: str, name: str, x: float = 0, y: float = 0) -> dict:
    existing = find_pg_by_name(parent_id, name)
    if existing:
        print(f"  [skip] {name} already exists (id={existing['id']})")
        return existing
    body = {
        "revision": {"version": 0},
        "component": {
            "name": name,
            "position": {"x": x, "y": y},
        },
    }
    result = nifi_post(f"/process-groups/{parent_id}/process-groups", body)
    print(f"  [created] {name} (id={result['id']})")
    return result


def create_processor(pg_id: str, name: str, proc_type: str, bundle: dict,
                     properties: dict, x: float = 0, y: float = 0,
                     schedule: str = "0 sec", auto_terminate: list | None = None) -> dict:
    body = {
        "revision": {"version": 0},
        "component": {
            "name": name,
            "type": proc_type,
            "bundle": bundle,
            "position": {"x": x, "y": y},
            "config": {
                "schedulingPeriod": schedule,
                "schedulingStrategy": "TIMER_DRIVEN",
                "autoTerminatedRelationships": auto_terminate or [],
                "properties": properties,
            },
        },
    }
    result = nifi_post(f"/process-groups/{pg_id}/processors", body)
    print(f"    [proc] {name} ({result['id']})")
    return result


def create_connection(pg_id: str, src_id: str, src_type: str,
                      dst_id: str, dst_type: str, relationships: list) -> dict:
    body = {
        "revision": {"version": 0},
        "component": {
            "source": {"id": src_id, "type": src_type, "groupId": pg_id},
            "destination": {"id": dst_id, "type": dst_type, "groupId": pg_id},
            "selectedRelationships": relationships,
            "backPressureObjectThreshold": 10000,
            "backPressureDataSizeThreshold": "1 GB",
            "flowFileExpiration": "0 sec",
            "prioritizers": [],
        },
    }
    result = nifi_post(f"/process-groups/{pg_id}/connections", body)
    print(f"    [conn] {relationships} → {dst_id[:8]}…")
    return result


# ── Flow builders ─────────────────────────────────────────────────────────────

def build_fetch_clips(pg_id: str):
    print("  Building FetchClips processors…")

    gen = create_processor(
        pg_id, "GenerateFlowFile",
        "org.apache.nifi.processors.standard.GenerateFlowFile", NAR_STD,
        {"File Size": "0 B", "Batch Size": "1", "Data Format": "Text", "Unique FlowFiles": "false"},
        x=0, y=0, schedule="15 min",
    )

    invoke = create_processor(
        pg_id, "InvokeHTTP",
        "org.apache.nifi.processors.standard.InvokeHTTP", NAR_STD,
        {
            "Remote URL": f"{APP_URL}/api/streamers/fetch-clips",
            "HTTP Method": "POST",
            "Content-Type": "application/json",
            "Read Timeout": "120 secs",
            "Connection Timeout": "10 secs",
        },
        x=0, y=200,
        auto_terminate=["Response", "No Retry", "Retry", "Failure", "Original"],
    )

    create_connection(pg_id, gen["id"], "PROCESSOR", invoke["id"], "PROCESSOR", ["success"])
    print("  FetchClips done.")


def build_process_clips(pg_id: str):
    print("  Building ProcessClips processors…")

    consume = create_processor(
        pg_id, "ConsumeKafka_2_6",
        "org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_6", NAR_KAFKA,
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "topic": "new_clips",
            "topic_type": "names",
            "group.id": "StreamersProcessClips",
            "auto.offset.reset": "latest",
            "security.protocol": "PLAINTEXT",
            "Commit Offsets": "true",
            "max.poll.records": "1",
        },
        x=0, y=0,
    )

    invoke = create_processor(
        pg_id, "InvokeHTTP",
        "org.apache.nifi.processors.standard.InvokeHTTP", NAR_STD,
        {
            "Remote URL": f"{APP_URL}/api/streamers/process-clip",
            "HTTP Method": "POST",
            "Content-Type": "application/json",
            "Read Timeout": "180 secs",
            "Connection Timeout": "10 secs",
            "send-message-body": "true",
        },
        x=0, y=200,
        auto_terminate=["No Retry", "Retry", "Failure", "Original"],
    )

    publish = create_processor(
        pg_id, "PublishKafka_2_6",
        "org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6", NAR_KAFKA,
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "topic": "processed_clips",
            "security.protocol": "PLAINTEXT",
            "use-transactions": "false",
            "acks": "all",
        },
        x=0, y=400,
        auto_terminate=["success", "failure"],
    )

    create_connection(pg_id, consume["id"], "PROCESSOR", invoke["id"], "PROCESSOR", ["success"])
    create_connection(pg_id, invoke["id"], "PROCESSOR", publish["id"], "PROCESSOR", ["Response"])
    print("  ProcessClips done.")


def build_publish_clip(pg_id: str):
    print("  Building PublishClip processors…")

    listen = create_processor(
        pg_id, "HandleHttpRequest",
        "org.apache.nifi.processors.standard.HandleHttpRequest", NAR_STD,
        {
            "Listening Port": "9001",
            "Allowed Paths": "/contentListener",
            "Allow GET": "false",
            "Allow POST": "true",
            "Allow PUT": "false",
            "Allow DELETE": "false",
            "HTTP Context Map": None,
        },
        x=0, y=0,
    )

    invoke = create_processor(
        pg_id, "InvokeHTTP",
        "org.apache.nifi.processors.standard.InvokeHTTP", NAR_STD,
        {
            "Remote URL": f"{APP_URL}/api/streamers/publish",
            "HTTP Method": "POST",
            "Content-Type": "application/json",
            "Read Timeout": "60 secs",
            "send-message-body": "true",
        },
        x=0, y=200,
        auto_terminate=["No Retry", "Retry", "Failure", "Original"],
    )

    respond = create_processor(
        pg_id, "HandleHttpResponse",
        "org.apache.nifi.processors.standard.HandleHttpResponse", NAR_STD,
        {"HTTP Status Code": "200", "HTTP Context Map": None},
        x=0, y=400,
        auto_terminate=["success", "failure"],
    )

    create_connection(pg_id, listen["id"], "PROCESSOR", invoke["id"], "PROCESSOR", ["success"])
    create_connection(pg_id, invoke["id"], "PROCESSOR", respond["id"], "PROCESSOR", ["Response"])
    print("  PublishClip done.")


def export_flow(pg_id: str, out_path: str):
    """Download the process group as importable JSON and save it."""
    r = client.get(
        f"{NIFI_URL}/nifi-api/process-groups/{pg_id}/download",
        headers=headers(),
        params={"includeReferencedServices": "true"},
    )
    if r.status_code == 200:
        with open(out_path, "w") as f:
            json.dump(r.json(), f, indent=2)
        print(f"  [exported] flow → {out_path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Connecting to NiFi…")
    root_id = get_root_pg_id()
    print(f"Root PG: {root_id}")

    print("\nCreating StreamersApp parent process group…")
    streamers_pg = create_pg(root_id, "StreamersApp", x=2000, y=0)
    s_id = streamers_pg["id"]

    print("\nCreating FetchClips…")
    fetch_pg = create_pg(s_id, "FetchClips", x=0, y=0)
    build_fetch_clips(fetch_pg["id"])

    print("\nCreating ProcessClips…")
    process_pg = create_pg(s_id, "ProcessClips", x=600, y=0)
    build_process_clips(process_pg["id"])

    print("\nCreating PublishClip…")
    publish_pg = create_pg(s_id, "PublishClip", x=1200, y=0)
    build_publish_clip(publish_pg["id"])

    # Export for repo
    export_dir = "/app/streamers" if os.path.exists("/app/streamers") else "streamers"
    export_flow(s_id, f"{export_dir}/StreamersApp.json")

    print("\nDone. StreamersApp flows deployed to NiFi.")
    print(f"StreamersApp PG id: {s_id}")
    print("\nNext steps:")
    print("  1. Start FetchClips and ProcessClips from the Streamers tab in the app")
    print("  2. Add Twitch credentials: kubectl set env deploy/cso-operator-app TWITCH_CLIENT_ID=... TWITCH_CLIENT_SECRET=...")
    print("  3. Set watch list from the Streamers tab or: kubectl set env deploy/cso-operator-app STREAMERS_WATCH_LIST=xQc,summit1g")


if __name__ == "__main__":
    main()
