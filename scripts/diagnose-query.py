"""In-pod diagnostic for the RAG Query path.

Run inside the `cso-operator-app` pod with python stdlib only — no deps to
install. Probes every hop the Ask button takes:

  1. Resolved env (VLLM_URL, EMBED_URL, QDRANT_URL).
  2. GET  vLLM /v1/models                — vLLM reachable?
  3. POST vLLM /v1/chat/completions      — vLLM accepts our request shape?
  4. GET  app  /api/health               — backend's own view of services.
  5. POST app  /api/query (SSE)          — full body, every byte, status,
                                           and content-type.

Each step prints STATUS / BODY / ERR so we can see exactly where the
pipeline dies. Nothing is fatal — every step continues on failure so
you get the full picture in one run.

Invocation from a /bash agent:

    kubectl cp scripts/diagnose-query.py \\
      $(kubectl get pod -l app=cso-operator-app -o jsonpath='{.items[0].metadata.name}'):/tmp/d.py \\
    && kubectl exec deploy/cso-operator-app -- python3 /tmp/d.py

Or — if the script is already baked into the image at /app/scripts:

    kubectl exec deploy/cso-operator-app -- python3 /app/scripts/diagnose-query.py
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request


VLLM_URL = os.environ.get("VLLM_URL", "http://vllm-service.default.svc.cluster.local:8000")
VLLM_MODEL = os.environ.get("VLLM_MODEL", "Qwen/Qwen2.5-3B-Instruct")
EMBED_URL = os.environ.get("EMBED_URL", "")
QDRANT_URL = os.environ.get("QDRANT_URL", "")
APP_BASE = "http://localhost:8000"


def hr(title: str) -> None:
    print()
    print("=" * 8, title, "=" * 8)


def get(url: str, timeout: float = 10.0):
    try:
        r = urllib.request.urlopen(url, timeout=timeout)
        body = r.read()
        return r.status, dict(r.headers), body, None
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers or {}), e.read(), None
    except Exception as e:
        return None, {}, b"", repr(e)


def post_json(url: str, payload: dict, timeout: float = 60.0):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=data, headers={"content-type": "application/json"}
    )
    try:
        r = urllib.request.urlopen(req, timeout=timeout)
        body = r.read()
        return r.status, dict(r.headers), body, None
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers or {}), e.read(), None
    except Exception as e:
        return None, {}, b"", repr(e)


def post_stream(url: str, payload: dict, timeout: float = 120.0):
    """POST and read the body chunk-by-chunk so we can see streaming output
    in real time even if the server never closes the connection."""
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=data, headers={"content-type": "application/json"}
    )
    started = time.time()
    try:
        r = urllib.request.urlopen(req, timeout=timeout)
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers or {}), e.read(), None
    except Exception as e:
        return None, {}, b"", repr(e)

    print("STATUS:", r.status)
    print("HEADERS:", json.dumps(dict(r.headers), indent=2))
    print("---BODY (streamed)---")
    chunks: list[bytes] = []
    try:
        while True:
            chunk = r.read(1024)
            if not chunk:
                break
            chunks.append(chunk)
            try:
                sys.stdout.write(chunk.decode("utf-8", "replace"))
            except Exception:
                sys.stdout.write(repr(chunk))
            sys.stdout.flush()
    except Exception as e:
        print(f"\n[stream read error: {e!r}]")
    elapsed = time.time() - started
    total = sum(len(c) for c in chunks)
    print(f"\n---END--- {total} bytes in {elapsed:.2f}s")
    return r.status, dict(r.headers), b"".join(chunks), None


# 1. Env
hr("env")
print("VLLM_URL   =", VLLM_URL)
print("VLLM_MODEL =", VLLM_MODEL)
print("EMBED_URL  =", EMBED_URL or "(unset — config default will be used)")
print("QDRANT_URL =", QDRANT_URL or "(unset — config default will be used)")

# 2. vLLM /v1/models
hr("GET vllm /v1/models")
status, _, body, err = get(f"{VLLM_URL}/v1/models", timeout=10)
print("STATUS:", status, "ERR:", err)
print(body[:500].decode("utf-8", "replace"))

# 3. vLLM chat completion (non-streaming, blog shape)
hr("POST vllm /v1/chat/completions")
status, _, body, err = post_json(
    f"{VLLM_URL}/v1/chat/completions",
    {
        "model": VLLM_MODEL,
        "messages": [
            {"role": "system", "content": "Briefly answer using this context."},
            {"role": "user", "content": "Context: hello world.\n\nQuestion: say hi"},
        ],
        "max_tokens": 32,
    },
    timeout=60,
)
print("STATUS:", status, "ERR:", err)
print(body[:1000].decode("utf-8", "replace"))

# 4. App health
hr("GET app /api/health")
status, _, body, err = get(f"{APP_BASE}/api/health", timeout=15)
print("STATUS:", status, "ERR:", err)
try:
    print(json.dumps(json.loads(body.decode()), indent=2))
except Exception:
    print(body[:1000].decode("utf-8", "replace"))

# 5. App /api/query — the actual Ask path
hr("POST app /api/query")
post_stream(f"{APP_BASE}/api/query", {"question": "What is StreamToVLLM?"}, timeout=120)
