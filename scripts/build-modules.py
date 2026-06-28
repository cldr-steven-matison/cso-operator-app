#!/usr/bin/env python3
"""
Reads the MODULES build arg (comma-separated list or "all") and writes
build/modules.json — the manifest the backend and frontend use to know
which optional modules are enabled in this image.

Usage: python3 scripts/build-modules.py [modules_arg]
  modules_arg: comma-separated module names, "all", or "" (none)
"""
import json
import os
import sys
from pathlib import Path

KNOWN_MODULES = ["streamers"]

arg = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("MODULES", "")
arg = arg.strip()

if arg.lower() == "all":
    enabled = KNOWN_MODULES[:]
else:
    enabled = [m.strip() for m in arg.split(",") if m.strip() in KNOWN_MODULES]

out_dir = Path(__file__).parent.parent / "build"
out_dir.mkdir(exist_ok=True)
(out_dir / "modules.json").write_text(json.dumps({"modules": enabled}, indent=2))

print(f"Modules enabled: {enabled or '(none)'}")
