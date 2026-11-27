#!/usr/bin/env python3
"""Check that consumer manifests and Cargo.lock agree with integration/localwright.json."""
import json
import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PIN = json.loads((ROOT / "integration/localwright.json").read_text(encoding="utf-8"))
revision = PIN["revision"]
manifest = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))
dependencies = manifest["dependencies"]
lock = tomllib.loads((ROOT / "Cargo.lock").read_text(encoding="utf-8"))

for name in ("localwright-core", "localwright-ui"):
    dependency = dependencies[name]
    if dependency.get("rev") != revision or dependency.get("version") != f"={PIN['required_version']}":
        raise SystemExit(f"{name} manifest pin differs from integration/localwright.json")

locked = {
    package["name"]: package
    for package in lock["package"]
    if package["name"] in ("localwright-core", "localwright-ui")
}
if set(locked) != {"localwright-core", "localwright-ui"}:
    raise SystemExit("Cargo.lock is missing a pinned Localwright package")

for name, package in locked.items():
    source = package.get("source", "")
    match = re.search(r"#([0-9a-f]{40})$", source)
    if not match or match.group(1) != revision:
        raise SystemExit(f"{name} Cargo.lock source does not match {revision}")

print(f"Localwright consumer pin verified: {revision}")
