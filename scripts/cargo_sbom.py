#!/usr/bin/env python3
"""Generate a Cargo dependency CycloneDX BOM and declared-license inventory."""
import argparse
import hashlib
import hashlib
import json
import subprocess
import tomllib
import uuid
from datetime import datetime, timezone
from pathlib import Path


def cargo_metadata(root):
    return json.loads(
        subprocess.check_output(
            ["cargo", "metadata", "--locked", "--format-version", "1"],
            cwd=root,
            text=True,
        )
    )


def generate(metadata, lock, lock_sha256, created):
    packages = {package["id"]: package for package in metadata["packages"]}
    nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
    root_id = metadata["resolve"]["root"]
    if not root_id or root_id not in packages:
        raise ValueError("Cargo metadata has no application root")
    checksum_by_name_version = {
        (package["name"], package["version"], package.get("source")): package["checksum"]
        for package in lock["package"]
        if "checksum" in package
    }

    def ref(package_id):
        digest = hashlib.sha256(package_id.encode("utf-8")).hexdigest()[:24]
        return f"urn:cargo:package:{digest}"

    def component(package_id, root=False):
        package = packages[package_id]
        license_name = package.get("license")
        license_file = package.get("license_file")
        if not license_name and not license_file:
            raise ValueError(f"Missing Cargo license metadata: {package['name']} {package['version']}")
        result = {
            "type": "application" if root else "library",
            "bom-ref": ref(package_id),
            "name": package["name"],
            "version": package["version"],
            "purl": f"pkg:cargo/{package['name']}@{package['version']}",
            "licenses": [
                {"license": {"name": license_name or license_file}}
            ],
        }
        if package.get("source"):
            result["properties"] = [{"name": "cargo:source", "value": package["source"]}]
        if package.get("repository"):
            result["externalReferences"] = [{"type": "vcs", "url": package["repository"]}]
        checksum = checksum_by_name_version.get(
            (package["name"], package["version"], package.get("source"))
        )
        if checksum:
            result["hashes"] = [{"alg": "SHA-256", "content": checksum}]
        return result

    component_rows = [component(package_id, package_id == root_id) for package_id in nodes]
    root_component = next(row for row in component_rows if row["bom-ref"] == ref(root_id))
    dependencies = []
    for package_id, node in nodes.items():
        dependencies.append(
            {
                "ref": ref(package_id),
                "dependsOn": sorted({ref(edge["pkg"]) for edge in node["deps"]}),
            }
        )
    rows = [
        {
            "name": packages[package_id]["name"],
            "version": packages[package_id]["version"],
            "source": packages[package_id].get("source"),
            "declared_license": packages[package_id].get("license"),
            "license_file": packages[package_id].get("license_file"),
        }
        for package_id in sorted(nodes, key=lambda item: (packages[item]["name"], packages[item]["version"], item))
    ]
    return (
        {
            "bomFormat": "CycloneDX",
            "specVersion": "1.5",
            "serialNumber": f"urn:uuid:{uuid.uuid4()}",
            "version": 1,
            "metadata": {
                "timestamp": created,
                "component": root_component,
                "properties": [{"name": "cargo:lockfile-sha256", "value": lock_sha256}],
            },
            "components": [row for row in component_rows if row["bom-ref"] != ref(root_id)],
            "dependencies": dependencies,
        },
        {"schema_version": 1, "packages": rows},
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    metadata = cargo_metadata(root)
    lock_path = root / "Cargo.lock"
    lock_bytes = lock_path.read_bytes()
    lock = tomllib.loads(lock_bytes.decode("utf-8"))
    lock_sha256 = hashlib.sha256(lock_bytes).hexdigest()
    timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    bom, licenses = generate(metadata, lock, lock_sha256, timestamp)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "data-studio.cdx.json").write_text(
        json.dumps(bom, indent=2) + "\n", encoding="utf-8"
    )
    (args.output_dir / "cargo-license-metadata.json").write_text(
        json.dumps(licenses, indent=2) + "\n", encoding="utf-8"
    )
    print(f"CycloneDX 1.5: {len(bom['components'])} dependency components; declared license metadata present")


if __name__ == "__main__":
    main()
