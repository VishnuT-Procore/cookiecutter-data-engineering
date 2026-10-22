#!/usr/bin/env python3
"""Download the pinned Flink connector jars into ingestion/flink/lib and verify their SHA-1 checksums.

    python ingestion/flink/fetch_jars.py
    MAVEN_REPO=https://my.mirror/maven2 python ingestion/flink/fetch_jars.py
"""

from __future__ import annotations

import hashlib
import os
import urllib.request
from pathlib import Path

LIB = Path(__file__).resolve().parent / "lib"

# (group, artifact, version). Keep in step with the Flink version in the Dockerfile.
JARS = [
    ("org.apache.flink", "flink-sql-connector-kafka", "3.4.0-1.20"),
    ("org.apache.iceberg", "iceberg-flink-runtime-1.20", "1.10.0"),
    ("org.apache.iceberg", "iceberg-aws-bundle", "1.10.0"),  # S3FileIO (also for S3-compatible storage)
    ("org.apache.hadoop", "hadoop-client-api", "3.3.6"),  # Iceberg's Flink runtime needs Hadoop classes
    ("org.apache.hadoop", "hadoop-client-runtime", "3.3.6"),
]


def _get(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=300) as response:
        return response.read()


def fetch(repo: str | None = None) -> list[Path]:
    repo = (repo or os.environ.get("MAVEN_REPO") or "https://repo1.maven.org/maven2").rstrip("/")
    LIB.mkdir(exist_ok=True)
    paths = []
    for group, artifact, version in JARS:
        name = f"{artifact}-{version}.jar"
        path = LIB / name
        url = f"{repo}/{group.replace('.', '/')}/{artifact}/{version}/{name}"
        expected = _get(url + ".sha1").decode().split()[0]
        if not path.exists() or hashlib.sha1(path.read_bytes()).hexdigest() != expected:
            data = _get(url)
            actual = hashlib.sha1(data).hexdigest()
            if actual != expected:
                raise RuntimeError(f"checksum mismatch for {name}: {actual} != {expected}")
            path.write_bytes(data)
            print(f"downloaded {name}")
        paths.append(path)
    return paths


if __name__ == "__main__":
    fetch()
