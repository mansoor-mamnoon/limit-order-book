# python/olob/checksum.py
from __future__ import annotations
import hashlib, json
from pathlib import Path
from typing import Iterable

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()

def write_checksums(paths: Iterable[Path], out_path: Path) -> dict:
    out = {}
    for p in paths:
        p = Path(p)
        out[str(p)] = sha256_file(p)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2)
    return out
