"""Filesystem DLQ helpers."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class FilesystemDlq:
    def __init__(self, root_dir: str) -> None:
        self.root = Path(root_dir)

    def write(self, kind: str, payload: dict[str, Any]) -> Path:
        target_dir = self.root / kind
        target_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        path = target_dir / f"{timestamp}-{uuid.uuid4().hex}.json"
        path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
        return path

    def iter_kind(self, kind: str) -> list[Path]:
        target = self.root / kind
        if not target.exists():
            return []
        return sorted(path for path in target.iterdir() if path.suffix == ".json")

