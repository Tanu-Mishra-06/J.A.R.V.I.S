"""Auto-detect the appropriate ingestion adapter for a given file."""

from typing import Optional

from app.ingestion.adapters.base import BaseAdapter
from app.ingestion.adapters.csv_adapter import CsvAdapter
from app.ingestion.adapters.json_adapter import JsonAdapter
from app.ingestion.adapters.log_adapter import LogAdapter

# Ordered by specificity — more structured formats first
_ADAPTERS: list[BaseAdapter] = [
    CsvAdapter(),
    JsonAdapter(),
    LogAdapter(),
]

# Extension-to-source-type mapping
_EXT_MAP = {
    ".csv": "csv",
    ".json": "json",
    ".jsonl": "jsonl",
    ".log": "log",
    ".txt": "log",
}


def detect_source_type(file_path: str) -> str:
    """Return a human-readable source type string based on file extension."""
    import os
    _, ext = os.path.splitext(file_path.lower())
    return _EXT_MAP.get(ext, "unknown")


def detect_adapter(file_path: str) -> Optional[BaseAdapter]:
    """Inspect the file and return the first adapter that can handle it.

    Reads the first 4 KB of the file for content-based sniffing.
    """
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as fh:
            sample = fh.read(4096)
    except Exception:
        sample = ""

    for adapter in _ADAPTERS:
        if adapter.detect(file_path, sample):
            return adapter

    return None
