"""Abstract base adapter for data ingestion."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ParseResult:
    """Container for parsed records from any adapter."""
    events: list[dict[str, Any]] = field(default_factory=list)
    metrics: list[dict[str, Any]] = field(default_factory=list)
    traces: list[dict[str, Any]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def total_records(self) -> int:
        return len(self.events) + len(self.metrics) + len(self.traces)

    @property
    def valid_records(self) -> int:
        return self.total_records

    @property
    def invalid_records(self) -> int:
        return len(self.errors)


class BaseAdapter(ABC):
    """Base class all ingestion adapters must implement."""

    @abstractmethod
    def detect(self, file_path: str, sample: str) -> bool:
        """Return True if this adapter can handle the given file.

        Args:
            file_path: Path to the file on disk.
            sample: First ~4 KB of the file content for sniffing.
        """
        ...

    @abstractmethod
    def parse(self, file_path: str, dataset_id: str) -> ParseResult:
        """Parse the file and return normalized records.

        Args:
            file_path: Absolute path to the data file.
            dataset_id: UUID of the parent Dataset record.
        """
        ...
