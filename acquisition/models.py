from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class AcquisitionRequest:
    catalog: str
    collections: list[str]
    wkt: str
    start_date: str
    end_date: str
    max_items: int | None = None
    only_main: bool = True
    batch_size: int = 20
    download_concurrency: int = 3
    max_buffered_batches: int = 2

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


TERMINAL_STATUSES = {"completed", "partial", "failed", "cancelled"}
ACTIVE_STATUSES = {"queued", "recovering", "discovering", "planning", "downloading", "finalizing", "cancelling"}
