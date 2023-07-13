from __future__ import annotations

from dataclasses import dataclass


@dataclass
class DatasetInfo:
    dataset_id: str
    dataset_versions: dict[str, 'DatasetVersionInfo']
    # common things go here?


@dataclass
class SerializationInfo:
    start_dataset_id: str
    start_dataset_version: str
    dataset_infos: dict[str, DatasetInfo]
    # common things go here?


@dataclass
class FileInfo:
    path: str
    byte_size: int
    executable: bool
    url: str
    annexed: bool
    annex_key: str | None = None
    annex_locations: list[str] | None = None


@dataclass
class DatasetVersionInfo:
    dataset_version: str
    files: dict[str, FileInfo]      # Associate path with file info
    sub_datasets: dict[str, 'SubdatasetInfo']    # Associate path with a dataset version info


@dataclass
class SubdatasetInfo:
    dataset_id: str
    dataset_version: str
