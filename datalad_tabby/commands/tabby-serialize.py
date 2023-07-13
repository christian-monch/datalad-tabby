from __future__ import annotations

import json
from argparse import ArgumentParser
from dataclasses import (
    dataclass,
    asdict,
)

from datalad_tabby.commands.iter_dataset import iter_dataset


argument_parser = ArgumentParser(
    prog='iterate_dataset',
    description='Iterate over the elements of a dataset',
)

argument_parser.add_argument(
    'top_level_dir',
    help='directory that contains a datalad dataset'
)

argument_parser.add_argument(
    'item_type',
    default='both',
    help='type of the items that should be returned ("file", "dataset", or "both" [default])'
)

argument_parser.add_argument(
    'traverse_sub_datasets',
    default=False,
    type=bool,
    help='traverse subdatasets ("True" or "False" [default])'
)


@dataclass
class DatasetInfo:
    dataset_id: str
    dataset_versions: dict[str, 'DatasetVersionInfo']
    # common things go here?


@dataclass
class FileInfo:
    path: str
    byte_size: int
    executable: bool
    annexed: bool
    annex_key: str | None = None
    annex_locations: list[str] | None = None


@dataclass
class DatasetVersionInfo:
    dataset_version: str
    files: dict[str, FileInfo]      # Associate path with file info
    sub_datasets: dict[str, 'DatasetVersionInfo']    # Associate path with a dataset version info


def add_dataset_info(uuid_str: str, status: dict) -> DatasetInfo:
    """ Add the dataset UUID to the status dict"""
    if uuid_str not in status:
        status[uuid_str] = DatasetInfo(
            dataset_id=uuid_str,
            dataset_versions=dict())
    return status[uuid_str]


def add_dataset_version_info(dataset_element: dict, status: dict) -> DatasetVersionInfo:

    def add_dataset_version(id_str: str, version_str: str) -> DatasetVersionInfo:
        dataset_info = add_dataset_info(id_str, status)
        if version_str not in dataset_info.dataset_versions:
            dataset_info.dataset_versions[version_str] = DatasetVersionInfo(version_str, dict(), dict())
        return dataset_info.dataset_versions[version_str]

    dataset_version_info = add_dataset_version(
        id_str=dataset_element['dataset_id'],
        version_str=dataset_element['dataset_version'])

    if 'root_dataset_id' in dataset_element:
        # We have to link this as a sub-dataset version of the super-dataset version.
        root_dataset_version_info = add_dataset_version(
            id_str=dataset_element['root_dataset_id'],
            version_str=dataset_element['root_dataset_version'])
        root_dataset_version_info.sub_datasets[dataset_element['dataset_path']] = dataset_version_info

    return dataset_version_info


def add_file_info(dataset_element: dict, dataset_version: DatasetVersionInfo):
    path = dataset_element['intra_dataset_path']
    if path not in dataset_version.files:
        annexed = dataset_element['annexed']
        dataset_version.files[path] = FileInfo(
            path=path,
            byte_size=dataset_element['bytesize'],
            executable=dataset_element['executable'],
            annexed=annexed,
            **(dict(
                annex_key=dataset_element['key'],
                annex_locations=dataset_element['locations']
            ) if annexed else dict())
        )
    return dataset_version.files[path]


def process(dataset_element: dict, status: dict):
    #print(json.dumps(dataset_element))
    #return
    dataset_info = add_dataset_info(dataset_element['dataset_id'], status)
    dataset_version_info = add_dataset_version_info(dataset_element, status)
    if dataset_element['type'] == 'file':
        file_info = add_file_info(dataset_element, dataset_version_info)
    elif dataset_element['type'] == 'dataset':
        # The dataset was already added above
        pass
    else:
        raise ValueError(f'unknown element type: ``{dataset_element["type"]}´´')


def main():
    arguments = argument_parser.parse_args()
    status = dict()
    for result in iter_dataset(dataset_dir=arguments.top_level_dir,
                               item_type=arguments.item_type,
                               traverse_sub_datasets=arguments.traverse_sub_datasets):
        process(result, status)
    print(json.dumps({key: asdict(value) for key, value in status.items()}))


if __name__ == '__main__':
    main()
