from __future__ import annotations

import json
from argparse import ArgumentParser
from dataclasses import asdict
from pathlib import Path

from datalad.distribution.dataset import require_dataset
from datalad_tabby.commands.iter_dataset import iter_dataset
from datalad_tabby.commands.model import (
    DatasetInfo,
    DatasetVersionInfo,
    FileInfo,
    SerializationInfo,
    SubdatasetInfo,
)


argument_parser = ArgumentParser(
    prog='iterate_dataset',
    description='Iterate over the elements of a dataset',
)

argument_parser.add_argument(
    'top_level_dir',
    help='directory that contains a datalad dataset'
)

argument_parser.add_argument(
    '-r', '--recursive',
    action='store_true',
    help='if given, traverse subdatasets)'
)

argument_parser.add_argument(
    '-o', '--output_dir',
    default='.',
    help='directory to which the output should be written'
)


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
        root_dataset_version_info.sub_datasets[dataset_element['dataset_path']] = SubdatasetInfo(
            dataset_id=dataset_element['root_dataset_id'],
            dataset_version=dataset_version_info.dataset_version)

    return dataset_version_info


def add_file_info(dataset_element: dict, dataset_version: DatasetVersionInfo):
    path = dataset_element['intra_dataset_path']
    if path not in dataset_version.files:
        annexed = dataset_element['annexed']
        dataset_version.files[path] = FileInfo(
            path=path,
            byte_size=dataset_element['bytesize'],
            executable=dataset_element['executable'],
            url='file://' + dataset_element['path'],
            annexed=annexed,
            **(dict(
                annex_key=dataset_element['key'],
                annex_locations=dataset_element['locations']
            ) if annexed else dict())
        )
    return dataset_version.files[path]


def process(dataset_element: dict, serialization: SerializationInfo):
    dataset_version_info = add_dataset_version_info(dataset_element, serialization.dataset_infos)
    if dataset_element['type'] == 'file':
        add_file_info(dataset_element, dataset_version_info)
    elif dataset_element['type'] == 'dataset':
        # The dataset was already added above
        pass
    else:
        raise ValueError(f'unknown element type: ``{dataset_element["type"]}´´')


def output_file_table(version_dir: Path, files: dict[str, FileInfo]):
    file_table = version_dir / 'file.tsv'
    with file_table.open(mode='wt') as f:
        f.write('path[POSIX]\tsize[bytes]\tchecksum[md5]\turl\tannex_key\tlocations\n')
        for path, file_info in files.items():
            f.write(f'{file_info.path}\t{file_info.byte_size}\t\t{file_info.url}')
            if file_info.annexed:
                f.write(f'\t{file_info.annex_key}')
                for location in file_info.annex_locations:
                    f.write(f'\t{location}')
            f.write('\n')


def output_subdataset_table(version_dir: Path, subdatasets: dict[str, SubdatasetInfo]):
    if not subdatasets:
        return
    file_table = version_dir / 'subdatasets.tsv'
    with file_table.open(mode='wt') as f:
        f.write('path[POSIX]\tdataset-UUID\tdataset-version\n')
        for path, subdataset_info in subdatasets.items():
            f.write(f'{path}\t{subdataset_info.dataset_id}\t{subdataset_info.dataset_version}\n')


def output_dataset_version(dataset_dir: Path, version: str, dataset_version_info: DatasetVersionInfo):
    version_dir = dataset_dir / version
    version_dir.mkdir(parents=True, exist_ok=True)
    output_file_table(version_dir, dataset_version_info.files)
    output_subdataset_table(version_dir, dataset_version_info.sub_datasets)


def output_dataset(output_dir: Path, dataset_id: str, dataset_info: DatasetInfo):
    id_dir = output_dir / dataset_id
    id_dir.mkdir(parents=True, exist_ok=True)
    for version, dataset_version_info in dataset_info.dataset_versions.items():
        output_dataset_version(id_dir, version, dataset_version_info)


def output(serialization: SerializationInfo, output_dir: str):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    for dataset_id, dataset_info in serialization.dataset_infos.items():
        output_dataset(output_dir, dataset_id, dataset_info)
    (output_dir / 'root').symlink_to(
        output_dir / serialization.start_dataset_id / serialization.start_dataset_version,
        target_is_directory=True)


def main():
    arguments = argument_parser.parse_args()

    dataset_dir = arguments.top_level_dir
    dataset = require_dataset(dataset_dir)
    start_dataset_id = dataset.id
    start_dataset_version = dataset.repo.get_hexsha()

    serialization = SerializationInfo(start_dataset_id, start_dataset_version, dict())
    for result in iter_dataset(dataset_dir=dataset_dir,
                               item_type='both',
                               traverse_sub_datasets=arguments.recursive):
        process(result, serialization)
    print(json.dumps(asdict(serialization)))
    output(serialization, output_dir=arguments.output_dir)


if __name__ == '__main__':
    main()
