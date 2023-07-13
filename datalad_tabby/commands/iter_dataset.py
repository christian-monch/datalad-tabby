#!/usr/bin/env python3
"""
This is a mock for a dataset iterator, which is required for serialization.
In order to keep this code simple, it uses an existing dataset traverser
from the following branch of metalad:

    https://github.com/christian-monch/datalad-metalad/tree/test-iterator-2

The branch is added to `requirements-devel.txt` as dependency

"""
from __future__ import annotations

from pathlib import Path

from datalad_metalad.pipeline.provider.datasettraverse import DatasetTraverser
from datalad_metalad.pipeline.pipelinedata import (
    PipelineDataState,
    ResultState,
)


def iter_dataset(dataset_dir: Path,
                 item_type: str = 'both',
                 traverse_sub_datasets: bool = False):
    traverser = DatasetTraverser(
        top_level_dir=dataset_dir,
        item_type=item_type,
        traverse_sub_datasets=traverse_sub_datasets
    )
    for element in traverser.next_object():
        if element.state == PipelineDataState.CONTINUE:
            for result in element.get_result('dataset-traversal-record'):
                if result.state == ResultState.SUCCESS:
                    output = result.to_dict()
                    for key in ('dataset_id', 'dataset_version', 'fs_base_path', 'root_dataset_id', 'root_dataset_version'):
                        if key in output:
                            output['element_info'][key] = output[key]
                    output['element_info']['status'] = 'ok'
                    yield output['element_info']
