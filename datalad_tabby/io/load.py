"""Utilities for loading a `tabby` record from disk"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import (
    Dict,
    List,
)

from .load_utils import (
    _assigned_context,
    _compact_obj,
    _build_import_trace,
    _build_overrides,
    _get_corresponding_context,
    _get_corresponding_sheet_fpath,
    _get_index_after_last_nonempty,
)


def load_tabby(
    src: Path,
    *,
    single: bool = True,
    jsonld: bool = True,
    recursive: bool = True,
) -> Dict | List:
    """Load a tabby (TSV) record as structured (JSON(-LD)) data

    The record is identified by the table/sheet file path ``src``. This need
    not be the root 'dataset' sheet, but can be any component of the full
    record.

    The ``single`` flag determines whether the record is interpreted as a
    single entity (i.e., JSON object), or many entities (i.e., JSON array of
    (homogeneous) objects).  Depending on the ``single`` flag, either a
    ``dict`` or a ``list`` is returned.

    Other tabby tables/sheets are loaded when ``@tabby-single|many-`` import
    statements are discovered. The corresponding data structures then replace
    the import statement at its location. Setting the ``recursive`` flag to
    ``False`` disables table import, which will result in only the record
    available at the ``src`` path being loaded.

    With the ``jsonld`` flag, a declared or default JSON-LD context is
    loaded and inserted into the record.
    """
    return (_load_tabby_single if single else _load_tabby_many)(
        src=src,
        jsonld=jsonld,
        recursive=recursive,
        trace=[],
    )


def _load_tabby_single(
    *,
    src: Path,
    jsonld: bool,
    recursive: bool,
    trace: List,
) -> Dict:
    obj = {}
    with src.open(newline='') as tsvfile:
        reader = csv.reader(tsvfile, delimiter='\t')
        # row_id is useful for error reporting
        for row_id, row in enumerate(reader):
            # row is a list of field, with only as many items
            # as this particular row has columns
            if not len(row) or not row[0] or row[0].startswith('#'):
                # skip empty rows, rows with no key, or rows with
                # a comment key
                continue
            key = row[0]
            val = row[1:]
            # cut `val` short and remove trailing empty items
            val = val[:_get_index_after_last_nonempty(val)]
            if not val:
                # skip properties with no value(s)
                continue
            # look for @tabby-... imports in values, and act on them
            val = [
                _resolve_value(
                    v,
                    src,
                    jsonld=jsonld,
                    recursive=recursive,
                    trace=trace,
                )
                for v in val
            ]
            # we do not amend values for keys!
            # another row for an already existing key overwrites
            # we support "sequence" values via multi-column values
            # supporting two ways just adds unnecessary complexity
            obj[key] = val

    # apply any overrides
    obj.update(_build_overrides(src, obj))

    obj = _compact_obj(obj)

    if not jsonld:
        # early exit
        return obj

    # with jsonld==True, looks for a context
    ctx = _get_corresponding_context(src)
    if ctx:
        _assigned_context(obj, ctx)

    return obj


def _load_tabby_many(
    *,
    src: Path,
    jsonld: bool,
    recursive: bool,
    trace: List,
) -> List[Dict]:
    array = list()
    fieldnames = None

    # with jsonld==True, looks for a context
    ctx = _get_corresponding_context(src)

    with src.open(newline='') as tsvfile:
        # we cannot use DictReader -- we need to support identically named
        # columns
        reader = csv.reader(tsvfile, delimiter='\t')
        # row_id is useful for error reporting
        for row_id, row in enumerate(reader):
            # row is a list of field, with only as many items
            # as this particular row has columns
            if not len(row) \
                    or row[0].startswith('#') \
                    or all(v is None for v in row):
                # skip empty rows, rows with no key, or rows with
                # a comment key
                continue
            if fieldnames is None:
                # the first non-ignored row defines the property names/keys
                # cut `val` short and remove trailing empty items
                fieldnames = row[:_get_index_after_last_nonempty(row)]
                continue

            obj = _manyrow2obj(src, row, jsonld, fieldnames, recursive, trace)

            if ctx:
                _assigned_context(obj, ctx)

            # simplify single-item lists to a plain value
            array.append(_compact_obj(obj))
    return array


def _resolve_value(
    v: str,
    src_sheet_fpath: Path,
    jsonld: bool,
    recursive: bool,
    trace: List,
):
    if not recursive or not v.startswith('@tabby-'):
        return v

    if v.startswith('@tabby-single-'):
        loader = _load_tabby_single
        src = _get_corresponding_sheet_fpath(src_sheet_fpath, v[14:])
    elif v.startswith('@tabby-many-'):
        loader = _load_tabby_many
        src = _get_corresponding_sheet_fpath(src_sheet_fpath, v[12:])
    else:
        # strange, but not enough reason to fail
        return v

    trace = _build_import_trace(src, trace)

    return loader(
        src=src,
        jsonld=jsonld,
        recursive=recursive,
        trace=trace,
    )


def _manyrow2obj(
    src: Path,
    row: List,
    jsonld: bool,
    fieldnames: List,
    recursive: bool,
    trace: List,
) -> Dict:
    # if we get here, this is a value row, representing an individual
    # object
    obj = {}
    vals = [
        # look for @tabby-... imports in values, and act on them.
        # keep empty for now to maintain fieldname association
        _resolve_value(
            v,
            src,
            jsonld=jsonld,
            recursive=recursive,
            trace=trace,
        ) if v else v
        for v in row
    ]
    if len(vals) > len(fieldnames):
        # we have extra values, merge then into the column
        # corresponding to the last key
        last_key_idx = len(fieldnames) - 1
        lc_vals = vals[last_key_idx:]
        lc_vals = lc_vals[:_get_index_after_last_nonempty(lc_vals)]
        vals[last_key_idx] = lc_vals

    # merge values with keys, amending duplicate keys as necessary
    for i, k in enumerate(fieldnames):
        if i >= len(vals):
            # no more values defined in this row, skip this key
            continue
        v = vals[i]
        if not v:
            # no value, nothing to store or append
            continue
        # treat any key as a potential multi-value scenario
        k_vals = obj.get(k, [])
        k_vals.append(v)
        obj[k] = k_vals

    # TODO optimize and not read spec from file for each row
    # apply any overrides
    obj.update(_build_overrides(src, obj))
    return obj