#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_indexer.py
"""
import string
import os.path as osp
import pathlib
import tempfile
import shutil

import pytest
from hypothesis import given
from hypothesis.strategies import integers, text, lists

from zsvc.indexer import parse_filename, to_filename, Indexer, Record, IndexerError

@pytest.fixture
def tmp(tmpdir):
    return pathlib.Path(tmpdir)

filenames = text(string.ascii_letters + string.digits+ '._', min_size=1)
paths = lists(filenames, min_size=1).map(lambda x: pathlib.Path(osp.join(*x)))

@given(paths, integers(), integers())
def test_parsename_inversion(name, version, commit):
    record = Record(name,version,commit)
    assert parse_filename(to_filename(record)) == record

@given(lists(integers(), min_size=1))
def test_versions_are_sorted(versions):
    i = Indexer({})
    name = 'xxx'
    for v in versions:
        with i.create_entry(pathlib.Path(name), commit=1, version=v):
            pass
    assert sorted(entry.version for entry in i.index[name]) == sorted(versions)
    assert i.get_next_version(name) == max(versions) + 1


#Can't use the tmpdir fixture because it does not clean itself when we
#select different paths
@given(paths=lists(paths))
def test_paths_indexing(paths):
    tmpdir = pathlib.Path(tempfile.mkdtemp())
    good_paths = []
    for p in paths:
        path = pathlib.Path(tmpdir/p).resolve()
        if path.exists():
            continue
        try:
            path.relative_to(tmpdir)
        except ValueError:
            continue
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except:
            continue
        try:
            path.touch(exist_ok=True)
        except:
            continue
        good_paths.append(str(path.relative_to(tmpdir)))
    with open(tmpdir/'COMMIT', 'w') as f:
        f.write("A Commit message")
    storage_loc = pathlib.Path(tempfile.mkdtemp())
    i = Indexer({}, storage_loc)
    i.index_folder(tmpdir)
    for gp in good_paths:
        assert gp in i.index
        assert (storage_loc/i.index[gp][-1].url).exists()

    with open(tmpdir/'COMMIT', 'w') as f:
        f.write("A new Commit message")

    i.index_folder(tmpdir)
    assert len(i.index['COMMIT']) == 2

    assert Indexer.from_dict(i.serialize()).index == i.index

    i2 = Indexer({}, storage_loc)
    i2.rebuild_index_from_storage()
    assert i2.index == i.index

    shutil.rmtree(tmpdir)
    shutil.rmtree(storage_loc)

def test_get(tmp):
    storage = tmp/'storage'
    inp = tmp/'input'
    storage.mkdir()
    inp.mkdir()
    i = Indexer({}, storage)
    for c in range(3):
        with open(inp/'COMMIT', 'w') as f:
            f.write(f"Commit {c+1}")
        i.index_folder(inp)
    assert i.get('COMMIT').version == 3
    assert i.get('COMMIT', version=1).version==1
    with pytest.raises(IndexerError):
        i.get('COMMIT', version=4)
    with open(storage/i.get('COMMIT', 2).url) as f:
        assert f.read() == 'Commit 2'

def test_commit_required(tmp):
    i = Indexer({})
    with pytest.raises(IndexerError):
        i.index_folder(tmp)

def test_handling_garbage(tmp):
    (tmp/'xxx').touch()
    (tmp/'1__1__COMMIT').touch()
    (tmp/'2__2__COMMIT').touch()
    i = Indexer({}, tmp)
    i.rebuild_index_from_storage()
    assert len(i.index['COMMIT']) == 2
    assert len(i.index) == 1
