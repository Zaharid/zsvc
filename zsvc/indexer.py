"""
indexer.py
"""
import os
import pathlib
import logging
import contextlib
import shutil

from dataclasses import dataclass, asdict

log = logging.getLogger(__name__)

class IndexerError(Exception): pass


@dataclass
class Record:
    path: pathlib.Path
    version: int
    commit: int

@dataclass
class Entry:
    version: int
    commit: int
    url: str

def parse_filename(path:pathlib.Path):
    parent = path.parent
    s = path.name
    version, commit, name = s.split('__', maxsplit=2)
    return Record(parent/name, int(version), int(commit))

def to_filename(record):
    name = record.path.name
    parent = record.path.parent
    return parent/f'{record.version}__{record.commit}__{name}'

class Indexer():
    def __init__(self, index, root='./'):
        self.root = pathlib.Path(root)
        self.index = index

    def get_next_version(self, name):
        if not name in self.index:
            return 1
        return self.index[name][-1].version + 1

    @contextlib.contextmanager
    def create_entry(self, name, commit ,version=None):
        key = str(name)
        if version is None:
            version = self.get_next_version(key)

        record = Record(name, version, commit)
        newname = to_filename(record)

        url = newname
        entry = Entry(version,commit,str(url))

        yield entry

        if key not in self.index:
            self.index[key] = []
        l = self.index[key]
        i = 0
        for i,ent in enumerate(reversed(l)):
            if ent.version < version:
                break
        else:
            i = len(l)
        l.insert(len(l)-i, entry)


    def rebuild_index_from_storage(self):
        self.index = {}
        path = self.root
        g = os.walk(path)
        for folder, dirs, files in g:
            root = pathlib.Path(folder).relative_to(path)
            for file in files:
                try:
                    record = parse_filename(pathlib.Path(file))
                except Exception as e:
                    log.error(f"Unrecongnized zsvc format: {root}/{file}")
                    continue
                name = root/record.path
                with self.create_entry(name, record.commit, version=record.version):
                    pass

    def serialize(self):
        return {name:[asdict(entry) for entry in entries] for name, entries in self.index.items()}

    @classmethod
    def from_dict(cls, d, *args, **kwargs):
        index = {name:[Entry(**entry) for entry in entries] for name, entries in d.items()}
        return cls(index, *args, **kwargs)

    def index_folder(self, path:pathlib.Path):
        commit = path/'COMMIT'
        if not commit.exists():
            raise IndexerError(f"Expecting a COMMIT file in {path}")
        c = self.get_next_version('COMMIT')
        g = os.walk(path)
        for folder, dirs, files in g:
            root = pathlib.Path(folder).relative_to(path)
            for name in files:
                file = root/name
                with self.create_entry(file, c) as entry:
                    dst = self.root/entry.url
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(path/file, dst)

    def get(self, key, version=None):
        entries = self.index[key]
        if version is None:
            return entries[-1]
        for entry in entries:
            if entry.version == version:
                return entry
        raise IndexerError(f"No such version for {key}")

