"""
driver.py
"""
import argparse
import pathlib
import tempfile
import logging
import sys
import json

import curio
import asyncwatch

from zsvc.utils import atomic_write
from zsvc.indexer import Indexer

log = logging.getLogger(__name__)

def check_is_writable(path):
    try:
        with tempfile.NamedTemporaryFile() as f:
            f.write(b'0')
    except Exception as e:
        log.error(f"Cannot write to location {path}")
        sys.exit(1)

def init_index(path):
    ind =  (path/'index.json')
    if ind.exists():
        try:
            with open(ind) as f:
                d = json.load(f)
            return Indexer.from_dict(d, path)
        except (IOError, json.JSONDecodeError) as e:
            log.error(e)
    i = Indexer({}, path)
    i.rebuild_index_from_storage()
    return i

async def write_index(index):
    await atomic_write(index.root/'index.json', json.dumps(index.serialize()).encode())


async def index_after_commit(p, index, waiting):
    try:
        async for e in asyncwatch.watch(p, asyncwatch.EVENTS.CLOSE_WRITE):
            if e.name == 'COMMIT':
                log.info(f"Indexing {p}")
                index.index_folder(p)
                await write_index(index)
                waiting.discard(p)
                return
    except asyncwatch.NoMoreWatches:
        log.warn(f"Discarded folder {p}")
        return

async def watch_data_dir(data_dir, index):
    tg = curio.TaskGroup()
    waiting = set()
    async for ev in asyncwatch.watch(data_dir, asyncwatch.EVENTS.CREATE|asyncwatch.EVENTS.CLOSE):
        if not ev.name:
            continue
        p = data_dir/ev.name
        if p.is_dir() and p not in waiting:
            log.info(f"Waiting for {p}")
            await tg.spawn(index_after_commit, p, index, waiting)
            waiting.add(p)


async def main(storage_dir, data_dir):
    index = init_index(storage_dir)
    tg = curio.TaskGroup()
    await tg.spawn(watch_data_dir, data_dir, index)
    await tg.join()


def launch():
    parser = argparse.ArgumentParser()
    parser.add_argument('storage_dir')
    parser.add_argument('tempdata_dir')
    args = parser.parse_args()
    sd = pathlib.Path(args.storage_dir)
    dd = pathlib.Path(args.tempdata_dir)

    logging.basicConfig(level=logging.INFO)

    check_is_writable(sd)
    check_is_writable(dd)

    curio.run(main(sd, dd))


if __name__ == '__main__':
    launch()
