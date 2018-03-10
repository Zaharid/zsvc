"""
test_utils.py
"""
import curio

from zsvc.utils import atomic_write


def test_atomic_write(tmpdir):
    async def t():
        await atomic_write(tmpdir/'xxx', b'hello')
        with open(tmpdir/'xxx') as f:
            assert f.read() == 'hello'
    curio.run(t())
