import os

from blob.backends.key_value import DictKVStorage
from blob.backends.storage import FileStorage
from blob.backends.proxy import DedupeProxy

storage = None
storage_path = None


def init(block_size: int, blob_size: int, path='./blob_storage'):
    global storage
    global storage_path
    if storage is None:
        try:
            storage = DedupeProxy(FileStorage(block_size, blob_size, path, DictKVStorage), DictKVStorage)
            storage_path = path
        except Exception:
            return 1
        else:
            return 0
    else:
        return 1  # storage already initialized


def get_block(block_id: int, block_data: bytearray):
    global storage
    if storage:
        try:
            data = storage.get_data(block_id)
        except Exception:
            return 1
        else:
            block_data.clear()
            block_data.extend(data)
            return 0
    return 1


def put_block(block_id: int, block_data: bytes):
    global storage
    if storage:
        try:
            storage.put_data(block_id, block_data)
        except Exception:
            return 1
        else:
            return 0
    else:
        return 1


def delete():
    global storage
    global storage_path
    if storage is not None:
        for path in os.listdir(storage_path):
            os.remove(os.path.join(storage_path, path))
        os.rmdir(storage_path)
        del storage
        storage = None
        return 0
    else:
        return 1
