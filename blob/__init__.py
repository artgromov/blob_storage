from blob.controller import Controller
from blob.backends.key_value import DictKVStorage
from blob.backends.storage import FileStorage
from blob.hashers import *
from blob.exceptions import *


controller = None
storage_path = './blob_storage'


def init(block_size: int, blob_size: int):
    global controller
    if controller is None:
        try:
            storage = FileStorage(block_size, blob_size, storage_path, DictKVStorage)
            controller = Controller(storage, DictKVStorage)
        except Exception:
            return 1
        else:
            return 0
    else:
        return 1  # blob already initialized


def get_block(block_id: int, block_data: bytearray):
    global controller
    if controller:
        try:
            data = controller[block_id]
        except Exception:
            return 1
        else:
            block_data.clear()
            block_data.extend(data)
            return 0
    return 1


def put_block(block_id: int, block_data: bytes):
    global controller
    if controller:
        try:
            controller[block_id] = block_data
        except Exception:
            return 1
        else:
            return 0
    else:
        return 0
