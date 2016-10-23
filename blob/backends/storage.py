import os

from blob.backends.kv_storage import KVStorage
from blob.exceptions import StorageBackendError


class Storage:
    def get_data(self, address: int):
        pass

    def put_data(self, address: int, block_data: bytes):
        pass

    def get_free_address(self):
        pass


class FileStorage(Storage):
    def __init__(self, block_size: int, blob_size: int, path: str, kv_storage: KVStorage.__class__):
        if not os.path.exists(path):
            os.makedirs(path)

        if block_size <= 0 or not isinstance(block_size, int):
            raise StorageBackendError('incorrect block_size')

        if blob_size <= 0 or not isinstance(blob_size, int):
            raise StorageBackendError('incorrect blob_size')

        self.block_size = block_size
        self.blob_size = blob_size
        self.path = os.path.abspath(path)

        self.blobs = kv_storage()
        self.meta_len = kv_storage()

    def get_data(self, address: int):
        if address not in self.meta_len:
            raise StorageBackendError('block is empty')

        blob, block = self.get_physical_address(address)
        with open(self.get_file_name(blob), 'rb') as file:
            file.seek(block * self.block_size)
            raw_data = file.read(self.block_size)
            block_data = raw_data[self.block_size - self.meta_len[address]:]
            return block_data

    def put_data(self, address: int, block_data: bytes):
        blob, block = self.get_physical_address(address)
        if blob not in self.blobs:
            self.init_blob(blob)

        data_len = len(block_data)

        if data_len > self.block_size:
            raise StorageBackendError('block_data is greater than allowed block_size')

        if data_len > 0:
            dst_file_name = self.get_file_name(blob)
            src_file_name = dst_file_name + '_old'
            os.replace(dst_file_name, src_file_name)
            with open(src_file_name, 'rb') as src_file, open(dst_file_name, 'wb') as dst_file:
                current_block = 0
                while current_block < self.blob_size:
                    old_data = src_file.read(self.block_size)
                    if current_block == block:
                        dst_file.write(block_data)
                    else:
                        dst_file.write(old_data)
                    current_block += 1
            os.remove(src_file_name)

        self.meta_len[address] = data_len

    def get_physical_address(self, address):
        if address < 0:
            raise StorageBackendError
        blob = address // self.blob_size
        block = address % self.blob_size
        return blob, block

    def init_blob(self, blob):
        if blob not in self.blobs:
            file_name = self.get_file_name(blob)
            with open(file_name, 'wb') as file:
                file.write(bytes('0', encoding='utf-8') * self.block_size * self.blob_size)

    def get_file_name(self, blob):
        file_name = 'blob_' + str(blob).rjust(5, '0')  # 5 as an example
        return os.path.join(self.path, file_name)

    def get_free_address(self):
        address = 0
        while True:
            if address not in self.meta_len:
                return address
            address += 1
