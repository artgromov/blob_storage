from blob.backends.storage import Storage
from blob.backends.key_value import KVStorage
from blob.hashers import *
from blob.exceptions import StorageBackendError


class DedupeProxy(Storage):
    def __init__(self, storage: Storage, kv_storage: KVStorage.__class__, hasher=sha256):
        self.storage = storage
        self.hasher = hasher

        self.by_address = kv_storage()
        self.by_hash = kv_storage()

    def get_data(self, address: int):
        try:
            storage_address = self.by_address[address]
        except KeyError:
            raise StorageBackendError('no block found with such address')

        return self.storage.get_data(storage_address)

    def put_data(self, address: int, block_data: bytes):
        block_hash = self.hasher(block_data)
        duplicate_address = self.check_duplicate(block_data, block_hash)
        if duplicate_address is not None:
            if address in self.by_address:
                storage_address = self.by_address[address]
                if self.by_address.count_links(storage_address) == 1 and storage_address != duplicate_address:
                    self.storage.del_data(storage_address)
            self.by_address[address] = duplicate_address

        else:
            try:
                storage_address = self.by_address[address]
            except KeyError:
                storage_address = self.storage.get_free_address()
            else:
                if self.by_address.count_links(storage_address) > 1:
                    storage_address = self.storage.get_free_address()
                else:
                    # removing old hash link to storage address
                    old_hash = None
                    for key in self.by_hash.keys():
                        if storage_address in self.by_hash[key]:
                            old_hash = key
                            break
                    if len(self.by_hash[old_hash]) > 1:
                        self.by_hash[old_hash].remove(storage_address)
                    else:
                        del self.by_hash[old_hash]

            self.by_address[address] = storage_address
            if block_hash in self.by_hash:
                self.by_hash[block_hash].append(storage_address)
            else:
                self.by_hash[block_hash] = [storage_address]

            self.storage.put_data(storage_address, block_data)

    def check_duplicate(self, block_data: bytes, block_hash: str):
        if block_hash in self.by_hash:
            # then compare content to avoid hash collisions
            storage_address_list = self.by_hash[block_hash]
            for addr in storage_address_list:
                data = self.storage.get_data(addr)
                if data == block_data:
                    return addr
        return None
