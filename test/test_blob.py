from unittest import TestCase

import blob
from test.rand import *


class TestBlob(TestCase):
    def test_blob(self):
        block_size_list = [2 ** i for i in range(3, 14, 2)]
        blob_size_list = [2 ** i for i in range(6)]
        storage_path = './blob_test_storage'
        num_of_tests = 50
        for block_size in block_size_list:
            for blob_size in blob_size_list:
                try:
                    blob.init(block_size, blob_size, storage_path)
                    data = dict()
                    last_data = rand_bytes(block_size, unique=True)

                    for i in range(num_of_tests):
                        duplicate = rand_weithed_bool(0.5)
                        if duplicate:
                            test_data = last_data
                        else:
                            test_data = rand_bytes(block_size, unique=True)
                            last_data = test_data

                        address = rand_range(num_of_tests)
                        data[address] = test_data
                        with self.subTest(('read-write test: block_size={}, blob_size={},' +
                                           'duplicate={}, address={}, test_data={}').format(block_size,
                                                                                            blob_size,
                                                                                            duplicate,
                                                                                            address,
                                                                                            test_data)):
                            blob.put_block(address, test_data)
                            got_data = bytearray()
                            blob.get_block(address, got_data)
                            self.assertEqual(got_data, test_data)

                    with self.subTest('deduplication: block_size={}, blob_size={}'.format(block_size,
                                                                                                blob_size)):
                        expected_unique = len(set(data.values()))
                        got_unique = len(blob.storage.storage.blocks_metadata.keys())
                        self.assertEqual(got_unique, expected_unique)

                finally:
                    blob.delete()
