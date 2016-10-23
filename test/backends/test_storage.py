from unittest import TestCase
import os

from blob.backends.key_value import DictKVStorage
from blob.backends.storage import FileStorage
from blob.exceptions import StorageBackendError
from test.rand import rand_bytes, rand_range


class TestFileStorage(TestCase):
    def setUp(self):
        self.block_size = 16
        self.blob_size = 8
        self.path = './blob_test_storage'
        self.storage = FileStorage(self.block_size, self.blob_size, self.path, DictKVStorage)
        if not os.path.exists(self.path):
            os.mkdir(self.path)

    def tearDown(self):
        for path in os.listdir(self.path):
            os.remove(os.path.join(self.path, path))
        os.rmdir(self.path)

    def test_init_blob(self):
        test_blob = 1
        test_file = self.storage.get_file_name(test_blob)
        self.storage.init_blob(test_blob)
        with self.subTest('test file exists'):
            self.assertTrue(os.path.exists(test_file))

        with self.subTest('test file filled with zeros'):
            with open(test_file, 'rb') as file:
                test_data = file.read()
                expected = bytes('0', encoding='utf-8') * self.block_size * self.blob_size
                self.assertEqual(test_data, expected)

    def test_get_data(self):
        test_blob = 0
        test_block = 3
        test_address = test_blob * self.blob_size + test_block
        test_data = bytes('abcd', encoding='utf-8')
        test_data_len = len(test_data)
        test_file = os.path.join(self.path, 'blob_' + str(test_blob).rjust(5, '0'))
        raw_blob = test_data.zfill(self.block_size * (test_block + 1)) + bytes(0).zfill(self.block_size * (self.blob_size - test_block - 1))

        with open(test_file, 'wb') as file:
            file.write(raw_blob)

        self.storage.blocks_metadata[test_address] = test_data_len
        got_data = self.storage.get_data(test_address)

        self.assertEqual(got_data, test_data)

    def test_put_data(self):
        test_address = 3
        test_blob, test_block = self.storage.get_physical_address(test_address)
        test_file = self.storage.get_file_name(test_blob)
        test_data = bytes('abcd', encoding='utf-8')
        expected = test_data.zfill(self.block_size)

        with self.subTest('string bigger than block_size'):
            with self.assertRaises(StorageBackendError):
                self.storage.put_data(test_address, test_data * 5)

        with self.subTest('normal string'):
            self.storage.put_data(test_address, expected)
            with open(test_file, 'rb') as file:
                file.seek(test_block * self.block_size)
                got_data = file.read(self.block_size)
            self.assertEqual(got_data, expected)

    def test_del_data(self):
        address = 10
        length = 10
        self.storage.blocks_metadata[address] = length
        with self.subTest('check deletion'):
            self.storage.del_data(address)
            self.assertTrue(address not in self.storage.blocks_metadata)

        with self.subTest('check raise with non existend address'):
            with self.assertRaises(StorageBackendError):
                self.storage.del_data(address)

    def test_put_get_data(self):
        num_of_tests = 20
        addr_block = [(rand_range(128), rand_bytes(self.block_size)) for i in range(num_of_tests)]
        for address, block in addr_block:
            with self.subTest('random test: address={}, block={}'.format(address, block)):
                self.storage.put_data(address, block)
                got_data = self.storage.get_data(address)
                self.assertEqual(got_data, block)



