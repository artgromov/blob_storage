from unittest import TestCase

from blob.backends.storage import Storage
from blob.backends.proxy import DedupeProxy
from blob.backends.key_value import DictKVStorage
from blob.exceptions import StorageBackendError
from blob.hashers import sha256

from test.rand import *


class StubStorage(Storage):
    def __init__(self):
        self.data = dict()
        self.last = None

    def get_data(self, address):
        return self.data[address]

    def put_data(self, address, block_data):
        self.data[address] = block_data
        self.last = block_data

    def del_data(self, address):
        del self.data[address]

    def get_free_address(self):
        address = 0
        while True:
            if address not in self.data:
                return address
            address += 1


class TestDedupeProxy(TestCase):
    def setUp(self):
        self.stub = StubStorage()
        self.storage = DedupeProxy(self.stub, DictKVStorage)

    def test_get_data(self):
        proxy_addr = rand_range(100)
        storage_addr = rand_range(100)
        test_data = rand_bytes(8)
        self.stub.put_data(storage_addr, test_data)
        self.storage.by_address[proxy_addr] = storage_addr

        with self.subTest('get test'.format(proxy_addr, storage_addr, test_data)):
            got_data = self.storage.get_data(proxy_addr)
            self.assertEqual(got_data, test_data)

        with self.subTest('get using wrong address'):
            with self.assertRaises(StorageBackendError):
                self.storage.get_data(200)

    def test_check_duplicate(self):
        test_data = rand_bytes(8)
        test_hash = sha256(test_data)
        storage_addr = rand_range(100)
        self.storage.by_hash[test_hash] = [storage_addr]
        self.stub.put_data(storage_addr, test_data)

        with self.subTest('known duplicate'):
            result = self.storage.check_duplicate(test_data, test_hash)
            self.assertEqual(result, storage_addr)

        with self.subTest('known unique'):
            unique_data = bytes('longer than 8 => content differs => unique', encoding='utf-8')
            unique_hash = sha256(unique_data)

            result = self.storage.check_duplicate(unique_data, unique_hash)
            self.assertIs(result, None)

        with self.subTest('emulate hash collision'):
            result = self.storage.check_duplicate(rand_bytes(8), test_hash)
            self.assertIs(result, None)

    def test_put_data(self):
        address1 = 0
        test_data1 = rand_bytes(8)
        test_hash1 = sha256(test_data1)
        with self.subTest('put unique data to new address'):
            self.storage.put_data(address1, test_data1)

            by_addr1 = self.storage.by_address[address1]
            by_hash1 = self.storage.by_hash[test_hash1]
            links_addr1 = self.storage.by_address.count_links(by_addr1)
            links_hash1 = len(by_hash1)

            self.assertTrue(self.stub.last == test_data1 and
                            by_addr1 in by_hash1 and
                            links_addr1 == 1 and
                            links_hash1 == 1)

        address2 = address1
        test_data2 = rand_bytes(8)
        test_hash2 = sha256(test_data2)
        with self.subTest('put unique data to old address with 1 link'):
            self.storage.put_data(address2, test_data2)

            by_addr2 = self.storage.by_address[address2]
            by_hash2 = self.storage.by_hash[test_hash2]
            links_addr2 = self.storage.by_address.count_links(by_addr2)
            links_hash2 = len(by_hash2)

            self.assertTrue(self.stub.last == test_data2 and
                            by_addr2 in by_hash2 and
                            links_addr2 == 1 and
                            links_hash2 == 1)

        address3 = 1
        test_data3 = test_data2
        test_hash3 = test_hash2
        with self.subTest('put duplicate data to new address'):
            self.storage.put_data(address3, test_data3)

            by_addr3 = self.storage.by_address[address3]
            by_hash3 = self.storage.by_hash[test_hash3]
            links_addr3 = self.storage.by_address.count_links(by_addr3)
            links_hash3 = len(by_hash3)

            self.assertTrue(self.stub.last == test_data3 and
                            by_addr3 == by_addr2 and
                            by_addr3 in by_hash3 and
                            links_addr3 == 2 and
                            links_hash3 == 1)

        address4 = address3
        test_data4 = rand_bytes(8)
        test_hash4 = sha256(test_data4)
        with self.subTest('put unique data to old address with 2 links'):
            self.storage.put_data(address4, test_data4)

            by_addr4 = self.storage.by_address[address4]
            by_addr2 = self.storage.by_address[address2]
            by_hash4 = self.storage.by_hash[test_hash4]
            by_hash2 = self.storage.by_hash[test_hash2]
            links_addr4 = self.storage.by_address.count_links(by_addr4)
            links_addr2 = self.storage.by_address.count_links(by_addr2)
            links_hash4 = len(by_hash4)
            links_hash2 = len(by_hash2)

            self.assertTrue(self.stub.last == test_data4 and
                            by_addr4 != by_addr2 and
                            by_addr4 in by_hash4 and
                            by_addr2 in by_hash2 and
                            links_addr4 == 1 and
                            links_addr2 == 1 and
                            links_hash4 == 1 and
                            links_hash2 == 1)

        address5 = 2
        test_data5 = rand_bytes(8)
        test_hash5 = sha256(test_data5)
        with self.subTest('hash collision emulation: put unique data to new address creating second hash link'):
            old_data = self.storage.by_hash[test_hash4]
            del self.storage.by_hash[test_hash4]
            self.storage.by_hash[test_hash5] = old_data

            self.storage.put_data(address5, test_data5)

            by_addr5 = self.storage.by_address[address5]
            by_addr4 = self.storage.by_address[address4]
            by_hash = self.storage.by_hash[test_hash5]
            links_addr5 = self.storage.by_address.count_links(by_addr5)
            links_addr4 = self.storage.by_address.count_links(by_addr4)
            links_hash = len(by_hash)

            self.assertTrue(self.stub.last == test_data5 and
                            by_addr4 != by_addr5 and
                            by_addr5 in by_hash and
                            by_addr4 in by_hash and
                            links_addr5 == 1 and
                            links_addr4 == 1 and
                            links_hash == 2)

        address6 = address5
        test_data6 = rand_bytes(8)
        test_hash6 = sha256(test_data6)
        with self.subTest('hash collision emulation: put unique data to new address removing second hash link'):
            self.storage.put_data(address6, test_data6)

            by_addr6 = self.storage.by_address[address6]
            by_addr4 = self.storage.by_address[address4]
            by_hash6 = self.storage.by_hash[test_hash6]
            by_hash4 = self.storage.by_hash[test_hash5]  # test_hash5 still points to test_data4 after emulation
            links_addr6 = self.storage.by_address.count_links(by_addr6)
            links_addr4 = self.storage.by_address.count_links(by_addr4)
            links_hash6 = len(by_hash6)
            links_hash4 = len(by_hash4)

            self.assertTrue(self.stub.last == test_data6 and
                            by_addr4 != by_addr6 and
                            by_hash4 != by_hash6 and
                            by_addr6 in by_hash6 and
                            links_addr6 == 1 and
                            links_addr4 == 1 and
                            links_hash6 == 1 and
                            links_hash4 == 1)

    def test_dedupe(self):
        num_of_tests = 1000
        data = dict()
        last_data = rand_bytes(8, unique=True)
        for i in range(num_of_tests):
            duplicate = rand_weithed_bool(0.8)
            if duplicate:
                test_data = last_data
            else:
                test_data = rand_bytes(8, unique=True)
                last_data = test_data

            address = rand_range(num_of_tests)
            data[address] = test_data
            with self.subTest('random put-get test: address={}, test_data={}, duplicate={}'.format(address, test_data, duplicate)):
                self.storage.put_data(address, test_data)
                got_data = self.storage.get_data(address)
                self.assertEqual(got_data, test_data)

        with self.subTest('check deduplication'):
            expected_data = set(data.values())
            expected_length = len(expected_data)
            got_data = set(self.stub.data.values())
            got_length = len(self.stub.data.values())
            self.assertTrue(got_data == expected_data and
                            got_length == expected_length)
