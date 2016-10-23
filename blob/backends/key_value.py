

class KVStorage:
    def __getitem__(self, key):
        pass

    def __setitem__(self, key, value):
        pass

    def __delitem__(self, key):
        pass

    def __contains__(self, key):
        pass

    def keys(self):
        pass

    def values(self):
        pass

    def count_links(self, value):
        pass


class DictKVStorage(KVStorage):
    def __init__(self):
        self.data = dict()

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value

    def __delitem__(self, key):
        del self.data[key]

    def __contains__(self, key):
        return key in self.data

    def __repr__(self):
        return repr(self.data)

    def keys(self):
        return self.data.keys()

    def values(self):
        return self.data.values()

    def count_links(self, value):
        return list(self.data.values()).count(value)

