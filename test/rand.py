import random


rand_range = random.randrange


def rand_bytes(length, unique=False, cache=[]):
    while True:
        data = bytes([random.randrange(0, 256) for i in range(length)])
        if unique:
            if data not in cache:
                cache.append(data)
                return data
        else:
            return data


def rand_weithed_bool(false_threshold=0.5):
    return random.random() >= false_threshold
