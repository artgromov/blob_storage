import hashlib


def sha256(data: bytes):
    return hashlib.sha256(data).hexdigest()
