class BlobError(Exception):
    pass


class BackendError(BlobError):
    pass


class StorageBackendError(BackendError):
    pass
