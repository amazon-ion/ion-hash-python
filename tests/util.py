import hashlib


def hash_function_provider(algorithm):
    def _f():
        if algorithm == "identity":
            return _IdentityHash()
        elif algorithm == "md5":
            return _MD5Hash()
    return _f


class _IdentityHash:
    def __init__(self):
        self._bytes = bytearray()

    def update(self, _bytes):
        self._bytes.extend(_bytes)

    def digest(self):
        _bytes = self._bytes
        self._bytes = bytearray()
        return _bytes


class _MD5Hash:
    def __init__(self):
        self._m = hashlib.md5()

    def update(self, _bytes):
        self._m.update(_bytes)

    def digest(self):
        digest = self._m.digest()
        self._m = hashlib.md5()
        return digest


def _hex_string(_bytes):
    if _bytes is None:
        return 'None'
    if isinstance(_bytes, bytes) or isinstance(_bytes, bytearray):
        return ''.join(' %02x' % x for x in _bytes)
    return _bytes

