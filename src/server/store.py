import uuid


class MessageStore(object):
    _store = {}

    @classmethod
    def add(self, message):
        _hash = uuid.uuid5(uuid.NAMESPACE_OID, message).hex
        self._store[_hash] = message
        return _hash

    @classmethod
    def get(self, msg_hash):
        return self._store.get(msg_hash)


