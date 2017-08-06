from collections import deque
from twisted.internet.defer import DeferredLock


class Subscriber(object):
    def __init__(self, connection, metadata):
        self.connection = connection
        self.metadata = metadata
        self._sq = SubscriberQueue()

        self._dependents = []
        self._parents = []
        self._dependency_info = {}

        self._notify_lock = DeferredLock()

    def get_id(self):
        return self.metadata.get('client_tag')

    def get_filter_exp(self):
        return self.metadata.get('filter_exp')

    def send_message(self, message_hash):
        print (self.get_queue_name(), "Appending msg to my queueu ", self.get_id(), message_hash, self._sq.size()) 
        self._sq.append(message_hash)

    def get_message(self):
        return self._sq.pop()

    def is_independent(self):
        return len(self._parents) == 0

    def get_queue_name(self):
        return self.metadata.get('queue_name')

    def get_parents(self):
        return self._parents

    def get_dependents(self):
        return self._dependents

    def add_dependent(self, dependent):
        self._dependents.append(dependent)

    def add_parent(self, parent):
        print("Add parent ", parent.get_id(), "for subscriber ", self.get_id())
        self._parents.append(parent)

    def notify_dependents(self, message_hash):
        dependents = self.get_dependents()
        if not dependents:
            return
        for dependent in dependents:
            print("Notifying message ", message_hash, "to dependent ",
                    dependent.get_id())
            dependent.notify(message_hash)

    def notify(self, message_hash):
        self._notify_lock.run(self._notify, message_hash)

    def _notify(self, message_hash):
        """Notify this subscriber that it should process the given message.

        This method is called when a parent subscriber receives an ACK for a
        message it was processing.
        """
        if message_hash not in self._dependency_info:
            self._dependency_info[message_hash] = len(self._parents)

        self._dependency_info[message_hash] -= 1
        parents_left = self._dependency_info[message_hash]

        if parents_left <= 0:
            print("All parents have consumed the message ", message_hash,
                    "I will consume this now ", self.get_id())
            self.send_message(message_hash)


class SubscriberQueue(object):
    def __init__(self):
        self.queue = deque()

    def size(self):
        return len(self.queue)

    def is_empty(self):
        return self.size() == 0

    def append(self, message_hash):
        self.queue.append(message_hash)

    def pop(self):
        if self.is_empty():
            return "NO MESSAGE"
        message_hash = self.queue.pop()
        return message_hash


