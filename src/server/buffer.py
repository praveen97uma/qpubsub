from collections import deque

from manager import SubscriptionManagerFactory


class MessageBufferHandlerFactory(object):
    _cache = {}

    @classmethod
    def get_handler(cls, queue_name, reactor):
        if queue_name not in cls._cache:
            handler = MessageBufferHandler(queue_name)
            cls._cache[queue_name] = handler
            handler.push_to_subscribers(reactor)
        return cls._cache[queue_name]

class MessageBufferHandler(object):
    def __init__(self, queue_name, buffer_size=10):
        self.queue_name = queue_name
        self.buffer_size = buffer_size
        self._buffer = deque(maxlen=self.buffer_size)
        self._subscription_manager = SubscriptionManagerFactory.get_manager(queue_name) 

    def push_to_subscribers(self, reactor):
        msg = self.get_message()
        if msg is not None:
            self._subscription_manager.broadcast_message(msg)

        reactor.callLater(0.01, self.push_to_subscribers, reactor)

    def is_buffer_full(self):
        return self.size() == self.buffer_size

    def is_empty(self):
        return self.size() == 0

    def push(self, message):
        self._buffer.append(message)

    def size(self):
        return len(self._buffer)

    def status(self):
        print (self.queue_name, "Messages in message buffer: ", self.size())

    def handle_message(self, message):
        if self.is_buffer_full():
            return "Failed"
        self.push(message)
        return "Success"

    def get_message(self):
        if self.is_empty():
            return None
        return self._buffer.pop()
