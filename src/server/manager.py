from collections import defaultdict

from store import MessageStore


class SubscriptionManagerFactory(object):
    _cache = {}

    @classmethod
    def get_manager(cls, queue_name):
        if not queue_name in cls._cache:
            cls._cache[queue_name] = QueueSubscriptionManager(queue_name)
        return cls._cache[queue_name]


class QueueSubscriptionManager(object):
    def __init__(self, queue_name):
        self.queue_name = queue_name
        self._subscribers = defaultdict(dict)

        # we maintain this to quickly check whether a given subscriber is
        # active at any point or not
        self._active_subscribers = dict()


    def add_subscription(self, subscriber):
        filter_exp = subscriber.get_filter_exp()
        subscriber_id = subscriber.get_id()

        print("Subscribing ", subscriber.get_id(), "to ", filter_exp)

        self._subscribers[filter_exp][subscriber_id] = subscriber
        self._active_subscribers[subscriber_id] = subscriber


    def remove_subscription(self, subscriber):
        print("Removing subscriber", subscriber.get_id(), "from pattern", subscriber.get_filter_exp())

        self._subscribers[subscriber.get_filter_exp()].pop(subscriber.get_id())
        self._active_subscribers.pop(subscriber.get_id())

    def has_subscriber(self, subscriber_id):
        return subscriber_id in self._active_subscribers

    def get_subscriber(self, subscriber_id):
        return self._active_subscribers.get(subscriber_id)

    def broadcast_message(self, msg_hash):
        message = MessageStore.get(msg_hash)

        for pattern, subscribers in self._subscribers.items():
            if pattern in message:
                for sub_id, subscriber in subscribers.items():
                    if subscriber.is_independent():
                        subscriber.send_message(msg_hash)


    def status(self):
        print(self._subscribers)


