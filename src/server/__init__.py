#!/usr/bin/env python
import json
import asyncio
import websockets
import threading
import uuid
import logging

from collections import defaultdict, deque

from autobahn.twisted.websocket import WebSocketServerProtocol, \
    WebSocketServerFactory

from buffer import MessageBufferHandlerFactory
from manager import SubscriptionManagerFactory
from store import MessageStore
from subscriber import Subscriber



class MyServerProtocol(WebSocketServerProtocol):

    def onConnect(self, request):
        print("Client connecting: {0}".format(request.peer))

    def onOpen(self):
        print("WebSocket connection open.")

    def onMessage(self, payload, isBinary):
        # echo back message verbatim

        message = json.loads(payload.decode('utf-8'))

        metadata = message.get('metadata')
        event = metadata.get('event')
        queue_name = metadata.get('queue_name')

        if event == "SUBSCRIBE":
            manager = SubscriptionManagerFactory.get_manager(queue_name)
            dependencies = metadata.get('dependencies', [])

            # also ensure all subscribers which this subscriber depends upon
            # are active

            subscriber = Subscriber(self, metadata)
            manager.add_subscription(subscriber)
            setattr(self, 'subscriber', subscriber)


            for subscriber_id in dependencies:
                parent = manager.get_subscriber(subscriber_id)
                if parent is None:
                    print("WARNING: ", "parent subscriber is not found")
                    continue
                print ("Parent ", parent)
                parent.add_dependent(subscriber)

                subscriber.add_parent(parent)


            manager.status()

        if event == "PUBLISH":
            payload = message.get('message')
            msg_hash = MessageStore.add(payload)
            handler = MessageBufferHandlerFactory.get_handler(
                    queue_name, self.factory.reactor)
            response = handler.handle_message(msg_hash)
            self.sendMessage(response.encode('utf-8'))

        if event == "REQUEST_MESSAGE":
            msg_hash = self.subscriber.get_message()
            message = MessageStore.get(msg_hash)

            payload = {
                'metadata': {
                    'message_hash': msg_hash,
                },
                'message_body': message
            }
            payload = json.dumps(payload)
            self.sendMessage(payload.encode('utf-8'))

        if event == "MESSAGE_PROCESSED_ACK":
            message_hash = metadata.get('message_hash')
            print("ACK received for message_hash", message_hash)
            self.subscriber.notify_dependents(message_hash)

    def onClose(self, wasClean, code, reason):
        if hasattr(self, 'subscriber'):
            SubscriptionManagerFactory.get_manager(self.subscriber.get_queue_name()).remove_subscription(self.subscriber)
        print("WebSocket connection closed: {0}".format(reason))


def start_server():
    import sys

    from twisted.python import log
    from twisted.internet import reactor

    log.startLogging(sys.stdout)

    factory = WebSocketServerFactory(u"ws://127.0.0.1:9000")
    factory.protocol = MyServerProtocol
    # factory.setProtocolOptions(maxConnections=2)

    # note to self: if using putChild, the child must be bytes...

    reactor.listenTCP(9000, factory)
    reactor.run()


if __name__ == '__main__':
    start_server()
