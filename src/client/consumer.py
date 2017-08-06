#!/usr/bin/env python
import json
import uuid

from autobahn.twisted.websocket import WebSocketClientProtocol, \
    WebSocketClientFactory


def get_protocol(queue_name, filter_exp, client_tag, callback,
        poll_interval=0.5,  dependencies=[]):

    client_processing_message = False

    class BaseQueueConsumer(WebSocketClientProtocol):

        def subscribe(self):
            metadata = {
                'event': 'SUBSCRIBE',
                'filter_exp': filter_exp,
                'queue_name': queue_name,
                'client_tag': client_tag,
                'dependencies': dependencies,
            }

            message = {
                'message': queue_name + "I am here",
                'metadata': metadata
            }

            msg = json.dumps(message)
            self.sendMessage(msg.encode('utf-8'))

        def consume(self):
            if not client_processing_message:
                metadata = {
                    'event': 'REQUEST_MESSAGE',
                    'filter_exp': filter_exp,
                    'queue_name': queue_name,
                    'client_tag': client_tag
                }

                message = {
                    'metadata': metadata
                }
                msg = json.dumps(message)
                self.sendMessage(msg.encode('utf-8'))

            self.factory.reactor.callLater(poll_interval, self.consume)

    class QueueConsumer(BaseQueueConsumer):

        def onConnect(self, response):
            print("Server connected: {0}".format(response.peer))

        def onOpen(self):
            print("WebSocket connection open.")
            print("Sending subscribe request")

            self.subscribe()
            self.consume()

        def onMessage(self, payload, isBinary):
            payload = payload.decode('utf-8')
            payload = json.loads(payload)

            if payload['metadata']['message_hash'] == 'NO MESSAGE':
                return

            print("Received payload ", payload)
            client_processing_message = True
            callback(payload['message_body'])
            self.ack_message(payload)


        def ack_message(self, payload):
            metadata = {
                'event': "MESSAGE_PROCESSED_ACK",
                'queue_name': queue_name,
                'client_tag': client_tag,
                'message_hash': payload['metadata'].get('message_hash') 
            }

            message = {
                'metadata': metadata
            }

            msg = json.dumps(message)
            print("Notifying server that message is processed ", payload)
            self.sendMessage(msg.encode('utf-8'))


        def onClose(self, wasClean, code, reason):
            print("WebSocket connection closed: {0}".format(reason))
    return QueueConsumer


def callback(message):
    import time
    time.sleep(1)




def start_consumer(client_tag, callback, dependencies):
    import sys

    from twisted.python import log
    from twisted.internet import reactor

    log.startLogging(sys.stdout)

    factory = WebSocketClientFactory(u"ws://127.0.0.1:9000")

    queue_name = "tweets"
    filter_exp = "test-"
    poll_interval=0.5

    factory.protocol = get_protocol(queue_name, filter_exp, client_tag,
            callback=callback, poll_interval=poll_interval,
            dependencies=dependencies)

    reactor.connectTCP("127.0.0.1", 9000, factory)
    reactor.run()
