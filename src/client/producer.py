#!/usr/bin/env python
import json
import uuid

from autobahn.twisted.websocket import WebSocketClientProtocol, \
    WebSocketClientFactory


class MyClientProtocol(WebSocketClientProtocol):

    def onConnect(self, response):
        print("Server connected: {0}".format(response.peer))

    def onOpen(self):
        print("WebSocket connection open.")
        print("Sending subscribe request")


        def publish():
            metadata = {
                'event': 'PUBLISH',
                'queue_name': 'tweets',
                'client_tag': 'Producer-' + uuid.uuid4().hex[:7]
            }

            message = {
                'message': "I am here test-" + uuid.uuid4().hex[:7],
                'metadata': metadata
            }

            msg = json.dumps(message)
            self.sendMessage(msg.encode('utf-8'))
            self.factory.reactor.callLater(1, publish)
        publish()

    def onMessage(self, payload, isBinary):
        if isBinary:
            print("Binary message received: {0} bytes".format(len(payload)))
        else:
            print("Text message received: {0}".format(payload.decode('utf8')))

    def onClose(self, wasClean, code, reason):
        print("WebSocket connection closed: {0}".format(reason))


if __name__ == '__main__':

    import sys

    from twisted.python import log
    from twisted.internet import reactor

    log.startLogging(sys.stdout)

    factory = WebSocketClientFactory(u"ws://127.0.0.1:9000")
    factory.protocol = MyClientProtocol

    reactor.connectTCP("127.0.0.1", 9000, factory)
    reactor.run()
