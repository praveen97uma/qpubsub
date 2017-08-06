import time

from consumer import start_consumer


def callback(message):
    time.sleep(3)

client_tag = 'E'
start_consumer(client_tag, callback, ['B', 'D'])

