import time

from consumer import start_consumer


def callback(message):
    time.sleep(3)

client_tag = 'D'
start_consumer(client_tag, callback, ['C'])

