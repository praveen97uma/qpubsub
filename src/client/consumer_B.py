import time

from consumer import start_consumer


def callback(message):
    time.sleep(4)

client_tag = 'B'
start_consumer(client_tag, callback, [])
