import time

from consumer import start_consumer


def callback(message):
    time.sleep(3)

client_tag = 'C'
start_consumer(client_tag, callback, ['A', 'B'])

