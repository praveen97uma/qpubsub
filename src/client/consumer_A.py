import time

from consumer import start_consumer


def callback(message):
    time.sleep(1)


client_tag = 'A'
start_consumer(client_tag, callback, [])

