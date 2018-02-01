#!/usr/bin/env python3
import json

from asynq import asynq


def otqcallback(channel, method, properties, body):
    """
    test callback. This simply prints the body

    :param channel: the channel
    :param method: method parameters
    :param properties: properties
    :param body: the body
    :return: None
    """

    uuid = json.loads(body.decode('utf-8'))['uuid']

    print("UUID RECEIVED : " + uuid)

    rec = asynq.ASynQ(url='amqp://guest:guest@localhost:5672/%2F?connection_attempts=3&heartbeat_interval=3600',
                      routing_key=uuid,
                      sender=True,otq=True)

    rec.client({"BANG": "TJU"})



def main():
    """
    this sets up a test asynq queue consumer
    :return:
    """
    rec = asynq.ASynQ(url='amqp://guest:guest@localhost:5672/%2F?connection_attempts=3&heartbeat_interval=3600',
                      routing_key='norm_queue',
                      sender=False)

    rec.serve(otqcallback)

    print("HOV!!!")

if __name__ == "__main__":
    main()
