#!/usr/bin/env python3
from asynq import asynq
import uuid


def myprint(ch, method, properties, body):
    print(body.decode('utf-8'))


def main():
    """
    full test of the asynchronous queue (for my purposes)
    :return: None
    """

    rec = asynq.ASynQ(url='amqp://guest:guest@localhost:5672/%2F?connection_attempts=3&heartbeat_interval=3600',
                      routing_key='norm_queue',
                      sender=True)
    my_uuid = str(uuid.uuid4())

    rec.client({"hej": "3", 'uuid': my_uuid})

    rec = asynq.ASynQ(url='amqp://guest:guest@localhost:5672/%2F?connection_attempts=3&heartbeat_interval=3600',
                      routing_key=my_uuid,
                      sender=False, otq=True)

    rec.serve(cb=myprint)

    print("DONE!")


if __name__ == "__main__":
    main()
