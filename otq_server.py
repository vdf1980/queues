#!/usr/bin/env python3

from asynq import asynq


def testcallback(channel, method, properties, body):
    """
    test callback. This simply prints the body

    :param channel: the channel
    :param method: method parameters
    :param properties: properties
    :param body: the body
    :return: None
    """
    print("FROM TESTER : " + str(body))


def main():
    """
    this sets up a test asynq queue consumer
    :return:
    """
    rec = asynq.ASynQ(url='amqp://guest:guest@localhost:5672/?connection_attempts=3&heartbeat_interval=3600',
                      routing_key='asynq_otq',
                      sender=False,otq=True)

    try:
        rec.serve(testcallback)
    except KeyboardInterrupt:
        rec.stop()


if __name__ == "__main__":
    main()
