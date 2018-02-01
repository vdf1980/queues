#!/usr/bin/env python3
import time

from asynq import asynq


def main():
    """
    this function defines a test client, which sends a message every 4000 seconds
    :return: None
    """

    i = 0

    try:
        while True:
            rec = asynq.ASynQ(url='amqp://guest:guest@localhost:5672/%2F?connection_attempts=3&heartbeat_interval=3600',
                              routing_key='asynq_otq',
                              sender=True,otq=True)

            rec.client({"hej":"3"})
            #rec.client("HEJ")
            i += 1
            time.sleep(4)

    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
