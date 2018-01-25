#!/usr/bin/env python3

"""
In order to learn how to do proper asynchronos pika queues
This is the consumer (receiver)
"""

import pika
import logging

log_format = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s')
logger = logging.getLogger(__name__)
print ("HEJ")

class Receiver(object):
    exchange = ''
    exchange_type = 'topic'
    queue = 'async_test'
    routing_key = 'async_queue'

    def __init__(self, url='localhost'):
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = url

    def connect(self):
        logger.info('connecting to %s', self._url)
        return pika.SelectConnection(pika.URLParameters(self._url),
                                     self.on_connection_open,
                                     stop_ioloop_on_close=False)

    def on_connection_open(self, unused_connection):
        logger.info("connection opened")
        self.add_on_connection_close_callback();
        self.open_channel()

    def add_on_connection_close_callback(self):
        logger.info('adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self,connection,reply_code,reply_text):
        self._channel = None
        if self._closing:
            self._connection.io_loop.stop()
        else:
            # this is unexpected
            logger.warning('connection closed. Reopening %s, %s',reply_code,reply_text)
            self._connection.add_timeout(5,self.reconnect)

    def reconnect(self):
        self._connection.ioloop.stop()
        if not self._closing:
            # unexpected
            self._connection = self.connect()

            self._connection.ioloop.start()

    def open_channel(self):
        logger.info('creating new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_oepn(self,channel):
        logger.info('channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.exchange)

    def add_on_channel_close_callback(self):
        logger.info('adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        logger.warning('channel %i closed: (%s) %s',channel,reply_code,reply_text)
        self._connection.close()

    def setup_exchange(self,exchange_name):
        logger.info('declare exchange %s',exchange_name)
        self._channel.exchange_declare(self.on_exchange_declare_ok,
                                       exchange_name,
                                       self.exchange_type)

    def on_exchange_declare_ok(self, unused_frame):
        logger.info('exchange declared!')
        self.setup_queue(self.queue)

    def setup_queue(self, queue_name):
        logger.info('declaring queue %s',queue_name)
        self._channel.queue_declare(self.on_queue_declareok, queue_name)

    def on_queue_declareok(self, method_frame):
        logger.info('binding %s to %s with %s', self.exchange, self.queue, self.routing_key)
        self._channel.queue_bind(self.on_bindok, self.queue, self.exchange, self.routing_key)

    def on_bindok(self, unused_frame):
        logger.info('queue bound')
        self.start_consuming()

    def start_consuming(self):
        logger.info('consuming started')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self.on_message, self.queue)

    def add_on_cancel_callback(self):
        logger.info('adding cancel callback for consumer')
        self._channel.add_on_cancel.callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        logger.info('consumer cancelled %r', method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, unused_channel, basic_deliver, properties, body):
        logger.info('message received: %s from %s: %s',basic_deliver.delivery_tag, properties.app_id, body)
        self.acknowledge_message(basic_deliver.delivery_tag)

    def acknowledge_message(self,delivery_tag):
        logger.info('acknowledging message %s',delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        if self._channel:
            logger.info('sending basic cancel')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def on_cancelok(self, unused_frame):
        logger.info('ackknowledged cancelation of consumer')
        self.close_channel()

    def close_channel(self):
        logger.info('closing channel')
        self._channel.close()

    def run(self):
        self._connection= self.connect()
        self._connection.ioloop.start()

    def stop(self):
        logger.info('stopping')
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()
        logger.info('stopped')

    def close_connection(self):
        logger.info('closing connection')
        self._connection.close()

def main():
    logging.basicConfig(level=logging.INFO, format= log_format)
    example = Receiver('amqp://guest:guest@localhost:5672/%2F')
    try:
        example.run()
    except KeyboardInterrupt:
        example.stop()

if __name__=="__main__":
    main()