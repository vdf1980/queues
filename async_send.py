#!/usr/bin/env python3

import pika
import json
import logging

log_format = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s')
logger = logging.getLogger(__name__)

class sender(object):
    exchange = ''
    exchange_type = 'topic'
    queue = 'async_test'
    routing_key = 'async_queue'
    interval = 1

    def __init__(self,url):
        self._connection = None
        self._channel = None
        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0
        self._stopping = False
        self._url = url
        self._closing = False

    def connect(self):
        logger.info('connecting to %s', self._url)
        return pika.SelectConnection(pika.URLParameters(self._url),
                                     self.on_connection_open,
                                     stop_ioloop_on_close = False)

    def on_connection_open(self, unused_conncetion):
        logger.info('connection opened')
        self.add_on_connection_close_callback()
        self.open_channel()

    def add_on_connection_close_callback(self):
        logger.info('adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            logger.warning('The connection closed: %s:%s - retrying',reply_code, reply_text)
            self._connection.add_timeout(5, self.reconnect)

    def reconnect(self):
        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0

        self._connection.ioloop.stop()

        self._connection.connect()

        self._connection.ioloop.start()

    def open_channel(self):
        logger.info('creating channel')
        self._connection(on_open_callback = self.on_channel_open)

    def on_channel_open(self, channel):
        logger.info('channel opened')

        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.exchange)

    def add_on_channel_close_callback(self):
        logger.info('adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        logger.warning('channel closed: %s: %s', reply_code,reply_text)
        if not self._closing:
            self._connection.close()

    def setup_exchange(self, exchange):
        logger.info('declaring exchange %s', exchange)
        self._channel.exchange_declare(self.on_declare_exchangeok,
                                        exchange,
                                        self.exchange_type)

    def on_declare_exchangeok(self, unused_frame):
        logger.info('exchange declared')
        self.setup_queue(self.queue)

    def setup_queue(self,queue):
        logger.info('declaring queue %s',queue)
        self._channel.queue_declare(self.on_queue_declareok, queue)

    def on_queue_declareok(self, method_frame):
        logger.info('binding %s and %s together with %s',self.exchange,self.queue,self.routing_key)
        self._channel.queue_bind(self.on_bindok, self.queue, self.exchange, self.routing_key)

    def on_bindok(self, unused_frame):
        logger.info('queue bound, publishing')
        self.start_publishing()

    def start_publishing(self):
        logger.info('issuing consumer rpcs')
        self.enable_delivery_confirmations()
        self.schedule_next_message()

    def enable_delivery_confirmations(self):
        logger.info('issuing confirm.select RPC')
        self._channel.confirm_delivery(self.on_delivery_confirmation)

    def on_delivery_confirmation(self, method_frame):
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()

        logger.info('received %s for %s', confirmation_type, method_frame.method.delivery_tag)
        if confirmation_type == 'ack':
            self._acked +=1
        elif confirmation_type == 'nack':
            self._nacked += 1

        self._deliveries.remove(method_frame.method.delivery_tag)
        logger.info('published %i messages, %i yet to confirm, %i acked and %i nacked',
                    self._message_number, len(self._deliveries), self._acked, self._nacked)

    def schedule_next_message(self):
        if self._stopping:
            return
        logger.info('scheduling message in %0.1f seconds', self.interval)
        self._connection.add_timeout(self.interval,self.publish_message)

    def publish_message(self):

        if self._stopping:
            return
        message = {'mango':'bango', 'fnylly':'hylly'}
        properties = pika.BasicProperties(app_id='sender',
                                          content_type='application/json',
                                          headers = message)

        self._channel.basic_publish(self.exchange, self.routing_key, json.dumps(message), properties)
        self._message_number +=1
        self._deliveries.append(self._message_number)
        logger.info('published message # %i',self._message_number)
        self.schedule_next_message()

    def close_channel(self):
        logger.info('closing the channel')
        if self._channel:
            self._channel.close()

    def run(self):
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        logger.info('stopping')
        self._stopping = True
        self.close_channel()
        self.close_connection()
        self._connection.ioloop.start()
        logger.info('stopped')

    def close_connection(self):
        logger.info('closing connection')
        self._closing = True
        self._connection.close()

def main():
    logging.basicConfig(level=logging.DEBUG, format = log_format)

    send = sender('amqp://guest:guest@localhost:5672/%2F?connection_attempts=3&heartbeat_interval=3600')
    try:
        send.run()
    except KeyboardInterrupt:
        send.stop()

if __name__=="__main__":
    main()





