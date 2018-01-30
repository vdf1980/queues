#!/usr/bin/env python3
import threading

import pika
import json
import logging


# TODO: We must setup an instance (?) for each time a message is sent: otherwise ioloop blocks
import time


class ASynQ(object):
    """
    this class implements an asynchronous queue. This should allow total control of pikas weird defaults
    """

    def __init__(self, url, routing_key, log_file='/dev/null', exchange='yacamc_exchange', exchange_type='direct',
                 queue=None, acked=True, sender = False):
        """
        this will set up an asynchronous queue on rabbitmq at url, with routing key routing_key, or give access if it
        already exists

        :param url: url of the amqp server (remember username/password)
        :param routing_key: routing key for the queue we wish to use
        :param exchange: the exchange we wish to bind the queue to
        :param exchange_type: the exchange type we wish to use (usually direct suffices)
        :param queue: the name of the queue. If not set explicitly, this will become the same as the routing_key
        :param acked: if this is true, message acknowledgements will be enabled
        :param sender: if true, this object will expect to send messages
        """

        if queue is None:
            queue = routing_key
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.queue = queue
        self.routing_key = routing_key
        self._url = url
        self.acked = acked

        self.cb = None

        self._connection = None
        self._channel = None
        self._closing = False

        log_format = '%(levelname) -10s %(asctime)s %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s'
        handler = logging.FileHandler(log_file)
        logging.basicConfig(level=logging.DEBUG, format=log_format)
        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(handler)

        # used only for sending
        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0
        self._stopping = False
        self._done_sending = False

        self.run()
        #self._connection = self.connect()

    def start_loop(self):
        self._connection.ioloop.start()

    # The following functions set up the actual queue.

    def on_delivery_confirmation(self, method_frame):
        """
        this is called (if acked is set to true in the constructor) when a message is sent and the server responds

        :param method_frame: contains the info received from the server
        :return: None
        """
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()

        self.logger.info('received %s for %s', confirmation_type, method_frame.method.delivery_tag)
        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1

        self._deliveries.remove(method_frame.method.delivery_tag)
        self.logger.info('published %i messages, %i yet to confirm, %i acked and %i nacked', self._message_number,
                         len(self._deliveries), self._acked, self._nacked)
        self._done_sending = True

    def on_bindok(self, unused_frame):
        """
        This is called once the queue has been successfully bound to the exchange via the routing_key

        :param unused_frame: unused
        :return: None
        """
        self.logger.info('queue bound')
        if self.acked:
            # if we wish to care about the servers replies, this is were we set up things
            self.logger.info('issuing confirm.select RPC')
            self._channel.confirm_delivery(self.on_delivery_confirmation)

    def on_queue_declareok(self, method_frame):
        """
        This is called once the declaring of the queue is done. We call the binding function

        :param method_frame: unused
        :return: None
        """
        self.logger.info('binding %s and %s together with %s', self.exchange, self.queue, self.routing_key)
        self._channel.queue_bind(self.on_bindok, self.queue, self.exchange, self.routing_key)

    def setup_queue(self):
        """
        This is the entries to the setup of the queue. It sets a callback to be called, when the declaration is done

        :return: None
        """
        self.logger.info('declaring queue %s', self.queue)
        self._channel.queue_declare(self.on_queue_declareok, self.queue)

    # The following sets up the exchange, which is the part of the queueing system that determines how messages are
    # sent from producers to consumers

    def on_exchange_declareok(self, unused_frame):
        """
        This is called when the exchange is setup. It simply starts the setup of the queue

        :param unused_frame: unused
        :return: none
        """
        self.logger.info('exchange declared')
        self.setup_queue()

    def setup_exchange(self):
        """
        this sets up the exchange, and sets the callback, when pika is done with the setup

        :return: None
        """
        self.logger.info('declaring exchange %s', self.exchange)
        self._channel.exchange_declare(self.on_exchange_declareok, self.exchange, self.exchange_type)

    # The following functions pertains to the set up of the channel, which is the structure that enables structured
    # communication between the client and the rabbitmq server

    def on_channel_closed(self, channel, reply_code, reply_text):
        """
        this is called, if the channel is closed by the server for some reason. It might be that we are shutting down,
        or something unexpected is happening. If the latter is true, shut down the connection (it will take care of
        reconnecting)

        :param channel: not used
        :param reply_code: reply code which specifies the reason why the channel was closed
        :param reply_text: corresponding text
        :return: None
        """
        self.logger.warning('channel closed: %s: %s', reply_code, reply_text)
        self._channel = None
        if not self._stopping:
            # this wasn't supposed to happen
            self._connection.close()

    def on_channel_opened(self, channel):
        """
        This is called when the channel is done opening. We set the object reference, and set up a rescuer if the
        channel is closed - and then we proceed with setting up the exchange.

        :param channel: the channel that was opened
        :return: None
        """
        self.logger.info('channel opened')
        self._channel = channel

        self.logger.info('adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

        self.setup_exchange()

    def open_channel(self):
        """
        this sets up the channel

        :return: None
        """
        self.logger.info('creating channel')
        self._connection.channel(on_open_callback=self.on_channel_opened)

    # The following functions are all related to setting up the connection, the most basic part of a queueing system

    def reconnect(self):
        """
        this reconnects to a connection.
        :return: None
        """
        # only used for sending:
        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0

        self._connection.ioloop.stop()
        self._connection.connect()
        self._connection.ioloop.start()

    def on_connection_closed(self, connection, reply_code, reply_text):
        """
        rescue code. This is called if the connection is closed for some reason. We just need to remember that it might
        be attempting to shut down (so we shouldn't restart, obviously)

        :param connection: not used
        :param reply_code: reply code which specifies the reason why the connection was closed
        :param reply_text: corresponding text
        :return: None
        """

        self._channel = None  # there cannot be a channel, since the connection holding it was shut down
        if self._closing:
            # we are trying to stop. Just do so.
            self._connection.ioloop.stop()
        else:
            # this is unexpected. Restart the connection (after a timeout)
            self.logger.warning('The connection closed: %s:%s - retrying', reply_code, reply_text)
            self._connection.add_timeout(5, self.reconnect)

    def on_connection_open(self, unused_conncetion):
        """
        called when the connection is opened. We use it to set up a rescuer if the connection is later closed, and also
        to open the channel

        :param unused_conncetion: unused
        :return: None
        """
        self.logger.info('connection opened, adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)
        self.open_channel()

    def connect(self):
        """
        this starts the cascade which sets up the proper queue. It returns a reference to the connection

        :return: the connection
        """
        self.logger.info('connecting to %s', self._url)
        return pika.SelectConnection(pika.URLParameters(self._url), self.on_connection_open, stop_ioloop_on_close=False)

    def send(self,message):
        if self._stopping:
            return
        properties = pika.BasicProperties(app_id='sender',
                                          content_type='application/json',
                                          headers=message)

        self._channel.basic_publish(self.exchange, self.routing_key, json.dumps(message), properties)
        self._message_number += 1
        self._deliveries.append(self._message_number)
        self.logger.info('published message # %i', self._message_number)
        self._connection.ioloop.start()
        while not self._done_sending:
            time.sleep(0.01)
        self.stop()

    def stop(self):
        self.logger.info('stopping')
        self._stopping = True
        if self._channel:
            self._channel.close()
        self._closing = True
        self._connection.close()
        self._connection.ioloop.start()
        self.logger.info('stopped')

    def start_consuming(self, cb=None):
        if cb is None:
            self.logger.error('consumption requires a callback routine')
            return

        self.logger.info('consuming started')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self.on_message, self.queue)

    def add_on_cancel_callback(self):
        self.logger.info('adding cancel callback for consumer')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        self.logger.info('consumer cancelled %r', method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, channel, method, properties, body):
        if self.acked:
            self.acknowledge_message(method.delivery_tag)
        if self.cb is not None:
            # call the userspecified callback
            self.cb(channel, method, properties, body)
        else:
            self.logger.error("Received message, but no callback routine set")

    def acknowledge_message(self, delivery_tag):
        self.logger.info('acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        if self._channel:
            self.logger.info('sending basic cancel')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def on_cancelok(self, unused_frame):
        self.logger.info('ackknowledged cancelation of consumer')
        if self._channel:
            self._channel.close()

    def run(self):
        print("HEJ")
        self._connection = self.connect()
        #self._connection.ioloop.start()

#class Receiver:
#    def __init__(self,url,routing_key,cb):

def main():
    send = ASynQ(url='amqp://guest:guest@localhost:5672/%2F?connection_attempts=3&heartbeat_interval=3600',
                 routing_key='asynq_test',
                 sender=True)
    try:
        send.send({'bisko':'basko'})
    except KeyboardInterrupt:
        send.stop()


if __name__ == "__main__":
    main()
