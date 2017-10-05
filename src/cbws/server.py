import pika
import json
import logging

from collections import defaultdict
from sockjs.tornado.conn import SockJSConnection
from sockjs.tornado import SockJSRouter
from tornado.ioloop import IOLoop
from tornado.web import Application

from uuid import uuid4

from cloudbrain.core.auth import CloudbrainAuth

_LOGGER = logging.getLogger()
_LOGGER.setLevel(logging.INFO)

recursivedict = lambda: defaultdict(recursivedict)


def _rt_stream_connection_factory(rabbitmq_address, rabbit_auth_url):
    """
    RtStreamConnection class factory.

    :param rabbitmq_address: RabbitMQ server address.
    :param rabbit_auth_url: RabbitMQ authentication server address.
    :return: RtStreamConnection
    """

    class RtStreamConnection(SockJSConnection):
        """RtStreamConnection connection implementation"""

        clients = set()

        def __init__(self, session):

            super(self.__class__, self).__init__(session)

            self.subscribers = recursivedict()
            self.total_records = recursivedict()

        def send_probe_factory(self, exchange_name, routing_key):

            def send_probe(body):
                logging.debug("GOT: " + body)
                buffer_content = json.loads(body)


                # FIXME: Keep old buffer parsing for backward compatibility.
                if type(buffer_content) == list:
                    for record in buffer_content:
                        self.subscribers[exchange_name][routing_key] \
                            ["total_records"] += 1
                        record["exchangeName"] = exchange_name
                        record["routingKey"] = routing_key
                        self.send(json.dumps(record))
                # FIXME: This is the new data format. Keep this parsing.
                elif type(buffer_content) == dict:
                    for record in buffer_content['chunk']:
                        self.subscribers[exchange_name][routing_key] \
                            ["total_records"] += 1
                        record["exchangeName"] = exchange_name
                        record["routingKey"] = routing_key
                        self.send(json.dumps(record))



            return send_probe

        def on_open(self, info):
            logging.info("Got a new connection...")
            self.clients.add(self)

        def on_message(self, message):
            """
            This will receive instructions from the client to change the
            stream. After the connection is established we expect to receive a
            JSON with exchangeName, routingKey, token; then we subscribe to
            RabbitMQ and start streaming the data.

            NOTE: it's not possible to open multiple connections from the same
            client. so in case we need to stream different devices/metrics/etc.
            at the same time, we need to use a solution that is like the
            multiplexing in the sockjs-tornado examples folder.

            :param message: subscription message to process

            """
            logging.info("Got a new subscription message: " + message)

            msg_dict = json.loads(message)
            if msg_dict['type'] == 'subscription':
                self.handle_channel_subscription(msg_dict)
            elif msg_dict['type'] == 'unsubscription':
                self.handle_channel_unsubscription(msg_dict)

        def handle_channel_subscription(self, stream_configuration):
            exchange_name = stream_configuration['exchangeName']
            routing_key = stream_configuration['routingKey']
            queue_name = 'websocket-client-%s' % str(uuid4())
            # Look for a token.
            token = (stream_configuration['token']
                     if 'token' in stream_configuration else None)
            # If there is a token, use it in place of rabbitmq user and pwd.
            if token:
                rabbitmq_user = token
                rabbitmq_pwd = ""
            # Otherwise, look for a username and password.
            elif ('user' in stream_configuration
                  and 'password' in stream_configuration):
                rabbitmq_user = stream_configuration['user']
                rabbitmq_pwd = stream_configuration['password']
            else:
                raise ValueError('Missing parameters in subscribe message '
                                 'to websocket server. You muse either pass '
                                 'the param "token" OR two params: '
                                 '"username" and "password".')

            if not self.routing_key_exists(exchange_name, routing_key):
                self.subscribers[exchange_name][routing_key] = {
                    "subscriber": TornadoSubscriber(
                        callback=self.send_probe_factory(
                            exchange_name, routing_key),
                        exchange_name=exchange_name,
                        routing_key=routing_key,
                        rabbit_auth_url=rabbit_auth_url,
                        rabbitmq_address=rabbitmq_address,
                        rabbitmq_user=rabbitmq_user,
                        rabbitmq_pwd=rabbitmq_pwd,
                        queue_name=queue_name,
                        token=token
                    ),
                    "total_records": 0
                }

                self.subscribers[exchange_name] \
                    [routing_key]["subscriber"].connect()

        def handle_channel_unsubscription(self, unsubscription_msg):
            exchange_name = unsubscription_msg['exchangeName']
            routing_key = unsubscription_msg['routingKey']

            logging.info("Unsubscription received for "
                         "exchange_name: %s, routing_key: %s"
                         % (exchange_name, routing_key))
            if self.routing_key_exists(exchange_name, routing_key):
                self.subscribers[exchange_name][routing_key] \
                    ["subscriber"].disconnect()

        def on_close(self):
            logging.info("Disconnecting client...")
            for exchange_name in self.subscribers:
                for routing_key in self.subscribers[exchange_name]:
                    subscriber = self.subscribers[exchange_name] \
                        [routing_key]["subscriber"]
                    if subscriber is not None:
                        logging.info(
                            "Disconnecting subscriber for exchange_name: %s, "
                            "routing_key: %s" % (exchange_name, routing_key))
                        subscriber.disconnect()

            self.subscribers = {}
            self.clients.remove(self)
            logging.info("Client disconnection complete!")

        def send_heartbeat(self):
            self.broadcast(self.clients, 'message')

        def routing_key_exists(self, exchange_name, routing_key):
            return (self.subscribers.has_key(exchange_name)
                    and self.subscribers[exchange_name].has_key(routing_key))

    return RtStreamConnection


class TornadoSubscriber(object):
    """
    See: https://pika.readthedocs.org/en/0.9.14/examples/tornado_consumer.html
    """

    def __init__(self, callback, exchange_name, routing_key, rabbit_auth_url,
                 rabbitmq_address, rabbitmq_user, rabbitmq_pwd,
                 queue_name, token=None):

        self.callback = callback
        self.exchange_name = exchange_name
        self.routing_key = routing_key

        self.rabbit_auth_url = rabbit_auth_url

        self.rabbitmq_address = rabbitmq_address
        self.rabbitmq_user = rabbitmq_user
        self.rabbitmq_pwd = rabbitmq_pwd
        self.queue_name = queue_name
        self.token = token

        self.connection = None
        self.channel = None
        self.consumer_tag = None

    def connect(self):
        auth = CloudbrainAuth(self.rabbit_auth_url)
        if self.token:
            credentials = pika.PlainCredentials(self.token, '')
            vhost = auth.get_vhost_by_token(self.token)
            connection_params = pika.ConnectionParameters(
                host=self.rabbitmq_address, virtual_host=vhost,
                credentials=credentials)
        else:
            credentials = pika.PlainCredentials(self.rabbitmq_user,
                                                self.rabbitmq_pwd)
            vhost = getattr(self, 'rabbitmq_vhost',
                            auth.get_vhost_by_username(self.rabbitmq_user))
            connection_params = pika.ConnectionParameters(
                host=self.rabbitmq_address, virtual_host=vhost,
                credentials=credentials)
        self.connection = pika.adapters.tornado_connection.TornadoConnection(
            connection_params,
            self.on_connected,
            stop_ioloop_on_close=False,
            custom_ioloop=IOLoop.instance())

    def disconnect(self):
        if self.connection is not None:
            self.connection.close()

    def on_connected(self, connection):
        self.connection = connection
        self.connection.add_on_close_callback(self.on_connection_closed)
        self.connection.add_backpressure_callback(self.on_backpressure_callback)
        self.open_channel()

    def on_connection_closed(self, connection, reply_code, reply_text):
        self.connection = None
        self.channel = None

    def on_backpressure_callback(self, connection):
        logging.info("******** Backpressure detected for exchange %s and "
                     "routing key %s" % (self.exchange_name, self.routing_key))

    def open_channel(self):
        self.connection.channel(self.on_channel_open)

    def on_channel_open(self, channel):
        self.channel = channel
        self.channel.add_on_close_callback(self.on_channel_closed)
        logging.info("Declaring exchange: %s" % self.exchange_name)

        if self.exchange_name == 'amq.topic':
            # Note: this is the reserved excahnge name for MQTT. Therefore,
            # "type" must be "topic" and "durable" must be set to "True".
            self.channel.exchange_declare(self.on_exchange_declareok,
                                          exchange=self.exchange_name,
                                          type='topic',
                                          durable=True)
        else:
            self.channel.exchange_declare(self.on_exchange_declareok,
                                          exchange=self.exchange_name,
                                          type='direct',
                                          passive=True)

    def on_channel_closed(self, channel, reply_code, reply_text):
        self.connection.close()

    def on_exchange_declareok(self, unused_frame):
        self.channel.queue_declare(self.on_queue_declareok,
                                   self.queue_name,
                                   exclusive=True)

    def on_queue_declareok(self, unused_frame):
        logging.info("Binding queue. Exchange name: %s. Routing key: %s"
                     % (self.exchange_name, self.routing_key))
        self.channel.queue_bind(
            self.on_bindok,
            exchange=self.exchange_name,
            queue=self.queue_name,
            routing_key=self.routing_key)

    def on_bindok(self, unused_frame):
        self.channel.add_on_cancel_callback(self.on_consumer_cancelled)
        self.consumer_tag = self.channel.basic_consume(self.on_message,
                                                       self.queue_name,
                                                       exclusive=True,
                                                       no_ack=True)

    def on_consumer_cancelled(self, method_frame):
        if self.channel:
            self.channel.close()

    def on_message(self, unused_channel, basic_deliver, properties, body):
        self.callback(body)


class WebsocketServer(object):
    def __init__(self, ws_server_port, rabbitmq_address, rabbit_auth_url):
        self.rabbitmq_address = rabbitmq_address
        self.ws_server_port = ws_server_port
        self.rabbit_auth_url = rabbit_auth_url

    def start(self):
        RtStreamConnection = _rt_stream_connection_factory(
            self.rabbitmq_address, self.rabbit_auth_url)

        # 1. Create chat router
        RtStreamRouter = SockJSRouter(RtStreamConnection, '/rt-stream')

        # 2. Create Tornado application
        app = Application(RtStreamRouter.urls)

        # 3. Make Tornado app listen on Pi
        app.listen(self.ws_server_port)

        print("Real-time data server running at "
              "http://localhost:%s" % self.ws_server_port)

        # 4. Start IOLoop
        IOLoop.instance().start()

    def stop(self):
        IOLoop.instance().stop()
