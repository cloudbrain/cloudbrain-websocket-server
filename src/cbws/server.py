import pika
import json
import logging

from collections import defaultdict
from sockjs.tornado.conn import SockJSConnection
from sockjs.tornado import SockJSRouter
from tornado.ioloop import IOLoop
from tornado.web import Application

from uuid import uuid4

_LOGGER = logging.getLogger()
_LOGGER.setLevel(logging.INFO)

recursivedict = lambda: defaultdict(recursivedict)


def _rt_stream_connection_factory(rabbitmq_address, rabbitmq_user,
                                  rabbitmq_pwd):
    """
    RtStreamConnection class factory.

    :param rabbitmq_address: RabbitMQ server address
    :return: RtStreamConnection
    """

    class RtStreamConnection(SockJSConnection):
        """RtStreamConnection connection implementation"""

        clients = set()

        def __init__(self, session):

            super(self.__class__, self).__init__(session)

            self.subscribers = recursivedict()
            self.total_records = recursivedict()

        def send_probe_factory(self, device_id, device_name, metric):

            def send_probe(body):
                logging.debug("GOT: " + body)
                buffer_content = json.loads(body)

                for record in buffer_content:
                    self.subscribers[device_id]\
                        [device_name][metric]["total_records"] += 1
                    record["device_id"] = device_id
                    record["device_name"] = device_name
                    record["metric"] = metric

                    self.send(json.dumps(record))

            return send_probe

        def on_open(self, info):
            logging.info("Got a new connection...")
            self.clients.add(self)

        def on_message(self, message):
            """
            This will receive instructions from the client to change the
            stream. After the connection is established we expect to receive a 
            JSON with deviceName, deviceId, metric; then we subscribe to 
            RabbitMQ and tart streaming the data.

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
            device_name = stream_configuration['deviceName']
            device_id = stream_configuration['deviceId']
            metric = stream_configuration['metric']
            downsampling_factor = stream_configuration.\
                get('downsampling_factor', 16)
            subscriber_id = str(uuid4())

            if not self.metric_exists(device_id, device_name, metric):
                self.subscribers[device_id][device_name][metric] = {
                    "subscriber": TornadoSubscriber(
                        callback=self.send_probe_factory(
                            device_id, device_name, metric),
                        device_name=device_name,
                        device_id=device_id,
                        rabbitmq_address=rabbitmq_address,
                        rabbitmq_user=rabbitmq_user,
                        rabbitmq_pwd=rabbitmq_pwd,
                        metric_name=metric,
                        queue_name=subscriber_id),
                    "downsampling_factor": downsampling_factor,
                    "total_records": 0
                }

                self.subscribers[device_id][device_name]\
                    [metric]["subscriber"].connect()

        def handle_channel_unsubscription(self, unsubscription_msg):
            device_name = unsubscription_msg['deviceName']
            device_id = unsubscription_msg['deviceId']
            metric = unsubscription_msg['metric']

            logging.info("Unsubscription received for device_id: %s, "
                         "device_name: %s, metric: %s"
                         % (device_id, device_name, metric))
            if self.metric_exists(device_id, device_name, metric):
                self.subscribers[device_id][device_name][metric]\
                    ["subscriber"].disconnect()

        def on_close(self):
            logging.info("Disconnecting client...")
            for device_id in self.subscribers:
                for device_name in self.subscribers[device_id]:
                    for metric in self.subscribers[device_id][device_name]:
                        subscriber = self.subscribers[device_id]\
                            [device_name][metric]["subscriber"]
                        if subscriber is not None:
                            logging.info(
                                "Disconnecting subscriber for device_id: %s, "
                                "device_name: %s, "
                                "metric: %s" % (device_id,
                                                device_name, metric))
                            subscriber.disconnect()

            self.subscribers = {}
            self.clients.remove(self)
            logging.info("Client disconnection complete!")

        def send_heartbeat(self):
            self.broadcast(self.clients, 'message')

        def metric_exists(self, device_id, device_name, metric):
            me = (self.subscribers.has_key(device_id)
                  and self.subscribers[device_id].has_key(device_name)
                  and self.subscribers[device_id][device_name].has_key(metric))
            return me

    return RtStreamConnection


class TornadoSubscriber(object):
    """
    See: https://pika.readthedocs.org/en/0.9.14/examples/tornado_consumer.html
    """

    def __init__(self, callback, device_name, device_id, rabbitmq_address,
                 rabbitmq_user,
                 rabbitmq_pwd, metric_name, queue_name):
        self.callback = callback
        self.device_name = device_name
        self.device_id = device_id
        self.metric_name = metric_name

        self.connection = None
        self.channel = None

        self.rabbitmq_address = rabbitmq_address
        self.rabbitmq_user = rabbitmq_user
        self.rabbitmq_pwd = rabbitmq_pwd
        self.queue_name = queue_name

        self.consumer_tag = None

    def connect(self):
        credentials = pika.PlainCredentials('cloudbrain', 'cloudbrain')
        self.connection = pika.adapters.tornado_connection.TornadoConnection(
            pika.ConnectionParameters(
                host=self.rabbitmq_address, credentials=credentials),
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
        logging.info("******** Backpressure detected for %s" % self.get_key())

    def open_channel(self):
        self.connection.channel(self.on_channel_open)

    def on_channel_open(self, channel):
        self.channel = channel
        self.channel.add_on_close_callback(self.on_channel_closed)
        logging.info("Declaring exchange: %s" % self.get_key())
        self.channel.exchange_declare(self.on_exchange_declareok,
                                      exchange=self.get_key(),
                                      type='direct',
                                      passive=True)

    def on_channel_closed(self, channel, reply_code, reply_text):
        self.connection.close()

    def on_exchange_declareok(self, unused_frame):
        self.channel.queue_declare(self.on_queue_declareok,
                                   self.queue_name,
                                   exclusive=True, )

    def on_queue_declareok(self, unused_frame):
        logging.info("Binding queue: " + self.get_key())
        self.channel.queue_bind(
            self.on_bindok,
            exchange=self.get_key(),
            queue=self.queue_name,
            routing_key=self.get_key())

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

    def get_key(self):
        key = "%s:%s:%s" % (self.device_id, self.device_name, self.metric_name)
        return key


class WebsocketServer(object):
    def __init__(self, ws_server_port, rabbitmq_address, rabbitmq_user,
                 rabbitmq_pwd):
        self.rabbitmq_address = rabbitmq_address
        self.rabbitmq_user = rabbitmq_user
        self.rabbitmq_pwd = rabbitmq_pwd
        self.ws_server_port = ws_server_port

    def start(self):
        RtStreamConnection = _rt_stream_connection_factory(
            self.rabbitmq_address,
            self.rabbitmq_user,
            self.rabbitmq_pwd)

        # 1. Create chat router
        RtStreamRouter = SockJSRouter(RtStreamConnection, '/rt-stream')

        # 2. Create Tornado application
        app = Application(RtStreamRouter.urls)

        # 3. Make Tornado app listen on Pi
        app.listen(self.ws_server_port)

        print "Real-time data server running at " \
              "http://localhost:%s" % self.ws_server_port

        # 4. Start IOLoop
        IOLoop.instance().start()

    def stop(self):
        IOLoop.instance().stop()
