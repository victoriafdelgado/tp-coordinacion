import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError

class _MessageMiddlewareRabbitMQ():
    def __init__(self, channel, connection):
        self.channel = channel
        self.connection = connection
        self._is_consuming = False

    def _define_callback(self, on_message_callback):
        def callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)
            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)
            on_message_callback(body, ack, nack)
        return callback

    def _start_consuming(self, on_message_callback, queue_name):
        if self._is_consuming:
            raise MessageMiddlewareMessageError()
        try:
            callback = self._define_callback(on_message_callback)
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(queue=queue_name,
                                        on_message_callback=callback)
            self._is_consuming = True
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(e)
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(e)
        finally:
            self._is_consuming = False

    def _stop_consuming(self):
        if not self._is_consuming:
            return
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(e)

    def _close(self):
        try:
            self.connection.close()
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareCloseError(e)
        

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        try:
            self.connection =  pika.BlockingConnection(pika.ConnectionParameters(host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=queue_name, durable=True)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(e)
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(e)
        self.queue_name = queue_name
        self._messagemwrabbit = _MessageMiddlewareRabbitMQ(self.channel, self.connection)
    
    def start_consuming(self, on_message_callback):
        self._messagemwrabbit._start_consuming(on_message_callback, self.queue_name)
    
    def stop_consuming(self):
        self._messagemwrabbit._stop_consuming()

    def send(self, message):
        try:
            self.channel.basic_publish(exchange='',
                                    routing_key=self.queue_name,
                                    body=message,
                                    properties=pika.BasicProperties(
                                        delivery_mode = pika.DeliveryMode.Persistent
                                    ))
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(e)
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(e)

    def close(self):
        self._messagemwrabbit._close()

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        try:
            self.connection =  pika.BlockingConnection(pika.ConnectionParameters(host))
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable = True)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(e)
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(e)
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self._messagemwrabbit = _MessageMiddlewareRabbitMQ(self.channel, self.connection)

    def start_consuming(self, on_message_callback):
        try:
            result = self.channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            
            for routing_key in self.routing_keys:
                    self.channel.queue_bind(exchange=self.exchange_name, 
                                            queue=queue_name,
                                            routing_key=routing_key)
                    
            
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(e)
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(e)

        self._messagemwrabbit._start_consuming(on_message_callback, queue_name)

    def stop_consuming(self):
        self._messagemwrabbit._stop_consuming()

    def send(self, message):
        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(exchange=self.exchange_name, 
                                        routing_key=routing_key, 
                                        body=message)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(e)
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(e)

    def close(self):
        self._messagemwrabbit._close()
