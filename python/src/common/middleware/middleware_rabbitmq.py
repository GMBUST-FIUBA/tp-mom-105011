import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareCloseError, MessageMiddlewareMessageError, MessageMiddlewareDisconnectedError

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self._queue = pika.BlockingConnection(pika.ConnectionParameters(host=host)).channel()
        self._queue_name = queue_name
        self._queue.queue_declare(queue=queue_name)
        self._queue.basic_qos(prefetch_count=1)
        self._is_consuming = False
    
    #Comienza a escuchar a la cola e invoca a on_message_callback tras
	#cada mensaje de datos o de control con el cuerpo del mensaje.
	# on_message_callback tiene como parámetros:
	# message - El valor tal y como lo recibe el método send de esta clase.
	# ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
	# nack - Función que al invocarse realiza nack al mensaje que se está consumiendo. 
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def start_consuming(self, on_message_callback):
        def internal_on_message_callback(ch, method, properties, body):
            ack_function = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
            nack_function = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)

            try:
                on_message_callback(body, ack_function, nack_function)
            except pika.exceptions.ChannelClosed:
                raise MessageMiddlewareDisconnectedError
            except Exception:
                raise MessageMiddlewareMessageError

        self._queue.basic_consume(queue=self._queue_name, on_message_callback=internal_on_message_callback)
        try:
            self._is_consuming = True
            self._queue.start_consuming()
        except pika.exceptions.ChannelClosed:
                raise MessageMiddlewareDisconnectedError
        except Exception:
            raise MessageMiddlewareMessageError
	
	#Si se estaba consumiendo desde la cola, se detiene la escucha. Si
	#no se estaba consumiendo de la cola, no tiene efecto, ni levanta
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    def stop_consuming(self):
        if self._is_consuming:
            try:
                self._queue.stop_consuming()
                self._is_consuming = False
            except pika.exceptions.ChannelClosed:
                raise MessageMiddlewareDisconnectedError
        
    #Envía un mensaje a la cola.
    #Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def send(self, message):
        try:
            self._queue.basic_publish(exchange="", routing_key=self._queue_name, body=message)
        except pika.exceptions.ChannelClosed:
            raise MessageMiddlewareDisconnectedError
        except Exception:
            raise MessageMiddlewareMessageError

    #Se desconecta de la cola.
    #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
    def close(self):
        try:
            self._queue.close()
        except Exception:
            raise MessageMiddlewareCloseError

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self._channel = pika.BlockingConnection(pika.ConnectionParameters(host=host)).channel()
        self._exchange_name = exchange_name
        self._host = host
        self._routing_keys = routing_keys
        self._channel.exchange_declare(exchange=self._exchange_name, exchange_type='direct')
        self._is_consuming = False

    #Comienza a escuchar el exchange e invoca a on_message_callback tras
	#cada mensaje de datos o de control con el cuerpo del mensaje.
	# on_message_callback tiene como parámetros:
	# message - El valor tal y como lo recibe el método send de esta clase.
	# ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
	# nack - Función que al invocarse realiza nack al mensaje que se está consumiendo. 
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def start_consuming(self, on_message_callback):
        # Crear nueva cola
        self._channel = pika.BlockingConnection(pika.ConnectionParameters(host=self._host)).channel()
        result = self._channel.queue_declare(queue='', exclusive=True)
        new_channel_name = result.method.queue

        # Definir wrapper de callback
        def internal_on_message_callback(ch, method, properties, body):
            ack_function = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
            nack_function = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)

            try:
                on_message_callback(body, ack_function, nack_function)
            except pika.exceptions.ChannelClosed:
                raise MessageMiddlewareDisconnectedError
            except Exception:
                raise MessageMiddlewareMessageError

        # Asociar cola a los routing_keys deseados
        for routing_key in self._routing_keys:
            self._channel.queue_bind(exchange=self._exchange_name, queue=new_channel_name, routing_key=routing_key)

        # Asociar callback
        self._channel.basic_consume(queue=new_channel_name, on_message_callback=internal_on_message_callback)
        # Consumir
        try:
            self._is_consuming = True
            self._channel.start_consuming()
        except pika.exceptions.ChannelClosed:
                raise MessageMiddlewareDisconnectedError
        except Exception:
            raise MessageMiddlewareMessageError
        
    #Si se estaba consumiendo desde el exchange, se detiene la escucha. Si
    #no se estaba consumiendo del exchange, no tiene efecto, ni levanta
    #Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    def stop_consuming(self):
        if self._is_consuming:
            try:
                self._channel.stop_consuming()
                self._is_consuming = False
            except pika.exceptions.ChannelClosed:
                raise MessageMiddlewareDisconnectedError
        
    #Envía un mensaje al tópico con el que se inicializó el exchange.
    #Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def send(self, message):
        try:
            self._channel.basic_publish(exchange=self._exchange_name, body=message, routing_key=self._routing_keys[0])
        except pika.exceptions.ChannelClosed:
            raise MessageMiddlewareDisconnectedError
        except Exception:
            raise MessageMiddlewareMessageError

    #Se desconecta del exchange al que estaba conectado.
    #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
    def close(self):
        try:
            self._channel.close()
        except Exception:
            raise MessageMiddlewareCloseError
