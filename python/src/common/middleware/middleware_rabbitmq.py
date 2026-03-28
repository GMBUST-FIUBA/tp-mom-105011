import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareCloseError, MessageMiddlewareMessageError, MessageMiddlewareDisconnectedError

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self._queue = pika.BlockingConnection(pika.ConnectionParameters(host=host)).channel()
        self._queue_name = queue_name
        self._queue.queue_declare(queue=queue_name)
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

        self._queue.basic_consume(queue=self._queue_name, on_message_callback=internal_on_message_callback, auto_ack=False)
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
            self._queue.stop_consuming()
        
    #Envía un mensaje a la cola.
    #Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
    #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
    def send(self, message):
        self._queue.basic_publish(exchange="", routing_key=self._queue_name, body=message)

    #Se desconecta de la cola.
    #Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
    def close(self):
        try:
            self._queue.close()
        except Exception as e:
            raise MessageMiddlewareCloseError

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass
