import os
import logging
import threading
import signal
import hashlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

CONTROL_KEY = "CONTROL_KEY"

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        self.control_exchange_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_CONTROL_EXCHANGE}_{ID}"]
        )

        self.control_exchange_publisher = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_CONTROL_EXCHANGE}_{i}" for i in range(SUM_AMOUNT)]
        )

        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.lock = threading.Lock()
        self.amount_by_fruit = {}    
        self.msg_count = {}       
        self.eof_total = {}     
        self.total = {}     

        self.control_thread = threading.Thread(target=self._control_exchange_loop)
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info("Handling SIGTERM")
        self.input_queue.stop_consuming()

    def _close(self):
        try:
            self.input_queue.close()
        except Exception as e:
            logging.error(f"Error al cerrar 'input_queue': {e}")
        try:
            self.control_exchange_publisher.close()
        except Exception as e:
            logging.error(f"Error al cerrar 'control_exchange_publisher': {e}")     
 
    def _broadcast(self, client_id, total=None):
        self.control_exchange_publisher.send(message_protocol.internal.serialize([client_id, ID, self.msg_count.get(client_id, 0), total]))

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")
        with self.lock:
            client_fruits = self.amount_by_fruit.setdefault(client_id,{})
            client_fruits[fruit] = client_fruits.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

            self.msg_count[client_id] = self.msg_count.get(client_id, 0 )+1

            if client_id in self.eof_total:
                self._broadcast(client_id)

    def _process_eof(self, client_id, total):
        with self.lock:
            self._broadcast(client_id, total)

    def process_control_message(self, message, ack, nack):
        client_id, sender_id, msg_count, msg_total = message_protocol.internal.deserialize(message)
        with self.lock:
            if msg_total is not None and client_id not in self.eof_total:
                self.eof_total[client_id] = msg_total
                self._broadcast(client_id)

            total = self.total.setdefault(client_id, {})
            total[sender_id] = max(total.get(sender_id, 0), msg_count)

            expected = self.eof_total.get(client_id)
            if expected is not None and sum(total.values()) == expected:
                self._flush(client_id)
        ack()

    def _flush(self, client_id):
        client_totals = self.amount_by_fruit.pop(client_id, {})
        for state in (self.msg_count, self.eof_total, self.total):
            state.pop(client_id, None)

        for final_fruit_item in client_totals.values():
            target = self.data_output_exchanges[
                int(hashlib.md5(final_fruit_item.fruit.encode()).hexdigest(), 16) % AGGREGATION_AMOUNT
            ]
            target.send(
                message_protocol.internal.serialize(
                    [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )

        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def _control_exchange_loop(self):
        try:
            self.control_exchange_consumer.start_consuming(self.process_control_message)
        finally:
            self._close_control()

    def _close_control(self):
        try:
            self.control_exchange_consumer.close()
        except Exception as e:
            logging.error(f"Error al cerrar 'control_exchange_consumer': {e}")
        for i, exchange in enumerate(self.data_output_exchanges):
            try:
                exchange.close()
            except Exception as e:
                logging.error(f"Error al cerrar 'data_output_exchange_{i}': {e}")

    def start(self):
        self.control_thread.start()
        try:
            self.input_queue.start_consuming(self.process_data_messsage)
        except Exception as e:
            logging.error(f"Error al empezar a consumir con 'input_queue': {e}")
        finally:
            while self.control_thread.is_alive():
                try: 
                    self.control_exchange_consumer.stop_consuming_threadsafe()
                except Exception as e:
                    logging.error(f"Error al detener 'control_exchange_consumer': {e}")
                self.control_thread.join()
            self._close()
        
def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0

if __name__ == "__main__":
    main()
