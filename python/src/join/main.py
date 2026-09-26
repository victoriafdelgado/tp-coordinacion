import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.aggregators = {}
        self.fruit_tops = {}

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info("Handling SIGTERM")
        try:
            self.input_queue.stop_consuming()
        except Exception as e:
            logging.error(f"Error al detener input_queue: {e}")

    def _close(self):
        try:
            self.input_queue.close()
        except Exception as e:
            logging.error(f"Error al cerrar 'input_queue': {e}")
        try:
            self.output_queue.close()
        except Exception as e:
            logging.error(f"Error al cerrar 'output_queue': {e}")     

    def process_message(self, message, ack, nack):
        logging.info("Received top")
        client_id, partial_top = message_protocol.internal.deserialize(message)

        self.aggregators[client_id] = self.aggregators.get(client_id, 0) + 1
        client_items = self.fruit_tops.setdefault(client_id, [])
        client_items.extend(
            fruit_item.FruitItem(fruit, amount) for fruit, amount in partial_top
        )  

        if self.aggregators[client_id] < AGGREGATION_AMOUNT:
            ack()
            return
        
        fruit_chunk = sorted(self.fruit_tops.pop(client_id))[-TOP_SIZE:]
        fruit_chunk.reverse()
        self.aggregators.pop(client_id)
        fruit_top = [(item.fruit, item.amount) for item in fruit_chunk]
        self.output_queue.send(message_protocol.internal.serialize([client_id, fruit_top]))
        ack()

    def start(self):
        try:
            self.input_queue.start_consuming(self.process_message)
        except Exception as e:
            logging.error(f"Error al empezar a consumir con 'input_queue': {e}")
            raise e
        finally:
            self._close()

def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()
    return 0

if __name__ == "__main__":
    main()
