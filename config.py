import logging
import pika

RMQ_HOST = "127.0.0.1"
RMQ_PORT = 5672

RMQ_USER = "guest"
RMQ_PASSWORD = "guest"

MQ_EXCHANGE = ""
MQ_ROUTING_KEY = "news"

connection_param = pika.ConnectionParameters(
    host=RMQ_HOST,
    port=RMQ_PORT,
    credentials=pika.PlainCredentials(
        username=RMQ_USER,
        password=RMQ_PASSWORD,
    ),
)


def get_connection() -> pika.BlockingConnection:
    return pika.BlockingConnection(
        parameters=connection_param,
    )


def config_logging(level: int = logging.INFO):
    logging.basicConfig(
        level=level,
        datefmt="%Y-%m-%d %H:%M:%S",
        format="[%(asctime)s.%(msecs)03d] %(funcName)20s %(module)s:%(lineno)d %(levelname)-8s - %(message)s",
    )
