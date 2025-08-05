import logging
import time
from typing import TYPE_CHECKING

from config import (
    get_connection,
    config_logging,
    MQ_EXCHANGE,
    MQ_ROUTING_KEY,
)

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel

log = logging.getLogger(__name__)


def produce_message(channel: "BlockingChannel") -> None:
    queue = channel.queue_declare(queue=MQ_ROUTING_KEY)
    log.info("Declared queue: %r %s", MQ_ROUTING_KEY, queue)
    message_channel = f"Hello World from {time.time()}"
    log.info("Publish message: %s", message_channel)
    channel.basic_publish(
        exchange=MQ_EXCHANGE,
        routing_key=MQ_ROUTING_KEY,
        body=message_channel,
    )
    log.warning("Published message: %s", message_channel)


def main():
    config_logging(level=logging.WARNING)
    with get_connection() as connection:
        log.info("Create connection: %s", connection)
        with connection.channel() as channel:
            log.info("Create channel: %s", channel)

            produce_message(channel=channel)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Bye")
