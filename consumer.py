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
    from pika.spec import BasicProperties, Basic

log = logging.getLogger(__name__)


def process_new_message(
    ch: "BlockingChannel",
    method: "Basic.Deliver",
    properties: "BasicProperties",
    body: bytes,
):
    log.debug("ch: %s", ch)
    log.debug("method: %s", method)
    log.debug("properties: %s", properties)
    log.debug("body: %s", body)

    log.warning("[ ] Start processing message(expensive task!) %r", body)
    start_time = time.time()
    ...
    time.sleep(1)
    ...
    end_time = time.time()
    log.info("Finish processing message %r, sending ack!", body)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    log.warning(
        " [X] Finish in %.2fs processing message: %r",
        end_time - start_time,
        body,
    )


def consumer_message(channel: "BlockingChannel") -> None:
    channel.basic_consume(
        queue=MQ_ROUTING_KEY,
        on_message_callback=process_new_message,
        # auto_ack=True,
    )
    log.warning("Waiting for message")
    channel.start_consuming()


def main():
    config_logging(level=logging.WARNING)
    with get_connection() as connection:
        log.info("Create connection: %s", connection)
        with connection.channel() as channel:
            log.info("Create channel: %s", channel)

            consumer_message(channel=channel)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Bye")
