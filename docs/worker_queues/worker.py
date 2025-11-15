import time
import os
import sys

import pika


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    chanel = connection.channel()

    chanel.queue_declare(queue="task_queue1", durable=True)

    def callback(ch, method, properties, body):
        print("проверка тела в байтовом ввиде", body)
        print(f" [x] Received {body.decode()}")
        time.sleep(body.count(b"."))
        print(" [x] Done")
        chanel.basic_ack(delivery_tag=method.delivery_tag)

    chanel.basic_consume(queue="task_queue", on_message_callback=callback)
    print("[*] Waiting for message. To exit press CTRL+C")

    chanel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
