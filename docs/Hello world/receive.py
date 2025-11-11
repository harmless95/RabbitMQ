import os
import sys

import pika


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    chanel = connection.channel()

    chanel.queue_declare(queue="hello")
    def callback(ch, method, properties, body):
        print(f"[x] Received {body}")

    chanel.basic_consume(queue="hello", auto_ack=True, on_message_callback=callback)
    print("[*] Waiting for message. To exit press CTRL+C")

    chanel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)