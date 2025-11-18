import asyncio
import aio_pika


async def main():
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    channel = await connection.channel()
    async with connection:
        await channel.default_exchange.publish(
            message=aio_pika.Message(body=b"Hello Vitalya message sent"),
            routing_key="my_queue",
        )
        print("Message sent")


if __name__ == "__main__":
    asyncio.run(main())
