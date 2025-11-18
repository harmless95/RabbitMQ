import asyncio
import aio_pika


async def main():
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    channel = await connection.channel()

    queue = await channel.declare_queue(name="my_queue")

    async with queue.iterator() as queue_iterable:
        async for message in queue_iterable:
            async with message.process():
                print(f"Message: {message.body.decode()}")


if __name__ == "__main__":
    asyncio.run(main())
