import asyncio
from rstream import Producer

STREAM_NAME = "hello_python_stream"
STREAM_RETENTION = 5000000000


async def main():
    async with Producer(
        host="localhost", username="guest", password="guest"
    ) as producer:
        await producer.create_stream(
            stream=STREAM_NAME,
            exists_ok=True,
            arguments={"MaxLenghtBytes": STREAM_RETENTION},
        )
        await producer.send(stream=STREAM_NAME, message=b"Hello World")


if __name__ == "__main__":
    asyncio.run(main())
