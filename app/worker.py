import aio_pika
import asyncio
import json
import os
from dotenv import load_dotenv
load_dotenv()  # optional, if you keep a .env file in the project

RABBIT_URL = os.getenv("RABBIT_URL")
if not RABBIT_URL:
    raise RuntimeError("RABBIT_URL is not set. Export it (e.g. export RABBIT_URL='amqps://user:pass@host/vhost') or load your .env.")


async def main():
    # Connect to RabbitMQ
    connection = await aio_pika.connect_robust(RABBIT_URL)
    channel = await connection.channel()
    
    # Declare the queue (durable = survives broker restart)
    queue = await channel.declare_queue("orders_queue", durable=True)
    
    print("Waiting for messages on 'orders_queue'...")
    
    # Consume messages in an async loop
    async with queue.iterator() as q:
        async for message in q:
            async with message.process():
                # Ensure the body is bytes (handles bytes or memoryview) and decode to str
                body_bytes = bytes(message.body)
                data = json.loads(body_bytes.decode("utf-8"))
                print("Received:", data)


if __name__ == "__main__":
    asyncio.run(main())