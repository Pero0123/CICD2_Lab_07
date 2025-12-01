import aio_pika
import asyncio
import json
import os
RABBIT_URL = os.getenv("RABBIT_URL")
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
                data = json.loads(message.body)
                print("Received:", data)

if __name__ == "__main__":
    asyncio.run(main())