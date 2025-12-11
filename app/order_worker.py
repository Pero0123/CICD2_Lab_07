import aio_pika
import asyncio
import json
import os
from dotenv import load_dotenv
load_dotenv()  # optional, if you keep a .env file in the project

RABBIT_URL = os.getenv("RABBIT_URL")
if not RABBIT_URL:
    raise RuntimeError("RABBIT_URL is not set. Export it (e.g. export RABBIT_URL='amqps://user:pass@host/vhost') or load your .env.")
EXCHANGE_NAME = "events_topic"


async def main():
    conn = await aio_pika.connect_robust(RABBIT_URL)
    ch = await conn.channel()
    
    # Declare the topic exchange
    ex = await ch.declare_exchange(EXCHANGE_NAME, aio_pika.ExchangeType.TOPIC)
    
    # Queue for order events
    queue = await ch.declare_queue("order_events_queue")
    
    # Bind to all routing keys starting with 'order.'
    await queue.bind(ex, routing_key="order.*")
    
    print("Listening for order events (routing key: 'order.*')...")
    
    async with queue.iterator() as q:
        async for msg in q:
            async with msg.process():
                data = json.loads(msg.body)
                print("Order Event:", msg.routing_key, data)


if __name__ == "__main__":
    asyncio.run(main())