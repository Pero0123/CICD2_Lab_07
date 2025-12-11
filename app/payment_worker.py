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
    
    # Declare the same topic exchange
    ex = await ch.declare_exchange(EXCHANGE_NAME, aio_pika.ExchangeType.TOPIC)
    
    # Queue for payment events
    queue = await ch.declare_queue("payment_events_queue")
    
    # Bind to all routing keys starting with 'payment.'
    await queue.bind(ex, routing_key="payment.#")
    
    print("Listening for payment events (routing key: 'payment.#')...")
    
    async with queue.iterator() as q:
        async for msg in q:
            async with msg.process():
                data = json.loads(msg.body)
                print("Payment Event:", msg.routing_key, data)


if __name__ == "__main__":
    asyncio.run(main())