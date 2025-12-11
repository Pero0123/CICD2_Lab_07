from fastapi import FastAPI
import aio_pika
import os
import json
from dotenv import load_dotenv
load_dotenv()

RABBIT_URL = os.getenv("RABBIT_URL")
if not RABBIT_URL:
    raise RuntimeError("RABBIT_URL is not set. Export it or add it to .env.")
app = FastAPI()



@app.post("/order")
async def publish_order(order: dict):
    """
    Receives an order as JSON and publishes it to a RabbitMQ queue.
    """
    # Connect to RabbitMQ
    connection = await aio_pika.connect_robust(RABBIT_URL)
    channel = await connection.channel()
    
    # Convert the order dict to bytes
    message = aio_pika.Message(body=json.dumps(order).encode())
    
    # Publish to the default exchange with routing_key = queue name
    await channel.default_exchange.publish(
        message,
        routing_key="orders_queue"
    )
    
    # Close the connection after publishing
    await connection.close()
    
    return {"status": "Message sent", "order": order}