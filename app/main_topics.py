from fastapi import FastAPI
import aio_pika
import json
import os

app = FastAPI()

EXCHANGE_NAME = "events_topic"
RABBIT_URL = os.getenv("RABBIT_URL")

async def get_exchange():
    """
    Open a connection, create a channel and declare a topic exchange.
    Returns (connection, channel, exchange).
    """
    conn = await aio_pika.connect_robust(RABBIT_URL)
    ch = await conn.channel()
    ex = await ch.declare_exchange(EXCHANGE_NAME, aio_pika.ExchangeType.TOPIC)
    return conn, ch, ex

@app.post("/order/create")
async def order_created(order: dict):
    """
    Publish an 'order.created' event.
    """
    conn, ch, ex = await get_exchange()
    msg = aio_pika.Message(body=json.dumps(order).encode())
    await ex.publish(msg, routing_key="order.created")
    await conn.close()
    return {"event": "order.created", "order": order}

@app.post("/payment/success")
async def payment_success(payment: dict):
    """
    Publish a 'payment.success' event.
    """
    conn, ch, ex = await get_exchange()
    msg = aio_pika.Message(body=json.dumps(payment).encode())
    await ex.publish(msg, routing_key="payment.success")
    await conn.close()
    return {"event": "payment.success", "payment": payment}