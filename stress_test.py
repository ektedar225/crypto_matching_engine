import asyncio
import aiohttp
import random
import time

URL = "http://127.0.0.1:8000/order"
SYMBOL = "BTC-USDT"
NUM_ORDERS = 1000 # number of orders to send
MAX_PRICE = 52000
MIN_PRICE = 48000
MAX_QTY = 5

async def send_order(session, order_type, side, quantity, price=None):
    data = {
        "symbol": SYMBOL,
        "order_type": order_type,
        "side": side,
        "quantity": quantity,
    }
    if order_type == "limit":
        data["price"] = price

    async with session.post(URL, json=data) as resp:
        response = await resp.json()
        print(response)

async def main():
    async with aiohttp.ClientSession() as session:
        tasks = []
        for _ in range(NUM_ORDERS):
            side = random.choice(["buy", "sell"])
            order_type = "limit"
            quantity = round(random.uniform(0.1, MAX_QTY), 2)
            price = round(random.uniform(MIN_PRICE, MAX_PRICE), 2)
            tasks.append(send_order(session, order_type, side, quantity, price))

        start = time.time()
        await asyncio.gather(*tasks)
        end = time.time()
        print(f"Sent {NUM_ORDERS} orders in {end - start:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(main())
