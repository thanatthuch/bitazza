import asyncio
import json
import websockets
from google.auth import default
from google.cloud import pubsub_v1


credentials, _ = default()
publisher = pubsub_v1.PublisherClient(credentials=credentials)

project_id = "exemplary-proxy-415101"
topic_name = "StreamingBinanceTopic"

# new key name mapping
key_mapping = {
    "e": "event",
    "E": "event_timestamp",
    "s": "symbol",
    "t": "trade_timestamp",
    "p": "price",
    "q": "quality",
    "b": "bid",
    "a": "ark",
    "T": "trade",
    "m": "maker",
    "M": "taker"
}

async def receive_messages(symbol, client):
    async for message in client:
        message = json.loads(message)
        updated_message = {key_mapping.get(key, key): value for key, value in message.items()}
        data_json = json.dumps(updated_message).encode("utf-8")
        topic_path = publisher.topic_path(project_id, topic_name)

        future = publisher.publish(topic_path, data=data_json)
        print(data_json)


async def connect_to_websockets(symbols):
    async def create_websocket(symbol):
        endpoint = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@trade"
        client = await websockets.connect(endpoint)
        return symbol, client

    clients = await asyncio.gather(*(create_websocket(symbol) for symbol in symbols))

    tasks = [receive_messages(symbol, client) for symbol, client in clients]
    await asyncio.gather(*tasks)


async def main():
    symbols = ["BTCUSDT", "AAVEUSDT", "STXUSDT", "ARBUSDT"]
    await connect_to_websockets(symbols)


asyncio.run(main())
