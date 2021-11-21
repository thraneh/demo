#!/usr/bin/env python

import asyncio
import websockets
import json


class Client:
    def __init__(self, uri):
        self._uri = uri

    # https://stackoverflow.com/a/42014617
    async def __aenter__(self):
        self._conn = websockets.connect(self._uri)
        self.websocket = await self._conn.__aenter__()
        return self

    async def __aexit__(self, *args, **kwargs):
        await self._conn.__aexit__(*args, **kwargs)

    async def send(self, message):
        await self.websocket.send(message)

    async def receive(self):
        return await self.websocket.recv()

    async def send_request(self, request_id, request_type, request):
        message = json.dumps([request_id, request_type, request])
        await self.send(message)

    async def get_response(self):
        message = await self.receive()
        print(message)
        response = json.loads(message)
        return response if len(response) == 3 else response + [0]

    async def logon(self, username, password):
        await self.send_request(
            123, "logon", dict(username=username, password=password)
        )
        [obj_type, response, request_id] = await self.get_response()
        if response.get("success", False):
            print("LOGON SUCCEEDED !!! account={}".format(response.get("account", "")))
        else:
            raise RuntimeError(
                "LOGON FAILED: reason={}".format(response.get("reason", ""))
            )

    async def subscribe(self, channel, exchange, symbol):
        await self.send_request(
            123,
            "subscribe",
            dict(
                channel=channel,
                exchange=exchange,
                symbol=symbol,
            ),
        )


async def runner(uri):
    async with Client(uri) as client:
        try:
            username = "tbom1"
            password = ""
            exchange = "deribit"
            symbol = "BTC-PERPETUAL"
            currency = 'USDT'
            await client.logon(username, password)
            await client.subscribe("reference_data", exchange, symbol)
            await client.subscribe("market_status", exchange, symbol)
            await client.subscribe("top_of_book", exchange, symbol)
            await client.subscribe("market_by_price", exchange, symbol)
            await client.subscribe("market_by_order", exchange, symbol)
            await client.subscribe("trade_summary", exchange, symbol)
            await client.subscribe("statistics", exchange, symbol)
            await client.subscribe("funds", '', currency)
            await client.subscribe("position", exchange, symbol)
            while True:
                [obj_type, response, request_id] = await client.get_response()
                print("obj_type={}, obj={}".format(obj_type, response))
        except websockets.ConnectionClosedOK:
            print("closed ok")
        except websockets.ConnectionClosedError:
            print("closed error")
        except websockets.ConnectionClosed:
            print("closed")
        print("done")


asyncio.run(runner("ws://localhost:3456"))
