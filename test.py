#!/usr/bin/env python

import asyncio
import json

from collections import defaultdict

import websockets


class Sources:
    """manage state by source"""

    def __init__(self, sources=None):
        """construct"""
        self._sources = {}
        self._update = dict(
            connected=self._update_connected,
            max_order_id=self._update_max_order_id,
            markets=self._update_markets,
        )
        if sources is not None:
            for source, obj in sources.items():
                self.update(source, obj)

    def update(self, source, obj):
        """incremental update"""
        tmp = self._get_source(source)
        for key, value in obj.items():
            self._update[key](tmp, value)

    def has(self, source, exchange, symbol):
        """test if source has market"""
        tmp = self._get_source(source)
        if tmp is not None:
            markets = tmp.get("markets")
            if markets is not None:
                symbols = markets.get(exchange)
                if symbols is not None:
                    return symbol in symbols
        return False

    def _get_source(self, source):
        result = self._sources.get(source)
        if result is None:
            self._sources[source] = {}
            result = self._sources[source]
        return result

    @classmethod
    def _update_connected(cls, source, value):
        """update connected"""
        assert isinstance(value, bool)
        source["connected"] = value

    @classmethod
    def _update_max_order_id(cls, source, value):
        """update max_order_id"""
        assert isinstance(value, int)
        source["max_order_id"] = value

    @classmethod
    def _update_markets(cls, source, value):
        """update markets"""
        markets = source.get("markets")
        if markets is None:
            source["markets"] = defaultdict(set)
            markets = source["markets"]
        for exchange, symbol in value:
            markets[exchange].add(symbol)


class Client:
    """manage websocket connection and client protocol"""

    def __init__(self, uri):
        self._uri = uri
        self._conn = None
        self.websocket = None

    # https://stackoverflow.com/a/42014617
    async def __aenter__(self):
        self._conn = websockets.connect(self._uri)
        self.websocket = await self._conn.__aenter__()
        return self

    async def __aexit__(self, *args, **kwargs):
        await self._conn.__aexit__(*args, **kwargs)

    async def _send(self, message):
        """send a raw message"""
        await self.websocket.send(message)

    async def _receive(self):
        """receive a raw message"""
        return await self.websocket.recv()

    async def _request(self, request_id, request_type, request):
        """encode request and send message"""
        message = json.dumps([request_id, request_type, request])
        await self._send(message)

    async def logon(self, username, password):
        """logon"""
        await self._request(123, "logon", dict(username=username, password=password))
        message = await self._receive()
        print(message)
        [obj_type, response, request_id] = json.loads(message)
        if not response.get("success", False):
            reason = response.get("reason", "")
            raise RuntimeError(f"LOGON FAILED: reason={reason}")
        result = Sources(response.get("sources"))
        return result

    async def subscribe(self, source, channel, exchange, symbol):
        """subscribe"""
        await self._request(
            123,
            "subscribe",
            dict(
                source=source,
                channel=channel,
                exchange=exchange,
                symbol=symbol,
            ),
        )

    async def next_update(self):
        """next update"""
        message = await self._receive()
        result = json.loads(message)
        return result if len(result) == 5 else result + [0]


class Subscriptions:
    """manage subscriptions"""

    def __init__(self, obj):
        """construct"""
        self._data = obj
        self._subscribed = defaultdict(set)

    async def subscribe(self, client, sources):
        """subscribe when markets become available"""
        remove = []
        for source, tmp in self._data.items():
            for (exchange, symbol), channels in tmp.items():
                if sources.has(source, exchange, symbol):
                    for channel in channels:
                        await client.subscribe(source, channel, exchange, symbol)
                    remove.append((source, exchange, symbol))
        for source, exchange, symbol in remove:
            tmp = self._data[source]
            del tmp[(exchange, symbol)]
            if len(tmp) == 0:
                del self._data[source]


async def runner(uri):
    """main"""
    async with Client(uri) as client:
        try:
            username = "tbom1"
            password = ""
            source = "deribit"
            exchange = "deribit"
            symbol = "BTC-PERPETUAL"
            currency = "USDT"

            subscriptions = Subscriptions(
                {
                    source: {
                        ("", currency): {
                            "funds",
                        },
                        (exchange, symbol): {
                            "reference_data",
                            "market_status",
                            "top_of_book",
                            "market_by_price",
                            "market_by_order",
                            "trade_summary",
                            "statistics",
                            "position",
                            "order",
                            "trade",
                        },
                    },
                }
            )

            sources = await client.logon(username, password)
            await subscriptions.subscribe(client, sources)

            while True:
                (
                    source,
                    create_time,
                    obj_type,
                    obj,
                    request_id,
                ) = await client.next_update()
                print(
                    f"source={source}, create_time={create_time}, obj_type={obj_type}, obj={obj}, request_id={request_id}"
                )

                if obj_type == "state":
                    sources.update(source, obj)
                    await subscriptions.subscribe(client, sources)

        except websockets.ConnectionClosedOK:
            print("closed ok")
        except websockets.ConnectionClosedError:
            print("closed error")
        except websockets.ConnectionClosed:
            print("closed")
        print("done")


asyncio.run(runner("ws://localhost:3456"))
