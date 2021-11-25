#!/usr/bin/env python

import asyncio
import itertools
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

    def next_order_id(self, source):
        """fetch next available order id"""
        tmp = self._get_source(source)
        return tmp.get("max_order_id", 1000) + 1

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

    async def _send(self, request):
        """send a raw message"""
        message = json.dumps(request)
        print(message)
        await self.websocket.send(message)

    async def _receive(self):
        """receive a raw message"""
        message = await self.websocket.recv()
        return json.loads(message)

    async def logon(self, username, password):
        """logon"""
        request = ["logon", dict(username=username, password=password)]
        await self._send(request)
        [response_type, response] = await self._receive()
        if response_type != "logon":
            raise RuntimeError(f"LOGON FAILED: wrong response type ({response_type})")
        if not response.get("success", False):
            reason = response.get("reason", "")
            raise RuntimeError(f"LOGON FAILED: reason={reason}")
        result = Sources(response.get("sources"))
        return result

    async def subscribe(self, source, channel, exchange, symbol):
        """subscribe"""
        request = [
            "subscribe",
            dict(
                channel=channel,
                exchange=exchange,
                symbol=symbol,
            ),
            source,
            123,
        ]
        await self._send(request)

    async def create_order(self, source, obj):
        """create order"""
        request = [
            "create_order",
            obj,
            source,
            123,
        ]
        await self._send(request)

    async def modify_order(self, source, obj):
        """modify order"""
        request = [
            "modify_order",
            obj,
            source,
            123,
        ]
        await self._send(request)

    async def cancel_order(self, source, obj):
        """cancel order"""
        request = [
            "cancel_order",
            obj,
            source,
            123,
        ]
        await self._send(request)

    async def next_update(self):
        """next update"""
        update = await self._receive()
        # https://stackoverflow.com/a/59141795
        return itertools.islice(itertools.chain(update, itertools.repeat(None)), 5)


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


async def runner(
    uri,
    username,
    password,
    gateway,
    exchange,
    symbol,
    currency,
    trading,
    log_market_data,
):
    """async main"""
    async with Client(uri) as client:
        try:
            latch = False
            order_id = None
            countdown = None

            market_data = {
                "reference_data",
                "market_status",
                "top_of_book",
                "market_by_price",
                "market_by_order",
                "trade_summary",
                "statistics",
            }
            account_management = {
                "funds",
                "position",
            }
            order_management = {
                "order_ack",
                "order",
                "trade",
                "create_order",
                "modify_order",
                "cancel-order",
            }

            subscriptions = Subscriptions(
                {
                    gateway: {
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
                    type_,
                    obj,
                    source,
                    timestamp,
                    request_id,
                ) = await client.next_update()

                is_market_data = type_ in market_data
                is_account_management = type_ in account_management
                is_order_management = type_ in order_management

                is_unexpected = not (
                    is_market_data or is_account_management or is_order_management
                )

                if (
                    is_account_management
                    or is_order_management
                    or is_unexpected
                    or (is_market_data and log_market_data)
                ):
                    print(
                        f"type={type_}, obj={obj}, source={source}, timestamp={timestamp}, request_id={request_id}"
                    )

                if type_ == "state":
                    sources.update(source, obj)
                    await subscriptions.subscribe(client, sources)

                if type_ == "top_of_book" and trading:

                    if not latch:
                        print(obj)
                        latch = True
                        countdown = 20
                        order_id = sources.next_order_id(source)
                        create_order = dict(
                            order_id=order_id,
                            exchange=exchange,
                            symbol=symbol,
                            side="BUY",
                            order_type="LIMIT",
                            time_in_force="GTC",
                            quantity=1,
                            price=obj["layer"][0] - 500,
                        )
                        await client.create_order(source, create_order)

                    if latch:
                        assert countdown is not None
                        if countdown > 0:
                            countdown = countdown - 1

                            if countdown == 10:
                                assert order_id is not None
                                modify_order = dict(
                                    order_id=order_id,
                                    price=obj["layer"][0] - 750,
                                )
                                await client.modify_order(source, modify_order)

                            if countdown == 0:
                                assert order_id is not None
                                cancel_order = dict(
                                    order_id=order_id,
                                )
                                await client.cancel_order(source, cancel_order)

        except websockets.ConnectionClosedOK:
            print("closed ok")
        except websockets.ConnectionClosedError:
            print("closed error")
        except websockets.ConnectionClosed:
            print("closed")
        print("done")


def main(
    uri,
    username,
    password,
    gateway,
    exchange,
    symbol,
    currency,
    trading,
    log_market_data,
):
    asyncio.run(
        runner(
            uri,
            username,
            password,
            gateway,
            exchange,
            symbol,
            currency,
            trading,
            log_market_data,
        )
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", default="ws://localhost:3456")
    parser.add_argument("--username", default="tbom1")
    parser.add_argument("--password", default="")
    parser.add_argument("--gateway", default="deribit")
    parser.add_argument("--exchange", default="deribit")
    parser.add_argument("--symbol", default="BTC-PERPETUAL")
    parser.add_argument("--currency", default="USDT")
    parser.add_argument("--trading", default=False)
    parser.add_argument("--log_market_data", default=False)
    results = parser.parse_args()
    main(
        results.uri,
        results.username,
        results.password,
        results.gateway,
        results.exchange,
        results.symbol,
        results.currency,
        results.trading,
        results.log_market_data,
    )
