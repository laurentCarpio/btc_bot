from __future__ import annotations

import asyncio
import json
import logging
from typing import Callable, Optional

import websockets


BINANCE_WS_URL = "wss://fstream.binance.com/stream"


class BinanceWsPublicClient:
    def __init__(
        self,
        symbol: str,
        on_book: Optional[Callable] = None,
        on_trade: Optional[Callable] = None,
        on_candle: Optional[Callable] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.symbol = symbol.lower()
        self.on_book = on_book
        self.on_trade = on_trade
        self.on_candle = on_candle
        self.logger = logger or logging.getLogger(__name__)

        self.ws = None
        self.listen_task: Optional[asyncio.Task] = None
        self._stop = False
        self.connected_event = asyncio.Event()

    async def connect(self):
        self._stop = False
        self.connected_event.clear()
        self.listen_task = asyncio.create_task(self._run())
        await self.connected_event.wait()

    async def close(self):
        self._stop = True
        self.connected_event.clear()

        if self.ws:
            await self.ws.close()

        if self.listen_task:
            self.listen_task.cancel()

    async def _run(self):
        stream = (
            f"{self.symbol}@depth20@100ms/"
            f"{self.symbol}@aggTrade/"
            f"{self.symbol}@kline_1m"
        )
        url = f"{BINANCE_WS_URL}?streams={stream}"

        while not self._stop:
            try:
                self.logger.info(f"[BinanceWS] connecting to {url}")

                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=10,
                ) as ws:
                    self.ws = ws
                    self.connected_event.set()
                    self.logger.info("[BinanceWS] connected")

                    async for msg in ws:
                        await self._handle_message(msg)

            except Exception as e:
                self.connected_event.clear()
                self.logger.warning(f"[BinanceWS] error: {e} → reconnecting in 3s")
                await asyncio.sleep(3)

    async def _handle_message(self, raw: str):
        try:
            data = json.loads(raw)
        except Exception:
            return

        if "stream" not in data:
            return

        stream = data["stream"]
        payload = data["data"]

        if "@depth" in stream:
            book = {
                "bids": payload.get("b", []),
                "asks": payload.get("a", []),
                "timestamp": payload.get("E"),
            }

            if self.on_book:
                await self._safe_call(self.on_book, book)

        elif "@aggTrade" in stream:
            trade = {
                "price": float(payload["p"]),
                "qty": float(payload["q"]),
                "side": "buy" if not payload["m"] else "sell",
                "timestamp": payload["T"],
            }

            if self.on_trade:
                await self._safe_call(self.on_trade, trade)

        elif "@kline_1m" in stream:
            k = payload.get("k", {})
            if not k:
                return

            candle = {
                "timestamp": payload.get("E"),
                "open_time": k.get("t"),
                "close_time": k.get("T"),
                "open": float(k["o"]),
                "high": float(k["h"]),
                "low": float(k["l"]),
                "close": float(k["c"]),
                "volume": float(k["v"]),
                "is_closed": bool(k.get("x", False)),
            }

            if self.on_candle:
                await self._safe_call(self.on_candle, candle)

    async def _safe_call(self, cb, data):
        try:
            if asyncio.iscoroutinefunction(cb):
                await cb(data)
            else:
                cb(data)
        except Exception as e:
            self.logger.error(f"[BinanceWS] callback error: {e}")