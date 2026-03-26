from __future__ import annotations

from btc_bot.live.bitget.ws_msg_dispatcher import WsMessageDispatcher


class BitgetLiveSocketBridge:
    """
    Bridge between Bitget public WS messages and the live orchestrator.
    Maintains a local order book because Bitget books channel sends:
      - initial snapshot
      - then incremental updates
    """

    def __init__(self, ws_pub_client, live_orchestrator, logger):
        self.logger = logger
        self.live_orchestrator = live_orchestrator

        self.dispatcher = WsMessageDispatcher(logger=logger)
        self.dispatcher.register_handler("books", self.on_book_message)
        self.dispatcher.register_handler("trade", self.on_trade_message)
        self.dispatcher.register_handler("candle1m", self.on_candle_1m_message)

        self.ws_pub_client = ws_pub_client.use_dispatcher(self.dispatcher)

        self._spread_stats = {
            "n": 0,
            "sum": 0.0,
            "min": float("inf"),
            "max": float("-inf"),
            "crossed": 0,
        }

        # local books per symbol
        self._books: dict[str, dict[str, dict[float, float]]] = {}

    async def connect(self):
        await self.ws_pub_client.connect()
        self.logger.info("[BitgetLiveSocketBridge] ✅ Public WS connected and subscribed")

    def flush_spread_stats(self):
        s = self._spread_stats
        if s["n"] <= 0 and s["crossed"] <= 0:
            return

        avg = (s["sum"] / s["n"]) if s["n"] > 0 else float("nan")

        self.logger.info(
            f"[BITGET_SPREAD] "
            f"n={s['n']} "
            f"avg={avg:.6f} "
            f"min={s['min']:.6f} "
            f"max={s['max']:.6f} "
            f"crossed={s['crossed']}"
        )

        self._spread_stats = {
            "n": 0,
            "sum": 0.0,
            "min": float("inf"),
            "max": float("-inf"),
            "crossed": 0,
        }

    def _get_or_create_book(self, symbol: str) -> dict[str, dict[float, float]]:
        if symbol not in self._books:
            self._books[symbol] = {
                "bids": {},
                "asks": {},
            }
        return self._books[symbol]

    @staticmethod
    def _apply_side_updates(side_book: dict[float, float], updates_raw: list):
        """
        Bitget rules:
        - same price + size == 0 => delete
        - same price + size != 0 => replace
        - price absent + size != 0 => insert
        """
        for row in updates_raw:
            if len(row) < 2:
                continue
            px = float(row[0])
            sz = float(row[1])

            if px <= 0:
                continue

            if sz <= 0.0:
                side_book.pop(px, None)
            else:
                side_book[px] = sz

    def _build_top_levels(self, symbol: str, depth: int = 16):
        book = self._get_or_create_book(symbol)

        bids = sorted(book["bids"].items(), key=lambda x: x[0], reverse=True)[:depth]
        asks = sorted(book["asks"].items(), key=lambda x: x[0])[:depth]

        return bids, asks

    async def on_book_message(self, message: dict):
        try:
            if "event" in message:
                return

            arg = message.get("arg", {})
            data = message.get("data", [])
            action = str(message.get("action", "unknown")).lower()
            symbol = arg.get("instId")

            if not symbol or not data:
                self.logger.warning("[books] ⚠️ Missing symbol or data")
                return

            entry = data[0]
            bids_raw = entry.get("bids", [])
            asks_raw = entry.get("asks", [])
            ts = int(entry.get("ts", message.get("ts", 0)))

            local_book = self._get_or_create_book(symbol)

            if action == "snapshot":
                local_book["bids"].clear()
                local_book["asks"].clear()

            self._apply_side_updates(local_book["bids"], bids_raw)
            self._apply_side_updates(local_book["asks"], asks_raw)

            bids, asks = self._build_top_levels(symbol, depth=16)

            if not bids or not asks:
                return

            best_bid = bids[0][0]
            best_ask = asks[0][0]

            if best_bid >= best_ask:
                self._spread_stats["crossed"] += 1
                self.logger.debug(
                    f"[books] dropped crossed/invalid book symbol={symbol} "
                    f"action={action} best_bid={best_bid} best_ask={best_ask}"
                )
                return

            spread = best_ask - best_bid
            s = self._spread_stats
            s["n"] += 1
            s["sum"] += spread
            s["min"] = min(s["min"], spread)
            s["max"] = max(s["max"], spread)

            await self.live_orchestrator.on_book_update(
                symbol=symbol,
                ts_ms=ts,
                bids=bids,
                asks=asks,
                action=action,
            )

        except Exception as e:
            self.logger.exception(f"[books] ❌ Bridge error: {e}")

    async def on_trade_message(self, message: dict):
        try:
            if "event" in message:
                return

            arg = message.get("arg", {})
            data = message.get("data", [])
            symbol = arg.get("instId")

            if not symbol or not data:
                self.logger.warning("[trade] ⚠️ Missing symbol or data")
                return

            for row in data:
                price = float(row.get("price", 0.0))
                size = float(row.get("size", 0.0))
                side = str(row.get("side", "")).lower()
                ts = int(row.get("ts", message.get("ts", 0)))

                if price <= 0 or size <= 0 or side not in ("buy", "sell"):
                    continue

                await self.live_orchestrator.on_trade_update(
                    symbol=symbol,
                    ts_ms=ts,
                    price=price,
                    size=size,
                    side=side,
                )

        except Exception as e:
            self.logger.exception(f"[trade] ❌ Bridge error: {e}")

    async def on_candle_1m_message(self, message: dict):
        try:
            if "event" in message:
                return

            arg = message.get("arg", {})
            data = message.get("data", [])
            symbol = arg.get("instId")

            if not symbol or not data:
                self.logger.warning("[candle1m] ⚠️ Missing symbol or data")
                return

            for row in data:
                if not isinstance(row, list) or len(row) < 6:
                    self.logger.warning(f"[candle1m] Unexpected row format: {row}")
                    continue

                ts = int(row[0])
                open_ = float(row[1])
                high = float(row[2])
                low = float(row[3])
                close = float(row[4])
                volume = float(row[5])

                if close <= 0 or high < low:
                    continue

                await self.live_orchestrator.on_candle_1m_update(
                    symbol=symbol,
                    ts_ms=ts,
                    open_=open_,
                    high=high,
                    low=low,
                    close=close,
                    volume=volume,
                )

        except Exception as e:
            self.logger.exception(f"[candle1m] ❌ Bridge error: {e}")