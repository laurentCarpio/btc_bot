from __future__ import annotations

from btc_bot.live.bitget.ws_msg_dispatcher import WsMessageDispatcher

class BitgetLiveSocketBridge:
    """
    Bridge between Bitget public WS messages and the live orchestrator.

    Responsibilities:
    - register handlers in WsMessageDispatcher
    - parse Bitget payloads for books / trade / candle1m
    - forward normalized events to LiveOrchestrator
    """

    def __init__(self, ws_pub_client, live_orchestrator, logger):
        self.logger = logger
        self.live_orchestrator = live_orchestrator

        self.dispatcher = WsMessageDispatcher(logger=logger)
        self.dispatcher.register_handler("books", self.on_book_message)
        self.dispatcher.register_handler("trade", self.on_trade_message)
        self.dispatcher.register_handler("candle1m", self.on_candle_1m_message)

        self.ws_pub_client = ws_pub_client.use_dispatcher(self.dispatcher)

    async def connect(self):
        await self.ws_pub_client.connect()
        self.logger.info("[BitgetLiveSocketBridge] ✅ Public WS connected and subscribed")

    async def on_book_message(self, message: dict):
        try:
            if "event" in message:
                return

            arg = message.get("arg", {})
            data = message.get("data", [])
            action = message.get("action", "unknown")
            symbol = arg.get("instId")

            if not symbol or not data:
                self.logger.warning("[books] ⚠️ Missing symbol or data")
                return

            entry = data[0]
            bids_raw = entry.get("bids", [])
            asks_raw = entry.get("asks", [])
            ts = int(entry.get("ts", message.get("ts", 0)))

            bids = [(float(p), float(s)) for p, s in bids_raw]
            asks = [(float(p), float(s)) for p, s in asks_raw]

            if not bids or not asks:
                return

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
            
            self.logger.debug(f"[trade] recv symbol={symbol} n_rows={len(data)} sample={data[0]}")

            for row in data:
                price = float(row.get("price", 0.0))
                size = float(row.get("size", 0.0))
                side = str(row.get("side", "")).lower()
                ts = int(row.get("ts", message.get("ts", 0)))

                self.logger.debug(f"[trade] parsed symbol={symbol} ts={ts} side={side} price={price} size={size}")
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

                # Bitget payload:
                # [ts, open, high, low, close, baseVol, quoteVol, usdtVol?]
                ts = int(row[0])
                open_ = float(row[1])
                high = float(row[2])
                low = float(row[3])
                close = float(row[4])
                volume = float(row[5])
                if close <= 0 or high < low :
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