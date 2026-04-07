from __future__ import annotations

import logging
from btc_bot.live.binance.rest_market_data import fetch_recent_klines_1m
from btc_bot.live.logging.trade_stats_tracker import TradeStatsTracker

class BinanceLiveSocketBridge:
    def __init__(
        self,
        ws_pub_client,
        live_orchestrator,
        logger: logging.Logger,
    ):
        self.ws_pub_client = ws_pub_client
        self.live_orchestrator = live_orchestrator
        self.logger = logger

        self.ws_pub_client.on_book = self._on_book
        self.ws_pub_client.on_trade = self._on_trade
        self.ws_pub_client.on_candle = self._on_candle

        self.trade_stats = TradeStatsTracker(window_s=2.0)
        self._spread_stats = {
            "n": 0,
            "sum": 0.0,
            "min": float("inf"),
            "max": float("-inf"),
            "crossed": 0,
        }

    async def connect(self):
        await self.warmup_recent_1m_klines(limit=300)
        await self.ws_pub_client.connect()
        self.logger.info("[BinanceLiveSocketBridge] ✅ Public WS connected and subscribed")

    async def close(self):
        await self.ws_pub_client.close()
        self.logger.info("[BinanceLiveSocketBridge] closed")

    async def warmup_recent_1m_klines(self, limit: int = 300):
        klines = fetch_recent_klines_1m(
            symbol=self.ws_pub_client.symbol.upper(),
            limit=limit,
        )

        self.logger.info(
            f"[BinanceLiveSocketBridge] warmup start symbol={self.ws_pub_client.symbol.upper()} n_klines={len(klines)}"
        )

        for k in klines:
            await self.live_orchestrator.on_candle_1m_update(
                symbol=self.ws_pub_client.symbol.upper(),
                ts_ms=int(k["close_time"]),
                open_=float(k["open"]),
                high=float(k["high"]),
                low=float(k["low"]),
                close=float(k["close"]),
                volume=float(k["volume"]),
            )

        self.logger.info(
            f"[BinanceLiveSocketBridge] warmup done symbol={self.ws_pub_client.symbol.upper()}"
        )

    def flush_spread_stats(self):
        s = self._spread_stats
        if s["n"] <= 0 and s["crossed"] <= 0:
            return

        avg = (s["sum"] / s["n"]) if s["n"] > 0 else float("nan")

        self.logger.info(
            f"[BINANCE_SPREAD] "
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
    
    def flush_trade_stats(self):
        s = self.trade_stats.snapshot()
        self.logger.info(
            f"[BINANCE_TRADE_STATS] "
            f"ntr={s['ntr']} "
            f"nps={s['nps']:.2f} "
            f"ti_abs={s['ti_abs']:.6f}"
        )
        
    async def _on_book(self, book: dict):
        try:
            bids = book.get("bids", [])
            asks = book.get("asks", [])
            ts_ms = int(book["timestamp"])

            if not bids or not asks:
                return

            if not hasattr(self, "_seen_first_book"):
                self._seen_first_book = True
                self.logger.info("[BinanceLiveSocketBridge] first book received")

            norm_bids = [(float(px), float(sz)) for px, sz in bids[:16] if float(sz) > 0.0]
            norm_asks = [(float(px), float(sz)) for px, sz in asks[:16] if float(sz) > 0.0]

            norm_bids.sort(key=lambda x: x[0], reverse=True)
            norm_asks.sort(key=lambda x: x[0])

            if not norm_bids or not norm_asks:
                return

            best_bid = norm_bids[0][0]
            best_ask = norm_asks[0][0]

            if best_bid >= best_ask:
                self._spread_stats["crossed"] += 1
                self.logger.debug(
                    f"[BinanceLiveSocketBridge] dropped crossed/invalid book "
                    f"best_bid={best_bid} best_ask={best_ask}"
                )
                return

            spread = best_ask - best_bid

            s = self._spread_stats
            s["n"] += 1
            s["sum"] += spread
            s["min"] = min(s["min"], spread)
            s["max"] = max(s["max"], spread)

            await self.live_orchestrator.on_book_update(
                symbol=self.ws_pub_client.symbol.upper(),
                ts_ms=ts_ms,
                bids=norm_bids,
                asks=norm_asks,
                action="update",
            )

        except Exception as e:
            self.logger.exception(f"[BinanceLiveSocketBridge] book handler error: {e}")

    async def _on_trade(self, trade: dict):
        try:
            ts_ms = int(trade["timestamp"])
            price = float(trade["price"])
            qty = float(trade["qty"])
            side = str(trade["side"]).lower()

            if not hasattr(self, "_seen_first_trade"):
                self._seen_first_trade = True
                self.logger.info("[BinanceLiveSocketBridge] first trade received")

            self.trade_stats.add_trade(
                ts_ms=ts_ms,
                qty=qty,
                side=side,
            )
            
            await self.live_orchestrator.on_trade_update(
                symbol=self.ws_pub_client.symbol.upper(),
                ts_ms=ts_ms,
                price=price,
                size=qty,
                side=side,
            )

        except Exception as e:
            self.logger.exception(f"[BinanceLiveSocketBridge] trade handler error: {e}")

    async def _on_candle(self, candle: dict):
        try:
            if not candle.get("is_closed", False):
                return

            if not hasattr(self, "_seen_first_candle"):
                self._seen_first_candle = True
                self.logger.info("[BinanceLiveSocketBridge] first closed 1m candle received")

            self.logger.debug(
                "[BinanceLiveSocketBridge] closed 1m candle "
                f"ts_ms={int(candle['close_time'])} "
                f"open={float(candle['open'])} "
                f"high={float(candle['high'])} "
                f"low={float(candle['low'])} "
                f"close={float(candle['close'])} "
                f"volume={float(candle['volume'])}"
            )
            await self.live_orchestrator.on_candle_1m_update(
                symbol=self.ws_pub_client.symbol.upper(),
                ts_ms=int(candle["close_time"]),
                open_=float(candle["open"]),
                high=float(candle["high"]),
                low=float(candle["low"]),
                close=float(candle["close"]),
                volume=float(candle["volume"]),
            )

        except Exception as e:
            self.logger.exception(f"[BinanceLiveSocketBridge] candle handler error: {e}")