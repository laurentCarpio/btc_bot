from collections import deque

class TradeStatsTracker:
    def __init__(self, window_s: float = 2.0):
        self.window_s = float(window_s)
        self.trades = deque()

    def add_trade(self, ts_ms: int, qty: float, side: str):
        ts_s = ts_ms / 1000.0
        self.trades.append((ts_s, float(qty), str(side).lower()))
        self._trim(ts_s)

    def _trim(self, now_s: float):
        cutoff = now_s - self.window_s
        while self.trades and self.trades[0][0] < cutoff:
            self.trades.popleft()

    def snapshot(self):
        if not self.trades:
            return {
                "ntr": 0,
                "nps": 0.0,
                "ti": 0.0,
                "ti_abs": 0.0,
            }

        buy_qty = 0.0
        sell_qty = 0.0

        for _, qty, side in self.trades:
            if side == "buy":
                buy_qty += qty
            elif side == "sell":
                sell_qty += qty

        total_qty = buy_qty + sell_qty
        ti = 0.0 if total_qty <= 0 else (buy_qty - sell_qty) / total_qty

        ntr = len(self.trades)
        return {
            "ntr": ntr,
            "nps": ntr / self.window_s,
            "ti": ti,
            "ti_abs": abs(ti),
        }