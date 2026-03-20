from __future__ import annotations

import uuid

from btc_bot.live.models import EntryDecision, OpenTrade


class PaperExecutor:
    def open_trade(self, decision: EntryDecision, entry_price: float):
        trade_id = f"paper_{uuid.uuid4().hex[:12]}"
        return OpenTrade(
            trade_id=trade_id,
            symbol=decision.symbol,
            entry_time=decision.timestamp,
            entry_price=float(entry_price),
            side=int(decision.side),
            router_branch=decision.router_branch,
            score_ml=decision.score,
            size_mult=float(decision.size_mult),
        )

    def close_trade(self, trade: OpenTrade, exit_price: float, exit_time):
        trade.status = "CLOSED"
        trade.metadata["exit_price"] = float(exit_price)
        trade.metadata["exit_time"] = exit_time
        return trade