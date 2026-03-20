from __future__ import annotations

from datetime import timedelta

from btc_bot.live.config import ExitConfig
from btc_bot.live.models import ExitDecision, OpenTrade
from btc_bot.live.trade.pnl import compute_pnl_bps


class TradeLifecycleManager:
    def __init__(self, cfg: ExitConfig) -> None:
        self.cfg = cfg

    def evaluate(self, trade: OpenTrade, now_ts, current_price: float) -> ExitDecision:
        age_s = (now_ts - trade.entry_time).total_seconds()
        pnl_bps = compute_pnl_bps(
            entry_price=trade.entry_price,
            current_price=current_price,
            side=trade.side,
        )

        if age_s >= self.cfg.hard_exit_at_s:
            return ExitDecision(
                should_exit=True,
                reason=f"hard_exit_{self.cfg.hard_exit_at_s}s",
                timestamp=now_ts,
                pnl_bps=pnl_bps,
            )

        if age_s >= self.cfg.check_negative_at_s and pnl_bps < 0:
            return ExitDecision(
                should_exit=True,
                reason=f"negative_at_{self.cfg.check_negative_at_s}s",
                timestamp=now_ts,
                pnl_bps=pnl_bps,
            )

        return ExitDecision(
            should_exit=False,
            reason="hold",
            timestamp=now_ts,
            pnl_bps=pnl_bps,
        )