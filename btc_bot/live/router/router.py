from __future__ import annotations

from btc_bot.live.models import RegimeSnapshot, RouterDecision


class BranchRouter:
    def __init__(self, mr_bucket: str = "b2") -> None:
        self.mr_bucket = mr_bucket

    def decide(self, dir0: int, regime: RegimeSnapshot) -> RouterDecision:
        branch = "MR" if regime.vol_bucket == self.mr_bucket else "BO"
        trade_dir = -int(dir0) if branch == "MR" else int(dir0)
        return RouterDecision(router_branch=branch, trade_dir=trade_dir)