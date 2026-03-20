from __future__ import annotations

from btc_bot.live.models import MLDecision


class SimpleSizer:
    def size_from_ml(self, ml_decision: MLDecision) -> float:
        if not ml_decision.enabled:
            return 1.0
        if not ml_decision.accepted:
            return 0.0
        return 1.0