from __future__ import annotations

from btc_bot.live.models import MLDecision


class SimpleSizer:
    def size_from_ml(self, ml_decision: MLDecision, vol_regime3: str) -> float:
        # --------------------------------------------------
        # NO ML outside LOW
        # --------------------------------------------------
        if vol_regime3 != "low":
            return 1.0

        # --------------------------------------------------
        # Safety checks
        # --------------------------------------------------
        if not ml_decision.enabled or ml_decision.score is None:
            return 0.0

        score = float(ml_decision.score)

        # --------------------------------------------------
        # Threshold ladder (VAL calibrated)
        # --------------------------------------------------
        if score >= 0.77:
            return 1.25

        elif score >= 0.75:
            return 1.10

        elif score >= 0.72:
            return 0.95

        elif score >= 0.70:
            return 0.80

        else:
            return 0.0