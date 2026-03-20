from __future__ import annotations

from typing import Dict

import pandas as pd

from btc_bot.live.ml.artifacts import LoadedModelArtifact
from btc_bot.live.models import MLDecision


class MLInferenceService:
    def __init__(self, artifact: LoadedModelArtifact | None) -> None:
        self.artifact = artifact

    def score(self, feature_map: Dict[str, float]) -> MLDecision:
        if self.artifact is None:
            return MLDecision(
                enabled=False,
                score=None,
                accepted=True,
                threshold=None,
            )

        row = {c: float(feature_map.get(c, 0.0)) for c in self.artifact.feature_cols}

        print("\n[ml_input_ordered]")
        for c in self.artifact.feature_cols:
            print(f"  {c}: {row[c]}")

        x = pd.DataFrame([row], columns=self.artifact.feature_cols)

        proba = self.artifact.model.predict_proba(x)
        score = float(proba[0][1])

        print(f"[ml_score] score={score:.6f} threshold={self.artifact.threshold:.6f}")

        return MLDecision(
            enabled=True,
            score=score,
            accepted=score >= self.artifact.threshold,
            threshold=self.artifact.threshold,
        )