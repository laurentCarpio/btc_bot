from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from math import isnan
from typing import Deque, Optional

import numpy as np


@dataclass
class MSCutShadowSnapshot:
    n_obs: int
    base_cut: float
    live_q95: float | None
    live_q97: float | None
    effective_cut_q95: float
    effective_cut_q97: float


class MSCutMonitor:
    """
    Shadow-only monitor for adaptive MS cut.

    It keeps a rolling history of MS values observed on pass_all_hard candidates,
    and exposes rolling quantiles without affecting trading decisions.
    """

    def __init__(
        self,
        base_cut: float,
        maxlen: int = 5000,
        min_obs: int = 200,
        q_low: float = 95.0,
        q_high: float = 97.0,
    ) -> None:
        self.base_cut = float(base_cut)
        self.maxlen = int(maxlen)
        self.min_obs = int(min_obs)
        self.q_low = float(q_low)
        self.q_high = float(q_high)

        self._ms_values: Deque[float] = deque(maxlen=self.maxlen)

    def update(self, ms: float, pass_all_hard: bool) -> None:
        if not pass_all_hard:
            return
        x = float(ms)
        if np.isfinite(x):
            self._ms_values.append(x)

    @property
    def n_obs(self) -> int:
        return len(self._ms_values)

    def _quantile(self, q: float) -> Optional[float]:
        if len(self._ms_values) < self.min_obs:
            return None
        arr = np.fromiter(self._ms_values, dtype=np.float64)
        if arr.size == 0:
            return None
        v = float(np.nanpercentile(arr, q))
        return v if np.isfinite(v) else None

    def get_shadow_snapshot(self) -> MSCutShadowSnapshot:
        q95 = self._quantile(self.q_low)
        q97 = self._quantile(self.q_high)

        effective_q95 = max(self.base_cut, q95) if q95 is not None else self.base_cut
        effective_q97 = max(self.base_cut, q97) if q97 is not None else self.base_cut

        return MSCutShadowSnapshot(
            n_obs=len(self._ms_values),
            base_cut=self.base_cut,
            live_q95=q95,
            live_q97=q97,
            effective_cut_q95=float(effective_q95),
            effective_cut_q97=float(effective_q97),
        )

    def would_keep(self, ms: float, quantile: str = "q97") -> Optional[bool]:
        snap = self.get_shadow_snapshot()
        x = float(ms)

        if quantile == "q95":
            cut = snap.effective_cut_q95
        elif quantile == "q97":
            cut = snap.effective_cut_q97
        else:
            raise ValueError(f"Unsupported quantile: {quantile}")

        return bool(x >= cut)