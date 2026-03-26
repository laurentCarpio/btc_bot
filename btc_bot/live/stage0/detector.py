from __future__ import annotations

import numpy as np

from btc_bot.live.config import Stage0Config, ThresholdConfig
from btc_bot.live.models import Stage0Candidate, Stage0Snapshot
from btc_bot.live.logging.trade_logger import logger_pub, logger_cand, log_candidate_event


def robust_z(x: float, med: float, mad: float) -> float:
    return (float(x) - float(med)) / (float(mad) + 1e-12)


class Stage0Detector:
    def __init__(
        self,
        cfg: Stage0Config,
        thr: ThresholdConfig,
        symbol: str,
        venue: str,
        ms_cut_monitor=None,
    ) -> None:
        self.cfg = cfg
        self.thr = thr
        self.symbol = symbol
        self.logger = logger_pub
        self.venue = venue

        # Near-miss tuning
        self.near_miss_max_failed = 2
        self.near_miss_ms_abs_max = 2.0

        # Shadow only: monitoring, not gating
        self.ms_cut_value = float(getattr(thr, "ms_cut_value", np.nan))
        self.ms_cut_monitor = ms_cut_monitor

        if not np.isfinite(self.ms_cut_value):
            self.logger.warning(
                f"[Stage0Detector] ms_cut_value missing or invalid for symbol={symbol}. "
                "MS cut shadow monitoring will be unavailable."
            )

    def compute_ms(self, snap: Stage0Snapshot) -> float:
        micro_abs = abs(snap.micro_bias_bps)
        obi_abs = abs(snap.OBI_10)
        ti_abs = abs(snap.TI)

        z_micro = robust_z(micro_abs, self.thr.med_micro_abs, self.thr.mad_micro_abs)
        z_obi = robust_z(obi_abs, self.thr.med_obi10_abs, self.thr.mad_obi10_abs)
        z_ti = robust_z(ti_abs, self.thr.med_ti_abs, self.thr.mad_ti_abs)
        z_thin = robust_z(snap.thinning_opp_3, self.thr.med_thin, self.thr.mad_thin)
        z_spread = robust_z(snap.spread_bps, self.thr.med_spread, self.thr.mad_spread)
        z_nps = robust_z(snap.nps, self.thr.med_nps, self.thr.mad_nps)

        return float(
            1.0 * z_micro
            + 0.9 * z_obi
            + 0.9 * z_ti
            + 0.7 * z_thin
            - 0.4 * z_spread
            + 0.2 * z_nps
        )

    def _compute_gaps(self, snap: Stage0Snapshot) -> dict[str, float]:
        """
        Gap > 0  => passe le seuil
        Gap < 0  => sous le seuil
        Les gaps sont exprimés en ratio relatif au seuil quand possible.
        """
        def rel_gap(value: float, threshold: float) -> float:
            if threshold == 0:
                return 0.0
            return (float(value) - float(threshold)) / (abs(float(threshold)) + 1e-12)

        return {
            "spread_rel": rel_gap(self.thr.spread_rel_max, snap.spread_rel_5m),      # inverse: plus petit = mieux
            "spread_ticks": rel_gap(self.thr.spread_ticks_max, snap.spread_ticks_1s),# inverse
            "range60": rel_gap(snap.range_60s_bps, self.thr.range60s_min),
            "obi10": rel_gap(abs(snap.OBI_10), self.thr.obi10_abs_min),
            "micro": rel_gap(abs(snap.micro_bias_bps), self.thr.micro_abs_min),
            "Ntot": 1.0 if snap.Ntot > 0.0 else -1.0,
            "TI": rel_gap(abs(snap.TI), self.thr.ti_abs_min),
            "nps": rel_gap(snap.nps, self.thr.nps_min),
            "persist": 1.0 if (
                snap.persist_micro_ms >= self.cfg.persist_ms_min
                or snap.persist_obi10_ms >= self.cfg.persist_ms_min
            ) else -1.0,
            "thin": rel_gap(snap.thinning_opp_3, self.thr.thin_min),
            "sign_micro": 1.0 if np.sign(snap.micro_bias_bps) == int(snap.dir0) else -1.0,
            "sign_ti": 1.0 if np.sign(snap.TI) == int(snap.dir0) else -1.0,
        }
    
    def _is_near_miss(self, failed: list[str], ms: float, gaps: dict[str, float]) -> bool:
        # Trop de filtres ratés = pas intéressant
        if len(failed) == 0:
            return False
        if len(failed) > 2:
            return False

        # 1 seul fail → near_miss si le fail est borderline
        if len(failed) == 1:
            f = failed[0]
            return gaps.get(f, -999.0) >= -0.10

        # 2 fails → beaucoup plus strict
        # On veut deux fails "proches" + un MS pas catastrophique
        if len(failed) == 2:
            g1 = gaps.get(failed[0], -999.0)
            g2 = gaps.get(failed[1], -999.0)
            return g1 >= -0.05 and g2 >= -0.05 and ms >= 0.0

        return False

    def detect(self, snap: Stage0Snapshot) -> Stage0Candidate:
        dir0 = int(snap.dir0)

        checks = {
            "dir0": dir0 != 0,
            "spread_rel": snap.spread_rel_5m <= self.thr.spread_rel_max,
            "spread_ticks": snap.spread_ticks_1s <= self.thr.spread_ticks_max,
            "range60": snap.range_60s_bps >= self.thr.range60s_min,
            "obi10": abs(snap.OBI_10) >= self.thr.obi10_abs_min,
            "micro": abs(snap.micro_bias_bps) >= self.thr.micro_abs_min,
            "Ntot": snap.Ntot > 0.0,
            "TI": abs(snap.TI) >= self.thr.ti_abs_min,
            "nps": snap.nps >= self.thr.nps_min,
            "persist": (
                snap.persist_micro_ms >= self.cfg.persist_ms_min
                or snap.persist_obi10_ms >= self.cfg.persist_ms_min
            ),
            "thin": snap.thinning_opp_3 >= self.thr.thin_min,
            "sign_micro": np.sign(snap.micro_bias_bps) == dir0,
            "sign_ti": np.sign(snap.TI) == dir0,
        }

        pass_all_hard = all(checks.values())
        ms = self.compute_ms(snap)
        gaps = self._compute_gaps(snap)

        # Shadow monitor only
        if self.ms_cut_monitor is not None:
            self.ms_cut_monitor.update(ms=ms, pass_all_hard=pass_all_hard)

        # IMPORTANT:
        # Live decision aligned with historical backtest reference.
        # No MS gating here.
        pass_ms_keep = bool(pass_all_hard)

        failed = [k for k, v in checks.items() if not v]

        if pass_all_hard:
            log_candidate_event(
                logger_cand,
                {
                    "venue": self.venue,
                    "event": "accept",
                    "ts": snap.timestamp.isoformat(),
                    "symbol": self.symbol,
                    "dir0": dir0,
                    "ms": ms,
                    "ms_cut_value": self.ms_cut_value,
                    "ms_above_cut": bool(np.isfinite(self.ms_cut_value) and ms >= self.ms_cut_value),
                    "micro": snap.micro_bias_bps,
                    "obi10": snap.OBI_10,
                    "ti": snap.TI,
                    "nps": snap.nps,
                    "thin": snap.thinning_opp_3,
                    "range60": snap.range_60s_bps,
                    "persist_micro_ms": snap.persist_micro_ms,
                    "persist_obi10_ms": snap.persist_obi10_ms,
                },
            )

        elif self._is_near_miss(failed, ms, gaps):
            log_candidate_event(
                logger_cand,
                {
                    "venue": self.venue,
                    "event": "near_miss",
                    "ts": snap.timestamp.isoformat(),
                    "symbol": self.symbol,
                    "dir0": dir0,
                    "failed": failed,
                    "ms": ms,
                    "ms_cut_value": self.ms_cut_value,
                    "ms_above_cut": bool(np.isfinite(self.ms_cut_value) and ms >= self.ms_cut_value),
                    "micro": snap.micro_bias_bps,
                    "obi10": snap.OBI_10,
                    "ti": snap.TI,
                    "nps": snap.nps,
                    "thin": snap.thinning_opp_3,
                    "range60": snap.range_60s_bps,
                    "persist_micro_ms": snap.persist_micro_ms,
                    "persist_obi10_ms": snap.persist_obi10_ms,
                    "gap_range60": gaps["range60"],
                    "gap_obi10": gaps["obi10"],
                    "gap_micro": gaps["micro"],
                    "gap_ti": gaps["TI"],
                    "gap_nps": gaps["nps"],
                    "gap_thin": gaps["thin"],
                },
            )
        else:
            self.logger.debug(
                "[Stage0Detector] reject "
                f"ts={snap.timestamp.isoformat()} "
                f"failed={failed} "
                f"spread_ticks_1s={snap.spread_ticks_1s:.4f} "
                f"spread_rel_5m={snap.spread_rel_5m:.4f} "
                f"range60={snap.range_60s_bps:.4f} "
                f"micro={snap.micro_bias_bps:.6f} "
                f"obi10={snap.OBI_10:.6f} "
                f"ti={snap.TI:.6f} "
                f"nps={snap.nps:.2f} "
                f"persist_micro_ms={snap.persist_micro_ms:.1f} "
                f"persist_obi10_ms={snap.persist_obi10_ms:.1f} "
                f"thin={snap.thinning_opp_3:.6f} "
                f"ms={ms:.6f}"
            )

        features = {
            "MS": ms,
            "micro_bias_bps": snap.micro_bias_bps,
            "abs_micro_bias_bps": abs(snap.micro_bias_bps),
            "OBI_10": snap.OBI_10,
            "abs_OBI_10": abs(snap.OBI_10),
            "TI": snap.TI,
            "abs_TI": abs(snap.TI),
            "thinning_opp_3": snap.thinning_opp_3,
            "persist_micro_ms": snap.persist_micro_ms,
            "persist_obi10_ms": snap.persist_obi10_ms,
            "range_60s_bps": snap.range_60s_bps,
            "ms_cut_value": self.ms_cut_value,
            "ms_above_cut": bool(np.isfinite(self.ms_cut_value) and ms >= self.ms_cut_value),
        }

        return Stage0Candidate(
            timestamp=snap.timestamp,
            symbol=self.symbol,
            dir0=dir0,
            ms=ms,
            features=features,
            pass_all_hard=bool(pass_all_hard),
            pass_ms_keep=bool(pass_ms_keep),
        )