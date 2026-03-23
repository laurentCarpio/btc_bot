from __future__ import annotations

from typing import List, Dict, Any
import requests


BINANCE_FAPI_REST = "https://fapi.binance.com"


def fetch_recent_klines_1m(symbol: str, limit: int = 300) -> List[Dict[str, Any]]:
    symbol = symbol.upper()
    limit = int(limit)

    resp = requests.get(
        f"{BINANCE_FAPI_REST}/fapi/v1/klines",
        params={
            "symbol": symbol,
            "interval": "1m",
            "limit": limit,
        },
        timeout=10,
    )
    resp.raise_for_status()
    rows = resp.json()

    out = []
    for r in rows:
        out.append(
            {
                "open_time": int(r[0]),
                "open": float(r[1]),
                "high": float(r[2]),
                "low": float(r[3]),
                "close": float(r[4]),
                "volume": float(r[5]),
                "close_time": int(r[6]),
            }
        )
    return out