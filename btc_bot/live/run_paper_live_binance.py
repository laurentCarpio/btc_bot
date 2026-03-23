from __future__ import annotations

import asyncio

from btc_bot.live.run_paper_live_common import run_paper_live
from btc_bot.live.binance.binance_ws_public_client import BinanceWsPublicClient
from btc_bot.live.binance.binance_live_socket_bridge import BinanceLiveSocketBridge
from btc_bot.live.logging.trade_logger import logger_pub


SYMBOL = "BTCUSDT"


async def main():
    ws_client = BinanceWsPublicClient(
        symbol=SYMBOL,
        logger=logger_pub,
    )

    await run_paper_live(
        symbol=SYMBOL,
        venue="binance",
        ws_client=ws_client,
        bridge_cls=BinanceLiveSocketBridge,
        logger=logger_pub,
    )


if __name__ == "__main__":
    asyncio.run(main())