from __future__ import annotations

import asyncio

from btc_bot.live.run_paper_live_common import run_paper_live
from btc_bot.live.bitget.bitget_live_socket_bridge import BitgetLiveSocketBridge
from btc_bot.live.bitget.ws_public_client import WsPublicClient
from btc_bot.live.logging.trade_logger import logger_pub


SYMBOL = "BTCUSDT"


async def main():
    ws_client = WsPublicClient(symbol=SYMBOL)

    await run_paper_live(
        symbol=SYMBOL,
        venue="bitget",
        ws_client=ws_client,
        bridge_cls=BitgetLiveSocketBridge,
        logger=logger_pub,
    )


if __name__ == "__main__":
    asyncio.run(main())