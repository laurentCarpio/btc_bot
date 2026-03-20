# bitget_ws_clients.py

import asyncio
import json
from datetime import datetime, timezone

from websockets import connect
from websockets.exceptions import ConnectionClosed

import btc_bot.live.bitget.ws_tools as ws_tools
import btc_bot.live.bitget.bitget_constants as const
from btc_bot.live.bitget.ws_msg_dispatcher import WsMessageDispatcher

from btc_bot.live.logging.trade_logger import logger_pub


class WsPublicClient:

    def __init__(self, symbol: str):

        self._ws_url = const.CONTRACT_WS_PUBLIC_URL
        self.verbose = ws_tools.parse_bool(const.VERBOSE_VALUE)

        self._ws = None
        self.subscriptions = set()       

        self.dispatcher = None
        self.last_pong_time = datetime.now(timezone.utc)

        # ✅ Initialise les tâches pour éviter tout AttributeError
        self.listen_task = None
        self.ping_task = None

        self.pong_watchdog_task = None

        self.subscriptions.add(ws_tools.build_subscribe_req("USDT-FUTURES", "books", "instId", symbol))
        self.subscriptions.add(ws_tools.build_subscribe_req("USDT-FUTURES", "trade", "instId", symbol))
        self.subscriptions.add(ws_tools.build_subscribe_req("USDT-FUTURES", "candle1m", "instId", symbol))

        self.connected_event = asyncio.Event()
        
    def use_dispatcher(self, dispatcher: WsMessageDispatcher):
        self.dispatcher = dispatcher
        return self

    async def connect(self):
        logger_pub.info("[WsPublicClient] connect() called")
        self.connected_event.clear()
        self._main_task = asyncio.create_task(self.start())
        try:
            await asyncio.wait_for(self.connected_event.wait(), timeout=15)
        except asyncio.TimeoutError:
            logger_pub.error("[WsPublicClient] ❌ WebSocket connection timed out")
            raise
        return self

    async def start(self):
        logger_pub.debug("[WsPublicClient] start() called 🚀")
        try:
            # 0. Connexion WebSocket
            self._ws = await connect(self._ws_url)
            logger_pub.info("[WsPublicClient] ✅ WebSocket connection established")

            # 1. Lancer la boucle d'écoute
            self.listen_task = asyncio.create_task(self._listen_loop())
            logger_pub.debug("[WsPublicClient] Listen loop started 🟢")

            # 2. Petite pause pour laisser le listener démarrer
            await asyncio.sleep(1.0)

            # 3. Send public subs early
            await self.subscribe(list(self.subscriptions))
            logger_pub.info("[WsPublicClient] 📡 Public subscriptions sent")

            # Ajoute cette ligne ici :
            self.connected_event.set()

            # 6. Boucle de ping
            await asyncio.sleep(0.5)  # Laisse le temps à listen_loop de récupérer ses premiers pongs
            self.ping_task = asyncio.create_task(self._ping_loop())
            logger_pub.debug("[WsPublicClient] 🫀 Ping loop started 🟢")

            # 7. Démarrer le watchdog de pong
            self.pong_watchdog_task = asyncio.create_task(self._pong_watchdog_loop())
            logger_pub.info("[WsPublicClient] 👀 Pong watchdog started")

        except Exception as e:
            logger_pub.error(f"[WsPublicClient] ❌ start() failed: {e}")
            raise
                 
    async def subscribe(self, channels: list[ws_tools.SubscribeReq]):
        for ch in channels:
            self.subscriptions.add(ch)
        msg = ws_tools.BaseWsReq("subscribe", channels)
        logger_pub.debug(f"[WsPublicClient] Subscribing to: {msg.to_dict()}")

        if self._ws is None:
            logger_pub.warning("[WsPublicClient] Cannot send subscribe: WebSocket not open")
            return

        try:
            await self._send_json(msg)
        except ConnectionClosed as e:
            logger_pub.warning(f"[WsPublicClient] Subscribe failed: connection closed: {e}")
            await self._reconnect()

    async def subscribe_all(self):
        """Public method to (re-)subscribe to all current channels in self.subscriptions."""
        if not self.subscriptions:
            logger_pub.warning("[WsPublicClient] No subscriptions to send.")
            return
        logger_pub.debug("[WsPublicClient] Sending subscriptions ✅")
        await self._resubscribe()
        logger_pub.info("[WsPublicClient] Subscriptions sent ✅")

        if not self.listen_task or self.listen_task.done():
            logger_pub.warning("[WsPublicClient] Listen task inactive — reconnect triggered")
            await self._reconnect()
        else:
            logger_pub.debug("[WsPublicClient] ✅ Listen task still alive after resub")

    async def unsubscribe_all(self):
        if self.subscriptions:
            await self.unsubscribe(list(self.subscriptions))
            self.subscriptions.clear()
            logger_pub.info("[WsPublicClient] ❌ Toutes les souscriptions ont été annulées.")

    async def unsubscribe(self, channels: list[ws_tools.SubscribeReq]):
        for ch in channels:
            self.subscriptions.discard(ch)
        msg = ws_tools.BaseWsReq("unsubscribe", channels)
        logger_pub.debug(f"[WsPublicClient] Unsubscribing from: {msg.to_dict()}")

        if self._ws is None:
            logger_pub.warning("[WsPublicClient] Cannot send unsubscribe: WebSocket not open")
            return

        try:
            await self._send_json(msg)
        except ConnectionClosed as e:
            logger_pub.warning(f"[WsPublicClient] Unsubscribe failed: connection closed: {e}")
            await self._reconnect()

    async def _resubscribe(self):
        if self.subscriptions:
            logger_pub.debug(f"[WsPublicClient] Re-subscribing to {len(self.subscriptions)} channels...")
            await self.subscribe(list(self.subscriptions))

    async def _send_json(self, msg: ws_tools.BaseWsReq):
        raw = json.dumps(msg.to_dict())
        await self._ws.send(raw)

    async def _listen_loop(self):
        logger_pub.info("[WsPublicClient] in Listen loop 🟢")
        try:
            async for message in self._ws:
                # Cas spécial : pong
                if message == 'pong':
                    self.last_pong_time = datetime.now(timezone.utc)
                    if self.verbose:
                        logger_pub.debug("[WsPublicClient] ← Pong received")
                    continue
                
                try:
                    data = json.loads(message)
                except json.JSONDecodeError as e:
                    logger_pub.warning(f"[WsPublicClient] Invalid JSON: {e} — message: {message}")
                    continue

                # Gestion login# 🔐 ACK
                try:
                    if data.get("event") == "error":
                        logger_pub.error(f"[WsPublicClient] Error from server: {data}")
                        continue

                    # Dispatcher
                    if self.dispatcher:
                        await self.dispatcher.dispatch(data)
                    else:
                        logger_pub.debug(f"[WsPublicClient] ← {data}")

                except Exception as e:
                    logger_pub.error(f"[WsPublicClient] Error in message handling: {e}")
                    continue  # Ne pas tuer la boucle d'écoute pour une erreur de traitement

        except ConnectionClosed as e:
            logger_pub.warning(f"[WsPublicClient] WebSocket closed: {e}")
            await self._reconnect()

        except Exception as e:
            logger_pub.error(f"[WsPublicClient] Listen error: {e}")
            await self._reconnect()

    async def _reconnect(self, max_attempts: int = 5, delay_seconds: int = 10):
        attempt = 0
        logger_pub.warning("[WsPublicClient] 🔁 Starting reconnection attempts...")

        while attempt < max_attempts:
            attempt += 1
            logger_pub.warning(f"[WsPublicClient] 🔁 Reconnect attempt {attempt}/{max_attempts}...")

            try:
                # 1. faire clean up 
                await self._cleanup()

                # 2. Réinitialiser les états critiques
                self.connected_event.clear()

                # 3. Attendre un petit délai avant la reconnexion (évite boucle infinie)
                await asyncio.sleep(3)
                
                # 4. Relancer toute la logique de connexion
                await self.start()  # Relance tout : connect, resubscribe, listen, ping
                logger_pub.info(f"[WsPublicClient] ✅ Reconnect attempt {attempt} succeeded.")
                return
            except Exception as e:
                logger_pub.error(f"[WsPublicClient] Reconnect attempt {attempt} failed: {e}")
                await asyncio.sleep(delay_seconds)

        logger_pub.error("[WsPublicClient] ❌ Max reconnect attempts reached. Giving up.")
        # Optionnel : raise, shutdown bot, ou notifier

    async def _ping_loop(self):
        logger_pub.debug("[WsPublicClient] Ping loop task entered 🫀")
        try:
            while True:
                await asyncio.sleep(25)
                now = datetime.now(timezone.utc)

                if (now - self.last_pong_time).total_seconds() > 35:
                    logger_pub.warning("[WsPublicClient] Ping timeout detected, triggering reconnect...")
                    await self._reconnect()
                    break

                if self._ws is None:
                    logger_pub.warning("[WsPublicClient] WebSocket is missing before ping, triggering reconnect...")
                    await self._reconnect()
                    break

                try:
                    await asyncio.wait_for(self._ws.send("ping"), timeout=5)
                    if self.verbose:
                        logger_pub.debug("[WsPublicClient] → Ping sent")
                except (asyncio.TimeoutError, Exception) as e:
                    logger_pub.error(f"[WsPublicClient] Ping send error: {e}")
                    await self._reconnect()
                    break

        except asyncio.CancelledError:
            logger_pub.info("[WsPublicClient] Ping loop cancelled gracefully")

    async def _pong_watchdog_loop(self):
        try:
            while True:
                await asyncio.sleep(10)  # Vérifie toutes les 10 secondes (ajuste si besoin)

                now = datetime.now(timezone.utc)
                delta = (now - self.last_pong_time).total_seconds()

                if delta > 60:  # Si pas de pong depuis 60 sec
                    logger_pub.warning(f"[WsPublicClient] 🚨 No pong received for {delta:.0f}s. Triggering reconnect...")
                    await self._reconnect()
                    return  # Arrête cette boucle après reconnexion (sera redémarrée dans start)
        except asyncio.CancelledError:
            logger_pub.debug("[WsPublicClient] Pong watchdog task cancelled")

    async def _cleanup(self):
        logger_pub.debug("[WsPublicClient] 🔧 Cleaning up before reconnect...")

        try:
            # 1. Annuler la tâche d'écoute
            if self.listen_task:
                self.listen_task.cancel()
                try:
                    await self.listen_task
                except asyncio.CancelledError:
                    logger_pub.debug("[WsPublicClient] Listen task cancelled")

            # 2. Annuler la tâche de ping
            if self.ping_task:
                self.ping_task.cancel()
                try:
                    await self.ping_task
                except asyncio.CancelledError:
                    logger_pub.debug("[WsPublicClient] Ping task cancelled")

            # 3. Annuler la tâche du watchdog pong
            if self.pong_watchdog_task:
                self.pong_watchdog_task.cancel()
                try:
                    await self.pong_watchdog_task
                except asyncio.CancelledError:
                    logger_pub.debug("[WsPublicClient] Pong watchdog task cancelled")
                self.pong_watchdog_task = None

            # 4. Fermer proprement le WebSocket
            if self._ws is not None:
                try:
                    await self._ws.close()
                    logger_pub.info("[WsPublicClient] 🔒 Old WebSocket closed")
                except Exception as e:
                    logger_pub.warning(f"[WsPublicClient] Failed to close old WebSocket: {e}")

            # 5. Réinitialiser les états critiques
            self.connected_event.clear()
            logger_pub.debug("[WsPublicClient] Internal events cleared")

        finally:
            # Réinitialiser toutes les références
            self.listen_task = None
            self.ping_task = None
            self._ws = None
