import asyncio
import inspect
import logging

class WsMessageDispatcher:
    def __init__(self, logger : logging.Logger):
        self._handlers = {}
        self.logger = logger

    def register_handler(self, channel: str, handler):
        """Associe un handler à un canal spécifique."""
        self._handlers[channel] = handler

    def clear(self):
        """Supprime tous les handlers enregistrés (utile pour les tests)."""
        self._handlers.clear()

    def __contains__(self, channel: str) -> bool:
        """Permet de tester si un handler est enregistré : 'orders' in dispatcher."""
        return channel in self._handlers

    async def dispatch(self, message: dict):
        """Méthode asynchrone utilisée en production (AWS Fargate)."""
        channel = message.get("arg", {}).get("channel")
        inst_id = message.get("arg", {}).get("instId", "unknown")
        
        if not channel:
            if message.get("event") == "login":
                # Message attendu, pas de warning nécessaire
                self.logger.debug(f"[Dispatcher] Ignored system message: {message}")
            else:
                self.logger.warning(f"[Dispatcher] Received message without channel info: {message}")
            return

        handler = self._handlers.get(channel)
        if handler is None:
            self.logger.warning(f"[Dispatcher] No handler registered for channel: {channel}")
            return
        #self.logger.debug(f"[Dispatcher] Dispatching: {message.get('arg', {}).get('instId', '❓')}")
        try:
            if inspect.iscoroutinefunction(handler):
                await handler(message)
            else:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, handler, message)
        except Exception as e:
            self.logger.exception(f"[Dispatcher] Exception while dispatching {channel} ({inst_id}): {e}")

# Example usage:
# dispatcher = WsMessageDispatcher()
# dispatcher.register_handler("account", on_account_message)
# dispatcher.register_handler("orders", on_orders_message)
# dispatcher.register_handler("orders-algo", on_orders_algo_message)
# ws_client.subscribe(channels, dispatcher.dispatch)
