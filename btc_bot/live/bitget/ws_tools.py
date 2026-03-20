# btc_bot.live.bitget.ws_tools.py

import hmac
import base64

# ----------- UTILS -----------
def create_sign(message: str, secret_key: str) -> str:
    mac = hmac.new(secret_key.encode(), message.encode(), digestmod='sha256')
    return base64.b64encode(mac.digest()).decode()

def pre_hash(timestamp: int, method: str, request_path: str) -> str:
    return f"{timestamp}{method.upper()}{request_path}"

def build_subscribe_req(instType, channel, third_key, third_value):
    if third_key == 'coin':
        return SubscribeReq(instType, channel, coin=third_value)
    elif third_key == 'instId':
        return SubscribeReq(instType, channel, third_value)
    else:
        raise ValueError("Unsupported third_key")

def extract_symbols_from_subscriptions(subscriptions: set) -> list[str]:
    return [
        sub["args"][0]["instId"]
        for sub in subscriptions
        if isinstance(sub, dict) and "args" in sub and sub["args"]
    ]

def parse_bool(value: str, default=True) -> bool:
    if value is None:
        return default
    return value.strip().lower() == "true"

class SubscribeReq:
    def __init__(self, instType, channel, *args, **kwargs):
        self.instType = instType
        self.channel = channel
        if args:
            self.instId = args[0]
            self.coin = None
        elif 'coin' in kwargs:
            self.coin = kwargs['coin']
            self.instId = None
        else:
            raise ValueError("Either instId or coin must be provided")

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __hash__(self):
        identifier = ('instId', self.instId) if self.instId is not None else ('coin', self.coin)
        return hash((self.instType, self.channel, identifier))

    def to_dict(self):
        return {
            "instType": self.instType,
            "channel": self.channel,
            **({"instId": self.instId} if self.instId else {}),
            **({"coin": self.coin} if self.coin else {})
        }

class BaseWsReq:
    def __init__(self, op, args):
        self.op = op
        self.args = args

    def to_dict(self):
        return {
            "op": self.op,
            "args": [arg.to_dict() for arg in self.args]
        }

class WsLoginReq:
    def __init__(self, api_key, passphrase, timestamp, sign):
        self.api_key = api_key
        self.passphrase = passphrase
        self.timestamp = timestamp
        self.sign = sign

    def to_dict(self):
        return {
            "apiKey": self.api_key,
            "passphrase": self.passphrase,
            "timestamp": str(self.timestamp),
            "sign": self.sign
        }