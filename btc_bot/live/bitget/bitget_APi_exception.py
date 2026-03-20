from typing import Any
import json

class BitgetAPIException(Exception):
    def __init__(self, response, json_res=None):
        self.status_code = getattr(response, "status", None)
        self.request = getattr(response, "request", None)
        self.response = response

        # Valeurs par défaut
        self.code = None
        self.message = "Unknown Bitget API error"

        # Traitement du JSON si dispo
        if json_res:
            self.code = json_res.get("code", None)
            self.message = json_res.get("msg", self.message)
        else:
            # Tentative de parsing brut (parfois Bitget renvoie du HTML ou du texte)
            try:
                content = getattr(response, "text", None)
                if content:
                    self.message = content
            except Exception:
                pass

        super().__init__(f"BitgetAPIException ({self.code}): {self.message}")

    def __str__(self):
        return f"BitgetAPIException ({self.code}): {self.message}"  

class BitgetRequestException(Exception):
    def __init__(self, message, code=None):
        self.message = message
        self.code = code  # Peut être None

    def __str__(self):
        return f'BitgetRequestException ({self.code}): {self.message}' if self.code else f'BitgetRequestException: {self.message}'
    
class BitgetParamsException(Exception):
    def __init__(self, message, code=None):
        self.message = message
        self.code = code

    def __str__(self):
        return f'BitgetParamsException ({self.code}): {self.message}' if self.code else f'BitgetParamsException: {self.message}'