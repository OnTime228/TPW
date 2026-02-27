from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from typing import Optional

import aiohttp


OAUTH_URL = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
CHAT_URL = "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"


@dataclass
class Token:
    value: str
    expires_at: float  # unix timestamp


class GigaChatClient:
    def __init__(self, *, auth_key: str, scope: str, ssl_verify: bool, model: str):
        self._auth_key = auth_key
        self._scope = scope
        self._ssl_verify = ssl_verify
        self._model = model
        self._token: Optional[Token] = None

    async def _get_token(self) -> str:
        now = time.time()
        if self._token and (self._token.expires_at - now) > 30:
            return self._token.value

        headers = {
            "Authorization": f"Basic {self._auth_key}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            # важно для GigaChat OAuth
            "RqUID": str(uuid.uuid4()),
        }

        # важно: grant_type обязателен
        data = {
            "scope": self._scope,
            "grant_type": "client_credentials",
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(OAUTH_URL, headers=headers, data=data, ssl=self._ssl_verify) as r:
                if r.status >= 400:
                    body = await r.text()
                    raise aiohttp.ClientResponseError(
                        request_info=r.request_info,
                        history=r.history,
                        status=r.status,
                        message=f"{r.reason}; body={body}",
                        headers=r.headers,
                    )
                js = await r.json()

        access_token = js["access_token"]
        expires_in = float(js.get("expires_in", 60 * 29))
        self._token = Token(value=access_token, expires_at=now + expires_in)
        return access_token

    async def chat(self, *, system: str, user: str) -> str:
        token = await self._get_token()

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        payload = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": 0,
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(CHAT_URL, headers=headers, json=payload, ssl=self._ssl_verify) as r:
                if r.status >= 400:
                    body = await r.text()
                    raise aiohttp.ClientResponseError(
                        request_info=r.request_info,
                        history=r.history,
                        status=r.status,
                        message=f"{r.reason}; body={body}",
                        headers=r.headers,
                    )
                js = await r.json()

        return js["choices"][0]["message"]["content"]