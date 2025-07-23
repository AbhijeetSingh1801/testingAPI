import requests
import pandas as pd
from requests.adapters import HTTPAdapter, Retry
from typing import Optional, Any
from pathlib import Path

class DevXClient:
    def __init__(self, base_url: str, token: str, timeout: int = 15):
        self.base_url = base_url.rstrip('/')
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        self.timeout = timeout

        # Session with retry logic
        self.session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def get_orders(self, params: dict) -> list[dict]:
        url = f"{self.base_url}/orders"
        try:
            resp = self.session.get(
                url, headers=self.headers, params=params, timeout=self.timeout
            )
            resp.raise_for_status()
            return resp.json().get("data", [])
        except requests.RequestException as e:
            print(f"❌ Request failed: {e}")
            return []

