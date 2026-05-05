from __future__ import annotations

import json
import logging
import os
from urllib.parse import urlencode
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)


class SearxngClient:
    def __init__(self, base_url: str | None = None) -> None:
        self.base_url = (base_url or os.getenv('SEARXNG_URL') or 'http://localhost:8080').rstrip('/')

    def search(
        self,
        *,
        query: str,
        limit: int = 10,
        category: str = 'general',
        language: str = 'auto',
        time_range: str | None = None,
    ) -> list[dict]:
        params = {
            'q': query,
            'format': 'json',
            'categories': category,
        }
        if language != 'auto':
            params['language'] = language
        if time_range:
            params['time_range'] = time_range
        url = f"{self.base_url}/search?{urlencode(params)}"
        request = Request(
            url,
            headers={
                'User-Agent': 'Mozilla/5.0',
                'Accept': 'application/json',
            },
        )
        try:
            with urlopen(request, timeout=20) as response:
                payload = json.loads(response.read().decode('utf-8', errors='ignore'))
        except Exception as exc:
            logger.warning('searxng search failed | query=%s | error=%s', query, exc)
            return []
        results = payload.get('results') or []
        return results[:limit]
