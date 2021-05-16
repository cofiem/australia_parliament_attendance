import hashlib
import html
import string
from pathlib import Path
from typing import Optional

import requests
from lxml import html


class CacheFiles:
    cache_chars = string.digits + string.ascii_letters
    local_cache_dir = "cache"

    def get_id(self, url: str):
        item_id = "".join(c if c in self.cache_chars else "_" for c in url).strip()
        return item_id

    def save_url_content(self, url: str, content: bytes) -> None:
        file_path = self.get_url_path(url)

        with open(file_path, "wb") as f:
            f.write(content)

    def load_url_content(self, url: str) -> Optional[bytes]:
        file_path = self.get_url_path(url)
        if not file_path.is_file():
            return None

        with open(file_path, "rb") as f:
            return f.read()

    def get_url_path(self, url: str):
        cache_dir = Path(self.local_cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
        item_name = self.get_hash(url) + ".txt"
        file_path = cache_dir / item_name
        return file_path

    def download_html(self, url: str):
        content = self.load_url_content(url)
        if content is None:
            page = requests.get(url, allow_redirects=True)
            content = page.content if page.status_code == 200 else b""
            self.save_url_content(url, content)

        if content is None:
            return None

        tree = html.fromstring(content) if content else None
        return tree

    def get_hash(self, value: str):
        value_bytes = value.encode("utf-8")
        hash_func = hashlib.sha256(value_bytes)
        result = hash_func.hexdigest()
        return result
