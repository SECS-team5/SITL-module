import fnmatch
import json
from typing import Any


class FakeRedis:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, Any]] = {}
        self.values: dict[str, str] = {}
        self.expirations: dict[str, int] = {}
        self.closed = False

    async def hgetall(self, key: str) -> dict[str, Any]:
        return dict(self.hashes.get(key, {}))

    async def hset(self, key: str, mapping: dict[str, Any]) -> None:
        bucket = self.hashes.setdefault(key, {})
        bucket.update(dict(mapping))

    async def expire(self, key: str, ttl: int) -> None:
        self.expirations[key] = ttl

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.values[key] = value
        if ex is not None:
            self.expirations[key] = ex

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def ping(self) -> bool:
        return True

    async def aclose(self) -> None:
        self.closed = True

    async def scan(self, cursor: int, match: str, count: int = 100) -> tuple[int, list[str]]:
        matched_keys = [key for key in self.values if fnmatch.fnmatch(key, match)]
        return 0, matched_keys

    async def scan_iter(self, match: str):
        for key in list(self.hashes):
            if fnmatch.fnmatch(key, match):
                yield key

    def decode_json(self, key: str) -> dict[str, Any]:
        return json.loads(self.values[key])
