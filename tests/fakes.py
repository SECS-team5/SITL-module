import fnmatch
from typing import Any


class FakeRedis:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, Any]] = {}
        self.expirations: dict[str, int] = {}

    async def hgetall(self, key: str) -> dict[str, Any]:
        return dict(self.hashes.get(key, {}))

    async def hset(self, key: str, mapping: dict[str, Any]) -> None:
        bucket = self.hashes.setdefault(key, {})
        bucket.update(dict(mapping))

    async def expire(self, key: str, ttl: int) -> None:
        self.expirations[key] = ttl

    async def scan_iter(self, match: str):
        for key in list(self.hashes):
            if fnmatch.fnmatch(key, match):
                yield key
