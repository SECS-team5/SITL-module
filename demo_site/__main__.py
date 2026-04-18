from __future__ import annotations

import os

from aiohttp import web

from demo_site.app import create_demo_app


def main() -> None:
    host = os.getenv("DEMO_HOST", "127.0.0.1")
    port = int(os.getenv("DEMO_PORT", "8080"))
    print(f"[demo-site] Starting on http://{host}:{port}")
    web.run_app(create_demo_app(), host=host, port=port)


if __name__ == "__main__":
    main()
