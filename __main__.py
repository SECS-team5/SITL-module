"""
Точка входа: python -m new_sitl

Запуск без брокера (standalone).
"""
import os
import sys

from new_sitl.src.new_sitl import NewSITL


def main():
    component_id = os.environ.get("COMPONENT_ID", "sitl_standalone")
    name = os.environ.get("COMPONENT_NAME", component_id.replace("_", " ").title())

    print(f"[{component_id}] Starting in standalone mode (no broker)")
    print(f"[{component_id}] NewSITL '{name}' ready")
    print(f"[{component_id}] Note: to run with broker, include this component in a system")


if __name__ == "__main__":
    main()
