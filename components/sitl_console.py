"""
SITL Console Client — интерактивная консоль для управления дронами.
Полностью работает через брокер сообщений, без прямого доступа к Redis.
"""
import asyncio
import os
import sys
import uuid
from typing import Dict, Any, List, Optional

# Добавьте путь к проекту
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from broker.system_bus import SystemBus
from sdk.messages import create_request


class SitlConsoleClient:
    """Консольный клиент для управления SITL дронами через брокер."""

    def __init__(self):
        self.bus = SystemBus()
        self._pending_responses: Dict[str, asyncio.Future] = {}
        self._response_topics = [
            "sitl.position.response",
            "sitl.drones.list.response",
            "sitl.positions.all.response"
        ]

    async def connect(self):
        """Инициализация подключений."""
        self.bus.start()

        # Подписка на все response топики
        for topic in self._response_topics:
            def make_handler(t):
                def handler(msg):
                    self._handle_response(msg)

                return handler

            self.bus.subscribe(topic, make_handler(topic))

        print("[Console] ✓ Подключено к системе SITL")
        print("[Console] ✓ Подписка на топики ответов выполнена\n")

    def _handle_response(self, msg: Dict[str, Any]):
        """Обработка ответа от системы."""
        correlation_id = msg.get("correlation_id")
        if correlation_id and correlation_id in self._pending_responses:
            future = self._pending_responses.pop(correlation_id)
            if not future.done():
                future.set_result(msg.get("payload", msg))

    async def disconnect(self):
        """Закрытие подключений."""
        self.bus.stop()
        print("\n[Console] Отключено от системы")

    async def set_home(self, drone_id: str, x: float, y: float, z: float):
        """Установить домашнюю позицию дрона."""
        payload = {
            "drone_id": drone_id,
            "x": x,
            "y": y,
            "z": z
        }
        self.bus.publish("sitl-drone-home", payload)
        print(f"✓ HOME установлен для {drone_id}: ({x}, {y}, {z})")
        await asyncio.sleep(0.2)  # Даем время на обработку

    async def set_velocity(self, drone_id: str, vx: float, vy: float, vz: float):
        """Установить вектор движения дрона."""
        payload = {
            "drone_id": drone_id,
            "vx": vx,
            "vy": vy,
            "vz": vz
        }
        self.bus.publish("sitl.commands", payload)
        print(f"✓ Вектор движения установлен для {drone_id}: ({vx}, {vy}, {vz})")
        await asyncio.sleep(0.2)

    async def get_position(self, drone_id: str, timeout: float = 3.0) -> Optional[Dict[str, Any]]:
        """Получить позицию конкретного дрона через брокер."""
        correlation_id = str(uuid.uuid4())
        future = asyncio.Future()
        self._pending_responses[correlation_id] = future

        request = create_request(
            action="request_position",
            payload={"drone_id": drone_id},
            sender="console_client",
            correlation_id=correlation_id,
            reply_to="sitl.position.response"
        )

        self.bus.publish("sitl.position.request", request)

        try:
            result = await asyncio.wait_for(future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            self._pending_responses.pop(correlation_id, None)
            print(f"✗ Timeout при запросе позиции {drone_id}")
            return None

    async def list_drones(self, status_filter: str = "all", timeout: float = 3.0) -> Optional[List[str]]:
        """Получить список всех активных дронов через брокер."""
        correlation_id = str(uuid.uuid4())
        future = asyncio.Future()
        self._pending_responses[correlation_id] = future

        request = create_request(
            action="list_drones",
            payload={"filter": status_filter},
            sender="console_client",
            correlation_id=correlation_id,
            reply_to="sitl.drones.list.response"
        )

        self.bus.publish("sitl.drones.list.request", request)

        try:
            result = await asyncio.wait_for(future, timeout=timeout)
            drone_ids = result.get("drone_ids", [])
            return drone_ids
        except asyncio.TimeoutError:
            self._pending_responses.pop(correlation_id, None)
            print(f"✗ Timeout при запросе списка дронов")
            return None

    async def get_all_positions(self, drone_ids: Optional[List[str]] = None, timeout: float = 3.0) -> Optional[
        Dict[str, Dict[str, Any]]]:
        """Получить позиции всех (или указанных) дронов через брокер."""
        correlation_id = str(uuid.uuid4())
        future = asyncio.Future()
        self._pending_responses[correlation_id] = future

        payload = {}
        if drone_ids:
            payload["drone_ids"] = drone_ids

        request = create_request(
            action="request_all_positions",
            payload=payload,
            sender="console_client",
            correlation_id=correlation_id,
            reply_to="sitl.positions.all.response"
        )

        self.bus.publish("sitl.positions.all.request", request)

        try:
            result = await asyncio.wait_for(future, timeout=timeout)
            positions = result.get("positions", {})
            errors = result.get("errors", {})

            if errors:
                print(f"⚠ Ошибки для некоторых дронов: {errors}")

            return positions
        except asyncio.TimeoutError:
            self._pending_responses.pop(correlation_id, None)
            print(f"✗ Timeout при запросе всех позиций")
            return None

    def print_position(self, drone_id: str, pos: Dict[str, Any]):
        """Красиво вывести позицию дрона."""
        print(f"\n{'=' * 60}")
        print(f"Дрон: {drone_id}")
        print(f"Позиция: ({pos.get('x', 'N/A'):.2f}, {pos.get('y', 'N/A'):.2f}, {pos.get('z', 'N/A'):.2f})")
        print(f"{'=' * 60}\n")

    def print_all_positions(self, positions: Dict[str, Dict[str, Any]]):
        """Красиво вывести все позиции."""
        if not positions:
            print("\n⚠ Нет данных для отображения\n")
            return

        print(f"\n{'=' * 80}")
        print(f"{'ID':<15} {'X':>12} {'Y':>12} {'Z':>12}")
        print(f"{'-' * 80}")
        for drone_id, pos in sorted(positions.items()):
            print(f"{drone_id:<15} {pos.get('x', 0):>12.2f} {pos.get('y', 0):>12.2f} {pos.get('z', 0):>12.2f}")
        print(f"{'=' * 80}\n")


async def interactive_menu(client: SitlConsoleClient):
    """Интерактивное меню."""
    while True:
        print("\n" + "=" * 60)
        print("SITL Console - Управление дронами")
        print("=" * 60)
        print("1. Установить HOME позицию")
        print("2. Установить вектор движения")
        print("3. Получить позицию дрона")
        print("4. Получить список всех дронов")
        print("5. Получить позиции всех дронов")
        print("6. Быстрый тест (создать 3 дрона)")
        print("0. Выход")
        print("=" * 60)

        choice = input("\nВыберите действие: ").strip()

        try:
            if choice == "1":
                drone_id = input("ID дрона: ").strip()
                x = float(input("X: "))
                y = float(input("Y: "))
                z = float(input("Z: "))
                await client.set_home(drone_id, x, y, z)

            elif choice == "2":
                drone_id = input("ID дрона: ").strip()
                vx = float(input("VX: "))
                vy = float(input("VY: "))
                vz = float(input("VZ: "))
                await client.set_velocity(drone_id, vx, vy, vz)

            elif choice == "3":
                drone_id = input("ID дрона: ").strip()
                pos = await client.get_position(drone_id)
                if pos:
                    client.print_position(drone_id, pos)

            elif choice == "4":
                print("\nФильтр: all / moving / idle")
                status_filter = input("Фильтр (Enter = all): ").strip() or "all"
                drone_ids = await client.list_drones(status_filter)
                if drone_ids:
                    print(f"\n✓ Найдено дронов: {len(drone_ids)}")
                    for drone_id in drone_ids:
                        print(f"  - {drone_id}")
                else:
                    print("\n⚠ Дроны не найдены")

            elif choice == "5":
                positions = await client.get_all_positions()
                if positions:
                    client.print_all_positions(positions)

            elif choice == "6":
                # Быстрый тест
                print("\n[Тест] Создание 3 дронов...")
                for i in range(1, 4):
                    drone_id = f"drone_{i}"
                    await client.set_home(drone_id, i * 10.0, i * 5.0, 0.0)
                    await client.set_velocity(drone_id, 1.0, 0.5, 0.1)

                print("[Тест] Ожидание 2 секунды для обновления позиций...")
                await asyncio.sleep(2)

                print("[Тест] Получение списка дронов...")
                drone_ids = await client.list_drones()
                if drone_ids:
                    print(f"✓ Найдено: {', '.join(drone_ids)}")

                print("[Тест] Получение позиций всех дронов...")
                positions = await client.get_all_positions()
                if positions:
                    client.print_all_positions(positions)

            elif choice == "0":
                print("Выход...")
                break

            else:
                print("✗ Неверный выбор")

        except KeyboardInterrupt:
            print("\n\nПрервано пользователем")
            break
        except ValueError as e:
            print(f"✗ Ошибка ввода: {e}")
        except Exception as e:
            import traceback
            print(f"✗ Ошибка: {e}")
            traceback.print_exc()


async def main():
    """Главная функция."""
    client = SitlConsoleClient()

    try:
        await client.connect()
        await interactive_menu(client)
    finally:
        await client.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nЗавершение работы...")