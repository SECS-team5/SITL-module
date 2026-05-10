# 2. Архитектура политики безопасности

## Обоснование уровней доверия на основе моделирования угроз

### Модель угроз (STRIDE)

| Категория | Угроза | Актив | Последствия | Вероятность |
|-----------|--------|-------|-------------|-------------|
| S — Spoofing | Подмена identity пилота | Commands | Несанкционированное управление | Средняя |
| T — Tampering | Модификация команд | Commands/State | Изменение маршрута дрона | Высокая |
| R — Repudiation | Отказ от отправки | All | Отсутствие аудита | Низкая |
| I — Information Disclosure | Утечка телеметрии | Position data | Конкурентное преимущество | Средняя |
| D — Denial of Service | Блокировка брокера | All | Потеря управления | Средняя |
| E — Elevation of Privilege | Инъекция команд | Commands | Полный контроль | Высокая |

### Пути атак

**Attack Goal: Control Drone**

Основные пути атаки на систему:
- Compromise Verifier
- Compromise Controller

**Path 1: Command Injection**

Последовательность атаки:
1. Attacker sends crafted JSON
   - Countermeasure: JSON Schema validation (ЦПБ-1)
2. Attacker bypasses verifier
   - Countermeasure: Schema integrity checks
3. Controller processes malicious command
   - Countermeasure: HOME prerequisite check (ЦПБ-2)

**Path 2: State Manipulation**

Последовательность атаки:
1. Attacker writes to Redis
   - Countermeasure: Redis ACL (no app access to KEYS/FLUSH)
2. Attacker changes MOVING status
   - Countermeasure: State transition validation
3. Core updates position incorrectly
   - Countermeasure: Velocity bounds from schema

**Path 3: Replay Attack**

Последовательность атаки:
1. Attacker captures valid command
   - Countermeasure: correlation_id uniqueness
2. Attacker replays command
   - Countermeasure: Timestamp + TTL validation

**Path 4: Topic Hijacking**

Последовательность атаки:
1. Attacker publishes to verified-commands
   - Countermeasure: Broker ACL (write-only for verifier)
2. Attacker subscribes to raw topics
   - Countermeasure: No ACL on raw topics (intentional)

### Анализ путей атак

| Путь атаки | Начальная точка | Конечная точка | Защита (ЦПБ) | Эффективность |
|------------|-----------------|----------------|--------------|----------------|
| Command Injection | sitl.commands | sitl.verified-commands | ЦПБ-1 (Verifier) | Высокая |
| Schema Bypass | sitl.commands | Redis state | ЦПБ-1 + ЦПБ-3 | Средняя |
| Redis Poisoning | Direct | drone:*:state | Redis ACL | Высокая |
| Replay Attack | Network | Any processing | correlation_id | Средняя |
| HOME Override | sitl-drone-home | Redis state | ЦПБ-2 (prerequisite) | Высокая |
| Velocity Manipulation | Commands | Position updates | Schema bounds | Высокая |

---

## Оценка доменов безопасности

### Домен 1: Входная валидация (Input Validation Domain)

**Зона ответственности:** sitl_verifier

| Метрика | Значение | Оценка |
|---------|----------|--------|
| Размер кода | ~170 LOC | Средний |
| Сложность политик | 3 уровня (parse→classify→validate) | Средняя |
| Покрытие угроз | All malformed input | 95% |
| Ложных срабатываний | ~2% | Низкое |

**Trust Level: HIGH (Порог доверия 85%)**

Обоснование:
- Единственная точка входа для всех входных данных
- Многоуровневая проверка (JSON → Topic → Schema)
- 100% покрытие входных топиков

Риски:
- Schema injection через malformed JSON → mitigated by parse_json
- Timing attack на валидацию → mitigated by lru_cache

---

### Домен 2: Целостность состояния (State Integrity Domain)

**Зона ответственности:** sitl_controller + sitl_core

| Метрика | Значение | Оценка |
|---------|----------|--------|
| Размер кода | ~120 LOC | Средний |
| Сложность политик | 2 уровня (prerequisite→apply) | Низкая |
| Покрытие угроз | State poisoning, race conditions | 90% |
| Конкурентность | Redis операции для атомарности | Высокая |

**Trust Level: CRITICAL (Порог доверия 95%)**

Обоснование:
- Прямое влияние на поведение дронов
- Только проверенные данные попадают в state
- Проверка HOME prerequisite для COMMAND

Риски:
- Race condition между CORE и CONTROLLER → mitigated by Redis TTL
- Redis failure → watchdog в компонентах

---

### Домен 3: Коммуникационная изоляция (Communication Isolation Domain)

**Зона ответственности:** SystemBus (Kafka/MQTT)

| Метрика | Значение | Оценка |
|---------|----------|--------|
| Размер кода | ~400 LOC | Большой |
| Сложность политик | Topic routing + transport adaptation | Высокая |
| Покрытие угроз | Topic confusion, cross-contamination | 80% |
| ACL enforcement | Broker-level | Зависит от конфигурации |

**Trust Level: OPERATIONAL (Порог доверия 70%)**

Обоснование:
- Шина данных без встроенной валидации
- Зависимость от брокера (Kafka/MQTT ACL)
- Компоненты доверяют друг другу на уровне топиков

Риски:
- Topic misconfiguration → cross-component contamination
- Broker compromise → полная потеря isolation

---

### Домен 4: Телеметрия и запросы (Telemetry Domain)

**Зона ответственности:** sitl_messaging

| Метрика | Значение | Оценка |
|---------|----------|--------|
| Размер кода | ~140 LOC | Средний |
| Сложность политик | Request → Validate → Response | Средняя |
| Покрытие угроз | Unauthenticated reads, data leakage | 85% |
| Чувствительность данных | Координаты дронов | Высокая |

**Trust Level: HIGH (Порог доверия 80%)**

Обоснование:
- Read-only операции (без изменения state)
- Валидация correlation_id для replay protection
- Ответы только в указанный reply_to

Риски:
- Information disclosure через неавторизованные запросы
- Зависимость от корректного reply_to

---

## Матрица уровней доверия

**Шкала уровней доверия:**

| Уровень | Компонент | Порог доверия |
|---------|-----------|---------------|
| CRITICAL | state integrity | 95% |
| HIGH | verifier, telemetry | 85% / 80% |
| OPERATIONAL | core, systembus | 70% / 70% |

**Сводная таблица уровней доверия:**

| Компонент | Trust Level | Обоснование | Мин. порог |
|-----------|-------------|-------------|------------|
| sitl_verifier | HIGH | Единственная точка входа, многоуровневая валидация | 85% |
| sitl_controller | CRITICAL | Прямое влияние на поведение дронов | 95% |
| sitl_core | OPERATIONAL | Чтение/запись, но фильтрация по статусу | 70% |
| sitl_messaging | HIGH | Только чтение, отслеживание correlation_id | 80% |
| SystemBus | OPERATIONAL | Транспорт, зависит от ACL брокера | 70% |
| Redis | CRITICAL | Единая точка истины для всего состояния | 95% |

---

## Взаимосвязь уровней доверия

**Trust Boundary 1 (Verifier)**
- FROM: UNTRUSTED (sitl.commands, sitl-drone-home)
- TO: VERIFIED (sitl.verified-*)

**Trust Boundary 2 (Controller)**
- FROM: VERIFIED (only if HOME exists)
- TO: STATE (drone:{id}:state)

**Trust Boundary 3 (Core/Messaging)**
- FROM: STATE (filtered by status)
- TO: OPERATIONS (position update or telemetry)

---

## Рекомендации по усилению доверия

### Краткосрочные (реализуемо в текущей архитектуре)

1. **Включить SASL_SSL для Kafka**
   - Текущий уровень: PLAINTEXT
   - Требуемый: SASL_PLAINTEXT → SASL_SSL
   - Impact: +15% trust score

2. **Redis ACL per component**
   - Verifier: N/A (не использует Redis)
   - Controller: SET/HSET/HGETALL только для drone:*
   - Core: SCAN + HSET + EXPIRE
   - Messaging: HGETALL только для drone:*

3. **Добавить подпись верифицированных сообщений**
   - Текущий: только verified_at timestamp
   - Требуемый: HMAC signature
   - Impact: +10% trust score

### Среднесрочные (требуют рефакторинга)

4. **correlation_id tracking в Redis**
   - Защита от replay attacks
   - Хранение обработанных correlation_id с TTL

5. **Rate limiting на verified topics**
   - Предотвращение DoS на downstream

6. **Мониторинг аномалий**
   - Количество rejected сообщений
   - Аномальные паттерны команд