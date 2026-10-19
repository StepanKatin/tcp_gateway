# TCP IoT Gateway

Асинхронный TCP-шлюз для телеметрии терминалов: приём пакетов → проверка CRC-16 Modbus → публикация в **Redis Stream** (FastStream) → HTTP-обсервабилити (health / ready / metrics).

Стек: Python 3.13, `asyncio`, Dishka, FastStream (`RedisBroker`), aiohttp, Prometheus + Grafana.

---

## Архитектура (кратко)

```text
Terminal (TCP :8000)
    → parse + CRC
    → PointPublisher → Redis Stream (XADD, maxlen)
    → ack

HTTP (:8001)
    /health  — процесс жив
    /ready   — готовность зависимостей
    /metrics — Prometheus

Prometheus (:9091 на хосте) ← scrape tcp_gateway:8001/metrics
Grafana    (:3001 на хосте) ← datasource Prometheus
```

Планируемый consumer (отдельный сервис):

```text
Redis Stream → loader (FastStream subscriber) → ClickHouse RAW
                                            → MATERIALIZED VIEW → CLEAN
```

### DI (Dishka)

| Scope | Зависимости |
|---|---|
| **APP** | Settings, Logger, `RedisBroker` (`start`/`stop`), PointPublisher, GatewayMetrics |
| **REQUEST** | ConnectionContext на одно TCP-соединение |

Lifecycle брокера:

```text
await broker.start()   # при создании APP-зависимости
…
await broker.stop()    # в finally контейнера / shutdown
```

Idle TCP > `TCP_TIMEOUT` (по умолчанию 60с) → соединение закрывается.

---

## Контракт протокола терминала

### Формат пакета

Текстовая строка UTF-8, поля через `;`, внутри поля `key:value`.  
Ровно **6** полей, последнее — `crc16`.

```text
imei:<IMEI>;datetime:<HH-MM-SS>;speed_can:<N>;speed_engine:<N>;dut_lvl:<N>;crc16:<INT>
```

Пример тела (без CRC):

```text
imei:129464373;datetime:12-54-46;speed_can:77;speed_engine:1500;dut_lvl:1024
```

### CRC-16 Modbus

- Считается от байт **полезной нагрузки** (всё до `;crc16:`), UTF-8.
- Алгоритм: CRC-16 Modbus (`0xFFFF` init, poly `0xA001`).
- В пакете передаётся десятичное целое.

Несовпадение CRC или формата → `'ack':'error_bad_packet';` и обрыв соединения.

### Ответы сервера (ack)

| Ответ | Когда |
|---|---|
| `'ack':'ok';` | пакет валиден, публикация в Redis Stream успешна |
| `'ack':'error_bad_packet';` | CRC / формат / нет обязательных полей |
| `'ack':'internal_error';` | ошибка брокера/Redis (пакет валиден; соединение можно продолжать) |

Семантика: **ok = точка принята в Stream** (не «уже в ClickHouse»).

---

## Контракт Redis Stream (для loader / consumer)

| Параметр | Значение |
|---|---|
| Тип | Redis **Stream** |
| Имя stream | `REDIS_STREAM_NAME` (default `telemetry.points`) |
| Запись | FastStream `broker.publish(message, stream=..., maxlen=...)` → `XADD` |
| Maxlen | `REDIS_STREAM_MAXLEN` (default `100000`) — обрезка старых записей |
| Payload | dict разобранного пакета (те же поля, включая `crc16`) |

Пример сообщения в stream (логическая структура):

```json
{
  "imei": "11111",
  "datetime": "12-00-00",
  "speed_can": "0",
  "speed_engine": "0",
  "dut_lvl": "0",
  "crc16": "32442"
}
```

Проверка после smoke:

```bash
docker compose exec redis redis-cli XLEN telemetry.points
docker compose exec redis redis-cli XRANGE telemetry.points - + COUNT 3
```

**Семантика для consumer:** буфер телеметрии; устаревшие точки можно терять (`maxlen`).  
Loader забирает через consumer group и пишет в ClickHouse RAW; очистка/фильтры — через **MATERIALIZED VIEW** в CH (не в gateway).

---

## Порты

| Сервис | Внутри compose | На хосте (по умолчанию) |
|---|---|---|
| TCP терминалы | `tcp_gateway:8000` | `8000` |
| HTTP metrics/health | `tcp_gateway:8001` | `8001` |
| Redis | `redis:6379` | `6379` |
| Prometheus | `prometheus:9090` | `9091` |
| Grafana | `grafana:3000` | `3001` |

Порты Prometheus/Grafana на хосте смещены (`9091` / `3001`), чтобы не конфликтовать с другими стеками.  
Внутри сети Grafana → `http://prometheus:9090`.

---

## Быстрый старт (Docker)

```bash
cp .env.example .env   # при необходимости
docker compose up -d --build
docker compose logs -f tcp_gateway
```

Ожидаемые логи: брокер поднят, `HTTP started ...:8001`, `TCP server started ...:8000`.

Smoke из `tcp_gateway/`:

```bash
python smoke_tcp.py
python smoke_tcp.py --mode bad_crc
python smoke_tcp.py --imei 11111 --count 3
```

HTTP:

```bash
curl -s localhost:8001/health
curl -s localhost:8001/ready
curl -s localhost:8001/metrics | grep tcp_gateway
```

UI:

- Prometheus: http://localhost:9091 (Status → Targets → `tcp_gateway`)
- Grafana: http://localhost:3001 (`GRAFANA_USER` / `GRAFANA_PASSWORD` из `.env`)

---

## Переменные окружения

См. `.env.example` (актуальные для Stream):

| Переменная | Смысл | Default |
|---|---|---|
| `REDIS_URL` | URL Redis | `redis://redis:6379/0` (в Docker) |
| `TCP_TIMEOUT` | idle TCP, сек | `60` |
| `REDIS_STREAM_NAME` | имя stream | `telemetry.points` |
| `REDIS_STREAM_MAXLEN` | maxlen XADD | `100000` |
| `HTTP_HOST` / `HTTP_PORT` | HTTP observability | `0.0.0.0` / `8001` |
| `GRAFANA_USER` / `GRAFANA_PASSWORD` | логин Grafana | `admin` / `admin` |

---

## Метрики (без label `imei`)

| Метрика | Тип | Labels |
|---|---|---|
| `tcp_gateway_packets_total` | Counter | `result=ok\|bad_packet\|redis_error` |
| `tcp_gateway_connections_active` | Gauge | — |
| `tcp_gateway_packet_duration_seconds` | Histogram | время от read до ack (успешный путь) |

High-cardinality labels (IMEI) сознательно не используются.

---

## Тесты и покрытие

```bash
cd tcp_gateway
.venv/bin/pytest -q
.venv/bin/pytest --cov=app --cov-report=term-missing -q
```

Ориентир покрытия: **~85%** (unit на fake publisher/metrics + HTTP api; без обязательного live Redis в pytest).

Интеграция: smoke + `XRANGE` / `XLEN` + `/metrics` при поднятом compose.

---

## Структура репозитория

```text
tcp_gateway/              # код шлюза (app/, tests/, smoke_tcp.py, dockerfile)
observability/            # prometheus.yml, grafana provisioning
docker-compose.yml
.env.example
README.md
```
---

## Дальше

Отдельный сервис **loader**: FastStream subscriber (consumer group на `telemetry.points`) → batch INSERT в ClickHouse `telemetry_raw` → **MATERIALIZED VIEW** наполняет `telemetry_clean`.

