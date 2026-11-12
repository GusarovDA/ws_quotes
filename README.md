# WebSocket-сервис потока котировок

Тема 8: «WebSocket-сервис» (FastAPI WebSocket + клиент на JS, имитация потока и подписки на тикеры).

## Запуск
```bash
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```
- Клиент: http://localhost:8000/
- Swagger: http://localhost:8000/docs
- Health-check: `GET /api/v1/health`

## Протокол (`ws://localhost:8000/ws/quotes`)
Клиент -> сервер (JSON):
| Сообщение | Действие |
|---|---|
| `{"action":"subscribe","symbols":["SBER","GAZP"]}` | подписаться |
| `{"action":"unsubscribe","symbols":["SBER"]}` | отписаться |
| `{"action":"list"}` | список доступных тикеров |
| `{"action":"ping"}` | проверка связи |

Сервер -> клиент:
- `{"type":"quote","symbol":"SBER","price":276.95,"change_pct":0.018,"ts":1731400000.1}`
- `{"type":"subscribed"|"unsubscribed","symbols":[...]}` — текущий список подписок
- `{"type":"symbols","symbols":[...]}`, `{"type":"pong"}`
- `{"type":"error","message":"..."}`

## Как устроено
- Цены имитируются геометрическим броуновским движением (`step_price`), всегда положительны.
- Одна фоновая задача (`price_generator`) обновляет цены раз в `TICK_SECONDS` (по умолчанию 1 с).
- `ConnectionManager` хранит подписки каждого клиента; рассылка идёт только подписчикам тикера.
- Мёртвые соединения удаляются; клиент автоматически переподключается (экспоненциальная задержка) и восстанавливает подписки.

## Тесты
```bash
pytest -q
```
Проверяется: health, раздача клиента, подписка/отписка, фильтрация по тикерам, ошибки ввода, независимость двух клиентов, положительность цены.
