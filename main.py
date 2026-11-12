"""WebSocket-сервис потока котировок (тема 8).

Клиент подключается к /ws/quotes, подписывается на тикеры и получает
обновления цен. Цены имитируются случайным блужданием (геометрическое
броуновское движение). Одна фоновая задача генерирует цены и рассылает
их только тем клиентам, которые подписаны на соответствующий тикер.
"""
import asyncio
import json
import logging
import math
import os
import random
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("quotes")

TICK_SECONDS = float(os.getenv("TICK_SECONDS", "1.0"))
ANNUAL_VOL = 0.25            # годовая волатильность для имитации
TRADING_SECONDS = 252 * 8 * 3600
STATIC_DIR = Path(__file__).parent / "static"

# Начальные цены (₽)
PRICES: dict[str, float] = {
    "SBER": 276.9, "GAZP": 167.5, "LKOH": 6500.0,
    "YNDX": 4100.0, "GMKN": 15800.0,
}


def step_price(price: float, dt: float = TICK_SECONDS) -> float:
    """Один шаг геометрического броуновского движения."""
    sigma = ANNUAL_VOL * math.sqrt(dt / TRADING_SECONDS) * 60  # усилено для наглядности
    return max(0.01, price * math.exp(random.gauss(0, sigma)))


class ConnectionManager:
    """Хранит соединения и подписки каждого клиента."""

    def __init__(self) -> None:
        self.subs: dict[WebSocket, set[str]] = {}

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self.subs[ws] = set()
        log.info("client connected (total=%d)", len(self.subs))

    def disconnect(self, ws: WebSocket) -> None:
        self.subs.pop(ws, None)
        log.info("client disconnected (total=%d)", len(self.subs))

    async def broadcast_quote(self, symbol: str, quote: dict) -> None:
        for ws, symbols in list(self.subs.items()):
            if symbol in symbols:
                try:
                    await ws.send_json(quote)
                except Exception:          # клиент уже отвалился
                    self.disconnect(ws)


manager = ConnectionManager()


async def price_generator() -> None:
    """Раз в TICK_SECONDS обновляет цены и рассылает подписчикам."""
    while True:
        await asyncio.sleep(TICK_SECONDS)
        for symbol in PRICES:
            old = PRICES[symbol]
            PRICES[symbol] = step_price(old)
            await manager.broadcast_quote(symbol, {
                "type": "quote",
                "symbol": symbol,
                "price": round(PRICES[symbol], 2),
                "change_pct": round((PRICES[symbol] / old - 1) * 100, 4),
                "ts": time.time(),
            })


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(price_generator())
    yield
    task.cancel()


app = FastAPI(title="Quotes WebSocket Service", lifespan=lifespan)


@app.get("/api/v1/health")
def health():
    return {"status": "ok", "clients": len(manager.subs), "symbols": list(PRICES)}


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")


def parse_symbols(msg: dict) -> list[str] | None:
    symbols = msg.get("symbols")
    if not isinstance(symbols, list) or not all(isinstance(s, str) for s in symbols):
        return None
    return [s.upper() for s in symbols]


@app.websocket("/ws/quotes")
async def quotes_ws(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
                action = msg["action"]
            except (json.JSONDecodeError, KeyError, TypeError):
                await ws.send_json({"type": "error", "message": "Ожидается JSON с полем action"})
                continue

            if action in ("subscribe", "unsubscribe"):
                symbols = parse_symbols(msg)
                if symbols is None:
                    await ws.send_json({"type": "error", "message": "symbols должен быть списком строк"})
                    continue
                unknown = [s for s in symbols if s not in PRICES]
                if unknown:
                    await ws.send_json({"type": "error", "message": f"Неизвестные тикеры: {unknown}"})
                    continue
                if action == "subscribe":
                    manager.subs[ws].update(symbols)
                else:
                    manager.subs[ws].difference_update(symbols)
                await ws.send_json({"type": action + "d", "symbols": sorted(manager.subs[ws])})
            elif action == "list":
                await ws.send_json({"type": "symbols", "symbols": list(PRICES)})
            elif action == "ping":
                await ws.send_json({"type": "pong"})
            else:
                await ws.send_json({"type": "error", "message": f"Неизвестное действие: {action}"})
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(ws)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
