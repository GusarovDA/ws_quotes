import os
os.environ["TICK_SECONDS"] = "0.05"   # быстрые тики для тестов

from fastapi.testclient import TestClient
from main import app, PRICES, step_price


def recv_until(ws, type_, limit=50):
    for _ in range(limit):
        m = ws.receive_json()
        if m["type"] == type_:
            return m
    raise AssertionError(f"не получено сообщение {type_}")


def test_health():
    with TestClient(app) as c:
        r = c.get("/api/v1/health")
        assert r.status_code == 200 and r.json()["status"] == "ok"


def test_index_served():
    with TestClient(app) as c:
        assert "Котировки" in c.get("/").text


def test_subscribe_receives_only_subscribed():
    with TestClient(app) as c, c.websocket_connect("/ws/quotes") as ws:
        ws.send_json({"action": "subscribe", "symbols": ["sber"]})
        assert ws.receive_json() == {"type": "subscribed", "symbols": ["SBER"]}
        for _ in range(5):
            q = recv_until(ws, "quote")
            assert q["symbol"] == "SBER" and q["price"] > 0


def test_unsubscribe_stops_updates():
    with TestClient(app) as c, c.websocket_connect("/ws/quotes") as ws:
        ws.send_json({"action": "subscribe", "symbols": ["SBER", "GAZP"]})
        recv_until(ws, "subscribed")
        ws.send_json({"action": "unsubscribe", "symbols": ["SBER"]})
        m = recv_until(ws, "unsubscribed")
        assert m["symbols"] == ["GAZP"]
        for _ in range(10):
            assert recv_until(ws, "quote")["symbol"] == "GAZP"


def test_unknown_symbol_and_bad_input():
    with TestClient(app) as c, c.websocket_connect("/ws/quotes") as ws:
        ws.send_json({"action": "subscribe", "symbols": ["FAKE"]})
        assert ws.receive_json()["type"] == "error"
        ws.send_text("не json")
        assert ws.receive_json()["type"] == "error"
        ws.send_json({"action": "dance"})
        assert ws.receive_json()["type"] == "error"
        ws.send_json({"action": "subscribe", "symbols": "SBER"})
        assert ws.receive_json()["type"] == "error"


def test_two_clients_have_independent_subscriptions():
    with TestClient(app) as c:
        with c.websocket_connect("/ws/quotes") as a, c.websocket_connect("/ws/quotes") as b:
            a.send_json({"action": "subscribe", "symbols": ["SBER"]})
            b.send_json({"action": "subscribe", "symbols": ["LKOH"]})
            recv_until(a, "subscribed"); recv_until(b, "subscribed")
            assert recv_until(a, "quote")["symbol"] == "SBER"
            assert recv_until(b, "quote")["symbol"] == "LKOH"
            assert c.get("/api/v1/health").json()["clients"] == 2
        assert c.get("/api/v1/health").json()["clients"] == 0


def test_price_stays_positive():
    p = 1.0
    for _ in range(10_000):
        p = step_price(p)
        assert p > 0
