#!/usr/bin/env python3
"""
Арбитр (он же Свидетель, Witness).
Не содержит PostgreSQL — только HTTP сервис.
Опрашивается стендбаем при потере связи с мастером.
Отвечает на вопрос: "ты видишь мастера?"
"""

import os
import logging
import requests
from flask import Flask, jsonify

MASTER_HOST   = os.environ.get("MASTER_HOST", "master")
STANDBY_HOST  = os.environ.get("STANDBY_HOST", "standby")
AGENT_PORT    = 8080
CHECK_TIMEOUT = 4

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] arbiter: %(message)s",
)
log = logging.getLogger("arbiter")

app = Flask(__name__)


def check_node_pg_alive(host: str) -> bool:
    """
    Спрашиваем агента на указанном хосте:
    живёт ли его локальный PostgreSQL?
    """
    try:
        url = f"http://{host}:{AGENT_PORT}/pg_alive"
        resp = requests.get(url, timeout=CHECK_TIMEOUT)
        return resp.json().get("alive", False)
    except Exception:
        return False


@app.route("/check/<host>")
def check(host: str):
    """
    Основной эндпоинт арбитра.
    Стендбай вызывает /check/master когда теряет связь с мастером.
    Арбитр сам проверяет мастера и возвращает результат.
    """
    if host not in (MASTER_HOST, STANDBY_HOST):
        return jsonify({"error": "неизвестный хост"}), 400

    alive = check_node_pg_alive(host)
    log.info("check/%s -> alive=%s", host, alive)
    return jsonify({"alive": alive, "host": host})


@app.route("/health")
def health():
    """Проверка что арбитр жив."""
    return jsonify({"status": "ok", "role": "arbiter"})


if __name__ == "__main__":
    log.info("Арбитр запущен. MASTER=%s STANDBY=%s", MASTER_HOST, STANDBY_HOST)
    app.run(host="0.0.0.0", port=AGENT_PORT)