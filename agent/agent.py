#!/usr/bin/env python3
"""
Агент отказоустойчивости PostgreSQL.
Запускается на каждом узле кластера (мастер и стендбай).
Общается с арбитром по HTTP для принятия решения о promote.
"""

import os
import time
import logging
import subprocess
import threading
from flask import Flask, jsonify
import requests
import psycopg2

# ---------------------------------------------------------------------------
# Конфигурация из переменных окружения (docker-compose.yml)
# ---------------------------------------------------------------------------
ROLE          = os.environ.get("ROLE", "master")     # роль узла: master | standby
SELF_HOST     = os.environ.get("SELF_HOST", "localhost")
PARTNER_HOST  = os.environ.get("PARTNER_HOST", "")   # мастер знает стендбай и наоборот
ARBITER_HOST  = os.environ.get("ARBITER_HOST", "arbiter")
PG_PASSWORD   = os.environ.get("POSTGRES_PASSWORD", "postgres")

AGENT_PORT         = 8080
PG_PORT            = 5432
PG_DATA            = "/var/lib/postgresql/data"
PG_USER            = "postgres"
PG_REPL_USER       = "replicator"

CHECK_INTERVAL     = 3   # секунды между проверками доступности партнёра
FAILURE_THRESHOLD  = 3   # сколько подряд неудач до обращения к арбитру
PARTNER_TIMEOUT    = 5   # таймаут подключения к партнёру

# ---------------------------------------------------------------------------
# Логирование
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger(f"agent-{ROLE}")

# ---------------------------------------------------------------------------
# Flask — HTTP API агента
# Используется арбитром и партнёрским агентом для опроса состояния
# ---------------------------------------------------------------------------
app = Flask(__name__)

# Разделяемое состояние между потоком мониторинга и Flask
state = {
    "role": ROLE,
    "pg_alive": False,
    "partner_alive": False,
    "arbiter_alive": False,
}


@app.route("/health")
def health():
    """Общее состояние агента — для отладки."""
    return jsonify(state)


@app.route("/pg_alive")
def pg_alive_endpoint():
    """
    Арбитр вызывает этот эндпоинт чтобы проверить
    живёт ли локальный PostgreSQL на данном узле.
    """
    alive = check_pg_local()
    return jsonify({"alive": alive, "role": state["role"]})


# ---------------------------------------------------------------------------
# Работа с PostgreSQL
# ---------------------------------------------------------------------------
def check_pg_local() -> bool:
    """Проверяем локальный PG через SELECT 1."""
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname="postgres",
            connect_timeout=3,
        )
        conn.close()
        return True
    except Exception:
        return False


def check_pg_remote(host: str) -> bool:
    """Проверяем удалённый PG через SELECT 1."""
    try:
        conn = psycopg2.connect(
            host=host,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname="postgres",
            connect_timeout=PARTNER_TIMEOUT,
        )
        conn.close()
        return True
    except Exception:
        return False


def pg_promote():
    log.warning("PROMOTE: повышаем стендбай до мастера!")
    result = subprocess.run(
        ["su", "-c", f"pg_ctl promote -D {PG_DATA}", "postgres"],
        capture_output=True, text=True
    )
    log.info("pg_ctl promote stdout: %s", result.stdout)
    log.info("pg_ctl promote stderr: %s", result.stderr)
    if result.returncode == 0:
        # Отключаем синхронный режим, т.к. реплик больше нет
        with open(f"{PG_DATA}/postgresql.conf", "a") as f:
            f.write("\nsynchronous_standby_names = ''\n")
            f.write("synchronous_commit = local\n")
        # Перезагружаем конфигурацию от пользователя postgres
        subprocess.run(["su", "-c", f"pg_ctl reload -D {PG_DATA}", "postgres"], capture_output=True)
        state["role"] = "promoted"
        log.warning("Promote успешен. Этот узел теперь МАСТЕР (синхронный режим отключён).")
    else:
        log.error("Promote ПРОВАЛИЛСЯ (rc=%d)", result.returncode)

def pg_block_writes():
    """
    Блокируем входящие подключения к PG через iptables.
    Вызывается на мастере когда нет ни реплики ни арбитра —
    защита от split-brain (два мастера одновременно).
    """
    log.warning("Блокируем порт %d через iptables (нет реплики и арбитра)", PG_PORT)
    subprocess.run(
        ["iptables", "-A", "INPUT", "-p", "tcp",
         "--dport", str(PG_PORT), "-j", "DROP"],
        capture_output=True
    )


def pg_unblock_writes():
    """Снимаем блокировку iptables когда связность восстановлена."""
    subprocess.run(
        ["iptables", "-D", "INPUT", "-p", "tcp",
         "--dport", str(PG_PORT), "-j", "DROP"],
        capture_output=True
    )


# ---------------------------------------------------------------------------
# Общение с арбитром
# ---------------------------------------------------------------------------
def ask_arbiter_about(target_host: str) -> bool | None:
    """
    Спрашиваем арбитра: доступен ли PG на target_host?
    Возвращает True/False или None если арбитр недоступен.
    None означает что мы не можем принять решение о promote.
    """
    try:
        url = f"http://{ARBITER_HOST}:{AGENT_PORT}/check/{target_host}"
        resp = requests.get(url, timeout=PARTNER_TIMEOUT)
        return resp.json().get("alive", False)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Мониторинг — стендбай
# ---------------------------------------------------------------------------
def monitor_as_standby():
    """
    Основной цикл мониторинга на стендбае.

    Логика принятия решения о promote:
    - Мастер недоступен N раз подряд → спрашиваем арбитра
    - Арбитр недоступен → НЕ делаем promote (нет кворума)
    - Арбитр говорит мастер жив → НЕ делаем promote (наша сеть)
    - Арбитр говорит мастер мёртв → делаем promote
    """
    failure_count = 0

    while True:
        # Если роль уже не standby (например, promoted), выходим из цикла
        if state["role"] != "standby":
            log.info("Роль изменилась на %s, завершаем мониторинг стендбая", state["role"])
            break

        time.sleep(CHECK_INTERVAL)

        master_alive = check_pg_remote(PARTNER_HOST)
        state["partner_alive"] = master_alive
        state["pg_alive"] = check_pg_local()

        if master_alive:
            if failure_count > 0:
                log.info("Мастер снова доступен. Сбрасываем счётчик.")
            failure_count = 0
            continue

        failure_count += 1
        log.warning("Мастер недоступен (попытка %d/%d)", failure_count, FAILURE_THRESHOLD)

        if failure_count < FAILURE_THRESHOLD:
            continue

        # Достигли порога — консультируемся с арбитром
        log.warning("Порог достигнут. Спрашиваем арбитра...")
        arbiter_says = ask_arbiter_about(PARTNER_HOST)
        state["arbiter_alive"] = arbiter_says is not None

        if arbiter_says is None:
            log.error(
                "Нет связи ни с мастером ни с арбитром. "
                "Promote не выполняем во избежание split-brain."
            )
            continue

        if arbiter_says is True:
            log.warning(
                "Арбитр подтверждает: мастер жив. "
                "Проблема на нашей стороне. Promote не выполняем."
            )
            failure_count = 0
            continue

        # Арбитр тоже не видит мастера — выполняем promote
        log.warning("Арбитр подтверждает: мастер недоступен. Выполняем promote.")
        pg_promote()
        failure_count = 0

# ---------------------------------------------------------------------------
# Мониторинг — мастер
# ---------------------------------------------------------------------------
def monitor_as_master():
    """
    Основной цикл мониторинга на мастере.

    Если пропала и реплика и арбитр — блокируем запись через iptables.
    Это защищает от ситуации когда стендбай уже сделал promote
    а мы продолжаем принимать записи (split-brain).
    """
    blocked = False

    while True:
        time.sleep(CHECK_INTERVAL)

        state["pg_alive"] = check_pg_local()
        replica_alive = check_pg_remote(PARTNER_HOST)
        state["partner_alive"] = replica_alive

        arbiter_says = ask_arbiter_about(SELF_HOST)
        arbiter_alive = arbiter_says is not None
        state["arbiter_alive"] = arbiter_alive

        if not replica_alive and not arbiter_alive:
            if not blocked:
                log.error(
                    "Нет реплики И нет арбитра. "
                    "Блокируем запись для защиты от split-brain."
                )
                pg_block_writes()
                blocked = True
        else:
            if blocked:
                log.info("Связность восстановлена. Снимаем блокировку.")
                pg_unblock_writes()
                blocked = False


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------
def main():
    log.info(
        "Агент запущен. ROLE=%s SELF=%s PARTNER=%s ARBITER=%s",
        ROLE, SELF_HOST, PARTNER_HOST, ARBITER_HOST
    )

    # Поток мониторинга в фоне, Flask в основном потоке
    if ROLE == "standby":
        t = threading.Thread(target=monitor_as_standby, daemon=True)
    else:
        t = threading.Thread(target=monitor_as_master, daemon=True)
    t.start()

    app.run(host="0.0.0.0", port=AGENT_PORT)


if __name__ == "__main__":
    main()