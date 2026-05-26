#!/usr/bin/env python3
"""
Стресс-тест отказоустойчивости (Часть 2 лабораторной).

Алгоритм:
1. Создаём чистую таблицу на мастере
2. Запускаем 1 млн асинхронных INSERT со случайной задержкой
3. Каждый подтверждённый INSERT записываем в память
4. На середине теста обрываем сеть мастера
5. Ждём promote стендбая
6. Читаем все строки из нового мастера
7. Проверяем: все подтверждённые строки должны быть в БД
   (лишние строки в БД — допустимы, потеря подтверждённых — нет)
"""

import asyncio
import random
import time
import subprocess
import logging
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("stress")

# ---------------------------------------------------------------------------
# Настройки подключения
# ---------------------------------------------------------------------------
MASTER_HOST   = "127.0.0.1"
MASTER_PORT   = 5432          # проброшен в docker-compose
STANDBY_PORT  = 5433          # проброшен в docker-compose
PG_USER       = "postgres"
PG_PASSWORD   = "postgres"
PG_DB         = "postgres"

TOTAL_INSERTS     = 1_0000
CONCURRENCY       = 64        # параллельных задач
FAILOVER_AT       = 0.5       # доля вставок до обрыва сети
STANDBY_WAIT      = 30        # секунд ждём promote
MASTER_CONTAINER  = "pg-failover-master-1"
DOCKER_NETWORK    = "pg-failover_pgnet"

# ---------------------------------------------------------------------------
# Глобальное состояние теста
# ---------------------------------------------------------------------------
confirmed: set[int] = set()   # id строк подтверждённых сервером
lock = asyncio.Lock()


def get_conn(host: str, port: int):
    return psycopg2.connect(
        host=host, port=port,
        user=PG_USER, password=PG_PASSWORD,
        dbname=PG_DB,
        connect_timeout=5,
    )


def setup_table():
    """Создаём чистую таблицу для теста."""
    log.info("Создаём таблицу stress_test на мастере...")
    conn = get_conn(MASTER_HOST, MASTER_PORT)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS stress_test;")
        cur.execute("CREATE TABLE stress_test (id BIGINT PRIMARY KEY);")
    conn.close()
    log.info("Таблица создана.")


async def insert_row(row_id: int, semaphore: asyncio.Semaphore):
    """
    Вставляем одну строку.
    Если транзакция подтверждена (commit без ошибки) — запоминаем id.
    Это и есть данные которые мы не должны потерять при failover.
    """
    await asyncio.sleep(random.uniform(0, 0.01))  # случайная задержка до 10мс

    loop = asyncio.get_event_loop()
    async with semaphore:
        try:
            conn = await loop.run_in_executor(
                None, lambda: get_conn(MASTER_HOST, MASTER_PORT)
            )
            conn.autocommit = False
            try:
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO stress_test VALUES (%s);", (row_id,))
                conn.commit()
                # Только после успешного commit фиксируем в памяти
                async with lock:
                    confirmed.add(row_id)
            except Exception:
                conn.rollback()
            finally:
                conn.close()
        except Exception:
            pass  # мастер недоступен — вставка не подтверждена, не записываем


def kill_master_network():
    """Отключаем мастер от docker-сети — имитация сбоя."""
    log.warning("=== ОБРЫВАЕМ СЕТЬ МАСТЕРА ===")
    subprocess.run(
        ["docker", "network", "disconnect", DOCKER_NETWORK, MASTER_CONTAINER],
        check=True,
    )
    log.warning("Мастер отключён от сети.")


def wait_for_new_master() -> bool:
    """
    Ждём пока стендбай завершит promote.
    Признак: pg_is_in_recovery() возвращает false.
    """
    log.info("Ожидаем promote стендбая (до %dс)...", STANDBY_WAIT)
    deadline = time.time() + STANDBY_WAIT
    while time.time() < deadline:
        try:
            conn = get_conn(MASTER_HOST, STANDBY_PORT)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SELECT pg_is_in_recovery();")
                in_recovery = cur.fetchone()[0]
            conn.close()
            if not in_recovery:
                log.info("Стендбай повышен до мастера!")
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def verify(host: str, port: int) -> bool:
    """
    Читаем все строки из нового мастера.
    Проверяем: каждый id из confirmed должен быть в БД.
    Лишние строки в БД (не в confirmed) — допустимы.
    """
    log.info("Читаем все строки из нового мастера...")
    conn = get_conn(host, port)
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM stress_test ORDER BY id;")
        db_rows = {row[0] for row in cur.fetchall()}
    conn.close()

    log.info("Подтверждено клиентом : %d строк", len(confirmed))
    log.info("Найдено в БД          : %d строк", len(db_rows))

    # Строки которые клиент считает записанными но их нет в БД — потеря данных
    lost = confirmed - db_rows
    if lost:
        log.error("ПОТЕРЯ ДАННЫХ! Пропало %d строк: %s...",
                  len(lost), sorted(lost)[:20])
        return False

    extra = db_rows - confirmed
    log.info("OK — потери данных нет. Лишних строк в БД (допустимо): %d", len(extra))
    return True


# ---------------------------------------------------------------------------
# Основной сценарий теста
# ---------------------------------------------------------------------------
async def run_inserts():
    semaphore = asyncio.Semaphore(CONCURRENCY)
    tasks = [insert_row(i, semaphore) for i in range(TOTAL_INSERTS)]
    
    # Запускаем разрыв сети как отдельную корутину с задержкой
    async def delayed_kill():
        await asyncio.sleep(5)  # через 5 секунд после старта
        kill_master_network()
    
    await asyncio.gather(
        asyncio.gather(*tasks),
        delayed_kill()           # параллельно с вставками
    )


def main():
    setup_table()
    asyncio.run(run_inserts())

    promoted = wait_for_new_master()
    if not promoted:
        log.error("Стендбай не выполнил promote за отведённое время. ТЕСТ ПРОВАЛЕН.")
        return

    success = verify(MASTER_HOST, STANDBY_PORT)
    if success:
        log.info("=== ТЕСТ ПРОЙДЕН ===")
    else:
        log.error("=== ТЕСТ ПРОВАЛЕН ===")


if __name__ == "__main__":
    main()