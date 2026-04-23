#!/bin/bash
# Точка входа контейнера для мастера и стендбая.
# Инициализирует PostgreSQL в зависимости от роли,
# затем запускает агент.
set -e

PG_DATA="/var/lib/postgresql/data"
PG_USER="postgres"
PG_REPL_USER="replicator"
PG_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
PARTNER_HOST="${PARTNER_HOST:-master}"

log() { echo "[entrypoint] $*"; }

init_master() {
    log "Инициализируем МАСТЕР..."

    if [ ! -f "$PG_DATA/PG_VERSION" ]; then
        log "Запускаем initdb..."
        su -c "initdb -D $PG_DATA" postgres
    fi

    # pg_hba.conf — перезаписываем полностью
    cat > "$PG_DATA/pg_hba.conf" << 'EOF'
local   all             all                                     trust
local   replication     all                                     trust
host    all             all             0.0.0.0/0               md5
host    replication     replicator      0.0.0.0/0               md5
EOF

    # Только listen_addresses — БЕЗ synchronous_standby_names
    cat >> "$PG_DATA/postgresql.conf" << 'EOF'

wal_level = replica
max_wal_senders = 5
wal_keep_size = 128
listen_addresses = '*'
EOF

    log "Временно запускаем PG только на сокете для создания пользователей..."
    su -c "pg_ctl start -D $PG_DATA -w -o \"-c listen_addresses=''\"" postgres

    until pg_isready -U postgres; do
        sleep 1
    done

    log "Создаём пользователей..."
    su -c "psql -U postgres -c \"ALTER USER postgres PASSWORD '$PG_PASSWORD';\"" postgres
    su -c "psql -U postgres -c \"CREATE USER replicator REPLICATION LOGIN ENCRYPTED PASSWORD '$PG_PASSWORD';\"" postgres || true

    su -c "pg_ctl stop -D $PG_DATA -m fast" postgres

    # Добавляем synchronous_commit ПОСЛЕ остановки — применится при финальном запуске
    cat >> "$PG_DATA/postgresql.conf" << 'EOF'

synchronous_commit = on
synchronous_standby_names = '*'
EOF

    log "Мастер инициализирован."
}

init_standby() {
    log "Инициализируем СТЕНДБАЙ..."

    if [ ! -f "$PG_DATA/PG_VERSION" ]; then
        log "Ждём готовности мастера..."
        # Ждём не просто pg_isready, а именно пока replicator существует
        until PGPASSWORD="$PG_PASSWORD" psql -h "$PARTNER_HOST" -U postgres -c "SELECT 1" postgres > /dev/null 2>&1; do
            sleep 2
        done

        log "Запускаем pg_basebackup с $PARTNER_HOST..."
        find "$PG_DATA" -mindepth 1 -delete 2>/dev/null || true

        su -c "PGPASSWORD='$PG_PASSWORD' pg_basebackup \
            -h $PARTNER_HOST \
            -U replicator \
            -D $PG_DATA \
            -P -Xs -R" postgres

# Исправляем права после pg_basebackup
        chown -R postgres:postgres "$PG_DATA"
        chmod 750 "$PG_DATA"

        log "pg_basebackup завершён."
    fi

    cat >> "$PG_DATA/postgresql.conf" << 'EOF'

synchronous_commit = on
EOF
}



if [ "$ROLE" = "master" ]; then
    init_master
else
    init_standby
fi

chown -R postgres:postgres "$PG_DATA"
chmod 750 "$PG_DATA"

log "Запускаем PostgreSQL..."

su -c "pg_ctl start -D $PG_DATA -w -l $PG_DATA/postgres.log" postgres

log "Запускаем агент..."
exec python3 /app/agent.py