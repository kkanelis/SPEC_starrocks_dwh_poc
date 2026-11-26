set -euo pipefail

SR_HOST="${SR_HOST:-127.0.0.1}"
SR_HTTP_PORT="${SR_HTTP_PORT:-8030}"
SR_MYSQL_PORT="${SR_MYSQL_PORT:-9030}"
SR_USER="${SR_USER:-root}"
SR_PASSWORD="${SR_PASSWORD:-}"          # <DB_PASSWORD> (lokali tuksa)
DB="poc_dwh"
CSV="${CSV:-/tmp/orders.csv}"
DDL="${DDL:-/tmp/01_orders_ddl_and_indexes.sql}"

sr() { mysql -h "$SR_HOST" -P "$SR_MYSQL_PORT" -u "$SR_USER" ${SR_PASSWORD:+-p"$SR_PASSWORD"} "$@"; }
now() { date +%s.%N; }
report() { # $1 nosaukums, $2 t0, $3 t1, $4 rindas, $5 baiti (neobligats)
  awk -v n="$1" -v t0="$2" -v t1="$3" -v r="$4" -v b="${5:-0}" 'BEGIN{
    d=t1-t0; printf "[%s] %.2f s | %.0f rindas/s", n, d, r/d;
    if (b>0) printf " | %.1f MiB/s", b/1048576/d; printf "\n" }'
}

echo "=== 0. Versija ==="
sr -N -e "SELECT current_version();"

echo "=== 1. DDL ==="
sr < "$DDL"

echo "=== 2. Stream Load (1M rindas) ==="
FILE_BYTES=$(stat -c %s "$CSV")
ROWS=$(( $(wc -l < "$CSV") - 1 ))
LABEL="orders_load_$(date +%s)"
echo "Fails: $FILE_BYTES baiti | Rindas (bez header): $ROWS"

T0=$(now)
curl --location-trusted -s -u "${SR_USER}:${SR_PASSWORD}" \
  -H "label:${LABEL}" \
  -H "format:csv" \
  -H "column_separator:," \
  -H "skip_header:1" \
  -H "columns:order_id,customer_id,product_id,total_price,status,order_date,delivery_date" \
  -T "$CSV" -X PUT \
  "http://${SR_HOST}:${SR_HTTP_PORT}/api/${DB}/orders/_stream_load" | tee /tmp/stream_load_result.json
T1=$(now)

grep -q '"Status": *"Success"' /tmp/stream_load_result.json || { echo "STREAM LOAD NEVEIKSMIGS"; exit 1; }
report "Stream Load (end-to-end)" "$T0" "$T1" "$ROWS" "$FILE_BYTES"
echo "(StarRocks pusee LoadTimeMs ir redzams JSON izvada augstak)"

echo "=== 3. Partijas un rindu skaits ==="
sr -e "SELECT COUNT(*) AS rows_total FROM ${DB}.orders;"
echo -n "Partiju skaits: "
sr -N -e "SHOW PARTITIONS FROM ${DB}.orders;" | wc -l

echo "=== 4. UPDATE (500k rindas: PROCESSING -> DELIVERED) ==="
# Sagatavosana (nav merama): simule, ka pirmie 500k pasutijumi vel ir PROCESSING
sr -e "UPDATE ${DB}.orders SET status='PROCESSING', delivery_date=NULL WHERE order_id <= 500000;"
sr -e "SELECT status, COUNT(*) FROM ${DB}.orders GROUP BY status;"

# Merijums: piegades datums tiek sadalits pa 1-10 dienam pec order_date
T0=$(now)
sr -e "UPDATE ${DB}.orders
       SET status = 'DELIVERED',
           delivery_date = CAST(days_add(order_date, (order_id % 10) + 1) AS DATE)
       WHERE order_id <= 500000;"
T1=$(now)
report "UPDATE 500k" "$T0" "$T1" 500000

echo "=== 5. Parbaude ==="
sr -e "SELECT status, COUNT(*) AS cnt, MIN(delivery_date) AS min_dd, MAX(delivery_date) AS max_dd,
              COUNT(DISTINCT delivery_date) AS distinct_dd
       FROM ${DB}.orders GROUP BY status;"
sr -e "SELECT COUNT(*) AS rows_total, COUNT(DISTINCT order_id) AS distinct_ids FROM ${DB}.orders;"
echo "Gatavs."
