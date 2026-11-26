# Tehniskā specifikācija un PoC atskaite: StarRocks DWH un Reāllaika Datu Konveijers

---

| Metadati | Vērtība |
| :--- | :--- |
| **Autors** | Krists Kārlis Grundmanis |
| **Datums** | 2026-11-26 |
| **Statuss** | Pabeigts. |
| **StarRocks versija** | 4.1.7-b81ff9c (`starrocks/allin1-ubuntu`, digest `sha256:7f6041f6d537162a2627668565630e3204756f5e85b60944de3acc2d3abd96f8`) |
| **Apache Flink versija** | 1.18.1 (Commit a8c8b1c), image `flink:1.18` |
| **Flink JAR faili projektā** | `flink-connector-starrocks.jar` (versija `1.2.9_flink-1.18`) un `flink-sql-connector-postgres-cdc-3.2.1.jar` |
| **Salīdzināšanas / Avota komponentes** | PostgreSQL 17 (TimescaleDB 2.31.0, `timescale/timescaledb:2.31.0-pg17`), Python 3.14.7 (`websockets==17.4`) |

---

## 1. Kopsavilkums un mērķis

Šī PoC mērķis ir praktiski izvērtēt StarRocks kā analītiskās datu noliktavas un reāllaika datu saņēmēja kandidātu, salīdzinot tā ielādes un vaicājumu darbību ar PostgreSQL/TimescaleDB un pārbaudot PostgreSQL CDC datu nogādāšanu. A daļā ir veikti masveida ielādes, UPDATE, indeksu un kārtošanas, materializēto skatu un selektīvu vaicājumu eksperimenti. B daļā ir izveidots Binance WebSocket -> failu avota -> Flink -> StarRocks konveijers; 240 sekunžu mērījumā svaiguma mediāna bija 4,074 s un p95 6,202 s. PostgreSQL CDC/TOAST pārbaudē statusa UPDATE saglabāja StarRocks pusē iepriekš ielādēto 4 668 rakstzīmju payload nemainītu; rezultātu robežas un neizpildītie gadījumi norādīti 10.3. un 11. sadaļā.

---

# A DAĻA: StarRocks DWH pamati, indeksācija un salīdzinājums pret TimescaleDB

## 2. Masveida ielāde (Stream Load), Tabulu dizains un Partial Update

### 2.1. Stream Load (1 miljona rindu ielāde)

```bash
curl --location-trusted -u root: -H "Expect:100-continue" \
  -H "column_separator:," -H "skip_header:1" \
  -T /tmp/orders.csv \
  http://<STARROCKS_HOST>:8030/api/poc_dwh/orders/_stream_load
```

- **Faila izmērs un rindu skaits:** 64 789 280 B (≈64,8 MB) / 1 000 000 rindas
- **Ielādes laiks un caurlaidība:** end-to-end 1,53 s, ≈42 MB/s (skripts izdrukāja 40,5 MiB/s), ≈655k rindas/s. Servera puse (`LoadTimeMs`) 1,45 s, ≈690k rindas/s. `NumberFilteredRows = 0`. Izveidojās 23 mēneša partīcijas.

### 2.2. Faktu tabula `orders` (`PRIMARY KEY` + atsevišķa `ORDER BY` atslēga)

```sql
CREATE TABLE orders (
  order_id     BIGINT NOT NULL,
  order_date   DATETIME NOT NULL,
  customer_id  INT,
  product_id   INT,
  total_price  DECIMAL(10,2),
  status       VARCHAR(20),
  delivery_date DATE,
  INDEX idx_status (status) USING BITMAP,
  INDEX idx_delivery_date (delivery_date) USING BITMAP
)
PRIMARY KEY (order_id, order_date)
PARTITION BY date_trunc('month', order_date)
DISTRIBUTED BY HASH(order_id)
ORDER BY (delivery_date, customer_id)
PROPERTIES (
  "enable_persistent_index" = "true",
  "replication_num" = "1"
);
```

- **Kāpēc PK ir atdalīta no `ORDER BY (delivery_date, customer_id)`:** Primary Key tabulā PK nosaka rindas identitāti (upsert un dzēšana pēc atslēgas, ko apkalpo persistent index), bet `ORDER BY` nosaka fizisko kārtību segmentos, pēc kuras tiek veidotas zone maps un short key indekss. Tā var optimizēt lasīšanu pēc `delivery_date`, nezaudējot ātru atjaunināšanu pēc `order_id`.

### 2.3. Masveida `UPDATE` (500k rindu)

```sql
UPDATE orders
SET status = 'DELIVERED',
    delivery_date = CAST(days_add(order_date, (order_id % 10) + 1) AS DATE)
WHERE order_id <= 500000;
```

- **Izpildes laiks un novērojumi:** 1,69 s (≈296k rindas/s). Pārbaude: DELIVERED pieauga par tieši 500 000 (324 974 → 824 974), `COUNT(DISTINCT order_id) = 1 000 000`, `delivery_date` sadalīts pa 688 dažādiem datumiem. Tā ir viena izpilde. Mehānismu (Delete-and-Insert) šajā mērījumā neizpētīju.
- Šis ir tas pats nosacījums, ko izmanto `part1_dwh_fundamentals/02_stream_load_and_updates.sh`.

---

## 3. Particionēšana, BITMAP indeksi un `EXPLAIN ANALYZE` analīze

### 3.1. Partīcijas un BITMAP indeksi

```sql
PARTITION BY date_trunc('month', order_date)
ALTER TABLE poc_dwh.orders_5m ADD INDEX idx_delivery_date (delivery_date) USING BITMAP;
```


### 3.2. Sekundārās laika ass vaicājuma izpildes profils

```sql
SELECT COUNT(*), SUM(total_price) FROM orders_5m WHERE delivery_date = '2026-06-15';
```

Profils paņemts ar `get_query_profile(<query_id>)`, jo `EXPLAIN ANALYZE` rāda saīsinātu profilu bez filtrēšanas skaitītājiem.

| **Metrika (5M rindas)**                  | **Ar definētu BITMAP** | **Bez BITMAP** |
| :--------------------------------------- | :-------------------- | :------------- |
| 3 vaicājumu partiju vidējais (ms/vaic.)  | 223,754 / 78,283 / 82,231 | 97,074 / 70,048 / 88,849 |
| `SegmentZoneMapFilterRows`               | 4 789 306             | 4 789 306      |
| `BitmapIndexFilterRows`                  | 0                     | 0              |
| `ZoneMapIndexFilterRows`                 | 145 158               | 145 158        |
| `RawRowsRead`                            | 65 536                | 65 536         |
| `RowsRead` (atbilst filtram)              | 5 438                 | 5 438          |
| Profilētā vaicājuma `ScanTime`             | 3,391 ms              | 0,732 ms       |

- **Mērījuma metodika:** katrai tabulai palaistas 3 partijas pa 20 vaicājumiem. Partijas vidējais ietver klienta turp-atpakaļ laiku; pirmajā partijā ietilpst aukstais vaicājums. Bāze netika atņemta. Katras tabulas pēdējais vaicājums profilēts atsevišķi; tā nav reprezentatīva latentuma mediāna.
- **Secinājums:** `idx_delivery_date` BITMAP indekss tabulā ir izveidots, bet šim vaicājumam profils rāda `BitmapIndexFilterRows = 0`. Abās tabulās ir vienāds `ORDER BY (delivery_date, customer_id)`, un profilos atlasē redzama `SegmentZoneMapFilterRows`/`ZoneMapIndexFilterRows` darbība. Šis eksperiments BITMAP indeksa ieguvumu nepierāda; laiku atšķirības nevar piedēvēt indeksam. Lai izolētu BITMAP ietekmi, būtu jāpārbauda filtrs pēc kolonnas, kas nav kārtošanas atslēga, un jāsalīdzina atkārtoti mērījumi ar kontrolētiem cache apstākļiem.
- Profila faili un datu ģenerators vairs nav projektā; te ir palicis tikai mērījumu kopsavilkums.

---

## 4. Datu marts (MV), SQL migrācija un TimescaleDB salīdzinājums

### 4.1. Asinhronais datu marts (`mv_daily_deliveries`)

```sql
CREATE MATERIALIZED VIEW mv_daily_deliveries
DISTRIBUTED BY HASH(delivery_date)
REFRESH ASYNC
AS SELECT delivery_date, COUNT(*) AS deliveries, SUM(total_price) AS revenue
FROM orders
WHERE status = 'DELIVERED'
GROUP BY delivery_date;
```

### 4.2. PostgreSQL → StarRocks SQL migrācijas paraugs

- **PostgreSQL (`DISTINCT ON`):**
  ```sql
  SELECT DISTINCT ON (customer_id)
         order_id, order_date, customer_id, product_id, total_price, status, delivery_date
  FROM orders
  ORDER BY customer_id, order_date DESC, order_id DESC;
  ```
- **StarRocks (`QUALIFY`):**
  ```sql
  SELECT order_id, order_date, customer_id, product_id, total_price, status, delivery_date
  FROM orders
  QUALIFY ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY order_date DESC, order_id DESC
  ) = 1;
  ```

### 4.3. Fināla salīdzinājums: StarRocks vs TimescaleDB (5 miljoni rindu)

- **Testa vaicājums:** `WHERE delivery_date = '2026-06-15'`, tabula particionēta pēc `order_date`. Abās sistēmās rezultāts sakrīt: `cnt = 5438`, `revenue = 2727556.02`.

| **Datubāze**              | **Tabulas konfigurācija**                                             | **Rindu skaits** | **Vaicājuma izpildes laiks**       |
| :------------------------ | :-------------------------------------------------------------------- | :--------------- | :--------------------------------- |
| **TimescaleDB**           | Hipertabula (`order_date`), 25 chunki, bez indeksa uz `delivery_date` | 5 000 000        | mediāna 249,636 ms (196,454–262,551, n=5) |
| **StarRocks**             | PK + `ORDER BY (delivery_date, customer_id)`; BITMAP definēts, bet profilā neizmantots | 5 000 000 | partiju vidējie 223,754 / 78,283 / 82,231 ms |
| **TimescaleDB** (papildu) | Hipertabula + B-tree uz `delivery_date`                               | 5 000 000        | mediāna 5,868 ms (5,173–7,020, n=5) |

| **Ielāde 5M rindu**        | **Laiks** | **Rindas/s** | **MB/s** | **MiB/s** |
| :------------------------- | :-------- | :----------- | :------- | :-------- |
| StarRocks Stream Load, BITMAP | 12,454 s | 401,478 | 26,724 | 25,486 |
| StarRocks Stream Load, bez BITMAP | 12,576 s | 397,585 | 26,466 | 25,240 |
| TimescaleDB `COPY` (ar PK) | 53,178 s | 94,023 | 6,259 | 5,969 |

- **Datu kopa:** testā bija 5 000 000 ierakstu, CSV izmērs bija 332 824 481 baiti, SHA-256 `3cb06dcc1e52306a1ca9d0262e0b608bcda2af83408ce6df8cd1f6a89e8b21db`. Abās StarRocks tabulās un TimescaleDB vaicājumā rezultāts sakrita: `cnt = 5438`, `revenue = 2727556.02`.
- Šie ir iepriekšējā testa rezultāti. 5M CSV fails un testa palaišanas faili vairs nav iekļauti projektā.
- **Arhitektūras skaidrojums un ierobežojumi:** TimescaleDB bez `delivery_date` indeksa plāns skenē chunkus, jo filtrs nav pēc hipertabulas laika ass; ar B-tree plāns izmanto indeksu un izpilde strauji samazinās. StarRocks profils rāda, ka šeit darbu veic kārtošanas/zonu kartes, nevis bitmap indekss. Laika metodes nav tieši salīdzināmas: TimescaleDB `psql` `\timing` un StarRocks klienta partiju vidējie ietver atšķirīgu klienta un izpildes virsslodzi. Tāpēc no šiem skaitļiem netiek pasludināts universāls paātrinājuma koeficients. StarRocks profilu atsevišķais `QueryExecutionWallTime` bija 21,156 ms ar definētu BITMAP un 15,618 ms bez tā; tie ir pa vienam profilētam vaicājumam, nevis atkārtojumu mediānas.

---

# B DAĻA: Reāllaika straumes / CDC konveijers (Flink → StarRocks + Async MV)

## 5. Arhitektūra, datu plūsma un DDL

```
Binance WebSocket -> part2_realtime_pipeline/scripts/binance_feed.py -> JSON faili (binance_out) -> Flink SQL (02_flink_pipeline.sql) -> poc_stream.crypto_trades -> poc_stream.mv_market_1min

```

### 5.1. Avota datu struktūra

Binance combined stream (`btcusdt@trade`, `ethusdt@trade`), viens JSON ziņojums rindā:

```json
{"stream":"btcusdt@trade","data":{"e":"trade","E":1795436774001,"s":"BTCUSDT","t":6900585515,"p":"61305.26000000","q":"0.00010000","T":1795436774000,"m":true,"M":true}}
```

### 5.2. StarRocks reāllaika mērķa tabula

```sql
CREATE TABLE poc_stream.crypto_trades (
  symbol VARCHAR(20) NOT NULL, trade_id BIGINT NOT NULL,
  price DECIMAL(18,8), quantity DECIMAL(18,8), quote_volume DECIMAL(24,8),
  trade_time DATETIME, is_buyer_maker BOOLEAN
)
PRIMARY KEY (symbol, trade_id)
DISTRIBUTED BY HASH(symbol) BUCKETS 4
ORDER BY (symbol, trade_time)
PROPERTIES ("compression"="LZ4", "enable_persistent_index"="true", "replication_num"="1");
```

**Modelēšanas lēmumu pamatojums:**

- **Tabulas modelis (`PRIMARY KEY`):** `(symbol, trade_id)` identificē darījumu unikāli, jo Binance `trade_id` ir unikāls katram simbolam. Atkārtota tā paša ieraksta ielāde pārraksta esošo rindu, nevis veido dublikātu. Viens restarta tests neuzrādīja dublikātus; tā ierobežojumi aprakstīti 8.1. sadaļā.
- **Kārtošanas un sadales atslēgas:** `ORDER BY (symbol, trade_time)` atbilst tipiskajam vaicājumam "darījumi simbolam laika intervālā", un PK ar kārtošanu ir atdalīti, tāpat kā A daļā. `HASH(symbol)` ir sadales atslēga, kas ietilpst PK.
- **Datu tipi:** `DECIMAL` glabā cenu un daudzumu ar 8 zīmēm aiz komata, kā Binance tos atsūta. Tas izvairās no `DOUBLE` noapaļošanas kļūdām. `quote_volume` ir `DECIMAL(24,8)`, tāpēc reizinājums tiek noapaļots līdz 8 zīmēm. To pārbauda `part2_realtime_pipeline/03_health_checks.sql` 2. vaicājums.

### 5.3. Asynchronous Materialized View

```sql
CREATE MATERIALIZED VIEW poc_stream.mv_market_1min
(symbol, candle_minute, trades_count, low_price, high_price, total_base_volume, total_usdt_volume)
DISTRIBUTED BY RANDOM
REFRESH SCHEDULE EVERY(INTERVAL 60 SECOND)
AS SELECT symbol, date_trunc('minute', trade_time) AS candle_minute,
          COUNT(*), MIN(price), MAX(price), SUM(quantity), SUM(quote_volume)
FROM poc_stream.crypto_trades
GROUP BY symbol, date_trunc('minute', trade_time);
```


## 6. Konfigurācija un savienotāji

### 6.1. Avota komponente: `binance_feed.py`

- **Faila ceļš:** `part2_realtime_pipeline/scripts/binance_feed.py`
- **Darbības princips:** savienojas ar Binance WebSocket un ik pēc 2 sekundēm saglabā saņemtos datus JSON failā. Fails vispirms tiek rakstīts kā pagaidu fails un pēc tam pārsaukts, lai Flink nelasītu nepabeigtu failu. Pēc savienojuma pārtrūkšanas skripts mēģina pieslēgties no jauna. Pārtrūkuma brīdī buferī esošie dati var pazust; tas netika pārbaudīts.

### 6.2. Flink SQL konfigurācija

```sql
SET 'execution.runtime-mode' = 'streaming';
SET 'table.local-time-zone' = 'UTC';
SET 'execution.checkpointing.interval' = '10 s';

CREATE TABLE flink_binance_source (
    `stream` STRING,
    `data` ROW<`s` STRING, `t` BIGINT, `p` DECIMAL(18,8),
               `q` DECIMAL(18,8), `T` BIGINT, `m` BOOLEAN>
) WITH (
    'connector' = 'filesystem',
    'path' = '/data/binance',
    'format' = 'json',
    'source.monitor-interval' = '2 s'
);

CREATE TABLE sr_crypto_trades (
    symbol STRING,
    trade_id BIGINT,
    price DECIMAL(18, 8),
    quantity DECIMAL(18, 8),
    quote_volume DECIMAL(24, 8),
    trade_time TIMESTAMP(3),
    is_buyer_maker BOOLEAN,
    PRIMARY KEY (symbol, trade_id) NOT ENFORCED
) WITH (
    'connector' = 'starrocks',
    'jdbc-url' = 'jdbc:mysql://quickstart:9030',   
    'load-url' = 'quickstart:8030',               
    'database-name' = 'poc_stream',
    'table-name' = 'crypto_trades',
    'username' = 'root',
    'password' = '',                               
    'sink.buffer-flush.max-rows' = '64000',
    'sink.buffer-flush.interval-ms' = '3000'
);

INSERT INTO sr_crypto_trades
SELECT
    `data`.`s`,
    `data`.`t`,
    `data`.`p`,
    `data`.`q`,
    CAST(`data`.`p` * `data`.`q` AS DECIMAL(24,8)),
    CAST(TO_TIMESTAMP_LTZ(`data`.`T`, 3) AS TIMESTAMP(3)),
    `data`.`m`
FROM flink_binance_source;

```

### 6.3. Kritisko parametru skaidrojums

| **Parametrs**                      | **Vērtība** | **Nozīme un tehniskais pamatojums**                                                                                                    |
| :--------------------------------- | :---------- | :------------------------------------------------------------------------------------------------------------------------------------- |
| `sink.buffer-flush.max-rows`       | `64000`     | Rindu robeža vienā ielādē. Pie izmērītā ātruma 64,75 rindas/s tā netiek sasniegta, tāpēc noteicošais ir laika intervāls. |
| `sink.buffer-flush.interval-ms`    | `3000`      | Dati tiek sūtīti aptuveni reizi 3 sekundēs jeb vidēji ap 194 rindām vienā ielādē. Tas samazina biežu mazu ielāžu skaitu, bet var pielikt līdz 3 sekundēm aiztures. |
| `execution.checkpointing.interval` | `10 s`      | Checkpoint intervāls Flink darbam. Konektora uzvedību pie checkpoint šajā PoC nepētīju.                                                                                                     |
| `source.monitor-interval`          | `2 s`       | Cik bieži Flink meklē jaunus failus mapē. Kopā ar 2 s faila ierakstīšanas partiju tas pievieno aiztures.                                                                                    |
| `REFRESH SCHEDULE EVERY`           | `60 SECOND` | MV atjauno reizi minūtē, tātad sveču dati kavējas līdz \~60 s.                                                                                                                              |
| `partial_update`                   | N/A         | Šajā konveijerā netiek izmantots, katrs ieraksts ir pilna rinda. Skat. 11. sadaļu.                                                                                                          |

## 7. Palaišanas instrukcija no nulles (Runbook)

### 7.1. Priekšnosacījumi un vides palaišana

```powershell
New-Item -ItemType Directory -Force connectors, binance_out | Out-Null
Copy-Item .env.example .env

(Get-Item .\connectors\flink-connector-starrocks.jar).Length
docker compose up -d
docker compose ps

$ready = $false
for ($i = 0; $i -lt 60; $i++) {
  docker compose exec -T quickstart mysql -h localhost -P 9030 -u root -e "SELECT 1" 2>$null
  if ($LASTEXITCODE -eq 0) { $ready = $true; break }
  Start-Sleep 5
}
if (-not $ready) { throw "StarRocks FE did not become ready within 5 minutes." }
```

`connectors\flink-connector-starrocks.jar` ir projektā, un tā versija ir `1.2.9_flink-1.18`. Flink izmanto `quickstart:9030` (JDBC) un `quickstart:8030` (Stream Load). `be-proxy` pārsūta datu ielādes savienojumu uz StarRocks.

### 7.2. Palaišanas soļi stingrā secībā

1. **Izveidot StarRocks datubāzi, tabulu un Materialized View:**
   ```powershell
   docker compose cp .\part2_realtime_pipeline\01_starrocks_stream_ddl.sql quickstart:/tmp/
   docker compose exec -T quickstart bash -c "mysql -h localhost -P 9030 -u root < /tmp/01_starrocks_stream_ddl.sql"
   ```
2. **Palaist avota plūsmu (atsevišķā logā, atstāt darboties):**
   ```powershell
   python -m pip install websockets==17.4
   python .\part2_realtime_pipeline\scripts\binance_feed.py files
   (Get-ChildItem .\binance_out -File | Measure-Object).Count
   ```
   `binance_out/` pirms starta jābūt tukšai: Flink lasa visu mapi, un vecie faili tiktu ielādēti atkārtoti un sagrozītu caurlaides mērījumu.
3. **Palaist Flink SQL darbu:**
   ```powershell
   docker compose cp .\part2_realtime_pipeline\02_flink_pipeline.sql jobmanager:/tmp/
   docker compose exec -T jobmanager ./bin/sql-client.sh -f /tmp/02_flink_pipeline.sql
   docker compose exec -T jobmanager flink list
   ```

Ja `taskmanager` tiek pārstartēts, jāpārstartē arī starpnieks: `docker compose restart taskmanager be-proxy`.
Compose Flink servisā ir uzstādīts arī PostgreSQL CDC konektors, bet CDC demonstrācija ir atsevišķs darbs, nevis daļa no Binance pamatdarba. CDC demonstrācijas SQL un palaišanas skripti pēc projekta tīrīšanas vairs nav iekļauti.

### 7.3. Pārbaude un validācija (Health Checks)

Pēc aptuveni 1–2 minūtēm palaidiet veselības pārbaužu SQL failu StarRocks konteinerī:

```powershell
docker compose cp .\part2_realtime_pipeline\03_health_checks.sql quickstart:/tmp/
docker compose exec -T quickstart bash -c "mysql -h localhost -P 9030 -u root < /tmp/03_health_checks.sql"
```

Gaidāms: `rows_total` aug, `bad_rows = 0`, un MV ir dati par simboliem un minūtēm. `task_runs` rāda, vai MV atjaunošana ir izdevusies.

## 8. Robežgadījumi, konsistence un kļūdu novēršana

### 8.1. Arhitektūras un robežgadījumu analīze

- **Pārāk daudz datu versiju (`too many versions`):** biežas ielādes palielina versiju skaitu, ko vēlāk apvieno fona process. Pie izmērītā ātruma un 3 sekunžu bufera sanāk apmēram viena ielāde 3 sekundēs. Faktisko ielāžu un versiju skaitu nenomērījām. Pie 50 vai vairāk ielādēm sekundē to nepārbaudījām.
- **Restarts un idempotence (izmērīts):** feed apturēts, rindu skaits stabils (264 059). Darbs atcelts un iesniegts no jauna bez saglabāta stāvokļa (`flink cancel` + atkārtots `sql-client.sh -f`), mapē 1448 faili. Pēc 180 s rindu skaits bija 264 059, tāpat kā pirms restarta, `COUNT(DISTINCT symbol, trade_id) = 264 059`. Tātad atkārtota nolasīšana neradīja dublikātus. Ierobežojums: nav apstiprināts, ka darbs visus 1448 failus tiešām izlasīja vēlreiz (skat. `stream_loads`), un tas ir viens tests uz viena mezgla.
- **Savienojuma pārrāvumi / Backpressure / TOAST:**
  - *Pārrāvums starp Flink un StarRocks BE (novērots):* kamēr BE nebija sasniedzams, darbs bija `RESTARTING` un dati neienāca. Pēc `be-proxy` pievienošanas darbs pats atsāka darbu un dati sāka plūst.
  - *Pārrāvums Binance WebSocket:* `binance_feed.py` pēc `ConnectionClosed` atkārtoti pieslēdzas (`async for ws in websockets.connect(...)`). Kods paredz atkārtotu pieslēgšanos, taču faktiskā uzvedība pārrāvuma laikā nav testēta. Lai to pārbaudītu, īslaicīgi jāpārtrauc tīkla savienojums, jāatjauno tas un jāpārbauda, vai skripts atsāk saņemt ziņojumus, vai pazūd dati buferī un vai pēc atjaunošanās rodas dublikāti.
  - *Pārslodze:* netika testēta. Jāpalielina avota plūsmas ātrums un jāskatās, vai Flink tiek galā un kā mainās datu aizture. 64,75 rindas/s ir šī testa vidējais ātrums, nevis maksimālā jauda.
  - *TOAST / lielie lauki:* šajā konveijerā nav, skat. 11. sadaļu.

### 8.2. Sastaptās kļūdas un risinājumi

| **Simptoms / Kļūdas ziņojums**                                                                                                 | **Cēlonis**                                                                                                                                         | **Risinājums**                                                                                                                                                                                                                                                 |
| :----------------------------------------------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `unique table's partition column must be key column`                                                                           | `PRIMARY KEY(order_id)` ar `PARTITION BY date_trunc('month', order_date)`                                                                           | Partīcijas kolonna iekļauta PK: `PRIMARY KEY (order_id, order_date)`                                                                                                                                                                                           |
| `TIMESTAMPDIFF(MICROSECOND, ...)` apstādina vaicājumu | Šajā StarRocks versijā šī laika vienība nedarbojās | Pārbaudei izmantots `SHOW MATERIALIZED VIEWS` un `task_runs` |
| `ERROR 1064 ... Can't support result other than column` | `QUALIFY` vaicājums neatbalstīja sākotnējo kolonnu formu | Izmantots `ROW_NUMBER()` ar konkrēti uzskaitītām kolonnām, kā parādīts 4.2. sadaļā |
| Flink darbs pārstartējas, un StarRocks nesaņem datus | StarRocks BE adrese nebija sasniedzama no Flink | Compose failā pievienots `be-proxy`; tas pārsūta savienojumu uz StarRocks |

## 9. Reāllaika konveijera veiktspēja

2026-11-26 mērījumā Binance BTCUSDT un ETHUSDT dati tika vākti 240 sekundes, ik pēc 2 sekundēm. Pašreiz projektā ir svaiguma mērīšanas skripts `part2_realtime_pipeline/scripts/measure_freshness.py`; iepriekšējā testa paraugu faili vairs nav iekļauti.

Lai mērījumu palaistu no jauna, vispirms jāpalaiž datu plūsma un pēc tam:

```powershell
python .\part2_realtime_pipeline\scripts\measure_freshness.py
```

Skripts izveido `benchmark_results` mapi un saglabā tajā jaunā mērījuma failus.

Svaiguma un caurlaidības rādītāji tabulā ir no šī 2026-11-26 testa; MV un datu pareizības rādītāji ir no agrākas atsevišķas health-check pārbaudes.

| **Rādītājs**                      | **Izmērītā vērtība**                                           | **Testa apstākļi / Piezīmes**                                                                                                                                                                                                                                                                    |
| :-------------------------------- | :------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Datu svaiguma nobīde**          | mediāna **4,074 s**, p95 **6,202 s**, max **7,652 s** (120 paraugi) | `CURRENT_TIMESTAMP(3) - MAX(trade_time)` StarRocks pusē; p95 rēķināts ar nearest-rank metodi. Tas mēra jaunākā saglabātā darījuma vecumu, nevis katra notikuma precīzu end-to-end aizturi; to ietekmē arī Binance/Docker pulksteņu atšķirība. |
| **Ielādes ātrums (Throughput)**   | **64,75 rindas/s** (15 540 jaunas rindas / 240 s)                | Novērotais vidējais šajā konkrētajā logā, nevis maksimālā jauda. Avota mapē izveidojās 107 JSON partiju faili.                                                                                                                                                                                  |
| **Ielādes biežums (Micro-batch)** | Nav izmērīts                                                 | Konfigurācija: `sink.buffer-flush.interval-ms = 3000`.                                                                                                                                                                                                                                           |
| **MV atjaunošanās laiks**         | `last_refresh_duration` **0,874 s** (agrākā palaišanā 1,249 s) | Agrāka atsevišķa pārbaude: `information_schema.task_runs` rādīja SUCCESS ik minūti. Blakus bija SKIPPED ieraksti citam MV (`mv-32685`), visticamāk vecajam `fintech_stream.mv_market_1min`; tas nav pārbaudīts. |
| **Datu pareizība**                | 212 770 rindas, `bad_rows = 0` (agrāks tests)               | `part2_realtime_pipeline/03_health_checks.sql` 2. vaicājums pārbaudīja, vai `quote_volume` sakrīt ar `price * quantity`. Svaiguma mērījumā šī pārbaude netika veikta. |

Pirmajā 240 sekunžu mēģinājumā dati uz brīdi pārstāja pienākt. Pēc tam skriptam pievienoja 30 sekunžu gaidīšanas robežu un atkārtotu pieslēgšanos. Atkārtotajā testā svaiguma rādītājs visu laiku bija zem 8 sekundēm. Pirmā mēģinājuma dati vairs nav saglabāti projektā.

## 10. Secinājumi un gatavība produkcijai

* **Kas strādā izcili:** 5 miljonu rindu ielāde StarRocks aizņēma ap 12,5 sekundēm. Reāllaika testā 4 minūšu laikā tika ielādētas 15 540 rindas; jaunākā darījuma vecums parasti bija dažas sekundes. Vienā darba restartēšanas testā dublikātus neredzējām.
* **Identificētie ierobežojumi PoC versijā:** testi veikti vienā datorā ar `replication_num = 1`, tāpēc nav pārbaudīta vairāku mezglu darbība un kļūmju atkopšana. Ilgstoša slodze un pārslodze netika testēta. Flink checkpoint atjaunošana un datu atkārtošana pēc restartēšanas nav pilnībā pārbaudīta. 5M datu fails un CDC testa palaišanas faili vairs nav projektā, tāpēc šos testus nevar palaist no jauna bez failu atjaunošanas.
* **Rekomendācijas produkcijai:** pirms lietošanas produkcijā pārbaudīt, ka StarRocks un Flink konektoru versijas ir saderīgas. Saglabāt Flink checkpoint drošā ārējā glabātuvē, piemēram, S3, un izmēģināt atjaunošanu. Ieslēgt partīciju tīrīšanu ar termiņu, ja vecie dati vairs nav vajadzīgi. Uzlikt brīdinājumus par datu aizturi, neveiksmīgām MV atjaunošanām un pārāk biežām ielādēm. Pārbaudīt sistēmu ar ilgstošu slodzi, vairākiem mezgliem un rezerves kopijas atjaunošanu. Paroles glabāt ārpus SQL failiem un nodrošināt piekļuves kontroli.

---

## 11. Specifiskā FinTech / CDC arhitektūra: TOAST, `partial_update` un JSON lauki

### 11.1. PostgreSQL CDC un `TOAST` lauku problēmas risinājums
* **Avota tabulas DDL un `REPLICA IDENTITY`:**
  ```sql
  CREATE TABLE IF NOT EXISTS public.orders_toast_test (
      order_id     BIGINT PRIMARY KEY,
      status       VARCHAR(20),
      raw_payload  TEXT,
      updated_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
  );

  ALTER TABLE public.orders_toast_test
      ALTER COLUMN raw_payload SET STORAGE EXTERNAL;

  ALTER TABLE public.orders_toast_test
      REPLICA IDENTITY FULL;

  DO $$
  BEGIN
      IF NOT EXISTS (
          SELECT 1 FROM pg_publication WHERE pubname = 'cdc_orders_pub'
      ) THEN
          EXECUTE 'CREATE PUBLICATION cdc_orders_pub FOR TABLE public.orders_toast_test';
      ELSIF NOT EXISTS (
          SELECT 1 FROM pg_publication_tables
          WHERE pubname = 'cdc_orders_pub'
            AND schemaname = 'public'
            AND tablename = 'orders_toast_test'
      ) THEN
          EXECUTE 'ALTER PUBLICATION cdc_orders_pub ADD TABLE public.orders_toast_test';
      END IF;
  END;
  $$;
  ```
* **Kāpēc izvēlēts šāds `REPLICA IDENTITY`:** `DEFAULT` WAL vecajā rindas identitātes daļā izmanto primāro atslēgu (`order_id`) un parasti rada mazāku WAL apjomu. Pirmajā testā ar `DEFAULT` Flink CDC 3.2.1/Debezium noraidīja UPDATE ar kļūdu `The "before" field ... is null`; šai demonstrācijai iestatīts `FULL`, kas vecajā attēlā iekļauj visas kolonnas. `FULL` var palielināt WAL, īpaši bieži atjaunojot rindas ar lieliem laukiem; šī izmaksu atšķirība netika mērīta.
* **Kas notiek ar nemainītu `TOAST` lauku `UPDATE` brīdī:** pirmajā testā ar `DEFAULT` PostgreSQL `pgoutput`/Debezium 1.9.8 UPDATE notikumā nemainītais `raw_payload` jaunajā attēlā bija atzīmēts ar `__debezium_unavailable_value`, nevis pilnu JSON saturu. Tas nav avotā apzināts SQL `NULL`. Flink sink shēmā `raw_payload` ir izlaists, un `partial_update_mode=column` nosūta tikai `order_id,status,updated_at`, tādēļ StarRocks statusa UPDATE saglabā iepriekšējo payload. Payload sākotnēji tika iesēts abās pusēs ar vienādu saturu; pārbaudīts ir daļēja statusa UPDATE saglabāšanas gadījums, nevis vispārīgs pilna payload backfill.

### 11.2. StarRocks `partial_update` konfigurācija un uzvedība
* **Izmantotie konektora un tabulas iestatījumi:**
  ```sql
  CREATE TABLE IF NOT EXISTS poc_stream.orders_toast_partial (
      order_id     BIGINT NOT NULL,
      status       VARCHAR(20),
      raw_payload  VARCHAR(65533),
      updated_at   DATETIME
  )
  ENGINE = OLAP
  PRIMARY KEY (order_id)
  DISTRIBUTED BY HASH(order_id) BUCKETS 4
  PROPERTIES (
      "enable_persistent_index" = "true",
      "replication_num" = "1"
  );

  CREATE TABLE sr_orders_status_partial (
      order_id BIGINT,
      status STRING,
      updated_at TIMESTAMP(3),
      PRIMARY KEY (order_id) NOT ENFORCED
  ) WITH (
      'connector' = 'starrocks',
      'jdbc-url' = 'jdbc:mysql://quickstart:9030',
      'load-url' = 'quickstart:8030',
      'database-name' = 'poc_stream',
      'table-name' = 'orders_toast_partial',
      'username' = 'root',
      'password' = '',
      'sink.properties.partial_update' = 'true',
      'sink.properties.partial_update_mode' = 'column',
      'sink.properties.columns' = 'order_id,status,updated_at,__op',
      'sink.buffer-flush.interval-ms' = '1000'
  );
  ```
* **Darbības princips un ierobežojumi:**
  * **Kā tiek aizsargātas nemainītās kolonnas:** `partial_update_mode = 'column'` atjaunina vienu un to pašu kolonnu kopu visiem slodzes ierakstiem; šis sinks sūta tikai `order_id,status,updated_at`. `raw_payload` nav ne Flink sink shēmā, ne `sink.properties.columns` sarakstā, tāpēc StarRocks saglabā jau esošo vērtību. `row` režīms ir piemērots, ja rindām vienas slodzes ietvaros atšķiras atjaunināmo kolonnu kopas; šis demonstrācijas piemērs to neizmanto.
  * **Ko darīt, ja kolonna tiešām tiek atjaunināta uz `NULL`:** šis šaurais sinks payload kolonnu neatjaunina, tāpēc saglabā esošo payload, bet nevar iestatīt payload uz SQL `NULL`. Lai atbalstītu abus gadījumus, CDC slānim jāsaglabā izmaiņu pazīme `raw_payload_changed` un jānovirza patiesi mainītās payload vērtības, ieskaitot apzinātu `NULL`, uz atsevišķu sink ceļu. Vienkāršs `IS NULL` nav pietiekams, ja konektors nepieejamu TOAST vērtību arī attēlo kā null.
  * **Novēloti / atkārtoti notikumi (Out-of-order / Idempotence):** PRIMARY KEY upsert novērš dublikāta rindu tam pašam `order_id`, bet pats nenoraida novecojušu notikumu; parasti uzvar pēdējais pienākušais ieraksts. `updated_at` tiek saglabāts, bet skripts neveic versiju salīdzināšanu vai vecu notikumu filtrēšanu. Šajā testā netika validēta novēlota notikuma kārtošana, restartēšana no checkpoint vai dublikātu uzvedība.

### 11.3. Lielo JSON / vārteju datu glabāšana un vaicāšana
* **Izvēlētais datu tips StarRocks tabulā:** `raw_payload` glabāts kā `VARCHAR(65533)`, nevis `JSON`, lai saglabātu testa payload kā tekstu un izmantotu `get_json_string` JSON atribūtu vaicājumiem. Tas ierobežo vienas vērtības maksimālo izmēru; produkcijas JSON tipa izvēle jābalsta uz izmēra un vaicājumu testiem.
* **Pārbaudes vaicājums (lauka izvilkšana un filtrēšana pēc `UPDATE`):**
  ```sql
  SELECT order_id, status, updated_at,
         get_json_string(raw_payload, '$.eventType') AS event_type,
         get_json_string(raw_payload, '$.gatewayId') AS gateway_id,
         LENGTH(raw_payload) AS payload_chars, md5(raw_payload) AS payload_md5
  FROM poc_stream.orders_toast_partial WHERE order_id = 1001;
  SELECT order_id, status FROM poc_stream.orders_toast_partial
  WHERE get_json_string(raw_payload, '$.gatewayId') = 'fintech-gateway-1';
  ```
* **Veiktspējas un izmēra novērojumi:** šajā 4 668 rakstzīmju statusa UPDATE testā payload kolonnu uz StarRocks pārsūtīt nevajadzēja; šis nav veiktspējas salīdzinājums. Lieli ieraksti palielina datu apjomu un var ietekmēt sākotnējo Stream Load, compaction, replikāciju un JSON izvilkšanas CPU izmaksas. Payload ielādes ātrums, `VersionCount`, compaction un JSON filtru aizture nav mērīti.
