# CDC Pipeline — Near Real-Time Lakehouse

> **Portfolio navigation:** For the expanded reference architecture, dashboard/API, Kubernetes manifests and observability tooling, see [CDC Pipeline Big Data](https://github.com/Elghouali/cdc-pipeline-bigdata). This repository preserves the original local lakehouse implementation and its setup instructions.

A fully self-contained CDC (Change Data Capture) pipeline that captures
row-level changes from a PostgreSQL e-commerce database and materialises
them as Iceberg tables in a Medallion Lakehouse.

Everything runs on a single machine with Docker Compose — no cloud needed.

---

## Architecture

```
PostgreSQL (WAL logical replication)
       │
       │  pgoutput replication slot + publication
       ▼
Kafka Connect  +  Debezium 2.5
       │
       │  topics: cdc.public.{customers,products,orders,order_items,payments,returns}
       ▼
Apache Kafka
       │
       ├── Spark Bronze Job ──► Iceberg Bronze  (raw CDC events, append)
       │                               │
       │           Spark Silver Job ◄──┘
       │                   │
       │                   ▼
       │           Iceberg Silver  (clean, typed, upserted by PK)
       │                   │
       │           Spark Gold Job
       │                   │
       │                   ▼
       │           Iceberg Gold  (business aggregates, 5-min refresh)
       │                   │
       │                   ▼
       │           Trino 435  (SQL query engine)
       │                   │
       │                   ▼
       │           Apache Superset  (dashboards)
       │
       ├── MinIO s3a://warehouse/   ← all Iceberg data files
       └── Hive Metastore           ← Iceberg catalog (backed by MariaDB)
```

### Medallion Layers

| Layer  | Table pattern | Description |
|--------|---------------|-------------|
| Bronze | `bronze_{entity}_cdc` | Raw Debezium JSON, append-only, partitioned by `ingestion_date` |
| Silver | `silver_{entity}` | Typed, deduplicated, MERGE INTO upsert, partitioned by `updated_date` |
| Gold   | `gold_*` | Business aggregates refreshed every 5 min, partitioned by `snapshot_date` |

---

## Prerequisites

| Requirement | Minimum |
|-------------|---------|
| Docker Desktop | 24.x with Docker Compose v2 |
| RAM allocated to Docker | 8 GB |
| Free disk space | 20 GB |
| OS | Linux, macOS, or Windows with WSL2 |

No local JVM, Python, or cloud account required.

---

## Step-by-Step Run Guide

### Step 0 — Prerequisites check

Make sure Docker Desktop is running and has at least **8 GB RAM** allocated:

- Windows / Mac: Docker Desktop → Settings → Resources → Memory ≥ 8 GB
- Verify Docker is working:

```bash
docker --version        # must be 24.x or later
docker compose version  # must be v2.x
```

---

### Step 1 — Clone / enter the project

```bash
cd cdc-pipeline
```

All commands below assume you are in the `cdc-pipeline/` directory.

---

### Step 2 — Create local credentials

```bash
cp .env.example .env
# Edit .env and replace every example password and secret before starting.
```

Use a **private, locally created** `.env` file for this demonstration stack. The example values are development placeholders only. Never commit a real secret, expose these services to the public internet, or reuse the example passwords in a deployment. All services read their configuration from this file at startup.

---

### Step 3 — Start the full pipeline

```bash
bash scripts/start.sh
```

This single command does everything in order:

| What happens | Typical duration |
|---|---|
| Docker pulls all 12 images (first run only) | 3–8 min |
| All containers start and health-check passes | 1–2 min |
| Debezium connector registers and starts snapshot | 20–30 s |
| Spark Bronze job starts streaming from Kafka | 2–5 min (JAR download, first run) |
| Spark Silver job starts consuming Bronze | ~10 s after Bronze |
| Spark Gold job starts 5-min aggregation cycle | ~10 s after Silver |
| Traffic simulator starts (INSERT/UPDATE/DELETE loop) | immediate |

The script blocks and keeps the simulator running. You will see this when ready:

```
[HH:MM:SS] ═══════════════════════════════════════════════
[HH:MM:SS]   Pipeline is LIVE
[HH:MM:SS] ═══════════════════════════════════════════════
```

---

### Step 4 — Verify services are up

Open these URLs in your browser:

| Service | URL | Login |
|---|---|---|
| **Dashboard CDC (portail unique)** | **http://localhost:5000** | **(no auth)** |
| Spark Master UI | http://localhost:8085 | (no auth) |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Kafka Connect | http://localhost:8083/connectors | (no auth) |
| Trino UI | http://localhost:8080 | (no auth) |
| Superset | http://localhost:8088 | admin / admin |

**Check Spark**: http://localhost:8085 should show 3 running applications (`cdc-bronze-ingestion`, `cdc-silver-transform`, `cdc-gold-aggregation`).

**Check Kafka connector**:
```bash
curl -s http://localhost:8083/connectors/ecommerce-postgres-connector/status \
  | python3 -m json.tool
```
Look for `"state": "RUNNING"` in the output.

**Check MinIO**: http://localhost:9001 → browse `warehouse/` bucket → you should see `analytics/bronze_*` folders appearing within 30–60 s.

---

### Step 5 — Query data with Trino

Wait ~2 minutes after the pipeline starts, then open the Trino CLI:

```bash
docker exec -it cdc-trino trino --catalog iceberg --schema analytics
```

Run a quick sanity check:

```sql
-- How many CDC events landed in Bronze?
SELECT source_table, COUNT(*) AS events
FROM (
  SELECT source_table FROM bronze_customers_cdc
  UNION ALL SELECT source_table FROM bronze_orders_cdc
  UNION ALL SELECT source_table FROM bronze_payments_cdc
) GROUP BY source_table;

-- Check Silver rows (upserted, deduplicated)
SELECT COUNT(*) FROM silver_customers;
SELECT COUNT(*) FROM silver_orders;

-- Check Gold (available after first 5-min cycle)
SELECT * FROM gold_orders LIMIT 5;
```

Type `exit` to leave the CLI.

---

### Step 5b — Open the unified Dashboard

Le dashboard unifié regroupe **toutes les informations du projet sur une seule page** :
statut des services, KPI analytiques live (via Trino), connecteurs Debezium,
applications Spark, graphiques et liens vers les autres UIs.

Ouvre simplement : **http://localhost:5000**

Onglets disponibles :

| Onglet | Contenu |
|--------|---------|
| Vue d'ensemble | Cartes KPI (CDC, commandes, clients, CA, latence) + liens rapides |
| Services | État UP/DOWN de Postgres, Kafka, Connect, Spark, Trino, MinIO, Superset |
| CDC & Bronze | Volume d'évènements par table, ventilation INSERT/UPDATE/DELETE, latence |
| Analytique Gold | Ventes mensuelles, mix paiement, taux annulation/retour, top 10 clients |
| Architecture | Schéma + description des couches Bronze/Silver/Gold |

Auto-refresh toutes les 15 secondes. Cliquer sur **↻ Rafraîchir** pour forcer.

---

### Step 6 — Set up the Superset dashboard

See **[superset_dashboard.md](superset_dashboard.md)** for the full chart-by-chart walkthrough. Quick summary:

1. Go to http://localhost:8088 → log in as `admin / admin`
2. **Settings → Database Connections → + Database** → choose **Trino**
3. SQLAlchemy URI: `trino://admin@trino:8080/iceberg` → Test → Connect
4. **Data → Datasets** → register each Gold table (`gold_orders`, `gold_sales_facts`, etc.)
5. **Dashboards → + Dashboard** → add charts from `trino/trino_queries.sql`

---

### Step 7 — Watch live data flow

The traffic simulator runs every 5 seconds. Verify end-to-end flow:

```bash
# Watch Kafka topics receive CDC events
docker exec cdc-kafka \
  kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic cdc.public.orders --from-beginning --max-messages 5

# Tail Spark Bronze logs
docker logs -f cdc-spark-master 2>&1 | grep "bronze"

# Tail Spark Silver logs
docker logs -f cdc-spark-master 2>&1 | grep "silver"
```

---

### Step 8 — Stop and clean up

```bash
# Stop all containers and delete all volumes (full reset)
docker compose down -v

# Stop containers only (keep data volumes)
docker compose down
```

### Service URLs

| Service          | URL                        | Login |
|------------------|----------------------------|-------|
| **Dashboard CDC**| **http://localhost:5000**  | (no auth) |
| Superset         | http://localhost:8088      | admin / admin |
| Trino UI         | http://localhost:8080      | (no auth) |
| Spark Master UI  | http://localhost:8085      | (no auth) |
| MinIO Console    | http://localhost:9001      | minioadmin / minioadmin |
| Kafka Connect    | http://localhost:8083      | (no auth) |
| PostgreSQL       | localhost:5432             | postgres / postgres |

---

## Folder Structure

```
cdc-pipeline/
├── docker-compose.yml              All 13 services (dashboard included)
├── .env                            Credentials (do not commit real secrets)
├── dashboard/                      Unified web dashboard (Flask + Chart.js)
│   ├── app.py                      Flask API + Trino client
│   ├── requirements.txt
│   ├── Dockerfile
│   └── static/                     index.html, app.js, style.css
├── config/
│   ├── debezium-connector.json     Kafka Connect source connector
│   ├── hive/
│   │   └── hive-site.xml           Metastore DB + S3A settings
│   ├── spark/
│   │   └── spark-defaults.conf     Iceberg catalog, S3A, JAR packages
│   └── trino/
│       ├── config.properties
│       ├── jvm.config
│       ├── node.properties
│       └── catalog/iceberg.properties
├── sql/
│   ├── init.sql                    DDL: 6 tables + Debezium user + publication
│   └── seed.sql                    ~100 seed rows per table
├── scripts/
│   ├── start.sh                    Full pipeline startup (entry point)
│   ├── register_connector.sh       POST connector to Kafka Connect REST API
│   └── simulate_changes.sh         Live INSERT / UPDATE / DELETE loop
├── spark_jobs/
│   ├── bronze_job.py               Kafka -> Bronze Iceberg (streaming append)
│   ├── silver_job.py               Bronze -> Silver (MERGE INTO upsert)
│   └── gold_job.py                 Silver -> Gold (5-min batch aggregation)
├── trino/
│   └── trino_queries.sql           8 KPI queries + 2 bonus queries
├── superset_dashboard.md           Step-by-step Superset setup guide
└── README.md                       This file
```

---

## Source Schema

Six e-commerce tables in PostgreSQL, `ecommerce` database, `public` schema:

| Table | Columns |
|-------|---------|
| `customers` | id, name, email, city, segment, signup_date |
| `products` | id, name, category, brand, price, stock_qty |
| `orders` | id, order_date, customer_id, status, channel |
| `order_items` | id, order_id, product_id, quantity, unit_price |
| `payments` | id, order_id, payment_date, method, amount, status |
| `returns` | id, order_id, return_date, reason, status |

---

## KPI Descriptions

| # | KPI | Source table |
|---|-----|-------------|
| 1 | CDC event counts by table | Bronze (UNION ALL) |
| 2 | Operation type breakdown (c/u/d/r) | Bronze (UNION ALL) |
| 3 | Source-to-analytics latency p95 | Bronze customers/orders/payments |
| 4 | Order status evolution (weekly) | `gold_orders` |
| 5 | Total sales by period and channel | `gold_sales_facts` |
| 6 | Payment method mix by month | `gold_payments_summary` |
| 7 | Cancellation and return rates | `gold_orders` |
| 8 | Revenue by segment and product category | `gold_sales_facts` |

Full SQL for all 8 KPIs is in `trino/trino_queries.sql`.
Chart-by-chart Superset setup is in `superset_dashboard.md`.

---

## Useful Commands

```bash
# Check connector status
curl -s http://localhost:8083/connectors/ecommerce-postgres-connector/status \
  | python3 -m json.tool

# Open Trino CLI
docker exec -it cdc-trino trino --catalog iceberg --schema analytics

# Re-register connector (after restart)
bash scripts/register_connector.sh

# Tail Spark logs
docker logs -f cdc-spark-master

# Stop and clean up all volumes
docker compose down -v
```

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| Hive Metastore exits on first start | Wait 2 min — it downloads the MySQL JDBC driver on first boot |
| Spark job S3A errors | Verify `config/spark/spark-defaults.conf` matches `.env` credentials |
| No CDC events in Kafka | Re-run `register_connector.sh`; check `docker logs cdc-kafka-connect` |
| Gold tables empty in Trino | Silver must run at least one batch first (~60-90 s after Bronze starts) |
| Docker out of memory | Increase Docker Desktop memory limit to 8 GB |
| Superset "No module trino" | Container installs it on first boot; wait 2–3 min for `superset run` to appear |

---

## First-Run Timing Reference

| Service | Typical time to healthy |
|---------|------------------------|
| PostgreSQL | 10–15 s |
| Kafka + Zookeeper | 20–30 s |
| Kafka Connect / Debezium | 30–60 s |
| Hive Metastore (schema init) | 60–120 s |
| Trino | 30–60 s |
| Spark (JAR download, first run only) | 2–5 min |
| Superset (pip install trino, first run) | 2–3 min |

`start.sh` waits for each service before proceeding — no manual timing needed.
