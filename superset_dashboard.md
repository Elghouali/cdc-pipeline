# Superset Dashboard Setup

Step-by-step guide to connect Apache Superset to Trino and build the CDC pipeline KPI dashboard.

## Prerequisites

- All services running: `./scripts/start.sh`
- Spark jobs have processed at least one Bronze → Silver → Gold cycle
- Superset: http://localhost:8088  (admin / admin)
- Trino:    http://localhost:8080

---

## Step 1 — Add Trino as a Database

1. Log in to Superset at **http://localhost:8088** with `admin / admin`.
2. Go to **Settings → Database Connections → + Database**.
3. Select **Trino** from the supported databases list.
4. Fill in the connection form:

   | Field              | Value                                    |
   |--------------------|------------------------------------------|
   | Display name       | `CDC Trino`                              |
   | SQLAlchemy URI     | `trino://admin@trino:8080/iceberg`       |
   | Expose in SQL Lab  | checked                                  |
   | Allow DML          | checked                                  |

   > From the host machine use `localhost` instead of `trino`.

5. Click **Test Connection** — confirm *Connection looks good!*
6. Click **Connect**.

---

## Step 2 — Register Datasets

For each Gold table, create a Superset dataset:

1. **Data → Datasets → + Dataset**
2. Database: `CDC Trino`  |  Schema: `analytics`
3. Register these tables:

   | Table                   | Time column   |
   |-------------------------|---------------|
   | `gold_customers`        | `snapshot_date` |
   | `gold_orders`           | `order_date`    |
   | `gold_sales_facts`      | `sale_date`     |
   | `gold_payments_summary` | (none — text month) |
   | `gold_returns_summary`  | (none — text month) |
   | `bronze_orders_cdc`     | `ingestion_timestamp` |

---

## Step 3 — Create the Dashboard

1. **Dashboards → + Dashboard** → name it **CDC Pipeline KPIs**
2. **Save** → **Edit Dashboard**

---

## Step 4 — Add Charts (one per KPI)

### KPI 1 — CDC Event Counts by Table

- **Chart type:** Table
- **SQL:** paste KPI 1 from `trino/trino_queries.sql`
- Columns: `source_table`, `total_events`, `inserts`, `updates`, `deletes`, `snapshots`
- Save as **"CDC Event Counts by Table"** → add to dashboard.

---

### KPI 2 — Operation Type Breakdown

- **Chart type:** Stacked Bar Chart
- **SQL:** paste KPI 2 from `trino_queries.sql`
- X-axis: `source_table`  |  Bars: `op`  |  Metric: `event_count`
- Save as **"Op Type Breakdown per Table"**.

---

### KPI 3 — Source-to-Analytics Latency

- **Chart type:** Table
- **SQL:** paste KPI 3 from `trino_queries.sql`
- Columns: `source_table`, `avg_latency_sec`, `p95_latency_sec`, `max_latency_sec`
- Add conditional formatting: red if `avg_latency_sec > 60`
- Save as **"CDC End-to-End Latency"**.

---

### KPI 4 — Order Status Evolution

- **Chart type:** Line Chart
- **Dataset:** `gold_orders`
- Time column: `order_date`  |  Time grain: Week
- Metric: COUNT(*)  |  Group by: `status`
- Save as **"Order Status Evolution (Weekly)"**.

---

### KPI 5 — Total Sales by Period & Channel

- **Chart type:** Mixed Chart (Bar + Line)
- **Dataset:** `gold_sales_facts`
- Time column: `sale_date`  |  Time grain: Month
- Bar metric: SUM(`gross_revenue`)  |  Group by: `channel`
- Line metric: SUM(`net_revenue`)
- Save as **"Monthly Sales by Channel"**.

---

### KPI 6 — Payment Method Mix

- **Chart type:** Pie Chart
- **Dataset:** `gold_payments_summary`
- Metric: SUM(`total_amount`)  |  Group by: `method`
- Save as **"Payment Method Distribution"**.

---

### KPI 7 — Cancellation & Return Rates

- **Chart type:** Dual Line Chart
- **SQL:** paste KPI 7 from `trino_queries.sql`
- Line 1: `cancellation_rate_pct`  |  Line 2: `return_rate_pct`
- X-axis: `order_month`
- Save as **"Cancellation & Return Rates (%)"**.

---

### KPI 8 — Revenue by Segment & Category

- **Chart type:** Heatmap
- **Dataset:** `gold_sales_facts`
- Metric: SUM(`gross_revenue`)
- Rows: `customer_segment`  |  Columns: `product_category`
- Save as **"Revenue Heatmap: Segment x Category"**.

---

### Bonus — Top 10 Customers by LTV

- **Chart type:** Table
- **Dataset:** `gold_customers`
- Columns: `name`, `city`, `segment`, `total_orders`, `total_spent`, `avg_order_value`, `last_order_date`
- Sort: `total_spent DESC`  |  Row limit: 10
- Save as **"Top 10 Customers by Lifetime Value"**.

---

## Step 5 — Assemble the Layout

Drag all charts into the **CDC Pipeline KPIs** dashboard:

```
┌───────────────────────┬──────────────────────┬──────────────────────┐
│  KPI 1 Event Counts   │  KPI 2 Op Breakdown  │  KPI 3 Latency       │
├───────────────────────┴──────────────────────┴──────────────────────┤
│               KPI 4  Order Status Evolution (Line)                   │
├───────────────────────┬──────────────────────┬──────────────────────┤
│  KPI 5 Sales/Channel  │  KPI 6 Payment Mix   │  KPI 7 Cancel/Return │
├───────────────────────┴──────────────────────┴──────────────────────┤
│               KPI 8  Revenue Heatmap (Segment x Category)            │
├──────────────────────────────────────────────────────────────────────┤
│               Bonus   Top 10 Customers by LTV                        │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Step 6 — Enable Auto-Refresh

1. Dashboard header → **⋮ menu → Set auto-refresh interval**
2. Choose **30 seconds**
3. **Save**

---

## Troubleshooting

| Symptom | Resolution |
|---------|-----------|
| "Table not found" | Wait for Spark jobs to create tables; check `docker logs cdc-spark-master` |
| Trino connection refused | `docker ps \| grep trino`; wait for healthy status |
| Empty Gold tables | Verify Silver job ran; manually trigger: `docker exec cdc-spark-master spark-submit /opt/spark-apps/gold_job.py` |
| Slow queries | Add partition filter: `WHERE snapshot_date = current_date` |
| pip install fails in Superset | Restart superset container; it installs `trino` and `sqlalchemy-trino` on first boot |
