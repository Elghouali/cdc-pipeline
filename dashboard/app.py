"""
CDC Pipeline Dashboard
======================

Backend Flask qui fait office de portail unique pour le pipeline CDC :

  * Statut en temps reel de chaque service de la stack (Postgres, Kafka,
    Kafka Connect, Spark, MinIO, Trino, Superset).
  * Requetes live sur Trino pour exposer les 8 KPI analytiques + bonus.
  * Lecture de l'etat des connecteurs Debezium et des topics Kafka.
  * Liste les liens rapides vers toutes les UIs (Spark, MinIO, Trino,
    Superset, Kafka Connect).
  * Sert le frontend statique (dashboard/static/index.html).

Lance via Docker Compose dans le meme reseau que la stack ; ainsi les
noms d'hote internes (trino, kafka-connect, minio, etc.) sont resolus.
"""

import logging
import os
import socket
import time
from typing import Any, Dict, List, Optional, Tuple

import json
import requests
import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from trino.dbapi import connect as trino_connect
from trino.exceptions import TrinoQueryError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("dashboard")

# Toutes les valeurs ci-dessous peuvent etre surchargees par variable
# d'environnement (utile si l'utilisateur lance hors Docker).
TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "admin")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "analytics")

KAFKA_CONNECT_URL = os.getenv("KAFKA_CONNECT_URL", "http://kafka-connect:8083")
MINIO_URL = os.getenv("MINIO_URL", "http://minio:9000")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "http://spark-master:8080")
SUPERSET_URL = os.getenv("SUPERSET_URL", "http://superset:8088")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "ecommerce")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

# Path du fichier de config Debezium (monte dans le container)
DEBEZIUM_CONFIG_PATH = os.getenv(
    "DEBEZIUM_CONFIG_PATH", "/app/config/debezium-connector.json"
)
DEBEZIUM_CONNECTOR_NAME = "ecommerce-postgres-connector"

# Liste des tables sources operationnelles
SOURCE_TABLES = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "returns",
]
KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka")
KAFKA_PORT = int(os.getenv("KAFKA_PORT", "9092"))
HIVE_HOST = os.getenv("HIVE_HOST", "hive-metastore")
HIVE_PORT = int(os.getenv("HIVE_PORT", "9083"))

# Liens publics affiches dans l'interface (accessibles depuis l'hote).
PUBLIC_LINKS = {
    "Spark Master UI": "http://localhost:8085",
    "MinIO Console": "http://localhost:9001",
    "Kafka Connect": "http://localhost:8083/connectors",
    "Trino UI": "http://localhost:8080",
    "Superset": "http://localhost:8088",
}

app = Flask(__name__, static_folder="static", static_url_path="")
CORS(app)


# ---------------------------------------------------------------------------
# Helpers : verifications de services
# ---------------------------------------------------------------------------

def _tcp_ping(host: str, port: int, timeout: float = 1.5) -> bool:
    """Renvoie True si le port TCP repond."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _http_ok(url: str, timeout: float = 2.0) -> bool:
    """Renvoie True si l'URL repond avec un code < 500."""
    try:
        r = requests.get(url, timeout=timeout)
        return r.status_code < 500
    except requests.RequestException:
        return False


# ---------------------------------------------------------------------------
# Helpers : connexion Trino
# ---------------------------------------------------------------------------

def trino_query(sql: str) -> Tuple[List[str], List[List[Any]]]:
    """Execute une requete SQL sur Trino et renvoie (colonnes, lignes)."""
    conn = trino_connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
    )
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description] if cur.description else []
    return cols, rows


def rows_to_dict(cols: List[str], rows: List[List[Any]]) -> List[Dict[str, Any]]:
    """Convertit (cols, rows) en liste de dicts JSON-serialisable."""
    result = []
    for r in rows:
        d = {}
        for c, v in zip(cols, r):
            # Datetime / date -> str
            if hasattr(v, "isoformat"):
                d[c] = v.isoformat()
            else:
                d[c] = v
        result.append(d)
    return result


# ---------------------------------------------------------------------------
# Endpoint : sante des services
# ---------------------------------------------------------------------------

@app.route("/api/health")
def api_health():
    """Retourne le statut UP/DOWN de chaque composant de la stack."""
    services = [
        {
            "name": "PostgreSQL",
            "role": "Base source (OLTP)",
            "up": _tcp_ping(POSTGRES_HOST, POSTGRES_PORT),
            "url": "http://localhost:5432",
        },
        {
            "name": "Kafka",
            "role": "Bus d'evenements CDC",
            "up": _tcp_ping(KAFKA_HOST, KAFKA_PORT),
            "url": "http://localhost:9092",
        },
        {
            "name": "Kafka Connect / Debezium",
            "role": "Capture des changements",
            "up": _http_ok(f"{KAFKA_CONNECT_URL}/connectors"),
            "url": PUBLIC_LINKS["Kafka Connect"],
        },
        {
            "name": "MinIO",
            "role": "Stockage objet S3",
            "up": _http_ok(f"{MINIO_URL}/minio/health/live"),
            "url": PUBLIC_LINKS["MinIO Console"],
        },
        {
            "name": "Hive Metastore",
            "role": "Catalogue Iceberg",
            "up": _tcp_ping(HIVE_HOST, HIVE_PORT),
            "url": "",
        },
        {
            "name": "Spark Master",
            "role": "Moteur streaming",
            "up": _http_ok(SPARK_MASTER_URL),
            "url": PUBLIC_LINKS["Spark Master UI"],
        },
        {
            "name": "Trino",
            "role": "Moteur SQL distribue",
            "up": _http_ok(f"http://{TRINO_HOST}:{TRINO_PORT}/v1/status"),
            "url": PUBLIC_LINKS["Trino UI"],
        },
        {
            "name": "Superset",
            "role": "Visualisation BI",
            "up": _http_ok(f"{SUPERSET_URL}/health"),
            "url": PUBLIC_LINKS["Superset"],
        },
    ]
    summary = {
        "total": len(services),
        "up": sum(1 for s in services if s["up"]),
        "down": sum(1 for s in services if not s["up"]),
        "checked_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    return jsonify({"services": services, "summary": summary})


# ---------------------------------------------------------------------------
# Endpoint : connecteurs Debezium
# ---------------------------------------------------------------------------

@app.route("/api/connectors")
def api_connectors():
    """Liste les connecteurs Kafka Connect et leur statut."""
    try:
        names = requests.get(f"{KAFKA_CONNECT_URL}/connectors", timeout=3).json()
    except Exception as exc:
        return jsonify({"error": str(exc), "connectors": []}), 200

    details = []
    for n in names:
        try:
            st = requests.get(
                f"{KAFKA_CONNECT_URL}/connectors/{n}/status", timeout=3
            ).json()
            details.append(
                {
                    "name": n,
                    "state": st.get("connector", {}).get("state", "UNKNOWN"),
                    "type": st.get("type", ""),
                    "tasks": [
                        {"id": t.get("id"), "state": t.get("state")}
                        for t in st.get("tasks", [])
                    ],
                }
            )
        except Exception as exc:
            details.append({"name": n, "state": "ERROR", "error": str(exc)})
    return jsonify({"connectors": details})


# ---------------------------------------------------------------------------
# Endpoint : applications Spark actives
# ---------------------------------------------------------------------------

@app.route("/api/spark")
def api_spark():
    """Renvoie la liste des apps Spark actives via la REST API du Master."""
    try:
        r = requests.get(f"{SPARK_MASTER_URL}/json/", timeout=3)
        data = r.json()
        apps = [
            {
                "id": a.get("id"),
                "name": a.get("name"),
                "state": a.get("state"),
                "cores": a.get("cores"),
                "memory_per_executor_mb": a.get("memoryperexecutor"),
                "submit_date": a.get("submitdate"),
                "duration": a.get("duration"),
            }
            for a in data.get("activeapps", [])
        ]
        completed = len(data.get("completedapps", []))
        return jsonify(
            {
                "workers": len(data.get("workers", [])),
                "active_apps": apps,
                "completed_apps": completed,
                "status": data.get("status"),
            }
        )
    except Exception as exc:
        return jsonify({"error": str(exc), "active_apps": []}), 200


# ---------------------------------------------------------------------------
# Endpoint : KPI analytiques (via Trino)
# ---------------------------------------------------------------------------

# Toutes les requetes sont copiees / adaptees de trino/trino_queries.sql.

KPI_QUERIES: Dict[str, str] = {
    # KPI 1 - Nombre d'evenements CDC par table
    "cdc_event_counts": """
        SELECT
            source_table,
            COUNT(*)                                   AS total_events,
            COUNT(*) FILTER (WHERE op = 'c')           AS inserts,
            COUNT(*) FILTER (WHERE op = 'u')           AS updates,
            COUNT(*) FILTER (WHERE op = 'd')           AS deletes,
            COUNT(*) FILTER (WHERE op = 'r')           AS snapshots
        FROM (
            SELECT op, source_table FROM bronze_customers_cdc
            UNION ALL SELECT op, source_table FROM bronze_products_cdc
            UNION ALL SELECT op, source_table FROM bronze_orders_cdc
            UNION ALL SELECT op, source_table FROM bronze_order_items_cdc
            UNION ALL SELECT op, source_table FROM bronze_payments_cdc
            UNION ALL SELECT op, source_table FROM bronze_returns_cdc
        ) b
        GROUP BY source_table
        ORDER BY total_events DESC
    """,
    # KPI 3 - Latence
    "latency": """
        SELECT
            source_table,
            COUNT(*) AS events,
            ROUND(AVG((to_unixtime(ingestion_timestamp) * 1000.0 - source_ts_ms) / 1000.0), 2) AS avg_latency_sec,
            ROUND(MAX((to_unixtime(ingestion_timestamp) * 1000.0 - source_ts_ms) / 1000.0), 2) AS max_latency_sec,
            ROUND(approx_percentile((to_unixtime(ingestion_timestamp) * 1000.0 - source_ts_ms) / 1000.0, 0.95), 2) AS p95_latency_sec
        FROM (
            SELECT source_table, source_ts_ms, ingestion_timestamp FROM bronze_customers_cdc WHERE source_ts_ms IS NOT NULL
            UNION ALL SELECT source_table, source_ts_ms, ingestion_timestamp FROM bronze_orders_cdc    WHERE source_ts_ms IS NOT NULL
            UNION ALL SELECT source_table, source_ts_ms, ingestion_timestamp FROM bronze_payments_cdc  WHERE source_ts_ms IS NOT NULL
        ) lat
        GROUP BY source_table
        ORDER BY avg_latency_sec DESC
    """,
    # KPI 4 - Statut commandes (hebdo)
    "orders_weekly": """
        SELECT
            date_format(date_trunc('week', order_date), '%Y-%m-%d') AS week_start,
            status,
            COUNT(*) AS order_count,
            ROUND(SUM(order_total), 2) AS week_revenue
        FROM gold_orders
        GROUP BY 1, 2
        ORDER BY 1 DESC, order_count DESC
        LIMIT 200
    """,
    # KPI 5 - Ventes par mois et canal
    "sales_by_channel": """
        SELECT
            date_format(sale_date, '%Y-%m') AS sale_month,
            channel,
            SUM(order_count) AS total_orders,
            ROUND(SUM(gross_revenue), 2) AS gross_revenue,
            ROUND(SUM(net_revenue),   2) AS net_revenue,
            SUM(total_items_sold) AS items_sold
        FROM gold_sales_facts
        GROUP BY 1, 2
        ORDER BY 1 DESC, gross_revenue DESC
        LIMIT 200
    """,
    # KPI 6 - Mix moyen de paiement
    "payment_methods": """
        SELECT
            payment_month,
            method,
            transaction_count,
            ROUND(total_amount,     2) AS total_collected,
            ROUND(completed_amount, 2) AS completed,
            ROUND(refunded_amount,  2) AS refunded
        FROM gold_payments_summary
        ORDER BY payment_month DESC, total_amount DESC
        LIMIT 100
    """,
    # KPI 7 - Annulations / retours
    "cancel_return_rates": """
        WITH monthly AS (
            SELECT
                date_format(order_date, '%Y-%m') AS order_month,
                COUNT(*) AS total_orders,
                COUNT(*) FILTER (WHERE status = 'cancelled') AS cancelled,
                COUNT(*) FILTER (WHERE status = 'returned')  AS returned,
                COUNT(*) FILTER (WHERE status = 'delivered') AS delivered
            FROM gold_orders
            GROUP BY 1
        )
        SELECT
            order_month,
            total_orders,
            cancelled,
            returned,
            delivered,
            ROUND(cancelled * 100.0 / NULLIF(total_orders, 0), 2) AS cancellation_rate_pct,
            ROUND(returned  * 100.0 / NULLIF(delivered,    0), 2) AS return_rate_pct
        FROM monthly
        ORDER BY order_month DESC
        LIMIT 24
    """,
    # KPI 8 - Revenu segment x categorie
    "revenue_segment_category": """
        SELECT
            customer_segment,
            product_category,
            SUM(order_count) AS total_orders,
            ROUND(SUM(gross_revenue), 2) AS gross_revenue,
            ROUND(SUM(net_revenue),   2) AS net_revenue
        FROM gold_sales_facts
        GROUP BY 1, 2
        ORDER BY gross_revenue DESC
        LIMIT 50
    """,
    # Bonus - Top 10 clients
    "top_customers": """
        SELECT
            customer_id,
            name,
            city,
            segment,
            total_orders,
            ROUND(total_spent, 2) AS lifetime_value,
            ROUND(avg_order_value, 2) AS avg_basket,
            CAST(last_order_date AS VARCHAR) AS last_order_date
        FROM gold_customers
        ORDER BY total_spent DESC
        LIMIT 10
    """,
}


@app.route("/api/kpi/<name>")
def api_kpi(name: str):
    """Execute la requete KPI demandee."""
    sql = KPI_QUERIES.get(name)
    if not sql:
        return jsonify({"error": f"unknown KPI '{name}'"}), 404
    try:
        cols, rows = trino_query(sql)
        return jsonify({"kpi": name, "data": rows_to_dict(cols, rows)})
    except TrinoQueryError as exc:
        return (
            jsonify(
                {
                    "kpi": name,
                    "error": str(exc),
                    "hint": (
                        "Les tables Iceberg ne sont peut-etre pas encore "
                        "creees. Attendre que Spark Bronze/Silver/Gold aient "
                        "tourne au moins un cycle (~3-5 min apres start.sh)."
                    ),
                    "data": [],
                }
            ),
            200,
        )
    except Exception as exc:
        logger.exception("KPI %s failed", name)
        return jsonify({"kpi": name, "error": str(exc), "data": []}), 200


# ---------------------------------------------------------------------------
# Endpoint : resume global (cartes en haut du dashboard)
# ---------------------------------------------------------------------------

@app.route("/api/summary")
def api_summary():
    """Quelques chiffres-cles pour le bandeau superieur."""
    out: Dict[str, Optional[float]] = {
        "total_cdc_events": None,
        "total_orders": None,
        "total_customers": None,
        "total_revenue": None,
        "avg_latency_sec": None,
    }
    try:
        # Total evenements CDC (Bronze, toutes tables confondues)
        _, r = trino_query(
            """
            SELECT SUM(c) FROM (
              SELECT COUNT(*) AS c FROM bronze_customers_cdc
              UNION ALL SELECT COUNT(*) FROM bronze_products_cdc
              UNION ALL SELECT COUNT(*) FROM bronze_orders_cdc
              UNION ALL SELECT COUNT(*) FROM bronze_order_items_cdc
              UNION ALL SELECT COUNT(*) FROM bronze_payments_cdc
              UNION ALL SELECT COUNT(*) FROM bronze_returns_cdc
            )
            """
        )
        if r and r[0][0] is not None:
            out["total_cdc_events"] = int(r[0][0])
    except Exception as exc:
        logger.debug("summary cdc events: %s", exc)

    try:
        _, r = trino_query("SELECT COUNT(*) FROM gold_orders")
        if r:
            out["total_orders"] = int(r[0][0] or 0)
    except Exception:
        pass

    try:
        _, r = trino_query("SELECT COUNT(*) FROM gold_customers")
        if r:
            out["total_customers"] = int(r[0][0] or 0)
    except Exception:
        pass

    try:
        _, r = trino_query(
            "SELECT ROUND(SUM(gross_revenue), 2) FROM gold_sales_facts"
        )
        if r and r[0][0] is not None:
            out["total_revenue"] = float(r[0][0])
    except Exception:
        pass

    try:
        _, r = trino_query(
            """
            SELECT ROUND(AVG((to_unixtime(ingestion_timestamp) * 1000.0 - source_ts_ms) / 1000.0), 2)
            FROM bronze_orders_cdc WHERE source_ts_ms IS NOT NULL
            """
        )
        if r and r[0][0] is not None:
            out["avg_latency_sec"] = float(r[0][0])
    except Exception:
        pass

    return jsonify(out)


# ---------------------------------------------------------------------------
# Endpoint : liens / metadonnees
# ---------------------------------------------------------------------------

@app.route("/api/links")
def api_links():
    return jsonify(
        [{"name": k, "url": v} for k, v in PUBLIC_LINKS.items()]
    )


@app.route("/api/info")
def api_info():
    return jsonify(
        {
            "project": "Pipeline Big Data CDC - Sujet 4",
            "module": "II-BDCC - Big Data",
            "stack": [
                "PostgreSQL",
                "Debezium",
                "Kafka",
                "Spark Structured Streaming",
                "Iceberg",
                "MinIO",
                "Hive Metastore",
                "Trino",
                "Superset",
            ],
            "layers": ["Bronze", "Silver", "Gold"],
        }
    )


# ---------------------------------------------------------------------------
# Helpers : connexion PostgreSQL
# ---------------------------------------------------------------------------

def pg_connect():
    """Renvoie une connexion psycopg2 vers la base source."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        connect_timeout=3,
    )


def pg_query(sql: str, params: Optional[tuple] = None) -> Tuple[List[str], List[List[Any]]]:
    """Execute une requete SQL sur Postgres et renvoie (colonnes, lignes)."""
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            if cur.description is None:
                return [], []
            cols = [d.name for d in cur.description]
            rows = cur.fetchall()
            return cols, [list(r) for r in rows]


def pg_execute(sql: str, params: Optional[tuple] = None) -> int:
    """Execute un INSERT/UPDATE/DELETE et renvoie le nombre de lignes touchees."""
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            conn.commit()
            return cur.rowcount


def pg_execute_returning(sql: str, params: Optional[tuple] = None) -> Optional[Any]:
    """Execute une requete avec RETURNING et renvoie la premiere colonne."""
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            row = cur.fetchone()
            conn.commit()
            return row[0] if row else None


def serialize_pg_value(v: Any) -> Any:
    """Convertit une valeur Postgres en JSON-friendly."""
    if v is None:
        return None
    if hasattr(v, "isoformat"):
        return v.isoformat()
    # Decimal -> float
    try:
        from decimal import Decimal
        if isinstance(v, Decimal):
            return float(v)
    except Exception:
        pass
    return v


# ---------------------------------------------------------------------------
# Endpoint : liste des tables sources Postgres
# ---------------------------------------------------------------------------

@app.route("/api/postgres/tables")
def api_postgres_tables():
    """Renvoie le nom, le nombre de lignes et la liste des colonnes
    de chaque table operationnelle dans la base source."""
    out = []
    for table in SOURCE_TABLES:
        try:
            # Nombre de lignes
            _, r = pg_query(f"SELECT COUNT(*) FROM public.{table}")
            count = int(r[0][0]) if r else 0

            # Colonnes (nom + type)
            cols_sql = """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position
            """
            _, cols_r = pg_query(cols_sql, (table,))
            columns = [{"name": c[0], "type": c[1]} for c in cols_r]

            out.append(
                {
                    "name": table,
                    "row_count": count,
                    "columns": columns,
                    "ok": True,
                }
            )
        except Exception as exc:
            out.append({"name": table, "row_count": 0, "columns": [], "ok": False, "error": str(exc)})
    return jsonify({"tables": out})


# ---------------------------------------------------------------------------
# Endpoint : contenu d'une table Postgres
# ---------------------------------------------------------------------------

@app.route("/api/postgres/table/<name>")
def api_postgres_table(name: str):
    """Renvoie les lignes d'une table source. Limite : 100 par defaut."""
    if name not in SOURCE_TABLES:
        return jsonify({"error": f"table '{name}' non autorisee"}), 400

    limit = min(int(request.args.get("limit", "50")), 500)
    order = request.args.get("order", "id DESC")
    # Whitelist du tri pour eviter l'injection
    if not all(c.isalnum() or c in " ,_" for c in order):
        order = "id DESC"

    try:
        cols, rows = pg_query(
            f"SELECT * FROM public.{name} ORDER BY {order} LIMIT %s",
            (limit,),
        )
        data = [
            {c: serialize_pg_value(v) for c, v in zip(cols, r)}
            for r in rows
        ]
        return jsonify({"table": name, "columns": cols, "rows": data, "count": len(data)})
    except Exception as exc:
        logger.exception("postgres table %s", name)
        return jsonify({"error": str(exc), "rows": [], "columns": []}), 200


# ---------------------------------------------------------------------------
# Endpoint : actions / scripts
#
# Reproduit le comportement du script simulate_changes.sh en se connectant
# directement a Postgres. Les changements sont aussitot captes par Debezium
# et propages dans Kafka -> Spark -> Iceberg -> Trino, donc le dashboard
# reflete les nouvelles donnees apres un cycle (~30-60 s).
# ---------------------------------------------------------------------------

@app.route("/api/actions/list")
def api_actions_list():
    """Liste les actions disponibles."""
    return jsonify(
        {
            "actions": [
                {"id": "register_connector", "label": "Enregistrer le connecteur Debezium",
                 "description": "POST de la config debezium-connector.json sur Kafka Connect"},
                {"id": "insert_customer", "label": "Inserer un nouveau client",
                 "description": "Ajoute un client aleatoire (declenchement INSERT CDC)"},
                {"id": "insert_product", "label": "Inserer un nouveau produit",
                 "description": "Ajoute un produit aleatoire (declenchement INSERT CDC)"},
                {"id": "create_order", "label": "Creer une commande complete",
                 "description": "Insert order + 2 order_items + payment (5 evenements CDC)"},
                {"id": "update_order_status", "label": "Avancer 3 commandes",
                 "description": "confirmed -> shipped -> delivered (UPDATE CDC)"},
                {"id": "cancel_order", "label": "Annuler une commande pending",
                 "description": "UPDATE status = cancelled"},
                {"id": "create_return", "label": "Creer un retour",
                 "description": "INSERT returns + UPDATE order -> returned"},
                {"id": "delete_items", "label": "Supprimer des order_items",
                 "description": "DELETE order_items d'une commande annulee"},
                {"id": "simulate_round", "label": "Lancer un round complet (tout)",
                 "description": "Execute tout simulate_changes.sh une fois (12 operations CDC)"},
            ]
        }
    )


def _action_insert_customer():
    new_id = pg_execute_returning("""
        INSERT INTO customers (name, email, city, segment, signup_date)
        VALUES (
          'DashClient_' || extract(epoch from now())::bigint,
          'dash_' || extract(epoch from now())::bigint || '@test.ma',
          (ARRAY['Casablanca','Rabat','Fes','Marrakech','Agadir','Tanger'])[floor(random()*6+1)],
          (ARRAY['B2B','B2C','Premium','Standard'])[floor(random()*4+1)],
          CURRENT_DATE
        )
        RETURNING id
    """)
    return {"event": "INSERT customers", "new_id": new_id}


def _action_insert_product():
    new_id = pg_execute_returning("""
        INSERT INTO products (name, category, brand, price, stock_qty)
        VALUES (
          'DashProd_' || extract(epoch from now())::bigint,
          (ARRAY['Electronics','Clothing','Food','Home','Sports'])[floor(random()*5+1)],
          'DashBrand',
          round((random() * 4900 + 99)::numeric, 2),
          (floor(random()*200)+5)::int
        )
        RETURNING id
    """)
    return {"event": "INSERT products", "new_id": new_id}


def _action_create_order():
    new_order = pg_execute_returning("""
        INSERT INTO orders (order_date, customer_id, status, channel)
        VALUES (
          CURRENT_DATE,
          (SELECT id FROM customers ORDER BY random() LIMIT 1),
          'pending',
          (ARRAY['web','mobile','in-store','marketplace'])[floor(random()*4+1)]
        ) RETURNING id
    """)
    if new_order is None:
        return {"event": "ERROR", "error": "no customers found"}

    pg_execute("""
        INSERT INTO order_items (order_id, product_id, quantity, unit_price)
        SELECT %s, id, (floor(random()*3)+1)::int, price
        FROM products ORDER BY random() LIMIT 2
    """, (new_order,))

    pg_execute("UPDATE orders SET status='confirmed' WHERE id=%s", (new_order,))

    pg_execute("""
        INSERT INTO payments (order_id, payment_date, method, amount, status)
        SELECT %s, CURRENT_DATE,
               (ARRAY['credit_card','debit_card','cash_on_delivery','bank_transfer','wallet'])[floor(random()*5+1)],
               COALESCE(SUM(quantity::numeric * unit_price), 0),
               'completed'
        FROM order_items WHERE order_id=%s
    """, (new_order, new_order))

    return {"event": "Order created (INSERT x4 + UPDATE x1)", "order_id": new_order}


def _action_update_order_status():
    n = pg_execute("""
        UPDATE orders
        SET status = CASE status WHEN 'confirmed' THEN 'shipped'
                                 WHEN 'shipped'   THEN 'delivered'
                                 ELSE status END
        WHERE id IN (
          SELECT id FROM orders WHERE status IN ('confirmed','shipped')
          ORDER BY random() LIMIT 3
        )
    """)
    return {"event": f"UPDATE orders ({n} lignes)", "rows": n}


def _action_cancel_order():
    n = pg_execute("""
        UPDATE orders SET status='cancelled'
        WHERE id=(SELECT id FROM orders WHERE status='pending' ORDER BY random() LIMIT 1)
    """)
    return {"event": f"UPDATE orders cancelled ({n} lignes)", "rows": n}


def _action_create_return():
    new_ret = pg_execute_returning("""
        INSERT INTO returns (order_id, return_date, reason, status)
        SELECT o.id, CURRENT_DATE,
               (ARRAY['defective','wrong_item','not_as_described','changed_mind','other'])[floor(random()*5+1)],
               'requested'
        FROM orders o LEFT JOIN returns r ON r.order_id=o.id
        WHERE o.status='delivered' AND r.id IS NULL
        ORDER BY random() LIMIT 1
        RETURNING order_id
    """)
    if new_ret:
        pg_execute("UPDATE orders SET status='returned' WHERE id=%s", (new_ret,))
        return {"event": "Return created", "order_id": new_ret}
    return {"event": "No eligible delivered order", "order_id": None}


def _action_delete_items():
    n = pg_execute("""
        DELETE FROM order_items
        WHERE order_id=(SELECT id FROM orders WHERE status='cancelled' ORDER BY random() LIMIT 1)
    """)
    return {"event": f"DELETE order_items ({n} lignes)", "rows": n}


def _action_register_connector():
    """Reproduit register_connector.sh : DELETE puis POST de la config."""
    try:
        with open(DEBEZIUM_CONFIG_PATH, "r") as f:
            config = json.load(f)
    except Exception as exc:
        return {"event": "ERROR", "error": f"config introuvable : {exc}"}

    # Supprimer eventuelle ancienne instance
    try:
        requests.delete(
            f"{KAFKA_CONNECT_URL}/connectors/{DEBEZIUM_CONNECTOR_NAME}",
            timeout=5,
        )
    except Exception:
        pass

    # POST de la nouvelle config
    try:
        r = requests.post(
            f"{KAFKA_CONNECT_URL}/connectors",
            json=config,
            timeout=10,
        )
        if r.status_code >= 400:
            return {"event": "ERROR", "error": f"HTTP {r.status_code}: {r.text[:200]}"}
        return {"event": "Connector registered", "name": DEBEZIUM_CONNECTOR_NAME}
    except Exception as exc:
        return {"event": "ERROR", "error": str(exc)}


def _action_simulate_round():
    """Execute toutes les actions d'un round simulate_changes.sh."""
    events = []
    for fn in (
        _action_insert_customer,
        _action_insert_product,
        _action_create_order,
        _action_update_order_status,
        _action_cancel_order,
    ):
        try:
            events.append(fn())
        except Exception as exc:
            events.append({"event": "ERROR", "error": str(exc)})
    return {"event": "Round complet execute", "details": events}


ACTION_MAP = {
    "register_connector": _action_register_connector,
    "insert_customer": _action_insert_customer,
    "insert_product": _action_insert_product,
    "create_order": _action_create_order,
    "update_order_status": _action_update_order_status,
    "cancel_order": _action_cancel_order,
    "create_return": _action_create_return,
    "delete_items": _action_delete_items,
    "simulate_round": _action_simulate_round,
}


@app.route("/api/actions/run/<action_id>", methods=["POST"])
def api_actions_run(action_id: str):
    """Execute l'action demandee."""
    fn = ACTION_MAP.get(action_id)
    if not fn:
        return jsonify({"error": f"action inconnue : {action_id}"}), 404
    try:
        result = fn()
        return jsonify({"ok": True, "action": action_id, "result": result})
    except Exception as exc:
        logger.exception("action %s", action_id)
        return jsonify({"ok": False, "action": action_id, "error": str(exc)}), 200


# ---------------------------------------------------------------------------
# Routes statiques (frontend)
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/<path:path>")
def static_proxy(path: str):
    return send_from_directory(app.static_folder, path)


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    logger.info("Dashboard demarre sur le port %d", port)
    app.run(host="0.0.0.0", port=port, debug=False)
