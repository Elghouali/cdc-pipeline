# Dashboard CDC — Portail unifié

Interface web unique qui regroupe tout le projet CDC sur une seule page :
statut des services, indicateurs analytiques en direct (via Trino),
connecteurs Debezium, applications Spark, graphiques et liens rapides.

## Démarrage

Le dashboard est lancé automatiquement avec le reste de la stack par
`bash scripts/start.sh`. Une fois la pipeline démarrée, ouvrir :

**http://localhost:5000**

## Lancement isolé (hors stack)

```bash
cd dashboard
pip install -r requirements.txt
# La stack doit déjà tourner ; le dashboard se connecte sur localhost
export TRINO_HOST=localhost
export KAFKA_CONNECT_URL=http://localhost:8083
export MINIO_URL=http://localhost:9000
export SPARK_MASTER_URL=http://localhost:8085
export SUPERSET_URL=http://localhost:8088
python app.py
```

## API exposée

| Endpoint | Description |
|----------|-------------|
| `GET /api/health` | Statut UP/DOWN de chaque composant de la stack |
| `GET /api/connectors` | Connecteurs Kafka Connect / Debezium et leurs tâches |
| `GET /api/spark` | Applications Spark actives (via Spark Master REST API) |
| `GET /api/summary` | Chiffres-clés (CDC total, commandes, CA, latence) |
| `GET /api/kpi/<name>` | Exécute la requête KPI sur Trino. Noms : `cdc_event_counts`, `latency`, `orders_weekly`, `sales_by_channel`, `payment_methods`, `cancel_return_rates`, `revenue_segment_category`, `top_customers` |
| `GET /api/links` | Liens vers les autres UIs (Spark, MinIO, Trino, Superset) |
| `GET /api/info` | Métadonnées projet (stack, couches) |
| `GET /api/postgres/tables` | Liste des 6 tables sources avec compte + colonnes |
| `GET /api/postgres/table/<name>` | Contenu d'une table source (max 50 lignes par défaut) |
| `GET /api/actions/list` | Liste des actions disponibles (insert/update/delete/register) |
| `POST /api/actions/run/<id>` | Lance une action : `insert_customer`, `insert_product`, `create_order`, `update_order_status`, `cancel_order`, `create_return`, `delete_items`, `register_connector`, `simulate_round` |

## Architecture du service

```
Browser ──HTTP──► Flask (app.py) ──► Trino (requêtes KPI)
                          │
                          ├──► Kafka Connect REST (connecteurs Debezium)
                          ├──► Spark Master REST (jobs en cours)
                          └──► MinIO / Postgres / Hive (ping TCP/HTTP)
```

## Personnalisation

- **Ajouter un KPI** : ajouter une requête SQL dans `KPI_QUERIES` (app.py),
  puis ajouter un appel `fetchJson("/api/kpi/<nom>")` dans `static/app.js`.
- **Changer les couleurs** : variables CSS au début de `static/style.css`.
- **Auto-refresh** : constante `REFRESH_MS` dans `static/app.js`.

## Dépendances

- Flask 3 + flask-cors + gunicorn
- trino (client Python officiel)
- Chart.js 4 (CDN, pas d'install)
