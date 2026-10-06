# Pipeline de données ELT : Snowflake · dbt · Airflow

Pipeline ELT automatisé qui transforme des données de ventes brutes en indicateur de chiffre d'affaires, rafraîchi chaque jour.

- **Snowflake** stocke les données brutes et exécute le SQL.
- **dbt** transforme les données (staging puis marts) et les teste.
- **Apache Airflow**, dans Docker, lance le pipeline chaque jour à 00:00 UTC.

## Architecture

```
FINANCE_DB.RAW (Snowflake)        dbt : staging (vues)        dbt : marts (table)
──────────────────────────        ────────────────────        ───────────────────
raw.customers    ──source()──►    stg_customers
raw.orders       ──source()──►    stg_orders       ──ref()──┐
raw.order_items  ──source()──►    stg_order_items  ──ref()──┴──►  daily_order_revenue
raw.products     ──source()──►    stg_products

Airflow (DAG dbt_snowflake_workflow, @daily) :
check_snowflake_connection >> dbt_run_staging >> dbt_run_marts >> dbt_test
```

## Structure du projet

```
mon_projet/
├── dags/dbt_dag.py          # DAG Airflow (4 tâches BashOperator)
├── models/
│   ├── sources.yml          # source raw_data (finance_db.raw)
│   ├── staging/             # stg_* : renommage et nettoyage (vues)
│   ├── marts/               # daily_order_revenue (table)
│   └── example/             # modèles d'exemple générés par dbt init
├── tests/snowflake_test.yml # tests de qualité (accepted_values, unique)
├── dbt_project.yml          # staging = view, marts = table
├── profiles.example.yml     # modèle de connexion Snowflake (sans secret)
├── Dockerfile               # image Airflow 2.10.2 + dbt-snowflake
└── docker-compose.yaml      # airflow-init, airflow-webserver, airflow-scheduler
```

## Installation

### 1. Snowflake

Dans une worksheet, avec le rôle `ACCOUNTADMIN` :

```sql
CREATE OR REPLACE WAREHOUSE finance_wh
  WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
CREATE OR REPLACE DATABASE finance_db;
CREATE OR REPLACE SCHEMA raw;
```

Créer ensuite les tables `raw.customers`, `raw.orders`, `raw.order_items` et `raw.products`, charger les CSV (bouton *Load Data* ou `COPY INTO`), puis créer l'utilisateur `dbt_user` avec les droits nécessaires.

### 2. dbt (en local)

```bash
python -m venv venv
venv\Scripts\activate          # Windows
pip install dbt-snowflake
```

Copier `profiles.example.yml` dans `~/.dbt/profiles.yml`, renseigner le compte et le mot de passe, puis vérifier la connexion :

```bash
dbt debug
dbt run
dbt test
```

### 3. Airflow (Docker)

Airflow ne fonctionne pas nativement sous Windows : il tourne ici dans Docker. Prérequis : Docker Desktop et un `~/.dbt/profiles.yml` valide (monté en lecture seule dans les conteneurs).

```bash
docker compose up -d
docker compose ps
```

Interface : http://localhost:8081 (utilisateur `admin` créé par le service `airflow-init`, à changer hors développement local). Activer le DAG `dbt_snowflake_workflow` pour lancer les exécutions quotidiennes.

## Résultats

- 4 vues de staging et la table `daily_order_revenue` (300 lignes) créées dans `FINANCE_DB.RAW`.
- 4 tâches Airflow sur 4 réussies, en environ 35 secondes par exécution.
- 2 tests de qualité sur 2 réussis.

## Améliorations possibles

- Agréger `daily_order_revenue` par jour (retirer `order_id` du `GROUP BY`).
- Ranger les objets dbt dans un schéma `ANALYTICS` séparé des données brutes.
- Passer en modèles `incremental` si le volume augmente.
- Ajouter une tâche d'ingestion en tête du DAG.
- Utiliser Postgres et LocalExecutor à la place de SQLite pour la production.

## Auteurs

Réalisé par **Hajar Oussama** et **Zouhair Serrar**, sous l'encadrement du **Prof. Karim**, ENSA Berrechid.
