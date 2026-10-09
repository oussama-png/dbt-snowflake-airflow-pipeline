# kafka-lab : streaming des commandes avec Apache Kafka

Simule l'arrivée de nouvelles commandes en temps réel et les charge dans Snowflake
(`RAW.ORDERS_STREAM`, `RAW.ORDER_ITEMS_STREAM`), où dbt les combine avec l'historique.

```
producer.py ──► Kafka (topic orders) ──► consumer_snowflake.py ──► Snowflake RAW.*_STREAM ──► dbt
```

## Fichiers

| Fichier | Rôle |
|---|---|
| `docker-compose.yaml` | Kafka 3.8 en mode KRaft (sans ZooKeeper) et Kafka UI |
| `producer.py` | Génère une fausse commande par seconde dans le topic `orders` |
| `consumer.py` | Lit le topic et affiche un chiffre d'affaires en temps réel (apprentissage) |
| `consumer_snowflake.py` | Charge les commandes dans Snowflake par micro-lots (30 s ou 50 messages) |

## Lancer

```bash
docker compose up -d
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders --partitions 1 --replication-factor 1
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt
.venv\Scripts\python consumer_snowflake.py
.venv\Scripts\python producer.py --count 20
```

Kafka UI : http://localhost:8085

`consumer_snowflake.py` lit la connexion Snowflake dans `~/.dbt/profiles.yml` (profil `mon_projet`) :
aucun identifiant n'est stocké dans ce dossier.
