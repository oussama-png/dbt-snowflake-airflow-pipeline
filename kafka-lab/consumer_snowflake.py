"""Niveau 5 : consumer Kafka -> Snowflake, par micro-lots.

Lit le topic `orders` et écrit dans FINANCE_DB.RAW.ORDERS_STREAM et RAW.ORDER_ITEMS_STREAM.
La connexion Snowflake est lue dans ~/.dbt/profiles.yml (profil mon_projet) : aucun secret dans ce fichier.

Lancer :  .venv\\Scripts\\python consumer_snowflake.py                 (Ctrl+C pour arrêter)
          .venv\\Scripts\\python consumer_snowflake.py --idle-stop 20  (arrêt après 20 s sans message)
"""
import argparse
import json
import os
import time
from datetime import datetime, timezone

import snowflake.connector
import yaml
from confluent_kafka import Consumer, KafkaError

TOPIC = "orders"
BATCH_SIZE = 50          # envoyer dès qu'on a 50 commandes...
BATCH_SECONDS = 30       # ...ou au plus tard 30 s après la première commande du lot
REQUIRED = ("order_id", "customer_id", "order_date", "status", "total_amount", "items")

DDL = [
    """CREATE TABLE IF NOT EXISTS RAW.ORDERS_STREAM (
        ID NUMBER, CUSTOMER_ID NUMBER, ORDER_DATE DATE, TOTAL_AMOUNT NUMBER, STATUS TEXT,
        KAFKA_PARTITION NUMBER, KAFKA_OFFSET NUMBER, INGESTED_AT TIMESTAMP_NTZ)""",
    """CREATE TABLE IF NOT EXISTS RAW.ORDER_ITEMS_STREAM (
        ID NUMBER, ORDER_ID NUMBER, PRODUCT_ID NUMBER, QUANTITY NUMBER, UNIT_PRICE NUMBER,
        INGESTED_AT TIMESTAMP_NTZ)""",
]


def snowflake_connect():
    """Ouvre une connexion avec les mêmes paramètres que dbt (profil mon_projet)."""
    with open(os.path.expanduser("~/.dbt/profiles.yml"), encoding="utf-8-sig") as f:
        profile = yaml.safe_load(f)["mon_projet"]
    cfg = profile["outputs"][profile["target"]]
    return snowflake.connector.connect(
        # "eu_west_3" -> "eu-west-3" : les "_" sont interdits dans un nom d'hôte (vérification TLS)
        account=cfg["account"].replace("_", "-"), user=cfg["user"], password=cfg["password"], role=cfg["role"],
        warehouse=cfg["warehouse"], database=cfg["database"], schema=cfg["schema"],
    )


def flush(conn, batch):
    """Écrit tout le lot en 2 requêtes (une par table), dans une seule transaction."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    orders = [(o["order_id"], o["customer_id"], o["order_date"], o["total_amount"], o["status"],
               part, off, now) for o, part, off in batch]
    items = [(o["order_id"] * 100 + i, o["order_id"], it["product_id"], it["quantity"], it["unit_price"], now)
             for o, _, _ in batch for i, it in enumerate(o["items"], start=1)]
    cur = conn.cursor()
    try:
        cur.execute("BEGIN")
        cur.executemany("INSERT INTO RAW.ORDERS_STREAM VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", orders)
        cur.executemany("INSERT INTO RAW.ORDER_ITEMS_STREAM VALUES (%s, %s, %s, %s, %s, %s)", items)
        cur.execute("COMMIT")
    except Exception:
        cur.execute("ROLLBACK")
        raise
    finally:
        cur.close()
    return len(orders), len(items)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--group", default="snowflake-loader")
    parser.add_argument("--idle-stop", type=float, default=0, help="arrêt après N s sans message (0 = jamais)")
    args = parser.parse_args()

    conn = snowflake_connect()
    for ddl in DDL:
        conn.cursor().execute(ddl)
    print("Connecté à Snowflake, tables RAW.ORDERS_STREAM et RAW.ORDER_ITEMS_STREAM prêtes.")

    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": args.group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,      # commit seulement APRÈS l'écriture dans Snowflake
    })
    consumer.subscribe([args.topic])

    batch, batch_started, last_message = [], None, time.time()
    rejected = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            now = time.time()
            if msg is not None and not msg.error():
                last_message = now
                try:
                    order = json.loads(msg.value())
                    missing = [k for k in REQUIRED if k not in order]
                except json.JSONDecodeError:
                    order, missing = None, ["JSON invalide"]
                if missing:
                    rejected += 1
                    print(f"  rejeté (offset {msg.offset()}) : champs manquants {missing}")
                else:
                    batch.append((order, msg.partition(), msg.offset()))
                    batch_started = batch_started or now
            elif msg is not None and msg.error().code() != KafkaError._PARTITION_EOF:
                print("Erreur Kafka :", msg.error())

            idle = args.idle_stop and now - last_message > args.idle_stop
            full = len(batch) >= BATCH_SIZE
            due = batch_started and now - batch_started >= BATCH_SECONDS
            if batch and (full or due or idle):
                n_orders, n_items = flush(conn, batch)
                consumer.commit(asynchronous=False)   # Snowflake a bien reçu le lot : on avance la position
                reason = "lot plein" if full else ("30 s écoulées" if due else "fin du flux")
                print(f"Lot envoyé ({reason}) : {n_orders} commandes, {n_items} lignes -> Snowflake")
                batch, batch_started = [], None
            if idle:
                print(f"Aucun message depuis {args.idle_stop:.0f} s, arrêt.")
                break
    except KeyboardInterrupt:
        print("\nArrêt demandé.")
        if batch:
            flush(conn, batch)
            consumer.commit(asynchronous=False)
            print(f"Dernier lot envoyé : {len(batch)} commandes.")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
