"""Niveau 2 : producer Kafka qui simule une boutique en ligne.

Chaque seconde, une commande (avec ses lignes) est envoyée dans le topic `orders`.
Le format suit les tables Snowflake RAW.ORDERS et RAW.ORDER_ITEMS.

Lancer :  .venv\\Scripts\\python producer.py            (infini, Ctrl+C pour arrêter)
          .venv\\Scripts\\python producer.py --count 10 (10 commandes puis arrêt)
"""
import argparse
import json
import random
import time
from datetime import date

from confluent_kafka import Producer

TOPIC = "orders"
STATUSES = ["Completed", "Pending", "Cancelled"]   # mêmes valeurs que le test dbt accepted_values


def build_order(order_id: int) -> dict:
    """Fabrique une fausse commande : 1 à 3 produits parmi les 10 du catalogue."""
    items = []
    for _ in range(random.randint(1, 3)):
        quantity = random.randint(1, 5)
        unit_price = random.randint(10, 200)
        items.append({"product_id": random.randint(1, 10), "quantity": quantity, "unit_price": unit_price})
    return {
        "order_id": order_id,
        "customer_id": random.randint(1, 100),          # les 100 clients de RAW.CUSTOMERS
        "order_date": date.today().isoformat(),
        "status": random.choice(STATUSES),
        "total_amount": sum(i["quantity"] * i["unit_price"] for i in items),
        "items": items,
    }


def on_delivery(err, msg):
    """Appelé par Kafka pour CHAQUE message : confirme qu'il est bien stocké (ou pas)."""
    if err is not None:
        print(f"  ÉCHEC de livraison : {err}")
    else:
        print(f"  -> stocké dans {msg.topic()} partition {msg.partition()} offset {msg.offset()}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default=TOPIC, help="topic Kafka")
    parser.add_argument("--count", type=int, default=0, help="nombre de commandes (0 = infini)")
    parser.add_argument("--interval", type=float, default=1.0, help="secondes entre deux commandes")
    parser.add_argument("--start-id", type=int, default=10001, help="1er order_id (au-dessus des 300 existants)")
    args = parser.parse_args()

    producer = Producer({
        "bootstrap.servers": "localhost:9092",   # le listener EXTERNAL du docker-compose
        "acks": "all",                           # Kafka confirme seulement quand le message est bien écrit
        "client.id": "boutique-simulee",
    })

    order_id = args.start_id
    sent = 0
    try:
        while args.count == 0 or sent < args.count:
            order = build_order(order_id)
            producer.produce(
                args.topic,
                key=str(order["customer_id"]),        # même client -> même partition -> ordre garanti
                value=json.dumps(order).encode("utf-8"),
                on_delivery=on_delivery,
            )
            print(f"Commande {order_id} client {order['customer_id']} {order['status']:9} {order['total_amount']} MAD")
            producer.poll(0)        # traite les confirmations de livraison en attente
            order_id += 1
            sent += 1
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nArrêt demandé.")
    finally:
        producer.flush(10)          # attend que tous les messages en mémoire soient envoyés
        print(f"{sent} commande(s) envoyée(s).")


if __name__ == "__main__":
    main()
