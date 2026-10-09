"""Niveau 3 : consumer Kafka qui lit les commandes et calcule un CA en temps réel.

Lancer :  .venv\\Scripts\\python consumer.py                 (Ctrl+C pour arrêter)
          .venv\\Scripts\\python consumer.py --idle-stop 10  (s'arrête après 10 s sans message)
"""
import argparse
import json
import time

from confluent_kafka import Consumer, KafkaError

TOPIC = "orders"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default=TOPIC, help="topic Kafka")
    parser.add_argument("--group", default="dashboard-ca", help="nom du consumer group")
    parser.add_argument("--idle-stop", type=float, default=0, help="arrêt après N s sans message (0 = jamais)")
    args = parser.parse_args()

    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": args.group,
        # Groupe qui n'a encore rien lu : commencer au début du topic ("latest" = seulement les nouveaux)
        "auto.offset.reset": "earliest",
        # On valide la position NOUS-MÊMES, après avoir traité le message (voir commit plus bas)
        "enable.auto.commit": False,
    })
    consumer.subscribe([args.topic])

    revenue = 0
    by_status = {}
    last_message = time.time()
    try:
        while True:
            msg = consumer.poll(1.0)            # attend un message au plus 1 seconde
            if msg is None:
                if args.idle_stop and time.time() - last_message > args.idle_stop:
                    print(f"Aucun message depuis {args.idle_stop:.0f} s, arrêt.")
                    break
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print("Erreur :", msg.error())
                continue

            last_message = time.time()
            try:
                order = json.loads(msg.value())
            except json.JSONDecodeError:
                print(f"[offset {msg.offset()}] message illisible ignoré : {msg.value()[:60]!r}")
                consumer.commit(message=msg)
                continue

            # --- Traitement : ici on met à jour des indicateurs (au niveau 5 : insertion Snowflake)
            by_status[order.get("status", "?")] = by_status.get(order.get("status", "?"), 0) + 1
            if order.get("status") == "Completed":
                revenue += order.get("total_amount", 0)
            print(f"[{args.group}][partition {msg.partition()} offset {msg.offset()}] commande {order.get('order_id')} "
                  f"{order.get('status')} | CA Completed cumulé = {revenue} | {by_status}")

            # --- Commit APRÈS le traitement : si le programme plante avant, le message sera relu
            consumer.commit(message=msg, asynchronous=False)
    except KeyboardInterrupt:
        print("\nArrêt demandé.")
    finally:
        consumer.close()                        # quitte proprement le groupe


if __name__ == "__main__":
    main()
