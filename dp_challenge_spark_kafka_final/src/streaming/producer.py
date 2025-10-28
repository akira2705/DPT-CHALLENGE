import csv
import json
import time
from kafka import KafkaProducer

TOPIC = "transactions"
BOOTSTRAP = "localhost:9093"  # host listener exposed by docker-compose

def main():
    producer = KafkaProducer(
        bootstrap_servers=[BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    with open("data/raw/transactions.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            evt = {
                "txn_id": int(row["txn_id"]),
                "customer_id": int(row["customer_id"]),
                "timestamp": row["timestamp"],
                "amount": None if row["amount"]=="" else float(row["amount"]),
                "currency": row["currency"],
                "device_lat": None if row["device_lat"] in ("", "NaN") else float(row["device_lat"]),
                "device_lon": None if row["device_lon"] in ("", "NaN") else float(row["device_lon"]),
                "category": row["category"],
                "notes": row["notes"]
            }
            producer.send(TOPIC, evt)
            print("Sent:", evt)
            time.sleep(0.5)
    producer.flush()
    print("Done.")

if __name__ == "__main__":
    main()
