from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions27',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_revenue = {}
msg_count = 0

for message in consumer:
    tx = message.value
    store = tx['store']
    
    store_counts[store] += 1
    total_revenue[store] = total_revenue.get(store, 0) + tx['amount']
    msg_count += 1
    
    if msg_count % 10 == 0:
        print("\n" + "="*50)
        print(f"{'SKLEP':<12} | {'LICZBA':<8} | {'SUMA (PLN)':<12} | {'ŚREDNIA':<8}")
        print("-" * 50)
        for s in sorted(store_counts.keys()):
            count = store_counts[s]
            total = total_revenue[s]
            print(f"{s:<12} | {count:<8} | {total:<12.2f} | {total/count:<8.2f}")
