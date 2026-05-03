from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions27',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
stats = {}
msg_count = 0

for message in consumer:
    tx = message.value
    cat = tx['category']
    amt = tx['amount']

    if cat not in stats:
        stats[cat] = {
            'count': 0,
            'sum': 0.0,
            'min': float('inf'),
            'max': 0.0
        }

    s = stats[cat]
    s['count'] += 1
    s['sum'] += amt
    s['min'] = min(s['min'], amt)
    s['max'] = max(s['max'], amt)
    
    msg_count += 1

    if msg_count % 10 == 0:
        print("\n" + "—" * 65)
        print(f"{'KATEGORIA':<15} | {'ILE':<5} | {'SUMA':<10} | {'MIN':<8} | {'MAX':<8}")
        print("—" * 65)
        for category, data in sorted(stats.items()):
            print(f"{category:<15} | {data['count']:<5} | {data['sum']:<10.2f} | {data['min']:<8.2f} | {data['max']:<8.2f}")
