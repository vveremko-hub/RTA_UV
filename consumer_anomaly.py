from kafka import KafkaConsumer
from collections import defaultdict, deque
import json
from datetime import datetime

consumer = KafkaConsumer(
    'transactions27',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_history = defaultdict(lambda: deque(maxlen=3))

for message in consumer:
    tx = message.value
    user = tx['user_id']
    now = datetime.fromisoformat(tx['timestamp'])
    
    history = user_history[user]
    
    if len(history) == 3:
        time_diff = (now - history[0]).total_seconds()
        if time_diff <= 60:
            print(f"ALERT FRAUD: User {user} - 4 transakcje w {time_diff:.1f}s!")
    
    history.append(now)
