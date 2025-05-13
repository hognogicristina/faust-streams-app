from kafka import KafkaProducer
import json
import time
import pandas as pd

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

df = pd.read_csv('sensor_data.csv')

room_map = {
    'S1': 'Living Room',
    'S2': 'Bedroom',
    'S3': 'Kitchen',
    'S4': 'Office'
}

for _, row in df.iterrows():
    for idx, (sensor, room_name) in enumerate(room_map.items(), start=1):
        producer.send('temperature-readings', {'room': room_name, 'value': row[f'{sensor}_Temp']})
        producer.send('co2-readings', {'room': room_name, 'ppm': row['S5_CO2']})
        producer.send('motion-readings', {'room': room_name, 'motion': bool(row['S6_PIR'] or row['S7_PIR'])})
        producer.send('light-readings', {'room': room_name, 'lux': row[f'{sensor}_Light']})
        producer.send('sound-readings', {'room': room_name, 'db': row[f'{sensor}_Sound']})
        producer.send('occupancy-readings', {'room': room_name, 'count': int(row['Room_Occupancy_Count'])})

    time.sleep(0.7)
