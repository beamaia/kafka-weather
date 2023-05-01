from kafka import KafkaConsumer
from kafka import TopicPartition
import json

consumer = KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset='earliest', value_deserializer=lambda x: json.loads(x))

tp = TopicPartition('weather', 0)
consumer.assign([tp])

consumer.seek(tp, 100)

for msg in consumer:
    print(msg.value, ' in partition: ', msg.partition)