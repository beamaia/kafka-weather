from kafka import KafkaProducer

def on_send_success(record):
    print('topic', record.topic)
    print('partition', record.partition)
    print('offset', record.offset)

producer = KafkaProducer(bootstrap_servers="localhost:9092")


for i in range(10):
    msg = f'mensagem {i}'
    producer.send('topicoTeste', msg.encode('ascii')).add_callback(on_send_success)
    
    # if i % 5 == 0:
producer.flush()