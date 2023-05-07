from kafka import KafkaProducer, KafkaConsumer, TopicPartition
import json
import time
import datetime

class UnsafeUvClient:
    uv_topic = 'uvIndex'

    def __init__(self):
        self.consumer = KafkaConsumer(bootstrap_servers='kafka:9092', auto_offset_reset='earliest', value_deserializer=lambda x: json.loads(x))
        self.consumer.subscribe(self.uv_topic)


    def __get_partition_messages(self, partitions):
        messages = []
        for _, message_list in partitions.items():
            for message in message_list:   
                msg_json = json.loads(message.value.replace("'", '"'))
                msg_json['hora'] = datetime.datetime.strptime(msg_json['hora'], '%Y-%m-%dT%H:%M')
                messages.append(msg_json)
        return messages
    
    def filter_data(self, data):
        now = datetime.datetime.now()
        sorted_data = sorted(data, key=lambda x: x['hora'])
        
        if now.minute >= 30:
            now = now.replace(minute=0, hour=now.hour+1)

        for message in sorted_data:
            time_ = message['hora']
            if time_.day == now.day and time_.month == now.month and time_.year == now.year and time_.hour == now.hour:
                return message
            
    
    def get_messages(self):
        messages = []

        while True:
            print("Attempting to poll data...")
            partitions = self.consumer.poll(timeout_ms=1000)
            
            if not len(partitions):
                break
            
            messages.extend(self.__get_partition_messages(partitions))
        
        print("Polling finished. Sending data...")

        return messages
    
    def uv_index_to_risk(self, uv_index):
        if uv_index <= 2:
            return 'Baixo'
        elif uv_index <= 5:
            return 'Moderado'
        elif uv_index <= 7:
            return 'Alto'
        elif uv_index <= 10:
            return 'Muito alto'
        else:
            return 'Extremo'
        
    def run(self):
        print("Verificando horarios bons para ir a praia...")
        messages = self.get_messages()
        message = self.filter_data(messages)

        if message:
            now = datetime.datetime.now()
            print(f"Horario {now.hour:02}:{now.minute:02} - Indice UV: {message['uv_index']:.2f} - Risco: {self.uv_index_to_risk(message['uv_index'])}")
        else:
            print("Nao ha dados disponiveis para o horario atual")

    def run_forever(self):
        while True:
            self.run()
            time.sleep(3600)

if __name__ == '__main__':
    client = UnsafeUvClient()
    client.run_forever()