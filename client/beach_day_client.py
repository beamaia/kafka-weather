from kafka import KafkaConsumer
import json
import datetime

class BeachDayClient:
    beach_day_topic = 'beachDay'

    def __init__(self):
        self.consumer = KafkaConsumer(bootstrap_servers='kafka:9092', auto_offset_reset='earliest', value_deserializer=lambda x: json.loads(x))
        self.consumer.subscribe(self.beach_day_topic)

    def __get_partition_messages(self, partitions):
        messages = []
        for _, message_list in partitions.items():
            for message in message_list:   
                msg_json = message.value
                msg_json['inicio'] = datetime.datetime.strptime(msg_json['inicio'], '%Y-%m-%dT%H:%M')
                msg_json['fim'] = datetime.datetime.strptime(msg_json['fim'], '%Y-%m-%dT%H:%M')

                messages.append(msg_json)

        return messages
    
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
    
    def run(self):
        print("Verificando horarios bons para ir a praia...")
        messages = self.get_messages()

        for message in messages:
            start = message['inicio']
            end = message['fim']

            print(f"Dia {start.day:02}/{start.month:02} - Horario bom para ir a praia: {start.hour:02}:{start.minute:02} - {end.hour:02}:{end.minute:02} ")

if __name__ == '__main__':
    client = BeachDayClient()
    client.run()