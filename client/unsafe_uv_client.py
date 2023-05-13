from kafka import KafkaProducer, KafkaConsumer, TopicPartition
import json
import time
import datetime
import argparse
import difflib

CITIES = json.loads(open('assets/cities.json', 'r').read())

class UnsafeUvClient:
    uv_topic = 'uvIndex'

    def __init__(self, city, date=None):
        self.consumer = KafkaConsumer(bootstrap_servers='kafka:9092', auto_offset_reset='earliest', value_deserializer=lambda x: json.loads(x))
        self.consumer.subscribe(self.uv_topic)
        
        self._verify_city(city)
        self.date = date

    def _verify_city(self, city):
        if not any(difflib.get_close_matches(city, CITIES, n=1, cutoff=0.8)):
            raise Exception(f"City {city} not found")
        else:
            self.city = difflib.get_close_matches(city, CITIES, n=1, cutoff=0.8)[0]
            print(f"City {city} found. Using {self.city} instead")

    def __get_partition_messages(self, partitions):
        messages = []
        for _, message_list in partitions.items():
            for message in message_list:   
                msg_json = json.loads(message.value.replace("'", '"'))

                if msg_json['local'] != self.city:
                    continue

                msg_json['hora'] = datetime.datetime.strptime(msg_json['hora'], '%Y-%m-%dT%H:%M')
                messages.append(msg_json)
        return messages
    
    def filter_data(self, data, time=datetime.datetime.now()):
        now = time
        print(time)
        sorted_data = sorted(data, key=lambda x: x['hora'])
        
        if now.minute >= 30:
            now = now.replace(minute=0, hour=now.hour+1)

        for message in sorted_data:
            time_ = message['hora']
            if time_.day == now.day and time_.month == now.month and time_.year == now.year and time_.hour == now.hour:
                return message
        
        return {}
            
    
    def get_messages(self):
        messages = []

        while True:
            print("Attempting to poll data...")
            partitions = self.consumer.poll(timeout_ms=1000)
            
            if not len(partitions):
                break
            
            messages.extend(self.__get_partition_messages(partitions))
        
        print("Polling finished. Sending data...")
        messages = [dict(tupleized) for tupleized in set(tuple(item.items()) for item in messages)]

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
    
    def get_events(self):
        messages = self.get_messages()
        return self.filter_data(messages)
    
    def get_events_by_date(self):
        messages = self.get_messages()
        print(messages)
        return self.filter_data(messages, self.date)

    def run(self):
        message = self.get_events()

        if message:
            now = datetime.datetime.now()
            print(f"Horario {now.hour:02}:{now.minute:02} - Indice UV: {message['uv_index']:.2f} - Risco: {self.uv_index_to_risk(message['uv_index'])}")
        else:
            print("Nao ha dados disponiveis para o horario atual")

    def run_forever(self):
        while True:
            self.run()
            time.sleep(3600)


def parse_args():
    parser = argparse.ArgumentParser(description='Unsafe UV Client')
    parser.add_argument('--run-once', action='store_true', help='Run only once')
    parser.add_argument('--run-forever', action='store_true', help='Run forever', default=True)
    parser.add_argument('--city', type=str, help='City to get UV index from', required=True)
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()

    if args.run_once:
        client = UnsafeUvClient(args.city)
        client.run()
    elif args.run_forever:
        client = UnsafeUvClient(args.city)
        client.run_forever()