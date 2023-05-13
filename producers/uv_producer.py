from kafka import KafkaProducer
import requests
import datetime
import json
import time

CITIES = json.loads(open('assets/cities.json', 'r').read())

class UvProducer:
    wave_topic = 'uvIndex'

    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers="kafka:9092", value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    def on_send_success(self, record):
        print('NEW DATA:')
        print('\tTopic: ', record.topic)
        print('\tPartition: ', record.partition)
        print('\tOffset: ', record.offset)

    def send_data(self, data, topic, key, with_callback=False):
        if with_callback:
            self.producer.send(topic, data, key=key.encode('utf-8')).add_callback(self.on_send_success)
        else:
            self.producer.send(topic, data, key=key.encode('utf-8'))

    def request_data(self, city):
        url = fr"https://air-quality-api.open-meteo.com/v1/air-quality?{city}&hourly=uv_index&timezone=America%2FSao_Paulo"
        response = requests.get(url)
        return response.json()

    def filter_data(self, data, city):
        all_time = data["hourly"]['time']
        all_uv_index = data["hourly"]['uv_index']

        events_uv = []

        format = '%Y-%m-%dT%H:%M'
        now = datetime.datetime.now()
        now = now.replace(minute=0, second=0, microsecond=0)
        
        for time_, uv_index in zip(all_time, all_uv_index):
            time_formatted = datetime.datetime.strptime(time_, format)

            # if time is less then today date, or if time is more than 3 days from today
            if time_formatted < now or time_formatted > now + datetime.timedelta(days=2):
                continue
            
            events_uv.append({
                'local': city,
                'hora': time_,
                'uv_index': uv_index,
            })

        return events_uv

    def run(self, city):
        data = self.request_data(CITIES[city])
        events_uv = self.filter_data(data, city)

        for e_wave in events_uv:
            self.send_data(str(e_wave), self.wave_topic, city)

        self.producer.flush()
        print(len(events_uv), ' events sent to Kafka at', datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S'), "for city", city)
        return events_uv
    
    def run_forever(self):
        while True:
            print('Producing data...')
            for city in CITIES:
                self.run(city)
            time.sleep(3600)

if __name__ == "__main__": 
    obj = UvProducer()
    obj.run_forever()