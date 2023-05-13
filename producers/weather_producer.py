from kafka import KafkaProducer
import requests
import datetime
import json
import time

CITIES = json.loads(open('assets/cities.json', 'r').read())

class WeatherProducer:
    temp_topic = 'temperature'
    prec_topic = 'precipitationProbability'

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
        url = fr"https://api.open-meteo.com/v1/forecast?{city}&hourly=temperature_2m,precipitation_probability&forecast_days=3&timezone=America%2FSao_Paulo"
        response = requests.get(url)
        return response.json()

    def filter_data(self, data, city):
        all_time = data["hourly"]['time']
        all_temp = data["hourly"]['temperature_2m']
        all_prec = data["hourly"]['precipitation_probability']

        events_temp = []
        events_prec = []

        format = '%Y-%m-%dT%H:%M'
        now = datetime.datetime.now()
        now = now.replace(minute=0, second=0, microsecond=0)
        
        for time_, temp, prec in zip(all_time, all_temp, all_prec):
            time_formatted = datetime.datetime.strptime(time_, format)

            if time_formatted < now or time_formatted > now + datetime.timedelta(days=2):
                continue
            
            events_temp.append({
                'local': city,
                'hora': time_,
                'temperatura': temp,
            })

            events_prec.append({
                'local': city,
                'hora': time_,
                'pp': prec
            })

        return events_temp, events_prec

    def run(self, city):
        data = self.request_data(CITIES[city])
        events_temp, events_prec = self.filter_data(data, city)

        for e_temp, e_prec in zip(events_temp, events_prec):
            self.send_data(str(e_temp), self.temp_topic, city)
            self.send_data(str(e_prec), self.prec_topic, city)

        self.producer.flush()
        print(len(events_temp), ' events sent to Kafka at', datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), "for city", city)
        return events_temp, events_prec
    
    def run_forever(self):
        while True:
            print('Producing data...')
            for city in CITIES:
                self.run(city)
            time.sleep(3600)

if __name__ == '__main__':
    obj = WeatherProducer()
    obj.run_forever()