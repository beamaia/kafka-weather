from kafka import KafkaProducer
import requests
import datetime
import json
import time

# TODO:
# - pull a cada hora
# - verificar se tá sendo deletado a cada hora
# - pq partição sempre é zero
# - fazer o de wave


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

    # voltar aqui pra ver se ta certo
    def send_data(self, data, topic, key):
        self.producer.send(topic, data, key=key.encode('utf-8')).add_callback(self.on_send_success)

    def request_data(self):
        url = r"https://api.open-meteo.com/v1/forecast?latitude=-20.67&longitude=-40.50&hourly=temperature_2m,precipitation_probability&forecast_days=3&timezone=America%2FSao_Paulo"
        response = requests.get(url)
        return response.json()

    def filter_data(self, data):
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
                'local': 'Guarapari',
                'hora': time_,
                'temperatura': temp,
            })

            events_prec.append({
                'local': 'Guarapari',
                'hora': time_,
                'pp': prec
            })

        return events_temp, events_prec

    def run(self):
        data = self.request_data()
        events_temp, events_prec = self.filter_data(data)

        for e_temp, e_prec in zip(events_temp, events_prec):
            self.send_data(str(e_temp), self.temp_topic, e_temp['hora'])
            self.send_data(str(e_prec), self.prec_topic, e_prec['hora'])

        self.producer.flush()
        print(len(events_temp), ' events sent to Kafka')
    
    def run_forever(self):
        while True:
            print('Producing data...')
            self.run()
            time.sleep(3600)
        
obj = WeatherProducer()
obj.run_forever()