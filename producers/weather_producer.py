from kafka import KafkaProducer
import requests
import datetime
import json

# TODO:
# - pull a cada hora
# - verificar se tá sendo deletado a cada hora
# - pq partição sempre é zero
# - fazer o de wave


class WeatherProducer:
    temp_topic = 'weather'
    prec_topic = 'precipitation'

    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    def on_send_success(self, record):
        print('topic', record.topic)
        print('partition', record.partition)
        print('offset', record.offset)

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
        
        for time, temp, prec in zip(all_time, all_temp, all_prec):
            time_formatted = datetime.datetime.strptime(time, format)

            if time_formatted < now:
                continue
            
            events_temp.append({
                'local': 'Guarapari',
                'hora': time,
                'temperatura': temp,
            })

            events_prec.append({
                'local': 'Guarapari',
                'hora': time,
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
        
obj = WeatherProducer()
obj.run()