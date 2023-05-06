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


class WaveProducer:
    wave_topic = 'waveHeight'

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
        url = r"https://marine-api.open-meteo.com/v1/marine?latitude=-20.67&longitude=-40.50&hourly=wave_height&length_unit=metric&timezone=America%2FSao_Paulo"
        response = requests.get(url)
        return response.json()

    def filter_data(self, data):
        all_time = data["hourly"]['time']
        all_wave = data["hourly"]['wave_height']

        events_wave = []

        format = '%Y-%m-%dT%H:%M'
        now = datetime.datetime.now()
        
        for time, wave in zip(all_time, all_wave):
            time_formatted = datetime.datetime.strptime(time, format)

            if time_formatted < now:
                continue
            
            events_wave.append({
                'local': 'Guarapari',
                'hora': time,
                'temperatura': wave,
            })

        return events_wave

    def run(self):
        data = self.request_data()
        events_wave = self.filter_data(data)

        for e_wave in events_wave:
            self.send_data(str(e_wave), self.wave_topic, e_wave['hora'])

        self.producer.flush()
    
    def run_forever(self):
        while True:
            print('Producing data...')
            self.run()
            time.sleep(3600)
        
obj = WaveProducer()
obj.run_forever()