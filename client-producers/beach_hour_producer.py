from kafka import KafkaProducer, KafkaConsumer
import json
import time
import datetime

CITIES = json.loads(open('assets/cities.json', 'r').read())

class BeachHourProducer:
    temp_topic = 'temperature'
    prec_topic = 'precipitationProbability'
    uv_topic = 'uvIndex'
    beach_topic = 'beachHour'

    def __init__(self):
        self.temp_consumer = KafkaConsumer(bootstrap_servers='kafka:9092', auto_offset_reset='earliest', value_deserializer=lambda x: json.loads(x))
        self.prec_consumer = KafkaConsumer(bootstrap_servers='kafka:9092', auto_offset_reset='earliest', value_deserializer=lambda x: json.loads(x))
        self.wave_consumer = KafkaConsumer(bootstrap_servers='kafka:9092', auto_offset_reset='earliest', value_deserializer=lambda x: json.loads(x))

        self.producer = KafkaProducer(bootstrap_servers="kafka:9092", value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        self.temp_consumer.subscribe(self.temp_topic)
        self.prec_consumer.subscribe(self.prec_topic)
        self.wave_consumer.subscribe(self.uv_topic)

    def on_send_success(self, record):
        print('NEW DATA:')
        print('\tTopic: ', record.topic)
        print('\tPartition: ', record.partition)
        print('\tOffset: ', record.offset)

    def send_data(self, data, topic, key, callback=False):
        if callback:
            self.producer.send(topic, data, key=key.encode('utf-8')).add_callback(self.on_send_success)
        else:
            self.producer.send(topic, data, key=key.encode('utf-8'))

    def transform_data(self, temp_messages, prec_messages, uv_messages):
        # intersection of hourr between sets of messages
        set_hours = set([x for x in temp_messages.keys()]).intersection(set([x for x in prec_messages.keys()]),  set([x for x in uv_messages.keys()]))
        hours = sorted(set_hours)
        transformed_data = []   
        
        for hour in hours:
            aux = {
                'hora': hour,
                'local': temp_messages[hour]['local'],
                'boa_hora': 0,
                'temperatura': temp_messages[hour]['temperatura'],
                'pp': prec_messages[hour]['pp'],
                'uv_index': uv_messages[hour]['uv_index'] 
            }
            
            transformed_data.append(aux)

        return transformed_data

    def filter_data(self, data):
        filtered_data = []
        now = datetime.datetime.now()
        now = now.replace(minute=0, second=0, microsecond=0)

        for aux in data:
            if aux['hora'] < now or aux['hora'] > now + datetime.timedelta(days=2):
                continue

            if aux['temperatura'] > 20 and aux['pp'] < 30 and aux['uv_index'] < 5:
                aux['boa_hora'] = 1
            else:
                aux['boa_hora'] = 0
            
            # transform to str
            aux['hora'] = aux['hora'].strftime('%Y-%m-%dT%H:%M')
            filtered_data.append(aux)
        
        return filtered_data
            
    
    def __get_partition_messages(self, partitions):
        messages = dict()
        for _, message_list in partitions.items():
            for message in message_list:   
                msg_json = json.loads(message.value.replace("'", '"'))

                # get city from obj
                city = msg_json['local']

                if not city in messages.keys():
                    messages[city] = dict()

                msg_json['hora'] = datetime.datetime.strptime(msg_json['hora'], '%Y-%m-%dT%H:%M')
                messages[city][msg_json['hora']] = msg_json
        
        return messages
    
    def get_messages(self):
        self.temp_consumer.resume()
        temp_messages = dict()
        prec_messages = dict()
        uv_message = dict()

        while True:
            print("Attempting to poll data...")
            temp_partitions = self.temp_consumer.poll(timeout_ms=1000)
            prec_partitions = self.prec_consumer.poll(timeout_ms=1000)
            wave_partitions = self.wave_consumer.poll(timeout_ms=1000)

            if not any([len(temp_partitions), len(prec_partitions), len(wave_partitions)]):
                break
            
            temp_messages.update(self.__get_partition_messages(temp_partitions))
            prec_messages.update(self.__get_partition_messages(prec_partitions))
            uv_message.update(self.__get_partition_messages(wave_partitions))
        
        print("Polling finished. Sending data...")
        print("Temp messages: ", len(temp_messages))	
        print("Prec messages: ", len(prec_messages))
        print("UV messages: ", len(uv_message))

        return temp_messages, prec_messages, uv_message
    
    def run(self):
        temp_messages, prec_messages, uv_message = self.get_messages()
        transformed_data_by_city = [self.transform_data(temp_messages[city], prec_messages[city], uv_message[city]) for city in CITIES]
        filtered_data = [self.filter_data(transformed_data) for transformed_data in transformed_data_by_city]
        
        print("Sending data...")
        for city_data in filtered_data:
            for data in city_data:
                self.send_data(data, self.beach_topic, data['local'])

            self.producer.flush()
            if len(city_data):
                print(len(city_data), ' events sent to Kafka at', datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S'), "for city", data['local'])
    
    
    def run_forever(self):
        while True:
            print('Producing data...')
            self.run()
            time.sleep(3600)

if __name__ == '__main__':  
    obj = BeachHourProducer()
    obj.run_forever()

