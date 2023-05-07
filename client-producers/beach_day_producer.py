from kafka import KafkaProducer, KafkaConsumer, TopicPartition
import json
import time
import datetime

class BeachHourProducer:
    beach_hour_topic = 'beachHour'
    beach_day_topic = 'beachDay'

    def __init__(self):
        self.consumer = KafkaConsumer(bootstrap_servers='kafka:9092', auto_offset_reset='earliest', value_deserializer=lambda x: json.loads(x))
        self.producer = KafkaProducer(bootstrap_servers="kafka:9092", value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.consumer.subscribe(self.beach_hour_topic)
    

    def on_send_success(self, record):
        print('NEW DATA:')
        print('\tTopic: ', record.topic)
        print('\tPartition: ', record.partition)
        print('\tOffset: ', record.offset)

    def send_data(self, data, topic, key):
        self.producer.send(topic, data, key=key.encode('utf-8')).add_callback(self.on_send_success)


    def transform_data(self, data):
        sorted_data = sorted(data, key=lambda x: x['hora'])
        intervals = []

        while len(sorted_data):        
            start = sorted_data.pop(0)
            start_time = start['hora']
            end_time = start_time.replace(minute=59)
            aux_time = start_time

            while True and len(sorted_data):
                
                data_time = sorted_data[0]['hora']
                if data_time == aux_time + datetime.timedelta(hours=1) and aux_time.day == data_time.day:
                    end = sorted_data.pop(0)
                    end_time = end['hora'].replace(minute=59)
                    aux_time = end_time
                else:
                    break
            
            intervals.append({
                'inicio': start_time.strftime('%Y-%m-%dT%H:%M'),
                'fim': end_time.strftime('%Y-%m-%dT%H:%M'),
            })

            if not len(data):
                break
            
        return intervals
    
    def filter_data(self, data):
        good_hours = []

        for _, value in data.items():
            if value['boa_hora'] == 1:
                good_hours.append(value)
        
        return good_hours
    
    def __get_partition_messages(self, partitions):
        messages = dict()
        for _, message_list in partitions.items():
            for message in message_list:   
                msg_json = message.value
                msg_json['hora'] = datetime.datetime.strptime(msg_json['hora'], '%Y-%m-%dT%H:%M')
                messages[msg_json['hora']] = msg_json
        return messages
    
    def get_messages(self):
        messages = dict()

        while True:
            print("Attempting to poll data...")
            partitions = self.consumer.poll(timeout_ms=1000)
            
            if not len(partitions):
                break
            
            messages.update(self.__get_partition_messages(partitions))
        
        print("Polling finished. Sending data...")

        return messages
    
    def run(self):
        messages = self.get_messages()
        filtered_data = self.filter_data(messages)
        transformed_data = self.transform_data(filtered_data)

        print("Sending data...")
        for data in transformed_data:
            self.send_data(data, self.beach_day_topic, data['inicio'])

        self.producer.flush()
        print(len(transformed_data), ' events sent to Kafka at', datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S'))
    
    def run_forever(self):
        while True:
            print('Producing data...')
            self.run()
            time.sleep(3600)
        
obj = BeachHourProducer()
obj.run_forever()

