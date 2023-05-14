from kafka import KafkaProducer, KafkaConsumer
import json
import time
import datetime

from decouple import config

CITIES = json.loads(open('assets/cities.json', 'r').read())

class BeachDayProducer:
    """
    This class is responsible for producing data to the beachDay topic.
    It consumes events from the beachHour topic and produces events to the beachDay topic.
    """
    beach_hour_topic = 'beachHour'
    beach_day_topic = 'beachDay'
    
    def __init__(self):
        server = config('KAFKA_SERVER')
        # Creates kafka consumer and producer
        self.consumer = KafkaConsumer(bootstrap_servers=f'{server}:9092', auto_offset_reset='earliest', value_deserializer=lambda x: json.loads(x))
        self.producer = KafkaProducer(bootstrap_servers=f'{server}:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        
        # Subscribes to the beachHour topic in order to consume events
        self.consumer.subscribe(self.beach_hour_topic)

    def on_send_success(self, record):
        """
        Callback function to be called when a message is successfully produced.

        Parameters
        ----------
        record : RecordMetadata
            RecordMetadata object returned by the producer.

        Returns
        -------
        None.
        """
        print('NEW DATA:')
        print('\tTopic: ', record.topic)
        print('\tPartition: ', record.partition)
        print('\tOffset: ', record.offset)

    def send_data(self, data, topic, key, callback=False):
        """
        Sends data to a topic.

        Parameters
        ----------
        data : dict
            Data to be sent.
        topic : str
            Topic to send data to.
        key : str
            Key to be used to send data.
        callback : bool, optional
            If true, a callback function will be called when a message is successfully produced.

        Returns
        -------
        None.
        """
        if callback:
            self.producer.send(topic, data, key=key.encode('utf-8')).add_callback(self.on_send_success)
        else:
            self.producer.send(topic, data, key=key.encode('utf-8'))


    def transform_data(self, data):
        """
        Transforms data from the beachHour topic into data to be sent to the beachDay topic.
        Creating intervals of good weather.

        Parameters
        ----------
        data : dict
            Data to be transformed.

        Returns
        -------
        dict
            Transformed data.
        """
        sorted_data = sorted(data, key=lambda x: x['hora'])
        intervals = []

        while len(sorted_data):        
            start = sorted_data.pop(0)
            start_time = start['hora']
            end_time = start_time
            aux_time = start_time

            while True and len(sorted_data):
                data_time = sorted_data[0]['hora']
                if data_time == aux_time + datetime.timedelta(hours=1) and aux_time.day == data_time.day:
                    end = sorted_data.pop(0)
                    end_time = end['hora']
                    aux_time = end_time
                else:
                    break
            
            event = {
                'local': start['local'],
                'inicio': start_time.strftime('%Y-%m-%dT%H:%M'),
                'fim': end_time.replace(minute=59).strftime('%Y-%m-%dT%H:%M'),
            }

            intervals.append(event)

            if not len(data):
                break
            
        return intervals
    
    def filter_data(self, data):
        """
        Filters data from the beachHour topic, returning only the good hours.

        Parameters
        ----------
        data : dict
            Data to be filtered.

        Returns
        -------
        list
            Filtered data.
        """
        good_hours = []

        for _, value in data.items():
            if value['boa_hora'] == 1:
                good_hours.append(value)
        
        return good_hours
    
    def __get_partition_messages(self, partitions):
        """
        Gets messages from dictionary of partitions.
        
        Parameters
        ----------
        partitions : dict
            Dictionary of partitions. Parition number as key and list of records as value.
        
        Returns
        -------
        dict
            Dictionary of messages. City as key and dictionary of messages by time.
        """
        messages = dict()
        for _, message_list in partitions.items():
            for message in message_list:   
                msg_json = message.value

                # get city from obj
                city = msg_json['local']

                if not city in messages.keys():
                    messages[city] = dict()

                msg_json['hora'] = datetime.datetime.strptime(msg_json['hora'], '%Y-%m-%dT%H:%M')
                messages[city][msg_json['hora']] = msg_json
        return messages
    
    def get_messages(self):
        """
        Gets messages from the beachHour topic.

        Parameters
        ----------
        None.

        Returns
        -------
        dict
            Dictionary of messages. City as key and dictionary of messages by time.
        """
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
        """
        Runs the producer.

        Parameters
        ----------
        None.

        Returns
        -------
        list
            List of transformed data.
        """
        messages = self.get_messages()
        filtered_data_by_city = [self.filter_data(messages[city]) for city in CITIES]
        transformed_data = [self.transform_data(filtered_data) for filtered_data in filtered_data_by_city]

        print("Sending data...")
        for city_data in transformed_data:
            for data in city_data:
                self.send_data(data, self.beach_day_topic, data['local'])

            self.producer.flush()
            if len(city_data):
                print(len(city_data), ' events sent to Kafka at', datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S'), "for city", data['local'])

        return transformed_data
    
    def run_forever(self):
        """
        Runs the producer forever.

        Parameters
        ----------
        None.

        Returns
        -------
        None.
        """

        while True:
            print('Producing data...')
            self.run()
            time.sleep(3600)

if __name__ == '__main__': 
    obj = BeachDayProducer()
    obj.run_forever()

