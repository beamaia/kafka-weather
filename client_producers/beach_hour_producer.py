from kafka import KafkaProducer, KafkaConsumer
import json
import time
import datetime

from decouple import config

CITIES = json.loads(open('assets/cities.json', 'r').read())

class BeachHourProducer:
    """
    This class is responsible for producing beach hour data to Kafka.
    It consumes data from the topics 'temperature', 'precipitationProbability' and 'uvIndex',
    filters it and sends it to the topic 'beachHour'.
    """
    temp_topic = 'temperature'
    prec_topic = 'precipitationProbability'
    uv_topic = 'uvIndex'
    beach_topic = 'beachHour'

    def __init__(self):
        server = config('KAFKA_SERVER')

        # Creates a KafkaConsumer object with the bootstrap server and a json serializer
        self.temp_consumer = KafkaConsumer(bootstrap_servers=f'{server}:9092', auto_offset_reset='earliest', value_deserializer=lambda x: json.loads(x))
        self.prec_consumer = KafkaConsumer(bootstrap_servers=f'{server}:9092', auto_offset_reset='earliest', value_deserializer=lambda x: json.loads(x))
        self.uvindex_consumer = KafkaConsumer(bootstrap_servers=f'{server}:9092', auto_offset_reset='earliest', value_deserializer=lambda x: json.loads(x))

        # Creates a KafkaProducer object with the bootstrap server and a json serializer
        self.producer = KafkaProducer(bootstrap_servers=f'{server}:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        # Subscribes to the topics
        self.temp_consumer.subscribe(self.temp_topic)
        self.prec_consumer.subscribe(self.prec_topic)
        self.uvindex_consumer.subscribe(self.uv_topic)

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
            Key to be associated with the data.
        callback : bool, optional
            If True, sends with callback. The default is False.

        Returns
        -------
        None.
        """
        if callback:
            self.producer.send(topic, data, key=key.encode('utf-8')).add_callback(self.on_send_success)
        else:
            self.producer.send(topic, data, key=key.encode('utf-8'))

    def transform_data(self, temp_messages, prec_messages, uv_messages):
        """
        Creates dictionary given the data from the topics.

        Parameters
        ----------
        temp_messages : dict
            Data from the temperature topic.
        prec_messages : dict
            Data from the precipitationProbability topic.
        uv_messages : dict
            Data from the uvIndex topic.

        Returns
        -------
        transformed_data : list
            List of dictionaries with the data from the topics in the format:
            ```
                {
                    'hora': datetime,
                    'local': str,
                    'boa_hora': int,
                    'temperatura': float,
                    'pp': float,
                    'uv_index': float
                }
            ```
        """

        # intersection of hour between sets of messages
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
        """
        Filters data from the topics considering the following conditions:
            - temperature > 20
            - precipitationProbability < 30
            - uvIndex < 5

        Parameters
        ----------
        data : list
            List of dictionaries with the data.
        Returns
        -------
        filtered_data : list
            List of dictionaries with the data.
        """
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
                msg_json = json.loads(message.value.replace("'", '"'))

                # get city from obj
                city = msg_json['local']

                if not city in messages.keys():
                    messages[city] = dict()

                msg_json['hora'] = datetime.datetime.strptime(msg_json['hora'], '%Y-%m-%dT%H:%M')
                messages[city][msg_json['hora']] = msg_json
        
        return messages
    
    def get_messages(self):
        """
        Gets messages from the topics.

        Parameters
        ----------
        None.

        Returns
        -------
        temp_messages : dict
            Dictionary of messages from the temperature topic.
        prec_messages : dict
            Dictionary of messages from the precipitationProbability topic.
        uv_messages : dict
            Dictionary of messages from the uvIndex topic.
        """
        self.temp_consumer.resume()
        temp_messages = dict()
        prec_messages = dict()
        uv_message = dict()

        while True:
            print("Attempting to poll data...")
            temp_partitions = self.temp_consumer.poll(timeout_ms=1000)
            prec_partitions = self.prec_consumer.poll(timeout_ms=1000)
            uvindex_partitions = self.uvindex_consumer.poll(timeout_ms=1000)

            if not any([len(temp_partitions), len(prec_partitions), len(uvindex_partitions)]):
                break
            
            temp_messages.update(self.__get_partition_messages(temp_partitions))
            prec_messages.update(self.__get_partition_messages(prec_partitions))
            uv_message.update(self.__get_partition_messages(uvindex_partitions))
        
        print("Polling finished. Sending data...")
        print("Temp messages: ", len(temp_messages))	
        print("Prec messages: ", len(prec_messages))
        print("UV messages: ", len(uv_message))

        return temp_messages, prec_messages, uv_message

    def run(self, cities=CITIES):
        """
        Runs the producer.

        Parameters
        ----------
        cities : list, optional
            List of cities to be considered. The default is CITIES.

        Returns
        -------
        filtered_data : list
            List of dictionaries with the data.
        """
        temp_messages, prec_messages, uv_message = self.get_messages()
        transformed_data_by_city = [self.transform_data(temp_messages[city], prec_messages[city], uv_message[city]) for city in cities]
        filtered_data = [self.filter_data(transformed_data) for transformed_data in transformed_data_by_city]
        
        print("Sending data...")
        for city_data in filtered_data:
            for data in city_data:
                self.send_data(data, self.beach_topic, data['local'])

            self.producer.flush()
            if len(city_data):
                print(len(city_data), ' events sent to Kafka at', datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S'), "for city", data['local'])

        return filtered_data
    
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
    obj = BeachHourProducer()
    obj.run_forever()

