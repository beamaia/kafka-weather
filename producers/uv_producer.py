from kafka import KafkaProducer
import requests
import datetime
import json
import time

from decouple import config
CITIES = json.loads(open('assets/cities.json', 'r').read())

class UvProducer:
    """
    This class is responsible for uv index weather data to Kafka.
    It requests data from the Open Meteo API, filters it and sends it to Kafka
    to the topics 'uvIndex'.
    """
    uv_index_topic = 'uvIndex'

    def __init__(self):
        server = config('KAFKA_SERVER')
        
        # Creates a KafkaProducer object with the bootstrap server and a json serializer
        self.producer = KafkaProducer(bootstrap_servers=f'{server}:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    def on_send_success(self, record):
        """
        Callback function to be called when a message is sent to Kafka.
        Prints the topic, partition and offset of the sent message.

        Parameters
        ----------
        record : RecordMetadata object
            RecordMetadata object returned by the producer.

        Returns
        -------
        None.
        """
        print('NEW DATA:')
        print('\tTopic: ', record.topic)
        print('\tPartition: ', record.partition)
        print('\tOffset: ', record.offset)

    def send_data(self, data, topic, key, with_callback=False):
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
        if with_callback:
            self.producer.send(topic, data, key=key.encode('utf-8')).add_callback(self.on_send_success)
        else:
            self.producer.send(topic, data, key=key.encode('utf-8'))

    def request_data(self, city):
        """
        Requests data from Open Meteo API. Uses the citys latitude and longitude
        to get the data. Returns a json object containing the uv index and time of 
        the data for the next 3 days, hourly format.

        Parameters
        ----------
        city : str 
            latitue and longitude to get data from
        
        Returns
        -------
        json object
            json object containing the uv index and time of the data for the next 3 days, hourly format.
        """
        url = fr"https://air-quality-api.open-meteo.com/v1/air-quality?{city}&hourly=uv_index&forecast_days=7&timezone=America%2FSao_Paulo"
        response = requests.get(url)
        return response.json()

    def filter_data(self, data, city):
        """
        Filters the data from the request. Returns a list of dictionaries containing
        the uv index and time for the next 3 days, hourly.

        Parameters
        ----------
        data : json object containing the data
        city : city to get data from

        Returns
        -------
        list of dictionaries
            list of dictionaries in the format:
                ```
                {
                    'local': city,
                    'hora': time_,
                    'uv_index': uv_index,
                }
                ```
        """	

        all_time = data["hourly"]['time']
        all_uv_index = data["hourly"]['uv_index']

        events_uv = []

        format = '%Y-%m-%dT%H:%M'
        now = datetime.datetime.now()
        now = now.replace(minute=0, second=0, microsecond=0)
        
        for time_, uv_index in zip(all_time, all_uv_index):
            time_formatted = datetime.datetime.strptime(time_, format)

            # if time is less then today date, or if time is more than 3 days from today
            if time_formatted < now:
                continue
            
            events_uv.append({
                'local': city,
                'hora': time_,
                'uv_index': uv_index,
            })

        return events_uv

    def run(self, city):
        """
        Runs the producer. Requests data from Open Meteo API, filters it and sends it to Kafka.

        Parameters
        ----------
        city : str
            city to get data from

        Returns
        -------
        list of dictionaries
            list of uv index events
        """
        
        data = self.request_data(CITIES[city])
        events_uv = self.filter_data(data, city)

        for e_uv_index in events_uv:
            self.send_data(e_uv_index, self.uv_index_topic, city)

        self.producer.flush()
        print(len(events_uv), ' events sent to Kafka at', datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S'), "for city", city)
        return events_uv
    
    def run_forever(self):
        """
        Runs the producer forever. Requests data from Open Meteo API,
        filters it and sends it to Kafka every hour. Executes for all
        cities in CITIES.

        Parameters
        ----------
        None.

        Returns
        -------
        None.
        """
        while True:
            print('Producing data...')
            for city in CITIES:
                self.run(city)
            time.sleep(3600)

if __name__ == "__main__": 
    obj = UvProducer()
    obj.run_forever()