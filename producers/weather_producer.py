from kafka import KafkaProducer
import requests
import datetime
import json
import time

from decouple import config

CITIES = json.loads(open('assets/cities.json', 'r').read())

class WeatherProducer:
    """
    This class is responsible for producing weather data to Kafka.
    It requests data from the Open Meteo API, filters it and sends it to Kafka
    to the topics 'temperature' and 'precipitationProbability'.
    """
    temp_topic = 'temperature'
    prec_topic = 'precipitationProbability'

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
        Sends data to Kafka. Considers that key is string.
        Can be sent with or without callback.

        Parameters
        ----------
        data : dict 
            data to be sent
        topic : str
            topic to send data to
        key : str 
            key of the message
        with_callback : bool, optional
            if True, sends with callback

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
        to get the data. Returns a json object containing the precipitation probability,
        temperature and time of the data for the next 3 days, hourly format.

        Parameters
        ----------
        city : str
            city to get data from

        Returns
        -------
        json object
        """	
        url = fr"https://api.open-meteo.com/v1/forecast?{city}&hourly=temperature_2m,precipitation_probability&forecast_days=3&forecast_days=7&timezone=America%2FSao_Paulo"
        response = requests.get(url)
        return response.json()

    def filter_data(self, data, city):
        """	
        Filters the data received from the Open Meteo API. Returns a list of
        dictionaries containing the temperature, time and precipitation probability
        for the next 3 days, hourly.

        Parameters
        ----------
        data : dict
            json object containing the data
        city : str
            city to get data from

        Returns
        -------
        tuple
            tuple of list of dictionaries in the format:
            ```
                {
                    'local': city,
                    'hora': time_,
                    'temperatura': temp,
                }
            ```

            and 

            ```
                {
                    'local': city,
                    'hora': time_,  
                    'pp': prec

                }
            ```
        """
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

            if time_formatted < now:
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
        """
        Runs the producer given a certain city. Requests data from Open Meteo API, 
        filters it and sends it to Kafka.

        Parameters
        ----------
        city : str
            city to get data from

        Returns
        -------
        tuple
            tuple of list of dictionaries for temperature and precipitation probability
        """	

        data = self.request_data(CITIES[city])
        events_temp, events_prec = self.filter_data(data, city)

        for e_temp, e_prec in zip(events_temp, events_prec):
            self.send_data(e_temp, self.temp_topic, city)
            self.send_data(e_prec, self.prec_topic, city)

        self.producer.flush()
        print(len(events_temp), ' events sent to Kafka at', datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), "for city", city)
        return events_temp, events_prec
    
    def run_forever(self):
        """
        Runs the producer forever. Requests data from Open Meteo API,
        filters it and sends it to Kafka every hour. Executes for all
        cities in CITIES.

        Parameters
        ----------
        None

        Returns
        -------
        None.
        """
        while True:
            print('Producing data...')
            for city in CITIES:
                self.run(city)
            time.sleep(3600)

if __name__ == '__main__':
    obj = WeatherProducer()
    obj.run_forever()