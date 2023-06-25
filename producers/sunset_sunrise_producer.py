from kafka import KafkaProducer
import requests
import datetime
import json
import time

from decouple import config
CITIES = json.loads(open('assets/cities.json', 'r').read())

class SunsetSunriseProducer:
    """
    
    """
    sunset_topic = 'sunset'
    sunrise_topic = 'sunrise'

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
        to get the data. Returns a json object containing the sunset, sunrise and time of 
        the data for the next 7 days.
        Parameters
        ----------
        city : str 
            latitue and longitude to get data from
        
        Returns
        -------
        json object
            json object containing the uv index and time of the data for the next 3 days, hourly format.
        """
        url = fr"https://api.open-meteo.com/v1/forecast?{city}&daily=sunrise,sunset&timezone=America%2FSao_Paulo"
        response = requests.get(url)
        return response.json()

    def filter_data(self, data, city):
        """

        """	

        all_sunrise = data["daily"]['sunrise']
        all_sunset = data["daily"]['sunset']

        events_sunrise = []
        events_sunset = []
        
        for sunrise, sunset in zip(all_sunrise, all_sunset):
            events_sunrise.append({
                'local': city,
                'hora': sunrise,
            })

            events_sunset.append({
                'local': city,
                'hora': sunset,
            })

        return events_sunrise, events_sunset

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
            list of sunrise and sunset events
        """
        
        data = self.request_data(CITIES[city])
        events_sunrise, events_sunset = self.filter_data(data, city)

        for sunrise, sunset in zip(events_sunrise, events_sunset):
            self.send_data(sunrise, self.sunrise_topic, city)
            self.send_data(sunset, self.sunset_topic, city)

        self.producer.flush()
        print(len(events_sunrise), ' events sent to Kafka at', datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S'), "for city", city)
        return events_sunrise, events_sunset
    
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
    obj = SunsetSunriseProducer()
    obj.run_forever()