from kafka import KafkaConsumer
import json
import datetime
import difflib
import argparse

from decouple import config

CITIES = json.loads(open('assets/cities.json', 'r').read())

class BeachDayClient:
    """
    This class is responsible for consuming beach day data from Kafka.
    It consumes data from the topic 'beachDay' and prints the beach day hours for the given city.
    """
    beach_day_topic = 'beachDay'

    def __init__(self, city):
        # Creates kafka consumer and subscribes to the beachDay topic
        server = config('KAFKA_SERVER')
        self.consumer = KafkaConsumer(bootstrap_servers=f'{server}:9092', auto_offset_reset='earliest', value_deserializer=lambda x: json.loads(x))
        self.consumer.subscribe(self.beach_day_topic)

        # Verifies if the given city is valid
        self.__verify_city(city)
    
    def __verify_city(self, city):
        """
        Verifies if the given city is valid. If it is, sets the city attribute to the given city.
        To be valid, the given city must be in the cities.json file.

        Parameters
        ----------
        city : str
            City to be verified.

        Raises
        ------
        Exception
            If the given city is not valid.

        Returns
        -------
        None.
        """
        if not any(difflib.get_close_matches(city, CITIES, n=1, cutoff=0.8)):
            raise Exception(f"City {city} not found")
        else:
            self.city = difflib.get_close_matches(city, CITIES, n=1, cutoff=0.8)[0]
            print(f"City {city} found. Using {self.city} instead")

    def __get_partition_messages(self, partitions):
        """
        Gets messages from dictionary of partitions.
        
        Parameters
        ----------
        partitions : dict
            Dictionary of partitions. Parition number as key and list of records as value.
        
        Returns
        -------
        list
            List of messages.
        """
        messages = []
        for _, message_list in partitions.items():
            for message in message_list:   
                msg_json = message.value
                if msg_json['local'] != self.city:
                    continue

                msg_json['inicio'] = datetime.datetime.strptime(msg_json['inicio'], '%Y-%m-%dT%H:%M')
                msg_json['fim'] = datetime.datetime.strptime(msg_json['fim'], '%Y-%m-%dT%H:%M')

                messages.append(msg_json)

        return messages
    
    def get_messages(self):
        """
        Gets messages from the beachDay topic.

        Parameters
        ----------
        None.

        Returns
        -------
        list
            List of messages.
        """
        messages = []

        while True:
            print("Attempting to poll data...")
            partitions = self.consumer.poll(timeout_ms=1000)
            
            if not len(partitions):
                break
            
            messages.extend(self.__get_partition_messages(partitions))
        
        print("Polling finished. Sending data...")
        messages = [dict(tupleized) for tupleized in set(tuple(item.items()) for item in messages)]
        messages = sorted(messages, key=lambda x: x['inicio'])
        return messages
                 
    def run(self):
        """
        Runs the client. Gets messages from the beachDay topic and prints the beach day hours 
        for the given city.

        Parameters
        ----------
        None.

        Returns
        -------
        None.
        """
        print("Verificando horarios bons para ir a praia...")
        messages = self.get_messages()

        for message in messages:
            start = message['inicio']
            end = message['fim']

            print(f"Dia {start.day:02}/{start.month:02} - Horario bom para ir a praia: {start.hour:02}:{start.minute:02} - {end.hour:02}:{end.minute:02} ")

def parse_args():
    """
    Parses the arguments passed to the client.

    Parameters
    ----------
    None.

    Returns
    -------
    args
        Command line arguments.
    """
    parser = argparse.ArgumentParser(description='Beach Day Client')
    parser.add_argument('--city', type=str, help='City to check beach day', required=True)
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    client = BeachDayClient(args.city)
    client.run()