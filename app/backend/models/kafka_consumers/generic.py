from kafka import KafkaProducer, KafkaConsumer
import json
import time
import datetime

from decouple import config


class GenericConsumer:
    """
    This class is responsible for consuming data to the beachDay topic.
    """   
    def __init__(self, topic):
        server = config('KAFKA_SERVER')
        # Creates kafka consumer and producer
        self.consumer = KafkaConsumer(bootstrap_servers=f'{server}:9092', auto_offset_reset='earliest', value_deserializer=lambda x: json.loads(x), enable_auto_commit=False, fetch_max_wait_ms=10000)
        
        # Subscribes to the beachHour topic in order to consume events
        self.topic = topic
        self.consumer.subscribe(self.topic)

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
        messages = []
        for _, message_list in partitions.items():
            for message in message_list:   
                msg_json = message.value
                messages.append(msg_json)
        return messages
    
    def __get_messages(self):
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
        messages = []

        while True:
            print("Attempting to poll data...")
            partitions = self.consumer.poll(timeout_ms=10000, update_offsets=False)
            
            if not len(partitions):
                break
            
            messages = self.__get_partition_messages(partitions)
        
        print("Polling finished. Sending data...")

        return messages
    
    def get(self, city: str = None):
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
        messages = self.__get_messages()


        if city:
            messages = self.__filter_city(messages, city)
        
        for message in messages:
            print(message)

        return messages


    def __filter_city(self, messages, city):
        """
        Filters messages by city.

        Parameters
        ----------
        messages : list
            List of messages.
        city : str
            City to filter messages.

        Returns
        -------
        list
            List of messages filtered by city.
        """
        return [message for message in messages if message['local'] == city]