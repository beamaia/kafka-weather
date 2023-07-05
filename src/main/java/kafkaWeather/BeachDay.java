package kafkaWeather;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;


import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import java.util.Properties;
import java.time.Duration;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.util.ArrayList;
import java.util.List;

class BeachDay{  
	public static void main(String[] args){    
        // Configuring serializers
		Serde<String> stringSerde = Serdes.String();
		final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();        
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
		
        
        Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-weather-beach-day");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092"); 
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        // Configuring consumer
		final Consumed<String, JsonNode> consumed = Consumed.with(stringSerde, jsonSerde);

		final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, JsonNode> beachDayGrouped = builder.stream("beachDayGrouped", consumed);

        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");

        // Join consecutives periods in intervals
        KTable<String, JsonNode> joinedIntervals = beachDayGrouped
            .selectKey((key, value) -> value.get("local").asText() + value.get("dia").asText() + value.get("isDay").asText())
            .groupByKey()           
            .reduce((key, value) -> {
                // print value
                System.out.println("value: " + value + "\n");
                
                // get intervals strings
                String intervalsString= value.get("intervalos").asText();

                // convert intervals string to array
                List<JsonNode> intervals = new ArrayList<>();
                try {
                    // Create an ObjectMapper instance
                    ObjectMapper mapper = new ObjectMapper();
        
                    // Read the JSON string as an ArrayNode
                    ArrayNode arrayNode = mapper.readValue(intervalsString, ArrayNode.class);
        
                    // Iterate over the elements in the ArrayNode and add them to the list
                    for (JsonNode jsonNode : arrayNode) {
                        intervals.add(jsonNode);
                    }
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }


                // create a new array to store the new intervals
                ArrayNode newIntervals = JsonNodeFactory.instance.arrayNode();

                if (intervals.size() == 1) {
                    JsonNode auxNode = intervals.get(0);
                    ObjectNode iniEnd = JsonNodeFactory.instance.objectNode();
                    iniEnd.put("inicio", auxNode.get("hora").asText());
                    iniEnd.put("fim", auxNode.get("hora").asText());
                    newIntervals.add(iniEnd);

                } else {
                // create loop to join consecutive intervals
                    while (intervals.size() > 1) {
                        // get the first interval
                        JsonNode outsideLoop = intervals.get(0);

                        // remove the first interval
                        intervals.remove(0);
                        
                        // aux node
                        ObjectNode auxNode = JsonNodeFactory.instance.objectNode();
                        
                        // aux node to store outsideLoop
                        auxNode.put("hora", outsideLoop.get("hora").asText());

                        // start node with aux value
                        ObjectNode iniEnd = JsonNodeFactory.instance.objectNode();
                        iniEnd.put("inicio", auxNode.get("hora").asText());
                        iniEnd.put("fim", auxNode.get("hora").asText());
                        
                        // iter to get the next interval
                        while (intervals.size() > 0) {

                            LocalDateTime startInterval = LocalDateTime.parse(auxNode.get("hora").asText(), formatter);

                            // get the next interval
                            JsonNode nextInterval = intervals.get(0);

                            // get the start of the next interval
                            LocalDateTime startNextInterval = LocalDateTime.parse(nextInterval.get("hora").asText(), formatter);

                            // check if the next interval is consecutive
                            if (startInterval.plusHours(1).equals(startNextInterval)) {
                                // remove the first interval
                                intervals.remove(0);

                                // update fim
                                iniEnd.put("fim", nextInterval.get("hora").asText());
                                auxNode = (ObjectNode) nextInterval;

                            } else {
                                break;
                            }
                        
                        }

                        // add the new interval to the new intervals array
                        newIntervals.add(iniEnd);

                        // verify if length is 0
                        if (intervals.size() == 0) {
                            break;
                        }
                    }
                }

                // change minutes of every final key to 59
                for (JsonNode interval : newIntervals) {
                    // get the hour
                    String hour = interval.get("fim").asText().substring(0, 13);
                    // change the minutes
                    String newHour = hour + ":59";
                    // update the hour
                    ((ObjectNode) interval).put("fim", newHour);
                }
                
                // create copy of value
                ObjectNode newValue = JsonNodeFactory.instance.objectNode();
                newValue.put("local", value.get("local").asText());
                newValue.put("boaHora", value.get("boaHora").asText());
                newValue.put("isDay", value.get("isDay").asText());
                newValue.put("dia", value.get("dia").asText());
                newValue.put("intervalos", newIntervals.toString());

                return newValue;

            });

        
        // break every value in interval into new events
        KStream<String, JsonNode> events = joinedIntervals
            .toStream()
            .flatMapValues(value -> {
                // get intervals strings
                String intervalsString= value.get("intervalos").asText();

                // convert intervals string to array
                List<JsonNode> intervals = new ArrayList<>();
                try {
                    // Create an ObjectMapper instance
                    ObjectMapper mapper = new ObjectMapper();
        
                    // Read the JSON string as an ArrayNode
                    ArrayNode arrayNode = mapper.readValue(intervalsString, ArrayNode.class);
        
                    // Iterate over the elements in the ArrayNode and add them to the list
                    for (JsonNode jsonNode : arrayNode) {
                        intervals.add(jsonNode);
                    }
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }

                // create a new array to store the new events
                ArrayNode newEvents = JsonNodeFactory.instance.arrayNode();

                // create loop to create individual events
                for (JsonNode interval : intervals) {
                    System.out.println("interval: " + interval + "\n");
                    // create copy of value
                    ObjectNode newValue = JsonNodeFactory.instance.objectNode();
                    newValue.put("local", value.get("local").asText());
                    newValue.put("isDay", value.get("isDay").asText());
                    newValue.put("dia", value.get("dia").asText());
                    newValue.put("boaHora", value.get("boaHora").asText());
                    newValue.put("inicio", interval.get("inicio").asText());
                    newValue.put("fim", interval.get("fim").asText());

                    // add the new event to the new events array
                    newEvents.add(newValue);
                }

                // return the new events
                return newEvents;
            }).selectKey((key, value) -> value.get("local").asText());

        // Print the final events to the console
        events.foreach((key, value) -> System.out.println("Event: " + value + " Key: " + key));

    
        // Write the final events back to Kafka
        events.to("beachDay", Produced.with(stringSerde, jsonSerde));

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.start();
	}
}  