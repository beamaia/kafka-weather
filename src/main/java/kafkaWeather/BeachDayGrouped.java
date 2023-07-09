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

class BeachDayGrouped{  
	public static void main(String[] args){    
        // Configuring serializers
		Serde<String> stringSerde = Serdes.String();
		final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();        
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
		
        
        Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-weather-beach-day-ordered");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092"); 
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        // Configuring consumer
		final Consumed<String, JsonNode> consumed = Consumed.with(stringSerde, jsonSerde);

		final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, JsonNode> beachHour = builder.stream("beachHour", consumed);

        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");

        KTable<String, JsonNode> aggregatedTable = beachHour
        .filter((key, value) -> value.get("boaHora").asBoolean())
        .selectKey((key, value) -> value.get("local").asText() + value.get("hora").asText().split("T")[0] + value.get("isDay").asText())
        .groupByKey()
        .aggregate(
            () -> JsonNodeFactory.instance.objectNode(),
            (key, value, aggregate) -> {
                ObjectNode result = JsonNodeFactory.instance.objectNode();
                result.put("local", value.get("local").asText());
                result.put("boaHora", value.get("boaHora").asBoolean());
                result.put("isDay", value.get("isDay").asBoolean());
                result.put("dia", value.get("hora").asText().split("T")[0]);
                
                if (aggregate.isEmpty()) {
                    // Handle the case when aggregate is null
                    // initialize JsonNode list
                    List<JsonNode> list = new ArrayList<JsonNode>();
                    
                    // create temp JsonNode
                    ObjectNode temp = JsonNodeFactory.instance.objectNode();
                    temp.put("hora", value.get("hora").asText());

                    // add temp JsonNode to list
                    list.add(temp);

                    // add list to result
                    result.put("intervalos", list.toString());
                    return result;
                }

                String periods = aggregate.get("intervalos").asText();

                List<JsonNode> jsonNodes = new ArrayList<>();
                try {
                    // Create an ObjectMapper instance
                    ObjectMapper mapper = new ObjectMapper();
        
                    // Read the JSON string as an ArrayNode
                    ArrayNode arrayNode = mapper.readValue(periods, ArrayNode.class);
        
                    // Iterate over the elements in the ArrayNode and add them to the list
                    for (JsonNode jsonNode : arrayNode) {
                        jsonNodes.add(jsonNode);
                    }
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                
                // create temp JsonNode
                ObjectNode temp = JsonNodeFactory.instance.objectNode();
                temp.put("hora", value.get("hora").asText());

                // Add temp JsonNode to list if it doesn't exist
                if (!jsonNodes.contains(temp))
                    jsonNodes.add(temp);

                // add list to result
                result.put("intervalos", jsonNodes.toString());

                return result;
            },
            Materialized.with(stringSerde, jsonSerde)
        );

        // Order the intervals
        KTable<String, JsonNode> orderedTable = aggregatedTable.
        mapValues((key, value) -> {
            // Get the list of intervals
            String periods = value.get("intervalos").asText();

            List<JsonNode> jsonNodes = new ArrayList<>();
            try {
                // Create an ObjectMapper instance
                ObjectMapper mapper = new ObjectMapper();
    
                // Read the JSON string as an ArrayNode
                ArrayNode arrayNode = mapper.readValue(periods, ArrayNode.class);
    
                // Iterate over the elements in the ArrayNode and add them to the list
                for (JsonNode jsonNode : arrayNode) {
                    jsonNodes.add(jsonNode);
                }
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

            // Order the list
            jsonNodes.sort((o1, o2) -> {
                String time1 = o1.get("hora").asText();
                String time2 = o2.get("hora").asText();

                LocalDateTime dateTime1 = LocalDateTime.parse(time1, formatter);
                LocalDateTime dateTime2 = LocalDateTime.parse(time2, formatter);

                return dateTime1.compareTo(dateTime2);
            });

            // Create a new JsonNode with the ordered list
            ObjectNode result = JsonNodeFactory.instance.objectNode();
            result.put("local", value.get("local").asText());
            result.put("boaHora", value.get("boaHora").asBoolean());
            result.put("isDay", value.get("isDay").asBoolean());
            result.put("dia", value.get("dia").asText());
            result.put("intervalos", jsonNodes.toString());

            return result;
        });

        // To stream
        KStream<String, JsonNode> joinedStream = orderedTable.toStream().selectKey((key, value) -> value.get("local").asText());

        // Print the final events to the console
        joinedStream.foreach((key, value) -> System.out.println("Event: " + value + " Key: " + key));
        
        // Write the final events back to Kafka
        joinedStream.to("beachDayGrouped", Produced.with(stringSerde, jsonSerde));

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.start();
	}
}  