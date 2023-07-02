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
                
                System.out.println("Aggregate" + aggregate.toString() + " Value" + value.toString());
                if (aggregate.isEmpty()) {
                    // Handle the case when aggregate is null
                    // initialize JsonNode list
                    List<JsonNode> list = new ArrayList<JsonNode>();
                    
                    // create temp JsonNode
                    ObjectNode temp = JsonNodeFactory.instance.objectNode();
                    temp.put("inicio", value.get("hora").asText());
                    temp.put("fim", value.get("hora").asText());

                    // add temp JsonNode to list
                    list.add(temp);

                    // add list to result
                    result.put("intervalos", list.toString());
                    return result;
                }

                // String jsonString = "[{\"inicio\":\"2023-07-03T15:00\",\"fim\":\"2023-07-03T15:00\"}]";

                // ObjectMapper objectMapper = new ObjectMapper();
                // JsonNode jsonNode = objectMapper.readTree(jsonString);
                // List<JsonNode> list = new ArrayList<>();
                // if (jsonNode.isArray()) {
                //     Iterator<JsonNode> iterator = jsonNode.elements();
                //     while (iterator.hasNext()) {
                //         JsonNode element = iterator.next();
                //         list.add(element);
                //     }
                // }

                // Handle the case when aggregate is not null
                // get intervalos from aggregate
                List<JsonNode> list = new ArrayList<JsonNode>();
                String intervalos = aggregate.get("intervalos").asText();
                
                System.out.println("Intervalo" + intervalos);

                // create temp JsonNode
                ObjectNode temp = JsonNodeFactory.instance.objectNode();
                temp.put("inicio", value.get("hora").asText());
                temp.put("fim", value.get("hora").asText());

                // Get string from JsonNode
                String tempString = temp.toString();

                // add temp string to intervalos
                intervalos = intervalos.substring(0, intervalos.length() - 1) + "," + tempString + "]";
                
                // update intervalos
                result.put("intervalos", intervalos);
                return result;
            },
            Materialized.with(stringSerde, jsonSerde)
        );

        // To stream
        KStream<String, JsonNode> joinedStream = aggregatedTable.toStream();

        // Print the final events to the console
        joinedStream.foreach((key, value) -> System.out.println("Event: " + value + " Key: " + key));
        
        // Write the final events back to Kafka
        joinedStream.to("beachDay", Produced.with(stringSerde, jsonSerde));

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.start();
	}
}  