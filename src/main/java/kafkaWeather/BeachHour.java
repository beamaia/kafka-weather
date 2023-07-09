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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;

import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;

import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;
import java.util.function.Function;
import java.time.Duration;


class BeachHour{  
	public static void main(String[] args){   
        // Configuring serializers
		Serde<String> stringSerde = Serdes.String();
		final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();        
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
		
        
        Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-weather-beach-hour");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092"); 
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        // Configuring consumer
		final Consumed<String, JsonNode> consumed = Consumed.with(stringSerde, jsonSerde);

		final StreamsBuilder builder = new StreamsBuilder();

        
        // Get stream from beachHour topic
        KStream<String, JsonNode> beachHour = builder.stream("beachHourFiltered", consumed);

        // Transform into ktable
        KTable<String, JsonNode> beachHourTable = beachHour
        .selectKey((key, value) -> value.get("local").asText() + value.get("hora").asText())
        .groupByKey()
        .reduce((aggValue, newValue) -> newValue);

        // Extract local and day from filteredTable
        Function<JsonNode, String> foreignKeyExtractor = (x) -> x.get("local").asText() + x.get("hora").asText().split("T")[0];

        // Get sunhour
        KTable<String, JsonNode> sunhour = builder.stream("sunHour", consumed)
        .selectKey((key, value) -> value.get("local").asText() + value.get("sunrise").asText().split("T")[0])
        .groupByKey()
        .reduce((aggValue, newValue) -> newValue);

        // Join the filteredTable and sunhour based on 'local' key
        KTable<String, JsonNode> joinedTable = beachHourTable.join(sunhour, foreignKeyExtractor, 
        (filteredValue, sunhourValue) -> {
            ObjectNode resultNode = JsonNodeFactory.instance.objectNode();
            resultNode.put("local", filteredValue.get("local"));
            resultNode.put("temperatura", filteredValue.get("temperatura"));
            resultNode.put("pp", filteredValue.get("pp"));
            resultNode.put("uv", filteredValue.get("uv"));
            resultNode.put("hora", filteredValue.get("hora"));
            resultNode.put("sunrise", sunhourValue.get("sunrise"));
            resultNode.put("sunset", sunhourValue.get("sunset"));
            return resultNode;
        });

        // Map values that are during day
        KTable<String, JsonNode> mappedTable = joinedTable.mapValues((key, value) -> {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");

            LocalDateTime sunrise = LocalDateTime.parse(value.get("sunrise").asText(), formatter);
            LocalDateTime sunset = LocalDateTime.parse(value.get("sunset").asText(), formatter);
            LocalDateTime hour = LocalDateTime.parse(value.get("hora").asText(), formatter);
            
            ((ObjectNode) value).remove("sunset");
            ((ObjectNode) value).remove("sunrise");
            
            // Check if hour is between sunrise and sunset
            if (hour.isAfter(sunrise) && hour.isBefore(sunset)) {
                ((ObjectNode) value).put("isDay", true);
            } else {
                ((ObjectNode) value).put("isDay", false);
            }
            
            // Check if good weather conditions
            if (value.get("temperatura").asDouble() > 20 && value.get("uv").asDouble() < 5 && value.get("pp").asDouble() < 30) {
                ((ObjectNode) value).put("boaHora", true);
            } else {
                ((ObjectNode) value).put("boaHora", false);
            }
            
            return value;
        });

        // Convert the joinedTable to a stream
        KStream<String, JsonNode> joinedStream = mappedTable.toStream().selectKey((key, value) -> value.get("local").asText());

        // Print the final events to the console
        joinedStream.foreach((key, value) -> System.out.println("Event: " + value + " Key: " + key));
        
        // Write the final events back to Kafka
        joinedStream.to("beachHour", Produced.with(stringSerde, jsonSerde));
        joinedStream.to("beachHourCopy", Produced.with(stringSerde, jsonSerde));

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.start();
	}
}  