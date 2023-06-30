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
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-weather");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092"); 
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        // Configuring consumer
		final Consumed<String, JsonNode> consumed = Consumed.with(stringSerde, jsonSerde);

		final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, JsonNode> beachHour = builder.stream("beachHour", consumed);

        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");
        final LocalDateTime now = LocalDateTime.now().withMinute(0).withSecond(0).withNano(0);

        final TimeWindowedKStream<String, JsonNode> aggregatedStream = beachHour
        .groupBy((key, value) -> value.get("hora").asText())
        .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
        .aggregate(
                () -> new ArrayList<>(),
                (key, value, aggregate) -> {
                    aggregate.add(value);
                    return aggregate;
                },
                Materialized.with(Serdes.String(), jsonSerde)
        )
        .toStream()
        .flatMapValues((ValueMapper<List<JsonNode>, Iterable<JsonNode>>) aggregate -> aggregate)
        .mapValues((ValueMapper<JsonNode, JsonNode>) value -> {
            String local = value.get("local").asText();
            String start = value.get("hora").asText();
            String end = LocalDateTime.parse(value.get("hora").asText()).plusHours(1).withMinute(59).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm"));

            return createEventJsonNode(local, start, end);
        });
        
        // To stream
        KStream<String, JsonNode> joinedStream = aggregatedStream.toStream();

        // Print the final events to the console
        joinedStream.foreach((key, value) -> System.out.println("Event: " + value + " Key: " + key));
        
        // Write the final events back to Kafka
        joinedStream.to("beachDay", Produced.with(stringSerde, jsonSerde));

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.start();

		
		System.out.println("Stream criado");

		// streams.close();
	}

    private static JsonNode createEventJsonNode(String city, String start, String end) {
        ObjectNode eventNode = JsonNodeFactory.instance.objectNode();
        eventNode.put("local", city);
        eventNode.put("inicio", start);
        eventNode.put("fim", end);
        return eventNode;
    }
}  