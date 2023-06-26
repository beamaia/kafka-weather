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
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;

import java.util.Properties;
import java.time.Duration;


class SunHour{  
	public static void main(String[] args){    
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-weather");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092"); 
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Configuring serializers
		Serde<String> stringSerde = Serdes.String();
		final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();        
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        // Configuring consumer
		final Consumed<String, JsonNode> consumed = Consumed.with(stringSerde, jsonSerde);

		final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, JsonNode> sunrise = builder.stream("sunrise", consumed);
        final KStream<String, JsonNode> sunset = builder.stream("sunset", consumed);

        // Joining streams
        // Perform necessary transformations on sunriseStream
        KTable<String, JsonNode> sunriseTable = sunrise
                .selectKey((key, value) -> value.get("local").asText() + value.get("hora").asText().split("T")[0])
                .groupByKey()
                .reduce((aggValue, newValue) -> newValue);

        // Perform necessary transformations on sunsetStream
        KTable<String, JsonNode> sunsetTable = sunset
                .selectKey((key, value) -> value.get("local").asText() + value.get("hora").asText().split("T")[0]) 
                .groupByKey()
                .reduce((aggValue, newValue) -> newValue);

        // Join the sunriseTable and sunsetTable based on 'local' key
        KTable<String, JsonNode> joinedTable = sunriseTable
        .join(sunsetTable, (sunriseValue, sunsetValue) -> {
            ObjectNode resultNode = JsonNodeFactory.instance.objectNode();
            resultNode.put("local", sunriseValue.get("local"));
            resultNode.put("sunrise", sunriseValue.get("hora"));
            resultNode.put("sunset", sunsetValue.get("hora"));
            return resultNode;
        });

        // Convert the joinedTable to a stream
        KStream<String, JsonNode> joinedStream = joinedTable.toStream();

        // Print the final events to the console
        joinedStream.foreach((key, value) -> System.out.println("Event: " + value + " Key: " + key));
        
        // Write the final events back to Kafka
        joinedStream.to("sunHour", Produced.with(stringSerde, jsonSerde));

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.start();

		
		System.out.println("Stream criado");

		// streams.close();
	}
}  