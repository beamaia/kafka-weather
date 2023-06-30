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

import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;
import java.time.Duration;


class BeachHour{  
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
        final KStream<String, JsonNode> uv = builder.stream("uvIndex", consumed);
        final KStream<String, JsonNode> temperature = builder.stream("temperature", consumed);
        final KStream<String, JsonNode> precipitationProbability = builder.stream("precipitationProbability", consumed);


        // Joining streams
        // Perform necessary transformations on temperature
        KTable<String, JsonNode> tempTable = temperature
        .selectKey((key, value) -> value.get("local").asText() + value.get("hora").asText())
        .groupByKey()
        .reduce((aggValue, newValue) -> newValue);

        // Perform necessary transformations on precipitationProbability
        KTable<String, JsonNode> precTable = precipitationProbability
        .selectKey((key, value) -> value.get("local").asText() + value.get("hora").asText())
        .groupByKey()
        .reduce((aggValue, newValue) -> newValue);

        // Perform necessary transformations on uvIndex
        KTable<String, JsonNode> uvTable = uv
        .selectKey((key, value) -> value.get("local").asText() + value.get("hora").asText())
        .groupByKey()
        .reduce((aggValue, newValue) -> newValue);

        // Join the sunriseTable and sunsetTable based on 'local' key
        KTable<String, JsonNode> joinTable = tempTable
        .join(precTable, (tempValue, precValue) -> {
            ObjectNode resultNode = JsonNodeFactory.instance.objectNode();
            resultNode.put("local", tempValue.get("local"));
            resultNode.put("temperatura", tempValue.get("temperatura"));
            resultNode.put("pp", precValue.get("pp"));
            return resultNode;
        }).join(uvTable, (joinPrecTempValue, uvValue) -> {
            ObjectNode resultNode = JsonNodeFactory.instance.objectNode();
            resultNode.put("local", joinPrecTempValue.get("local"));
            resultNode.put("temperatura", joinPrecTempValue.get("temperatura"));
            resultNode.put("pp", joinPrecTempValue.get("pp"));
            resultNode.put("uv", uvValue.get("uv_index"));
            return resultNode;
        });
        
        // Filter null values
        KTable<String, JsonNode> filteredTable = joinTable.filter((key, value) -> {
            if (value == null) {
                return false; // Exclude null values
            }
            
            // Check if any field within the JSON object is null
            if (value.get("local") == NullNode.getInstance() || 
                value.get("temperatura") == NullNode.getInstance() || 
                value.get("pp") == NullNode.getInstance() || 
                value.get("uv") == NullNode.getInstance()) {
                return false; // Exclude if any field is null
            }
            
            return true; // Include if all fields are non-null
        });
        
        // Convert the joinedTable to a stream
        KStream<String, JsonNode> joinedStream = filteredTable.toStream();

        // Print the final events to the console
        joinedStream.foreach((key, value) -> System.out.println("Event: " + value + " Key: " + key));
        
        // Write the final events back to Kafka
        joinedStream.to("beachDay", Produced.with(stringSerde, jsonSerde));

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.start();
	}
}  