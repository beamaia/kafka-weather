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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;

import java.util.Properties;

// Set logging level to ERROR
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Level;


class MainClass{  
	public static void main(String[] args){    
		// Set logging level to ERROR
		// Logger.getLogger("org.apache.kafka").setLevel(Level.OFF);
		java.util.logging.Logger.getLogger("org.apache.kafka.clients.consumer.internals").setLevel(java.util.logging.Level.OFF);
		
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-teste");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092"); 
		// props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");


		Serde<String> stringSerde = Serdes.String();

		final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

		final Consumed<String, JsonNode> consumed = Consumed.with(stringSerde, jsonSerde);

		final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, JsonNode> views = builder.stream("uvIndex", consumed)
		.filter((key, value) -> {
			JsonNode localNode = value.get("local");
			return localNode.asText().equals("Cabo Frio");
		});


		views.to("sunHours", Produced.with(stringSerde, jsonSerde));

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.start();

		
		System.out.println("Stream criado");

		// streams.close();
	}
}  