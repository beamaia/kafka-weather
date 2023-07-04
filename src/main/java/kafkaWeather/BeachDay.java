// package kafkaWeather;

// import org.apache.kafka.common.serialization.Serdes;
// import org.apache.kafka.common.serialization.Serde;
// import org.apache.kafka.streams.kstream.Produced;
// import org.apache.kafka.streams.kstream.Consumed;
// import org.apache.kafka.streams.kstream.KStream;
// import org.apache.kafka.streams.StreamsBuilder;
// import org.apache.kafka.streams.kstream.KTable;
// import org.apache.kafka.streams.StreamsConfig;
// import org.apache.kafka.streams.KafkaStreams;
// import org.apache.kafka.common.serialization.Serializer;
// import org.apache.kafka.common.serialization.Deserializer;
// import org.apache.kafka.streams.kstream.Joined;
// import org.apache.kafka.streams.kstream.ValueJoiner;
// import org.apache.kafka.streams.kstream.JoinWindows;
// import org.apache.kafka.streams.kstream.ValueMapper;
// import org.apache.kafka.streams.kstream.TimeWindows;
// import org.apache.kafka.streams.kstream.Windowed;

// import com.fasterxml.jackson.databind.JsonNode;
// import com.fasterxml.jackson.databind.ObjectMapper;
// import com.fasterxml.jackson.databind.node.JsonNodeFactory;
// import com.fasterxml.jackson.databind.node.ObjectNode;
// import org.apache.kafka.connect.json.JsonDeserializer;
// import org.apache.kafka.connect.json.JsonSerializer;


// import org.apache.kafka.streams.*;
// import org.apache.kafka.streams.kstream.*;

// import java.time.LocalDateTime;
// import java.time.LocalTime;
// import java.time.format.DateTimeFormatter;

// import java.util.Properties;
// import java.time.Duration;
// import com.fasterxml.jackson.databind.node.ArrayNode;
// import com.fasterxml.jackson.databind.ObjectMapper;
// import com.fasterxml.jackson.core.JsonProcessingException;

// import java.util.ArrayList;
// import java.util.List;

// class BeachDayOrdered{  
// 	public static void main(String[] args){    
//         // Configuring serializers
// 		Serde<String> stringSerde = Serdes.String();
// 		final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
//         final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();        
//         final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
		
        
//         Properties props = new Properties();
// 		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-weather-beach-day");
// 		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092"); 
//         props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//         props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


//         // Configuring consumer
// 		final Consumed<String, JsonNode> consumed = Consumed.with(stringSerde, jsonSerde);

// 		final StreamsBuilder builder = new StreamsBuilder();
//         final KStream<String, JsonNode> beachHour = builder.stream("beachHour", consumed);

//         final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");

//         // Join consecutives periods in intervals
//         KTable<String, JsonNode> joinedIntervals = beachHour
//             .selectKey((key, value) -> value.get("local").asText() + value.get("dia").asText().split("T")[0] + value.get("isDay").asText())
//             .groupByKey()
//             .windowedBy(TimeWindows.of(Duration.ofHours(1)))
//             .mapValues((value) -> {

//                 // get the intervals from value
//                 ArrayNode intervals = (ArrayNode) value.get("intervals");

//                 // create a new array to store the new intervals
//                 ArrayNode newIntervals = JsonNodeFactory.instance.arrayNode();

//                 // get the first interval
//                 JsonNode firstInterval = intervals.get(0);

//                 // get the last interval
//                 JsonNode lastInterval = intervals.get(intervals.size() - 1);

//                 // create loop to join consecutive intervals
//                 while (intervals.size() > 1) {
//                     // get the first interval
//                     firstInterval = intervals.get(0);

//                     // remove the first interval
//                     intervals.remove(0);

//                     // get the start of the first interval
//                     ObjectNode newInterval = JsonNodeFactory.instance.objectNode();
                    
//                     newInterval.put("inicio", firstInterval.get("hora").asText());
//                     newInterval.put("fim", firstInterval.get("hora").asText());
                    
//                     // aux node
//                     ObjectNode auxNode = JsonNodeFactory.instance.objectNode();
                    
//                     // aux node to first interval
//                     auxNode.put("inicio", firstInterval.get("hora").asText());
//                     auxNode.put("fim", firstInterval.get("hora").asText());
                    
                    
//                     // iter to get the next interval
//                     while (intervals.size() > 1) {

//                         LocalDateTime startInterval = LocalDateTime.parse(auxNode.get("hora").asText(), formatter);
//                         // get the next interval
//                         JsonNode nextInterval = intervals.get(0);

//                         // get the start of the next interval
//                         LocalDateTime startNextInterval = LocalDateTime.parse(nextInterval.get("hora").asText(), formatter);

//                         // check if the next interval is consecutive
//                         if (startFirstInterval.plusHours(1).equals(startNextInterval)) {
//                             // remove the first interval
//                             intervals.remove(0);

//                             // remove the next interval
//                             intervals.remove(0);

//                             // update fim
//                             newInterval.put("fim", nextInterval.get("hora").asText());
//                             auxTime = startNextInterval;

//                         } else {
//                             break;
//                         }
                    

//                     }

//                     // add the new interval to the new intervals array
//                     newIntervals.add(newInterval);

//                     // verify if length is 0
//                     if (intervals.size() == 0) {
//                         break;
//                     }
//                 }
                

//                 }
                
            
//             })


//         // To stream
//         KStream<String, JsonNode> joinedStream = orderedTable.toStream();

//         // Print the final events to the console
//         joinedStream.foreach((key, value) -> System.out.println("Event: " + value + " Key: " + key));
        
//         // Write the final events back to Kafka
//         joinedStream.to("beachDayOrdered", Produced.with(stringSerde, jsonSerde));

// 		KafkaStreams streams = new KafkaStreams(builder.build(), props);
// 		streams.start();
// 	}
// }  