echo "Deleting topics..."
/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic uvIndex  
/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic temperature
/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic precipitationProbability  
/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic beachDay  