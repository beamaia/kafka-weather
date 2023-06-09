echo "Deleting topics..."
/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic uvIndex  
/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic temperature
/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic precipitationProbability  
/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic beachDay  
/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic beachHour

/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic sunset
/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic sunrise
/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic sunHour

/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic beachHourFiltered
/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic beachDayGrouped
