echo "Creating topics..."
/kafka/bin/kafka-topics.sh --create --topic uvIndex --partitions 3 --replication-factor 1 --bootstrap-server kafka:9092
/kafka/bin/kafka-topics.sh --create --topic temperature --partitions 3 --replication-factor 1 --bootstrap-server kafka:9092
/kafka/bin/kafka-topics.sh --create --topic precipitationProbability --partitions 3 --replication-factor 1 --bootstrap-server kafka:9092
/kafka/bin/kafka-topics.sh --create --topic beachHour --partitions 3 --replication-factor 1 --bootstrap-server kafka:9092
/kafka/bin/kafka-topics.sh --create --topic beachDay --partitions 3 --replication-factor 1 --bootstrap-server kafka:9092

/kafka/bin/kafka-topics.sh --create --topic sunset --partitions 3 --replication-factor 1 --bootstrap-server kafka:9092
/kafka/bin/kafka-topics.sh --create --topic sunrise --partitions 3 --replication-factor 1 --bootstrap-server kafka:9092
/kafka/bin/kafka-topics.sh --create --topic sunHour --partitions 3 --replication-factor 1 --bootstrap-server kafka:9092

/kafka/bin/kafka-topics.sh --create --topic beachHourFiltered --partitions 3 --replication-factor 1 --bootstrap-server kafka:9092
/kafka/bin/kafka-topics.sh --create --topic beachDayGrouped --partitions 3 --replication-factor 1 --bootstrap-server kafka:9092