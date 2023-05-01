kafka-topics.sh --bootstrap-server localhost:9092 --create --topic wave_height --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic temperature --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic precipitation_probability --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic beach_day --partitions 3 --replication-factor 1
