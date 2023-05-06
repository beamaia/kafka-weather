kafka-topics.sh --bootstrap-server localhost:9092 --create --topic wave_height --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic weather --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic precipitation --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic beach_day --partitions 3 --replication-factor 1