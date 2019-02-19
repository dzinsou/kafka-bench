# kafka-bench
Kafka bench tool (only for Kerberized cluster)

## Usage
### Compile
mvn clean install

### Run Consumer

Consume messages from a list of topics.

**Arguments**
* bootstrap.servers (comma separated)
* security.protocol (PLAINTEXT, SASL_PLAINTEXT, SASL_SSL)
* sasl.kerberos.service.name (check your JAAS config)
* group.id (A unique string that identifies the consumer group this consumer belongs to)
* auto.offset.reset (earliest or latest)
* enable.auto.commit (If true the consumer's offset will be periodically committed in the background)
* topics list (comma separated)

**Example**

java -cp kafka-bench-0.2.0.jar -Djava.security.auth.login.config=kafka_client_jaas.conf \\<br/>
fr.dzinsou.kafkabench.MyKafkaConsumer \\<br/>
broker1:6667,broker2:6667,broker2:6667 \\<br/>
SASL_PLAINTEXT \\<br/>
kafka \\<br/>
kafka-bench \\<br/>
latest \\<br/>
false \\<br/>
topic1,topic2

### Run Producer

Produce message on a topic.

**Arguments**

* -producer_properties (kafka producer properties)
* -topic (topic name)
* -batch_message_size (message size in bytes)
* -batch_message_count (number of messages per iteration)
* -batch_iteration_count (number of iterations)
* -batch_iteration_pause (pause between iterations in millis)

**Example**

java -cp kafka-bench-0.2.0.jar -Djava.security.auth.login.config=kafka_client_jaas.conf \\<br/>
fr.dzinsou.kafkabench.MyKafkaProducer \\<br/>
-producer_properties producer.properties \\<br/>
-topic topic1 \\<br/>
-batch_message_size 1024 \\<br/>
-batch_message_count 1000000 \\<br/>
-batch_iteration_count 1 \\<br/>
-batch_iteration_pause 10000

### Generate script to create topic(s)

Generate Kafka script to create topic(s) based on an existing configuration.

**Arguments**

* zkUrl (ZooKeeper URL *host:port*)
* createCommandPath (Path of kafka-topics.sh script. For HDP clusters */usr/hdp/current/kafka-broker/bin/kafka-topics.sh*)

**Example**

java -cp kafka-bench-0.2.0.jar -Djava.security.auth.login.config=kafka_client_jaas.conf \\<br/>
fr.dzinsou.kafkabench.MyKafkaShowCreateTopic \\<br/>
localhost:2181 \\<br/>
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh
