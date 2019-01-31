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

java -cp kafka-bench-0.1.0.jar -Djava.security.auth.login.config=kafka_client_jaas.conf \\<br/>
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

* bootstrap.servers (comma separated)
* security.protocol (PLAINTEXT, SASL_PLAINTEXT, SASL_SSL)
* sasl.kerberos.service.name (check your JAAS config)
* topic name
* iterations count (number of message to send)

**Example**

java -cp kafka-bench-0.1.0.jar -Djava.security.auth.login.config=kafka_client_jaas.conf \\<br/>
broker1:6667,broker2:6667,broker2:6667 \\<br/>
SASL_PLAINTEXT \\<br/>
kafka \\<br/>
topic \\<br/>
10
