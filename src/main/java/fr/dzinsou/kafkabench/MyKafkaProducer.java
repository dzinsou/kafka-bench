package fr.dzinsou.kafkabench;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.Future;

public class MyKafkaProducer implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyKafkaProducer.class);

    private Producer<String, String> kafkaProducer;

    private String kafkaBootstrapServers;
    private String securityProtocol;
    private String kafkaKrbServiceName;

    public MyKafkaProducer(String kafkaBootstrapServers, String securityProtocol, String kafkaKrbServiceName) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.securityProtocol = securityProtocol;
        this.kafkaKrbServiceName = kafkaKrbServiceName;
        this.init();
    }

    private void init() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaBootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("security.protocol", this.securityProtocol);
        if (this.kafkaKrbServiceName != null && !this.kafkaKrbServiceName.equals("")) {
            producerProps.put("sasl.kerberos.service.name", this.kafkaKrbServiceName);
        }
        this.kafkaProducer = new KafkaProducer<>(producerProps);
    }

    public Future<RecordMetadata> sendMessage(String topicName, String messageKey, String messageValue, Callback callback) {
        return this.kafkaProducer.send(new ProducerRecord<>(topicName, messageKey, messageValue), callback);
    }

    @Override
    public void close() {
        if (this.kafkaProducer != null) {
            LOGGER.info("Closing producer [{}]", this.kafkaBootstrapServers);
            this.kafkaProducer.close();
        }
    }

    public static void main(String[] args) {
        String kafkaBootstrapServers = args[0];
        String securityProtocol = args[1];
        String kafkaKrbServiceName = args[2];
        String topicName = args[3];
        int iterationsCount = Integer.parseInt(args[4]);

        try (MyKafkaProducer myKafkaProducer = new MyKafkaProducer(kafkaBootstrapServers, securityProtocol, kafkaKrbServiceName)) {
            for (int i = 0; i < iterationsCount; i++) {
                myKafkaProducer.sendMessage(topicName, null, "hello world", null);
            }
        }
    }
}
