package fr.dzinsou.kafkabench;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class MyKafkaConsumer implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyKafkaConsumer.class);

    private String kafkaBootstrapServers;
    private String securityProtocol;
    private String kafkaKrbServiceName;
    private String consumerGroupId;
    private String autoOffsetResetConfig;
    private String enableAutoCommitConfig;
    private List<String> topics;

    private org.apache.kafka.clients.consumer.KafkaConsumer<String, String> kafkaConsumer;

    public MyKafkaConsumer(String kafkaBootstrapServers, String securityProtocol, String kafkaKrbServiceName,
                           String consumerGroupId, String autoOffsetResetConfig, String enableAutoCommitConfig,
                           List<String> topics) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.securityProtocol = securityProtocol;
        this.kafkaKrbServiceName = kafkaKrbServiceName;
        this.consumerGroupId = consumerGroupId;
        this.autoOffsetResetConfig = autoOffsetResetConfig;
        this.enableAutoCommitConfig = enableAutoCommitConfig;
        this.topics = topics;
    }

    public void run() {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaBootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, this.consumerGroupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, this.autoOffsetResetConfig);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, this.enableAutoCommitConfig);
        consumerProps.put("security.protocol", this.securityProtocol);
        if (this.kafkaKrbServiceName != null && !this.kafkaKrbServiceName.equals("")) {
            consumerProps.put("sasl.kerberos.service.name", this.kafkaKrbServiceName);
        }

        this.kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(consumerProps);
        this.kafkaConsumer.subscribe(this.topics);

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(0);
            if (records.count() > 0) {
                LOGGER.info(String.format("Received [%d] records", records.count()));
            }
        }
    }

    @Override
    public void close() {
        if (this.kafkaConsumer != null) {
            LOGGER.info("Closing consumer [{}]", this.kafkaBootstrapServers);
            this.kafkaConsumer.close();
        }
    }

    public static void main(String[] args) {
        String kafkaBootstrapServers = args[0];
        String securityProtocol = args[1];
        String kafkaKrbServiceName = args[2];
        String consumerGroupId = args[3];
        String autoOffsetResetConfig = args[4];
        String enableAutoCommitConfig = args[5];
        List<String> topics = Arrays.asList(args[6].split(","));

        try (MyKafkaConsumer myKafkaConsumer = new MyKafkaConsumer(kafkaBootstrapServers, securityProtocol, kafkaKrbServiceName, consumerGroupId, autoOffsetResetConfig, enableAutoCommitConfig, topics)) {
            myKafkaConsumer.run();
        }
    }
}
