package fr.dzinsou.kafkabench;

import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
                for (ConsumerRecord<String, String> record : records) {
                    LOGGER.info("Offset [{}] --- Key [{}] --- Value --- [{}]", record.offset(), record.key(), record.value());
                }
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

    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addRequiredOption("bootstrap_servers", "bootstrap_servers",true, "bootstrap servers");
        options.addRequiredOption("security_protocol", "security_protocol", true, "security protocol");
        options.addRequiredOption("krb_service_name", "krb_service_name", true, "kafka kerberos service name");
        options.addRequiredOption("group_id", "group_id", true, "consumer group ID");
        options.addRequiredOption("auto_offset_reset", "auto_offset_reset", true, "consumer auto offset reset");
        options.addRequiredOption("enable_auto_commit", "enable_auto_commit", true, "enable auto commit");
        options.addRequiredOption("topics", "topics", true, "topics");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String kafkaBootstrapServers = cmd.getOptionValue("bootstrap_servers");
        String securityProtocol = cmd.getOptionValue("security_protocol");
        String kafkaKrbServiceName = cmd.getOptionValue("krb_service_name");
        String consumerGroupId = cmd.getOptionValue("group_id");
        String autoOffsetResetConfig = cmd.getOptionValue("auto_offset_reset");
        String enableAutoCommitConfig = cmd.getOptionValue("enable_auto_commit");
        List<String> topics = Arrays.asList(cmd.getOptionValue("topics").split(","));

        try (MyKafkaConsumer myKafkaConsumer = new MyKafkaConsumer(kafkaBootstrapServers, securityProtocol, kafkaKrbServiceName, consumerGroupId, autoOffsetResetConfig, enableAutoCommitConfig, topics)) {
            myKafkaConsumer.run();
        }
    }
}
