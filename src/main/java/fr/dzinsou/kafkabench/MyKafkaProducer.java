package fr.dzinsou.kafkabench;

import org.apache.commons.cli.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;

public class MyKafkaProducer implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyKafkaProducer.class);

    private Producer<String, String> kafkaProducer;

    private String producerPropertiesFilePath;

    public MyKafkaProducer(String producerPropertiesFilePath) throws IOException {
        this.producerPropertiesFilePath = producerPropertiesFilePath;
        this.init();
    }

    private void init() throws IOException {
        try (FileInputStream fis = new FileInputStream(this.producerPropertiesFilePath)) {
            Properties producerProps = new Properties();
            producerProps.load(fis);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            this.kafkaProducer = new KafkaProducer<>(producerProps);
        }
    }

    public Future<RecordMetadata> sendMessage(String topicName, String messageKey, String messageValue, Callback callback) {
        return this.kafkaProducer.send(new ProducerRecord<>(topicName, messageKey, messageValue), callback);
    }

    @Override
    public void close() {
        if (this.kafkaProducer != null) {
            this.kafkaProducer.close();
        }
    }

    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("producer_properties", true, "Producer properties file");
        options.addOption("topic", true, "Kafka topic");
        options.addOption("batch_message_size", true, "Batch message size (bytes)");
        options.addOption("batch_message_count", true, "Batch message count per iteration");
        options.addOption("batch_iteration_count", true, "Batch iteration count");
        options.addOption("batch_iteration_pause", true, "Batch pause between iterations (milliseconds)");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String producerPropertiesFilePath = cmd.getOptionValue("producer_properties");
        String topicName = cmd.getOptionValue("topic");
        int batchMessageSize = Integer.parseInt(cmd.getOptionValue("batch_message_size"));
        long batchMessageCount = Integer.parseInt(cmd.getOptionValue("batch_message_count"));
        long batchIterationCount = Integer.parseInt(cmd.getOptionValue("batch_iteration_count"));
        long batchIterationPauseMillis = Integer.parseInt(cmd.getOptionValue("batch_iteration_pause"));
        String batchMessage = RandomStringUtils.randomAlphabetic(batchMessageSize);

        LOGGER.info("producerPropertiesFilePath: [{}]", producerPropertiesFilePath);
        LOGGER.info("batchMessageSize          : [{}]", batchMessageSize);
        LOGGER.info("batchMessageCount         : [{}]", batchMessageCount);
        LOGGER.info("batchIterationCount       : [{}]", batchIterationCount);
        LOGGER.info("batchIterationPauseMillis : [{}]", batchIterationPauseMillis);
        LOGGER.info("batchMessage              : [{}]", batchMessage);

        try (MyKafkaProducer myKafkaProducer = new MyKafkaProducer(producerPropertiesFilePath)) {
            MyKafkaCallback myKafkaCallback = new MyKafkaCallback();
            for (int i = 0; i < batchIterationCount; i++) {
                LOGGER.info("Iteration [{}]", i);
                for (int j = 0; j < batchMessageCount; j++) {
                    myKafkaProducer.sendMessage(topicName, "message"+j, batchMessage, myKafkaCallback);
                    myKafkaCallback.incrementSent();
                }
                Thread.sleep(batchIterationPauseMillis);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e.getCause());
        }
    }

    /**
     * Kafka callback
     */
    static class MyKafkaCallback implements Callback {
        private final Logger LOGGER = LoggerFactory.getLogger(MyKafkaCallback.class);

        private int counter = 0;
        private int failed = 0;
        private int succeeded = 0;
        private int sent = 0;

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            counter++;

            if (e != null) {
                failed++;
                LOGGER.error(e.getMessage());
            } else {
                succeeded++;
            }

            if (counter % 1000 == 0) LOGGER.info("Status - [{}][{}][{}]", failed, succeeded, sent - counter);
        }

        private void incrementSent() {
            sent++;
        }
    }
}
