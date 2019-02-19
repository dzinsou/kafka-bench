package fr.dzinsou.kafkabench;

import kafka.admin.AdminUtils;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class MyKafkaShowCreateTopic {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyKafkaShowCreateTopic.class);

    public static void main(String[] args) {
        String zkUrl = args[0];
        String createCommandPath = args[1];
        ZkUtils zkUtils = ZkUtils.apply(zkUrl, 30000, 30000, JaasUtils.isZkSecurityEnabled());

        StringBuilder kafkaCreateScript = new StringBuilder();
        for (String topic : JavaConversions.seqAsJavaList(zkUtils.getAllTopics())) {
            LOGGER.info("Processing topic: {}", topic);

            List<String> topicAssignmentInputList = new ArrayList<String>() {{ add(topic); }};
            Map<Object, Seq<Object>> topicAssignment = JavaConversions.asJavaMap(
                    zkUtils.getPartitionAssignmentForTopics(JavaConversions.asScalaBuffer(topicAssignmentInputList)).get(topic).get()
            );
            int partitionCount = topicAssignment.size();
            int replicationFactor = topicAssignment.get(0).size();
            LOGGER.debug("\tPartitionCount: {}", partitionCount);
            LOGGER.debug("\tReplicationFactor: {}", replicationFactor);
            topicAssignment.forEach((k, v) -> LOGGER.info("\tPartition {}: {}", k, JavaConversions.seqAsJavaList(v)));

            Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topic);
            LOGGER.debug("\tConfiguration: {}", props);

            StringBuilder kafkaCreateCommand = new StringBuilder();
            kafkaCreateCommand.append(createCommandPath).append(" ");
            kafkaCreateCommand.append("--zookeeper ").append(zkUrl).append(" ");
            kafkaCreateCommand.append("--create").append(" ");
            kafkaCreateCommand.append("--topic ").append(topic).append(" ");
            kafkaCreateCommand.append("--partitions ").append(partitionCount).append(" ");
            kafkaCreateCommand.append("--replication-factor ").append(replicationFactor).append(" ");
            for (String confName : props.stringPropertyNames()) {
                String confValue = props.getProperty(confName);
                kafkaCreateCommand.append("--config ").append(confName).append("=").append(confValue).append(" ");
            }
            kafkaCreateScript.append(kafkaCreateCommand.toString().trim()).append("\n");
        }

        LOGGER.info("\n{}", kafkaCreateScript.toString().trim());
    }
}
