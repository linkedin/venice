package com.linkedin.venice.kafka;

import com.linkedin.venice.meta.Version;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import kafka.admin.AdminUtils;
import kafka.common.TopicExistsException;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;


public class TopicManager {

  private final String zkConnection;
  private final int sessionTimeoutMs;
  private final int connectionTimeoutMs;

  private static final Logger logger = Logger.getLogger(TopicManager.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  public TopicManager(String zkConnection, int sessionTimeoutMs, int connectionTimeoutMs){
    this.zkConnection = zkConnection;
    this.sessionTimeoutMs = sessionTimeoutMs;
    this.connectionTimeoutMs = connectionTimeoutMs;
  }

  public TopicManager(String zkConnection){
    this.zkConnection = zkConnection;
    this.sessionTimeoutMs = 10*1000;
    this.connectionTimeoutMs = 8*1000;
  }

  //Store creation is relatively rare, so don't hold onto the zkConnection Object
  public void createTopic(String topicName, int numPartitions, int replication){
    logger.info("Creating topic: " + topicName + " partitions: " + numPartitions + " replication: " + replication);
    ZkClient zkClient = null;
    try {
      zkClient = new ZkClient(zkConnection, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
      ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkConnection), false);
      AdminUtils.createTopic(zkUtils, topicName, numPartitions, replication, new Properties());
    } catch (TopicExistsException e) {
      logger.warn("Met error when creating kakfa topic: " + topicName, e);
    } finally {
      zkClient.close();
    }
  }

  public void deleteTopic(String topicName){
    logger.info("Deleting topic: " + topicName);
    ZkClient zkClient = null;
    try {
      zkClient = new ZkClient(zkConnection, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
      ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkConnection), false);
      AdminUtils.deleteTopic(zkUtils, topicName);
    } finally {
      zkClient.close();
    }
  }

  public Set<String> listTopics(){
    String brokers = brokerListFromZk(zkConnection);
    KafkaConsumer consumer = getConsumer(brokers);
    Set<String> topics = consumer.listTopics().keySet();
    consumer.close();
    return topics;
  }

  public void deleteOldTopicsForStore(String storename, int numberOfVersionsToRetain){
    List<Integer> versionNumbers = listTopics().stream()
        .filter(topic -> topic.startsWith(storename)) /* early cheap filter */
        .filter(topic -> Version.topicIsValidStoreVersion(topic))
        .filter(topic -> Version.parseStoreFromKafkaTopicName(topic).equals(storename))
        .map(topic -> Version.parseVersionFromKafkaTopicName(topic))
        .collect(Collectors.toList());
    Collections.sort(versionNumbers); /* ascending */
    Collections.reverse(versionNumbers); /* descending */
    for (int i=0; i<versionNumbers.size(); i++){
      if (i < numberOfVersionsToRetain){
        continue;
      } else {
        String topicToDelete = new Version(storename, versionNumbers.get(i)).kafkaTopicName();
        deleteTopic(topicToDelete);
      }
    }
  }

  public void deleteTopicsForStoreOlderThanVersion(String storename, int oldestVersionToKeep){
    listTopics().stream()
        .filter(topic -> topic.startsWith(storename)) /* early cheap filter */
        .filter(topic -> Version.topicIsValidStoreVersion(topic))
        .filter(topic -> Version.parseStoreFromKafkaTopicName(topic).equals(storename))
        .map(topic -> Version.parseVersionFromKafkaTopicName(topic))
        .filter(version -> version < oldestVersionToKeep)
        .map(version -> new Version(storename, version).kafkaTopicName())
        .forEach(topic -> deleteTopic(topic));
  }


  private static KafkaConsumer<byte[], byte[]> getConsumer(String brokers){
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    return new KafkaConsumer<>(props);
  }

  private static String brokerListFromZk(String zkConnection) {
    ZkClient zkClient = null;
    StringJoiner brokers = new StringJoiner(",");
    try {
      int sessionTimeoutMs = 5000;
      int connectionTimeoutMs = 5000;
      zkClient = new ZkClient(zkConnection, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
      List<String> brokerIds = zkClient.getChildren("/brokers/ids");
      for (String brokerId : brokerIds) {
        String brokerJson = zkClient.readData("/brokers/ids/" + brokerId);

        try {
          Map<String, Object> brokerData = mapper.readValue(brokerJson, Map.class);
          brokers.add(brokerData.get("host") + ":" + brokerData.get("port"));
        } catch (IOException e) {
          System.err.println("Cannot parse broker data: " + brokerJson);
          continue;
        }
      }
    } finally {
      if (zkClient != null) {
        zkClient.close();
      }
    }
    return brokers.toString();
  }
}
