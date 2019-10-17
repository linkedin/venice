package com.linkedin.venice.controller.kafka;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.stats.TopicMonitorStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.testng.annotations.Test;

import static com.linkedin.venice.kafka.TopicManager.*;
import static org.mockito.Mockito.*;


public class TestTopicMonitor {

  private KafkaConsumer getKafkaConsumer(String kafkaAddress) {
    Properties kafkaProps = new Properties();
    kafkaProps.put("bootstrap.servers", kafkaAddress);
    kafkaProps.put("group.id", "controller-topic-monitor;" + Utils.getHostName());
    kafkaProps.put("enable.auto.commit", "false");
    kafkaProps.put("auto.commit.interval.ms", "1000");
    kafkaProps.put("session.timeout.ms", "30000");
    kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      /* Only using consumer to list topics, key and value type are bogus */
    return new KafkaConsumer<String, String>(kafkaProps);
  }

  @Test
  public void topicMonitorStartsAndStops() throws Exception {
    String storeName = "myStore";
    String clusterName = "myCluster";

    KafkaBrokerWrapper kafka = ServiceFactory.getKafkaBroker();

    Store mockStore = mock(Store.class);
    doReturn(1).when(mockStore).getLargestUsedVersionNumber();

    Admin mockAdmin = mock(VeniceHelixAdmin.class);
    doReturn(true).when(mockAdmin).isMasterController(clusterName);
    doReturn(mockStore).when(mockAdmin).getStore(clusterName, storeName);
    doReturn(kafka.getAddress()).when(mockAdmin).getKafkaBootstrapServers(anyBoolean());

    TopicMonitorStats stats = mock(TopicMonitorStats.class);

    int pollIntervalMs = 1; /* ms */
    int replicationFactor = 1;
    doReturn(replicationFactor).when(mockAdmin).getReplicationFactor(clusterName, storeName);
    doReturn(Arrays.asList(clusterName)).when(mockAdmin).getClusterOfStoreInMasterController(storeName);
    TopicMonitor mon = new TopicMonitor(mockAdmin, pollIntervalMs, TestUtils.getVeniceConsumerFactory(kafka.getAddress()), stats);
    mon.start();

    int partitionNumber = 4;
    TopicManager topicManager = new TopicManager(kafka.getZkAddress(), DEFAULT_SESSION_TIMEOUT_MS, DEFAULT_CONNECTION_TIMEOUT_MS,
        DEFAULT_KAFKA_OPERATION_TIMEOUT_MS, 100, 0l, TestUtils.getVeniceConsumerFactory(kafka.getAddress()));
    topicManager.createTopic(storeName + "_v1", partitionNumber, 1); /* topic, partitions, replication */
    topicManager.createTopic(storeName + "_v2", partitionNumber, 1); /* topic, partitions, replication */
    KafkaConsumer<String, String> kafkaClient = getKafkaConsumer(kafka.getAddress());

    /* wait for kafka broker to create the topic */
    TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS,
        () -> kafkaClient.listTopics().containsKey(storeName + "_v2"));
    kafkaClient.close();

    verify(mockAdmin, timeout(100).atLeastOnce()).addVersion(clusterName, storeName, 2, partitionNumber, replicationFactor);
    verify(mockAdmin, never()).addVersion(clusterName, storeName, 1, partitionNumber, replicationFactor);

    mon.stop();
    kafka.close();
    topicManager.close();
  }

  @Test
  public void topicMonitorWithMasterControllerFailover() throws Exception {
    String storeName = "myStore";
    String clusterName = "myCluster";

    KafkaBrokerWrapper kafka = ServiceFactory.getKafkaBroker();

    Store mockStore = mock(Store.class);
    doReturn(1).when(mockStore).getLargestUsedVersionNumber();

    Admin mockAdmin = mock(VeniceHelixAdmin.class);
    doReturn(Collections.emptyList()).when(mockAdmin).getClusterOfStoreInMasterController(storeName);
    doReturn(mockStore).when(mockAdmin).getStore(clusterName, storeName);
    doReturn(kafka.getAddress()).when(mockAdmin).getKafkaBootstrapServers(anyBoolean());

    TopicMonitorStats stats = mock(TopicMonitorStats.class);

    int pollIntervalMs = 1; /* ms */
    int replicationFactor = 1;
    doReturn(replicationFactor).when(mockAdmin).getReplicationFactor(clusterName, storeName);
    TopicMonitor mon = new TopicMonitor(mockAdmin, pollIntervalMs, TestUtils.getVeniceConsumerFactory(kafka.getAddress()), stats);
    mon.start();

    int partitionNumber = 4;
    TopicManager topicManager = new TopicManager(kafka.getZkAddress(), DEFAULT_SESSION_TIMEOUT_MS, DEFAULT_CONNECTION_TIMEOUT_MS,
        DEFAULT_KAFKA_OPERATION_TIMEOUT_MS, 100, 0l, TestUtils.getVeniceConsumerFactory(kafka.getAddress()));
    topicManager.createTopic(storeName + "_v2", partitionNumber, 1); /* topic, partitions, replication */

    KafkaConsumer<String, String> kafkaClient = getKafkaConsumer(kafka.getAddress());
    /* wait for kafka broker to create the topic */
    TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS,
        () -> kafkaClient.listTopics().containsKey(storeName + "_v2"));
    kafkaClient.close();
    // No version creation if not master controller
    verify(mockAdmin, after(1000).never()).addVersion(clusterName, storeName, 2, partitionNumber, replicationFactor);

    // Version creation after master controller fails over
    doReturn(Arrays.asList(clusterName)).when(mockAdmin).getClusterOfStoreInMasterController(storeName);
    doReturn(true).when(mockAdmin).isMasterController(clusterName);
    verify(mockAdmin, timeout(1000).atLeastOnce()).addVersion(clusterName, storeName, 2, partitionNumber, replicationFactor);

    mon.stop();
    kafka.close();
    topicManager.close();
  }

  @Test
  public void topicMonitorWithSingleTopicFailure() throws Exception {
    String storeNameA = "aStore";
    String storeNameB = "bStore";
    String clusterName = "myCluster";
    KafkaBrokerWrapper kafka = ServiceFactory.getKafkaBroker();

    Store mockStore = mock(Store.class);
    doReturn(0).when(mockStore).getLargestUsedVersionNumber();

    Admin mockAdmin = mock(VeniceHelixAdmin.class);
    doReturn(true).when(mockAdmin).isMasterController(clusterName);
    doReturn(mockStore).when(mockAdmin).getStore(clusterName, storeNameA);
    doReturn(mockStore).when(mockAdmin).getStore(clusterName, storeNameB);
    doReturn(kafka.getAddress()).when(mockAdmin).getKafkaBootstrapServers(anyBoolean());

    TopicMonitorStats stats = mock(TopicMonitorStats.class);

    int replicationFactor = 1;
    int partitionNumber = 4;
    // The first store will always fail with a VeniceException on addVersion
    doThrow(new VeniceException()).when(mockAdmin).addVersion(clusterName, storeNameA, 1, partitionNumber, replicationFactor);

    int pollIntervalMs = 1; /* ms */
    doReturn(replicationFactor).when(mockAdmin).getReplicationFactor(clusterName, storeNameA);
    doReturn(Arrays.asList(clusterName)).when(mockAdmin).getClusterOfStoreInMasterController(storeNameA);
    doReturn(Arrays.asList(clusterName)).when(mockAdmin).getClusterOfStoreInMasterController(storeNameB);
    TopicMonitor mon = new TopicMonitor(mockAdmin, pollIntervalMs, TestUtils.getVeniceConsumerFactory(kafka.getAddress()), stats);
    mon.start();

    TopicManager topicManager = new TopicManager(kafka.getZkAddress(), DEFAULT_SESSION_TIMEOUT_MS, DEFAULT_CONNECTION_TIMEOUT_MS,
        DEFAULT_KAFKA_OPERATION_TIMEOUT_MS, 100, 0l, TestUtils.getVeniceConsumerFactory(kafka.getAddress()));
    topicManager.createTopic(storeNameA + "_v1", partitionNumber, 1); /* topic, partitions, replication */
    topicManager.createTopic(storeNameB + "_v1", partitionNumber, 1); /* topic, partitions, replication */
    KafkaConsumer<String, String> kafkaClient = getKafkaConsumer(kafka.getAddress());

    /* wait for kafka broker to create the topic */
    TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS,
        () -> kafkaClient.listTopics().containsKey(storeNameA + "_v1") && kafkaClient.listTopics().containsKey(storeNameB + "_v1"));
    kafkaClient.close();

    // Ensure the method addVersion is called on the first store to throw a VeniceException
    verify(mockAdmin, timeout(100).atLeastOnce()).addVersion(clusterName, storeNameA, 1, partitionNumber, replicationFactor);
    verify(stats, atLeastOnce()).recordClusterSkippedByException();
    // Verify the second store is getting processed despite of the failure from the first store
    verify(mockAdmin, timeout(100).atLeastOnce()).getStore(clusterName, storeNameB);

    mon.stop();
    kafka.close();
    topicManager.close();
  }
}
