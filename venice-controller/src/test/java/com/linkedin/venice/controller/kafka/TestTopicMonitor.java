package com.linkedin.venice.controller.kafka;

import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;


/**
 * Created by mwise on 5/6/16.
 */
public class TestTopicMonitor {
  @Test
  public void topicMonitorStartsAndStops() throws Exception {
    String storeName = "myStore";
    String clusterName = "myCluster";

    KafkaBrokerWrapper kafka = new ServiceFactory().getKafkaBroker();

    Store mockStore = Mockito.mock(Store.class);
    doReturn(1).when(mockStore).getLargestUsedVersionNumber();

    Admin mockAdmin = Mockito.mock(VeniceHelixAdmin.class);
    doReturn(mockStore).when(mockAdmin).getStore(clusterName, storeName);

    doReturn(kafka.getAddress()).when(mockAdmin).getKafkaBootstrapServers();

    int pollIntervalMs = 1; /* ms */
    int replicationFactor = 1;
    TopicMonitor mon = new TopicMonitor(mockAdmin, clusterName, replicationFactor, pollIntervalMs);
    mon.start();

    int partitionNumber = 4;
    TopicManager topicManager = new TopicManager(kafka.getZkAddress());
    topicManager.createTopic(storeName + "_v1", partitionNumber, 1); /* topic, partitions, replication */
    topicManager.createTopic(storeName + "_v2", partitionNumber, 1); /* topic, partitions, replication */

    Properties kafkaProps = new Properties();
    kafkaProps.put("bootstrap.servers", kafka.getAddress());
    kafkaProps.put("group.id", "controller-topic-monitor;" + Utils.getHostName());
    kafkaProps.put("enable.auto.commit", "false");
    kafkaProps.put("auto.commit.interval.ms", "1000");
    kafkaProps.put("session.timeout.ms", "30000");
    kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      /* Only using consumer to list topics, key and value type are bogus */
    KafkaConsumer<String, String> kafkaClient = new KafkaConsumer<String, String>(kafkaProps);

    /* wait for kafka broker to create the topic */
    TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS,
        () -> kafkaClient.listTopics().containsKey(storeName + "_v2"));
    kafkaClient.close();
    Thread.sleep(100);

    Mockito.verify(mockAdmin, atLeastOnce()).addVersion(clusterName, storeName, 2, partitionNumber, replicationFactor);
    Mockito.verify(mockAdmin, Mockito.never()).addVersion(clusterName, storeName, 1, partitionNumber, replicationFactor);

    mon.stop();
    kafka.close();
    topicManager.close();
  }
}
