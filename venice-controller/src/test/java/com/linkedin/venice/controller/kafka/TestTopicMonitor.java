package com.linkedin.venice.controller.kafka;

import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
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

    Admin mockAdmin = Mockito.mock(VeniceHelixAdmin.class);
    List<Version> existingVersions = new ArrayList<>();
    doReturn(existingVersions).when(mockAdmin).versionsForStore(anyString(), anyString());
    doReturn(kafka.getAddress()).when(mockAdmin).getKafkaBootstrapServers();

    int pollIntervalMs = 1; /* ms */
    int replicationFactor = 1;
    TopicMonitor mon = new TopicMonitor(mockAdmin, clusterName, replicationFactor, pollIntervalMs);
    mon.start();

    TopicManager topicManager = new TopicManager(kafka.getZkAddress());
    topicManager.createTopic(storeName + "_v1", 4, 1); /* topic, partitions, replication */

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
        () -> kafkaClient.listTopics().containsKey(storeName + "_v1"));
    kafkaClient.close();
    Thread.sleep(100);

    Mockito.verify(mockAdmin, atLeastOnce()).addVersion(anyString(), anyString(), anyInt(), anyInt(), anyInt());

    mon.stop();
    kafka.close();
    topicManager.close();

  }

  @Test
  public void validVersionsGetIdentified(){
    List<Version> versionList = new ArrayList<Version>();

    Assert.assertTrue(TopicMonitor.isValidNewVersion(1, versionList),
        "new version on empty version set must be valid");
    Assert.assertTrue(TopicMonitor.isValidNewVersion(2, versionList),
        "new version on empty version set must be valid");
    Assert.assertTrue(TopicMonitor.isValidNewVersion(20, versionList),
        "new version on empty version set must be valid");

    versionList.add(new Version("myStore", 5));

    Assert.assertFalse(TopicMonitor.isValidNewVersion(1, versionList),
        "smaller than all existing versions must make invalid new version");
    Assert.assertFalse(TopicMonitor.isValidNewVersion(5, versionList), "existing version must make invalid new version");
    Assert.assertTrue(TopicMonitor.isValidNewVersion(6, versionList), "next version must make valid new version");
    Assert.assertFalse(TopicMonitor.isValidNewVersion(0, versionList), "0 must make invalid new version");
    Assert.assertFalse(TopicMonitor.isValidNewVersion(-1, versionList), "-1 must make invalid new version");

    versionList.add(new Version("myStore", 8));
    Assert.assertFalse(TopicMonitor.isValidNewVersion(1, versionList),
        "smaller than all existing versions must make invalid new version");
    Assert.assertFalse(TopicMonitor.isValidNewVersion(5, versionList), "existing version must make invalid new version");
    Assert.assertFalse(TopicMonitor.isValidNewVersion(8, versionList), "existing version must make invalid new version");
    Assert.assertTrue(TopicMonitor.isValidNewVersion(7, versionList), "rollback version must make valid new version");
    Assert.assertTrue(TopicMonitor.isValidNewVersion(9, versionList), "next version must make valid new version");
  }
}
