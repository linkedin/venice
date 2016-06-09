package com.linkedin.venice.kafka;

import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.meta.Version;
import java.util.Set;
import junit.framework.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Created by mwise on 6/9/16.
 */
public class TopicManagerTest {

  private KafkaBrokerWrapper kafka;
  private TopicManager manager;

  @BeforeClass
  public void setup() {
    kafka = ServiceFactory.getKafkaBroker();
    manager = new TopicManager(kafka.getZkAddress());
  }

  @AfterClass
  public void teardown(){
    kafka.close();
  }

  @Test
  public void testCreateTopic() throws Exception {
    Set<String> topicsInCluster = null;
    String topicName = "mytopic";
    int partitions = 1;
    int replicas = 1;
    manager.createTopic(topicName, partitions, replicas);
    for (int attempts = 100; attempts>0; attempts--) {
      topicsInCluster = manager.listTopics();
      if (topicsInCluster.contains(topicName)){
        break;
      }
      Thread.sleep(10);
    }
    Assert.assertTrue(topicsInCluster.contains(topicName));
    manager.createTopic(topicName, partitions, replicas); /* should be noop */
    Assert.assertTrue(topicsInCluster.contains(topicName));
  }

  @Test
  public void testDeleteTopic() throws InterruptedException {
    Set<String> topicsInCluster = null;
    String topicName = "mytopic2";
    int partitions = 1;
    int replicas = 1;
    manager.createTopic(topicName, partitions, replicas);
    for (int attempts = 100; attempts>0; attempts--) {
      topicsInCluster = manager.listTopics();
      if (topicsInCluster.contains(topicName)){
        break;
      }
      Thread.sleep(10);
    }
    Assert.assertTrue(topicsInCluster.contains(topicName));

    manager.deleteTopic(topicName);
    for (int attempts = 100; attempts>0; attempts--) {
      topicsInCluster = manager.listTopics();
      if (!topicsInCluster.contains(topicName)){
        break;
      }
      Thread.sleep(10);
    }
    Assert.assertFalse(topicsInCluster.contains(topicName));
  }

  @Test
  public void testDeleteOldTopics() throws InterruptedException {
    String storeName = "myStore";
    int maxVersion = 10;
    for (int i=1;i<=maxVersion;i++){
      String topic = new Version(storeName, i).kafkaTopicName();
      manager.createTopic(topic, 1, 1);
    }
    while (!manager.listTopics().contains(new Version(storeName, maxVersion).kafkaTopicName())){
      Thread.sleep(10); /* wait for topics to appear */
    }
    manager.deleteOldTopicsForStore(storeName, 2);
    Set<String> topicsInCluster = null;
    for (int attempts = 100; attempts>0; attempts--) {
      topicsInCluster = manager.listTopics();
      if (!topicsInCluster.contains(new Version(storeName, maxVersion-2).kafkaTopicName())){
        break;
      }
      Thread.sleep(10);
    }
    Assert.assertFalse(topicsInCluster.contains(new Version(storeName, 1).kafkaTopicName()));
    Assert.assertFalse(topicsInCluster.contains(new Version(storeName, 2).kafkaTopicName()));
    Assert.assertFalse(topicsInCluster.contains(new Version(storeName, 3).kafkaTopicName()));
    Assert.assertFalse(topicsInCluster.contains(new Version(storeName, 4).kafkaTopicName()));
    Assert.assertFalse(topicsInCluster.contains(new Version(storeName, 5).kafkaTopicName()));
    Assert.assertFalse(topicsInCluster.contains(new Version(storeName, 6).kafkaTopicName()));
    Assert.assertFalse(topicsInCluster.contains(new Version(storeName, 7).kafkaTopicName()));
    Assert.assertFalse(topicsInCluster.contains(new Version(storeName, 8).kafkaTopicName()));
    Assert.assertTrue(topicsInCluster.contains(new Version(storeName, 9).kafkaTopicName()));
    Assert.assertTrue(topicsInCluster.contains(new Version(storeName, 10).kafkaTopicName()));

    manager.deleteOldTopicsForStore(storeName, 2); /* Should be no-op */
    Assert.assertTrue(topicsInCluster.contains(new Version(storeName, 9).kafkaTopicName()));
    Assert.assertTrue(topicsInCluster.contains(new Version(storeName, 10).kafkaTopicName()));

  }

  @Test
  public void testDeleteOldTopicsByOldestToKeep() throws InterruptedException {
    String storeName = "myStore";
    int maxVersion = 10;
    for (int i=1;i<=maxVersion;i++){
      String topic = new Version(storeName, i).kafkaTopicName();
      manager.createTopic(topic, 1, 1);
    }
    while (!manager.listTopics().contains(new Version(storeName, maxVersion).kafkaTopicName())){
      Thread.sleep(10); /* wait for topics to appear */
    }
    manager.deleteTopicsForStoreOlderThanVersion(storeName, 9);
    Set<String> topicsInCluster = null;
    for (int attempts = 100; attempts>0; attempts--) {
      topicsInCluster = manager.listTopics();
      if (!topicsInCluster.contains(new Version(storeName, maxVersion-2).kafkaTopicName())){
        break;
      }
      Thread.sleep(10);
    }
    Assert.assertFalse(topicsInCluster.contains(new Version(storeName, 1).kafkaTopicName()));
    Assert.assertFalse(topicsInCluster.contains(new Version(storeName, 2).kafkaTopicName()));
    Assert.assertFalse(topicsInCluster.contains(new Version(storeName, 3).kafkaTopicName()));
    Assert.assertFalse(topicsInCluster.contains(new Version(storeName, 4).kafkaTopicName()));
    Assert.assertFalse(topicsInCluster.contains(new Version(storeName, 5).kafkaTopicName()));
    Assert.assertFalse(topicsInCluster.contains(new Version(storeName, 6).kafkaTopicName()));
    Assert.assertFalse(topicsInCluster.contains(new Version(storeName, 7).kafkaTopicName()));
    Assert.assertFalse(topicsInCluster.contains(new Version(storeName, 8).kafkaTopicName()));
    Assert.assertTrue(topicsInCluster.contains(new Version(storeName, 9).kafkaTopicName()));
    Assert.assertTrue(topicsInCluster.contains(new Version(storeName, 10).kafkaTopicName()));

    manager.deleteTopicsForStoreOlderThanVersion(storeName, 9);; /* Should be no-op */
    Assert.assertTrue(topicsInCluster.contains(new Version(storeName, 9).kafkaTopicName()));
    Assert.assertTrue(topicsInCluster.contains(new Version(storeName, 10).kafkaTopicName()));

  }

}