package com.linkedin.venice.kafka;

import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.TestUtils;
import com.linkedin.venice.meta.Version;

import java.io.IOException;
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
  public void teardown() throws IOException {
    kafka.close();
    manager.close();
  }

  @Test
  public void testCreateTopic() throws Exception {
    Set<String> topicsInCluster = null;
    String topicName = TestUtils.getUniqueString("testCreateTopic");
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

    // Create a topic
    Set<String> topicsInCluster = null;
    String topicName = TestUtils.getUniqueString("testDeleteTopic");
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

    // Delete that topic
    manager.deleteTopic(topicName);

    // Wait for it to go away (delete is async)
    for (int attempts = 100; attempts>0; attempts--) {
      topicsInCluster = manager.listTopics();
      if (!topicsInCluster.contains(topicName)){
        break;
      }
      Thread.sleep(10);
    }

    // Assert that it is gone
    Assert.assertFalse(topicsInCluster.contains(topicName));
  }

  @Test
  public void testDeleteOldTopics() throws InterruptedException {

    // Create 4 version topics, myStore_v1, myStore_v2, ...
    String storeName = TestUtils.getUniqueString("testDeleteOldTopics");
    int maxVersion = 4;
    for (int i=1;i<=maxVersion;i++){
      String topic = new Version(storeName, i).kafkaTopicName();
      manager.createTopic(topic, 1, 1);
      while (!manager.listTopics().contains(topic)){
        Thread.sleep(10); /* wait for topic to appear */
      }
    }

    // Delete every version topic except for the newest 2 (by version number)
    manager.deleteOldTopicsForStore(storeName, 2);
    Set<String> topicsInCluster = null;
    for (int attempts = 100; attempts>0; attempts--) {
      topicsInCluster = manager.listTopics();
      if (
          !topicsInCluster.contains(new Version(storeName, 1).kafkaTopicName())
          && !topicsInCluster.contains(new Version(storeName, 2).kafkaTopicName())
          ){
        break;
      }
      Thread.sleep(10);
    }
    Assert.assertFalse(topicsInCluster.contains(new Version(storeName, 1).kafkaTopicName()));
    Assert.assertFalse(topicsInCluster.contains(new Version(storeName, 2).kafkaTopicName()));
    Assert.assertTrue(topicsInCluster.contains(new Version(storeName, 3).kafkaTopicName()));
    Assert.assertTrue(topicsInCluster.contains(new Version(storeName, 4).kafkaTopicName()));

    // Do it again, nothing should change (because only those 2 topics are still around)
    manager.deleteOldTopicsForStore(storeName, 2);
    Assert.assertTrue(topicsInCluster.contains(new Version(storeName, 3).kafkaTopicName()));
    Assert.assertTrue(topicsInCluster.contains(new Version(storeName, 4).kafkaTopicName()));

  }

  @Test
  public void testDeleteOldTopicsByOldestToKeep() throws InterruptedException {

    // Create 4 version topics, myStore_v1, myStore_v2, ...
    String storeName = TestUtils.getUniqueString("testDeleteOldTopicsByOldestToKeep");
    int maxVersion = 4;
    for (int i=1;i<=maxVersion;i++){
      String topic = new Version(storeName, i).kafkaTopicName();
      manager.createTopic(topic, 1, 1);
      while (!manager.listTopics().contains(topic)){
        Thread.sleep(10); /* wait for topic to appear */
      }
    }

    // Delete every topic with a version number older than 3
    manager.deleteTopicsForStoreOlderThanVersion(storeName, 3);
    Set<String> topicsInCluster = null;
    for (int attempts = 100; attempts>0; attempts--) {
      topicsInCluster = manager.listTopics();
      if (
          !topicsInCluster.contains(new Version(storeName, 1).kafkaTopicName())
          && !topicsInCluster.contains(new Version(storeName, 2).kafkaTopicName())
          ){
        break;
      }
      Thread.sleep(10);
    }
    Assert.assertFalse(topicsInCluster.contains(new Version(storeName, 1).kafkaTopicName()));
    Assert.assertFalse(topicsInCluster.contains(new Version(storeName, 2).kafkaTopicName()));
    Assert.assertTrue(topicsInCluster.contains(new Version(storeName, 3).kafkaTopicName()));
    Assert.assertTrue(topicsInCluster.contains(new Version(storeName, 4).kafkaTopicName()));

    // Do it again, nothing should change
    manager.deleteTopicsForStoreOlderThanVersion(storeName, 3);
    Assert.assertTrue(topicsInCluster.contains(new Version(storeName, 3).kafkaTopicName()));
    Assert.assertTrue(topicsInCluster.contains(new Version(storeName, 4).kafkaTopicName()));
  }

}