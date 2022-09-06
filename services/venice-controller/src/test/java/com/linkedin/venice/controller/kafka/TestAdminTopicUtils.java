package com.linkedin.venice.controller.kafka;

import com.linkedin.venice.exceptions.VeniceException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestAdminTopicUtils {
  @Test
  public void testGetTopicNameFromClusterName() {
    String clusterName = "test_cluster";

    Assert.assertEquals(AdminTopicUtils.getTopicNameFromClusterName(clusterName), "venice_admin_test_cluster");
  }

  @Test
  public void testGetClusterNameFromTopicName() {
    String topicName = "venice_admin_test_cluster";

    Assert.assertEquals(AdminTopicUtils.getClusterNameFromTopicName(topicName), "test_cluster");
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testGetClusterNameFromTopicNameWithInvalidAdminTopicName() {
    String topicName = "invalid_topic_name";

    AdminTopicUtils.getClusterNameFromTopicName(topicName);
  }

  @Test
  public void testIsAdminTopic() {
    String validAdminTopicName = "venice_admin_test_cluster";
    Assert.assertTrue(AdminTopicUtils.isAdminTopic(validAdminTopicName));

    String invalidAdminTopicName = "invalid_topic_name";
    Assert.assertFalse(AdminTopicUtils.isAdminTopic(invalidAdminTopicName));
  }

  @Test
  public void testIsKafkaInternalTopic() {
    String kafkaInternalTopic = "__consumer_offsets";
    Assert.assertTrue(AdminTopicUtils.isKafkaInternalTopic(kafkaInternalTopic));

    String monitorTopic = "kafka-monitor-topic";
    Assert.assertTrue(AdminTopicUtils.isKafkaInternalTopic(monitorTopic));

    String veniceTopic = "test_store_v1";
    Assert.assertFalse(AdminTopicUtils.isKafkaInternalTopic(veniceTopic));
  }
}
