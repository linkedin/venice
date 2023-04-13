package com.linkedin.venice.kafka;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.kafka.admin.KafkaAdminWrapper;
import com.linkedin.venice.utils.Utils;
import java.util.Properties;
import org.apache.kafka.common.config.TopicConfig;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestTopicManager {
  @Test
  public void testUpdateTopicMinISRCommand() {
    String topicName = Utils.getUniqueString("testTopic");
    Properties properties = new Properties();
    properties.setProperty(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1");
    KafkaAdminWrapper mockKafkaReadOnlyAdmin = mock(KafkaAdminWrapper.class);
    doReturn(properties).when(mockKafkaReadOnlyAdmin).getTopicConfig(topicName);

    int newMinISR = 2;
    KafkaAdminWrapper mockKafkaWriteOnlyAdmin = mock(KafkaAdminWrapper.class);
    ArgumentCaptor<Properties> propertiesArgumentCaptor = ArgumentCaptor.forClass(Properties.class);
    TopicManager topicManager = new TopicManager(mockKafkaWriteOnlyAdmin, mockKafkaReadOnlyAdmin);
    topicManager.updateTopicMinInSyncReplica(topicName, newMinISR);
    verify(mockKafkaWriteOnlyAdmin, atLeastOnce()).setTopicConfig(eq(topicName), propertiesArgumentCaptor.capture());
    Properties capturedProperties = propertiesArgumentCaptor.getValue();
    Assert.assertEquals(
        capturedProperties.getProperty(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG),
        Integer.toString(newMinISR));
  }
}
