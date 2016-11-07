package com.linkedin.venice.controller.kafka.offsets;

import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.consumer.VeniceConsumerFactory;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.TestUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Properties;

public class TestAdminOffsetManager {
  private final String clusterName = "test_cluster";
  private String topicName = AdminTopicUtils.getTopicNameFromClusterName(clusterName);
  private final int partitionId = AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID;
  private VeniceConsumerFactory consumerFactory = Mockito.mock(VeniceConsumerFactory.class);
  private KafkaConsumerWrapper consumer;

  @BeforeMethod
  private void setUp() {
    topicName = AdminTopicUtils.getTopicNameFromClusterName(clusterName);
    consumer = Mockito.mock(KafkaConsumerWrapper.class);
    Mockito.doReturn(consumer).when(consumerFactory).getConsumer(Mockito.any());
  }

  @Test
  public void testGetLastOffset() {
    int offset = 10;
    OffsetRecord offsetRecord = TestUtils.getOffsetRecord(offset);
    OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset, ByteUtils.toHexString(offsetRecord.toBytes()));
    Mockito.doReturn(offsetAndMetadata)
        .when(consumer)
        .committed(topicName, partitionId);
    AdminOffsetManager offsetManager = new AdminOffsetManager(consumerFactory, new Properties());
    OffsetRecord actualOffsetRecord = offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);

    Assert.assertEquals(actualOffsetRecord.getOffset(), offset);
  }

  @Test
  public void testGetLastOffsetWhenNonExist() {
    Mockito.doReturn(null)
        .when(consumer)
        .committed(topicName, partitionId);

    AdminOffsetManager offsetManager = new AdminOffsetManager(consumerFactory, new Properties());
    OffsetRecord actualOffsetRecord = offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);

    Assert.assertEquals(actualOffsetRecord.getOffset(), -1);
  }

  @Test
  public void testRecordOffset() {
    long offset = 10;
    OffsetRecord offsetRecord = TestUtils.getOffsetRecord(offset);

    AdminOffsetManager offsetManager = new AdminOffsetManager(consumerFactory, new Properties());
    ArgumentCaptor<String> topicNameCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> partitionIdCaptor = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<OffsetAndMetadata> offsetAndMetadataCaptor = ArgumentCaptor.forClass(OffsetAndMetadata.class);
    offsetManager.recordOffset(topicName, partitionId, offsetRecord);
    Mockito.verify(consumer).commitSync(topicNameCaptor.capture(), partitionIdCaptor.capture(), offsetAndMetadataCaptor.capture());
    Assert.assertEquals(topicNameCaptor.getValue(), topicName);
    Assert.assertEquals(partitionIdCaptor.getValue().intValue(), partitionId);
    Assert.assertEquals(offsetAndMetadataCaptor.getValue().offset(), offset);
  }

  @Test (expectedExceptions = VeniceException.class)
  public void testClearOffset() {
    KafkaConsumerWrapper consumer = Mockito.mock(KafkaConsumerWrapper.class);
    AdminOffsetManager offsetManager = new AdminOffsetManager(consumerFactory, new Properties());
    offsetManager.clearOffset(topicName, partitionId);
  }
}
