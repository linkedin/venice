package com.linkedin.venice.controller.kafka.offsets;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerConfig;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.consumer.VeniceConsumerFactory;
import com.linkedin.venice.offsets.OffsetRecord;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.zookeeper.data.Stat;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

import static org.mockito.Matchers.eq;

public class TestAdminOffsetManager {
  private final String clusterName = "test_cluster";
  private final String topicName = AdminTopicUtils.getTopicNameFromClusterName(clusterName);
  private final int partitionId = AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID;

  @Test
  public void testGetLastOffset() {
    int offset = 10;
    OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset, new String(new OffsetRecord(offset).toBytes(), StandardCharsets.UTF_8));
    KafkaConsumerWrapper consumer = Mockito.mock(KafkaConsumerWrapper.class);
    Mockito.doReturn(offsetAndMetadata)
        .when(consumer)
        .committed(topicName, partitionId);
    AdminOffsetManager offsetManager = new AdminOffsetManager(consumer);
    OffsetRecord actualOffsetRecord = offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);

    Assert.assertEquals(actualOffsetRecord.getOffset(), offset);
  }

  @Test
  public void testGetLastOffsetWhenNonExist() {
    KafkaConsumerWrapper consumer = Mockito.mock(KafkaConsumerWrapper.class);
    Mockito.doReturn(null)
        .when(consumer)
        .committed(topicName, partitionId);

    AdminOffsetManager offsetManager = new AdminOffsetManager(consumer);
    OffsetRecord actualOffsetRecord = offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);

    Assert.assertEquals(actualOffsetRecord.getOffset(), -1);
  }

  @Test
  public void testRecordOffset() {
    KafkaConsumerWrapper consumer = Mockito.mock(KafkaConsumerWrapper.class);
    long offset = 10;
    OffsetRecord offsetRecord = new OffsetRecord(offset);

    AdminOffsetManager offsetManager = new AdminOffsetManager(consumer);
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
    AdminOffsetManager offsetManager = new AdminOffsetManager(consumer);
    offsetManager.clearOffset(topicName, partitionId);
  }
}
