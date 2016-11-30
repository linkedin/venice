package com.linkedin.venice.controller.kafka.offsets;

import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.consumer.VeniceConsumerFactory;
import com.linkedin.venice.kafka.protocol.state.ProducerPartitionState;
import com.linkedin.venice.kafka.validation.SegmentStatus;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.TestUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
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

  private ProducerPartitionState getProducerPartitionState(long messageTimestamp) {
    ProducerPartitionState state = new ProducerPartitionState();
    state.segmentNumber = SegmentStatus.IN_PROGRESS.ordinal();
    state.segmentNumber = 1;
    state.messageSequenceNumber = 1;
    state.messageTimestamp = messageTimestamp;
    state.checksumType = CheckSumType.NONE.ordinal();
    state.checksumState = ByteBuffer.wrap(new byte[0]);
    state.aggregates = new HashMap<>();
    state.debugInfo = new HashMap<>();

    return state;
  }

  @Test (expectedExceptions = IllegalArgumentException.class)
  public void testFilterOldStatesWithNegativeKeepNum() {
    OffsetRecord record = new OffsetRecord();
    AdminOffsetManager.filterOldStates(record, -1);
  }

  @Test
  public void testFilterOldStatesWithEmptyProducerStates() {
    OffsetRecord record = new OffsetRecord();
    AdminOffsetManager.filterOldStates(record, 3);
    Assert.assertEquals(record.getProducerPartitionStateMap().size(), 0);
  }

  @Test
  public void testFilterOldStatesWithFewerProducerStates() {
    OffsetRecord record = new OffsetRecord();
    Map<CharSequence, ProducerPartitionState> producerStates = record.getProducerPartitionStateMap();
    producerStates.put("test_guid1", getProducerPartitionState(200));
    producerStates.put("test_guid2", getProducerPartitionState(201));

    AdminOffsetManager.filterOldStates(record, 3);
    producerStates = record.getProducerPartitionStateMap();

    Assert.assertTrue(producerStates.containsKey("test_guid1"));
    Assert.assertTrue(producerStates.containsKey("test_guid2"));
  }

  @Test
  public void testFilterOldStatesWithDuplicateTimestamp() {
    OffsetRecord record = new OffsetRecord();
    Map<CharSequence, ProducerPartitionState> producerStates = record.getProducerPartitionStateMap();

    // Generate multiple ProducerPartitionState
    producerStates.put("test_guid1", getProducerPartitionState(200));
    producerStates.put("test_guid8", getProducerPartitionState(201));
    producerStates.put("test_guid3", getProducerPartitionState(202));
    producerStates.put("test_guid2", getProducerPartitionState(203));
    producerStates.put("test_guid6", getProducerPartitionState(204));
    producerStates.put("test_guid5", getProducerPartitionState(206));
    producerStates.put("test_guid7", getProducerPartitionState(206));
    producerStates.put("test_guid0", getProducerPartitionState(207));
    producerStates.put("test_guid4", getProducerPartitionState(208));

    AdminOffsetManager.filterOldStates(record, 3);
    producerStates = record.getProducerPartitionStateMap();
    Assert.assertTrue(producerStates.containsKey("test_guid4"));
    Assert.assertTrue(producerStates.containsKey("test_guid0"));
    Assert.assertTrue(producerStates.containsKey("test_guid7") || producerStates.containsKey("test_guid5"));
  }

  @Test
  public void testFilterOldStates() {
    OffsetRecord record = new OffsetRecord();
    Map<CharSequence, ProducerPartitionState> producerStates = record.getProducerPartitionStateMap();

    // Generate multiple ProducerPartitionState
    producerStates.put("test_guid1", getProducerPartitionState(200));
    producerStates.put("test_guid8", getProducerPartitionState(201));
    producerStates.put("test_guid3", getProducerPartitionState(202));
    producerStates.put("test_guid2", getProducerPartitionState(203));
    producerStates.put("test_guid6", getProducerPartitionState(204));
    producerStates.put("test_guid5", getProducerPartitionState(205));
    producerStates.put("test_guid7", getProducerPartitionState(206));
    producerStates.put("test_guid0", getProducerPartitionState(207));
    producerStates.put("test_guid4", getProducerPartitionState(208));

    AdminOffsetManager.filterOldStates(record, 3);
    producerStates = record.getProducerPartitionStateMap();
    Assert.assertTrue(producerStates.containsKey("test_guid4"));
    Assert.assertTrue(producerStates.containsKey("test_guid0"));
    Assert.assertTrue(producerStates.containsKey("test_guid7"));
  }
}
