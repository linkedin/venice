package com.linkedin.venice.controller.kafka.offsets;

import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.kafka.protocol.state.ProducerPartitionState;
import com.linkedin.venice.kafka.validation.SegmentStatus;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.utils.TestUtils;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.zookeeper.data.Stat;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.*;

public class TestAdminOffsetManager {
  private final String clusterName = "test_cluster";
  private final String zkOffsetNodePath = AdminOffsetManager.getAdminTopicOffsetNodePathForCluster(clusterName);
  private String topicName = AdminTopicUtils.getTopicNameFromClusterName(clusterName);
  private final int partitionId = AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID;
  private ZkClient zkClient;
  private HelixAdapterSerializer adapterSerializer;

  @BeforeMethod
  private void setUp() {
    zkClient = Mockito.mock(ZkClient.class);
    adapterSerializer = Mockito.mock(HelixAdapterSerializer.class);
  }

  static class OffsetRecordSerializerMatcher extends ArgumentMatcher<OffsetRecordSerializer> {
    public OffsetRecordSerializerMatcher() {
    }
    @Override
    public boolean matches(Object argument) {
      return argument instanceof OffsetRecordSerializer;
    }
  }

  static OffsetRecordSerializer offsetRecordSerializerTypeCheck() {
    return argThat(new OffsetRecordSerializerMatcher());
  }

  @Test
  public void testGetLastOffset() {
    int offset = 10;
    OffsetRecord offsetRecord = TestUtils.getOffsetRecord(offset);
    doReturn(offsetRecord)
        .when(zkClient)
        .readData(zkOffsetNodePath, null);

    AdminOffsetManager offsetManager = new AdminOffsetManager(zkClient, adapterSerializer);
    verify(adapterSerializer).registerSerializer(
        eq(AdminOffsetManager.getAdminTopicOffsetNodePathPattern()),
        offsetRecordSerializerTypeCheck());
    OffsetRecord actualOffsetRecord = offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);

    Assert.assertEquals(actualOffsetRecord.getOffset(), offset);
  }

  @Test
  public void testGetLastOffsetWhenNonExist() {
    doReturn(null)
        .when(zkClient)
        .readData(zkOffsetNodePath, null);

    AdminOffsetManager offsetManager = new AdminOffsetManager(zkClient, adapterSerializer);
    OffsetRecord actualOffsetRecord = offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);

    Assert.assertEquals(actualOffsetRecord.getOffset(), -1);
  }

  @Test
  public void testRecordOffset() throws InterruptedException {
    long offset = 10;
    OffsetRecord offsetRecord = TestUtils.getOffsetRecord(offset);

    doReturn(new Stat()).when(zkClient)
        .writeDataGetStat(any(), any(), anyInt());

    AdminOffsetManager offsetManager = new AdminOffsetManager(zkClient, adapterSerializer);
    offsetManager.recordOffset(topicName, partitionId, offsetRecord);

    verify(zkClient).writeDataGetStat(
        eq(zkOffsetNodePath),
        eq(offsetRecord),
        anyInt());
  }

  @Test (expectedExceptions = VeniceException.class)
  public void testClearOffset() {
    AdminOffsetManager offsetManager = new AdminOffsetManager(zkClient, adapterSerializer);
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
