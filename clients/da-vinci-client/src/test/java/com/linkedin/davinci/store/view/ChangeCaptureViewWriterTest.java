package com.linkedin.davinci.store.view;

import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;
import static com.linkedin.venice.writer.VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType;
import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.venice.client.change.capture.protocol.RecordChangeEvent;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.EndOfIncrementalPush;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.views.ChangeCaptureView;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ChangeCaptureViewWriterTest {
  private static final Schema SCHEMA = AvroCompatibilityHelper.parse("\"string\"");
  private static final byte[] KEY = "fishy_name".getBytes();
  private static final ByteBuffer OLD_VALUE = ByteBuffer.wrap("herring".getBytes());
  private static final ByteBuffer NEW_VALUE = ByteBuffer.wrap("silver_darling".getBytes());
  private static final String STORE_NAME = "Clupea-pallasii";
  private static final String PUSH_JOB_ID = "sitka-sound";
  public static final String LTX_1 = "ltx1";
  public static final String LVA_1 = "lva1";
  public static final String LOR_1 = "lor1";

  @Test
  public void testConstructVersionSwapMessage() {
    Map<String, Long> highWaterMarks = new HashMap<>();
    highWaterMarks.put(LOR_1, 111L);
    highWaterMarks.put(LTX_1, 99L);
    highWaterMarks.put(LVA_1, 22222L);
    PartitionConsumptionState mockLeaderPartitionConsumptionState = mock(PartitionConsumptionState.class);
    when(mockLeaderPartitionConsumptionState.getLeaderFollowerState()).thenReturn(LeaderFollowerStateType.LEADER);
    when(mockLeaderPartitionConsumptionState.getLatestProcessedUpstreamRTOffsetMap()).thenReturn(highWaterMarks);
    when(mockLeaderPartitionConsumptionState.getPartition()).thenReturn(1);

    PartitionConsumptionState mockFollowerPartitionConsumptionState = mock(PartitionConsumptionState.class);
    when(mockFollowerPartitionConsumptionState.getLeaderFollowerState()).thenReturn(LeaderFollowerStateType.STANDBY);
    when(mockFollowerPartitionConsumptionState.getLatestProcessedUpstreamRTOffsetMap()).thenReturn(highWaterMarks);
    when(mockFollowerPartitionConsumptionState.getPartition()).thenReturn(1);

    VersionSwap versionSwapMessage = new VersionSwap();
    versionSwapMessage.oldServingVersionTopic = Version.composeKafkaTopic(STORE_NAME, 1);
    versionSwapMessage.newServingVersionTopic = Version.composeKafkaTopic(STORE_NAME, 2);

    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageUnion = versionSwapMessage;

    Store mockStore = mock(Store.class);
    VeniceProperties props = new VeniceProperties();
    Object2IntMap<String> urlMappingMap = new Object2IntOpenHashMap<>();
    // Add ID's to the region's to name the sort order of the RMD
    urlMappingMap.put(LTX_1, 0);
    urlMappingMap.put(LVA_1, 1);
    urlMappingMap.put(LOR_1, 2);
    VeniceWriter mockVeniceWriter = mock(VeniceWriter.class);

    VeniceServerConfig mockVeniceServerConfig = mock(VeniceServerConfig.class);
    when(mockVeniceServerConfig.getKafkaClusterUrlToIdMap()).thenReturn(urlMappingMap);

    VeniceConfigLoader mockVeniceConfigLoader = mock(VeniceConfigLoader.class);
    when(mockVeniceConfigLoader.getCombinedProperties()).thenReturn(props);
    when(mockVeniceConfigLoader.getVeniceServerConfig()).thenReturn(mockVeniceServerConfig);

    // Build the change capture writer and set the mock writer
    ChangeCaptureViewWriter changeCaptureViewWriter =
        new ChangeCaptureViewWriter(mockVeniceConfigLoader, mockStore, SCHEMA, Collections.emptyMap());
    changeCaptureViewWriter.setVeniceWriter(mockVeniceWriter);

    // Verify that we never produce the version swap from a follower replica
    changeCaptureViewWriter.processControlMessage(controlMessage, 1, mockFollowerPartitionConsumptionState, 1);
    verify(mockVeniceWriter, never()).sendControlMessage(any(), anyInt(), anyMap(), any(), any());

    // Verify that we never produce anything if it's not a VersionSwap Message
    ControlMessage ignoredControlMessage = new ControlMessage();
    ignoredControlMessage.controlMessageUnion = new EndOfIncrementalPush();
    changeCaptureViewWriter.processControlMessage(ignoredControlMessage, 1, mockLeaderPartitionConsumptionState, 1);
    verify(mockVeniceWriter, never()).sendControlMessage(any(), anyInt(), anyMap(), any(), any());

    // Verify that we only transmit for the version that we're transiting FROM
    VersionSwap ignoredVersionSwapMessage = new VersionSwap();
    ignoredVersionSwapMessage.oldServingVersionTopic = Version.composeKafkaTopic(STORE_NAME, 2);
    ignoredVersionSwapMessage.newServingVersionTopic = Version.composeKafkaTopic(STORE_NAME, 3);
    ignoredControlMessage.controlMessageUnion = ignoredVersionSwapMessage;
    changeCaptureViewWriter.processControlMessage(ignoredControlMessage, 1, mockLeaderPartitionConsumptionState, 1);
    verify(mockVeniceWriter, never()).sendControlMessage(any(), anyInt(), anyMap(), any(), any());

    changeCaptureViewWriter.processControlMessage(controlMessage, 1, mockLeaderPartitionConsumptionState, 1);
    ArgumentCaptor<ControlMessage> messageArgumentCaptor = ArgumentCaptor.forClass(ControlMessage.class);

    // Verify and capture input
    verify(mockVeniceWriter).sendControlMessage(
        messageArgumentCaptor.capture(),
        eq(1),
        anyMap(),
        isNull(),
        eq(DEFAULT_LEADER_METADATA_WRAPPER));

    ControlMessage sentControlMessage = messageArgumentCaptor.getValue();
    assertEquals(sentControlMessage.controlMessageType, 8);
    Assert.assertTrue(sentControlMessage.controlMessageUnion instanceof VersionSwap);
    VersionSwap sentVersionSwapMessage = (VersionSwap) sentControlMessage.controlMessageUnion;
    assertEquals(sentVersionSwapMessage.oldServingVersionTopic, Version.composeKafkaTopic(STORE_NAME, 1));
    assertEquals(sentVersionSwapMessage.newServingVersionTopic, Version.composeKafkaTopic(STORE_NAME, 2));
    assertEquals(sentVersionSwapMessage.localHighWatermarks.get(0), highWaterMarks.get(LTX_1));
    assertEquals(sentVersionSwapMessage.localHighWatermarks.get(1), highWaterMarks.get(LVA_1));
    assertEquals(sentVersionSwapMessage.localHighWatermarks.get(2), highWaterMarks.get(LOR_1));
  }

  @Test
  public void testBuildWriterOptions() {
    // Set up mocks
    Store mockStore = mock(Store.class);

    Version version = new VersionImpl(STORE_NAME, 1, PUSH_JOB_ID);
    when(mockStore.getVersion(1)).thenReturn(Optional.of(version));
    when(mockStore.getName()).thenReturn(STORE_NAME);

    VeniceProperties props = new VeniceProperties();
    Object2IntMap<String> urlMappingMap = new Object2IntOpenHashMap<>();

    VeniceServerConfig mockVeniceServerConfig = mock(VeniceServerConfig.class);
    when(mockVeniceServerConfig.getKafkaClusterUrlToIdMap()).thenReturn(urlMappingMap);

    VeniceConfigLoader mockVeniceConfigLoader = mock(VeniceConfigLoader.class);
    when(mockVeniceConfigLoader.getCombinedProperties()).thenReturn(props);
    when(mockVeniceConfigLoader.getVeniceServerConfig()).thenReturn(mockVeniceServerConfig);

    ChangeCaptureViewWriter changeCaptureViewWriter =
        new ChangeCaptureViewWriter(mockVeniceConfigLoader, mockStore, SCHEMA, Collections.emptyMap());

    VeniceWriterOptions writerOptions = changeCaptureViewWriter.buildWriterOptions(1);

    assertEquals(writerOptions.getTopicName(), STORE_NAME + "_v1" + ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX);
    assertFalse(writerOptions.isChunkingEnabled());
    assertEquals(
        version.getPartitionerConfig().getPartitionerClass(),
        writerOptions.getPartitioner().getClass().getCanonicalName());
  }

  @Test
  public void testProcessRecord() throws ExecutionException, InterruptedException {
    // Set up mocks
    Store mockStore = mock(Store.class);
    VeniceProperties props = new VeniceProperties();
    Object2IntMap<String> urlMappingMap = new Object2IntOpenHashMap<>();
    PubSubProduceResult produceResultMock = Mockito.mock(PubSubProduceResult.class);
    VeniceWriter mockVeniceWriter = mock(VeniceWriter.class);
    doReturn(produceResultMock).when(mockVeniceWriter).syncPut(any(), any(), anyInt());
    VeniceServerConfig mockVeniceServerConfig = mock(VeniceServerConfig.class);
    when(mockVeniceServerConfig.getKafkaClusterUrlToIdMap()).thenReturn(urlMappingMap);

    VeniceConfigLoader mockVeniceConfigLoader = mock(VeniceConfigLoader.class);
    when(mockVeniceConfigLoader.getCombinedProperties()).thenReturn(props);
    when(mockVeniceConfigLoader.getVeniceServerConfig()).thenReturn(mockVeniceServerConfig);

    ChangeCaptureViewWriter changeCaptureViewWriter =
        new ChangeCaptureViewWriter(mockVeniceConfigLoader, mockStore, SCHEMA, Collections.emptyMap());

    Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(SCHEMA, 1);
    List<Long> vectors = Arrays.asList(1L, 2L, 3L);
    GenericRecord rmdRecordWithValueLevelTimeStamp = new GenericData.Record(rmdSchema);
    rmdRecordWithValueLevelTimeStamp.put(TIMESTAMP_FIELD_NAME, 20L);
    rmdRecordWithValueLevelTimeStamp.put(REPLICATION_CHECKPOINT_VECTOR_FIELD, vectors);
    changeCaptureViewWriter.setVeniceWriter(mockVeniceWriter);

    // Update Case
    changeCaptureViewWriter.processRecord(NEW_VALUE, OLD_VALUE, KEY, 1, 1, 1, rmdRecordWithValueLevelTimeStamp);

    // Insert Case
    changeCaptureViewWriter.processRecord(NEW_VALUE, null, KEY, 1, 1, 1, rmdRecordWithValueLevelTimeStamp);

    // Deletion Case
    changeCaptureViewWriter.processRecord(null, OLD_VALUE, KEY, 1, 1, 1, rmdRecordWithValueLevelTimeStamp);

    // Set up argument captors
    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<RecordChangeEvent> eventCaptor = ArgumentCaptor.forClass(RecordChangeEvent.class);

    // Verify and capture input
    verify(mockVeniceWriter, atLeastOnce()).syncPut(keyCaptor.capture(), eventCaptor.capture(), eq(1));

    List<RecordChangeEvent> changeEvents = eventCaptor.getAllValues();
    List<byte[]> keys = keyCaptor.getAllValues();

    // Verify Update
    assertEquals(keys.get(0), KEY);
    assertEquals(changeEvents.get(0).key.array(), KEY);
    assertEquals(changeEvents.get(0).replicationCheckpointVector, vectors);
    assertEquals(changeEvents.get(0).currentValue.value, NEW_VALUE);
    assertEquals(changeEvents.get(0).previousValue.value, OLD_VALUE);

    // Verify Insert
    assertEquals(keys.get(1), KEY);
    assertEquals(changeEvents.get(1).key.array(), KEY);
    assertEquals(changeEvents.get(1).replicationCheckpointVector, vectors);
    assertEquals(changeEvents.get(1).currentValue.value, NEW_VALUE);
    Assert.assertNull(changeEvents.get(1).previousValue);

    // Verify delete
    assertEquals(keys.get(2), KEY);
    assertEquals(changeEvents.get(2).key.array(), KEY);
    assertEquals(changeEvents.get(2).replicationCheckpointVector, vectors);
    Assert.assertNull(changeEvents.get(2).currentValue);
    assertEquals(changeEvents.get(2).previousValue.value, OLD_VALUE);

    // Test close
    changeCaptureViewWriter.close();
    verify(mockVeniceWriter).close();
  }

}
