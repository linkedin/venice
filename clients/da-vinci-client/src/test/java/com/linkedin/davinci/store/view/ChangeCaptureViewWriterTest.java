package com.linkedin.davinci.store.view;

import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;
import static com.linkedin.venice.writer.VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER;
import static org.mockito.Mockito.mock;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType;
import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.venice.client.change.capture.protocol.RecordChangeEvent;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.EndOfIncrementalPush;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.views.ChangeCaptureView;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
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

  private VeniceWriterFactory veniceWriterFactory;

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    veniceWriterFactory = mock(VeniceWriterFactory.class);
  }

  @Test
  public void testConstructVersionSwapMessage() {
    Map<String, Long> highWaterMarks = new HashMap<>();
    highWaterMarks.put(LOR_1, 111L);
    highWaterMarks.put(LTX_1, 99L);
    highWaterMarks.put(LVA_1, 22222L);
    PartitionConsumptionState mockLeaderPartitionConsumptionState = mock(PartitionConsumptionState.class);
    Mockito.when(mockLeaderPartitionConsumptionState.getLeaderFollowerState())
        .thenReturn(LeaderFollowerStateType.LEADER);
    Mockito.when(mockLeaderPartitionConsumptionState.getLatestProcessedUpstreamRTOffsetMap())
        .thenReturn(highWaterMarks);
    Mockito.when(mockLeaderPartitionConsumptionState.getPartition()).thenReturn(1);

    PartitionConsumptionState mockFollowerPartitionConsumptionState = mock(PartitionConsumptionState.class);
    Mockito.when(mockFollowerPartitionConsumptionState.getLeaderFollowerState())
        .thenReturn(LeaderFollowerStateType.STANDBY);
    Mockito.when(mockFollowerPartitionConsumptionState.getLatestProcessedUpstreamRTOffsetMap())
        .thenReturn(highWaterMarks);
    Mockito.when(mockFollowerPartitionConsumptionState.getPartition()).thenReturn(1);

    VersionSwap versionSwapMessage = new VersionSwap();
    versionSwapMessage.oldServingVersionTopic = Version.composeKafkaTopic(STORE_NAME, 1);
    versionSwapMessage.newServingVersionTopic = Version.composeKafkaTopic(STORE_NAME, 2);

    KafkaKey kafkaKey = mock(KafkaKey.class);
    KafkaMessageEnvelope kafkaMessageEnvelope = mock(KafkaMessageEnvelope.class);
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageUnion = versionSwapMessage;

    Version version = new VersionImpl(STORE_NAME, 1, PUSH_JOB_ID);
    VeniceProperties props = VeniceProperties.empty();
    Object2IntMap<String> urlMappingMap = new Object2IntOpenHashMap<>();
    // Add ID's to the region's to name the sort order of the RMD
    urlMappingMap.put(LTX_1, 0);
    urlMappingMap.put(LVA_1, 1);
    urlMappingMap.put(LOR_1, 2);
    CompletableFuture<PubSubProduceResult> mockFuture = mock(CompletableFuture.class);

    VeniceWriter mockVeniceWriter = mock(VeniceWriter.class);
    Mockito.when(mockVeniceWriter.put(Mockito.any(), Mockito.any(), Mockito.anyInt())).thenReturn(mockFuture);

    VeniceServerConfig mockVeniceServerConfig = mock(VeniceServerConfig.class);
    Mockito.when(mockVeniceServerConfig.getKafkaClusterUrlToIdMap()).thenReturn(urlMappingMap);
    PubSubProducerAdapterFactory mockPubSubProducerAdapterFactory = mock(PubSubProducerAdapterFactory.class);
    PubSubClientsFactory mockPubSubClientsFactory = mock(PubSubClientsFactory.class);
    Mockito.when(mockPubSubClientsFactory.getProducerAdapterFactory()).thenReturn(mockPubSubProducerAdapterFactory);
    Mockito.when(mockVeniceServerConfig.getPubSubClientsFactory()).thenReturn(mockPubSubClientsFactory);

    VeniceConfigLoader mockVeniceConfigLoader = mock(VeniceConfigLoader.class);
    Mockito.when(mockVeniceConfigLoader.getCombinedProperties()).thenReturn(props);
    Mockito.when(mockVeniceConfigLoader.getVeniceServerConfig()).thenReturn(mockVeniceServerConfig);

    // Build the change capture writer and set the mock writer
    ChangeCaptureViewWriter changeCaptureViewWriter = new ChangeCaptureViewWriter(
        mockVeniceConfigLoader,
        version,
        SCHEMA,
        Collections.emptyMap(),
        veniceWriterFactory);
    changeCaptureViewWriter.setVeniceWriter(mockVeniceWriter);

    // Verify that we never produce the version swap from a follower replica
    changeCaptureViewWriter.processControlMessage(
        kafkaKey,
        kafkaMessageEnvelope,
        controlMessage,
        1,
        mockFollowerPartitionConsumptionState);
    Mockito.verify(mockVeniceWriter, Mockito.never())
        .sendControlMessage(Mockito.any(), Mockito.anyInt(), Mockito.anyMap(), Mockito.any(), Mockito.any());

    // Verify that we never produce anything if it's not a VersionSwap Message
    ControlMessage ignoredControlMessage = new ControlMessage();
    ignoredControlMessage.controlMessageUnion = new EndOfIncrementalPush();
    changeCaptureViewWriter.processControlMessage(
        kafkaKey,
        kafkaMessageEnvelope,
        ignoredControlMessage,
        1,
        mockLeaderPartitionConsumptionState);
    Mockito.verify(mockVeniceWriter, Mockito.never())
        .sendControlMessage(Mockito.any(), Mockito.anyInt(), Mockito.anyMap(), Mockito.any(), Mockito.any());

    // Verify that we only transmit for the version that we're transiting FROM
    VersionSwap ignoredVersionSwapMessage = new VersionSwap();
    ignoredVersionSwapMessage.oldServingVersionTopic = Version.composeKafkaTopic(STORE_NAME, 2);
    ignoredVersionSwapMessage.newServingVersionTopic = Version.composeKafkaTopic(STORE_NAME, 3);
    ignoredControlMessage.controlMessageUnion = ignoredVersionSwapMessage;
    changeCaptureViewWriter.processControlMessage(
        kafkaKey,
        kafkaMessageEnvelope,
        ignoredControlMessage,
        1,
        mockLeaderPartitionConsumptionState);
    Mockito.verify(mockVeniceWriter, Mockito.never())
        .sendControlMessage(Mockito.any(), Mockito.anyInt(), Mockito.anyMap(), Mockito.any(), Mockito.any());

    changeCaptureViewWriter
        .processControlMessage(kafkaKey, kafkaMessageEnvelope, controlMessage, 1, mockLeaderPartitionConsumptionState);
    ArgumentCaptor<ControlMessage> messageArgumentCaptor = ArgumentCaptor.forClass(ControlMessage.class);

    // Verify and capture input
    Mockito.verify(mockVeniceWriter)
        .sendControlMessage(
            messageArgumentCaptor.capture(),
            Mockito.eq(1),
            Mockito.anyMap(),
            Mockito.isNull(),
            Mockito.eq(DEFAULT_LEADER_METADATA_WRAPPER));

    ControlMessage sentControlMessage = messageArgumentCaptor.getValue();
    Assert.assertEquals(sentControlMessage.controlMessageType, 8);
    Assert.assertTrue(sentControlMessage.controlMessageUnion instanceof VersionSwap);
    VersionSwap sentVersionSwapMessage = (VersionSwap) sentControlMessage.controlMessageUnion;
    Assert.assertEquals(sentVersionSwapMessage.oldServingVersionTopic, Version.composeKafkaTopic(STORE_NAME, 1));
    Assert.assertEquals(sentVersionSwapMessage.newServingVersionTopic, Version.composeKafkaTopic(STORE_NAME, 2));
    Assert.assertEquals(sentVersionSwapMessage.localHighWatermarks.get(0), highWaterMarks.get(LTX_1));
    Assert.assertEquals(sentVersionSwapMessage.localHighWatermarks.get(1), highWaterMarks.get(LVA_1));
    Assert.assertEquals(sentVersionSwapMessage.localHighWatermarks.get(2), highWaterMarks.get(LOR_1));
  }

  @Test
  public void testBuildWriterOptions() {
    // Set up mocks
    Store mockStore = mock(Store.class);

    Version version = new VersionImpl(STORE_NAME, 1, PUSH_JOB_ID);
    Mockito.when(mockStore.getVersionOrThrow(1)).thenReturn(version);
    Mockito.when(mockStore.getName()).thenReturn(STORE_NAME);

    VeniceProperties props = VeniceProperties.empty();
    Object2IntMap<String> urlMappingMap = new Object2IntOpenHashMap<>();
    CompletableFuture<PubSubProduceResult> mockFuture = mock(CompletableFuture.class);

    VeniceWriter mockVeniceWriter = mock(VeniceWriter.class);
    Mockito.when(mockVeniceWriter.put(Mockito.any(), Mockito.any(), Mockito.anyInt())).thenReturn(mockFuture);

    VeniceServerConfig mockVeniceServerConfig = mock(VeniceServerConfig.class);
    Mockito.when(mockVeniceServerConfig.getKafkaClusterUrlToIdMap()).thenReturn(urlMappingMap);

    VeniceConfigLoader mockVeniceConfigLoader = mock(VeniceConfigLoader.class);
    Mockito.when(mockVeniceConfigLoader.getCombinedProperties()).thenReturn(props);
    PubSubProducerAdapterFactory mockPubSubProducerAdapterFactory = mock(PubSubProducerAdapterFactory.class);
    PubSubClientsFactory mockPubSubClientsFactory = mock(PubSubClientsFactory.class);
    Mockito.when(mockPubSubClientsFactory.getProducerAdapterFactory()).thenReturn(mockPubSubProducerAdapterFactory);
    Mockito.when(mockVeniceServerConfig.getPubSubClientsFactory()).thenReturn(mockPubSubClientsFactory);
    Mockito.when(mockVeniceConfigLoader.getVeniceServerConfig()).thenReturn(mockVeniceServerConfig);

    ChangeCaptureViewWriter changeCaptureViewWriter = new ChangeCaptureViewWriter(
        mockVeniceConfigLoader,
        version,
        SCHEMA,
        Collections.emptyMap(),
        veniceWriterFactory);

    VeniceWriterOptions writerOptions = changeCaptureViewWriter.buildWriterOptions();

    Assert
        .assertEquals(writerOptions.getTopicName(), STORE_NAME + "_v1" + ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX);
    Assert.assertFalse(writerOptions.isChunkingEnabled());
    Assert.assertEquals(
        version.getPartitionerConfig().getPartitionerClass(),
        writerOptions.getPartitioner().getClass().getCanonicalName());
  }

  @Test
  public void testProcessRecord() throws ExecutionException, InterruptedException {
    // Set up mocks
    Version version = new VersionImpl(STORE_NAME, 1, PUSH_JOB_ID);
    VeniceProperties props = VeniceProperties.empty();
    Object2IntMap<String> urlMappingMap = new Object2IntOpenHashMap<>();
    CompletableFuture<PubSubProduceResult> mockFuture = mock(CompletableFuture.class);

    VeniceWriter mockVeniceWriter = mock(VeniceWriter.class);
    Mockito.when(mockVeniceWriter.put(Mockito.any(), Mockito.any(), Mockito.anyInt())).thenReturn(mockFuture);

    VeniceServerConfig mockVeniceServerConfig = mock(VeniceServerConfig.class);
    Mockito.when(mockVeniceServerConfig.getKafkaClusterUrlToIdMap()).thenReturn(urlMappingMap);

    VeniceConfigLoader mockVeniceConfigLoader = mock(VeniceConfigLoader.class);
    PubSubProducerAdapterFactory mockPubSubProducerAdapterFactory = mock(PubSubProducerAdapterFactory.class);
    PubSubClientsFactory mockPubSubClientsFactory = mock(PubSubClientsFactory.class);
    Mockito.when(mockPubSubClientsFactory.getProducerAdapterFactory()).thenReturn(mockPubSubProducerAdapterFactory);
    Mockito.when(mockVeniceServerConfig.getPubSubClientsFactory()).thenReturn(mockPubSubClientsFactory);
    Mockito.when(mockVeniceConfigLoader.getCombinedProperties()).thenReturn(props);
    Mockito.when(mockVeniceConfigLoader.getVeniceServerConfig()).thenReturn(mockVeniceServerConfig);

    ChangeCaptureViewWriter changeCaptureViewWriter = new ChangeCaptureViewWriter(
        mockVeniceConfigLoader,
        version,
        SCHEMA,
        Collections.emptyMap(),
        veniceWriterFactory);

    Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(SCHEMA, 1);
    List<Long> vectors = Arrays.asList(1L, 2L, 3L);
    GenericRecord rmdRecordWithValueLevelTimeStamp = new GenericData.Record(rmdSchema);
    rmdRecordWithValueLevelTimeStamp.put(TIMESTAMP_FIELD_NAME, 20L);
    rmdRecordWithValueLevelTimeStamp.put(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME, vectors);
    changeCaptureViewWriter.setVeniceWriter(mockVeniceWriter);
    Lazy<GenericRecord> dummyValueProvider = Lazy.of(() -> null);
    // Update Case
    changeCaptureViewWriter
        .processRecord(NEW_VALUE, OLD_VALUE, KEY, 1, 1, rmdRecordWithValueLevelTimeStamp, dummyValueProvider)
        .get();

    // Insert Case
    changeCaptureViewWriter
        .processRecord(NEW_VALUE, null, KEY, 1, 1, rmdRecordWithValueLevelTimeStamp, dummyValueProvider)
        .get();

    // Deletion Case
    changeCaptureViewWriter
        .processRecord(null, OLD_VALUE, KEY, 1, 1, rmdRecordWithValueLevelTimeStamp, dummyValueProvider)
        .get();

    // Set up argument captors
    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<RecordChangeEvent> eventCaptor = ArgumentCaptor.forClass(RecordChangeEvent.class);

    // Verify and capture input
    Mockito.verify(mockVeniceWriter, Mockito.atLeastOnce())
        .put(keyCaptor.capture(), eventCaptor.capture(), Mockito.eq(1));

    List<RecordChangeEvent> changeEvents = eventCaptor.getAllValues();
    List<byte[]> keys = keyCaptor.getAllValues();

    // Verify Update
    Assert.assertEquals(keys.get(0), KEY);
    Assert.assertEquals(changeEvents.get(0).key.array(), KEY);
    Assert.assertEquals(changeEvents.get(0).replicationCheckpointVector, vectors);
    Assert.assertEquals(changeEvents.get(0).currentValue.value, NEW_VALUE);
    Assert.assertEquals(changeEvents.get(0).previousValue.value, OLD_VALUE);

    // Verify Insert
    Assert.assertEquals(keys.get(1), KEY);
    Assert.assertEquals(changeEvents.get(1).key.array(), KEY);
    Assert.assertEquals(changeEvents.get(1).replicationCheckpointVector, vectors);
    Assert.assertEquals(changeEvents.get(1).currentValue.value, NEW_VALUE);
    Assert.assertNull(changeEvents.get(1).previousValue);

    // Verify delete
    Assert.assertEquals(keys.get(2), KEY);
    Assert.assertEquals(changeEvents.get(2).key.array(), KEY);
    Assert.assertEquals(changeEvents.get(2).replicationCheckpointVector, vectors);
    Assert.assertNull(changeEvents.get(2).currentValue);
    Assert.assertEquals(changeEvents.get(2).previousValue.value, OLD_VALUE);

    // Test close
    changeCaptureViewWriter.close(true);
    Mockito.verify(mockVeniceWriter).close(true);
  }

}
