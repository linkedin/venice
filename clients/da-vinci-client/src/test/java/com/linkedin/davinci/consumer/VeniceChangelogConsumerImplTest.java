package com.linkedin.davinci.consumer;

import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.CONSUMER_METRIC_ENTITIES;
import static com.linkedin.venice.kafka.protocol.enums.ControlMessageType.START_OF_SEGMENT;
import static com.linkedin.venice.stats.ClientType.CHANGE_DATA_CAPTURE_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.FAIL;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.SUCCESS;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.consumer.stats.BasicConsumerStats;
import com.linkedin.davinci.kafka.consumer.TestPubSubTopic;
import com.linkedin.davinci.repository.NativeMetadataRepositoryViewAdapter;
import com.linkedin.davinci.store.record.ByteBufferValueRecord;
import com.linkedin.davinci.utils.ChunkAssembler;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.EndOfPush;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.PubSubUtil;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.PubSubTopicType;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.schema.rmd.RmdConstants;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.serialization.StoreDeserializerCache;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.logging.log4j.Logger;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link VeniceChangelogConsumerImpl}.
 */
public class VeniceChangelogConsumerImplTest {
  private String storeName;
  private RecordSerializer<String> keySerializer;
  private RecordSerializer<String> valueSerializer;
  private Schema rmdSchema;
  private SchemaReader schemaReader;
  private PubSubPosition mockPubSubPosition;
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private final Schema valueSchema = AvroCompatibilityHelper.parse("\"string\"");
  private D2ControllerClient mockD2ControllerClient;
  private PubSubConsumerAdapter mockPubSubConsumer;
  private PubSubTopic oldVersionTopic;
  private ChangelogClientConfig changelogClientConfig;
  NativeMetadataRepositoryViewAdapter mockRepository;
  private VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory;
  private static final long pollTimeoutMs = 1000L;
  private static final int partitionCount = 2;

  @BeforeMethod
  public void setUp() {
    storeName = Utils.getUniqueString();
    mockPubSubPosition = ApacheKafkaOffsetPosition.of(0L);
    schemaReader = mock(SchemaReader.class);
    Schema keySchema = AvroCompatibilityHelper.parse("\"string\"");
    doReturn(keySchema).when(schemaReader).getKeySchema();
    doReturn(valueSchema).when(schemaReader).getValueSchema(1);
    rmdSchema = RmdSchemaGenerator.generateMetadataSchema(valueSchema, 1);

    keySerializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(keySchema);
    valueSerializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(valueSchema);

    mockD2ControllerClient = mock(D2ControllerClient.class);
    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    doReturn(1).when(storeInfo).getCurrentVersion();
    doReturn(2).when(storeInfo).getPartitionCount();
    doReturn(storeInfo).when(storeResponse).getStore();
    doReturn(storeResponse).when(mockD2ControllerClient).getStore(storeName);
    MultiSchemaResponse multiRMDSchemaResponse = mock(MultiSchemaResponse.class);
    MultiSchemaResponse.Schema rmdSchemaFromMultiSchemaResponse = mock(MultiSchemaResponse.Schema.class);
    doReturn(1).when(rmdSchemaFromMultiSchemaResponse).getRmdValueSchemaId();
    doReturn(1).when(rmdSchemaFromMultiSchemaResponse).getId();
    doReturn(rmdSchema.toString()).when(rmdSchemaFromMultiSchemaResponse).getSchemaStr();
    doReturn(new MultiSchemaResponse.Schema[] { rmdSchemaFromMultiSchemaResponse }).when(multiRMDSchemaResponse)
        .getSchemas();
    doReturn(multiRMDSchemaResponse).when(mockD2ControllerClient).getAllReplicationMetadataSchemas(storeName);

    mockRepository = mock(NativeMetadataRepositoryViewAdapter.class);
    Store store = mock(Store.class);
    Version mockVersion = new VersionImpl(storeName, 1, "foo");
    mockVersion.setPartitionCount(partitionCount);
    when(store.getCurrentVersion()).thenReturn(1);
    when(store.getCompressionStrategy()).thenReturn(CompressionStrategy.NO_OP);
    when(store.getPartitionCount()).thenReturn(2);
    when(mockRepository.getStore(anyString())).thenReturn(store);
    when(mockRepository.getValueSchema(storeName, 1)).thenReturn(new SchemaEntry(1, valueSchema));
    when(store.getVersionOrThrow(Mockito.anyInt())).thenReturn(mockVersion);
    when(store.getVersion(Mockito.anyInt())).thenReturn(mockVersion);
    when(store.isEnableReads()).thenReturn(true);

    mockPubSubConsumer = mock(PubSubConsumerAdapter.class);
    oldVersionTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 1));

    changelogClientConfig = getChangelogClientConfig();
    changelogClientConfig.getInnerClientConfig()
        .setMetricsRepository(getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true));

    veniceChangelogConsumerClientFactory = spy(
        new VeniceChangelogConsumerClientFactory(
            changelogClientConfig,
            changelogClientConfig.getInnerClientConfig().getMetricsRepository()));
  }

  @Test
  public void testConfig() {
    ChangelogClientConfig config = new ChangelogClientConfig();
    assertNotNull(config.getConsumerProperties());
    assertTrue(config.getConsumerProperties().isEmpty());
    assertThrows(NullPointerException.class, () -> config.setConsumerProperties(null));
    Properties newProps = new Properties();
    newProps.setProperty("foo", "bar");
    config.setConsumerProperties(newProps);
    assertNotNull(config.getConsumerProperties());
    assertFalse(config.getConsumerProperties().isEmpty());
  }

  @Test
  public void testAfterImageConsumerSeek() throws ExecutionException, InterruptedException {
    MultiSchemaResponse multiRMDSchemaResponse = mock(MultiSchemaResponse.class);
    MultiSchemaResponse.Schema rmdSchemaFromMultiSchemaResponse = mock(MultiSchemaResponse.Schema.class);
    doReturn(rmdSchema.toString()).when(rmdSchemaFromMultiSchemaResponse).getSchemaStr();
    doReturn(new MultiSchemaResponse.Schema[] { rmdSchemaFromMultiSchemaResponse }).when(multiRMDSchemaResponse)
        .getSchemas();
    doReturn(multiRMDSchemaResponse).when(mockD2ControllerClient).getAllReplicationMetadataSchemas(storeName);

    prepareVersionTopicRecordsToBePolled(0L, 5L, mockPubSubConsumer, oldVersionTopic, 0, true);

    PubSubConsumerAdapter mockInternalSeekConsumer = Mockito.mock(PubSubConsumerAdapter.class);

    // Build records with EOP for the internal seek consumer. internalSeekToEndOfPush polls with
    // a 5000ms timeout, so we must use anyLong() instead of the default pollTimeoutMs matcher.
    List<DefaultPubSubMessage> seekRecords = new ArrayList<>();
    for (long i = 0L; i < 10L; i++) {
      seekRecords.add(
          constructConsumerRecord(
              oldVersionTopic,
              0,
              "newValue" + i,
              "key" + i,
              Arrays.asList(i, i),
              mockPubSubPosition));
    }
    seekRecords.add(constructEndOfPushMessage(oldVersionTopic, 0, 11L));
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> seekRecordsMap = new HashMap<>();
    seekRecordsMap.put(new PubSubTopicPartitionImpl(oldVersionTopic, 0), seekRecords);
    doReturn(seekRecordsMap).when(mockInternalSeekConsumer).poll(Mockito.anyLong());

    VeniceAfterImageConsumerImpl<String, Utf8> veniceChangelogConsumer = new VeniceAfterImageConsumerImpl<>(
        changelogClientConfig,
        mockPubSubConsumer,
        Lazy.of(() -> mockInternalSeekConsumer),
        PubSubMessageDeserializer.createDefaultDeserializer(),
        veniceChangelogConsumerClientFactory);
    NativeMetadataRepositoryViewAdapter mockRepository = mock(NativeMetadataRepositoryViewAdapter.class);
    Store store = mock(Store.class);
    Version mockVersion = new VersionImpl(storeName, 1, "foo");
    mockVersion.setPartitionCount(2);
    when(store.getCurrentVersion()).thenReturn(1);
    when(store.getCompressionStrategy()).thenReturn(CompressionStrategy.NO_OP);
    when(store.getPartitionCount()).thenReturn(2);
    when(mockRepository.getStore(anyString())).thenReturn(store);
    when(store.getVersion(Mockito.anyInt())).thenReturn(mockVersion);
    when(store.isEnableReads()).thenReturn(true);
    veniceChangelogConsumer.setStoreRepository(mockRepository);

    Assert.assertEquals(veniceChangelogConsumer.getPartitionCount(), 2);

    when(store.getVersionOrThrow(Mockito.anyInt())).thenReturn(mockVersion);
    veniceChangelogConsumer.subscribe(new HashSet<>(Arrays.asList(0))).get();

    Set<Integer> partitionSet = new HashSet<>();
    partitionSet.add(0);

    // try and do it with an empty set, this should complete immediately
    veniceChangelogConsumer.seekToEndOfPush(Collections.emptySet()).get();
    Mockito.verifyNoInteractions(mockInternalSeekConsumer);

    veniceChangelogConsumer.seekToEndOfPush(partitionSet).get();

    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(oldVersionTopic, 0);

    PubSubPosition p11 = ApacheKafkaOffsetPosition.of(11L);
    Mockito.verify(mockPubSubConsumer).subscribe(eq(pubSubTopicPartition), eq(p11), eq(true));
  }

  @Test
  public void testAdjustCheckpoints() {
    PubSubConsumerAdapter mockConsumer = Mockito.mock(PubSubConsumerAdapter.class);
    Map<Integer, VeniceChangeCoordinate> checkpoints = new HashMap<>();
    Map<Integer, Long> currentVersionLastHeartbeat = new HashMap<>();
    List<PubSubTopicPartition> topicPartitionList = new ArrayList<>();
    doAnswer(invocation -> {
      PubSubTopicPartition partition = invocation.getArgument(0);
      PubSubPosition position1 = invocation.getArgument(1);
      PubSubPosition position2 = invocation.getArgument(2);
      return PubSubUtil.computeOffsetDelta(partition, position1, position2, mockConsumer);
    }).when(mockConsumer).positionDifference(any(), any(), any());

    PubSubTopicPartition partition0 =
        new PubSubTopicPartitionImpl(new TestPubSubTopic("topic1", "store1", PubSubTopicType.VERSION_TOPIC), 0);
    PubSubTopicPartition partition1 =
        new PubSubTopicPartitionImpl(new TestPubSubTopic("topic1", "store1", PubSubTopicType.VERSION_TOPIC), 1);
    PubSubTopicPartition partition2 =
        new PubSubTopicPartitionImpl(new TestPubSubTopic("topic1", "store1", PubSubTopicType.VERSION_TOPIC), 2);
    topicPartitionList.add(partition0);
    topicPartitionList.add(partition1);
    topicPartitionList.add(partition2);

    long offset = 1L;
    PubSubPosition aheadPosition = ApacheKafkaOffsetPosition.of(offset + 1L);
    PubSubPosition behindPosition = ApacheKafkaOffsetPosition.of(offset);

    currentVersionLastHeartbeat.put(0, 1L);
    currentVersionLastHeartbeat.put(1, 2L);

    VeniceChangeCoordinate aheadCoordinate = new VeniceChangeCoordinate("topic1", aheadPosition, 0);
    VeniceChangeCoordinate behindCoordinate = new VeniceChangeCoordinate("topic1", behindPosition, 1);
    VeniceChangeCoordinate otherCoordinate = new VeniceChangeCoordinate("topic1", aheadPosition, 2);

    checkpoints.put(0, aheadCoordinate);
    checkpoints.put(1, behindCoordinate);
    checkpoints.put(2, otherCoordinate);

    // The heartbeat is ahead of the checkpoint — should update
    PubSubPosition newerAheadPosition = ApacheKafkaOffsetPosition.of(offset + 2L);
    Mockito.when(mockConsumer.getPositionByTimestamp(partition0, 1L)).thenReturn(newerAheadPosition);

    // The heartbeat is behind the checkpoint — should not update
    Mockito.when(mockConsumer.getPositionByTimestamp(partition1, 2L)).thenReturn(behindPosition);

    // The heartbeat exists, but EOP already exists — and heartbeat is not newer — should not update
    Mockito.when(mockConsumer.getPositionByTimestamp(partition2, 1L)).thenReturn(aheadPosition);

    VeniceAfterImageConsumerImpl.adjustSeekCheckPointsBasedOnHeartbeats(
        checkpoints,
        currentVersionLastHeartbeat,
        mockConsumer,
        topicPartitionList);

    // Should replace with a new VeniceChangeCoordinate (same data but different object)
    Assert.assertNotSame(checkpoints.get(0), aheadCoordinate, "Expected new coordinate at partition 0");

    // Should remain unchanged
    Assert.assertEquals(checkpoints.get(1), behindCoordinate, "Coordinate at partition 1 should not change");
    Assert.assertEquals(checkpoints.get(2), otherCoordinate, "Coordinate at partition 2 should not change");

    // Test case: checkpoint is removed; fallback to heartbeat
    checkpoints.remove(1);
    VeniceAfterImageConsumerImpl.adjustSeekCheckPointsBasedOnHeartbeats(
        checkpoints,
        currentVersionLastHeartbeat,
        mockConsumer,
        topicPartitionList);
    Assert.assertEquals(
        checkpoints.get(1).getPosition(),
        behindPosition,
        "Partition 1 should have fallback heartbeat checkpoint added");

    // Simulate all heartbeat positions as null
    Mockito.when(mockConsumer.getPositionByTimestamp(partition0, 1L)).thenReturn(null);
    Mockito.when(mockConsumer.getPositionByTimestamp(partition1, 2L)).thenReturn(null);
    Mockito.when(mockConsumer.getPositionByTimestamp(partition2, 1L)).thenReturn(null);

    VeniceChangeCoordinate formerCoordinate0 = checkpoints.get(0);
    VeniceChangeCoordinate formerCoordinate1 = checkpoints.get(1);
    VeniceChangeCoordinate formerCoordinate2 = checkpoints.get(2);

    VeniceAfterImageConsumerImpl.adjustSeekCheckPointsBasedOnHeartbeats(
        checkpoints,
        currentVersionLastHeartbeat,
        mockConsumer,
        topicPartitionList);

    // Nothing should change because no heartbeat positions were available
    Assert.assertEquals(checkpoints.get(0), formerCoordinate0);
    Assert.assertEquals(checkpoints.get(1), formerCoordinate1);
    Assert.assertEquals(checkpoints.get(2), formerCoordinate2);
  }

  @Test
  public void testBootstrapState() {
    VeniceChangelogConsumerImpl veniceChangelogConsumer = mock(VeniceChangelogConsumerImpl.class);
    Map<Integer, Boolean> bootstrapStateMap = new VeniceConcurrentHashMap<>();
    bootstrapStateMap.put(0, false);
    doReturn(bootstrapStateMap).when(veniceChangelogConsumer).getPartitionToBootstrapState();
    doCallRealMethod().when(veniceChangelogConsumer).maybeUpdatePartitionToBootstrapMap(any(), any());
    long currentTimestamp = System.currentTimeMillis();
    PubSubTopicRepository topicRepository = new PubSubTopicRepository();
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(topicRepository.getTopic("foo_v1"), 0);
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageType = START_OF_SEGMENT.getValue();
    KafkaMessageEnvelope kafkaMessageEnvelope = new KafkaMessageEnvelope();
    kafkaMessageEnvelope.producerMetadata = new ProducerMetadata();
    kafkaMessageEnvelope.producerMetadata.messageTimestamp = currentTimestamp - TimeUnit.MINUTES.toMillis(2);
    kafkaMessageEnvelope.payloadUnion = controlMessage;
    DefaultPubSubMessage message = new ImmutablePubSubMessage(
        KafkaKey.HEART_BEAT,
        kafkaMessageEnvelope,
        pubSubTopicPartition,
        mockPubSubPosition,
        0,
        0);
    doReturn(currentTimestamp).when(veniceChangelogConsumer).getSubscribeTime();
    veniceChangelogConsumer.maybeUpdatePartitionToBootstrapMap(message, pubSubTopicPartition);
    assertFalse(bootstrapStateMap.get(0));
    kafkaMessageEnvelope.producerMetadata.messageTimestamp = currentTimestamp - TimeUnit.SECONDS.toMillis(30);
    veniceChangelogConsumer.maybeUpdatePartitionToBootstrapMap(message, pubSubTopicPartition);
    Assert.assertTrue(bootstrapStateMap.get(0));
  }

  @Test
  public void testConsumeAfterImage()
      throws ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException {
    prepareVersionTopicRecordsToBePolled(0L, 5L, mockPubSubConsumer, oldVersionTopic, 0, true);
    VeniceChangelogConsumerImpl<String, Utf8> veniceChangelogConsumer = new VeniceAfterImageConsumerImpl<>(
        changelogClientConfig,
        mockPubSubConsumer,
        PubSubMessageDeserializer.createDefaultDeserializer(),
        veniceChangelogConsumerClientFactory);

    BasicConsumerStats consumerStats = spy(veniceChangelogConsumer.getChangeCaptureStats());
    Field changeCaptureStatsField = VeniceChangelogConsumerImpl.class.getDeclaredField("changeCaptureStats");
    changeCaptureStatsField.setAccessible(true);
    changeCaptureStatsField.set(veniceChangelogConsumer, consumerStats);

    veniceChangelogConsumer.setStoreRepository(mockRepository);

    Assert.assertEquals(veniceChangelogConsumer.getPartitionCount(), 2);
    Set<Integer> subscribedPartitions = Collections.singleton(0);
    veniceChangelogConsumer.subscribe(subscribedPartitions).get();
    verify(mockPubSubConsumer)
        .subscribe(new PubSubTopicPartitionImpl(oldVersionTopic, 0), PubSubSymbolicPosition.EARLIEST);
    when(mockPubSubConsumer.getAssignment()).thenReturn(
        new HashSet<>(
            veniceChangelogConsumer
                .getPartitionListToSubscribe(subscribedPartitions, Collections.emptySet(), oldVersionTopic)));

    assertEquals(veniceChangelogConsumer.getLastHeartbeatPerPartition().size(), 1);
    assertTrue(veniceChangelogConsumer.getLastHeartbeatPerPartition().get(0) <= System.currentTimeMillis());

    List<PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessages =
        (List<PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate>>) veniceChangelogConsumer
            .poll(pollTimeoutMs);

    verify(consumerStats).emitPollCountMetrics(SUCCESS);
    verify(consumerStats).emitRecordsConsumedCountMetrics(pubSubMessages.size());

    for (int i = 0; i < 5; i++) {
      PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate> pubSubMessage = pubSubMessages.get(i);
      Utf8 messageStr = pubSubMessage.getValue().getCurrentValue();
      Assert.assertEquals(messageStr.toString(), "newValue" + i);
    }

    prepareVersionTopicRecordsToBePolled(5L, 15L, mockPubSubConsumer, oldVersionTopic, 0, true);
    pubSubMessages = (List<PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate>>) veniceChangelogConsumer
        .poll(pollTimeoutMs);
    assertFalse(pubSubMessages.isEmpty());
    Assert.assertEquals(pubSubMessages.size(), 10);
    for (int i = 5; i < 15; i++) {
      PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate> pubSubMessage = pubSubMessages.get(i - 5);
      Utf8 pubSubMessageValue = pubSubMessage.getValue().getCurrentValue();
      Assert.assertEquals(pubSubMessageValue.toString(), "newValue" + i);
    }

    veniceChangelogConsumer.close();
    verify(mockPubSubConsumer).batchUnsubscribe(any());
    verify(mockPubSubConsumer).close();
    verify(veniceChangelogConsumerClientFactory).deregisterClient(changelogClientConfig.getConsumerName());
    assertEquals(veniceChangelogConsumer.getLastHeartbeatPerPartition().size(), 0);
  }

  @Test
  public void testConsumeAfterImageWithCompaction()
      throws ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException {
    prepareVersionTopicRecordsToBePolled(0L, 5L, mockPubSubConsumer, oldVersionTopic, 0, true);
    ChangelogClientConfig changelogClientConfig = getChangelogClientConfig().setShouldCompactMessages(true);
    VeniceChangelogConsumerImpl<String, Utf8> veniceChangelogConsumer = new VeniceAfterImageConsumerImpl<>(
        changelogClientConfig,
        mockPubSubConsumer,
        PubSubMessageDeserializer.createDefaultDeserializer(),
        veniceChangelogConsumerClientFactory);

    BasicConsumerStats consumerStats = spy(veniceChangelogConsumer.getChangeCaptureStats());
    Field changeCaptureStatsField = VeniceChangelogConsumerImpl.class.getDeclaredField("changeCaptureStats");
    changeCaptureStatsField.setAccessible(true);
    changeCaptureStatsField.set(veniceChangelogConsumer, consumerStats);

    veniceChangelogConsumer.setStoreRepository(mockRepository);

    Assert.assertEquals(veniceChangelogConsumer.getPartitionCount(), 2);

    veniceChangelogConsumer.subscribe(new HashSet<>(Arrays.asList(0))).get();
    verify(mockPubSubConsumer)
        .subscribe(new PubSubTopicPartitionImpl(oldVersionTopic, 0), PubSubSymbolicPosition.EARLIEST);

    List<PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessages =
        new ArrayList<>(veniceChangelogConsumer.poll(pollTimeoutMs));
    for (int i = 0; i < 5; i++) {
      PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate> pubSubMessage = pubSubMessages.get(i);
      Utf8 messageStr = pubSubMessage.getValue().getCurrentValue();
      Assert.assertEquals(messageStr.toString(), "newValue" + i);
    }

    prepareVersionTopicRecordsToBePolled(5L, 15L, mockPubSubConsumer, oldVersionTopic, 0, true);
    pubSubMessages = new ArrayList<>(veniceChangelogConsumer.poll(pollTimeoutMs));

    verify(consumerStats, times(2)).emitPollCountMetrics(SUCCESS);
    verify(consumerStats).emitRecordsConsumedCountMetrics(pubSubMessages.size());

    assertFalse(pubSubMessages.isEmpty());
    Assert.assertEquals(pubSubMessages.size(), 10);
    for (int i = 5; i < 15; i++) {
      PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate> pubSubMessage = pubSubMessages.get(i - 5);
      Utf8 pubSubMessageValue = pubSubMessage.getValue().getCurrentValue();
      Assert.assertEquals(pubSubMessageValue.toString(), "newValue" + i);
    }

    veniceChangelogConsumer.close();
    verify(mockPubSubConsumer, times(3)).batchUnsubscribe(any());
    verify(mockPubSubConsumer).close();
  }

  @Test
  public void testPollFailure()
      throws ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException {
    VeniceChangelogConsumerImpl<String, Utf8> veniceChangelogConsumer = new VeniceAfterImageConsumerImpl<>(
        changelogClientConfig,
        mockPubSubConsumer,
        PubSubMessageDeserializer.createDefaultDeserializer(),
        veniceChangelogConsumerClientFactory);

    BasicConsumerStats consumerStats = spy(veniceChangelogConsumer.getChangeCaptureStats());
    Field changeCaptureStatsField = VeniceChangelogConsumerImpl.class.getDeclaredField("changeCaptureStats");
    changeCaptureStatsField.setAccessible(true);
    changeCaptureStatsField.set(veniceChangelogConsumer, consumerStats);

    veniceChangelogConsumer.setStoreRepository(mockRepository);

    Assert.assertEquals(veniceChangelogConsumer.getPartitionCount(), 2);
    veniceChangelogConsumer.subscribe(new HashSet<>(Collections.singletonList(0))).get();
    verify(mockPubSubConsumer)
        .subscribe(new PubSubTopicPartitionImpl(oldVersionTopic, 0), PubSubSymbolicPosition.EARLIEST);
    doThrow(new NullPointerException("Simulated NPE")).when(mockPubSubConsumer).poll(anyLong());

    assertThrows(Exception.class, () -> veniceChangelogConsumer.poll(pollTimeoutMs));
    verify(consumerStats).emitPollCountMetrics(VeniceResponseStatusCategory.FAIL);
    // Not a chunked record so this shouldn't be recorded
    verify(consumerStats, never()).emitChunkedRecordCountMetrics(VeniceResponseStatusCategory.FAIL);
    verify(consumerStats, never()).emitRecordsConsumedCountMetrics(anyInt());
  }

  @Test
  public void testVersionSwapDataChangeListener() {
    VeniceAfterImageConsumerImpl mockConsumer = Mockito.mock(VeniceAfterImageConsumerImpl.class);
    when(mockConsumer.subscribed()).thenReturn(true);
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(
        new TestPubSubTopic(storeName + "_v1", storeName, PubSubTopicType.VERSION_TOPIC),
        1);

    Set<PubSubTopicPartition> topicPartitionSet = new HashSet<>();
    topicPartitionSet.add(topicPartition);
    when(mockConsumer.getTopicAssignment()).thenReturn(topicPartitionSet);
    when(mockConsumer.internalSeekToEndOfPush(anySet(), Mockito.any(), anyBoolean()))
        .thenReturn(CompletableFuture.completedFuture(null));

    String storeName = "Leppalúði_store";

    NativeMetadataRepositoryViewAdapter mockRepository = Mockito.mock(NativeMetadataRepositoryViewAdapter.class);

    Version mockVersion = Mockito.mock(Version.class);
    when(mockVersion.kafkaTopicName()).thenReturn(storeName + "_v5");

    Store mockStore = Mockito.mock(Store.class);
    when(mockStore.getCurrentVersion()).thenReturn(5);
    when(mockRepository.getStore(storeName)).thenReturn(mockStore);
    when(mockStore.getVersion(5)).thenReturn(mockVersion);

    BasicConsumerStats mockChangeCaptureStats = mock(BasicConsumerStats.class);

    VersionSwapDataChangeListener changeListener =
        new VersionSwapDataChangeListener(mockConsumer, mockRepository, storeName, "", mockChangeCaptureStats, false);
    changeListener.handleStoreChanged(mockStore);
    verify(mockConsumer).internalSeekToEndOfPush(anySet(), any(), anyBoolean());
    verify(mockChangeCaptureStats).emitVersionSwapCountMetrics(SUCCESS);
  }

  @Test
  public void testVersionSwapDataChangeListenerFailure() {
    VeniceAfterImageConsumerImpl<String, Utf8> veniceChangelogConsumer = spy(
        new VeniceAfterImageConsumerImpl<>(
            changelogClientConfig,
            mockPubSubConsumer,
            PubSubMessageDeserializer.createDefaultDeserializer(),
            veniceChangelogConsumerClientFactory));
    when(veniceChangelogConsumer.subscribed()).thenReturn(true);
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(
        new TestPubSubTopic(storeName + "_v1", storeName, PubSubTopicType.VERSION_TOPIC),
        1);

    Set<PubSubTopicPartition> topicPartitionSet = new HashSet<>();
    topicPartitionSet.add(topicPartition);
    when(veniceChangelogConsumer.getTopicAssignment()).thenReturn(topicPartitionSet);
    when(veniceChangelogConsumer.internalSeekToEndOfPush(anySet(), any(), anyBoolean()))
        .thenThrow(new RuntimeException("seek failure"));

    String storeName = "Leppalúði_store";
    NativeMetadataRepositoryViewAdapter mockRepository = Mockito.mock(NativeMetadataRepositoryViewAdapter.class);

    Version mockVersion = Mockito.mock(Version.class);
    when(mockVersion.kafkaTopicName()).thenReturn(storeName + "_v5");

    Store mockStore = Mockito.mock(Store.class);
    when(mockStore.getCurrentVersion()).thenReturn(5);
    when(mockRepository.getStore(storeName)).thenReturn(mockStore);
    when(mockStore.getVersion(5)).thenReturn(mockVersion);

    BasicConsumerStats mockChangeCaptureStats = mock(BasicConsumerStats.class);
    VersionSwapDataChangeListener changeListener = new VersionSwapDataChangeListener(
        veniceChangelogConsumer,
        mockRepository,
        storeName,
        "",
        mockChangeCaptureStats,
        false);
    changeListener.handleStoreChanged(mockStore);
    verify(veniceChangelogConsumer).handleVersionSwapFailure(any());

    assertThrows(VeniceException.class, () -> veniceChangelogConsumer.poll(pollTimeoutMs));

    verify(veniceChangelogConsumer, times(3)).internalSeekToEndOfPush(anySet(), any(), anyBoolean());
    verify(mockChangeCaptureStats).emitVersionSwapCountMetrics(FAIL);
  }

  @Test
  public void testMetricReportingThread() {
    prepareVersionTopicRecordsToBePolled(0L, 5L, mockPubSubConsumer, oldVersionTopic, 0, true);
    VeniceChangelogConsumerImpl<String, Utf8> veniceChangelogConsumer = new VeniceAfterImageConsumerImpl<>(
        changelogClientConfig,
        mockPubSubConsumer,
        PubSubMessageDeserializer.createDefaultDeserializer(),
        veniceChangelogConsumerClientFactory);
    veniceChangelogConsumer.setStoreRepository(mockRepository);

    Assert.assertEquals(veniceChangelogConsumer.getPartitionCount(), partitionCount);

    VeniceChangelogConsumerImpl.HeartbeatReporterThread reporterThread =
        veniceChangelogConsumer.getHeartbeatReporterThread();

    Map<Integer, Long> lastHeartbeat = new HashMap<>();
    BasicConsumerStats consumerStats = Mockito.mock(BasicConsumerStats.class);
    Set<PubSubTopicPartition> topicPartitionSet = new HashSet<>();
    topicPartitionSet.add(new PubSubTopicPartitionImpl(oldVersionTopic, 1));

    reporterThread.recordStats(lastHeartbeat, consumerStats, topicPartitionSet);
    Mockito.verify(consumerStats).emitCurrentConsumingVersionMetrics(1, 1);
    Mockito.verify(consumerStats, never()).emitHeartBeatDelayMetrics(anyLong());

    lastHeartbeat.put(0, 1L);
    reporterThread.recordStats(lastHeartbeat, consumerStats, topicPartitionSet);
    Mockito.verify(consumerStats).emitHeartBeatDelayMetrics(anyLong());

    reporterThread.start();
    reporterThread.interrupt();
  }

  @Test
  public void testChunkingSuccess() throws NoSuchFieldException, IllegalAccessException {
    VeniceChangelogConsumerImpl veniceChangelogConsumer = mock(VeniceChangelogConsumerImpl.class);

    Field changelogClientConfigField = VeniceChangelogConsumerImpl.class.getDeclaredField("changelogClientConfig");
    changelogClientConfigField.setAccessible(true);
    changelogClientConfigField.set(veniceChangelogConsumer, changelogClientConfig);

    VeniceCompressor compressor = mock(VeniceCompressor.class);
    when(veniceChangelogConsumer.getVersionCompressor(any(PubSubTopic.class))).thenReturn(compressor);

    ChunkAssembler chunkAssembler = mock(ChunkAssembler.class);
    Field chunkAssemblerField = VeniceChangelogConsumerImpl.class.getDeclaredField("chunkAssembler");
    chunkAssemblerField.setAccessible(true);
    chunkAssemblerField.set(veniceChangelogConsumer, chunkAssembler);

    BasicConsumerStats consumerStats = mock(BasicConsumerStats.class);
    Field changeCaptureStatsField = VeniceChangelogConsumerImpl.class.getDeclaredField("changeCaptureStats");
    changeCaptureStatsField.setAccessible(true);
    changeCaptureStatsField.set(veniceChangelogConsumer, consumerStats);

    ReadWriteLock subscriptionLock = new ReentrantReadWriteLock();
    Field subscriptionLockField = VeniceChangelogConsumerImpl.class.getDeclaredField("subscriptionLock");
    subscriptionLockField.setAccessible(true);
    subscriptionLockField.set(veniceChangelogConsumer, subscriptionLock);

    PubSubConsumerAdapter pubSubConsumer = mock(PubSubConsumerAdapter.class);
    Field pubSubConsumerField = VeniceChangelogConsumerImpl.class.getDeclaredField("pubSubConsumer");
    pubSubConsumerField.setAccessible(true);
    pubSubConsumerField.set(veniceChangelogConsumer, pubSubConsumer);

    StoreDeserializerCache storeDeserializerCache = mock(StoreDeserializerCache.class);
    Field storeDeserializerCacheField = VeniceChangelogConsumerImpl.class.getDeclaredField("storeDeserializerCache");
    storeDeserializerCacheField.setAccessible(true);
    storeDeserializerCacheField.set(veniceChangelogConsumer, storeDeserializerCache);
    RecordDeserializer valueDeserializer = mock(RecordDeserializer.class);
    when(storeDeserializerCache.getDeserializer(anyInt(), anyInt())).thenReturn(valueDeserializer);

    RecordDeserializer keyDeserializer = mock(RecordDeserializer.class);
    Field keyDeserializerField = VeniceChangelogConsumerImpl.class.getDeclaredField("keyDeserializer");
    keyDeserializerField.setAccessible(true);
    keyDeserializerField.set(veniceChangelogConsumer, keyDeserializer);

    int partition = 0;
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 2));
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(pubSubTopic, partition);

    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> messagesMap = new HashMap<>();
    List<DefaultPubSubMessage> messages = new ArrayList<>();

    byte[] serializedKey = Integer.toString(1).getBytes();

    DefaultPubSubMessage pubSubMessage1 = mock(DefaultPubSubMessage.class);
    when(pubSubMessage1.getPosition()).thenReturn(mock(PubSubPosition.class));
    when(pubSubMessage1.getKey()).thenReturn(mock(KafkaKey.class));
    KafkaMessageEnvelope value1 = mock(KafkaMessageEnvelope.class);
    Put put1 = new Put();
    put1.schemaId = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();
    value1.payloadUnion = put1;
    when(pubSubMessage1.getValue()).thenReturn(value1);
    messages.add(pubSubMessage1);

    DefaultPubSubMessage pubSubMessage2 = mock(DefaultPubSubMessage.class);
    when(pubSubMessage2.getPosition()).thenReturn(mockPubSubPosition);
    when(pubSubMessage2.getKey()).thenReturn(mock(KafkaKey.class));
    KafkaMessageEnvelope value2 = mock(KafkaMessageEnvelope.class);
    Put put2 = new Put();
    put2.schemaId = AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion();
    value2.payloadUnion = put2;

    KafkaKey mockKey = mock(KafkaKey.class);
    when(mockKey.getKey()).thenReturn(serializedKey);
    when(pubSubMessage2.getValue()).thenReturn(value2);
    messages.add(pubSubMessage2);

    messagesMap.put(topicPartition, messages);
    when(pubSubConsumer.poll(pollTimeoutMs)).thenReturn(messagesMap);

    doCallRealMethod().when(veniceChangelogConsumer).convertPubSubMessageToChangeEvent(pubSubMessage1, topicPartition);
    doCallRealMethod().when(veniceChangelogConsumer).convertPubSubMessageToChangeEvent(pubSubMessage2, topicPartition);

    PubSubPosition p0 = ApacheKafkaOffsetPosition.of(0L);
    when(chunkAssembler.bufferAndAssembleRecord(topicPartition, put2.schemaId, null, put2.putValue, p0, compressor))
        .thenReturn(mock(ByteBufferValueRecord.class));

    doCallRealMethod().when(veniceChangelogConsumer).internalPoll(pollTimeoutMs, false);
    veniceChangelogConsumer.internalPoll(pollTimeoutMs, false);

    verify(consumerStats, times(2)).emitChunkedRecordCountMetrics(SUCCESS);
    verify(consumerStats).emitPollCountMetrics(SUCCESS);
    verify(consumerStats).emitRecordsConsumedCountMetrics(1);
  }

  @Test
  public void testChunkingFailure() throws NoSuchFieldException, IllegalAccessException {
    VeniceChangelogConsumerImpl veniceChangelogConsumer = mock(VeniceChangelogConsumerImpl.class);
    doNothing().when(veniceChangelogConsumer).throwIfReadsDisabled();

    Field changelogClientConfigField = VeniceChangelogConsumerImpl.class.getDeclaredField("changelogClientConfig");
    changelogClientConfigField.setAccessible(true);
    changelogClientConfigField.set(veniceChangelogConsumer, changelogClientConfig);

    VeniceCompressor compressor = mock(VeniceCompressor.class);
    when(veniceChangelogConsumer.getVersionCompressor(any(PubSubTopic.class))).thenReturn(compressor);

    BasicConsumerStats consumerStats = mock(BasicConsumerStats.class);
    Field changeCaptureStatsField = VeniceChangelogConsumerImpl.class.getDeclaredField("changeCaptureStats");
    changeCaptureStatsField.setAccessible(true);
    changeCaptureStatsField.set(veniceChangelogConsumer, consumerStats);

    ReadWriteLock subscriptionLock = new ReentrantReadWriteLock();
    Field subscriptionLockField = VeniceChangelogConsumerImpl.class.getDeclaredField("subscriptionLock");
    subscriptionLockField.setAccessible(true);
    subscriptionLockField.set(veniceChangelogConsumer, subscriptionLock);

    PubSubConsumerAdapter pubSubConsumer = mock(PubSubConsumerAdapter.class);
    Field pubSubConsumerField = VeniceChangelogConsumerImpl.class.getDeclaredField("pubSubConsumer");
    pubSubConsumerField.setAccessible(true);
    pubSubConsumerField.set(veniceChangelogConsumer, pubSubConsumer);

    int partition = 0;
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 2));
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(pubSubTopic, partition);

    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> messagesMap = new HashMap<>();
    List<DefaultPubSubMessage> messages = new ArrayList<>();

    DefaultPubSubMessage pubSubMessage = mock(DefaultPubSubMessage.class);
    when(pubSubMessage.getPosition()).thenReturn(mock(PubSubPosition.class));
    when(pubSubMessage.getKey()).thenReturn(mock(KafkaKey.class));
    KafkaMessageEnvelope value = mock(KafkaMessageEnvelope.class);
    Put put = new Put();
    put.schemaId = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();
    value.payloadUnion = put;
    when(pubSubMessage.getValue()).thenReturn(value);
    messages.add(pubSubMessage);

    messagesMap.put(topicPartition, messages);
    when(pubSubConsumer.poll(pollTimeoutMs)).thenReturn(messagesMap);

    doCallRealMethod().when(veniceChangelogConsumer).convertPubSubMessageToChangeEvent(pubSubMessage, topicPartition);

    doCallRealMethod().when(veniceChangelogConsumer).internalPoll(pollTimeoutMs, false);
    assertThrows(Exception.class, () -> veniceChangelogConsumer.internalPoll(pollTimeoutMs, false));

    verify(consumerStats).emitChunkedRecordCountMetrics(FAIL);
    verify(consumerStats).emitPollCountMetrics(FAIL);
    verify(consumerStats, never()).emitRecordsConsumedCountMetrics(anyInt());
  }

  @Test
  public void testSeekToTimestampWithErrorLogging() throws ExecutionException, InterruptedException, TimeoutException {
    Map<Integer, Long> partitionTimestampMap = new HashMap<>();
    partitionTimestampMap.put(0, 1000L);
    // Verify null response for offsetForTime
    PubSubConsumerAdapter nullResponsePubSubConsumer = mock(PubSubConsumerAdapter.class);
    doReturn(null).when(nullResponsePubSubConsumer).getPositionByTimestamp(any(), anyLong());
    PubSubPosition mockedPubSubPosition = mock(PubSubPosition.class);
    when(nullResponsePubSubConsumer.endPosition(any())).thenReturn(mockedPubSubPosition);
    VeniceChangelogConsumerImpl<String, Utf8> veniceChangelogConsumer = new VeniceAfterImageConsumerImpl<>(
        changelogClientConfig,
        nullResponsePubSubConsumer,
        PubSubMessageDeserializer.createDefaultDeserializer(),
        veniceChangelogConsumerClientFactory);
    veniceChangelogConsumer.setStoreRepository(mockRepository);
    veniceChangelogConsumer.internalSeekToTimestamps(partitionTimestampMap).get(10, TimeUnit.SECONDS);
    verify(nullResponsePubSubConsumer, times(1)).endPosition(any());
    verify(nullResponsePubSubConsumer, times(1)).subscribe(any(), any(PubSubPosition.class), eq(true));
    // Verify failed seek logging
    Logger mockLogger = mock(Logger.class);
    PubSubConsumerAdapter mockErrorPubSubConsumer = mock(PubSubConsumerAdapter.class);
    doThrow(new NullPointerException("mock NPE")).when(mockErrorPubSubConsumer)
        .getPositionByTimestamp(any(), anyLong());
    veniceChangelogConsumer = new VeniceAfterImageConsumerImpl<>(
        changelogClientConfig,
        mockErrorPubSubConsumer,
        PubSubMessageDeserializer.createDefaultDeserializer(),
        veniceChangelogConsumerClientFactory);
    veniceChangelogConsumer.setStoreRepository(mockRepository);
    CompletableFuture<Void> seekFuture =
        veniceChangelogConsumer.internalSeekToTimestamps(partitionTimestampMap, mockLogger);
    try {
      seekFuture.get(10, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof NullPointerException);
    } catch (Exception e) {
      Assert.fail("Unexpected exception");
    }
    ArgumentCaptor<Object> logParams = ArgumentCaptor.forClass(Object.class);
    verify(mockLogger).error(anyString(), logParams.capture(), logParams.capture(), any());
    List<Object> params = logParams.getAllValues();
    // The params should be topic name, PubSubTopicPartition and the timestamp
    Assert.assertEquals(params.size(), 2);
    Assert.assertTrue(params.get(0) instanceof String);
    Assert.assertTrue(params.get(1) instanceof Long);
    Long timestamp = (Long) params.get(1);
    Assert.assertEquals(timestamp.longValue(), 1000L);
  }

  @Test
  public void testConcurrentPolls() throws ExecutionException, InterruptedException {
    VeniceChangelogConsumerImpl<String, Utf8> veniceChangelogConsumer = new VeniceAfterImageConsumerImpl<>(
        changelogClientConfig,
        mockPubSubConsumer,
        PubSubMessageDeserializer.createDefaultDeserializer(),
        veniceChangelogConsumerClientFactory);

    VeniceChangelogConsumerImpl<String, Utf8> spyConsumer = Mockito.spy(veniceChangelogConsumer);
    doNothing().when(spyConsumer).throwIfReadsDisabled();

    /*
     * We make this test deterministic by making the first poll hold the lock longer than the second poll, to ensure
     * the second poll times out before it can acquire the lock. Thus, ensuring poll on the PubSubConsumer only gets
     * invoked once.
     */
    CountDownLatch firstPollStarted = new CountDownLatch(1);
    AtomicBoolean firstPollCompleted = new AtomicBoolean(false);

    when(mockPubSubConsumer.poll(anyLong())).thenAnswer(invocation -> {
      firstPollStarted.countDown();
      // Hold the lock for longer than the second poll's timeout
      Thread.sleep(pollTimeoutMs * 3);
      firstPollCompleted.set(true);
      return Collections.emptyMap();
    });

    ExecutorService executorService = Executors.newFixedThreadPool(2);

    // First poll task - will acquire the lock and hold it
    Callable<Void> firstPollTask = () -> {
      spyConsumer.poll(pollTimeoutMs * 4); // Long timeout to ensure it gets the lock
      return null;
    };

    // Second poll task - will timeout waiting for the lock
    Callable<Void> secondPollTask = () -> {
      // Wait for first poll to start, then try to poll with short timeout
      firstPollStarted.await();
      spyConsumer.poll(pollTimeoutMs / 2); // Short timeout to ensure it times out
      return null;
    };

    Future<Void> pollFuture1 = executorService.submit(firstPollTask);
    Future<Void> pollFuture2 = executorService.submit(secondPollTask);

    pollFuture1.get();
    pollFuture2.get();

    // Verify that only one call was made to the underlying consumer
    verify(mockPubSubConsumer, times(1)).poll(anyLong());
    assertTrue(firstPollCompleted.get(), "First poll should have completed successfully");
  }

  @Test
  public void testPollBeforeSubscribeCompletes() throws ExecutionException, InterruptedException, TimeoutException {
    NativeMetadataRepositoryViewAdapter delayedMockRepository = mock(NativeMetadataRepositoryViewAdapter.class);
    Store store = mock(Store.class);
    Version mockVersion = new VersionImpl(storeName, 1, "foo");
    mockVersion.setPartitionCount(2);
    when(store.getCurrentVersion()).thenReturn(1);
    when(store.getCompressionStrategy()).thenReturn(CompressionStrategy.NO_OP);
    when(store.getPartitionCount()).thenReturn(2);
    when(delayedMockRepository.getStore(anyString())).thenReturn(store);
    when(delayedMockRepository.getValueSchema(storeName, 1)).thenReturn(new SchemaEntry(1, valueSchema));
    when(store.getVersionOrThrow(Mockito.anyInt())).thenReturn(mockVersion);
    when(store.getVersion(Mockito.anyInt())).thenReturn(mockVersion);
    when(store.isEnableReads()).thenReturn(true);

    CountDownLatch subscribeStarted = new CountDownLatch(1);

    doAnswer(invocation -> {
      subscribeStarted.countDown();
      // Hold the lock longer than poll timeout
      Thread.sleep(pollTimeoutMs * 2);
      return null;
    }).when(delayedMockRepository).subscribe(anyString());

    prepareVersionTopicRecordsToBePolled(0L, 5L, mockPubSubConsumer, oldVersionTopic, 0, false);

    VeniceAfterImageConsumerImpl<String, Utf8> veniceChangelogConsumer = spy(
        new VeniceAfterImageConsumerImpl<>(
            changelogClientConfig,
            mockPubSubConsumer,
            PubSubMessageDeserializer.createDefaultDeserializer(),
            veniceChangelogConsumerClientFactory));
    veniceChangelogConsumer.setStoreRepository(delayedMockRepository);

    // Call subscribe without waiting on the CompletableFuture
    CompletableFuture<Void> subscribeFuture = veniceChangelogConsumer.subscribe(Collections.singleton(0));
    assertTrue(subscribeStarted.await(5, TimeUnit.SECONDS), "Subscribe should have started");

    // Attempt to poll while subscribe is holding the lock - should return empty
    Collection<PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate>> firstPollResult =
        veniceChangelogConsumer.poll(pollTimeoutMs / 2);
    assertTrue(firstPollResult.isEmpty(), "Poll should return empty when subscribe holds the lock");

    // Wait for subscribe to complete
    subscribeFuture.get(10, TimeUnit.SECONDS);

    // Now poll should succeed and return data
    Collection<PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate>> secondPollResult =
        veniceChangelogConsumer.poll(pollTimeoutMs);
    assertFalse(secondPollResult.isEmpty(), "Poll should return data after subscribe completes");
    Assert.assertEquals(secondPollResult.size(), 5);

    // Verify that getVersionCompressor was called at least once during the successful poll
    verify(veniceChangelogConsumer, atLeastOnce()).getVersionCompressor(any(PubSubTopic.class));
  }

  @Test
  public void testVersionSwapByControlMessage() throws ExecutionException, InterruptedException, TimeoutException {
    ChangelogClientConfig versionSwapConfig = getChangelogClientConfig().setVersionSwapByControlMessageEnabled(true);
    // Should fail without providing client region name and total region count.
    Assert.assertThrows(
        VeniceException.class,
        () -> new VeniceAfterImageConsumerImpl<>(
            versionSwapConfig,
            mockPubSubConsumer,
            PubSubMessageDeserializer.createDefaultDeserializer(),
            veniceChangelogConsumerClientFactory));

    PubSubTopic newVersionTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 2));
    PubSubTopic oldVersionTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 1));
    String clientRegion = "region1";
    String remoteRegion = "region2";
    versionSwapConfig.setClientRegionName(clientRegion).setTotalRegionCount(2);
    PubSubConsumerAdapter mockInternalSeekConsumer = Mockito.mock(PubSubConsumerAdapter.class);
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> newVersionPubSubMessagesMap = new HashMap<>();
    PubSubTopicPartition newTopicPartition1 = new PubSubTopicPartitionImpl(newVersionTopic, 1);
    newVersionPubSubMessagesMap.put(
        newTopicPartition1,
        Collections.singletonList(
            constructVersionSwapMessage(
                newVersionTopic,
                oldVersionTopic.getName(),
                newVersionTopic.getName(),
                clientRegion,
                clientRegion,
                2L,
                1,
                Collections.emptyList())));
    when(mockInternalSeekConsumer.poll(anyLong())).thenThrow(new VeniceException("Test exception"))
        .thenReturn(newVersionPubSubMessagesMap);
    VeniceAfterImageConsumerImpl<String, Utf8> veniceChangeLogConsumer = new VeniceAfterImageConsumerImpl<>(
        versionSwapConfig,
        mockPubSubConsumer,
        Lazy.of(() -> mockInternalSeekConsumer),
        PubSubMessageDeserializer.createDefaultDeserializer(),
        veniceChangelogConsumerClientFactory);
    NativeMetadataRepositoryViewAdapter mockRepository = mock(NativeMetadataRepositoryViewAdapter.class);
    Store store = mock(Store.class);
    Version mockOldVersion = new VersionImpl(storeName, 1, "foo");
    Version mockNewVersion = new VersionImpl(storeName, 2, "foo");
    mockOldVersion.setPartitionCount(2);
    mockNewVersion.setPartitionCount(2);
    mockOldVersion.setCompressionStrategy(CompressionStrategy.NO_OP);
    mockNewVersion.setCompressionStrategy(CompressionStrategy.NO_OP);
    when(store.getCurrentVersion()).thenReturn(2);
    when(store.getCompressionStrategy()).thenReturn(CompressionStrategy.NO_OP);
    when(store.getPartitionCount()).thenReturn(2);
    when(mockRepository.getStore(storeName)).thenReturn(store);
    when(mockRepository.getValueSchema(storeName, 1)).thenReturn(new SchemaEntry(1, valueSchema));
    when(store.getVersionOrThrow(1)).thenReturn(mockOldVersion);
    when(store.getVersion(1)).thenReturn(mockOldVersion);
    when(store.getVersionOrThrow(2)).thenReturn(mockNewVersion);
    when(store.getVersion(2)).thenReturn(mockNewVersion);
    when(store.isEnableReads()).thenReturn(true);
    veniceChangeLogConsumer.setStoreRepository(mockRepository);

    // partition 0 has an irrelevant version swap and partition 1 has a relevant version swap
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> pubSubMessagesMap = new HashMap<>();
    PubSubTopicPartition topicPartition0 = new PubSubTopicPartitionImpl(oldVersionTopic, 0);
    pubSubMessagesMap.put(
        topicPartition0,
        Collections.singletonList(
            constructVersionSwapMessage(
                oldVersionTopic,
                oldVersionTopic.getName(),
                newVersionTopic.getName(),
                remoteRegion,
                clientRegion,
                1L,
                0,
                Collections.emptyList())));
    PubSubTopicPartition topicPartition1 = new PubSubTopicPartitionImpl(oldVersionTopic, 1);
    pubSubMessagesMap.put(
        topicPartition1,
        Collections.singletonList(
            constructVersionSwapMessage(
                oldVersionTopic,
                oldVersionTopic.getName(),
                newVersionTopic.getName(),
                clientRegion,
                clientRegion,
                2L,
                1,
                Collections.emptyList())));
    doReturn(pubSubMessagesMap).when(mockPubSubConsumer).poll(pollTimeoutMs);
    Set<PubSubTopicPartition> currentAssignment = new HashSet<>();
    currentAssignment.add(topicPartition0);
    currentAssignment.add(topicPartition1);
    doReturn(currentAssignment).when(mockPubSubConsumer).getAssignment();

    veniceChangeLogConsumer.poll(pollTimeoutMs);
    VersionSwapMessageState versionSwapMessageState = veniceChangeLogConsumer.getVersionSwapMessageState();
    Assert.assertNotNull(versionSwapMessageState);
    Assert.assertEquals(versionSwapMessageState.getVersionSwapGenerationId(), 2L);
    Assert.assertEquals(versionSwapMessageState.getOldVersionTopic(), oldVersionTopic.getName());
    Assert.assertEquals(versionSwapMessageState.getNewVersionTopic(), newVersionTopic.getName());
    Assert.assertFalse(versionSwapMessageState.isVersionSwapMessagesReceivedForAllPartitions());
    Assert.assertEquals(versionSwapMessageState.getAssignedPartitions().size(), 2);
    Assert.assertEquals(versionSwapMessageState.getIncompletePartitions().size(), 2);
    Assert.assertNull(versionSwapMessageState.getVersionSwapLowWatermarkPosition(oldVersionTopic.getName(), 0));
    Assert.assertNotNull(versionSwapMessageState.getVersionSwapLowWatermarkPosition(oldVersionTopic.getName(), 1));
    PubSubPosition partition1LowWatermark =
        versionSwapMessageState.getVersionSwapLowWatermarkPosition(oldVersionTopic.getName(), 1);
    prepareVersionTopicRecordsToBePolled(0L, 5L, mockPubSubConsumer, oldVersionTopic, 1, false, true);
    Collection<PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessages =
        veniceChangeLogConsumer.poll(pollTimeoutMs);
    for (PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate> pubSubMessage: pubSubMessages) {
      // VeniceChangeCoordinate should be set to version swap low watermark
      Assert.assertEquals(pubSubMessage.getOffset().getPosition(), partition1LowWatermark);
    }
    // New subscribe should be blocked
    CompletableFuture<Void> subscribeDuringVersionSwapFuture =
        veniceChangeLogConsumer.subscribe(Collections.singleton(0));
    Assert.assertThrows(
        TimeoutException.class,
        () -> subscribeDuringVersionSwapFuture.get(pollTimeoutMs, TimeUnit.MILLISECONDS));
    // We should still be able to unsubscribe
    veniceChangeLogConsumer.unsubscribe(Collections.singleton(0));
    Assert.assertEquals(versionSwapMessageState.getAssignedPartitions().size(), 1);
    Assert.assertEquals(versionSwapMessageState.getIncompletePartitions().size(), 1);

    // Allow version swap to complete
    pubSubMessagesMap = new HashMap<>();
    pubSubMessagesMap.put(
        topicPartition1,
        Collections.singletonList(
            constructVersionSwapMessage(
                oldVersionTopic,
                oldVersionTopic.getName(),
                newVersionTopic.getName(),
                clientRegion,
                remoteRegion,
                2L,
                1,
                Collections.emptyList())));
    doReturn(pubSubMessagesMap).when(mockPubSubConsumer).poll(pollTimeoutMs);
    veniceChangeLogConsumer.poll(pollTimeoutMs);
    Assert.assertEquals(versionSwapMessageState.getIncompletePartitions().size(), 0);
    Assert.assertTrue(versionSwapMessageState.isVersionSwapMessagesReceivedForAllPartitions());

    prepareVersionTopicRecordsToBePolled(5L, 10L, mockPubSubConsumer, newVersionTopic, 1, false, true);
    // First poll should not swap since we configured the internal consumer to fail on first attempt
    Assert.assertTrue(veniceChangeLogConsumer.poll(pollTimeoutMs).isEmpty());
    verify(mockPubSubConsumer, never()).subscribe(any(PubSubTopicPartition.class), any(PubSubPosition.class), eq(true));
    // Next few polls should swap
    TestUtils.waitForNonDeterministicAssertion(
        5,
        TimeUnit.SECONDS,
        () -> Assert.assertFalse(veniceChangeLogConsumer.poll(pollTimeoutMs).isEmpty()));
    ArgumentCaptor<PubSubTopicPartition> pubSubTopicCaptor = ArgumentCaptor.forClass(PubSubTopicPartition.class);
    verify(mockPubSubConsumer, times(1)).subscribe(pubSubTopicCaptor.capture(), any(PubSubPosition.class), eq(true));
    Assert.assertEquals(pubSubTopicCaptor.getValue().getTopicName(), newVersionTopic.getName());
    // The subscribe future during version swap should also complete now
    subscribeDuringVersionSwapFuture.get(pollTimeoutMs, TimeUnit.MILLISECONDS);
  }

  private void prepareVersionTopicRecordsToBePolled(
      long startIdx,
      long endIdx,
      PubSubConsumerAdapter pubSubConsumerAdapter,
      PubSubTopic versionTopic,
      int partition,
      boolean prepareEndOfPush) {
    prepareVersionTopicRecordsToBePolled(
        startIdx,
        endIdx,
        pubSubConsumerAdapter,
        versionTopic,
        partition,
        prepareEndOfPush,
        false);
  }

  private void prepareVersionTopicRecordsToBePolled(
      long startIdx,
      long endIdx,
      PubSubConsumerAdapter pubSubConsumerAdapter,
      PubSubTopic versionTopic,
      int partition,
      boolean prepareEndOfPush,
      boolean indexPosition) {
    List<DefaultPubSubMessage> consumerRecordList = new ArrayList<>();
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> consumerRecordsMap = new HashMap<>();
    for (long i = startIdx; i < endIdx; i++) {
      PubSubPosition pubSubPosition;
      if (indexPosition) {
        pubSubPosition = ApacheKafkaOffsetPosition.of(i);
      } else {
        pubSubPosition = mockPubSubPosition;
      }
      DefaultPubSubMessage pubSubMessage = constructConsumerRecord(
          versionTopic,
          partition,
          "newValue" + i,
          "key" + i,
          Arrays.asList(i, i),
          pubSubPosition);
      consumerRecordList.add(pubSubMessage);
    }

    if (prepareEndOfPush) {
      consumerRecordList.add(constructEndOfPushMessage(versionTopic, partition, 0L));
    }
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(versionTopic, partition);
    consumerRecordsMap.put(pubSubTopicPartition, consumerRecordList);
    doReturn(consumerRecordsMap).when(pubSubConsumerAdapter).poll(pollTimeoutMs);
  }

  private DefaultPubSubMessage constructVersionSwapMessage(
      PubSubTopic versionTopic,
      String oldTopic,
      String newTopic,
      String sourceRegion,
      String destinationRegion,
      long generationId,
      int partition,
      List<ByteBuffer> localHighWatermarkPubSubPositions) {
    KafkaKey kafkaKey = new KafkaKey(MessageType.CONTROL_MESSAGE, new byte[0]);
    VersionSwap versionSwapMessage = new VersionSwap();
    versionSwapMessage.oldServingVersionTopic = oldTopic;
    versionSwapMessage.newServingVersionTopic = newTopic;
    versionSwapMessage.localHighWatermarkPubSubPositions = localHighWatermarkPubSubPositions;
    versionSwapMessage.sourceRegion = sourceRegion;
    versionSwapMessage.destinationRegion = destinationRegion;
    versionSwapMessage.generationId = generationId;

    KafkaMessageEnvelope kafkaMessageEnvelope = new KafkaMessageEnvelope();
    ProducerMetadata producerMetadata = new ProducerMetadata();
    producerMetadata.setMessageTimestamp(1000L);
    kafkaMessageEnvelope.setProducerMetadata(producerMetadata);
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageUnion = versionSwapMessage;
    controlMessage.controlMessageType = ControlMessageType.VERSION_SWAP.getValue();
    kafkaMessageEnvelope.payloadUnion = controlMessage;
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(versionTopic, partition);
    return new ImmutablePubSubMessage(kafkaKey, kafkaMessageEnvelope, pubSubTopicPartition, mockPubSubPosition, 0, 0);
  }

  private DefaultPubSubMessage constructConsumerRecord(
      PubSubTopic changeCaptureVersionTopic,
      int partition,
      String newValue,
      String key,
      List<Long> replicationCheckpointVector,
      PubSubPosition pubSubPosition) {
    final GenericRecord rmdRecord = new GenericData.Record(rmdSchema);
    rmdRecord.put(RmdConstants.TIMESTAMP_FIELD_NAME, 0L);
    rmdRecord.put(RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME, replicationCheckpointVector);
    ByteBuffer bytes =
        ByteBuffer.wrap(FastSerializerDeserializerFactory.getFastAvroGenericSerializer(rmdSchema).serialize(rmdRecord));
    ProducerMetadata producerMetadata = new ProducerMetadata();
    producerMetadata.messageTimestamp = 10000L;
    KafkaMessageEnvelope kafkaMessageEnvelope = new KafkaMessageEnvelope(
        MessageType.PUT.getValue(),
        producerMetadata,
        new Put(ByteBuffer.wrap(valueSerializer.serialize(newValue)), 1, 1, bytes),
        null);
    KafkaKey kafkaKey = new KafkaKey(MessageType.PUT, keySerializer.serialize(key));
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(changeCaptureVersionTopic, partition);
    return new ImmutablePubSubMessage(kafkaKey, kafkaMessageEnvelope, pubSubTopicPartition, pubSubPosition, 0, 0);
  }

  private DefaultPubSubMessage constructEndOfPushMessage(PubSubTopic versionTopic, int partition, Long offset) {
    KafkaKey kafkaKey = new KafkaKey(MessageType.CONTROL_MESSAGE, new byte[0]);
    EndOfPush endOfPush = new EndOfPush();
    KafkaMessageEnvelope kafkaMessageEnvelope = new KafkaMessageEnvelope();
    ProducerMetadata producerMetadata = new ProducerMetadata();
    producerMetadata.setMessageTimestamp(1000L);
    kafkaMessageEnvelope.setProducerMetadata(producerMetadata);
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageUnion = endOfPush;
    controlMessage.controlMessageType = ControlMessageType.END_OF_PUSH.getValue();
    kafkaMessageEnvelope.payloadUnion = controlMessage;
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(versionTopic, partition);
    PubSubPosition mockPubSubPosition = ApacheKafkaOffsetPosition.of(offset);
    return new ImmutablePubSubMessage(kafkaKey, kafkaMessageEnvelope, pubSubTopicPartition, mockPubSubPosition, 0, 0);
  }

  @Test
  public void testSwitchToNewTopic() {
    VeniceAfterImageConsumerImpl<String, Utf8> veniceChangelogConsumer = new VeniceAfterImageConsumerImpl<>(
        changelogClientConfig,
        mockPubSubConsumer,
        PubSubMessageDeserializer.createDefaultDeserializer(),
        veniceChangelogConsumerClientFactory);
    veniceChangelogConsumer.setStoreRepository(mockRepository);

    // Mock the consumer assignment to include the old version topic partition 0
    PubSubTopicPartition subscribedPartition = new PubSubTopicPartitionImpl(oldVersionTopic, 0);
    doReturn(Collections.synchronizedSet(new HashSet<>(Collections.singleton(subscribedPartition))))
        .when(mockPubSubConsumer)
        .getAssignment();

    // switchToNewTopic with the same topic should return false (no-op)
    assertFalse(veniceChangelogConsumer.switchToNewTopic(oldVersionTopic, 0));

    // switchToNewTopic with a different topic on the same partition should return true
    PubSubTopic newVersionTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 2));
    assertTrue(veniceChangelogConsumer.switchToNewTopic(newVersionTopic, 0));
  }

  private ChangelogClientConfig getChangelogClientConfig() {
    ChangelogClientConfig changelogClientConfig =
        new ChangelogClientConfig<>().setD2ControllerClient(mockD2ControllerClient)
            .setSchemaReader(schemaReader)
            .setStoreName(storeName)
            .setConsumerProperties(new Properties())
            .setViewName("");
    changelogClientConfig.getInnerClientConfig()
        .setMetricsRepository(getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true));
    return changelogClientConfig;
  }
}
