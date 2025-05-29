package com.linkedin.davinci.consumer;

import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.CONSUMER_METRIC_ENTITIES;
import static com.linkedin.venice.kafka.protocol.enums.ControlMessageType.START_OF_SEGMENT;
import static com.linkedin.venice.stats.ClientType.CHANGE_DATA_CAPTURE_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.consumer.stats.BasicConsumerStats;
import com.linkedin.davinci.kafka.consumer.TestPubSubTopic;
import com.linkedin.davinci.repository.NativeMetadataRepositoryViewAdapter;
import com.linkedin.davinci.utils.ChunkAssembler;
import com.linkedin.venice.client.change.capture.protocol.RecordChangeEvent;
import com.linkedin.venice.client.change.capture.protocol.ValueBytes;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.EndOfPush;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.StartOfPush;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.PubSubTopicType;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.schema.rmd.RmdConstants;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.views.ChangeCaptureView;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceChangelogConsumerImplTest {
  private String storeName;
  private RecordSerializer<String> keySerializer;
  private RecordSerializer<String> valueSerializer;
  private Schema rmdSchema;
  private SchemaReader schemaReader;
  private PubSubPosition mockPubSubPosition;
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private final Schema valueSchema = AvroCompatibilityHelper.parse("\"string\"");

  @BeforeMethod
  public void setUp() {
    storeName = Utils.getUniqueString();
    mockPubSubPosition = mock(PubSubPosition.class);
    schemaReader = mock(SchemaReader.class);
    Schema keySchema = AvroCompatibilityHelper.parse("\"string\"");
    doReturn(keySchema).when(schemaReader).getKeySchema();
    doReturn(valueSchema).when(schemaReader).getValueSchema(1);
    rmdSchema = RmdSchemaGenerator.generateMetadataSchema(valueSchema, 1);

    keySerializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(keySchema);
    valueSerializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(valueSchema);
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
  public void testConsumeBeforeAndAfterImage() throws ExecutionException, InterruptedException {
    D2ControllerClient d2ControllerClient = mock(D2ControllerClient.class);
    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    doReturn(1).when(storeInfo).getCurrentVersion();
    doReturn(2).when(storeInfo).getPartitionCount();
    doReturn(storeInfo).when(storeResponse).getStore();
    doReturn(storeResponse).when(d2ControllerClient).getStore(storeName);

    PubSubConsumerAdapter mockPubSubConsumer = mock(PubSubConsumerAdapter.class);
    doReturn(new HashSet<>()).when(mockPubSubConsumer).getAssignment();
    PubSubTopic newVersionTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 2));
    PubSubTopic oldVersionTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 1));
    PubSubTopic newChangeCaptureTopic =
        pubSubTopicRepository.getTopic(newVersionTopic.getName() + ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX);
    PubSubTopic oldChangeCaptureTopic =
        pubSubTopicRepository.getTopic(oldVersionTopic.getName() + ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX);

    int partition = 0;
    prepareChangeCaptureRecordsToBePolled(
        0L,
        5L,
        mockPubSubConsumer,
        oldChangeCaptureTopic,
        partition,
        oldVersionTopic,
        newVersionTopic,
        false,
        false);
    ChangelogClientConfig changelogClientConfig =
        getChangelogClientConfig(d2ControllerClient).setViewName("changeCaptureView").setIsBeforeImageView(true);
    VeniceChangelogConsumerImpl<String, Utf8> veniceChangelogConsumer =
        new VeniceChangelogConsumerImpl<>(changelogClientConfig, mockPubSubConsumer);

    NativeMetadataRepositoryViewAdapter mockRepository = mock(NativeMetadataRepositoryViewAdapter.class);
    Store store = mock(Store.class);
    Version mockVersion = new VersionImpl(storeName, 1, "foo");
    mockVersion.setPartitionCount(2);
    when(store.getCurrentVersion()).thenReturn(1);
    when(store.getCompressionStrategy()).thenReturn(CompressionStrategy.NO_OP);
    when(mockRepository.getStore(anyString())).thenReturn(store);
    when(store.getVersionOrThrow(Mockito.anyInt())).thenReturn(mockVersion);
    when(mockRepository.getValueSchema(storeName, 1)).thenReturn(new SchemaEntry(1, valueSchema));
    when(store.getPartitionCount()).thenReturn(2);
    when(store.getVersion(Mockito.anyInt())).thenReturn(mockVersion);
    veniceChangelogConsumer.setStoreRepository(mockRepository);

    Assert.assertEquals(veniceChangelogConsumer.getPartitionCount(), 2);

    veniceChangelogConsumer.subscribe(new HashSet<>(Arrays.asList(0))).get();
    veniceChangelogConsumer.seekToTimestamp(System.currentTimeMillis() - 10000L);
    PubSubTopicPartition oldVersionTopicPartition = new PubSubTopicPartitionImpl(oldVersionTopic, 0);
    verify(mockPubSubConsumer).subscribe(oldVersionTopicPartition, OffsetRecord.LOWEST_OFFSET);

    veniceChangelogConsumer.subscribe(new HashSet<>(Arrays.asList(0))).get();

    List<PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessages =
        (List<PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate>>) veniceChangelogConsumer.poll(100);
    for (int i = 0; i < 5; i++) {
      PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate> pubSubMessage = pubSubMessages.get(i);
      ChangeEvent<Utf8> changeEvent = pubSubMessage.getValue();
      Assert.assertEquals(changeEvent.getCurrentValue().toString(), "newValue" + i);
      Assert.assertEquals(changeEvent.getPreviousValue().toString(), "oldValue" + i);
    }
    // Verify version swap happened.

    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(newChangeCaptureTopic, 0);
    verify(mockPubSubConsumer).subscribe(pubSubTopicPartition, OffsetRecord.LOWEST_OFFSET);
    pubSubMessages =
        (List<PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate>>) veniceChangelogConsumer.poll(100);
    Assert.assertTrue(pubSubMessages.isEmpty());

    doReturn(Collections.singleton(pubSubTopicPartition)).when(mockPubSubConsumer).getAssignment();
    veniceChangelogConsumer.pause(Collections.singleton(0));
    verify(mockPubSubConsumer).pause(any());

    veniceChangelogConsumer.resume(Collections.singleton(0));
    verify(mockPubSubConsumer).resume(any());
  }

  @Test
  public void testAfterImageConsumerSeek() throws ExecutionException, InterruptedException {
    D2ControllerClient d2ControllerClient = mock(D2ControllerClient.class);
    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    doReturn(1).when(storeInfo).getCurrentVersion();
    doReturn(2).when(storeInfo).getPartitionCount();
    doReturn(storeInfo).when(storeResponse).getStore();
    doReturn(storeResponse).when(d2ControllerClient).getStore(storeName);
    MultiSchemaResponse multiRMDSchemaResponse = mock(MultiSchemaResponse.class);
    MultiSchemaResponse.Schema rmdSchemaFromMultiSchemaResponse = mock(MultiSchemaResponse.Schema.class);
    doReturn(rmdSchema.toString()).when(rmdSchemaFromMultiSchemaResponse).getSchemaStr();
    doReturn(new MultiSchemaResponse.Schema[] { rmdSchemaFromMultiSchemaResponse }).when(multiRMDSchemaResponse)
        .getSchemas();
    doReturn(multiRMDSchemaResponse).when(d2ControllerClient).getAllReplicationMetadataSchemas(storeName);

    PubSubConsumerAdapter mockPubSubConsumer = mock(PubSubConsumerAdapter.class);
    PubSubTopic oldVersionTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 1));

    prepareVersionTopicRecordsToBePolled(0L, 5L, mockPubSubConsumer, oldVersionTopic, 0, true);
    ChangelogClientConfig changelogClientConfig = getChangelogClientConfig(d2ControllerClient).setViewName("");

    PubSubConsumerAdapter mockInternalSeekConsumer = Mockito.mock(PubSubConsumerAdapter.class);

    prepareChangeCaptureRecordsToBePolled(
        0L,
        10L,
        mockInternalSeekConsumer,
        oldVersionTopic,
        0,
        oldVersionTopic,
        null,
        true,
        false);
    VeniceAfterImageConsumerImpl<String, Utf8> veniceChangelogConsumer = new VeniceAfterImageConsumerImpl<>(
        changelogClientConfig,
        mockPubSubConsumer,
        Lazy.of(() -> mockInternalSeekConsumer));
    NativeMetadataRepositoryViewAdapter mockRepository = mock(NativeMetadataRepositoryViewAdapter.class);
    Store store = mock(Store.class);
    Version mockVersion = new VersionImpl(storeName, 1, "foo");
    mockVersion.setPartitionCount(2);
    when(store.getCurrentVersion()).thenReturn(1);
    when(store.getCompressionStrategy()).thenReturn(CompressionStrategy.NO_OP);
    when(store.getPartitionCount()).thenReturn(2);
    when(mockRepository.getStore(anyString())).thenReturn(store);
    when(store.getVersion(Mockito.anyInt())).thenReturn(mockVersion);
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
    Mockito.verify(mockPubSubConsumer).subscribe(pubSubTopicPartition, 10);
  }

  @Test
  public void testAdjustCheckpoints() {
    PubSubConsumerAdapter mockConsumer = Mockito.mock(PubSubConsumerAdapter.class);
    Map<Integer, VeniceChangeCoordinate> checkpoints = new HashMap<>();
    Map<Integer, Long> currentVersionLastHeartbeat = new HashMap<>();
    List<PubSubTopicPartition> topicPartitionList = new ArrayList<>();

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
    PubSubPosition aheadPosition = new PubSubPosition() {
      @Override
      public long getNumericOffset() {
        return offset + 1; // greater than 1
      }

      @Override
      public PubSubPositionWireFormat getPositionWireFormat() {
        return null;
      }

      @Override
      public int getHeapSize() {
        return 0;
      }
    };

    PubSubPosition behindPosition = new PubSubPosition() {
      @Override
      public long getNumericOffset() {
        return offset;
      }

      @Override
      public PubSubPositionWireFormat getPositionWireFormat() {
        return null;
      }

      @Override
      public int getHeapSize() {
        return 0;
      }
    };

    currentVersionLastHeartbeat.put(0, 1L);
    currentVersionLastHeartbeat.put(1, 2L);

    VeniceChangeCoordinate aheadCoordinate = new VeniceChangeCoordinate("topic1", aheadPosition, 0);
    VeniceChangeCoordinate behindCoordinate = new VeniceChangeCoordinate("topic1", behindPosition, 1);
    VeniceChangeCoordinate otherCoordinate = new VeniceChangeCoordinate("topic1", aheadPosition, 2);

    checkpoints.put(0, aheadCoordinate);
    checkpoints.put(1, behindCoordinate);
    checkpoints.put(2, otherCoordinate);

    // The heartbeat is ahead of the checkpoint — should update
    PubSubPosition newerAheadPosition = new PubSubPosition() {
      @Override
      public long getNumericOffset() {
        return offset + 2; // greater than 1
      }

      @Override
      public PubSubPositionWireFormat getPositionWireFormat() {
        return null;
      }

      @Override
      public int getHeapSize() {
        return 0;
      }
    };
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
        checkpoints.get(1).getPosition().getNumericOffset(),
        behindPosition.getNumericOffset(),
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
    D2ControllerClient d2ControllerClient = mock(D2ControllerClient.class);
    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    doReturn(1).when(storeInfo).getCurrentVersion();
    doReturn(2).when(storeInfo).getPartitionCount();
    doReturn(storeInfo).when(storeResponse).getStore();
    doReturn(storeResponse).when(d2ControllerClient).getStore(storeName);
    MultiSchemaResponse multiRMDSchemaResponse = mock(MultiSchemaResponse.class);
    MultiSchemaResponse.Schema rmdSchemaFromMultiSchemaResponse = mock(MultiSchemaResponse.Schema.class);
    doReturn(1).when(rmdSchemaFromMultiSchemaResponse).getRmdValueSchemaId();
    doReturn(1).when(rmdSchemaFromMultiSchemaResponse).getId();
    doReturn(rmdSchema.toString()).when(rmdSchemaFromMultiSchemaResponse).getSchemaStr();
    doReturn(new MultiSchemaResponse.Schema[] { rmdSchemaFromMultiSchemaResponse }).when(multiRMDSchemaResponse)
        .getSchemas();
    doReturn(multiRMDSchemaResponse).when(d2ControllerClient).getAllReplicationMetadataSchemas(storeName);

    PubSubConsumerAdapter mockPubSubConsumer = mock(PubSubConsumerAdapter.class);
    PubSubTopic oldVersionTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 1));
    prepareVersionTopicRecordsToBePolled(0L, 5L, mockPubSubConsumer, oldVersionTopic, 0, true);
    ChangelogClientConfig changelogClientConfig = getChangelogClientConfig(d2ControllerClient).setViewName("");
    changelogClientConfig.getInnerClientConfig()
        .setMetricsRepository(getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true));
    VeniceChangelogConsumerImpl<String, Utf8> veniceChangelogConsumer =
        new VeniceAfterImageConsumerImpl<>(changelogClientConfig, mockPubSubConsumer);

    BasicConsumerStats consumerStats = spy(veniceChangelogConsumer.getChangeCaptureStats());
    Field changeCaptureStatsField = VeniceChangelogConsumerImpl.class.getDeclaredField("changeCaptureStats");
    changeCaptureStatsField.setAccessible(true);
    changeCaptureStatsField.set(veniceChangelogConsumer, consumerStats);

    NativeMetadataRepositoryViewAdapter mockRepository = mock(NativeMetadataRepositoryViewAdapter.class);
    Store store = mock(Store.class);
    Version mockVersion = new VersionImpl(storeName, 1, "foo");
    mockVersion.setPartitionCount(2);
    when(store.getCurrentVersion()).thenReturn(1);
    when(store.getCompressionStrategy()).thenReturn(CompressionStrategy.NO_OP);
    when(store.getPartitionCount()).thenReturn(2);
    when(mockRepository.getStore(anyString())).thenReturn(store);
    when(mockRepository.getValueSchema(storeName, 1)).thenReturn(new SchemaEntry(1, valueSchema));
    when(store.getVersionOrThrow(Mockito.anyInt())).thenReturn(mockVersion);
    when(store.getVersion(Mockito.anyInt())).thenReturn(mockVersion);
    veniceChangelogConsumer.setStoreRepository(mockRepository);

    Assert.assertEquals(veniceChangelogConsumer.getPartitionCount(), 2);
    veniceChangelogConsumer.subscribe(new HashSet<>(Arrays.asList(0))).get();
    verify(mockPubSubConsumer).subscribe(new PubSubTopicPartitionImpl(oldVersionTopic, 0), OffsetRecord.LOWEST_OFFSET);

    List<PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessages =
        (List<PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate>>) veniceChangelogConsumer.poll(100);

    verify(consumerStats).emitPollCountMetrics(VeniceResponseStatusCategory.SUCCESS);
    verify(consumerStats).emitRecordsConsumedCountMetrics(pubSubMessages.size());

    for (int i = 0; i < 5; i++) {
      PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate> pubSubMessage = pubSubMessages.get(i);
      Utf8 messageStr = pubSubMessage.getValue().getCurrentValue();
      Assert.assertEquals(messageStr.toString(), "newValue" + i);
    }

    prepareVersionTopicRecordsToBePolled(5L, 15L, mockPubSubConsumer, oldVersionTopic, 0, true);
    pubSubMessages =
        (List<PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate>>) veniceChangelogConsumer.poll(100);
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
  public void testConsumeAfterImageWithCompaction()
      throws ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException {
    D2ControllerClient d2ControllerClient = mock(D2ControllerClient.class);
    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    doReturn(1).when(storeInfo).getCurrentVersion();
    doReturn(2).when(storeInfo).getPartitionCount();
    doReturn(storeInfo).when(storeResponse).getStore();
    doReturn(storeResponse).when(d2ControllerClient).getStore(storeName);
    MultiSchemaResponse multiRMDSchemaResponse = mock(MultiSchemaResponse.class);
    MultiSchemaResponse.Schema rmdSchemaFromMultiSchemaResponse = mock(MultiSchemaResponse.Schema.class);
    doReturn(1).when(rmdSchemaFromMultiSchemaResponse).getRmdValueSchemaId();
    doReturn(1).when(rmdSchemaFromMultiSchemaResponse).getId();
    doReturn(rmdSchema.toString()).when(rmdSchemaFromMultiSchemaResponse).getSchemaStr();
    doReturn(new MultiSchemaResponse.Schema[] { rmdSchemaFromMultiSchemaResponse }).when(multiRMDSchemaResponse)
        .getSchemas();
    doReturn(multiRMDSchemaResponse).when(d2ControllerClient).getAllReplicationMetadataSchemas(storeName);

    PubSubConsumerAdapter mockPubSubConsumer = mock(PubSubConsumerAdapter.class);
    PubSubTopic oldVersionTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 1));

    prepareVersionTopicRecordsToBePolled(0L, 5L, mockPubSubConsumer, oldVersionTopic, 0, true);
    ChangelogClientConfig changelogClientConfig =
        new ChangelogClientConfig<>().setD2ControllerClient(d2ControllerClient)
            .setSchemaReader(schemaReader)
            .setStoreName(storeName)
            .setShouldCompactMessages(true)
            .setViewName("");
    changelogClientConfig.getInnerClientConfig()
        .setMetricsRepository(getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true));
    VeniceChangelogConsumerImpl<String, Utf8> veniceChangelogConsumer =
        new VeniceAfterImageConsumerImpl<>(changelogClientConfig, mockPubSubConsumer);

    BasicConsumerStats consumerStats = spy(veniceChangelogConsumer.getChangeCaptureStats());
    Field changeCaptureStatsField = VeniceChangelogConsumerImpl.class.getDeclaredField("changeCaptureStats");
    changeCaptureStatsField.setAccessible(true);
    changeCaptureStatsField.set(veniceChangelogConsumer, consumerStats);

    NativeMetadataRepositoryViewAdapter mockRepository = mock(NativeMetadataRepositoryViewAdapter.class);
    Store store = mock(Store.class);
    Version mockVersion = new VersionImpl(storeName, 1, "foo");
    mockVersion.setPartitionCount(2);
    when(store.getCurrentVersion()).thenReturn(1);
    when(store.getCompressionStrategy()).thenReturn(CompressionStrategy.NO_OP);
    when(store.getPartitionCount()).thenReturn(2);
    when(mockRepository.getStore(anyString())).thenReturn(store);
    when(mockRepository.getValueSchema(storeName, 1)).thenReturn(new SchemaEntry(1, valueSchema));
    when(store.getVersionOrThrow(Mockito.anyInt())).thenReturn(mockVersion);
    when(store.getVersion(Mockito.anyInt())).thenReturn(mockVersion);
    veniceChangelogConsumer.setStoreRepository(mockRepository);

    Assert.assertEquals(veniceChangelogConsumer.getPartitionCount(), 2);

    veniceChangelogConsumer.subscribe(new HashSet<>(Arrays.asList(0))).get();
    verify(mockPubSubConsumer).subscribe(new PubSubTopicPartitionImpl(oldVersionTopic, 0), OffsetRecord.LOWEST_OFFSET);

    List<PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessages =
        new ArrayList<>(veniceChangelogConsumer.poll(100));
    for (int i = 0; i < 5; i++) {
      PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate> pubSubMessage = pubSubMessages.get(i);
      Utf8 messageStr = pubSubMessage.getValue().getCurrentValue();
      Assert.assertEquals(messageStr.toString(), "newValue" + i);
    }

    prepareVersionTopicRecordsToBePolled(5L, 15L, mockPubSubConsumer, oldVersionTopic, 0, true);
    pubSubMessages = new ArrayList<>(veniceChangelogConsumer.poll(100));

    verify(consumerStats, times(2)).emitPollCountMetrics(VeniceResponseStatusCategory.SUCCESS);
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
  public void testPollFailure() throws NoSuchFieldException, IllegalAccessException {
    VeniceChangelogConsumerImpl veniceChangelogConsumer = mock(VeniceChangelogConsumerImpl.class);

    BasicConsumerStats consumerStats = mock(BasicConsumerStats.class);
    Field changeCaptureStatsField = VeniceChangelogConsumerImpl.class.getDeclaredField("changeCaptureStats");
    changeCaptureStatsField.setAccessible(true);
    changeCaptureStatsField.set(veniceChangelogConsumer, consumerStats);

    long timeout = 1L;
    String topicSuffix = "suffix";
    boolean includeControlMessage = true;

    doCallRealMethod().when(veniceChangelogConsumer).internalPoll(timeout, topicSuffix, includeControlMessage);
    assertThrows(
        Exception.class,
        () -> veniceChangelogConsumer.internalPoll(timeout, topicSuffix, includeControlMessage));
    verify(consumerStats).emitPollCountMetrics(VeniceResponseStatusCategory.FAIL);
    verify(consumerStats, times(0)).emitRecordsConsumedCountMetrics(anyInt());
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
    when(mockConsumer.internalSeekToEndOfPush(Mockito.anySet(), Mockito.any(), Mockito.anyBoolean()))
        .thenReturn(CompletableFuture.completedFuture(null));

    String storeName = "Leppalúði_store";

    NativeMetadataRepositoryViewAdapter mockRepository = Mockito.mock(NativeMetadataRepositoryViewAdapter.class);

    Version mockVersion = Mockito.mock(Version.class);
    when(mockVersion.kafkaTopicName()).thenReturn(storeName + "_v5");

    Store mockStore = Mockito.mock(Store.class);
    when(mockStore.getCurrentVersion()).thenReturn(5);
    when(mockRepository.getStore(storeName)).thenReturn(mockStore);
    when(mockStore.getVersion(5)).thenReturn(mockVersion);

    VersionSwapDataChangeListener changeListener =
        new VersionSwapDataChangeListener(mockConsumer, mockRepository, storeName, "");
    changeListener.handleStoreChanged(mockStore);
    Mockito.verify(mockConsumer).internalSeekToEndOfPush(Mockito.anySet(), Mockito.any(), Mockito.anyBoolean());

  }

  @Test
  public void testHandleVersionSwapControlMessage() throws NoSuchFieldException, IllegalAccessException {
    VeniceChangelogConsumerImpl veniceChangelogConsumer = mock(VeniceChangelogConsumerImpl.class);

    Map<Integer, Map<Integer, List<Long>>> currentVersionHighWatermarks = new VeniceConcurrentHashMap<>();
    Field currentVersionHighWatermarksField =
        VeniceChangelogConsumerImpl.class.getDeclaredField("currentVersionHighWatermarks");
    currentVersionHighWatermarksField.setAccessible(true);
    currentVersionHighWatermarksField.set(veniceChangelogConsumer, currentVersionHighWatermarks);

    ChunkAssembler chunkAssembler = mock(ChunkAssembler.class);
    Field chunkAssemblerField = VeniceChangelogConsumerImpl.class.getDeclaredField("chunkAssembler");
    chunkAssemblerField.setAccessible(true);
    chunkAssemblerField.set(veniceChangelogConsumer, chunkAssembler);

    BasicConsumerStats consumerStats = mock(BasicConsumerStats.class);
    Field changeCaptureStatsField = VeniceChangelogConsumerImpl.class.getDeclaredField("changeCaptureStats");
    changeCaptureStatsField.setAccessible(true);
    changeCaptureStatsField.set(veniceChangelogConsumer, consumerStats);

    PubSubTopicRepository pubSubTopicRepository = mock(PubSubTopicRepository.class);
    PubSubTopic pubSubTopic = mock(PubSubTopic.class);
    when(pubSubTopicRepository.getTopic(anyString())).thenReturn(pubSubTopic);

    Field pubSubTopicRepositoryField = VeniceChangelogConsumerImpl.class.getDeclaredField("pubSubTopicRepository");
    pubSubTopicRepositoryField.setAccessible(true);
    pubSubTopicRepositoryField.set(veniceChangelogConsumer, pubSubTopicRepository);

    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageType = ControlMessageType.VERSION_SWAP.getValue();
    controlMessage.controlMessageUnion = ControlMessageType.VERSION_SWAP.getNewInstance();
    VersionSwap versionSwapMessage = new VersionSwap();
    versionSwapMessage.oldServingVersionTopic = "topic_v1";
    versionSwapMessage.newServingVersionTopic = "topic_v2";
    versionSwapMessage.localHighWatermarks = Arrays.asList(1L, 2L, 3L);
    controlMessage.controlMessageUnion = versionSwapMessage;

    PubSubTopicPartition pubSubTopicPartition = mock(PubSubTopicPartition.class);
    String topicSuffix = "suffix";
    Integer partition = 1;
    when(pubSubTopicPartition.getPartitionNumber()).thenReturn(partition);

    doCallRealMethod().when(veniceChangelogConsumer)
        .handleVersionSwapControlMessage(controlMessage, pubSubTopicPartition, topicSuffix, partition);
    assertTrue(
        veniceChangelogConsumer
            .handleVersionSwapControlMessage(controlMessage, pubSubTopicPartition, topicSuffix, partition));
    verify(veniceChangelogConsumer).switchToNewTopic(pubSubTopic, topicSuffix, partition);
    verify(consumerStats).emitVersionSwapCountMetrics(VeniceResponseStatusCategory.SUCCESS);
  }

  @Test
  public void testHandleVersionSwapControlMessageFail() throws NoSuchFieldException, IllegalAccessException {
    VeniceChangelogConsumerImpl veniceChangelogConsumer = mock(VeniceChangelogConsumerImpl.class);

    BasicConsumerStats consumerStats = mock(BasicConsumerStats.class);
    Field changeCaptureStatsField = VeniceChangelogConsumerImpl.class.getDeclaredField("changeCaptureStats");
    changeCaptureStatsField.setAccessible(true);
    changeCaptureStatsField.set(veniceChangelogConsumer, consumerStats);

    PubSubTopicRepository pubSubTopicRepository = mock(PubSubTopicRepository.class);
    PubSubTopic pubSubTopic = mock(PubSubTopic.class);
    when(pubSubTopicRepository.getTopic(anyString())).thenReturn(pubSubTopic);

    Field pubSubTopicRepositoryField = VeniceChangelogConsumerImpl.class.getDeclaredField("pubSubTopicRepository");
    pubSubTopicRepositoryField.setAccessible(true);
    pubSubTopicRepositoryField.set(veniceChangelogConsumer, pubSubTopicRepository);

    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageType = ControlMessageType.VERSION_SWAP.getValue();
    controlMessage.controlMessageUnion = ControlMessageType.VERSION_SWAP.getNewInstance();
    VersionSwap versionSwapMessage = new VersionSwap();
    versionSwapMessage.oldServingVersionTopic = "topic_v1";
    versionSwapMessage.newServingVersionTopic = "topic_v2";
    versionSwapMessage.localHighWatermarks = Arrays.asList(1L, 2L, 3L);
    controlMessage.controlMessageUnion = versionSwapMessage;

    PubSubTopicPartition pubSubTopicPartition = mock(PubSubTopicPartition.class);
    String topicSuffix = "suffix";
    Integer partition = 1;
    when(pubSubTopicPartition.getPartitionNumber()).thenReturn(partition);

    doCallRealMethod().when(veniceChangelogConsumer)
        .handleVersionSwapControlMessage(controlMessage, pubSubTopicPartition, topicSuffix, partition);
    assertThrows(
        Exception.class,
        () -> veniceChangelogConsumer
            .handleVersionSwapControlMessage(controlMessage, pubSubTopicPartition, topicSuffix, partition));
    verify(consumerStats).emitVersionSwapCountMetrics(VeniceResponseStatusCategory.FAIL);
  }

  @Test
  public void testMetricReportingThread() throws InterruptedException {
    D2ControllerClient d2ControllerClient = mock(D2ControllerClient.class);
    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    doReturn(1).when(storeInfo).getCurrentVersion();
    doReturn(2).when(storeInfo).getPartitionCount();
    doReturn(storeInfo).when(storeResponse).getStore();
    doReturn(storeResponse).when(d2ControllerClient).getStore(storeName);
    MultiSchemaResponse multiRMDSchemaResponse = mock(MultiSchemaResponse.class);
    MultiSchemaResponse.Schema rmdSchemaFromMultiSchemaResponse = mock(MultiSchemaResponse.Schema.class);
    doReturn(rmdSchema.toString()).when(rmdSchemaFromMultiSchemaResponse).getSchemaStr();
    doReturn(new MultiSchemaResponse.Schema[] { rmdSchemaFromMultiSchemaResponse }).when(multiRMDSchemaResponse)
        .getSchemas();
    doReturn(multiRMDSchemaResponse).when(d2ControllerClient).getAllReplicationMetadataSchemas(storeName);

    PubSubConsumerAdapter mockPubSubConsumer = mock(PubSubConsumerAdapter.class);
    PubSubTopic oldVersionTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 1));

    prepareVersionTopicRecordsToBePolled(0L, 5L, mockPubSubConsumer, oldVersionTopic, 0, true);
    ChangelogClientConfig changelogClientConfig = getChangelogClientConfig(d2ControllerClient).setViewName("");
    VeniceChangelogConsumerImpl<String, Utf8> veniceChangelogConsumer =
        new VeniceAfterImageConsumerImpl<>(changelogClientConfig, mockPubSubConsumer);

    NativeMetadataRepositoryViewAdapter mockRepository = mock(NativeMetadataRepositoryViewAdapter.class);
    Store store = mock(Store.class);
    Version mockVersion = new VersionImpl(storeName, 1, "foo");
    mockVersion.setPartitionCount(2);
    when(store.getCurrentVersion()).thenReturn(1);
    when(store.getCompressionStrategy()).thenReturn(CompressionStrategy.NO_OP);
    when(store.getPartitionCount()).thenReturn(2);
    when(mockRepository.getStore(anyString())).thenReturn(store);
    when(mockRepository.getValueSchema(storeName, 1)).thenReturn(new SchemaEntry(1, valueSchema));
    when(store.getVersionOrThrow(Mockito.anyInt())).thenReturn(mockVersion);
    when(store.getVersion(Mockito.anyInt())).thenReturn(mockVersion);
    veniceChangelogConsumer.setStoreRepository(mockRepository);

    Assert.assertEquals(veniceChangelogConsumer.getPartitionCount(), 2);

    VeniceChangelogConsumerImpl.HeartbeatReporterThread reporterThread =
        veniceChangelogConsumer.getHeartbeatReporterThread();

    ConcurrentHashMap<Integer, Long> lastHeartbeat = new VeniceConcurrentHashMap<>();
    BasicConsumerStats consumerStats = Mockito.mock(BasicConsumerStats.class);
    Set<PubSubTopicPartition> topicPartitionSet = new HashSet<>();
    topicPartitionSet.add(new PubSubTopicPartitionImpl(oldVersionTopic, 1));

    reporterThread.recordStats(lastHeartbeat, consumerStats, topicPartitionSet);
    Mockito.verify(consumerStats).emitCurrentConsumingVersionMetrics(1, 1);
    Mockito.verify(consumerStats).emitHeartBeatDelayMetrics(anyLong());

    reporterThread.start();
    reporterThread.interrupt();
  }

  @Test
  public void testChunkingSuccess() throws NoSuchFieldException, IllegalAccessException {
    VeniceChangelogConsumerImpl veniceChangelogConsumer = mock(VeniceChangelogConsumerImpl.class);

    DefaultPubSubMessage pubSubMessage = mock(DefaultPubSubMessage.class);
    when(pubSubMessage.getPosition()).thenReturn(mock(PubSubPosition.class));
    when(pubSubMessage.getKey()).thenReturn(mock(KafkaKey.class));

    KafkaMessageEnvelope value = mock(KafkaMessageEnvelope.class);
    Put put = new Put();
    put.schemaId = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();
    value.payloadUnion = put;
    when(pubSubMessage.getValue()).thenReturn(value);

    HashMap<Integer, VeniceCompressor> compressorMap = mock(HashMap.class);
    when(compressorMap.get(anyInt())).thenReturn(mock(VeniceCompressor.class));
    Field compressorMapField = VeniceChangelogConsumerImpl.class.getDeclaredField("compressorMap");
    compressorMapField.setAccessible(true);
    compressorMapField.set(veniceChangelogConsumer, compressorMap);

    ChunkAssembler chunkAssembler = mock(ChunkAssembler.class);
    Field chunkAssemblerField = VeniceChangelogConsumerImpl.class.getDeclaredField("chunkAssembler");
    chunkAssemblerField.setAccessible(true);
    chunkAssemblerField.set(veniceChangelogConsumer, chunkAssembler);

    BasicConsumerStats consumerStats = mock(BasicConsumerStats.class);
    Field changeCaptureStatsField = VeniceChangelogConsumerImpl.class.getDeclaredField("changeCaptureStats");
    changeCaptureStatsField.setAccessible(true);
    changeCaptureStatsField.set(veniceChangelogConsumer, consumerStats);

    int partition = 0;
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 2));
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(pubSubTopic, partition);

    doCallRealMethod().when(veniceChangelogConsumer)
        .convertPubSubMessageToPubSubChangeEventMessage(pubSubMessage, topicPartition);
    veniceChangelogConsumer.convertPubSubMessageToPubSubChangeEventMessage(pubSubMessage, topicPartition);

    put.schemaId = AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion();
    veniceChangelogConsumer.convertPubSubMessageToPubSubChangeEventMessage(pubSubMessage, topicPartition);

    put.schemaId = 1;
    veniceChangelogConsumer.convertPubSubMessageToPubSubChangeEventMessage(pubSubMessage, topicPartition);

    verify(consumerStats, times(2)).emitChunkedRecordCountMetrics(VeniceResponseStatusCategory.SUCCESS);
  }

  @Test
  public void testChunkingFailure() throws NoSuchFieldException, IllegalAccessException {
    VeniceChangelogConsumerImpl veniceChangelogConsumer = mock(VeniceChangelogConsumerImpl.class);

    DefaultPubSubMessage pubSubMessage = mock(DefaultPubSubMessage.class);
    when(pubSubMessage.getKey()).thenReturn(mock(KafkaKey.class));

    KafkaMessageEnvelope value = mock(KafkaMessageEnvelope.class);
    Put put = new Put();
    put.schemaId = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();
    value.payloadUnion = put;
    when(pubSubMessage.getValue()).thenReturn(value);

    HashMap<Integer, VeniceCompressor> compressorMap = mock(HashMap.class);
    when(compressorMap.get(anyInt())).thenReturn(mock(VeniceCompressor.class));
    Field compressorMapField = VeniceChangelogConsumerImpl.class.getDeclaredField("compressorMap");
    compressorMapField.setAccessible(true);
    compressorMapField.set(veniceChangelogConsumer, compressorMap);

    ChunkAssembler chunkAssembler = mock(ChunkAssembler.class);
    Field chunkAssemblerField = VeniceChangelogConsumerImpl.class.getDeclaredField("chunkAssembler");
    chunkAssemblerField.setAccessible(true);
    chunkAssemblerField.set(veniceChangelogConsumer, chunkAssembler);

    BasicConsumerStats consumerStats = mock(BasicConsumerStats.class);
    Field changeCaptureStatsField = VeniceChangelogConsumerImpl.class.getDeclaredField("changeCaptureStats");
    changeCaptureStatsField.setAccessible(true);
    changeCaptureStatsField.set(veniceChangelogConsumer, consumerStats);

    int partition = 0;
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 2));
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(pubSubTopic, partition);

    doCallRealMethod().when(veniceChangelogConsumer)
        .convertPubSubMessageToPubSubChangeEventMessage(pubSubMessage, topicPartition);
    assertThrows(
        Exception.class,
        () -> veniceChangelogConsumer.convertPubSubMessageToPubSubChangeEventMessage(pubSubMessage, topicPartition));

    verify(consumerStats).emitChunkedRecordCountMetrics(VeniceResponseStatusCategory.FAIL);
  }

  private void prepareChangeCaptureRecordsToBePolled(
      long startIdx,
      long endIdx,
      PubSubConsumerAdapter pubSubConsumer,
      PubSubTopic changeCaptureTopic,
      int partition,
      PubSubTopic oldVersionTopic,
      PubSubTopic newVersionTopic,
      boolean addEndOfPushMessage,
      boolean repeatMessages) {
    List<DefaultPubSubMessage> pubSubMessageList = new ArrayList<>();

    // Add a start of push message
    pubSubMessageList.add(constructStartOfPushMessage(oldVersionTopic, partition));

    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> pubSubMessagesMap = new HashMap<>();
    for (long i = startIdx; i < endIdx; i++) {
      DefaultPubSubMessage pubSubMessage = constructChangeCaptureConsumerRecord(
          changeCaptureTopic,
          partition,
          "oldValue" + i,
          "newValue" + i,
          "key" + i,
          Arrays.asList(i, i));
      pubSubMessageList.add(pubSubMessage);
    }

    if (repeatMessages) {
      for (long i = startIdx; i < endIdx; i++) {
        DefaultPubSubMessage pubSubMessage = constructChangeCaptureConsumerRecord(
            changeCaptureTopic,
            partition,
            "oldValue" + i,
            "newValue" + i,
            "key" + i,
            Arrays.asList(i, i));
        pubSubMessageList.add(pubSubMessage);
      }
    }

    if (addEndOfPushMessage) {
      pubSubMessageList.add(constructEndOfPushMessage(changeCaptureTopic, partition, endIdx + 1));
    }

    if (newVersionTopic != null) {
      pubSubMessageList.add(
          constructVersionSwapMessage(
              oldVersionTopic,
              oldVersionTopic,
              newVersionTopic,
              partition,
              Arrays.asList(Long.valueOf(endIdx), Long.valueOf(endIdx))));
    }
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(changeCaptureTopic, partition);
    pubSubMessagesMap.put(topicPartition, pubSubMessageList);
    doReturn(pubSubMessagesMap).when(pubSubConsumer).poll(Mockito.anyLong());
  }

  private void prepareVersionTopicRecordsToBePolled(
      long startIdx,
      long endIdx,
      PubSubConsumerAdapter pubSubConsumerAdapter,
      PubSubTopic versionTopic,
      int partition,
      boolean prepareEndOfPush) {
    List<DefaultPubSubMessage> consumerRecordList = new ArrayList<>();
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> consumerRecordsMap = new HashMap<>();
    for (long i = startIdx; i < endIdx; i++) {
      DefaultPubSubMessage pubSubMessage =
          constructConsumerRecord(versionTopic, partition, "newValue" + i, "key" + i, Arrays.asList(i, i));
      consumerRecordList.add(pubSubMessage);
    }

    if (prepareEndOfPush) {
      consumerRecordList.add(constructEndOfPushMessage(versionTopic, partition, 0L));
    }
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(versionTopic, partition);
    consumerRecordsMap.put(pubSubTopicPartition, consumerRecordList);
    doReturn(consumerRecordsMap).when(pubSubConsumerAdapter).poll(100);
  }

  private DefaultPubSubMessage constructVersionSwapMessage(
      PubSubTopic versionTopic,
      PubSubTopic oldTopic,
      PubSubTopic newTopic,
      int partition,
      List<Long> localHighWatermarks) {
    KafkaKey kafkaKey = new KafkaKey(MessageType.CONTROL_MESSAGE, new byte[0]);
    VersionSwap versionSwapMessage = new VersionSwap();
    versionSwapMessage.oldServingVersionTopic = oldTopic.getName();
    versionSwapMessage.newServingVersionTopic = newTopic.getName();
    versionSwapMessage.localHighWatermarks = localHighWatermarks;

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

  private DefaultPubSubMessage constructChangeCaptureConsumerRecord(
      PubSubTopic changeCaptureVersionTopic,
      int partition,
      String oldValue,
      String newValue,
      String key,
      List<Long> replicationCheckpointVector) {
    ValueBytes oldValueBytes = new ValueBytes();
    oldValueBytes.schemaId = 1;
    oldValueBytes.value = ByteBuffer.wrap(valueSerializer.serialize(oldValue));
    ValueBytes newValueBytes = new ValueBytes();
    newValueBytes.schemaId = 1;
    newValueBytes.value = ByteBuffer.wrap(valueSerializer.serialize(newValue));
    RecordChangeEvent recordChangeEvent = new RecordChangeEvent();
    recordChangeEvent.currentValue = newValueBytes;
    recordChangeEvent.previousValue = oldValueBytes;
    recordChangeEvent.key = ByteBuffer.wrap(key.getBytes());
    recordChangeEvent.replicationCheckpointVector = replicationCheckpointVector;
    final RecordSerializer<RecordChangeEvent> recordChangeSerializer = FastSerializerDeserializerFactory
        .getFastAvroGenericSerializer(AvroProtocolDefinition.RECORD_CHANGE_EVENT.getCurrentProtocolVersionSchema());
    recordChangeSerializer.serialize(recordChangeEvent);
    ProducerMetadata producerMetadata = new ProducerMetadata();
    producerMetadata.setMessageTimestamp(1000L);
    KafkaMessageEnvelope kafkaMessageEnvelope = new KafkaMessageEnvelope(
        MessageType.PUT.getValue(),
        producerMetadata,
        new Put(ByteBuffer.wrap(recordChangeSerializer.serialize(recordChangeEvent)), 1, 0, ByteBuffer.allocate(0)),
        null);
    kafkaMessageEnvelope.setProducerMetadata(producerMetadata);
    KafkaKey kafkaKey = new KafkaKey(MessageType.PUT, keySerializer.serialize(key));
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(changeCaptureVersionTopic, partition);
    return new ImmutablePubSubMessage(kafkaKey, kafkaMessageEnvelope, pubSubTopicPartition, mockPubSubPosition, 0, 0);
  }

  private DefaultPubSubMessage constructConsumerRecord(
      PubSubTopic changeCaptureVersionTopic,
      int partition,
      String newValue,
      String key,
      List<Long> replicationCheckpointVector) {
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
    return new ImmutablePubSubMessage(kafkaKey, kafkaMessageEnvelope, pubSubTopicPartition, mockPubSubPosition, 0, 0);
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
    PubSubPosition mockPubSubPosition = mock(PubSubPosition.class);
    doReturn(offset).when(mockPubSubPosition).getNumericOffset();
    return new ImmutablePubSubMessage(kafkaKey, kafkaMessageEnvelope, pubSubTopicPartition, mockPubSubPosition, 0, 0);
  }

  private DefaultPubSubMessage constructStartOfPushMessage(PubSubTopic versionTopic, int partition) {
    KafkaKey kafkaKey = new KafkaKey(MessageType.CONTROL_MESSAGE, new byte[0]);
    StartOfPush startOfPush = new StartOfPush();
    startOfPush.compressionStrategy = CompressionStrategy.NO_OP.getValue();
    KafkaMessageEnvelope kafkaMessageEnvelope = new KafkaMessageEnvelope();
    ProducerMetadata producerMetadata = new ProducerMetadata();
    producerMetadata.setMessageTimestamp(1000L);
    kafkaMessageEnvelope.setProducerMetadata(producerMetadata);
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageUnion = startOfPush;
    controlMessage.controlMessageType = ControlMessageType.START_OF_PUSH.getValue();
    kafkaMessageEnvelope.payloadUnion = controlMessage;
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(versionTopic, partition);
    return new ImmutablePubSubMessage(kafkaKey, kafkaMessageEnvelope, pubSubTopicPartition, mockPubSubPosition, 0, 0);
  }

  private ChangelogClientConfig getChangelogClientConfig(D2ControllerClient d2ControllerClient) {
    ChangelogClientConfig changelogClientConfig =
        new ChangelogClientConfig<>().setD2ControllerClient(d2ControllerClient)
            .setSchemaReader(schemaReader)
            .setStoreName(storeName);
    changelogClientConfig.getInnerClientConfig()
        .setMetricsRepository(getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true));
    return changelogClientConfig;
  }
}
