package com.linkedin.davinci.consumer;

import static com.linkedin.venice.kafka.protocol.enums.ControlMessageType.START_OF_SEGMENT;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.consumer.stats.BasicConsumerStats;
import com.linkedin.davinci.kafka.consumer.TestPubSubTopic;
import com.linkedin.davinci.repository.ThinClientMetaStoreBasedRepository;
import com.linkedin.venice.client.change.capture.protocol.RecordChangeEvent;
import com.linkedin.venice.client.change.capture.protocol.ValueBytes;
import com.linkedin.venice.compression.CompressionStrategy;
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
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
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
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.views.ChangeCaptureView;
import io.tehuti.metrics.MetricsRepository;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private final Schema valueSchema = AvroCompatibilityHelper.parse("\"string\"");

  @BeforeMethod
  public void setUp() {
    storeName = Utils.getUniqueString();
    schemaReader = mock(SchemaReader.class);
    Schema keySchema = AvroCompatibilityHelper.parse("\"string\"");
    doReturn(keySchema).when(schemaReader).getKeySchema();
    doReturn(valueSchema).when(schemaReader).getValueSchema(1);
    rmdSchema = RmdSchemaGenerator.generateMetadataSchema(valueSchema, 1);

    keySerializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(keySchema);
    valueSerializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(valueSchema);
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
        getChangelogClientConfig(d2ControllerClient).setViewName("changeCaptureView");
    VeniceChangelogConsumerImpl<String, Utf8> veniceChangelogConsumer =
        new VeniceChangelogConsumerImpl<>(changelogClientConfig, mockPubSubConsumer);
    Assert.assertEquals(veniceChangelogConsumer.getPartitionCount(), 2);

    ThinClientMetaStoreBasedRepository mockRepository = mock(ThinClientMetaStoreBasedRepository.class);
    Store store = mock(Store.class);
    Version mockVersion = new VersionImpl(storeName, 1, "foo");
    Mockito.when(store.getCurrentVersion()).thenReturn(1);
    Mockito.when(store.getCompressionStrategy()).thenReturn(CompressionStrategy.NO_OP);
    Mockito.when(mockRepository.getStore(anyString())).thenReturn(store);
    Mockito.when(store.getVersionOrThrow(Mockito.anyInt())).thenReturn(mockVersion);
    Mockito.when(mockRepository.getValueSchema(storeName, 1)).thenReturn(new SchemaEntry(1, valueSchema));

    veniceChangelogConsumer.setStoreRepository(mockRepository);
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

    VeniceChangelogConsumerImpl mockInternalSeekConsumer = Mockito.mock(VeniceChangelogConsumerImpl.class);
    Mockito.when(mockInternalSeekConsumer.subscribe(any())).thenReturn(CompletableFuture.completedFuture(null));
    Mockito.when(mockInternalSeekConsumer.getPubSubConsumer()).thenReturn(mockPubSubConsumer);
    prepareChangeCaptureRecordsToBePolled(
        0L,
        10L,
        mockPubSubConsumer,
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
    veniceChangelogConsumer.versionSwapDetectionIntervalTimeInMs = 1;
    Assert.assertEquals(veniceChangelogConsumer.getPartitionCount(), 2);

    ThinClientMetaStoreBasedRepository mockRepository = mock(ThinClientMetaStoreBasedRepository.class);
    Store store = mock(Store.class);
    Version mockVersion = new VersionImpl(storeName, 1, "foo");
    Mockito.when(store.getCurrentVersion()).thenReturn(1);
    Mockito.when(store.getCompressionStrategy()).thenReturn(CompressionStrategy.NO_OP);
    Mockito.when(mockRepository.getStore(anyString())).thenReturn(store);
    Mockito.when(store.getVersionOrThrow(Mockito.anyInt())).thenReturn(mockVersion);
    veniceChangelogConsumer.setStoreRepository(mockRepository);
    veniceChangelogConsumer.subscribe(new HashSet<>(Arrays.asList(0))).get();

    Set<Integer> partitionSet = new HashSet<>();
    partitionSet.add(0);
    veniceChangelogConsumer.seekToEndOfPush(partitionSet).get();

    Mockito.verify(mockInternalSeekConsumer).subscribe(partitionSet);
    Mockito.verify(mockInternalSeekConsumer).unsubscribe(partitionSet);
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(oldVersionTopic, 0);
    Mockito.verify(mockPubSubConsumer).subscribe(pubSubTopicPartition, 10);
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
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> message =
        new ImmutablePubSubMessage<>(KafkaKey.HEART_BEAT, kafkaMessageEnvelope, pubSubTopicPartition, 0, 0, 0);
    doReturn(currentTimestamp).when(veniceChangelogConsumer).getSubscribeTime();
    veniceChangelogConsumer.maybeUpdatePartitionToBootstrapMap(message, pubSubTopicPartition);
    Assert.assertFalse(bootstrapStateMap.get(0));
    kafkaMessageEnvelope.producerMetadata.messageTimestamp = currentTimestamp - TimeUnit.SECONDS.toMillis(30);
    veniceChangelogConsumer.maybeUpdatePartitionToBootstrapMap(message, pubSubTopicPartition);
    Assert.assertTrue(bootstrapStateMap.get(0));
  }

  @Test
  public void testConsumeAfterImage() throws ExecutionException, InterruptedException {
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
    PubSubTopic oldChangeCaptureTopic =
        pubSubTopicRepository.getTopic(oldVersionTopic + ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX);

    prepareVersionTopicRecordsToBePolled(0L, 5L, mockPubSubConsumer, oldVersionTopic, 0, true);
    ChangelogClientConfig changelogClientConfig = getChangelogClientConfig(d2ControllerClient).setViewName("");
    VeniceChangelogConsumerImpl<String, Utf8> veniceChangelogConsumer =
        new VeniceAfterImageConsumerImpl<>(changelogClientConfig, mockPubSubConsumer);
    Assert.assertEquals(veniceChangelogConsumer.getPartitionCount(), 2);

    ThinClientMetaStoreBasedRepository mockRepository = mock(ThinClientMetaStoreBasedRepository.class);
    Store store = mock(Store.class);
    Version mockVersion = new VersionImpl(storeName, 1, "foo");
    Mockito.when(store.getCurrentVersion()).thenReturn(1);
    Mockito.when(store.getCompressionStrategy()).thenReturn(CompressionStrategy.NO_OP);
    Mockito.when(mockRepository.getStore(anyString())).thenReturn(store);
    Mockito.when(mockRepository.getValueSchema(storeName, 1)).thenReturn(new SchemaEntry(1, valueSchema));
    Mockito.when(store.getVersionOrThrow(Mockito.anyInt())).thenReturn(mockVersion);
    veniceChangelogConsumer.setStoreRepository(mockRepository);
    veniceChangelogConsumer.subscribe(new HashSet<>(Arrays.asList(0))).get();
    verify(mockPubSubConsumer).subscribe(new PubSubTopicPartitionImpl(oldVersionTopic, 0), OffsetRecord.LOWEST_OFFSET);

    List<PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessages =
        (List<PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate>>) veniceChangelogConsumer.poll(100);
    for (int i = 0; i < 5; i++) {
      PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate> pubSubMessage = pubSubMessages.get(i);
      Utf8 messageStr = pubSubMessage.getValue().getCurrentValue();
      Assert.assertEquals(messageStr.toString(), "newValue" + i);
    }
    prepareChangeCaptureRecordsToBePolled(
        0L,
        10L,
        mockPubSubConsumer,
        oldChangeCaptureTopic,
        0,
        oldVersionTopic,
        null,
        false,
        false);
    pubSubMessages =
        (List<PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate>>) veniceChangelogConsumer.poll(100);
    Assert.assertFalse(pubSubMessages.isEmpty());
    Assert.assertEquals(pubSubMessages.size(), 10);
    for (int i = 0; i < 10; i++) {
      PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate> pubSubMessage = pubSubMessages.get(i);
      Utf8 pubSubMessageValue = pubSubMessage.getValue().getCurrentValue();
      Assert.assertEquals(pubSubMessageValue.toString(), "newValue" + i);
    }

    veniceChangelogConsumer.close();
    verify(mockPubSubConsumer, times(2)).batchUnsubscribe(any());
    verify(mockPubSubConsumer).close();
  }

  @Test
  public void testConsumeAfterImageWithCompaction() throws ExecutionException, InterruptedException {
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
    PubSubTopic oldChangeCaptureTopic =
        pubSubTopicRepository.getTopic(oldVersionTopic + ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX);

    prepareVersionTopicRecordsToBePolled(0L, 5L, mockPubSubConsumer, oldVersionTopic, 0, true);
    ChangelogClientConfig changelogClientConfig =
        new ChangelogClientConfig<>().setD2ControllerClient(d2ControllerClient)
            .setSchemaReader(schemaReader)
            .setStoreName(storeName)
            .setShouldCompactMessages(true)
            .setViewName("");
    changelogClientConfig.getInnerClientConfig().setMetricsRepository(new MetricsRepository());
    VeniceChangelogConsumerImpl<String, Utf8> veniceChangelogConsumer =
        new VeniceAfterImageConsumerImpl<>(changelogClientConfig, mockPubSubConsumer);
    Assert.assertEquals(veniceChangelogConsumer.getPartitionCount(), 2);

    ThinClientMetaStoreBasedRepository mockRepository = mock(ThinClientMetaStoreBasedRepository.class);
    Store store = mock(Store.class);
    Version mockVersion = new VersionImpl(storeName, 1, "foo");
    Mockito.when(store.getCurrentVersion()).thenReturn(1);
    Mockito.when(store.getCompressionStrategy()).thenReturn(CompressionStrategy.NO_OP);
    Mockito.when(mockRepository.getStore(anyString())).thenReturn(store);
    Mockito.when(mockRepository.getValueSchema(storeName, 1)).thenReturn(new SchemaEntry(1, valueSchema));
    Mockito.when(store.getVersionOrThrow(Mockito.anyInt())).thenReturn(mockVersion);
    veniceChangelogConsumer.setStoreRepository(mockRepository);
    veniceChangelogConsumer.subscribe(new HashSet<>(Arrays.asList(0))).get();
    verify(mockPubSubConsumer).subscribe(new PubSubTopicPartitionImpl(oldVersionTopic, 0), OffsetRecord.LOWEST_OFFSET);

    List<PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate>> pubSubMessages =
        new ArrayList<>(veniceChangelogConsumer.poll(100));
    for (int i = 0; i < 5; i++) {
      PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate> pubSubMessage = pubSubMessages.get(i);
      Utf8 messageStr = pubSubMessage.getValue().getCurrentValue();
      Assert.assertEquals(messageStr.toString(), "newValue" + i);
    }
    prepareChangeCaptureRecordsToBePolled(
        0L,
        10L,
        mockPubSubConsumer,
        oldChangeCaptureTopic,
        0,
        oldVersionTopic,
        null,
        false,
        true);
    pubSubMessages = new ArrayList<>(veniceChangelogConsumer.poll(100));
    Assert.assertFalse(pubSubMessages.isEmpty());
    Assert.assertEquals(pubSubMessages.size(), 10);
    for (int i = 0; i < 10; i++) {
      PubSubMessage<String, ChangeEvent<Utf8>, VeniceChangeCoordinate> pubSubMessage = pubSubMessages.get(i);
      Utf8 pubSubMessageValue = pubSubMessage.getValue().getCurrentValue();
      Assert.assertEquals(pubSubMessageValue.toString(), "newValue" + i);
    }

    veniceChangelogConsumer.close();
    verify(mockPubSubConsumer, times(2)).batchUnsubscribe(any());
    verify(mockPubSubConsumer).close();
  }

  @Test
  public void testVersionSwapDataChangeListener() {
    VeniceAfterImageConsumerImpl mockConsumer = Mockito.mock(VeniceAfterImageConsumerImpl.class);
    Mockito.when(mockConsumer.subscribed()).thenReturn(true);
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(
        new TestPubSubTopic(storeName + "_v1", storeName, PubSubTopicType.VERSION_TOPIC),
        1);

    Set<PubSubTopicPartition> topicPartitionSet = new HashSet<>();
    topicPartitionSet.add(topicPartition);
    Mockito.when(mockConsumer.getTopicAssignment()).thenReturn(topicPartitionSet);
    Mockito.when(mockConsumer.seekToEndOfPush(Mockito.anySet())).thenReturn(CompletableFuture.completedFuture(null));

    String storeName = "Leppalúði_store";

    ThinClientMetaStoreBasedRepository mockRepository = Mockito.mock(ThinClientMetaStoreBasedRepository.class);
    Store mockStore = Mockito.mock(Store.class);
    Mockito.when(mockStore.getCurrentVersion()).thenReturn(5);
    Mockito.when(mockRepository.getStore(storeName)).thenReturn(mockStore);

    VersionSwapDataChangeListener changeListener =
        new VersionSwapDataChangeListener(mockConsumer, mockRepository, storeName, "");
    changeListener.handleStoreChanged(null);
    Mockito.verify(mockConsumer).seekToEndOfPush(Mockito.anySet());

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
    Assert.assertEquals(veniceChangelogConsumer.getPartitionCount(), 2);

    VeniceChangelogConsumerImpl.HeartbeatReporterThread reporterThread =
        veniceChangelogConsumer.getHeartbeatReporterThread();

    Map<Integer, Long> lastHeartbeat = new HashMap<>();
    BasicConsumerStats consumerStats = Mockito.mock(BasicConsumerStats.class);
    Set<PubSubTopicPartition> topicPartitionSet = new HashSet<>();
    topicPartitionSet.add(new PubSubTopicPartitionImpl(oldVersionTopic, 1));

    reporterThread.recordStats(lastHeartbeat, consumerStats, topicPartitionSet);
    Mockito.verify(consumerStats).recordMaximumConsumingVersion(1);
    Mockito.verify(consumerStats).recordMinimumConsumingVersion(1);

    reporterThread.start();
    reporterThread.interrupt();
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
    List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> pubSubMessageList = new ArrayList<>();

    // Add a start of push message
    pubSubMessageList.add(constructStartOfPushMessage(oldVersionTopic, partition));

    Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> pubSubMessagesMap =
        new HashMap<>();
    for (long i = startIdx; i < endIdx; i++) {
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> pubSubMessage = constructChangeCaptureConsumerRecord(
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
        PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> pubSubMessage = constructChangeCaptureConsumerRecord(
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
    List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> consumerRecordList = new ArrayList<>();
    Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> consumerRecordsMap =
        new HashMap<>();
    for (long i = startIdx; i < endIdx; i++) {
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> pubSubMessage =
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

  private PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> constructVersionSwapMessage(
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
    return new ImmutablePubSubMessage<>(kafkaKey, kafkaMessageEnvelope, pubSubTopicPartition, 0, 0, 0);
  }

  private PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> constructChangeCaptureConsumerRecord(
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
        new Put(ByteBuffer.wrap(recordChangeSerializer.serialize(recordChangeEvent)), 0, 0, ByteBuffer.allocate(0)),
        null);
    kafkaMessageEnvelope.setProducerMetadata(producerMetadata);
    KafkaKey kafkaKey = new KafkaKey(MessageType.PUT, keySerializer.serialize(key));
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(changeCaptureVersionTopic, partition);
    return new ImmutablePubSubMessage<>(kafkaKey, kafkaMessageEnvelope, pubSubTopicPartition, 0, 0, 0);
  }

  private PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> constructConsumerRecord(
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
    return new ImmutablePubSubMessage<>(kafkaKey, kafkaMessageEnvelope, pubSubTopicPartition, 0, 0, 0);
  }

  private PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> constructEndOfPushMessage(
      PubSubTopic versionTopic,
      int partition,
      Long offset) {
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
    return new ImmutablePubSubMessage<>(kafkaKey, kafkaMessageEnvelope, pubSubTopicPartition, offset, 0, 0);
  }

  private PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> constructStartOfPushMessage(
      PubSubTopic versionTopic,
      int partition) {
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
    return new ImmutablePubSubMessage<>(kafkaKey, kafkaMessageEnvelope, pubSubTopicPartition, 0, 0, 0);
  }

  private ChangelogClientConfig getChangelogClientConfig(D2ControllerClient d2ControllerClient) {
    ChangelogClientConfig changelogClientConfig =
        new ChangelogClientConfig<>().setD2ControllerClient(d2ControllerClient)
            .setSchemaReader(schemaReader)
            .setStoreName(storeName);
    changelogClientConfig.getInnerClientConfig().setMetricsRepository(new MetricsRepository());
    return changelogClientConfig;
  }
}
