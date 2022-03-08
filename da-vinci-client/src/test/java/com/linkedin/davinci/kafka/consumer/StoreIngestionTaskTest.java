package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.helix.LeaderFollowerIngestionProgressNotifier;
import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.davinci.helix.OnlineOfflineIngestionProgressNotifier;
import com.linkedin.davinci.notifier.LogNotifier;
import com.linkedin.davinci.notifier.PartitionPushStatusNotifier;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.stats.AggStoreIngestionStats;
import com.linkedin.davinci.stats.AggVersionedDIVStats;
import com.linkedin.davinci.stats.AggVersionedStorageIngestionStats;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.AbstractStoragePartition;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.davinci.store.rocksdb.RocksDBServerConfig;
import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceIngestionTaskKilledException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.exceptions.validation.CorruptDataException;
import com.linkedin.venice.exceptions.validation.FatalDataValidationException;
import com.linkedin.venice.exceptions.validation.MissingDataException;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.TopicManagerRepository;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.TopicSwitch;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.kafka.validation.checksum.CheckSum;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.IncrementalPushPolicy;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.offsets.DeepCopyStorageMetadataService;
import com.linkedin.venice.offsets.InMemoryStorageMetadataService;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.partitioner.UserPartitionAwarePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.schema.rmd.ReplicationMetadataSchemaGenerator;
import com.linkedin.venice.schema.rmd.ReplicationMetadataSchemaEntry;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.unit.kafka.InMemoryKafkaBroker;
import com.linkedin.venice.unit.kafka.SimplePartitioner;
import com.linkedin.venice.unit.kafka.consumer.MockInMemoryConsumer;
import com.linkedin.venice.unit.kafka.consumer.poll.AbstractPollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.ArbitraryOrderingPollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.BlockingObserverPollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.CompositePollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.DuplicatingPollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.FilteringPollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.PollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.RandomPollStrategy;
import com.linkedin.venice.unit.kafka.producer.MockInMemoryProducer;
import com.linkedin.venice.unit.kafka.producer.TransformingProducer;
import com.linkedin.venice.unit.matchers.ExceptionClassMatcher;
import com.linkedin.venice.unit.matchers.LongEqualOrGreaterThanMatcher;
import com.linkedin.venice.unit.matchers.NonEmptyStringMatcher;
import com.linkedin.venice.utils.ByteArray;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.DiskUsage;
import com.linkedin.venice.utils.MockTime;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.KafkaProducerWrapper;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.*;
import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.VeniceConstants.*;
import static com.linkedin.venice.utils.TestUtils.*;
import static com.linkedin.venice.utils.Time.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

/**
 * Unit tests for the KafkaPerStoreConsumptionTask.
 *
 * Be ware that most of the test cases in this suite depend on {@link StoreIngestionTaskTest#TEST_TIMEOUT_MS}
 * Adjust it based on environment if timeout failure occurs.
 */
@Test(singleThreaded = true)
public class StoreIngestionTaskTest {
  private static final Logger logger = Logger.getLogger(StoreIngestionTaskTest.class);

  private static final long TEST_TIMEOUT_MS;
  private static final int RUN_TEST_FUNCTION_TIMEOUT_SECONDS = 10;
  private static final long READ_CYCLE_DELAY_MS = 5;
  private static final long EMPTY_POLL_SLEEP_MS = 0;

  static {
    StoreIngestionTask.SCHEMA_POLLING_DELAY_MS = 100;
    IngestionNotificationDispatcher.PROGRESS_REPORT_INTERVAL = -1; // Report all the time.
    // Report progress/throttling for every message
    StoreIngestionTask.OFFSET_REPORTING_INTERVAL = 1;
    TEST_TIMEOUT_MS = 1000 * READ_CYCLE_DELAY_MS;
  }

  private InMemoryKafkaBroker inMemoryLocalKafkaBroker;
  private InMemoryKafkaBroker inMemoryRemoteKafkaBroker;
  private String localAndRemoteKafkaBootstrapServers;
  private VeniceWriterFactory mockWriterFactory;
  private VeniceWriter localVeniceWriter;
  private StorageEngineRepository mockStorageEngineRepository;
  private VeniceNotifier mockLogNotifier, mockPartitionStatusNotifier, mockLeaderFollowerStateModelNotifier, mockOnlineOfflineStateModelNotifier;
  private List<Object[]> mockNotifierProgress;
  private List<Object[]> mockNotifierEOPReceived;
  private List<Object[]> mockNotifierCompleted;
  private List<Object[]> mockNotifierError;
  private StorageMetadataService mockStorageMetadataService;
  private AbstractStorageEngine mockAbstractStorageEngine;
  private EventThrottler mockBandwidthThrottler;
  private EventThrottler mockRecordsThrottler;
  private Map<String, EventThrottler> kafkaUrlToRecordsThrottler;
  private KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler;
  private ReadOnlySchemaRepository mockSchemaRepo;
  private ReadOnlyStoreRepository mockMetadataRepo;
  /** N.B.: This mock can be used to verify() calls, but not to return arbitrary things. */
  private KafkaConsumerWrapper mockLocalKafkaConsumer;
  private KafkaConsumerWrapper mockRemoteKafkaConsumer;
  private TopicManager mockTopicManager;
  private TopicManagerRepository mockTopicManagerRepository;
  private AggStoreIngestionStats mockStoreIngestionStats;
  private AggVersionedDIVStats mockVersionedDIVStats;
  private AggVersionedStorageIngestionStats mockVersionedStorageIngestionStats;
  private StoreIngestionTask storeIngestionTaskUnderTest;
  private ExecutorService taskPollingService;
  private StoreBufferService storeBufferService;
  private BooleanSupplier isCurrentVersion;
  private Optional<HybridStoreConfig> hybridStoreConfig;
  private VeniceServerConfig veniceServerConfig;
  private RocksDBServerConfig rocksDBServerConfig;
  private long databaseSyncBytesIntervalForTransactionalMode = 1;
  private long databaseSyncBytesIntervalForDeferredWriteMode = 2;

  private static String storeNameWithoutVersionInfo;
  private static String topic;

  private static final int PARTITION_COUNT = 10;
  private static final Set<Integer> ALL_PARTITIONS = new HashSet<>();
  static { for (int partition = 0; partition < PARTITION_COUNT; partition++) { ALL_PARTITIONS.add(partition); } }
  private static final int PARTITION_FOO = 1;
  private static final int PARTITION_BAR = 2;
  private static final int SCHEMA_ID = -1;
  private static final int EXISTING_SCHEMA_ID = 1;
  private static final int NON_EXISTING_SCHEMA_ID = 2;
  private static final Schema STRING_SCHEMA = Schema.parse("\"string\"");

  private static final byte[] putKeyFoo = getRandomKey(PARTITION_FOO);
  private static final byte[] putKeyFoo2 = getRandomKey(PARTITION_FOO);
  private static final byte[] putKeyBar = getRandomKey(PARTITION_BAR);
  private static final byte[] putValue = "TestValuePut".getBytes(StandardCharsets.UTF_8);
  private static final byte[] putValueToCorrupt = "Please corrupt me!".getBytes(StandardCharsets.UTF_8);
  private static final byte[] deleteKeyFoo = getRandomKey(PARTITION_FOO);

  private static final int REPLICATION_METADATA_VERSION_ID = 1;
  private static final Schema REPLICATION_METADATA_SCHEMA = ReplicationMetadataSchemaGenerator.generateMetadataSchema(STRING_SCHEMA, 1);
  private static final RecordSerializer REPLICATION_METADATA_SERIALIZER = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(REPLICATION_METADATA_SCHEMA);

  private static final long putKeyFooTimestamp = 2L;
  private static final long deleteKeyFooTimestamp = 2L;
  private static final long putKeyFooOffset = 1L;
  private static final long deleteKeyFooOffset = 2L;

  private static final byte[] putKeyFooReplicationMetadataWithValueSchemaIdBytesDefault = createReplicationMetadataWithValueSchemaId(putKeyFooTimestamp - 1, putKeyFooOffset, EXISTING_SCHEMA_ID);
  private static final byte[] putKeyFooReplicationMetadataWithValueSchemaIdBytes = createReplicationMetadataWithValueSchemaId(putKeyFooTimestamp, putKeyFooOffset, EXISTING_SCHEMA_ID);
  private static final byte[] deleteKeyFooReplicationMetadataWithValueSchemaIdBytes = createReplicationMetadataWithValueSchemaId(deleteKeyFooTimestamp, deleteKeyFooOffset, EXISTING_SCHEMA_ID);

  private boolean databaseChecksumVerificationEnabled = false;

  private Supplier<Optional<StoreVersionState>> storeVersionStateSupplier = (Optional::empty);

  private static byte[] getRandomKey(Integer partition) {
    String randomString = Utils.getUniqueString("KeyForPartition" + partition);
    return ByteBuffer.allocate(randomString.length() + 1)
        .put(partition.byteValue())
        .put(randomString.getBytes())
        .array();
  }

  private static byte[] createReplicationMetadataWithValueSchemaId(long timestamp, long offset, int valueSchemaId) {
    GenericRecord replicationMetadataRecord = new GenericData.Record(REPLICATION_METADATA_SCHEMA);
    replicationMetadataRecord.put(TIMESTAMP_FIELD_NAME, timestamp);
    replicationMetadataRecord.put(REPLICATION_CHECKPOINT_VECTOR_FIELD, Collections.singletonList(offset));

    byte[] replicationMetadata = REPLICATION_METADATA_SERIALIZER.serialize(replicationMetadataRecord);

    ByteBuffer replicationMetadataWithValueSchemaId = ByteUtils.prependIntHeaderToByteBuffer(ByteBuffer.wrap(replicationMetadata), valueSchemaId, false);
    replicationMetadataWithValueSchemaId.position(replicationMetadataWithValueSchemaId.position() - ByteUtils.SIZE_OF_INT);
    return ByteUtils.extractByteArray(replicationMetadataWithValueSchemaId);
  }

  @BeforeClass(alwaysRun = true)
  public void suiteSetUp() throws Exception {
    taskPollingService = Executors.newFixedThreadPool(1);

    storeBufferService = new StoreBufferService(3, 10000, 1000);
    storeBufferService.start();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() throws Exception {
    taskPollingService.shutdownNow();
    storeBufferService.stop();
  }

  @BeforeMethod(alwaysRun = true)
  public void methodSetUp() throws Exception {
    storeNameWithoutVersionInfo = Utils.getUniqueString("TestTopic");
    topic = Version.composeKafkaTopic(storeNameWithoutVersionInfo, 1);

    inMemoryLocalKafkaBroker = new InMemoryKafkaBroker("local");
    inMemoryLocalKafkaBroker.createTopic(topic, PARTITION_COUNT);
    inMemoryRemoteKafkaBroker = new InMemoryKafkaBroker("remote");
    inMemoryRemoteKafkaBroker.createTopic(topic, PARTITION_COUNT);

    localAndRemoteKafkaBootstrapServers = String.join(",", inMemoryLocalKafkaBroker.getKafkaBootstrapServer(), inMemoryRemoteKafkaBroker.getKafkaBootstrapServer());
    localVeniceWriter = getVeniceWriter(() -> new MockInMemoryProducer(inMemoryLocalKafkaBroker));

    mockStorageEngineRepository = mock(StorageEngineRepository.class);

    mockLogNotifier = mock(LogNotifier.class);
    mockNotifierProgress = new ArrayList<>();
    doAnswer(invocation -> {
      Object[] args = invocation.getArguments();
      mockNotifierProgress.add(args);
      return null;
    }).when(mockLogNotifier).progress(anyString(), anyInt(), anyLong());
    mockNotifierEOPReceived = new ArrayList<>();
    doAnswer(invocation -> {
      Object[] args = invocation.getArguments();
      mockNotifierEOPReceived.add(args);
      return null;
    }).when(mockLogNotifier).endOfPushReceived(anyString(), anyInt(), anyLong());
    mockNotifierCompleted = new ArrayList<>();
    doAnswer(invocation -> {
      Object[] args = invocation.getArguments();
      mockNotifierCompleted.add(args);
      return null;
    }).when(mockLogNotifier).completed(anyString(), anyInt(), anyLong());
    mockNotifierError = new ArrayList<>();
    doAnswer(invocation -> {
      Object[] args = invocation.getArguments();
      mockNotifierError.add(args);
      return null;
    }).when(mockLogNotifier).error(anyString(), anyInt(), anyString(), any());

    mockPartitionStatusNotifier = mock(PartitionPushStatusNotifier.class);
    mockLeaderFollowerStateModelNotifier = mock(LeaderFollowerIngestionProgressNotifier.class);
    mockOnlineOfflineStateModelNotifier = mock(OnlineOfflineIngestionProgressNotifier.class);

    mockAbstractStorageEngine = mock(AbstractStorageEngine.class);
    mockStorageMetadataService = mock(StorageMetadataService.class);

    mockBandwidthThrottler = mock(EventThrottler.class);
    mockRecordsThrottler = mock(EventThrottler.class);
    mockSchemaRepo = mock(ReadOnlySchemaRepository.class);
    mockMetadataRepo = mock(ReadOnlyStoreRepository.class);
    mockLocalKafkaConsumer = mock(KafkaConsumerWrapper.class);
    mockRemoteKafkaConsumer = mock(KafkaConsumerWrapper.class);
    kafkaUrlToRecordsThrottler = new HashMap<>();
    kafkaClusterBasedRecordThrottler = new KafkaClusterBasedRecordThrottler(kafkaUrlToRecordsThrottler);

    mockTopicManager = mock(TopicManager.class);
    mockTopicManagerRepository = mock(TopicManagerRepository.class);
    doReturn(mockTopicManager).when(mockTopicManagerRepository).getTopicManager();

    mockStoreIngestionStats = mock(AggStoreIngestionStats.class);
    mockVersionedDIVStats = mock(AggVersionedDIVStats.class);
    mockVersionedStorageIngestionStats = mock(AggVersionedStorageIngestionStats.class);

    isCurrentVersion = () -> false;
    hybridStoreConfig = Optional.empty();
    databaseChecksumVerificationEnabled = false;
    rocksDBServerConfig = mock(RocksDBServerConfig.class);

    doReturn(true).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);
    doReturn(false).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, NON_EXISTING_SCHEMA_ID);

    doReturn(new ReplicationMetadataSchemaEntry(EXISTING_SCHEMA_ID, REPLICATION_METADATA_VERSION_ID, REPLICATION_METADATA_SCHEMA))
        .when(mockSchemaRepo).getReplicationMetadataSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID, REPLICATION_METADATA_VERSION_ID);

    storeVersionStateSupplier = getStoreVersionStateSupplierDefault();
  }

  private VeniceWriter getVeniceWriter(String topic, Supplier<KafkaProducerWrapper> producerSupplier, int amplificationFactor) {
    return new TestVeniceWriter(new VeniceProperties(new Properties()), topic, new DefaultSerializer(),
        new DefaultSerializer(), new DefaultSerializer(), getVenicePartitioner(amplificationFactor), SystemTime.INSTANCE, producerSupplier);
  }

  private VenicePartitioner getVenicePartitioner(int amplificationFactor) {
    VenicePartitioner partitioner;
    VenicePartitioner simplePartitioner = new SimplePartitioner();
    if (amplificationFactor == 1) {
      partitioner = simplePartitioner;
    } else {
      partitioner = new UserPartitionAwarePartitioner(simplePartitioner, amplificationFactor);
    }
    return partitioner;
  }

  private VeniceWriter getVeniceWriter(Supplier<KafkaProducerWrapper> producerSupplier) {
    return new TestVeniceWriter(new VeniceProperties(new Properties()), topic, new DefaultSerializer(),
        new DefaultSerializer(), new DefaultSerializer(), new SimplePartitioner(), SystemTime.INSTANCE, producerSupplier);
  }

  private VeniceWriter getCorruptedVeniceWriter(byte[] valueToCorrupt, InMemoryKafkaBroker kafkaBroker) {
    return getVeniceWriter(() -> new CorruptedKafkaProducer(new MockInMemoryProducer(kafkaBroker), valueToCorrupt));
  }

  class CorruptedKafkaProducer extends TransformingProducer {
    public CorruptedKafkaProducer(KafkaProducerWrapper baseProducer, byte[] valueToCorrupt) {
      super(baseProducer, (topicName, key, value, partition) -> {
        KafkaMessageEnvelope transformedMessageEnvelope = value;

        if (MessageType.valueOf(transformedMessageEnvelope) == MessageType.PUT) {
          Put put = (Put) transformedMessageEnvelope.payloadUnion;
          if (put.putValue.array() == valueToCorrupt) {
            put.putValue = ByteBuffer.wrap("CORRUPT_VALUE".getBytes());
            transformedMessageEnvelope.payloadUnion = put;
          }
        }

        return new TransformingProducer.SendMessageParameters(topic, key, transformedMessageEnvelope, partition);
      });
    }
  }

  private long getOffset(Future<RecordMetadata> recordMetadataFuture)
      throws ExecutionException, InterruptedException {
    return recordMetadataFuture.get().offset();
  }

  private void runTest(Set<Integer> partitions, Runnable assertions, boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    runTest(partitions, () -> {}, assertions, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  private void runHybridTest(Set<Integer> partitions, Runnable assertions, boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    runTest(new RandomPollStrategy(), partitions, () -> {}, assertions,
        Optional.of(new HybridStoreConfigImpl(100, 100,
            HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD, DataReplicationPolicy.NON_AGGREGATE,
            BufferReplayPolicy.REWIND_FROM_EOP)),
        Optional.empty(), isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  private void runTest(Set<Integer> partitions,
      Runnable beforeStartingConsumption,
      Runnable assertions,
      boolean isLeaderFollowerModelEnabled,
      boolean isActiveActiveReplicationEnabled) throws Exception {
    runTest(partitions, beforeStartingConsumption, assertions, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled, Collections.emptyMap());
  }

  private void runTest(PollStrategy pollStrategy,
      Set<Integer> partitions,
      Runnable beforeStartingConsumption,
      Runnable assertions,
      boolean isLeaderFollowerModelEnabled,
      boolean isActiveActiveReplicationEnabled) throws Exception {
    runTest(pollStrategy, partitions, beforeStartingConsumption, assertions, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled, Collections.emptyMap());
  }

  private void runTest(Set<Integer> partitions,
      Runnable beforeStartingConsumption,
      Runnable assertions,
      boolean isLeaderFollowerModelEnabled,
      boolean isActiveActiveReplicationEnabled,
      Map<String, Object> extraServerProperties) throws Exception {
    runTest(new RandomPollStrategy(), partitions, beforeStartingConsumption, assertions, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled, extraServerProperties);
  }

  private void runTest(PollStrategy pollStrategy, Set<Integer> partitions, Runnable beforeStartingConsumption,
      Runnable assertions, boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled,
      Map<String, Object> extraServerProperties) throws Exception {
    runTest(pollStrategy, partitions, beforeStartingConsumption, assertions, this.hybridStoreConfig, false, Optional.empty(), isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled, 1, extraServerProperties);
  }

  private void runTest(PollStrategy pollStrategy,
      Set<Integer> partitions,
      Runnable beforeStartingConsumption,
      Runnable assertions,
      Optional<HybridStoreConfig> hybridStoreConfig,
      Optional<DiskUsage> diskUsageForTest,
      boolean isLeaderFollowerModelEnabled,
      boolean isActiveActiveReplicationEnabled) throws Exception {
    runTest(pollStrategy, partitions, beforeStartingConsumption, assertions, hybridStoreConfig, false, diskUsageForTest,
        isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled, 1, Collections.emptyMap());
  }

  private void runTest(PollStrategy pollStrategy,
      Set<Integer> partitions,
      Runnable beforeStartingConsumption,
      Runnable assertions,
      Optional<HybridStoreConfig> hybridStoreConfig,
      boolean incrementalPushEnabled,
      Optional<DiskUsage> diskUsageForTest,
      boolean isLeaderFollowerModelEnabled,
      boolean isActiveActiveReplicationEnabled,
      int amplificationFactor,
      Map<String, Object> extraServerProperties) throws Exception {

    int partitionCount = PARTITION_COUNT / amplificationFactor;
    VenicePartitioner partitioner = getVenicePartitioner(1); // Only get base venice partitioner
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setPartitionerClass(partitioner.getClass().getName());
    partitionerConfig.setAmplificationFactor(amplificationFactor);

    MockStoreVersionConfigs storeAndVersionConfigs = setupStoreAndVersionMocks(partitionCount, partitionerConfig, hybridStoreConfig, incrementalPushEnabled, isLeaderFollowerModelEnabled, false, isActiveActiveReplicationEnabled);
    Store mockStore = storeAndVersionConfigs.store;
    Version version = storeAndVersionConfigs.version;
    VeniceStoreVersionConfig storeConfig = storeAndVersionConfigs.storeVersionConfig;

    StoreIngestionTaskFactory ingestionTaskFactory = getIngestionTaskFactoryBuilder(pollStrategy, partitions, hybridStoreConfig, diskUsageForTest, amplificationFactor, extraServerProperties).build();

    Properties kafkaProps = new Properties();
    kafkaProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, inMemoryLocalKafkaBroker.getKafkaBootstrapServer());

    int leaderSubPartition = PartitionUtils.getLeaderSubPartition(PARTITION_FOO, amplificationFactor);
    storeIngestionTaskUnderTest = ingestionTaskFactory.getNewIngestionTask(mockStore, version, kafkaProps,
        isCurrentVersion, storeConfig, leaderSubPartition, false,
        new StorageEngineBackedCompressorFactory(mockStorageMetadataService), Optional.empty());
    doReturn(new DeepCopyStorageEngine(mockAbstractStorageEngine)).when(mockStorageEngineRepository).getLocalStorageEngine(topic);

    Future testSubscribeTaskFuture = null;
    try {
      for (int partition: partitions) {
        storeIngestionTaskUnderTest.subscribePartition(topic, partition);
      }

      beforeStartingConsumption.run();

      // MockKafkaConsumer is prepared. Schedule for polling.
      testSubscribeTaskFuture = taskPollingService.submit(storeIngestionTaskUnderTest);

      assertions.run();

    } finally {
      storeIngestionTaskUnderTest.close();
      if (testSubscribeTaskFuture != null) {
        testSubscribeTaskFuture.get(RUN_TEST_FUNCTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      }
    }
  }

  private MockStoreVersionConfigs setupStoreAndVersionMocks(int partitionCount, PartitionerConfig partitionerConfig,
      Optional<HybridStoreConfig> hybridStoreConfig, boolean incrementalPushEnabled,
      boolean isLeaderFollowerModelEnabled, boolean isNativeReplicationEnabled,
      boolean isActiveActiveReplicationEnabled) {
    boolean isHybrid = hybridStoreConfig.isPresent();
    HybridStoreConfig hybridSoreConfigValue = null;
    if (isHybrid) {
      hybridSoreConfigValue = hybridStoreConfig.get();
    }

    VeniceStoreVersionConfig storeConfig = mock(VeniceStoreVersionConfig.class);
    doReturn(topic).when(storeConfig).getStoreVersionName();
    doReturn(0).when(storeConfig).getTopicOffsetCheckIntervalMs();
    doReturn(READ_CYCLE_DELAY_MS).when(storeConfig).getKafkaReadCycleDelayMs();
    doReturn(EMPTY_POLL_SLEEP_MS).when(storeConfig).getKafkaEmptyPollSleepMs();
    doReturn(databaseSyncBytesIntervalForTransactionalMode).when(storeConfig).getDatabaseSyncBytesIntervalForTransactionalMode();
    doReturn(databaseSyncBytesIntervalForDeferredWriteMode).when(storeConfig).getDatabaseSyncBytesIntervalForDeferredWriteMode();
    doReturn(false).when(storeConfig).isReadOnlyForBatchOnlyStoreEnabled();

    Store mockStore = mock(Store.class);
    Version version = new VersionImpl(storeNameWithoutVersionInfo, 1, "1", partitionCount);

    doReturn(storeNameWithoutVersionInfo).when(mockStore).getName();

    version.setPartitionerConfig(partitionerConfig);
    doReturn(partitionerConfig).when(mockStore).getPartitionerConfig();

    version.setLeaderFollowerModelEnabled(isLeaderFollowerModelEnabled);
    doReturn(isLeaderFollowerModelEnabled).when(mockStore).isLeaderFollowerModelEnabled();

    version.setIncrementalPushEnabled(incrementalPushEnabled);
    doReturn(incrementalPushEnabled).when(mockStore).isIncrementalPushEnabled();

    version.setIncrementalPushPolicy(IncrementalPushPolicy.PUSH_TO_VERSION_TOPIC);
    doReturn(IncrementalPushPolicy.PUSH_TO_VERSION_TOPIC).when(mockStore).getIncrementalPushPolicy();

    version.setHybridStoreConfig(hybridSoreConfigValue);
    doReturn(hybridSoreConfigValue).when(mockStore).getHybridStoreConfig();
    doReturn(isHybrid).when(mockStore).isHybrid();

    version.setBufferReplayEnabledForHybrid(true);

    version.setNativeReplicationEnabled(isNativeReplicationEnabled);
    doReturn(isNativeReplicationEnabled).when(mockStore).isNativeReplicationEnabled();

    version.setPushStreamSourceAddress("");
    doReturn("").when(mockStore).getPushStreamSourceAddress();

    doReturn(false).when(mockStore).isWriteComputationEnabled();

    doReturn(1).when(mockStore).getPartitionCount();

    doReturn(false).when(mockStore).isHybridStoreDiskQuotaEnabled();
    doReturn(-1).when(mockStore).getCurrentVersion();
    doReturn(1).when(mockStore).getBootstrapToOnlineTimeoutInHours();

    version.setActiveActiveReplicationEnabled(isActiveActiveReplicationEnabled);
    doReturn(isActiveActiveReplicationEnabled).when(mockStore).isActiveActiveReplicationEnabled();
    version.setReplicationMetadataVersionId(REPLICATION_METADATA_VERSION_ID);

    doReturn(Optional.of(version)).when(mockStore).getVersion(anyInt());
    doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeNameWithoutVersionInfo);
    doReturn(mockStore).when(mockMetadataRepo).getStore(storeNameWithoutVersionInfo);

    return new MockStoreVersionConfigs(mockStore, version, storeConfig);
  }

  private StoreIngestionTaskFactory.Builder getIngestionTaskFactoryBuilder(PollStrategy pollStrategy,
      Set<Integer> partitions,
      Optional<HybridStoreConfig> hybridStoreConfig,
      Optional<DiskUsage> diskUsageForTest,
      int amplificationFactor,
      Map<String, Object> extraServerProperties) {
    KafkaClientFactory mockFactory = mock(KafkaClientFactory.class);

    MockInMemoryConsumer inMemoryLocalKafkaConsumer = new MockInMemoryConsumer(inMemoryLocalKafkaBroker, pollStrategy, mockLocalKafkaConsumer);
    MockInMemoryConsumer inMemoryRemoteKafkaConsumer = new MockInMemoryConsumer(inMemoryRemoteKafkaBroker, pollStrategy, mockRemoteKafkaConsumer);

    doAnswer(invocation -> {
      Properties consumerProps = invocation.getArgument(0, Properties.class);
      String kafkaUrl = consumerProps.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
      if (kafkaUrl.equals(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer())) {
        return inMemoryRemoteKafkaConsumer;
      }

      return inMemoryLocalKafkaConsumer;
    }).when(mockFactory).getConsumer(any());

    mockWriterFactory = mock(VeniceWriterFactory.class);
    doReturn(null).when(mockWriterFactory).createBasicVeniceWriter(any());
    StorageMetadataService offsetManager;
    logger.info("mockStorageMetadataService: " + mockStorageMetadataService.getClass().getName());
    final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer = AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    if (mockStorageMetadataService.getClass() != InMemoryStorageMetadataService.class) {
      for (int partition : PartitionUtils.getSubPartitions(partitions, amplificationFactor)) {
        doReturn(new OffsetRecord(partitionStateSerializer)).when(mockStorageMetadataService).getLastOffset(topic, partition);
        if (hybridStoreConfig.isPresent()) {
          doReturn(Optional.of(new StoreVersionState())).when(mockStorageMetadataService).getStoreVersionState(topic);
        } else {
          doReturn(storeVersionStateSupplier.get()).when(mockStorageMetadataService).getStoreVersionState(topic);
        }
      }
    }
    offsetManager = new DeepCopyStorageMetadataService(mockStorageMetadataService);
    Queue<VeniceNotifier> onlineOfflineNotifiers = new ConcurrentLinkedQueue<>(Arrays.asList(mockLogNotifier,
        mockPartitionStatusNotifier, mockOnlineOfflineStateModelNotifier));
    Queue<VeniceNotifier> leaderFollowerNotifiers = new ConcurrentLinkedQueue<>(Arrays.asList(mockLogNotifier,
        mockPartitionStatusNotifier, mockLeaderFollowerStateModelNotifier));
    DiskUsage diskUsage;
    if (diskUsageForTest.isPresent()){
      diskUsage = diskUsageForTest.get();
    } else {
      diskUsage = mock(DiskUsage.class);
      doReturn(false).when(diskUsage).isDiskFull(anyLong());
    }

    veniceServerConfig = buildVeniceServerConfig(extraServerProperties);

    AggKafkaConsumerService aggKafkaConsumerService = mock(AggKafkaConsumerService.class);

    doAnswer(invocation -> {
      Properties consumerProps = invocation.getArgument(0, Properties.class);
      String kafkaUrl = consumerProps.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
      if (kafkaUrl.equals(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer())) {
        return inMemoryRemoteKafkaConsumer;
      }

      return inMemoryLocalKafkaConsumer;
    }).when(aggKafkaConsumerService).getConsumer(any(), any());

    EventThrottler mockUnorderedBandwidthThrottler = mock(EventThrottler.class);
    EventThrottler mockUnorderedRecordsThrottler = mock(EventThrottler.class);
    return StoreIngestionTaskFactory.builder()
        .setVeniceWriterFactory(mockWriterFactory)
        .setKafkaClientFactory(mockFactory)
        .setStorageEngineRepository(mockStorageEngineRepository)
        .setStorageMetadataService(offsetManager)
        .setOnlineOfflineNotifiersQueue(onlineOfflineNotifiers)
        .setLeaderFollowerNotifiersQueue(leaderFollowerNotifiers)
        .setBandwidthThrottler(mockBandwidthThrottler)
        .setRecordsThrottler(mockRecordsThrottler)
        .setUnorderedBandwidthThrottler(mockUnorderedBandwidthThrottler)
        .setUnorderedRecordsThrottler(mockUnorderedRecordsThrottler)
        .setKafkaClusterBasedRecordThrottler(kafkaClusterBasedRecordThrottler)
        .setSchemaRepository(mockSchemaRepo)
        .setMetadataRepository(mockMetadataRepo)
        .setTopicManagerRepository(mockTopicManagerRepository)
        .setStoreIngestionStats(mockStoreIngestionStats)
        .setVersionedDIVStats(mockVersionedDIVStats)
        .setVersionedStorageIngestionStats(mockVersionedStorageIngestionStats)
        .setStoreBufferService(storeBufferService)
        .setServerConfig(veniceServerConfig)
        .setDiskUsage(diskUsage)
        .setAggKafkaConsumerService(aggKafkaConsumerService)
        .setCacheWarmingThreadPool(Executors.newFixedThreadPool(1))
        .setPartitionStateSerializer(partitionStateSerializer);
  }

  Supplier<Optional<StoreVersionState>> getStoreVersionStateSupplierDefault() {
    StoreVersionState storeVersionState = new StoreVersionState();
    return (() ->Optional.of(storeVersionState));
  }

  Supplier<Optional<StoreVersionState>> getStoreVersionStateSupplier(boolean sorted) {
    StoreVersionState storeVersionState = new StoreVersionState();
    storeVersionState.sorted = sorted;
    return (() ->Optional.of(storeVersionState));
  }

  private Pair<TopicPartition, Long> getTopicPartitionOffsetPair(RecordMetadata recordMetadata) {
    return new Pair<>(new TopicPartition(recordMetadata.topic(), recordMetadata.partition()), recordMetadata.offset());
  }

  private Pair<TopicPartition, Long> getTopicPartitionOffsetPair(String topic, int partition, long offset) {
    return new Pair<>(new TopicPartition(topic, partition), offset);
  }

  /**
   * Verifies that the VeniceMessages from KafkaConsumer are processed appropriately as follows:
   * 1. A VeniceMessage with PUT requests leads to invoking of AbstractStorageEngine#put.
   * 2. A VeniceMessage with DELETE requests leads to invoking of AbstractStorageEngine#delete.
   * 3. A VeniceMessage with a Kafka offset that was already processed is ignored.
   */
  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testVeniceMessagesProcessing(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    RecordMetadata putMetadata = (RecordMetadata) localVeniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID, putKeyFooTimestamp, null).get();
    RecordMetadata deleteMetadata = (RecordMetadata) localVeniceWriter.delete(deleteKeyFoo, deleteKeyFooTimestamp, null).get();

    Queue<AbstractPollStrategy> pollStrategies = new LinkedList<>();
    pollStrategies.add(new RandomPollStrategy());

    // We re-deliver the old put out of order, so we can make sure it's ignored.
    Queue<Pair<TopicPartition, Long>> pollDeliveryOrder = new LinkedList<>();
    pollDeliveryOrder.add(getTopicPartitionOffsetPair(putMetadata));
    pollStrategies.add(new ArbitraryOrderingPollStrategy(pollDeliveryOrder));

    PollStrategy pollStrategy = new CompositePollStrategy(pollStrategies);

    runTest(pollStrategy, Utils.setOf(PARTITION_FOO), () -> {}, () -> {
      // Verify it retrieves the offset from the OffSet Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).getLastOffset(topic, PARTITION_FOO);

      verifyPutAndDelete(1, isActiveActiveReplicationEnabled, true);

      // Verify it commits the offset to Offset Manager
      OffsetRecord expectedOffsetRecordForDeleteMessage = getOffsetRecord(deleteMetadata.offset());
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS))
          .put(topic, PARTITION_FOO, expectedOffsetRecordForDeleteMessage);
    }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testAmplificationFactor(boolean isActiveActiveReplicationEnabled) throws Exception {
    final int amplificationFactor = 2;
    inMemoryLocalKafkaBroker.createTopic(Version.composeRealTimeTopic(storeNameWithoutVersionInfo), PARTITION_COUNT / amplificationFactor);
    mockStorageMetadataService = new InMemoryStorageMetadataService();

    AbstractStoragePartition mockStoragePartition = mock(AbstractStoragePartition.class);
    doReturn(mockStoragePartition).when(mockAbstractStorageEngine).getPartitionOrThrow(anyInt());

    doReturn(putKeyFooReplicationMetadataWithValueSchemaIdBytesDefault).when(mockStoragePartition).getReplicationMetadata(putKeyFoo);
    doReturn(deleteKeyFooReplicationMetadataWithValueSchemaIdBytes).when(mockStoragePartition).getReplicationMetadata(deleteKeyFoo);

    SchemaEntry schemaEntry = new SchemaEntry(1, "\"string\"");
    doReturn(schemaEntry).when(mockSchemaRepo).getLatestValueSchema(storeNameWithoutVersionInfo);

    VeniceWriter vtWriter = getVeniceWriter(topic, () -> new MockInMemoryProducer(inMemoryLocalKafkaBroker), amplificationFactor);
    VeniceWriter rtWriter = getVeniceWriter(Version.composeRealTimeTopic(storeNameWithoutVersionInfo), () -> new MockInMemoryProducer(
        inMemoryLocalKafkaBroker), 1);
    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(100,
        100,
        HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP
    );

    runTest(new RandomPollStrategy(),
            Utils.setOf(PARTITION_FOO),
            () -> {},
            () -> {
      vtWriter.broadcastStartOfPush(new HashMap<>());
      vtWriter.broadcastEndOfPush(new HashMap<>());
      doReturn(vtWriter).when(mockWriterFactory).createBasicVeniceWriter(anyString(), anyBoolean(), any(), any());

      verify(mockLogNotifier, never()).completed(anyString(), anyInt(), anyLong());
      vtWriter.broadcastTopicSwitch(Collections.singletonList(inMemoryLocalKafkaBroker.getKafkaBootstrapServer()),
              Version.composeRealTimeTopic(storeNameWithoutVersionInfo),
              System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(10),
              new HashMap<>());
      storeIngestionTaskUnderTest.promoteToLeader(topic, PARTITION_FOO, new LeaderFollowerPartitionStateModel.LeaderSessionIdChecker(1, new AtomicLong(1)));
      try {
        rtWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID, putKeyFooTimestamp, null).get();
        rtWriter.delete(deleteKeyFoo, deleteKeyFooTimestamp, null).get();

        verifyPutAndDelete(amplificationFactor, isActiveActiveReplicationEnabled, false);
      } catch (Exception e) {
        e.printStackTrace();
      }
      },
            Optional.of(hybridStoreConfig),
            false,
            Optional.empty(), true,
            isActiveActiveReplicationEnabled,
            amplificationFactor,
            Collections.singletonMap(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 3L)
    );
  }

  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testMissingMessagesForTopicWithLogCompactionEnabled(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    // enable log compaction
    when(mockTopicManager.isTopicCompactionEnabled(topic)).thenReturn(true);

    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    RecordMetadata putMetadata1 = (RecordMetadata) localVeniceWriter.put(putKeyFoo, putValueToCorrupt, SCHEMA_ID).get();
    RecordMetadata putMetadata2 = (RecordMetadata) localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID).get();
    RecordMetadata putMetadata3 = (RecordMetadata) localVeniceWriter.put(putKeyFoo2, putValueToCorrupt, SCHEMA_ID).get();
    RecordMetadata putMetadata4 = (RecordMetadata) localVeniceWriter.put(putKeyFoo2, putValue, SCHEMA_ID).get();

    Queue<Pair<TopicPartition, Long>> pollDeliveryOrder = new LinkedList<>();
    /**
     * The reason to put offset -1 and offset 0 in the deliveryOrder queue is that the SOS and SOP need to be polled.
     */
    pollDeliveryOrder.add(getTopicPartitionOffsetPair(topic, PARTITION_FOO, -1));
    pollDeliveryOrder.add(getTopicPartitionOffsetPair(topic, PARTITION_FOO, 0));
    /**
     * The reason to put "putMetadata1" and "putMetadata3" in the deliveryOrder queue is that
     * {@link AbstractPollStrategy#poll(InMemoryKafkaBroker, Map, long)} is always trying to return the next message
     * after whats in the queue. One at a time. Here we want to only deliver the unique entries after compaction:
     * putMetadata2 and putMetadata4
     */
    pollDeliveryOrder.add(getTopicPartitionOffsetPair(putMetadata1));
    pollDeliveryOrder.add(getTopicPartitionOffsetPair(putMetadata3));
    PollStrategy pollStrategy = new ArbitraryOrderingPollStrategy(pollDeliveryOrder);

    runTest(pollStrategy, Utils.setOf(PARTITION_FOO), () -> {}, () -> {
      // Verify it retrieves the offset from the OffSet Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).getLastOffset(topic, PARTITION_FOO);

      // Verify StorageEngine#put is invoked only once and with appropriate key & value.
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS))
          .put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(SCHEMA_ID, putValue).serialize()));

      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS))
          .put(PARTITION_FOO, putKeyFoo2, ByteBuffer.wrap(ValueRecord.create(SCHEMA_ID, putValue).serialize()));

      // Verify it commits the offset to Offset Manager
      OffsetRecord expectedOffsetRecordForLastMessage = getOffsetRecord(putMetadata4.offset());
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS))
          .put(topic, PARTITION_FOO, expectedOffsetRecordForLastMessage);
    }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testVeniceMessagesProcessingWithExistingSchemaId(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    long fooLastOffset = getOffset(localVeniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID));

    doReturn(true).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      // Verify it retrieves the offset from the OffSet Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).getLastOffset(topic, PARTITION_FOO);

      // Verify StorageEngine#put is invoked only once and with appropriate key & value.
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS))
          .put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(EXISTING_SCHEMA_ID, putValue).serialize()));
      verify(mockSchemaRepo, timeout(TEST_TIMEOUT_MS)).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);

      // Verify it commits the offset to Offset Manager
      OffsetRecord expected = getOffsetRecord(fooLastOffset);
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).put(topic, PARTITION_FOO, expected);
    }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  /**
   * Test the situation where records arrive faster than the schemas.
   * In this case, Venice would keep polling schemaRepo until schemas arrive.
   */
  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testVeniceMessagesProcessingWithTemporarilyNotAvailableSchemaId(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, NON_EXISTING_SCHEMA_ID);
    long existingSchemaOffset = getOffset(localVeniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID));

    when(mockSchemaRepo.hasValueSchema(storeNameWithoutVersionInfo, NON_EXISTING_SCHEMA_ID))
        .thenReturn(false, false, true);
    doReturn(true).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);

    runTest(Utils.setOf(PARTITION_FOO), () -> {}, () -> {
      // Verify it retrieves the offset from the OffSet Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).getLastOffset(topic, PARTITION_FOO);

      // Verify that after retrying 3 times, record with 'NON_EXISTING_SCHEMA_ID' was put into BDB.
      verify(mockSchemaRepo, timeout(TEST_TIMEOUT_MS).atLeast(3)).hasValueSchema(storeNameWithoutVersionInfo, NON_EXISTING_SCHEMA_ID);
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS)).
          put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(NON_EXISTING_SCHEMA_ID, putValue).serialize()));

      //Verify that the following record is consumed well.
      verify(mockSchemaRepo, timeout(TEST_TIMEOUT_MS)).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS))
          .put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(EXISTING_SCHEMA_ID, putValue).serialize()));

      OffsetRecord expected = getOffsetRecord(existingSchemaOffset);
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).put(topic, PARTITION_FOO, expected);
    }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  /**
   * Test the situation where records' schemas never arrive. In the case, the StoreIngestionTask will keep being blocked.
   */
  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testVeniceMessagesProcessingWithNonExistingSchemaId(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, NON_EXISTING_SCHEMA_ID);
    localVeniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID);

    doReturn(false).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, NON_EXISTING_SCHEMA_ID);
    doReturn(true).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      // Verify it retrieves the offset from the OffSet Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).getLastOffset(topic, PARTITION_FOO);

      //StoreIngestionTask#checkValueSchemaAvail will keep polling for 'NON_EXISTING_SCHEMA_ID'. It blocks the
      //#putConsumerRecord and will not enter drainer queue
      verify(mockSchemaRepo, after(TEST_TIMEOUT_MS).never()).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);
      verify(mockSchemaRepo, atLeastOnce()).hasValueSchema(storeNameWithoutVersionInfo, NON_EXISTING_SCHEMA_ID);
      verify(mockAbstractStorageEngine, never()).put(eq(PARTITION_FOO), any(), any(byte[].class));

      // Only two records(start_of_segment, start_of_push) offset were able to be recorded before 'NON_EXISTING_SCHEMA_ID' blocks #putConsumerRecord
      verify(mockStorageMetadataService, atMost(2)).put(eq(topic), eq(PARTITION_FOO), any(OffsetRecord.class));
    }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testReportStartWhenRestarting(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    final long STARTING_OFFSET = 2;
    runTest(Utils.setOf(PARTITION_FOO, PARTITION_BAR), () -> {
      doReturn(getOffsetRecord(STARTING_OFFSET)).when(mockStorageMetadataService).getLastOffset(anyString(), anyInt());
    }, () -> {
      // Verify STARTED is NOT reported when offset is 0
      verify(mockLogNotifier, never()).started(topic, PARTITION_BAR);
    }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testNotifier(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    long fooLastOffset = getOffset(localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    long barLastOffset = getOffset(localVeniceWriter.put(putKeyBar, putValue, SCHEMA_ID));
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    runTest(Utils.setOf(PARTITION_FOO, PARTITION_BAR), () -> {
      /**
       * Considering that the {@link VeniceWriter} will send an {@link ControlMessageType#END_OF_PUSH},
       * we need to add 1 to last data message offset.
       */
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce()).completed(topic, PARTITION_FOO, fooLastOffset + 1);
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce()).completed(topic, PARTITION_BAR, barLastOffset  + 1);
      verify(mockPartitionStatusNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce()).completed(topic, PARTITION_FOO,
          fooLastOffset + 1);
      verify(mockPartitionStatusNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce()).completed(topic, PARTITION_BAR,
          barLastOffset  + 1);
      if (isLeaderFollowerModelEnabled) {
        verify(mockLeaderFollowerStateModelNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce()).catchUpBaseTopicOffsetLag(topic, PARTITION_FOO);
        verify(mockLeaderFollowerStateModelNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce()).catchUpBaseTopicOffsetLag(topic, PARTITION_BAR);
        verify(mockLeaderFollowerStateModelNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce()).completed(topic, PARTITION_FOO,
            fooLastOffset + 1);
        verify(mockLeaderFollowerStateModelNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce()).completed(topic, PARTITION_BAR,
            barLastOffset  + 1);
        verify(mockOnlineOfflineStateModelNotifier, never()).completed(topic, PARTITION_FOO, fooLastOffset + 1);
        verify(mockOnlineOfflineStateModelNotifier, never()).completed(topic, PARTITION_BAR, barLastOffset  + 1);
      } else {
        verify(mockOnlineOfflineStateModelNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce()).completed(topic, PARTITION_FOO,
            fooLastOffset + 1);
        verify(mockOnlineOfflineStateModelNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce()).completed(topic, PARTITION_BAR,
            barLastOffset  + 1);
        verify(mockLeaderFollowerStateModelNotifier, never()).completed(topic, PARTITION_FOO, fooLastOffset + 1);
        verify(mockLeaderFollowerStateModelNotifier, never()).completed(topic, PARTITION_BAR, barLastOffset  + 1);
      }

      /**
       * It seems Mockito will mess up the verification if there are two functions with the same name:
       * {@link StorageMetadataService#put(String, StoreVersionState)}
       * {@link StorageMetadataService#put(String, int, OffsetRecord)}
       *
       * So if the first function gets invoked, Mockito will try to match the second function this test wanted,
       * which is specified by the following statements, which will cause 'argument mismatch error'.
       * For now, to bypass this issue, this test will try to wait the async consumption to be done before
       * any {@link StorageMetadataService#put} function verification by verifying {@link VeniceNotifier#completed(String, int, long)}
       * first.
       */
      verify(mockStorageMetadataService)
          .put(eq(topic), eq(PARTITION_FOO), eq(getOffsetRecord(fooLastOffset + 1, true)));
      verify(mockStorageMetadataService)
          .put(eq(topic), eq(PARTITION_BAR), eq(getOffsetRecord(barLastOffset + 1, true)));
      verify(mockLogNotifier, atLeastOnce()).started(topic, PARTITION_FOO);
      verify(mockLogNotifier, atLeastOnce()).started(topic, PARTITION_BAR);
      verify(mockLogNotifier, atLeastOnce()).endOfPushReceived(topic, PARTITION_FOO, fooLastOffset);
      verify(mockLogNotifier, atLeastOnce()).endOfPushReceived(topic, PARTITION_BAR, barLastOffset);
      verify(mockPartitionStatusNotifier, atLeastOnce()).started(topic, PARTITION_FOO);
      verify(mockPartitionStatusNotifier, atLeastOnce()).started(topic, PARTITION_BAR);
      verify(mockPartitionStatusNotifier, atLeastOnce()).endOfPushReceived(topic, PARTITION_FOO, fooLastOffset);
      verify(mockPartitionStatusNotifier, atLeastOnce()).endOfPushReceived(topic, PARTITION_BAR, barLastOffset);
    }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testReadyToServePartition(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    runTest(Utils.setOf(PARTITION_FOO),
        () -> {
          Store mockStore = mock(Store.class);
          doReturn(true).when(mockStore).isHybrid();
          doReturn(Optional.of(new VersionImpl("storeName", 1))).when(mockStore).getVersion(1);
          doReturn(storeNameWithoutVersionInfo).when(mockStore).getName();
          doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeNameWithoutVersionInfo);
        },
        () -> {
          verify(mockAbstractStorageEngine, never()).preparePartitionForReading(PARTITION_BAR);
          verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS)).preparePartitionForReading(PARTITION_FOO);
        },
        isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testReadyToServePartitionValidateIngestionSuccess(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());
    Store mockStore = mock(Store.class);
    doReturn(mockStore).when(mockMetadataRepo).getStore(storeNameWithoutVersionInfo);
    doReturn(false).when(mockStore).isHybrid();
    doReturn(storeNameWithoutVersionInfo).when(mockStore).getName();
    mockAbstractStorageEngine.addStoragePartition(PARTITION_FOO);
    AbstractStoragePartition mockPartition = mock(AbstractStoragePartition.class);
    doReturn(mockPartition).when(mockAbstractStorageEngine).getPartitionOrThrow(PARTITION_FOO);
    doReturn(true).when(mockPartition).validateBatchIngestion();
    StoragePartitionConfig storagePartitionConfigFoo = new StoragePartitionConfig(topic, PARTITION_FOO);

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      verify(mockAbstractStorageEngine, never()).preparePartitionForReading(PARTITION_FOO);
    }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testReadyToServePartitionWriteOnly(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());
    Store mockStore = mock(Store.class);

    doReturn(mockStore).when(mockMetadataRepo).getStore(storeNameWithoutVersionInfo);
    doReturn(true).when(mockStore).isHybrid();
    mockAbstractStorageEngine.addStoragePartition(PARTITION_FOO);

    doReturn(storeNameWithoutVersionInfo).when(mockStore).getName();
    StoragePartitionConfig storagePartitionConfigFoo = new StoragePartitionConfig(topic, PARTITION_FOO);
    storagePartitionConfigFoo.setWriteOnlyConfig(true);
    StoragePartitionConfig storagePartitionConfigBar = new StoragePartitionConfig(topic, PARTITION_BAR);

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      verify(mockAbstractStorageEngine, never()).preparePartitionForReading(PARTITION_FOO);
      verify(mockAbstractStorageEngine, never()).preparePartitionForReading(PARTITION_BAR);
    }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testResetPartition(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID).get();

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS))
          .put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(SCHEMA_ID, putValue).serialize()));

      storeIngestionTaskUnderTest.resetPartitionConsumptionOffset(topic, PARTITION_FOO);

      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS).times(2))
          .put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(SCHEMA_ID, putValue).serialize()));
    }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testResetPartitionAfterUnsubscription(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID).get();

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS))
          .put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(SCHEMA_ID, putValue).serialize()));

      storeIngestionTaskUnderTest.unSubscribePartition(topic, PARTITION_FOO);
      doThrow(new UnsubscribedTopicPartitionException(topic, PARTITION_FOO))
          .when(mockLocalKafkaConsumer).resetOffset(topic, PARTITION_FOO);
      // Reset should be able to handle the scenario, when the topic partition has been unsubscribed.
      storeIngestionTaskUnderTest.resetPartitionConsumptionOffset(topic, PARTITION_FOO);
      verify(mockLocalKafkaConsumer, timeout(TEST_TIMEOUT_MS)).unSubscribe(topic, PARTITION_FOO);
      // StoreIngestionTask won't invoke consumer.resetOffset() if it already unsubscribe from that topic/partition
      verify(mockLocalKafkaConsumer, timeout(TEST_TIMEOUT_MS).times(0)).resetOffset(topic, PARTITION_FOO);
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).clearOffset(topic, PARTITION_FOO);
    }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  /**
   * In this test, partition FOO will complete successfully, but partition BAR will be missing a record.
   *
   * The {@link VeniceNotifier} should see the completion and error reported for the appropriate partitions.
   */
  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testDetectionOfMissingRecord(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    long fooLastOffset = getOffset(localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    long barOffsetToSkip = getOffset(localVeniceWriter.put(putKeyBar, putValue, SCHEMA_ID));
    localVeniceWriter.put(putKeyBar, putValue, SCHEMA_ID);
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    PollStrategy pollStrategy = new FilteringPollStrategy(new RandomPollStrategy(),
        Utils.setOf(new Pair(new TopicPartition(topic, PARTITION_BAR), barOffsetToSkip)));

    runTest(pollStrategy, Utils.setOf(PARTITION_FOO, PARTITION_BAR), () -> {}, () -> {
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce()).endOfPushReceived(topic, PARTITION_FOO, fooLastOffset);
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).error(
          eq(topic), eq(PARTITION_BAR), argThat(new NonEmptyStringMatcher()),
          argThat(new ExceptionClassMatcher(MissingDataException.class)));

      // After we verified that completed() and error() are called, the rest should be guaranteed to be finished, so no need for timeouts

      verify(mockLogNotifier, atLeastOnce()).started(topic, PARTITION_FOO);
      verify(mockLogNotifier, atLeastOnce()).started(topic, PARTITION_BAR);
    }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  /**
   * In this test, partition FOO will complete normally, but partition BAR will contain a duplicate record. The
   * {@link VeniceNotifier} should see the completion for both partitions.
   */
  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testSkippingOfDuplicateRecord(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    long fooLastOffset = getOffset(localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    long barOffsetToDupe = getOffset(localVeniceWriter.put(putKeyBar, putValue, SCHEMA_ID));
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    PollStrategy pollStrategy = new DuplicatingPollStrategy(new RandomPollStrategy(),
        Utils.mutableSetOf(new Pair(new TopicPartition(topic, PARTITION_BAR), barOffsetToDupe)));

    runTest(pollStrategy, Utils.setOf(PARTITION_FOO, PARTITION_BAR), () -> {}, () -> {
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce()).endOfPushReceived(topic, PARTITION_FOO, fooLastOffset);
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce()).endOfPushReceived(topic, PARTITION_BAR, barOffsetToDupe);
      verify(mockLogNotifier, after(TEST_TIMEOUT_MS).never()).endOfPushReceived(topic, PARTITION_BAR, barOffsetToDupe + 1);

      // After we verified that completed() is called, the rest should be guaranteed to be finished, so no need for timeouts

      verify(mockLogNotifier, atLeastOnce()).started(topic, PARTITION_FOO);
      verify(mockLogNotifier, atLeastOnce()).started(topic, PARTITION_BAR);
    }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testThrottling(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);
    localVeniceWriter.delete(deleteKeyFoo, null);

    runTest(new RandomPollStrategy(1), Utils.setOf(PARTITION_FOO), () -> {}, () -> {
      // START_OF_SEGMENT, START_OF_PUSH, PUT, DELETE
      verify(mockBandwidthThrottler, timeout(TEST_TIMEOUT_MS).times(4)).maybeThrottle(anyDouble());
      verify(mockRecordsThrottler, timeout(TEST_TIMEOUT_MS).times(4)).maybeThrottle(1);
    }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  /**
   * This test crafts a couple of invalid message types, and expects the {@link StoreIngestionTask} to fail fast. The
   * message in {@link #PARTITION_FOO} will receive a bad message type, whereas the message in {@link #PARTITION_BAR}
   * will receive a bad control message type.
   */
  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testBadMessageTypesFailFast(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    int badMessageTypeId = 99; // Venice got 99 problems, but a bad message type ain't one.

    // Dear future maintainer,
    //
    // I sincerely hope Venice never ends up with 99 distinct message types, but I'll
    // make sure anyway. If you end up breaking this check, holla at me, I'm curious
    // to know how you managed to end up in this mess ;)
    //
    // - @felixgv on the twitters

    try {
      MessageType.valueOf(badMessageTypeId);
      fail("The message type " + badMessageTypeId + " is valid. "
          + "This test needs to be updated in order to send an invalid message type...");
    } catch (VeniceMessageException e) {
      // Good
    }

    try {
      ControlMessageType.valueOf(badMessageTypeId);
      fail("The control message type " + badMessageTypeId + " is valid. "
          + "This test needs to be updated in order to send an invalid control message type...");
    } catch (VeniceMessageException e) {
      // Good
    }

    localVeniceWriter = getVeniceWriter(() -> new TransformingProducer(new MockInMemoryProducer(inMemoryLocalKafkaBroker),
        (topicName, key, value, partition) -> {
          KafkaMessageEnvelope transformedMessageEnvelope = value;

          switch (partition) {
            case PARTITION_FOO:
              transformedMessageEnvelope.messageType = badMessageTypeId;
              break;
            case PARTITION_BAR:
              if (MessageType.valueOf(transformedMessageEnvelope) == MessageType.CONTROL_MESSAGE) {
                ControlMessage transformedControlMessage = (ControlMessage) transformedMessageEnvelope.payloadUnion;
                if (ControlMessageType.valueOf(transformedControlMessage) == ControlMessageType.START_OF_SEGMENT) {
                  transformedControlMessage.controlMessageType = badMessageTypeId;
                  transformedMessageEnvelope.payloadUnion = transformedControlMessage;
                }
              }
              break;
          }

          return new TransformingProducer.SendMessageParameters(topic, key, transformedMessageEnvelope, partition);
        }));
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);
    localVeniceWriter.put(putKeyBar, putValue, SCHEMA_ID);

    runTest(Utils.setOf(PARTITION_FOO, PARTITION_BAR), () -> {
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).error(
          eq(topic), eq(PARTITION_FOO), argThat(new NonEmptyStringMatcher()),
          argThat(new ExceptionClassMatcher(VeniceException.class)));

      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).error(
          eq(topic), eq(PARTITION_BAR), argThat(new NonEmptyStringMatcher()),
          argThat(new ExceptionClassMatcher(VeniceException.class)));
    }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  /**
   * In this test, {@link #PARTITION_BAR} will finish a regular push, and then get some more messages afterwards,
   * including a corrupt message followed by a good one. We expect the Notifier to not report any errors after the
   * EOP.
   */
  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testCorruptMessagesDoNotFailFastAfterEOP(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    VeniceWriter veniceWriterForDataDuringPush = getVeniceWriter(() -> new MockInMemoryProducer(
        inMemoryLocalKafkaBroker));
    VeniceWriter veniceWriterForDataAfterPush = getCorruptedVeniceWriter(putValueToCorrupt, inMemoryLocalKafkaBroker);

    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    long lastOffsetBeforeEOP = getOffset(veniceWriterForDataDuringPush.put(putKeyBar, putValue, SCHEMA_ID));
    veniceWriterForDataDuringPush.close();
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    // We simulate the version swap...
    isCurrentVersion = () -> true;

    // After the end of push, we simulate a nearline writer, which somehow pushes corrupt data.
    getOffset(veniceWriterForDataAfterPush.put(putKeyBar, putValueToCorrupt, SCHEMA_ID));
    long lastOffset = getOffset(veniceWriterForDataAfterPush.put(putKeyBar, putValue, SCHEMA_ID));
    veniceWriterForDataAfterPush.close();

    logger.info("lastOffsetBeforeEOP: " + lastOffsetBeforeEOP + ", lastOffset: " + lastOffset);

    try {
      runTest(Utils.setOf(PARTITION_BAR), () -> {

        TestUtils.waitForNonDeterministicCompletion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, () -> {
          for (Object[] args : mockNotifierProgress) {
            if (args[0].equals(topic) && args[1].equals(PARTITION_BAR) && ((long) args[2]) >= lastOffsetBeforeEOP) {
              return true;
            }
          }
          return false;
        });
        TestUtils.waitForNonDeterministicCompletion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, () -> {
          for (Object[] args : mockNotifierCompleted) {
            if (args[0].equals(topic) && args[1].equals(PARTITION_BAR) && ((long) args[2]) > lastOffsetBeforeEOP) {
              return true;
            }
          }
          return false;
        });
        for (Object[] args : mockNotifierError) {
          Assert.assertFalse(
              args[0].equals(topic)
                  && args[1].equals(PARTITION_BAR)
                  && ((String)args[2]).length() > 0
                  && args[3] instanceof CorruptDataException);
        }

      }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
    } catch (VerifyError e) {
      StringBuilder msg = new StringBuilder();
      ClassLoader cl = ClassLoader.getSystemClassLoader();
      URL[] urls = ((URLClassLoader)cl).getURLs();
      msg.append("VerifyError, possibly from junit or mockito version conflict. \nPrinting junit on classpath:\n");
      Arrays.asList(urls).stream().filter(url -> url.getFile().contains("junit")).forEach(url -> msg.append(url + "\n"));
      msg.append("Printing mockito on classpath:\n");
      Arrays.asList(urls).stream().filter(url -> url.getFile().contains("mockito")).forEach(url -> msg.append(url + "\n"));
      throw new VeniceException(msg.toString(), e);
    }
  }

  /**
   * In this test, {@link #PARTITION_FOO} will finish a regular push, and then get some more messages afterwards,
   * including a corrupt message followed by a missing message and a good one.
   * We expect the Notifier to not report any errors after the EOP.
   */
  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testDIVErrorMessagesNotFailFastAfterEOP(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    VeniceWriter veniceWriterCorrupted = getCorruptedVeniceWriter(putValueToCorrupt, inMemoryLocalKafkaBroker);

    // do a batch push
    veniceWriterCorrupted.broadcastStartOfPush(new HashMap<>());
    long lastOffsetBeforeEOP = getOffset(veniceWriterCorrupted.put(putKeyBar, putValue, SCHEMA_ID));
    veniceWriterCorrupted.broadcastEndOfPush(new HashMap<>());

    // simulate the version swap.
    isCurrentVersion = () -> true;

    // After the end of push, the venice writer continue puts a corrupt data and end the segment.
    getOffset(veniceWriterCorrupted.put(putKeyFoo, putValueToCorrupt, SCHEMA_ID));
    veniceWriterCorrupted.endSegment(PARTITION_FOO, true);

    // a missing msg
    long fooOffsetToSkip = getOffset(veniceWriterCorrupted.put(putKeyFoo, putValue, SCHEMA_ID));
    // a normal msg
    long lastOffset = getOffset(veniceWriterCorrupted.put(putKeyFoo2, putValue, SCHEMA_ID));
    veniceWriterCorrupted.close();

    PollStrategy pollStrategy = new FilteringPollStrategy(new RandomPollStrategy(),
        Utils.setOf(new Pair(new TopicPartition(topic, PARTITION_FOO), fooOffsetToSkip)));

    logger.info("lastOffsetBeforeEOP: " + lastOffsetBeforeEOP + ", lastOffset: " + lastOffset);

    runTest(pollStrategy, Utils.setOf(PARTITION_FOO), () -> {}, () -> {
      for (Object[] args : mockNotifierError) {
        Assert.assertFalse(
            args[0].equals(topic)
                && args[1].equals(PARTITION_FOO)
                && ((String)args[2]).length() > 0
                && args[3] instanceof FatalDataValidationException);
      }
    }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }


  /**
   * In this test, the {@link #PARTITION_FOO} will receive a well-formed message, while the {@link #PARTITION_BAR} will
   * receive a corrupt message. We expect the Notifier to report as such.
   * <p>
   * N.B.: There was an edge case where this test was flaky. The edge case is now fixed, but the invocationCount of 100
   * should ensure that if this test is ever made flaky again, it will be detected right away. The skipFailedInvocations
   * annotation parameter makes the test skip any invocation after the first failure.
   */
  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class, invocationCount = 100, skipFailedInvocations = true)
  public void testCorruptMessagesFailFast(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    VeniceWriter veniceWriterForData = getCorruptedVeniceWriter(putValueToCorrupt, inMemoryLocalKafkaBroker);

    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    long fooLastOffset = getOffset(veniceWriterForData.put(putKeyFoo, putValue, SCHEMA_ID));
    getOffset(veniceWriterForData.put(putKeyBar, putValueToCorrupt, SCHEMA_ID));
    veniceWriterForData.close();
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    runTest(Utils.setOf(PARTITION_FOO, PARTITION_BAR), () -> {
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS))
          .completed(eq(topic), eq(PARTITION_FOO), LongEqualOrGreaterThanMatcher.get(fooLastOffset));

      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).error(
          eq(topic), eq(PARTITION_BAR), argThat(new NonEmptyStringMatcher()),
          argThat(new ExceptionClassMatcher(CorruptDataException.class)));

      /**
       * Let's make sure that {@link PARTITION_BAR} never sends a completion notification.
       *
       * This used to be flaky, because the {@link StoreIngestionTask} occasionally sent
       * a completion notification for partitions where it had already detected an error.
       * Now, the {@link StoreIngestionTask} keeps track of the partitions that had error
       * and avoids sending completion notifications for those. The high invocationCount on
       * this test is to detect this edge case.
       */
      verify(mockLogNotifier, never()).completed(eq(topic), eq(PARTITION_BAR), anyLong());
    }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testSubscribeCompletedPartition(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    final int offset = 100;
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    runTest(Utils.setOf(PARTITION_FOO),
        () -> doReturn(getOffsetRecord(offset, true)).when(mockStorageMetadataService).getLastOffset(topic, PARTITION_FOO),
        () -> {
          verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).completed(topic, PARTITION_FOO, offset);
        },
        isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled
    );
  }

  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testSubscribeCompletedPartitionUnsubscribe(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    final int offset = 100;
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    Map<String, Object> extraServerProperties = new HashMap<>();
    extraServerProperties.put(SERVER_SHARED_CONSUMER_POOL_ENABLED, true);
    extraServerProperties.put(SERVER_UNSUB_AFTER_BATCHPUSH, true);

    runTest(Utils.setOf(PARTITION_FOO),
        () -> {
          doReturn(true).when(mockLocalKafkaConsumer).hasSubscription(anySet());
          Store mockStore = mock(Store.class);
          doReturn(1).when(mockStore).getCurrentVersion();
          doReturn(Optional.of(new VersionImpl("storeName", 1))).when(mockStore).getVersion(1);
          doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeNameWithoutVersionInfo);
          doReturn(getOffsetRecord(offset, true)).when(mockStorageMetadataService).getLastOffset(topic, PARTITION_FOO);
        },
        () -> {
          verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).completed(topic, PARTITION_FOO, offset);
          verify(mockLocalKafkaConsumer).batchUnsubscribe(Collections.singleton(new TopicPartition(topic, PARTITION_FOO)));
          verify(mockLocalKafkaConsumer, never()).unSubscribe(topic, PARTITION_BAR);
        },
        isLeaderFollowerModelEnabled,
        isActiveActiveReplicationEnabled,
        extraServerProperties
    );
  }

  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testCompleteCalledWhenUnsubscribeAfterBatchPushDisabled(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    final int offset = 10;
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());

    runTest(Utils.setOf(PARTITION_FOO),
        () -> {
          Store mockStore = mock(Store.class);
          storeIngestionTaskUnderTest.unSubscribePartition(topic, PARTITION_FOO);
          doReturn(1).when(mockStore).getCurrentVersion();
          doReturn(Optional.of(new VersionImpl("storeName", 1, Version.numberBasedDummyPushId(1)))).when(mockStore).getVersion(1);
          doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeNameWithoutVersionInfo);
          doReturn(getOffsetRecord(offset, true)).when(mockStorageMetadataService).getLastOffset(topic, PARTITION_FOO);
        },
        () -> {
          verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).completed(topic, PARTITION_FOO, offset);
          waitForNonDeterministicCompletion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS,
              () -> !storeIngestionTaskUnderTest.isRunning());
        },
        isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled
    );
  }

  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testUnsubscribeConsumption(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).started(topic, PARTITION_FOO);
      //Start of push has already been consumed. Stop consumption
      storeIngestionTaskUnderTest.unSubscribePartition(topic, PARTITION_FOO);
      waitForNonDeterministicCompletion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS,
          () -> storeIngestionTaskUnderTest.isRunning() == false);
    }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testKillConsumption(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    final Thread writingThread = new Thread(() -> {
      while (true) {
        localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);
        localVeniceWriter.put(putKeyBar, putValue, SCHEMA_ID);
        try {
          TimeUnit.MILLISECONDS.sleep(READ_CYCLE_DELAY_MS);
        } catch (InterruptedException e) {
          break;
        }
      }
    });

    try {
      runTest(Utils.setOf(PARTITION_FOO, PARTITION_BAR), () -> {
        localVeniceWriter.broadcastStartOfPush(new HashMap<>());
        writingThread.start();
      }, () -> {
        verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).started(topic, PARTITION_FOO);
        verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).started(topic, PARTITION_BAR);

        //Start of push has already been consumed. Stop consumption
        storeIngestionTaskUnderTest.kill();
        // task should report an error to notifier that it's killed.
        verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).error(
            eq(topic), eq(PARTITION_FOO), argThat(new NonEmptyStringMatcher()),
            argThat(new ExceptionClassMatcher(VeniceIngestionTaskKilledException.class)));
        verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).error(
            eq(topic), eq(PARTITION_BAR), argThat(new NonEmptyStringMatcher()),
            argThat(new ExceptionClassMatcher(VeniceIngestionTaskKilledException.class)));

        waitForNonDeterministicCompletion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS,
            () -> storeIngestionTaskUnderTest.isRunning() == false);
      }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
    } finally {
      writingThread.interrupt();
    }
  }

  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testKillActionPriority(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    runTest(Utils.setOf(PARTITION_FOO), () -> {
      localVeniceWriter.broadcastStartOfPush(new HashMap<>());
      localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);
      // Add a reset consumer action
      storeIngestionTaskUnderTest.resetPartitionConsumptionOffset(topic, PARTITION_FOO);
      // Add a kill consumer action in higher priority than subscribe and reset.
      storeIngestionTaskUnderTest.kill();
    }, () -> {
      // verify subscribe has not been processed. Because consumption task should process kill action at first
      verify(mockStorageMetadataService, after(TEST_TIMEOUT_MS).never()).getLastOffset(topic, PARTITION_FOO);
      waitForNonDeterministicCompletion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS,
          () -> storeIngestionTaskUnderTest.getConsumers() != null);
      Collection<KafkaConsumerWrapper> consumers = (storeIngestionTaskUnderTest.getConsumers());
      /**
       * Consumers are constructed lazily; if the store ingestion task is killed before it tries to subscribe to any
       * topics, there is no consumer.
       */
      assertEquals(consumers.size(), 0,
          "subscribe should not be processed in this consumer.");

      // Verify offset has not been processed. Because consumption task should process kill action at first.
      // offSetManager.clearOffset should only be invoked one time during clean up after killing this task.
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).clearOffset(topic, PARTITION_FOO);

      waitForNonDeterministicCompletion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS,
          () -> storeIngestionTaskUnderTest.isRunning() == false);
    }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  private byte[] getNumberedKey(int number) {
    return ByteBuffer.allocate(putKeyFoo.length + Integer.BYTES).put(putKeyFoo).putInt(number).array();
  }
  private byte[] getNumberedValue(int number) {
    return ByteBuffer.allocate(putValue.length + Integer.BYTES).put(putValue).putInt(number).array();
  }

  @Test(dataProvider = "Three-True-and-False", invocationCount = 3, skipFailedInvocations = true, dataProviderClass = DataProviderUtils.class)
  public void testDataValidationCheckPointing(boolean sortedInput, boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    final Map<Integer, Long> maxOffsetPerPartition = new HashMap<>();
    final Map<Pair<Integer, ByteArray>, ByteArray> pushedRecords = new HashMap<>();
    final int totalNumberOfMessages = 1000;
    final int totalNumberOfConsumptionRestarts = 10;

    storeVersionStateSupplier = getStoreVersionStateSupplier(sortedInput);
    localVeniceWriter.broadcastStartOfPush(sortedInput, new HashMap<>());
    for (int i = 0; i < totalNumberOfMessages; i++) {
      byte[] key = getNumberedKey(i);
      byte[] value = getNumberedValue(i);

      RecordMetadata recordMetadata = (RecordMetadata) localVeniceWriter.put(key, value, SCHEMA_ID).get();

      maxOffsetPerPartition.put(recordMetadata.partition(), recordMetadata.offset());
      pushedRecords.put(new Pair(recordMetadata.partition(), new ByteArray(key)), new ByteArray(value));
    }
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    // Basic sanity checks
    assertEquals(pushedRecords.size(), totalNumberOfMessages, "We did not produce as many unique records as we expected!");
    assertFalse(maxOffsetPerPartition.isEmpty(), "There should be at least one partition getting anything published into it!");

    Set<Integer> relevantPartitions = Utils.setOf(PARTITION_FOO);

    // This doesn't really need to be atomic, but it does need to be final, and int/Integer cannot be mutated.
    final AtomicInteger messagesConsumedSoFar = new AtomicInteger(0);

    PollStrategy pollStrategy = new BlockingObserverPollStrategy(
        new RandomPollStrategy(false),
        topicPartitionOffsetRecordPair -> {
          if (null == topicPartitionOffsetRecordPair || null == topicPartitionOffsetRecordPair.getSecond()) {
            logger.info("Received null OffsetRecord!");
          } else if (messagesConsumedSoFar.incrementAndGet() % (totalNumberOfMessages / totalNumberOfConsumptionRestarts) == 0) {
            logger.info("Restarting consumer after consuming " + messagesConsumedSoFar.get() + " messages so far.");
            relevantPartitions.stream().forEach(partition ->
                storeIngestionTaskUnderTest.unSubscribePartition(topic, partition));
            relevantPartitions.stream().forEach(partition ->
                storeIngestionTaskUnderTest.subscribePartition(topic, partition));
          } else {
            logger.info("TopicPartition: " + topicPartitionOffsetRecordPair.getFirst() +
                ", Offset: " + topicPartitionOffsetRecordPair.getSecond());
          }
        }
    );

    runTest(pollStrategy, relevantPartitions, () -> {}, () -> {
      // Verify that all partitions reported success.
      maxOffsetPerPartition.entrySet().stream().filter(entry -> relevantPartitions.contains(entry.getKey())).forEach(entry -> {
            int partition = entry.getKey();
            long offset = entry.getValue();
            logger.info("Verifying completed was called for partition " + partition + " and offset " + offset + " or greater.");
            verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce()).completed(eq(topic), eq(partition), LongEqualOrGreaterThanMatcher.get(offset));
          }
      );

      // After this, all asynchronous processing should be finished, so there's no need for time outs anymore.

      // Verify that no partitions reported errors.
      relevantPartitions.stream().forEach(partition ->
          verify(mockLogNotifier, never()).error(eq(topic), eq(partition), anyString(), any()));

      // Verify that we really unsubscribed and re-subscribed.
      relevantPartitions.stream().forEach(partition -> {
        verify(mockLocalKafkaConsumer, atLeast(totalNumberOfConsumptionRestarts + 1)).subscribe(eq(topic), eq(partition), anyLong());
        verify(mockLocalKafkaConsumer, atLeast(totalNumberOfConsumptionRestarts)).unSubscribe(topic, partition);

        if (sortedInput) {
          // Check database mode switches from deferred-write to transactional after EOP control message
          StoragePartitionConfig deferredWritePartitionConfig = new StoragePartitionConfig(topic, partition);
          deferredWritePartitionConfig.setDeferredWrite(true);
          // SOP control message and restart
          verify(mockAbstractStorageEngine, atLeast(1))
              .beginBatchWrite(eq(deferredWritePartitionConfig), any(), eq(Optional.empty()));
          StoragePartitionConfig transactionalPartitionConfig = new StoragePartitionConfig(topic, partition);
          // only happen after EOP control message
          verify(mockAbstractStorageEngine, times(1))
              .endBatchWrite(transactionalPartitionConfig);
        }
      });

      // Verify that the storage engine got hit with every record.
      verify(mockAbstractStorageEngine, atLeast(pushedRecords.size())).put(anyInt(), any(), any(ByteBuffer.class));

      // Verify that every record hit the storage engine
      pushedRecords.entrySet().stream().forEach(entry -> {
        int partition = entry.getKey().getFirst();
        byte[] key = entry.getKey().getSecond().get();
        byte[] value = ValueRecord.create(SCHEMA_ID, entry.getValue().get()).serialize();
        verify(mockAbstractStorageEngine, atLeastOnce()).put(partition, key, ByteBuffer.wrap(value));
      });

      /**
       * Verify that for batch push part, {@link PartitionConsumptionState#latestProcessedUpstreamRTOffsetMap} should
       * never be updated
       */
      relevantPartitions.stream().forEach(partition -> {
        Assert.assertTrue(storeIngestionTaskUnderTest.getPartitionConsumptionState(partition).isPresent());
        PartitionConsumptionState pcs = storeIngestionTaskUnderTest.getPartitionConsumptionState(partition).get();
        Assert.assertTrue(pcs.getLatestProcessedUpstreamRTOffsetMap().isEmpty());
      });
    }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testKillAfterPartitionIsCompleted(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled)
      throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    long fooLastOffset = getOffset(localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      verify(mockLogNotifier, after(TEST_TIMEOUT_MS).never()).error(eq(topic), eq(PARTITION_FOO), anyString(), any());
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce()).started(topic, PARTITION_FOO);

      storeIngestionTaskUnderTest.kill();
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce()).endOfPushReceived(topic, PARTITION_FOO, fooLastOffset);
    }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testNeverReportProgressBeforeStart(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled)
      throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    // Read one message for each poll.
    runTest(new RandomPollStrategy(1), Utils.setOf(PARTITION_FOO), () -> {}, () -> {
      verify(mockLogNotifier, after(TEST_TIMEOUT_MS).never()).progress(topic, PARTITION_FOO, 0);
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce()).started(topic, PARTITION_FOO);
      // The current behavior is only to sync offset/report progress after processing a pre-configured amount
      // of messages in bytes, since control message is being counted as 0 bytes (no data persisted in disk),
      // then no progress will be reported during start, but only for processed messages.
      verify(mockLogNotifier, after(TEST_TIMEOUT_MS).never()).progress(any(), anyInt(), anyInt());
    }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testOffsetPersistent(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled)
      throws Exception {
    // Do not persist every message.
    List<Long> offsets = new ArrayList<>();
    for (int i = 0; i < PARTITION_COUNT; i++) {
      offsets.add(5l);
    }
    databaseSyncBytesIntervalForTransactionalMode = 1000;
    try {
      localVeniceWriter.broadcastStartOfPush(new HashMap<>());
      localVeniceWriter.broadcastEndOfPush(new HashMap<>());
      localVeniceWriter.broadcastStartOfBufferReplay(offsets,"t", topic, new HashMap<>());
      /**
       * Persist for every control message except START_OF_SEGMENT and END_OF_SEGMENT:
       * START_OF_PUSH, END_OF_PUSH, START_OF_BUFFER_REPLAY
       */
      runHybridTest(
          Utils.setOf(PARTITION_FOO),
          () -> {
            verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS).times(3)).put(eq(topic), eq(PARTITION_FOO), any());
          },
          isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
    }finally {
      databaseSyncBytesIntervalForTransactionalMode = 1;
    }

  }

  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testVeniceMessagesProcessingWithSortedInput(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    storeVersionStateSupplier = getStoreVersionStateSupplier(true);
    localVeniceWriter.broadcastStartOfPush(true, new HashMap<>());
    RecordMetadata putMetadata = (RecordMetadata) localVeniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID).get();
    RecordMetadata deleteMetadata = (RecordMetadata) localVeniceWriter.delete(deleteKeyFoo, null).get();
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      // Verify it retrieves the offset from the Offset Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).getLastOffset(topic, PARTITION_FOO);

      verifyPutAndDelete(1, isActiveActiveReplicationEnabled, true);

      // Verify it commits the offset to Offset Manager after receiving EOP control message
      OffsetRecord expectedOffsetRecordForDeleteMessage = getOffsetRecord(deleteMetadata.offset() + 1, true);
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS))
          .put(topic, PARTITION_FOO, expectedOffsetRecordForDeleteMessage);
      // Deferred write is not going to commit offset for every message, but will commit offset for every control message
      // The following verification is for START_OF_PUSH control message
      verify(mockStorageMetadataService, times(1))
          .put(topic, PARTITION_FOO, getOffsetRecord(putMetadata.offset() - 1));
      // Check database mode switches from deferred-write to transactional after EOP control message
      StoragePartitionConfig deferredWritePartitionConfig = new StoragePartitionConfig(topic, PARTITION_FOO);
      deferredWritePartitionConfig.setDeferredWrite(true);
      verify(mockAbstractStorageEngine, times(1))
          .beginBatchWrite(eq(deferredWritePartitionConfig), any(), eq(Optional.empty()));
      StoragePartitionConfig transactionalPartitionConfig = new StoragePartitionConfig(topic, PARTITION_FOO);
      verify(mockAbstractStorageEngine, times(1))
          .endBatchWrite(transactionalPartitionConfig);
    }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testVeniceMessagesProcessingWithSortedInputVerifyChecksum(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    databaseChecksumVerificationEnabled = true;
    doReturn(false).when(rocksDBServerConfig).isRocksDBPlainTableFormatEnabled();
    storeVersionStateSupplier = getStoreVersionStateSupplier(true);
    localVeniceWriter.broadcastStartOfPush(true, new HashMap<>());
    RecordMetadata putMetadata = (RecordMetadata) localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID).get();
    //intentionally not sending the EOP so that expectedSSTFileChecksum calculation does not get reset.
    //veniceWriter.broadcastEndOfPush(new HashMap<>());

    Optional<CheckSum> checksum = CheckSum.getInstance(CheckSumType.MD5);
    checksum.get().update(putKeyFoo);
    checksum.get().update(SCHEMA_ID);
    checksum.get().update(putValue);

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      // Verify it retrieves the offset from the Offset Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).getLastOffset(topic, PARTITION_FOO);

      // Verify StorageEngine#put is invoked only once and with appropriate key & value.
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS))
          .put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(SCHEMA_ID, putValue).serialize()));


      StoragePartitionConfig deferredWritePartitionConfig = new StoragePartitionConfig(topic, PARTITION_FOO);
      deferredWritePartitionConfig.setDeferredWrite(true);
      ArgumentCaptor<Optional<Supplier<byte[]>>> checksumCaptor = ArgumentCaptor.forClass(Optional.class);

      //verify the checksum matches.
      verify(mockAbstractStorageEngine, times(1)).beginBatchWrite(eq(deferredWritePartitionConfig), any(),
          checksumCaptor.capture());
      Optional<Supplier<byte[]>> checksumSupplier = checksumCaptor.getValue();
      Assert.assertTrue(checksumSupplier.isPresent());
      Assert.assertTrue(Arrays.equals(checksumSupplier.get().get(), checksum.get().getCheckSum()));
    }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testDelayedTransitionToOnlineInHybridMode(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    final long MESSAGES_BEFORE_EOP = 100;
    final long MESSAGES_AFTER_EOP = 100;

    // This does not entirely make sense, but we can do better when we have a real integration test which includes buffer replay
    // TODO: We now have a real integration test which includes buffer replay, maybe fix up this test?
    final long TOTAL_MESSAGES_PER_PARTITION = (MESSAGES_BEFORE_EOP + MESSAGES_AFTER_EOP) / ALL_PARTITIONS.size();
    when(mockTopicManager.getPartitionLatestOffset(anyString(), anyInt())).thenReturn(TOTAL_MESSAGES_PER_PARTITION);

    mockStorageMetadataService = new InMemoryStorageMetadataService();
    hybridStoreConfig = Optional.of(new HybridStoreConfigImpl(10, 20,
        HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD, DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP));
    runTest(ALL_PARTITIONS, () -> {
          localVeniceWriter.broadcastStartOfPush(Collections.emptyMap());
          for (int i = 0; i < MESSAGES_BEFORE_EOP; i++) {
            try {
              localVeniceWriter.put(getNumberedKey(i), getNumberedValue(i), SCHEMA_ID).get();
            } catch (InterruptedException | ExecutionException e) {
              throw new VeniceException(e);
            }
          }
          localVeniceWriter.broadcastEndOfPush(Collections.emptyMap());

        }, () -> {
          verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeast(ALL_PARTITIONS.size())).started(eq(topic), anyInt());
          verify(mockLogNotifier, never()).completed(anyString(), anyInt(), anyLong());

          if (isLeaderFollowerModelEnabled) {
            localVeniceWriter.broadcastTopicSwitch(Collections.singletonList(inMemoryLocalKafkaBroker.getKafkaBootstrapServer()),
                Version.composeRealTimeTopic(storeNameWithoutVersionInfo),
                System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(10),
                Collections.emptyMap());
          } else {
            List<Long> offsets = new ArrayList<>(PARTITION_COUNT);
            for (int i = 0; i < PARTITION_COUNT; i++) {
              offsets.add(5L);
            }
            localVeniceWriter.broadcastStartOfBufferReplay(offsets, "source K cluster", "source K topic", new HashMap<>());
          }

          for (int i = 0; i < MESSAGES_AFTER_EOP; i++) {
            try {
              localVeniceWriter.put(getNumberedKey(i), getNumberedValue(i), SCHEMA_ID).get();
            } catch (InterruptedException | ExecutionException e) {
              throw new VeniceException(e);
            }
          }

          verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeast(ALL_PARTITIONS.size())).completed(anyString(), anyInt(), anyLong());
        },
        isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled
    );
  }

  /**
   * This test writes a message to Kafka then creates a StoreIngestionTask (and StoreBufferDrainer)  It also passes a DiskUsage
   * object to the StoreIngestionTask that always reports disk full.  This means when the StoreBufferDrainer tries to persist
   * the record, it will receive a disk full error.  This test checks for that disk full error on the Notifier object.
   * @throws Exception
   */
  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void StoreIngestionTaskRespectsDiskUsage(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID);
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());
    DiskUsage diskFullUsage = mock(DiskUsage.class);
    doReturn(true).when(diskFullUsage).isDiskFull(anyLong());
    doReturn("mock disk full disk usage").when(diskFullUsage).getDiskStatus();
    doReturn(true).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);

    runTest(new RandomPollStrategy(), Utils.setOf(PARTITION_FOO), () -> {
    }, () -> {
      waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {

        // If the partition already got EndOfPushReceived, then all errors will be suppressed and not reported.
        // The speed for a partition to get EOP is non-deterministic, adds the if check here to make this test not flaky.
        if (mockNotifierEOPReceived.isEmpty()) {
          Assert.assertFalse(mockNotifierError.isEmpty(), "Disk Usage should have triggered an ingestion error");
          String errorMessages = mockNotifierError.stream().map(o -> ((Exception) o[3]).getMessage()) //elements in object array are 0:store name (String), 1: partition (int), 2: message (String), 3: cause (Exception)
              .collect(Collectors.joining());
          Assert.assertTrue(errorMessages.contains("Disk is full"),
              "Expecting disk full error, found following error messages instead: " + errorMessages);
        }
      });
    }, Optional.empty(), Optional.of(diskFullUsage), isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testIncrementalPush(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    storeVersionStateSupplier = getStoreVersionStateSupplier(true);
    localVeniceWriter.broadcastStartOfPush(true, new HashMap<>());
    long fooOffset = getOffset(localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    String version = String.valueOf(System.currentTimeMillis());
    localVeniceWriter.broadcastStartOfIncrementalPush(version, new HashMap<>());
    long fooNewOffset = getOffset(localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    localVeniceWriter.broadcastEndOfIncrementalPush(version, new HashMap<>());
    //Records order are: StartOfSeg, StartOfPush, data, EndOfPush, EndOfSeg, StartOfSeg, StartOfIncrementalPush
    //data, EndOfIncrementalPush

    runTest(new RandomPollStrategy(), Utils.setOf(PARTITION_FOO), () -> {}, () -> {
      waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        //sync the offset when receiving EndOfPush
        verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS).atLeastOnce())
            .put(eq(topic), eq(PARTITION_FOO), eq(getOffsetRecord(fooOffset + 1, true)));
        //sync the offset when receiving StartOfIncrementalPush and EndOfIncrementalPush
        verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS).atLeastOnce())
            .put(eq(topic), eq(PARTITION_FOO), eq(getOffsetRecord(fooNewOffset - 1, true, version)));
        verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS).atLeastOnce())
            .put(eq(topic), eq(PARTITION_FOO), eq(getOffsetRecord(fooNewOffset + 1, true, version)));

        verify(mockLogNotifier, atLeastOnce()).started(topic, PARTITION_FOO);

        //since notifier reporting happens before offset update, it actually reports previous offsets
        verify(mockLogNotifier, atLeastOnce()).endOfPushReceived(topic, PARTITION_FOO, fooOffset);
        verify(mockLogNotifier, atLeastOnce()).endOfIncrementalPushReceived(eq(topic), eq(PARTITION_FOO), eq(fooNewOffset), anyLong(), eq(version));
      });
    }, Optional.empty(), true, Optional.empty(), isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled, 1, Collections.emptyMap());
  }


  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testCacheWarming(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    storeVersionStateSupplier = getStoreVersionStateSupplier(true);
    localVeniceWriter.broadcastStartOfPush(true, new HashMap<>());
    long fooOffset = getOffset(localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    Map<String, Object> extraServerProperties = new HashMap<>();
    extraServerProperties.put(SERVER_CACHE_WARMING_STORE_LIST, storeNameWithoutVersionInfo);
    extraServerProperties.put(SERVER_CACHE_WARMING_BEFORE_READY_TO_SERVE_ENABLED, true);

    //Records order are: StartOfSeg, StartOfPush, data, EndOfPush, EndOfSeg
    runTest(new RandomPollStrategy(), Utils.setOf(PARTITION_FOO),
        () -> {},
        () -> waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          //sync the offset when receiving EndOfPush
          verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS).atLeastOnce())
              .put(eq(topic), eq(PARTITION_FOO), eq(getOffsetRecord(fooOffset + 1, true)));

          verify(mockLogNotifier, atLeastOnce()).started(topic, PARTITION_FOO);
          //since notifier reporting happens before offset update, it actually reports previous offsets
          verify(mockLogNotifier, atLeastOnce()).endOfPushReceived(topic, PARTITION_FOO, fooOffset);
          // Since the completion report will be async, the completed offset could be `END_OF_PUSH` or `END_OF_SEGMENT` for batch push job.
          verify(mockLogNotifier).completed(eq(topic), eq(PARTITION_FOO),
              longThat(completionOffset ->  (completionOffset == fooOffset + 1) || (completionOffset == fooOffset + 2))
          );
          verify(mockAbstractStorageEngine).warmUpStoragePartition(PARTITION_FOO);
        }), Optional.empty(), false, Optional.empty(), isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled, 1, extraServerProperties
    );
  }

  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testSchemaCacheWarming(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    storeVersionStateSupplier = getStoreVersionStateSupplier(true);
    localVeniceWriter.broadcastStartOfPush(true, new HashMap<>());
    long fooOffset = getOffset(localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());
    SchemaEntry schemaEntry = new SchemaEntry(1, STRING_SCHEMA);
    //Records order are: StartOfSeg, StartOfPush, data, EndOfPush, EndOfSeg
    runTest(new RandomPollStrategy(), Utils.setOf(PARTITION_FOO),
        () -> {
          Store mockStore = mock(Store.class);
          doReturn(true).when(mockStore).isReadComputationEnabled();
          doReturn(true).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);
          doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeNameWithoutVersionInfo);
          doReturn(schemaEntry).when(mockSchemaRepo).getValueSchema(anyString(), anyInt());
          doReturn(Optional.of(new VersionImpl("storeName", 1))).when(mockStore).getVersion(1);
        },
        () -> waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          verify(mockLogNotifier, atLeastOnce()).started(topic, PARTITION_FOO);
          //since notifier reporting happens before offset update, it actually reports previous offsets
          verify(mockLogNotifier, atLeastOnce()).endOfPushReceived(topic, PARTITION_FOO, fooOffset);
          // Since the completion report will be async, the completed offset could be `END_OF_PUSH` or `END_OF_SEGMENT` for batch push job.
          verify(mockLogNotifier).completed(eq(topic), eq(PARTITION_FOO),
              longThat(completionOffset ->  (completionOffset == fooOffset + 1) || (completionOffset == fooOffset + 2))
          );
        }), Optional.empty(), false, Optional.empty(),
        isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled, 1, Collections.singletonMap(SERVER_NUM_SCHEMA_FAST_CLASS_WARMUP, 1)
    );
  }

  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testDelayCompactionForSamzaReprocessingWorkload(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(false, new HashMap<>());
    long fooOffset = getOffset(localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());
    //Continue to write some message after EOP
    localVeniceWriter.put(putKeyFoo2, putValue, SCHEMA_ID);

    mockStorageMetadataService = new InMemoryStorageMetadataService();
    // To pass back the mock storage partition initialized in a lambda function
    final CompletableFuture<AbstractStoragePartition> mockStoragePartitionWrapper = new CompletableFuture<>();

    //Records order are: StartOfSeg, StartOfPush, data, EndOfPush, EndOfSeg
    runTest(new RandomPollStrategy(), Utils.setOf(PARTITION_FOO),
        () -> {
          mockAbstractStorageEngine.addStoragePartition(PARTITION_FOO);
          AbstractStoragePartition mockPartition = mock(AbstractStoragePartition.class);
          mockStoragePartitionWrapper.complete(mockPartition);
          doReturn(mockPartition).when(mockAbstractStorageEngine).getPartitionOrThrow(PARTITION_FOO);
          doReturn(CompletableFuture.completedFuture(null)).when(mockPartition).compactDB();
        },
        () -> waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          // Consumer should pause the consumption before EOP
          verify(mockLocalKafkaConsumer).pause(topic, PARTITION_FOO);
          verify(mockStoragePartitionWrapper.get()).compactDB();
          // Consumer should resume the consumption right before EOP
          verify(mockLocalKafkaConsumer).subscribe(eq(topic), eq(PARTITION_FOO), eq(fooOffset));

          verify(mockLogNotifier, atLeastOnce()).started(topic, PARTITION_FOO);
          //since notifier reporting happens before offset update, it actually reports previous offsets
          verify(mockLogNotifier, atLeastOnce()).endOfPushReceived(topic, PARTITION_FOO, fooOffset);
          // Since the completion report will be async, the completed offset could be `END_OF_PUSH` or `END_OF_SEGMENT` for batch push job.
          verify(mockLogNotifier).completed(eq(topic), eq(PARTITION_FOO),
              longThat(completionOffset ->  (completionOffset == fooOffset + 1) || (completionOffset == fooOffset + 2))
          );
        }), Optional.empty(), false, Optional.empty(), isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled, 1, Collections.singletonMap(SERVER_AUTO_COMPACTION_FOR_SAMZA_REPROCESSING_JOB_ENABLED, false)
    );
  }


  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testReportErrorWithEmptyPcsMap(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID);
    doThrow(new VeniceException("fake exception")).when(mockVersionedStorageIngestionStats).resetIngestionTaskErroredGauge(anyString(), anyInt());

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).error(eq(topic), eq(PARTITION_FOO), anyString(), any());
    }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "L/F-A/A", dataProviderClass = DataProviderUtils.class)
  public void testPartitionExceptionIsolation(boolean isLeaderFollowerModelEnabled, boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    getOffset(localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    long barLastOffset = getOffset(localVeniceWriter.put(putKeyBar, putValue, SCHEMA_ID));
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    doThrow(new VeniceException("fake storage engine exception")).when(mockAbstractStorageEngine).put(eq(PARTITION_FOO),
        any(), any(ByteBuffer.class));

    runTest(Utils.setOf(PARTITION_FOO, PARTITION_BAR), () -> {
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS))
          .completed(eq(topic), eq(PARTITION_BAR), LongEqualOrGreaterThanMatcher.get(barLastOffset));

      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).error(eq(topic), eq(PARTITION_FOO), anyString(), any());
      verify(mockLogNotifier, never()).completed(eq(topic), eq(PARTITION_FOO), anyLong());
      // Error partition should be unsubscribed
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).stopped(eq(topic), eq(PARTITION_FOO), anyLong());
      verify(mockLogNotifier, never()).error(eq(topic), eq(PARTITION_BAR), anyString(), any());
      assertTrue(storeIngestionTaskUnderTest.isRunning(), "The StoreIngestionTask should still be running");
    }, isLeaderFollowerModelEnabled, isActiveActiveReplicationEnabled);
  }

  private static class TestVeniceWriter<K,V> extends VeniceWriter{

    protected TestVeniceWriter(VeniceProperties props, String topicName, VeniceKafkaSerializer keySerializer,
        VeniceKafkaSerializer valueSerializer, VeniceKafkaSerializer updateSerializer, VenicePartitioner partitioner,
        Time time, Supplier supplier) {
      super(props, topicName, keySerializer, valueSerializer, updateSerializer, partitioner, time, Optional.empty(),
          Optional.empty(), supplier);
    }
  }

  private VeniceServerConfig buildVeniceServerConfig(Map<String, Object> extraProperties) {
    PropertyBuilder propertyBuilder = new PropertyBuilder();
    propertyBuilder.put(CLUSTER_NAME, "");
    propertyBuilder.put(ZOOKEEPER_ADDRESS, "");
    propertyBuilder.put(KAFKA_ZK_ADDRESS, "");
    propertyBuilder.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 500L);
    propertyBuilder.put(KAFKA_BOOTSTRAP_SERVERS, localAndRemoteKafkaBootstrapServers);
    propertyBuilder.put(HYBRID_QUOTA_ENFORCEMENT_ENABLED, false);
    propertyBuilder.put(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, databaseChecksumVerificationEnabled);
    propertyBuilder.put(SERVER_LOCAL_CONSUMER_CONFIG_PREFIX, new VeniceProperties());
    propertyBuilder.put(SERVER_REMOTE_CONSUMER_CONFIG_PREFIX, new VeniceProperties());
    propertyBuilder.put(SERVER_AUTO_COMPACTION_FOR_SAMZA_REPROCESSING_JOB_ENABLED, true);
    extraProperties.forEach(propertyBuilder::put);

    Map<String, Map<String, String>> kafkaClusterMap = new HashMap<>();
    Map<String, String> localKafkaMapping = new HashMap<>();
    localKafkaMapping.put(KAFKA_CLUSTER_MAP_KEY_NAME, "dev");
    localKafkaMapping.put(KAFKA_CLUSTER_MAP_KEY_URL, inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
    kafkaClusterMap.put(String.valueOf(0), localKafkaMapping);

    Map<String, String> remoteKafkaMapping = new HashMap<>();
    remoteKafkaMapping.put(KAFKA_CLUSTER_MAP_KEY_NAME, "remote");
    remoteKafkaMapping.put(KAFKA_CLUSTER_MAP_KEY_URL, inMemoryRemoteKafkaBroker.getKafkaBootstrapServer());
    kafkaClusterMap.put(String.valueOf(1), remoteKafkaMapping);

    return new VeniceServerConfig(propertyBuilder.build(), Optional.of(kafkaClusterMap));
  }

  private void verifyPutAndDelete(int amplificationFactor, boolean isActiveActiveReplicationEnabled, boolean recordsInBatchPush) {
    VenicePartitioner partitioner = getVenicePartitioner(amplificationFactor);
    int targetPartitionPutKeyFoo = partitioner.getPartitionId(putKeyFoo, PARTITION_COUNT);
    int targetPartitionDeleteKeyFoo = partitioner.getPartitionId(deleteKeyFoo, PARTITION_COUNT);

    // Batch push records for Active/Active do not persist replication metadata.
    if (isActiveActiveReplicationEnabled && !recordsInBatchPush) {
      // Verify StorageEngine#putWithReplicationMetadata is invoked only once and with appropriate key & value.
      verify(mockAbstractStorageEngine, timeout(100000))
          .putWithReplicationMetadata(targetPartitionPutKeyFoo, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(EXISTING_SCHEMA_ID, putValue).serialize()), putKeyFooReplicationMetadataWithValueSchemaIdBytes);
      // Verify StorageEngine#deleteWithReplicationMetadata is invoked only once and with appropriate key.
      verify(mockAbstractStorageEngine, timeout(100000))
          .deleteWithReplicationMetadata(targetPartitionDeleteKeyFoo, deleteKeyFoo, deleteKeyFooReplicationMetadataWithValueSchemaIdBytes);

      // Verify StorageEngine#put is never invoked for put operation.
      verify(mockAbstractStorageEngine, never()).put(eq(targetPartitionPutKeyFoo), eq(putKeyFoo), any(ByteBuffer.class));
      // Verify StorageEngine#Delete is never invoked for delete operation.
      verify(mockAbstractStorageEngine, never()).delete(eq(targetPartitionDeleteKeyFoo), eq(deleteKeyFoo));
    } else {
      // Verify StorageEngine#put is invoked only once and with appropriate key & value.
      verify(mockAbstractStorageEngine, timeout(100000))
          .put(targetPartitionPutKeyFoo, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(EXISTING_SCHEMA_ID, putValue).serialize()));
      // Verify StorageEngine#Delete is invoked only once and with appropriate key.
      verify(mockAbstractStorageEngine, timeout(100000)).delete(targetPartitionDeleteKeyFoo, deleteKeyFoo);

      // Verify StorageEngine#putWithReplicationMetadata is never invoked for put operation.
      verify(mockAbstractStorageEngine, never())
          .putWithReplicationMetadata(targetPartitionPutKeyFoo, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(EXISTING_SCHEMA_ID, putValue).serialize()), putKeyFooReplicationMetadataWithValueSchemaIdBytes);
      // Verify StorageEngine#deleteWithReplicationMetadata is never invoked for delete operation..
      verify(mockAbstractStorageEngine, never())
          .deleteWithReplicationMetadata(targetPartitionDeleteKeyFoo, deleteKeyFoo, deleteKeyFooReplicationMetadataWithValueSchemaIdBytes);
    }
  }

  @Test
  public void testRecordsCanBeThrottledPerRegion() throws ExecutionException, InterruptedException {
    int partitionCount = 2;
    int amplificationFactor = 1;

    VenicePartitioner partitioner = getVenicePartitioner(1); // Only get base venice partitioner
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setPartitionerClass(partitioner.getClass().getName());
    partitionerConfig.setAmplificationFactor(amplificationFactor);

    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(100,
        100,
        HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP
    );

    MockStoreVersionConfigs storeAndVersionConfigs = setupStoreAndVersionMocks(partitionCount, partitionerConfig,
        Optional.of(hybridStoreConfig), false, true, false, true);
    Store mockStore = storeAndVersionConfigs.store;
    Version version = storeAndVersionConfigs.version;
    VeniceStoreVersionConfig storeConfig = storeAndVersionConfigs.storeVersionConfig;

    Map<String, Object> extraServerProperties = new HashMap<>();
    extraServerProperties.put(SERVER_ENABLE_LIVE_CONFIG_BASED_KAFKA_THROTTLING, true);

    StoreIngestionTaskFactory ingestionTaskFactory = getIngestionTaskFactoryBuilder(new RandomPollStrategy(),
        Utils.setOf(PARTITION_FOO), Optional.of(hybridStoreConfig), Optional.empty(), 1, extraServerProperties).build();

    Properties kafkaProps = new Properties();
    kafkaProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, inMemoryLocalKafkaBroker.getKafkaBootstrapServer());

    int leaderSubPartition = PartitionUtils.getLeaderSubPartition(PARTITION_FOO, amplificationFactor);
    storeIngestionTaskUnderTest = ingestionTaskFactory.getNewIngestionTask(mockStore, version, kafkaProps,
        isCurrentVersion, storeConfig, leaderSubPartition, false,
        new StorageEngineBackedCompressorFactory(mockStorageMetadataService), Optional.empty());

    AtomicLong remoteKafkaQuota = new AtomicLong(10);

    MockTime testTime = new MockTime();
    long timeWindowMS = 1000L;
    // Unlimited
    EventThrottler localThrottler = new EventThrottler(testTime, -1, timeWindowMS, "local_throttler", true,
        EventThrottler.REJECT_STRATEGY);

    // Modifiable remote throttler
    EventThrottler remoteThrottler = new EventThrottler(testTime, remoteKafkaQuota::get, timeWindowMS, "remote_throttler", true,
        EventThrottler.REJECT_STRATEGY);

    kafkaUrlToRecordsThrottler.put(inMemoryLocalKafkaBroker.getKafkaBootstrapServer(), localThrottler);
    kafkaUrlToRecordsThrottler.put(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer(), remoteThrottler);

    String rtTopic = Version.composeRealTimeTopic(storeNameWithoutVersionInfo);
    inMemoryLocalKafkaBroker.createTopic(rtTopic, partitionCount);
    inMemoryRemoteKafkaBroker.createTopic(rtTopic, partitionCount);

    storeIngestionTaskUnderTest.consumerSubscribe(rtTopic, PARTITION_FOO, 0, inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
    storeIngestionTaskUnderTest.consumerSubscribe(rtTopic, PARTITION_FOO, 0, inMemoryRemoteKafkaBroker.getKafkaBootstrapServer());

    VeniceWriter localRtWriter = getVeniceWriter(rtTopic, () -> new MockInMemoryProducer(inMemoryLocalKafkaBroker), 1);
    VeniceWriter remoteRtWriter = getVeniceWriter(rtTopic, () -> new MockInMemoryProducer(inMemoryRemoteKafkaBroker), 1);

    for (int i = 0; i < 5; i++) {
      localRtWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID, putKeyFooTimestamp, null).get();
    }

    for (int i = 0; i < 5; i++) {
      remoteRtWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID, putKeyFooTimestamp, null).get();
    }

    // Verify can ingest at least some record from local and remote Kafka
    Map<String, ConsumerRecords<KafkaKey, KafkaMessageEnvelope>> kafkaUrlToConsumerRecords1 = storeIngestionTaskUnderTest.consumerPoll(1 * Time.MS_PER_SECOND);
    Assert.assertEquals(kafkaUrlToConsumerRecords1.size(), 2);
    Assert.assertFalse(kafkaUrlToConsumerRecords1.get(inMemoryLocalKafkaBroker.getKafkaBootstrapServer()).isEmpty());
    Assert.assertFalse(kafkaUrlToConsumerRecords1.get(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer()).isEmpty());

    // Drain local and remote Kafka
    while (true) {
      Map<String, ConsumerRecords<KafkaKey, KafkaMessageEnvelope>> kafkaUrlToConsumerRecords = storeIngestionTaskUnderTest.consumerPoll(1 * Time.MS_PER_SECOND);
      if (kafkaUrlToConsumerRecords.size() == 0) {
        break;
      }
    }

    // Pause remote kafka consumption
    remoteKafkaQuota.set(0);

    for (int i = 0; i < 5; i++) {
      localRtWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID, putKeyFooTimestamp, null).get();
    }

    for (int i = 0; i < 5; i++) {
      remoteRtWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID, putKeyFooTimestamp, null).get();
    }

    // Verify does not ingest from remote Kafka
    Map<String, ConsumerRecords<KafkaKey, KafkaMessageEnvelope>> kafkaUrlToConsumerRecords2 = storeIngestionTaskUnderTest.consumerPoll(1 * Time.MS_PER_SECOND);
    Assert.assertEquals(kafkaUrlToConsumerRecords2.size(), 1);
    Assert.assertFalse(kafkaUrlToConsumerRecords2.containsKey(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer()));

    // Resume remote Kafka consumption
    remoteKafkaQuota.set(10);
    testTime.sleep(timeWindowMS); // sleep so throttling window is reset and we don't run into race conditions

    // Verify resumes ingestion from remote Kafka
    int mockInteractionsBeforePoll = Mockito.mockingDetails(mockRemoteKafkaConsumer).getInvocations().size();
    Map<String, ConsumerRecords<KafkaKey, KafkaMessageEnvelope>> kafkaUrlToConsumerRecords3 = storeIngestionTaskUnderTest.consumerPoll(1 * Time.MS_PER_SECOND);
    Assert.assertTrue(kafkaUrlToConsumerRecords3.containsKey(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer()));
    int mockInteractionsAfterPoll = Mockito.mockingDetails(mockRemoteKafkaConsumer).getInvocations().size();
    Assert.assertEquals(mockInteractionsBeforePoll, mockInteractionsAfterPoll, "Remote consumer should not poll for new records but return previously cached records");
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testIsReadyToServe(boolean isDaVinciClient) {
    int partitionCount = 2;
    int amplificationFactor = 1;

    VenicePartitioner partitioner = getVenicePartitioner(1); // Only get base venice partitioner
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setPartitionerClass(partitioner.getClass().getName());
    partitionerConfig.setAmplificationFactor(amplificationFactor);

    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(100,
        100,
        100,
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP
    );

    MockStoreVersionConfigs storeAndVersionConfigs = setupStoreAndVersionMocks(partitionCount, partitionerConfig,
        Optional.of(hybridStoreConfig), false, true, true, true);
    Store mockStore = storeAndVersionConfigs.store;
    Version version = storeAndVersionConfigs.version;
    VeniceStoreVersionConfig storeConfig = storeAndVersionConfigs.storeVersionConfig;

    Map<String, Object> extraServerProperties = new HashMap<>();

    StoreIngestionTaskFactory ingestionTaskFactory = getIngestionTaskFactoryBuilder(new RandomPollStrategy(),
        Utils.setOf(PARTITION_FOO), Optional.of(hybridStoreConfig), Optional.empty(), 1, extraServerProperties)
        .setIsDaVinciClient(isDaVinciClient)
        .setTopicManagerRepositoryJavaBased(mockTopicManagerRepository)
        .build();

    TopicManager mockTopicManagerRemoteKafka = mock(TopicManager.class);

    doReturn(mockTopicManager).when(mockTopicManagerRepository).getTopicManager(inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
    doReturn(mockTopicManagerRemoteKafka).when(mockTopicManagerRepository).getTopicManager(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer());

    doReturn(true).when(mockTopicManager).containsTopic(anyString());
    doReturn(true).when(mockTopicManagerRemoteKafka).containsTopic(anyString());

    Properties kafkaProps = new Properties();
    kafkaProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, inMemoryLocalKafkaBroker.getKafkaBootstrapServer());

    int leaderSubPartition = PartitionUtils.getLeaderSubPartition(PARTITION_FOO, amplificationFactor);
    storeIngestionTaskUnderTest = ingestionTaskFactory.getNewIngestionTask(mockStore, version, kafkaProps,
        isCurrentVersion, storeConfig, leaderSubPartition, false,
        new StorageEngineBackedCompressorFactory(mockStorageMetadataService), Optional.empty());


    String rtTopic = Version.composeRealTimeTopic(mockStore.getName());
    TopicSwitch topicSwitchWithSourceRealTimeTopic = new TopicSwitch();
    topicSwitchWithSourceRealTimeTopic.sourceKafkaServers = new ArrayList<>();
    topicSwitchWithSourceRealTimeTopic.sourceKafkaServers.add(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer());
    topicSwitchWithSourceRealTimeTopic.sourceTopicName = rtTopic;

    OffsetRecord mockOffsetRecordLagCaughtUp = mock(OffsetRecord.class);
    doReturn(5L).when(mockOffsetRecordLagCaughtUp).getLocalVersionTopicOffset();
    doReturn(rtTopic).when(mockOffsetRecordLagCaughtUp).getLeaderTopic();
    doReturn(5L).when(mockOffsetRecordLagCaughtUp).getUpstreamOffset(inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
    doReturn(5L).when(mockOffsetRecordLagCaughtUp).getUpstreamOffset(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer());

    OffsetRecord mockOffsetRecordLagCaughtUpTimestampLagging = mock(OffsetRecord.class);
    doReturn(5L).when(mockOffsetRecordLagCaughtUpTimestampLagging).getLocalVersionTopicOffset();
    doReturn(rtTopic).when(mockOffsetRecordLagCaughtUpTimestampLagging).getLeaderTopic();
    doReturn(5L).when(mockOffsetRecordLagCaughtUpTimestampLagging).getUpstreamOffset(inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
    doReturn(5L).when(mockOffsetRecordLagCaughtUpTimestampLagging).getUpstreamOffset(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer());
    doReturn(System.currentTimeMillis() - MS_PER_HOUR).when(mockOffsetRecordLagCaughtUpTimestampLagging).getLatestProducerProcessingTimeInMs();

    // If EOP is not received, partition is not ready to serve
    PartitionConsumptionState mockPcsEOPNotReceived = mock(PartitionConsumptionState.class);
    doReturn(false).when(mockPcsEOPNotReceived).isEndOfPushReceived();
    Assert.assertFalse(storeIngestionTaskUnderTest.isReadyToServe(mockPcsEOPNotReceived));

    // If EOP is received and partition has reported COMPLETED, then partition is ready to serve
    PartitionConsumptionState mockPcsCompleted = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcsCompleted).isEndOfPushReceived();
    doReturn(true).when(mockPcsCompleted).isComplete();
    Assert.assertTrue(storeIngestionTaskUnderTest.isReadyToServe(mockPcsCompleted));

    // If partition has not reported COMPLETED but is not waiting for replication lag to catch up, it is ready to serve
    PartitionConsumptionState mockPcsNotWaitingForReplicationLag = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcsNotWaitingForReplicationLag).isEndOfPushReceived();
    doReturn(false).when(mockPcsNotWaitingForReplicationLag).isComplete();
    doReturn(false).when(mockPcsNotWaitingForReplicationLag).isWaitingForReplicationLag();
    Assert.assertTrue(storeIngestionTaskUnderTest.isReadyToServe(mockPcsNotWaitingForReplicationLag));

    // If partition is waiting for replication lag to catch up, but buffer replay has not started, it is not ready to serve
    PartitionConsumptionState mockPcsHybridButBufferReplayNotStarted = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcsHybridButBufferReplayNotStarted).isEndOfPushReceived();
    doReturn(false).when(mockPcsHybridButBufferReplayNotStarted).isComplete();
    doReturn(true).when(mockPcsHybridButBufferReplayNotStarted).isWaitingForReplicationLag();
    doReturn(true).when(mockPcsHybridButBufferReplayNotStarted).isHybrid();
    doReturn(null).when(mockPcsHybridButBufferReplayNotStarted).getTopicSwitch();
    Assert.assertFalse(storeIngestionTaskUnderTest.isReadyToServe(mockPcsHybridButBufferReplayNotStarted));

    // Since replication lag is caught up, partition is ready to serve
    PartitionConsumptionState mockPcsBufferReplayStartedLagCaughtUp = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcsBufferReplayStartedLagCaughtUp).isEndOfPushReceived();
    doReturn(false).when(mockPcsBufferReplayStartedLagCaughtUp).isComplete();
    doReturn(true).when(mockPcsBufferReplayStartedLagCaughtUp).isWaitingForReplicationLag();
    doReturn(true).when(mockPcsBufferReplayStartedLagCaughtUp).isHybrid();
    doReturn(topicSwitchWithSourceRealTimeTopic).when(mockPcsBufferReplayStartedLagCaughtUp).getTopicSwitch();
    doReturn(mockOffsetRecordLagCaughtUp).when(mockPcsBufferReplayStartedLagCaughtUp).getOffsetRecord();
    doReturn(5L).when(mockTopicManager).getPartitionLatestOffsetAndRetry(anyString(), anyInt(), anyInt());
    doReturn(5L).when(mockTopicManagerRemoteKafka).getPartitionLatestOffsetAndRetry(anyString(), anyInt(), anyInt());
    Assert.assertTrue(storeIngestionTaskUnderTest.isReadyToServe(mockPcsBufferReplayStartedLagCaughtUp));

    // Remote replication lag has not caught up but host has caught up to lag in local VT, so DaVinci replica will be
    // marked ready to serve but not storage node replica
    PartitionConsumptionState mockPcsBufferReplayStartedRemoteLagging = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcsBufferReplayStartedRemoteLagging).isEndOfPushReceived();
    doReturn(false).when(mockPcsBufferReplayStartedRemoteLagging).isComplete();
    doReturn(true).when(mockPcsBufferReplayStartedRemoteLagging).isWaitingForReplicationLag();
    doReturn(true).when(mockPcsBufferReplayStartedRemoteLagging).isHybrid();
    doReturn(topicSwitchWithSourceRealTimeTopic).when(mockPcsBufferReplayStartedRemoteLagging).getTopicSwitch();
    doReturn(mockOffsetRecordLagCaughtUpTimestampLagging).when(mockPcsBufferReplayStartedRemoteLagging).getOffsetRecord();
    doReturn(5L).when(mockTopicManager).getPartitionLatestOffsetAndRetry(anyString(), anyInt(), anyInt());
    doReturn(150L).when(mockTopicManagerRemoteKafka).getPartitionLatestOffsetAndRetry(anyString(), anyInt(), anyInt());
    Assert.assertEquals(storeIngestionTaskUnderTest.isReadyToServe(mockPcsBufferReplayStartedRemoteLagging), isDaVinciClient);

    // If there are issues in replication from remote RT -> local VT, DaVinci client will still report ready to serve
    // since they would have consumed all available data in local VT.
    // Venice storage nodes will not be ready to serve since they will detect lag with remote colo and wait for ingestion
    // to catch up before serving data.
    PartitionConsumptionState mockPcsOffsetLagCaughtUpTimestampLagging = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcsOffsetLagCaughtUpTimestampLagging).isEndOfPushReceived();
    doReturn(false).when(mockPcsOffsetLagCaughtUpTimestampLagging).isComplete();
    doReturn(true).when(mockPcsOffsetLagCaughtUpTimestampLagging).isWaitingForReplicationLag();
    doReturn(true).when(mockPcsOffsetLagCaughtUpTimestampLagging).isHybrid();
    doReturn(topicSwitchWithSourceRealTimeTopic).when(mockPcsOffsetLagCaughtUpTimestampLagging).getTopicSwitch();
    doReturn(mockOffsetRecordLagCaughtUpTimestampLagging).when(mockPcsOffsetLagCaughtUpTimestampLagging).getOffsetRecord();
    doReturn(5L).when(mockTopicManager).getPartitionLatestOffsetAndRetry(anyString(), anyInt(), anyInt());
    doReturn(5L).when(mockTopicManagerRemoteKafka).getPartitionLatestOffsetAndRetry(anyString(), anyInt(), anyInt());
    doReturn(System.currentTimeMillis() - 2 * MS_PER_DAY).when(mockTopicManager).getProducerTimestampOfLastDataRecord(anyString(), anyInt(), anyInt());
    doReturn(System.currentTimeMillis()).when(mockTopicManagerRemoteKafka).getProducerTimestampOfLastDataRecord(anyString(), anyInt(), anyInt());
    Assert.assertEquals(storeIngestionTaskUnderTest.isReadyToServe(mockPcsOffsetLagCaughtUpTimestampLagging), isDaVinciClient);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testActiveActiveStoreIsReadyToServe(boolean isDaVinciClient) {
    int partitionCount = 2;
    int amplificationFactor = 1;
    VenicePartitioner partitioner = getVenicePartitioner(1);
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setPartitionerClass(partitioner.getClass().getName());
    partitionerConfig.setAmplificationFactor(amplificationFactor);
    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(100, 100, -1, DataReplicationPolicy.ACTIVE_ACTIVE,
        BufferReplayPolicy.REWIND_FROM_EOP);

    MockStoreVersionConfigs storeAndVersionConfigs = setupStoreAndVersionMocks(partitionCount, partitionerConfig,
        Optional.of(hybridStoreConfig), false, true, true, true);
    Store mockStore = storeAndVersionConfigs.store;
    Version version = storeAndVersionConfigs.version;
    VeniceStoreVersionConfig storeConfig = storeAndVersionConfigs.storeVersionConfig;

    StoreIngestionTaskFactory ingestionTaskFactory = getIngestionTaskFactoryBuilder(new RandomPollStrategy(),
        Utils.setOf(PARTITION_FOO), Optional.of(hybridStoreConfig), Optional.empty(), 1, new HashMap<>())
        .setIsDaVinciClient(isDaVinciClient)
        .setTopicManagerRepositoryJavaBased(mockTopicManagerRepository)
        .build();

    TopicManager mockTopicManagerRemoteKafka = mock(TopicManager.class);
    doReturn(mockTopicManager).when(mockTopicManagerRepository).getTopicManager(inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
    doReturn(mockTopicManagerRemoteKafka).when(mockTopicManagerRepository).getTopicManager(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer());
    doReturn(true).when(mockTopicManager).containsTopic(anyString());
    doReturn(true).when(mockTopicManagerRemoteKafka).containsTopic(anyString());

    Properties kafkaProps = new Properties();
    kafkaProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
    int leaderSubPartition = PartitionUtils.getLeaderSubPartition(PARTITION_FOO, amplificationFactor);
    storeIngestionTaskUnderTest = ingestionTaskFactory.getNewIngestionTask(mockStore, version, kafkaProps,
        isCurrentVersion, storeConfig, leaderSubPartition, false,
        new StorageEngineBackedCompressorFactory(mockStorageMetadataService), Optional.empty());

    String rtTopic = Version.composeRealTimeTopic(mockStore.getName());
    TopicSwitch topicSwitchWithMultipleSourceKafkaServers = new TopicSwitch();
    topicSwitchWithMultipleSourceKafkaServers.sourceKafkaServers = new ArrayList<>();
    topicSwitchWithMultipleSourceKafkaServers.sourceKafkaServers.add(inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
    topicSwitchWithMultipleSourceKafkaServers.sourceKafkaServers.add(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer());
    topicSwitchWithMultipleSourceKafkaServers.sourceTopicName = rtTopic;

    OffsetRecord mockOffsetRecord = mock(OffsetRecord.class);
    doReturn(5L).when(mockOffsetRecord).getLocalVersionTopicOffset();
    doReturn(rtTopic).when(mockOffsetRecord).getLeaderTopic();
    doReturn(5L).when(mockOffsetRecord).getUpstreamOffset(inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
    doReturn(5L).when(mockOffsetRecord).getUpstreamOffset(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer());

    // Local replication are caught up but remote replication are not. A/A storage node replica is not ready to serve
    // Since host has caught up to lag in local VT, DaVinci replica will be marked ready to serve
    PartitionConsumptionState mockPcsMultipleSourceKafkaServers = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcsMultipleSourceKafkaServers).isEndOfPushReceived();
    doReturn(false).when(mockPcsMultipleSourceKafkaServers).isComplete();
    doReturn(true).when(mockPcsMultipleSourceKafkaServers).isWaitingForReplicationLag();
    doReturn(true).when(mockPcsMultipleSourceKafkaServers).isHybrid();
    doReturn(topicSwitchWithMultipleSourceKafkaServers).when(mockPcsMultipleSourceKafkaServers).getTopicSwitch();
    doReturn(mockOffsetRecord).when(mockPcsMultipleSourceKafkaServers).getOffsetRecord();
    doReturn(5L).when(mockTopicManager).getPartitionLatestOffsetAndRetry(anyString(), anyInt(), anyInt());
    doReturn(150L).when(mockTopicManagerRemoteKafka).getPartitionLatestOffsetAndRetry(anyString(), anyInt(), anyInt());
    Assert.assertEquals(storeIngestionTaskUnderTest.isReadyToServe(mockPcsMultipleSourceKafkaServers), isDaVinciClient);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testProcessTopicSwitch(boolean isDaVinciClient) {
    int amplificationFactor = 1;
    VenicePartitioner partitioner = getVenicePartitioner(amplificationFactor);
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setPartitionerClass(partitioner.getClass().getName());
    partitionerConfig.setAmplificationFactor(amplificationFactor);
    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(100,
        100,
        100,
        DataReplicationPolicy.AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP
    );
    MockStoreVersionConfigs storeAndVersionConfigs = setupStoreAndVersionMocks(2, partitionerConfig,
        Optional.of(hybridStoreConfig), false, true, true, false);
    Store mockStore = storeAndVersionConfigs.store;
    Version version = storeAndVersionConfigs.version;
    VeniceStoreVersionConfig storeConfig = storeAndVersionConfigs.storeVersionConfig;
    StoreIngestionTaskFactory ingestionTaskFactory = getIngestionTaskFactoryBuilder(new RandomPollStrategy(),
        Utils.setOf(PARTITION_FOO), Optional.of(hybridStoreConfig), Optional.empty(), amplificationFactor, new HashMap<>())
        .setIsDaVinciClient(isDaVinciClient)
        .setTopicManagerRepositoryJavaBased(mockTopicManagerRepository)
        .build();
    int leaderSubPartition = PartitionUtils.getLeaderSubPartition(PARTITION_FOO, amplificationFactor);
    Properties kafkaProps = new Properties();
    kafkaProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, inMemoryLocalKafkaBroker.getKafkaBootstrapServer());

    storeIngestionTaskUnderTest = ingestionTaskFactory.getNewIngestionTask(mockStore, version, kafkaProps,
        isCurrentVersion, storeConfig, leaderSubPartition, false,
        new StorageEngineBackedCompressorFactory(mockStorageMetadataService), Optional.empty());

    TopicManager mockTopicManagerRemoteKafka = mock(TopicManager.class);
    doReturn(mockTopicManagerRemoteKafka).when(mockTopicManagerRepository).getTopicManager(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer());

    TopicSwitch topicSwitchWithRemoteRealTimeTopic = new TopicSwitch();
    topicSwitchWithRemoteRealTimeTopic.sourceKafkaServers = new ArrayList<>();
    topicSwitchWithRemoteRealTimeTopic.sourceKafkaServers.add(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer());
    topicSwitchWithRemoteRealTimeTopic.sourceTopicName = Version.composeRealTimeTopic(mockStore.getName());
    topicSwitchWithRemoteRealTimeTopic.rewindStartTimestamp = System.currentTimeMillis();
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageUnion = topicSwitchWithRemoteRealTimeTopic;
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    OffsetRecord mockOffsetRecord = mock(OffsetRecord.class);
    doReturn(mockOffsetRecord).when(mockPcs).getOffsetRecord();
    storeIngestionTaskUnderTest.processTopicSwitch(controlMessage, PARTITION_FOO, 10, mockPcs);

    verify(mockTopicManagerRemoteKafka, isDaVinciClient ? never() : times(1))
        .getPartitionOffsetByTime(anyString(), anyInt(), anyLong());
  }

  @Test
  public void testUpdateConsumedUpstreamRTOffsetMapDuringRTSubscription() {
    String storeName = Utils.getUniqueString("store");
    String versionTopic = Version.composeKafkaTopic(storeName, 1);

    VeniceStoreVersionConfig mockVeniceStoreVersionConfig = mock(VeniceStoreVersionConfig.class);
    doReturn(versionTopic).when(mockVeniceStoreVersionConfig).getStoreVersionName();

    ReadOnlyStoreRepository mockReadOnlyStoreRepository = mock(ReadOnlyStoreRepository.class);
    Store mockStore = mock(Store.class);
    doReturn(mockStore).when(mockReadOnlyStoreRepository).getStoreOrThrow(eq(storeName));
    doReturn(false).when(mockStore).isHybridStoreDiskQuotaEnabled();

    Properties mockKafkaConsumerProperties = mock(Properties.class);
    doReturn("localhost").when(mockKafkaConsumerProperties).getProperty(eq(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));

    VeniceServerConfig mockVeniceServerConfig = mock(VeniceServerConfig.class);
    VeniceProperties mockVeniceProperties = mock(VeniceProperties.class);
    doReturn(true).when(mockVeniceProperties).isEmpty();
    doReturn(mockVeniceProperties).when(mockVeniceServerConfig).getKafkaConsumerConfigsForLocalConsumption();

    StoreIngestionTaskFactory.Builder builder = TestUtils.getStoreIngestionTaskBuilder(storeName, 1)
        .setTopicManagerRepository(mockTopicManagerRepository)
        .setStorageMetadataService(mockStorageMetadataService)
        .setMetadataRepository(mockReadOnlyStoreRepository)
        .setTopicManagerRepository(mockTopicManagerRepository)
        .setServerConfig(mockVeniceServerConfig);

    LeaderFollowerStoreIngestionTask ingestionTask = new LeaderFollowerStoreIngestionTask(
        builder,
        mockStore,
        mock(Version.class),
        mockKafkaConsumerProperties,
        () -> true,
        mockVeniceStoreVersionConfig,
        0,
        false,
        Optional.empty(),
        mock(StorageEngineBackedCompressorFactory.class));

    TopicSwitch topicSwitch = new TopicSwitch();
    topicSwitch.sourceKafkaServers = Collections.singletonList("localhost");
    topicSwitch.sourceTopicName = "test_rt";
    topicSwitch.rewindStartTimestamp = System.currentTimeMillis();
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(IN_TRANSITION_FROM_STANDBY_TO_LEADER).when(mockPcs).getLeaderFollowerState();
    doReturn(topicSwitch).when(mockPcs).getTopicSwitch();
    OffsetRecord mockOffsetRecord = mock(OffsetRecord.class);
    doReturn("test_rt").when(mockOffsetRecord).getLeaderTopic();
    doReturn(1000L).when(mockPcs).getLeaderOffset(anyString());
    doReturn(mockOffsetRecord).when(mockPcs).getOffsetRecord();
    // Test whether consumedUpstreamRTOffsetMap is updated when leader subscribes to RT after state transition
    ingestionTask.startConsumingAsLeaderInTransitionFromStandby(mockPcs);
    verify(mockPcs, times(1)).updateLeaderConsumedUpstreamRTOffset(
        eq(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY), eq(1000L));

    mockPcs = mock(PartitionConsumptionState.class);
    doReturn(LEADER).when(mockPcs).getLeaderFollowerState();
    doReturn(topicSwitch).when(mockPcs).getTopicSwitch();
    mockOffsetRecord = mock(OffsetRecord.class);
    doReturn("test_rt").when(mockOffsetRecord).getLeaderTopic();
    doReturn(1000L).when(mockOffsetRecord).getUpstreamOffset(anyString());
    doReturn(1000L).when(mockPcs).getLatestProcessedUpstreamRTOffset(anyString());
    doReturn(mockOffsetRecord).when(mockPcs).getOffsetRecord();
    // Test whether consumedUpstreamRTOffsetMap is updated when leader subscribes to RT after executing TS
    ingestionTask.leaderExecuteTopicSwitch(mockPcs, topicSwitch);
    verify(mockPcs, times(1)).updateLeaderConsumedUpstreamRTOffset(
        eq(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY), eq(1000L));
  }

  private static class MockStoreVersionConfigs {
    Store store;
    Version version;
    VeniceStoreVersionConfig storeVersionConfig;

    private MockStoreVersionConfigs(Store store, Version version, VeniceStoreVersionConfig storeVersionConfig) {
      this.store = store;
      this.version = version;
      this.storeVersionConfig = storeVersionConfig;
    }
  }
}
