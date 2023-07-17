package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.IN_TRANSITION_FROM_STANDBY_TO_LEADER;
import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.LEADER;
import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.STANDBY;
import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.FREEZE_INGESTION_IF_READY_TO_SERVE_OR_LOCAL_DATA_EXISTS;
import static com.linkedin.venice.ConfigKeys.HYBRID_QUOTA_ENFORCEMENT_ENABLED;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_CLUSTER_MAP_KEY_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_CLUSTER_MAP_KEY_URL;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_ENABLE_LIVE_CONFIG_BASED_KAFKA_THROTTLING;
import static com.linkedin.venice.ConfigKeys.SERVER_LOCAL_CONSUMER_CONFIG_PREFIX;
import static com.linkedin.venice.ConfigKeys.SERVER_NUM_SCHEMA_FAST_CLASS_WARMUP;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_REMOTE_CONSUMER_CONFIG_PREFIX;
import static com.linkedin.venice.ConfigKeys.SERVER_UNSUB_AFTER_BATCHPUSH;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;
import static com.linkedin.venice.utils.TestUtils.getOffsetRecord;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicCompletion;
import static com.linkedin.venice.utils.Time.MS_PER_DAY;
import static com.linkedin.venice.utils.Time.MS_PER_HOUR;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyDouble;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.longThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.helix.LeaderFollowerIngestionProgressNotifier;
import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.davinci.notifier.LogNotifier;
import com.linkedin.davinci.notifier.PartitionPushStatusNotifier;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.stats.AggHostLevelIngestionStats;
import com.linkedin.davinci.stats.AggVersionedDIVStats;
import com.linkedin.davinci.stats.AggVersionedIngestionStats;
import com.linkedin.davinci.stats.HostLevelIngestionStats;
import com.linkedin.davinci.stats.KafkaConsumerServiceStats;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.AbstractStoragePartition;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.davinci.store.rocksdb.RocksDBServerConfig;
import com.linkedin.venice.exceptions.MemoryLimitExhaustedException;
import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceIngestionTaskKilledException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.exceptions.validation.CorruptDataException;
import com.linkedin.venice.exceptions.validation.FatalDataValidationException;
import com.linkedin.venice.exceptions.validation.MissingDataException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.TopicManagerRepository;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
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
import com.linkedin.venice.meta.DataRecoveryVersionConfig;
import com.linkedin.venice.meta.DataRecoveryVersionConfigImpl;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.offsets.DeepCopyStorageMetadataService;
import com.linkedin.venice.offsets.InMemoryStorageMetadataService;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.partitioner.UserPartitionAwarePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
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
import com.linkedin.venice.unit.kafka.consumer.poll.PubSubTopicPartitionOffset;
import com.linkedin.venice.unit.kafka.consumer.poll.RandomPollStrategy;
import com.linkedin.venice.unit.kafka.producer.MockInMemoryProducerAdapter;
import com.linkedin.venice.unit.kafka.producer.TransformingProducerAdapter;
import com.linkedin.venice.unit.matchers.ExceptionClassMatcher;
import com.linkedin.venice.unit.matchers.LongEqualOrGreaterThanMatcher;
import com.linkedin.venice.unit.matchers.NonEmptyStringMatcher;
import com.linkedin.venice.utils.ByteArray;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.DiskUsage;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.pools.LandFillObjectPool;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Beware that most of the test cases in this suite depend on {@link StoreIngestionTaskTest#TEST_TIMEOUT_MS}
 * Adjust it based on environment if timeout failure occurs.
 */
@Test(singleThreaded = true)
public abstract class StoreIngestionTaskTest {
  private static final Logger LOGGER = LogManager.getLogger(StoreIngestionTaskTest.class);

  private static final long READ_CYCLE_DELAY_MS = 5;
  private static final long TEST_TIMEOUT_MS = 1000 * READ_CYCLE_DELAY_MS;
  private static final int RUN_TEST_FUNCTION_TIMEOUT_SECONDS = 10;
  private static final long EMPTY_POLL_SLEEP_MS = 0;

  private static final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  // TODO: Test other permutations of these params
  private static final PubSubMessageDeserializer pubSubDeserializer = new PubSubMessageDeserializer(
      new OptimizedKafkaValueSerializer(),
      new LandFillObjectPool<>(KafkaMessageEnvelope::new),
      new LandFillObjectPool<>(KafkaMessageEnvelope::new));

  static {
    StoreIngestionTask.SCHEMA_POLLING_DELAY_MS = 100;
    IngestionNotificationDispatcher.PROGRESS_REPORT_INTERVAL = -1; // Report all the time.
  }

  private InMemoryKafkaBroker inMemoryLocalKafkaBroker;
  private InMemoryKafkaBroker inMemoryRemoteKafkaBroker;
  private MockInMemoryConsumer inMemoryLocalKafkaConsumer;
  private MockInMemoryConsumer inMemoryRemoteKafkaConsumer;
  private VeniceWriterFactory mockWriterFactory;
  private VeniceWriter localVeniceWriter;
  private StorageEngineRepository mockStorageEngineRepository;
  private VeniceNotifier mockLogNotifier, mockPartitionStatusNotifier, mockLeaderFollowerStateModelNotifier;
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
  private PubSubConsumerAdapter mockLocalKafkaConsumer;
  private PubSubConsumerAdapter mockRemoteKafkaConsumer;
  private TopicManager mockTopicManager;
  private TopicManagerRepository mockTopicManagerRepository;
  private AggHostLevelIngestionStats mockAggStoreIngestionStats;
  private HostLevelIngestionStats mockStoreIngestionStats;
  private AggVersionedDIVStats mockVersionedDIVStats;
  private AggVersionedIngestionStats mockVersionedStorageIngestionStats;
  private StoreIngestionTask storeIngestionTaskUnderTest;
  private ExecutorService taskPollingService;
  private StoreBufferService storeBufferService;
  private AggKafkaConsumerService aggKafkaConsumerService;
  private BooleanSupplier isCurrentVersion;
  private Optional<HybridStoreConfig> hybridStoreConfig;
  private VeniceServerConfig veniceServerConfig;
  private RocksDBServerConfig rocksDBServerConfig;
  private long databaseSyncBytesIntervalForTransactionalMode = 1;
  private long databaseSyncBytesIntervalForDeferredWriteMode = 2;
  private KafkaConsumerService localKafkaConsumerService;
  private KafkaConsumerService remoteKafkaConsumerService;

  private StorePartitionDataReceiver localConsumedDataReceiver;
  private StorePartitionDataReceiver remoteConsumedDataReceiver;

  private static String storeNameWithoutVersionInfo;
  private String topic;
  private PubSubTopic pubSubTopic;
  private PubSubTopicPartition fooTopicPartition;
  private PubSubTopicPartition barTopicPartition;

  private Runnable runnableForKillNonCurrentVersion;

  private static final int PARTITION_COUNT = 10;
  private static final Set<Integer> ALL_PARTITIONS = new HashSet<>();
  static {
    for (int partition = 0; partition < PARTITION_COUNT; partition++) {
      ALL_PARTITIONS.add(partition);
    }
  }
  private static final int PARTITION_FOO = 1;
  private static final int PARTITION_BAR = 2;
  private static final int SCHEMA_ID = 1;
  private static final int EXISTING_SCHEMA_ID = 1;
  private static final int NON_EXISTING_SCHEMA_ID = 2;
  private static final Schema STRING_SCHEMA = Schema.parse("\"string\"");

  private static final byte[] putKeyFoo = getRandomKey(PARTITION_FOO);
  private static final byte[] putKeyFoo2 = getRandomKey(PARTITION_FOO);
  private static final byte[] putKeyBar = getRandomKey(PARTITION_BAR);
  private static final byte[] putValue = new VeniceAvroKafkaSerializer(STRING_SCHEMA).serialize(null, "TestValuePut");
  private static final byte[] putValueToCorrupt = "Please corrupt me!".getBytes(StandardCharsets.UTF_8);
  private static final byte[] deleteKeyFoo = getRandomKey(PARTITION_FOO);

  private static final int REPLICATION_METADATA_VERSION_ID = 1;
  private static final Schema REPLICATION_METADATA_SCHEMA = RmdSchemaGenerator.generateMetadataSchema(STRING_SCHEMA, 1);
  private static final RecordSerializer REPLICATION_METADATA_SERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(REPLICATION_METADATA_SCHEMA);

  private static final long PUT_KEY_FOO_TIMESTAMP = 2L;
  private static final long DELETE_KEY_FOO_TIMESTAMP = 2L;
  private static final long PUT_KEY_FOO_OFFSET = 1L;
  private static final long DELETE_KEY_FOO_OFFSET = 2L;

  private static final byte[] putKeyFooReplicationMetadataWithValueSchemaIdBytesDefault =
      createReplicationMetadataWithValueSchemaId(PUT_KEY_FOO_TIMESTAMP - 1, PUT_KEY_FOO_OFFSET, EXISTING_SCHEMA_ID);
  private static final byte[] putKeyFooReplicationMetadataWithValueSchemaIdBytes =
      createReplicationMetadataWithValueSchemaId(PUT_KEY_FOO_TIMESTAMP, PUT_KEY_FOO_OFFSET, EXISTING_SCHEMA_ID);
  private static final byte[] deleteKeyFooReplicationMetadataWithValueSchemaIdBytes =
      createReplicationMetadataWithValueSchemaId(DELETE_KEY_FOO_TIMESTAMP, DELETE_KEY_FOO_OFFSET, EXISTING_SCHEMA_ID);

  private boolean databaseChecksumVerificationEnabled = false;
  private KafkaConsumerServiceStats kafkaConsumerServiceStats = mock(KafkaConsumerServiceStats.class);
  private PubSubConsumerAdapterFactory mockFactory = mock(PubSubConsumerAdapterFactory.class);

  private Supplier<StoreVersionState> storeVersionStateSupplier = () -> new StoreVersionState();

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

    ByteBuffer replicationMetadataWithValueSchemaId =
        ByteUtils.prependIntHeaderToByteBuffer(ByteBuffer.wrap(replicationMetadata), valueSchemaId, false);
    replicationMetadataWithValueSchemaId
        .position(replicationMetadataWithValueSchemaId.position() - ByteUtils.SIZE_OF_INT);
    return ByteUtils.extractByteArray(replicationMetadataWithValueSchemaId);
  }

  @BeforeClass(alwaysRun = true)
  public void suiteSetUp() throws Exception {
    taskPollingService = Executors.newFixedThreadPool(1);

    storeBufferService = new StoreBufferService(3, 10000, 1000, isStoreWriterBufferAfterLeaderLogicEnabled());
    storeBufferService.start();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() throws Exception {
    TestUtils.shutdownExecutor(taskPollingService);
    storeBufferService.stop();
  }

  @AfterMethod(alwaysRun = true)
  public void methodCleanUp() throws Exception {
    if (localKafkaConsumerService != null) {
      localKafkaConsumerService.stopInner();
    }
    if (remoteKafkaConsumerService != null) {
      remoteKafkaConsumerService.stopInner();
    }
  }

  @BeforeMethod(alwaysRun = true)
  public void methodSetUp() throws Exception {
    aggKafkaConsumerService = mock(AggKafkaConsumerService.class);
    storeNameWithoutVersionInfo = Utils.getUniqueString("TestTopic");
    topic = Version.composeKafkaTopic(storeNameWithoutVersionInfo, 1);
    pubSubTopic = pubSubTopicRepository.getTopic(topic);
    fooTopicPartition = new PubSubTopicPartitionImpl(pubSubTopic, PARTITION_FOO);
    barTopicPartition = new PubSubTopicPartitionImpl(pubSubTopic, PARTITION_BAR);

    inMemoryLocalKafkaBroker = new InMemoryKafkaBroker("local");
    inMemoryLocalKafkaBroker.createTopic(topic, PARTITION_COUNT);
    inMemoryRemoteKafkaBroker = new InMemoryKafkaBroker("remote");
    inMemoryRemoteKafkaBroker.createTopic(topic, PARTITION_COUNT);

    localVeniceWriter = getVeniceWriter(new MockInMemoryProducerAdapter(inMemoryLocalKafkaBroker));

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
    }).when(mockLogNotifier).completed(anyString(), anyInt(), anyLong(), anyString());
    mockNotifierError = new ArrayList<>();
    doAnswer(invocation -> {
      Object[] args = invocation.getArguments();
      mockNotifierError.add(args);
      return null;
    }).when(mockLogNotifier).error(anyString(), anyInt(), anyString(), any());

    mockPartitionStatusNotifier = mock(PartitionPushStatusNotifier.class);
    mockLeaderFollowerStateModelNotifier = mock(LeaderFollowerIngestionProgressNotifier.class);

    mockStorageMetadataService = mock(StorageMetadataService.class);

    mockBandwidthThrottler = mock(EventThrottler.class);
    mockRecordsThrottler = mock(EventThrottler.class);
    mockSchemaRepo = mock(ReadOnlySchemaRepository.class);
    mockMetadataRepo = mock(ReadOnlyStoreRepository.class);
    mockLocalKafkaConsumer = mock(PubSubConsumerAdapter.class);
    mockRemoteKafkaConsumer = mock(PubSubConsumerAdapter.class);
    kafkaUrlToRecordsThrottler = new HashMap<>();
    kafkaClusterBasedRecordThrottler = new KafkaClusterBasedRecordThrottler(kafkaUrlToRecordsThrottler);

    mockTopicManager = mock(TopicManager.class);
    mockTopicManagerRepository = mock(TopicManagerRepository.class);
    doReturn(mockTopicManager).when(mockTopicManagerRepository).getTopicManager();

    mockAggStoreIngestionStats = mock(AggHostLevelIngestionStats.class);
    mockStoreIngestionStats = mock(HostLevelIngestionStats.class);
    doReturn(mockStoreIngestionStats).when(mockAggStoreIngestionStats).getStoreStats(anyString());

    mockVersionedDIVStats = mock(AggVersionedDIVStats.class);
    mockVersionedStorageIngestionStats = mock(AggVersionedIngestionStats.class);

    isCurrentVersion = () -> false;
    hybridStoreConfig = Optional.empty();

    databaseChecksumVerificationEnabled = false;
    rocksDBServerConfig = mock(RocksDBServerConfig.class);

    doReturn(true).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);
    doReturn(false).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, NON_EXISTING_SCHEMA_ID);

    doReturn(new RmdSchemaEntry(EXISTING_SCHEMA_ID, REPLICATION_METADATA_VERSION_ID, REPLICATION_METADATA_SCHEMA))
        .when(mockSchemaRepo)
        .getReplicationMetadataSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID, REPLICATION_METADATA_VERSION_ID);

    setDefaultStoreVersionStateSupplier();

    runnableForKillNonCurrentVersion = mock(Runnable.class);
  }

  private VeniceWriter getVeniceWriter(String topic, PubSubProducerAdapter producerAdapter, int amplificationFactor) {
    VeniceWriterOptions veniceWriterOptions =
        new VeniceWriterOptions.Builder(topic).setKeySerializer(new DefaultSerializer())
            .setValueSerializer(new DefaultSerializer())
            .setWriteComputeSerializer(new DefaultSerializer())
            .setPartitioner(getVenicePartitioner(amplificationFactor))
            .setTime(SystemTime.INSTANCE)
            .build();
    return new VeniceWriter(veniceWriterOptions, VeniceProperties.empty(), producerAdapter);
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

  private VeniceWriter getVeniceWriter(PubSubProducerAdapter producerAdapter) {
    VeniceWriterOptions veniceWriterOptions =
        new VeniceWriterOptions.Builder(topic).setKeySerializer(new DefaultSerializer())
            .setValueSerializer(new DefaultSerializer())
            .setWriteComputeSerializer(new DefaultSerializer())
            .setPartitioner(new SimplePartitioner())
            .setTime(SystemTime.INSTANCE)
            .build();
    return new VeniceWriter(veniceWriterOptions, VeniceProperties.empty(), producerAdapter);
  }

  private VeniceWriter getCorruptedVeniceWriter(byte[] valueToCorrupt, InMemoryKafkaBroker kafkaBroker) {
    return getVeniceWriter(
        new CorruptedKafkaProducerAdapter(new MockInMemoryProducerAdapter(kafkaBroker), topic, valueToCorrupt));
  }

  static class CorruptedKafkaProducerAdapter extends TransformingProducerAdapter {
    public CorruptedKafkaProducerAdapter(PubSubProducerAdapter baseProducer, String topic, byte[] valueToCorrupt) {
      super(baseProducer, (topicName, key, value, partition) -> {
        KafkaMessageEnvelope transformedMessageEnvelope = value;

        if (MessageType.valueOf(transformedMessageEnvelope) == MessageType.PUT) {
          Put put = (Put) transformedMessageEnvelope.payloadUnion;
          if (put.putValue.array() == valueToCorrupt) {
            put.putValue = ByteBuffer.wrap("CORRUPT_VALUE".getBytes());
            transformedMessageEnvelope.payloadUnion = put;
          }
        }

        return new TransformingProducerAdapter.SendMessageParameters(topic, key, transformedMessageEnvelope, partition);
      });
    }
  }

  private long getOffset(Future<PubSubProduceResult> produceResultFuture)
      throws ExecutionException, InterruptedException {
    return produceResultFuture.get().getOffset();
  }

  private void runTest(Set<Integer> partitions, Runnable assertions, boolean isActiveActiveReplicationEnabled)
      throws Exception {
    runTest(partitions, () -> {}, assertions, isActiveActiveReplicationEnabled);
  }

  private void runTest(
      Set<Integer> partitions,
      Runnable beforeStartingConsumption,
      Runnable assertions,
      boolean isActiveActiveReplicationEnabled) throws Exception {
    runTest(
        new RandomPollStrategy(),
        partitions,
        beforeStartingConsumption,
        assertions,
        this.hybridStoreConfig,
        false,
        Optional.empty(),
        isActiveActiveReplicationEnabled,
        1,
        Collections.emptyMap(),
        storeVersionConfigOverride -> {});
  }

  private void runTest(
      Set<Integer> partitions,
      Runnable beforeStartingConsumption,
      Runnable assertions,
      boolean isActiveActiveReplicationEnabled,
      Consumer<VeniceStoreVersionConfig> storeVersionConfigOverride) throws Exception {
    runTest(
        new RandomPollStrategy(),
        partitions,
        beforeStartingConsumption,
        assertions,
        this.hybridStoreConfig,
        false,
        Optional.empty(),
        isActiveActiveReplicationEnabled,
        1,
        Collections.emptyMap(),
        storeVersionConfigOverride);
  }

  private void runTest(
      PollStrategy pollStrategy,
      Set<Integer> partitions,
      Runnable beforeStartingConsumption,
      Runnable assertions,
      boolean isActiveActiveReplicationEnabled) throws Exception {
    runTest(
        pollStrategy,
        partitions,
        beforeStartingConsumption,
        assertions,
        this.hybridStoreConfig,
        false,
        Optional.empty(),
        isActiveActiveReplicationEnabled,
        1,
        Collections.emptyMap(),
        storeVersionConfigOverride -> {});
  }

  private void runTest(
      PollStrategy pollStrategy,
      Set<Integer> partitions,
      Runnable beforeStartingConsumption,
      Runnable assertions,
      Optional<HybridStoreConfig> hybridStoreConfig,
      boolean incrementalPushEnabled,
      Optional<DiskUsage> diskUsageForTest,
      boolean isActiveActiveReplicationEnabled,
      int amplificationFactor,
      Map<String, Object> extraServerProperties) throws Exception {
    runTest(
        pollStrategy,
        partitions,
        beforeStartingConsumption,
        assertions,
        hybridStoreConfig,
        incrementalPushEnabled,
        diskUsageForTest,
        isActiveActiveReplicationEnabled,
        amplificationFactor,
        extraServerProperties,
        storeVersionConfigOverride -> {});
  }

  /**
   * A simple framework to specify how to run the task and how to assert the results
   * @param pollStrategy, the polling strategy for Kakfa consumer to poll messages written by local venice writer
   * @param partitions, the number of partitions
   * @param beforeStartingConsumption, the pre-test logic to set up test-related logic before starting the SIT
   * @param assertions, the assertion logic to verify the code path is running as expected
   *                    Note that due to concurrency nature of codes, it's often needed to run {@link Mockito#verify} with
   *                    {@link org.mockito.verification.VerificationWithTimeout} to wait for certain code path to be executed.
   *                    Otherwise, the main thread, i.e. the unit test thread, can terminate the task early.
   * @param hybridStoreConfig, the config for hybrid store
   * @param incrementalPushEnabled, the flag to turn on incremental push for SIT
   * @param diskUsageForTest, optionally field to mock the disk usage for the test
   * @param isActiveActiveReplicationEnabled, the flag to turn on ActiveActiveReplication for SIT
   * @param amplificationFactor, the amplificationFactor
   * @param extraServerProperties, the extra config for server
   * @param storeVersionConfigOverride, the override for store version config
   * @throws Exception
   */
  private void runTest(
      PollStrategy pollStrategy,
      Set<Integer> partitions,
      Runnable beforeStartingConsumption,
      Runnable assertions,
      Optional<HybridStoreConfig> hybridStoreConfig,
      boolean incrementalPushEnabled,
      Optional<DiskUsage> diskUsageForTest,
      boolean isActiveActiveReplicationEnabled,
      int amplificationFactor,
      Map<String, Object> extraServerProperties,
      Consumer<VeniceStoreVersionConfig> storeVersionConfigOverride) throws Exception {

    int partitionCount = PARTITION_COUNT / amplificationFactor;
    VenicePartitioner partitioner = getVenicePartitioner(1); // Only get base venice partitioner
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setPartitionerClass(partitioner.getClass().getName());
    partitionerConfig.setAmplificationFactor(amplificationFactor);

    MockStoreVersionConfigs storeAndVersionConfigs = setupStoreAndVersionMocks(
        partitionCount,
        partitionerConfig,
        hybridStoreConfig,
        incrementalPushEnabled,
        false,
        isActiveActiveReplicationEnabled,
        storeVersionConfigOverride);
    Store mockStore = storeAndVersionConfigs.store;
    Version version = storeAndVersionConfigs.version;
    VeniceStoreVersionConfig storeConfig = storeAndVersionConfigs.storeVersionConfig;

    StoreIngestionTaskFactory ingestionTaskFactory = getIngestionTaskFactoryBuilder(
        pollStrategy,
        partitions,
        diskUsageForTest,
        amplificationFactor,
        extraServerProperties,
        false).build();

    Properties kafkaProps = new Properties();
    kafkaProps.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryLocalKafkaBroker.getKafkaBootstrapServer());

    int leaderSubPartition = PartitionUtils.getLeaderSubPartition(PARTITION_FOO, amplificationFactor);
    storeIngestionTaskUnderTest = ingestionTaskFactory.getNewIngestionTask(
        mockStore,
        version,
        kafkaProps,
        isCurrentVersion,
        storeConfig,
        leaderSubPartition,
        false,
        Optional.empty());

    Future testSubscribeTaskFuture = null;
    try {
      for (int partition: partitions) {
        storeIngestionTaskUnderTest
            .subscribePartition(new PubSubTopicPartitionImpl(pubSubTopic, partition), Optional.empty());
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

  private MockStoreVersionConfigs setupStoreAndVersionMocks(
      int partitionCount,
      PartitionerConfig partitionerConfig,
      Optional<HybridStoreConfig> hybridStoreConfig,
      boolean incrementalPushEnabled,
      boolean isNativeReplicationEnabled,
      boolean isActiveActiveReplicationEnabled) {
    return setupStoreAndVersionMocks(
        partitionCount,
        partitionerConfig,
        hybridStoreConfig,
        incrementalPushEnabled,
        isNativeReplicationEnabled,
        isActiveActiveReplicationEnabled,
        storeVersionConfigOverride -> {});
  }

  private MockStoreVersionConfigs setupStoreAndVersionMocks(
      int partitionCount,
      PartitionerConfig partitionerConfig,
      Optional<HybridStoreConfig> hybridStoreConfig,
      boolean incrementalPushEnabled,
      boolean isNativeReplicationEnabled,
      boolean isActiveActiveReplicationEnabled,
      Consumer<VeniceStoreVersionConfig> storeVersionConfigOverride) {
    boolean isHybrid = hybridStoreConfig.isPresent();
    HybridStoreConfig hybridSoreConfigValue = null;
    if (isHybrid) {
      hybridSoreConfigValue = hybridStoreConfig.get();
    }

    // mock the store config
    VeniceStoreVersionConfig storeConfig = getDefaultMockVeniceStoreVersionConfig(storeVersionConfigOverride);

    Store mockStore = mock(Store.class);
    Version version = new VersionImpl(storeNameWithoutVersionInfo, 1, "1", partitionCount);

    doReturn(storeNameWithoutVersionInfo).when(mockStore).getName();

    version.setPartitionerConfig(partitionerConfig);
    doReturn(partitionerConfig).when(mockStore).getPartitionerConfig();

    version.setIncrementalPushEnabled(incrementalPushEnabled);
    doReturn(incrementalPushEnabled).when(mockStore).isIncrementalPushEnabled();

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
    version.setRmdVersionId(REPLICATION_METADATA_VERSION_ID);

    doReturn(Optional.of(version)).when(mockStore).getVersion(anyInt());
    doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeNameWithoutVersionInfo);
    doReturn(mockStore).when(mockMetadataRepo).getStore(storeNameWithoutVersionInfo);

    return new MockStoreVersionConfigs(mockStore, version, storeConfig);
  }

  private StoreIngestionTaskFactory.Builder getIngestionTaskFactoryBuilder(
      PollStrategy pollStrategy,
      Set<Integer> partitions,
      Optional<DiskUsage> diskUsageForTest,
      int amplificationFactor,
      Map<String, Object> extraServerProperties,
      Boolean isLiveConfigEnabled) {
    doReturn(new DeepCopyStorageEngine(mockAbstractStorageEngine)).when(mockStorageEngineRepository)
        .getLocalStorageEngine(topic);

    inMemoryLocalKafkaConsumer =
        new MockInMemoryConsumer(inMemoryLocalKafkaBroker, pollStrategy, mockLocalKafkaConsumer);
    inMemoryRemoteKafkaConsumer =
        new MockInMemoryConsumer(inMemoryRemoteKafkaBroker, pollStrategy, mockRemoteKafkaConsumer);

    doAnswer(invocation -> {
      VeniceProperties consumerProps = invocation.getArgument(0, VeniceProperties.class);
      String kafkaUrl = consumerProps.toProperties().getProperty(KAFKA_BOOTSTRAP_SERVERS);
      if (kafkaUrl.equals(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer())) {
        return inMemoryRemoteKafkaConsumer;
      }
      return inMemoryLocalKafkaConsumer;
    }).when(mockFactory).create(any(), anyBoolean(), any(), any());

    mockWriterFactory = mock(VeniceWriterFactory.class);
    doReturn(null).when(mockWriterFactory).createVeniceWriter(any());
    StorageMetadataService offsetManager;
    LOGGER.info("mockStorageMetadataService: {}", mockStorageMetadataService.getClass().getName());
    final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    if (mockStorageMetadataService.getClass() != InMemoryStorageMetadataService.class) {
      for (int partition: PartitionUtils.getSubPartitions(partitions, amplificationFactor)) {
        doReturn(new OffsetRecord(partitionStateSerializer)).when(mockStorageMetadataService)
            .getLastOffset(topic, partition);
      }
    }
    offsetManager = new DeepCopyStorageMetadataService(mockStorageMetadataService);
    Queue<VeniceNotifier> leaderFollowerNotifiers = new ConcurrentLinkedQueue<>(
        Arrays.asList(mockLogNotifier, mockPartitionStatusNotifier, mockLeaderFollowerStateModelNotifier));
    DiskUsage diskUsage;
    if (diskUsageForTest.isPresent()) {
      diskUsage = diskUsageForTest.get();
    } else {
      diskUsage = mock(DiskUsage.class);
      doReturn(false).when(diskUsage).isDiskFull(anyLong());
    }

    // Recreate the map so that immutable maps that get passed in can be mutated...
    extraServerProperties = new HashMap<>(extraServerProperties);
    veniceServerConfig = buildVeniceServerConfig(extraServerProperties);

    MetricsRepository mockMetricsRepository = mock(MetricsRepository.class);
    final Sensor mockSensor = mock(Sensor.class);
    doReturn(mockSensor).when(mockMetricsRepository).sensor(anyString(), any());
    Properties localKafkaProps = new Properties();
    localKafkaProps.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
    localKafkaConsumerService = getConsumerAssignmentStrategy().constructor.construct(
        mockFactory,
        localKafkaProps,
        10,
        1,
        mockBandwidthThrottler,
        mockRecordsThrottler,
        kafkaClusterBasedRecordThrottler,
        mockMetricsRepository,
        inMemoryLocalKafkaBroker.getKafkaBootstrapServer(),
        1000,
        mock(TopicExistenceChecker.class),
        isLiveConfigEnabled,
        pubSubDeserializer,
        SystemTime.INSTANCE,
        kafkaConsumerServiceStats,
        false);
    localKafkaConsumerService.start();

    Properties remoteKafkaProps = new Properties();
    remoteKafkaProps.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryRemoteKafkaBroker.getKafkaBootstrapServer());
    remoteKafkaConsumerService = getConsumerAssignmentStrategy().constructor.construct(
        mockFactory,
        remoteKafkaProps,
        10,
        1,
        mockBandwidthThrottler,
        mockRecordsThrottler,
        kafkaClusterBasedRecordThrottler,
        mockMetricsRepository,
        inMemoryLocalKafkaBroker.getKafkaBootstrapServer(),
        1000,
        mock(TopicExistenceChecker.class),
        isLiveConfigEnabled,
        pubSubDeserializer,
        SystemTime.INSTANCE,
        kafkaConsumerServiceStats,
        false);
    remoteKafkaConsumerService.start();

    doReturn(100L).when(mockBandwidthThrottler).getMaxRatePerSecond();
    prepareAggKafkaConsumerServiceMock();

    return StoreIngestionTaskFactory.builder()
        .setVeniceWriterFactory(mockWriterFactory)
        .setStorageEngineRepository(mockStorageEngineRepository)
        .setStorageMetadataService(offsetManager)
        .setLeaderFollowerNotifiersQueue(leaderFollowerNotifiers)
        .setSchemaRepository(mockSchemaRepo)
        .setMetadataRepository(mockMetadataRepo)
        .setTopicManagerRepository(mockTopicManagerRepository)
        .setHostLevelIngestionStats(mockAggStoreIngestionStats)
        .setVersionedDIVStats(mockVersionedDIVStats)
        .setVersionedIngestionStats(mockVersionedStorageIngestionStats)
        .setStoreBufferService(storeBufferService)
        .setServerConfig(veniceServerConfig)
        .setDiskUsage(diskUsage)
        .setAggKafkaConsumerService(aggKafkaConsumerService)
        .setCompressorFactory(new StorageEngineBackedCompressorFactory(mockStorageMetadataService))
        .setPubSubTopicRepository(pubSubTopicRepository)
        .setPartitionStateSerializer(partitionStateSerializer)
        .setRunnableForKillIngestionTasksForNonCurrentVersions(runnableForKillNonCurrentVersion);
  }

  abstract KafkaConsumerService.ConsumerAssignmentStrategy getConsumerAssignmentStrategy();

  abstract boolean isStoreWriterBufferAfterLeaderLogicEnabled();

  private void prepareAggKafkaConsumerServiceMock() {
    doAnswer(invocation -> {
      String kafkaUrl = invocation.getArgument(0, String.class);
      StoreIngestionTask storeIngestionTask = invocation.getArgument(1, StoreIngestionTask.class);
      PubSubTopicPartition topicPartition = invocation.getArgument(2, PubSubTopicPartition.class);
      long offset = invocation.getArgument(3, Long.class);
      KafkaConsumerService kafkaConsumerService;
      int kafkaClusterId;
      boolean local = kafkaUrl.equals(inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
      if (local) {
        kafkaConsumerService = localKafkaConsumerService;
        kafkaClusterId = 0;
      } else {
        kafkaConsumerService = remoteKafkaConsumerService;
        kafkaClusterId = 1;
      }
      StorePartitionDataReceiver dataReceiver =
          new StorePartitionDataReceiver(storeIngestionTask, topicPartition, kafkaUrl, kafkaClusterId);
      kafkaConsumerService.startConsumptionIntoDataReceiver(topicPartition, offset, dataReceiver);

      if (local) {
        localConsumedDataReceiver = dataReceiver;
      } else {
        remoteConsumedDataReceiver = dataReceiver;
      }

      return null;
    }).when(aggKafkaConsumerService).subscribeConsumerFor(anyString(), any(), any(), anyLong());

    doAnswer(invocation -> {
      PubSubTopic versionTopic = invocation.getArgument(0, PubSubTopic.class);
      return localKafkaConsumerService.hasAnySubscriptionFor(versionTopic)
          || remoteKafkaConsumerService.hasAnySubscriptionFor(versionTopic);
    }).when(aggKafkaConsumerService).hasAnyConsumerAssignedForVersionTopic(any());

    doAnswer(invocation -> {
      PubSubTopicPartition pubSubTopicPartition = invocation.getArgument(1, PubSubTopicPartition.class);
      return inMemoryLocalKafkaConsumer.hasSubscription(pubSubTopicPartition)
          || inMemoryRemoteKafkaConsumer.hasSubscription(pubSubTopicPartition);
    }).when(aggKafkaConsumerService).hasConsumerAssignedFor(any(), any());

    doAnswer(invocation -> {
      String kafkaUrl = invocation.getArgument(0, String.class);
      PubSubTopicPartition pubSubTopicPartition = invocation.getArgument(2, PubSubTopicPartition.class);
      if (kafkaUrl.equals(inMemoryLocalKafkaBroker.getKafkaBootstrapServer())) {
        return inMemoryLocalKafkaConsumer.hasSubscription(pubSubTopicPartition);
      }
      return inMemoryRemoteKafkaConsumer.hasSubscription(pubSubTopicPartition);
    }).when(aggKafkaConsumerService).hasConsumerAssignedFor(anyString(), any(), any());

    doAnswer(invocation -> {
      PubSubTopic versionTopic = invocation.getArgument(0, PubSubTopic.class);
      Set<PubSubTopicPartition> topicPartitions = invocation.getArgument(1, Set.class);
      /**
       * The internal {@link SharedKafkaConsumer} has special logic for unsubscription to avoid some race condition
       * between the fast unsubscribe and re-subscribe.
       * Please check {@link SharedKafkaConsumer#unSubscribe} to find more details.
       *
       * We shouldn't use {@link #mockLocalKafkaConsumer} or {@link #inMemoryRemoteKafkaConsumer} here since
       * they don't have the proper synchronization.
       */
      localKafkaConsumerService.batchUnsubscribe(versionTopic, topicPartitions);
      remoteKafkaConsumerService.batchUnsubscribe(versionTopic, topicPartitions);
      return null;
    }).when(aggKafkaConsumerService).batchUnsubscribeConsumerFor(any(), anySet());

    doAnswer(invocation -> {
      PubSubTopic versionTopic = invocation.getArgument(0, PubSubTopic.class);
      PubSubTopicPartition pubSubTopicPartition = invocation.getArgument(1, PubSubTopicPartition.class);
      /**
       * The internal {@link SharedKafkaConsumer} has special logic for unsubscription to avoid some race condition
       * between the fast unsubscribe and re-subscribe.
       * Please check {@link SharedKafkaConsumer#unSubscribe} to find more details.
       *
       * We shouldn't use {@link #mockLocalKafkaConsumer} or {@link #inMemoryRemoteKafkaConsumer} here since
       * they don't have the proper synchronization.
       */
      if (inMemoryLocalKafkaConsumer.hasSubscription(pubSubTopicPartition)) {
        localKafkaConsumerService.unSubscribe(versionTopic, pubSubTopicPartition);
      }
      if (inMemoryRemoteKafkaConsumer.hasSubscription(pubSubTopicPartition)) {
        remoteKafkaConsumerService.unSubscribe(versionTopic, pubSubTopicPartition);
      }
      return null;
    }).when(aggKafkaConsumerService).unsubscribeConsumerFor(any(), any());

    doAnswer(invocation -> {
      PubSubTopicPartition pubSubTopicPartition = invocation.getArgument(1, PubSubTopicPartition.class);
      if (inMemoryLocalKafkaConsumer.hasSubscription(pubSubTopicPartition)) {
        inMemoryLocalKafkaConsumer.resetOffset(pubSubTopicPartition);
      }
      if (inMemoryRemoteKafkaConsumer.hasSubscription(pubSubTopicPartition)) {
        inMemoryRemoteKafkaConsumer.resetOffset(pubSubTopicPartition);
      }
      return null;
    }).when(aggKafkaConsumerService).resetOffsetFor(any(), any());

    doAnswer(invocation -> {
      PubSubTopicPartition pubSubTopicPartition = invocation.getArgument(1, PubSubTopicPartition.class);
      if (inMemoryLocalKafkaConsumer.hasSubscription(pubSubTopicPartition)) {
        inMemoryLocalKafkaConsumer.pause(pubSubTopicPartition);
      }
      if (inMemoryRemoteKafkaConsumer.hasSubscription(pubSubTopicPartition)) {
        inMemoryRemoteKafkaConsumer.pause(pubSubTopicPartition);
      }
      return null;
    }).when(aggKafkaConsumerService).pauseConsumerFor(any(), any());

    doAnswer(invocation -> {
      PubSubTopic versionTopic = invocation.getArgument(0, PubSubTopic.class);
      if (localKafkaConsumerService.hasAnySubscriptionFor(versionTopic)) {
        return Collections.singleton(inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
      }
      return Collections.emptySet();
    }).when(aggKafkaConsumerService).getKafkaUrlsFor(any());

    doAnswer(invocation -> {
      PubSubTopicPartition pubSubTopicPartition = invocation.getArgument(1, PubSubTopicPartition.class);
      if (inMemoryLocalKafkaConsumer.hasSubscription(pubSubTopicPartition)) {
        inMemoryLocalKafkaConsumer.resume(pubSubTopicPartition);
      }
      if (inMemoryRemoteKafkaConsumer.hasSubscription(pubSubTopicPartition)) {
        inMemoryRemoteKafkaConsumer.resume(pubSubTopicPartition);
      }
      return null;
    }).when(aggKafkaConsumerService).resumeConsumerFor(any(), any());
  }

  void setDefaultStoreVersionStateSupplier() {
    setStoreVersionStateSupplier(new StoreVersionState());
  }

  void setupMockAbstractStorageEngine(AbstractStoragePartition metadataPartition) {
    mockAbstractStorageEngine = mock(AbstractStorageEngine.class);
    doReturn(metadataPartition).when(mockAbstractStorageEngine).createStoragePartition(any());
    doReturn(true).when(mockAbstractStorageEngine).checkDatabaseIntegrity(anyInt(), any(), any());
  }

  void setStoreVersionStateSupplier(StoreVersionState svs) {
    storeVersionStateSupplier = () -> svs;
    AbstractStoragePartition metadataPartition = mock(AbstractStoragePartition.class);
    InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer =
        AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer();
    doReturn(storeVersionStateSerializer.serialize(null, svs)).when(metadataPartition).get(any(byte[].class));
    setupMockAbstractStorageEngine(metadataPartition);
    doReturn(svs).when(mockAbstractStorageEngine).getStoreVersionState();
    doReturn(svs).when(mockStorageMetadataService).getStoreVersionState(topic);
  }

  void setStoreVersionStateSupplier(boolean sorted) {
    StoreVersionState storeVersionState = new StoreVersionState();
    storeVersionState.sorted = sorted;
    setStoreVersionStateSupplier(storeVersionState);
  }

  private PubSubTopicPartitionOffset getTopicPartitionOffsetPair(PubSubProduceResult produceResult) {
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(
        pubSubTopicRepository.getTopic(produceResult.getTopic()),
        produceResult.getPartition());
    return new PubSubTopicPartitionOffset(pubSubTopicPartition, produceResult.getOffset());
  }

  private PubSubTopicPartitionOffset getTopicPartitionOffsetPair(String topic, int partition, long offset) {
    return new PubSubTopicPartitionOffset(
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), partition),
        offset);
  }

  /**
   * Verifies that the VeniceMessages from KafkaConsumer are processed appropriately as follows:
   * 1. A VeniceMessage with PUT requests leads to invoking of AbstractStorageEngine#put.
   * 2. A VeniceMessage with DELETE requests leads to invoking of AbstractStorageEngine#delete.
   * 3. A VeniceMessage with a Kafka offset that was already processed is ignored.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testVeniceMessagesProcessing(boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    PubSubProduceResult putMetadata = (PubSubProduceResult) localVeniceWriter
        .put(putKeyFoo, putValue, EXISTING_SCHEMA_ID, PUT_KEY_FOO_TIMESTAMP, null)
        .get();
    PubSubProduceResult deleteMetadata =
        (PubSubProduceResult) localVeniceWriter.delete(deleteKeyFoo, DELETE_KEY_FOO_TIMESTAMP, null).get();

    Queue<AbstractPollStrategy> pollStrategies = new LinkedList<>();
    pollStrategies.add(new RandomPollStrategy());

    // We re-deliver the old put out of order, so we can make sure it's ignored.
    Queue<PubSubTopicPartitionOffset> pollDeliveryOrder = new LinkedList<>();
    pollDeliveryOrder.add(getTopicPartitionOffsetPair(putMetadata));
    pollStrategies.add(new ArbitraryOrderingPollStrategy(pollDeliveryOrder));

    PollStrategy pollStrategy = new CompositePollStrategy(pollStrategies);

    runTest(pollStrategy, Utils.setOf(PARTITION_FOO), () -> {}, () -> {
      // Verify it retrieves the offset from the OffSet Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).getLastOffset(topic, PARTITION_FOO);
      verifyPutAndDelete(1, isActiveActiveReplicationEnabled, true);
      // Verify it commits the offset to Offset Manager
      OffsetRecord expectedOffsetRecordForDeleteMessage = getOffsetRecord(deleteMetadata.getOffset());
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS))
          .put(topic, PARTITION_FOO, expectedOffsetRecordForDeleteMessage);

      verify(mockVersionedStorageIngestionStats, timeout(TEST_TIMEOUT_MS).atLeast(3))
          .recordConsumedRecordEndToEndProcessingLatency(any(), eq(1), anyDouble(), anyLong());
    }, isActiveActiveReplicationEnabled);

    // verify the shared consumer should be detached when the ingestion task is closed.
    verify(aggKafkaConsumerService).unsubscribeAll(pubSubTopic);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testAmplificationFactor(boolean isActiveActiveReplicationEnabled) throws Exception {
    final int amplificationFactor = 2;
    inMemoryLocalKafkaBroker
        .createTopic(Version.composeRealTimeTopic(storeNameWithoutVersionInfo), PARTITION_COUNT / amplificationFactor);
    mockStorageMetadataService = new InMemoryStorageMetadataService();

    AbstractStoragePartition mockStoragePartition = mock(AbstractStoragePartition.class);
    doReturn(mockStoragePartition).when(mockAbstractStorageEngine).getPartitionOrThrow(anyInt());
    doReturn(new ReentrantReadWriteLock()).when(mockAbstractStorageEngine).getRWLockForPartitionOrThrow(anyInt());

    doReturn(putKeyFooReplicationMetadataWithValueSchemaIdBytesDefault).when(mockStoragePartition)
        .getReplicationMetadata(putKeyFoo);
    doReturn(deleteKeyFooReplicationMetadataWithValueSchemaIdBytes).when(mockStoragePartition)
        .getReplicationMetadata(deleteKeyFoo);

    SchemaEntry schemaEntry = new SchemaEntry(1, "\"string\"");
    doReturn(schemaEntry).when(mockSchemaRepo).getSupersetOrLatestValueSchema(storeNameWithoutVersionInfo);

    VeniceWriter vtWriter =
        getVeniceWriter(topic, new MockInMemoryProducerAdapter(inMemoryLocalKafkaBroker), amplificationFactor);
    VeniceWriter rtWriter = getVeniceWriter(
        Version.composeRealTimeTopic(storeNameWithoutVersionInfo),
        new MockInMemoryProducerAdapter(inMemoryLocalKafkaBroker),
        1);
    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(
        100,
        100,
        HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP);

    runTest(new RandomPollStrategy(), Utils.setOf(PARTITION_FOO), () -> {}, () -> {
      vtWriter.broadcastStartOfPush(new HashMap<>());
      vtWriter.broadcastEndOfPush(new HashMap<>());
      doReturn(vtWriter).when(mockWriterFactory).createVeniceWriter(any(VeniceWriterOptions.class));
      verify(mockLogNotifier, never()).completed(anyString(), anyInt(), anyLong());
      vtWriter.broadcastTopicSwitch(
          Collections.singletonList(inMemoryLocalKafkaBroker.getKafkaBootstrapServer()),
          Version.composeRealTimeTopic(storeNameWithoutVersionInfo),
          System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(10),
          new HashMap<>());
      storeIngestionTaskUnderTest.promoteToLeader(
          fooTopicPartition,
          new LeaderFollowerPartitionStateModel.LeaderSessionIdChecker(1, new AtomicLong(1)));
      try {
        rtWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID, PUT_KEY_FOO_TIMESTAMP, null).get();
        rtWriter.delete(deleteKeyFoo, DELETE_KEY_FOO_TIMESTAMP, null).get();

        verifyPutAndDelete(amplificationFactor, isActiveActiveReplicationEnabled, false);
      } catch (Exception e) {
        e.printStackTrace();
      }
    },
        Optional.of(hybridStoreConfig),
        false,
        Optional.empty(),
        isActiveActiveReplicationEnabled,
        amplificationFactor,
        Collections.singletonMap(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 3L));
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testMissingMessagesForTopicWithLogCompactionEnabled(boolean isActiveActiveReplicationEnabled)
      throws Exception {
    // enable log compaction
    when(mockTopicManager.isTopicCompactionEnabled(pubSubTopic)).thenReturn(true);

    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    PubSubProduceResult putMetadata1 =
        (PubSubProduceResult) localVeniceWriter.put(putKeyFoo, putValueToCorrupt, SCHEMA_ID).get();
    localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID).get();
    PubSubProduceResult putMetadata3 =
        (PubSubProduceResult) localVeniceWriter.put(putKeyFoo2, putValueToCorrupt, SCHEMA_ID).get();
    PubSubProduceResult putMetadata4 =
        (PubSubProduceResult) localVeniceWriter.put(putKeyFoo2, putValue, SCHEMA_ID).get();

    Queue<PubSubTopicPartitionOffset> pollDeliveryOrder = new LinkedList<>();
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
      OffsetRecord expectedOffsetRecordForLastMessage = getOffsetRecord(putMetadata4.getOffset());
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS))
          .put(topic, PARTITION_FOO, expectedOffsetRecordForLastMessage);
    }, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testVeniceMessagesProcessingWithExistingSchemaId(boolean isActiveActiveReplicationEnabled)
      throws Exception {
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
    }, isActiveActiveReplicationEnabled);
  }

  /**
   * Test the situation where records arrive faster than the schemas.
   * In this case, Venice would keep polling schemaRepo until schemas arrive.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testVeniceMessagesProcessingWithTemporarilyNotAvailableSchemaId(boolean isActiveActiveReplicationEnabled)
      throws Exception {
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
      verify(mockSchemaRepo, timeout(TEST_TIMEOUT_MS).atLeast(3))
          .hasValueSchema(storeNameWithoutVersionInfo, NON_EXISTING_SCHEMA_ID);
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS)).put(
          PARTITION_FOO,
          putKeyFoo,
          ByteBuffer.wrap(ValueRecord.create(NON_EXISTING_SCHEMA_ID, putValue).serialize()));

      // Verify that the following record is consumed well.
      verify(mockSchemaRepo, timeout(TEST_TIMEOUT_MS)).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS))
          .put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(EXISTING_SCHEMA_ID, putValue).serialize()));

      OffsetRecord expected = getOffsetRecord(existingSchemaOffset);
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).put(topic, PARTITION_FOO, expected);
    }, isActiveActiveReplicationEnabled);
  }

  /**
   * Test the situation where records' schemas never arrive. In the case, the StoreIngestionTask will keep being blocked.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testVeniceMessagesProcessingWithNonExistingSchemaId(boolean isActiveActiveReplicationEnabled)
      throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, NON_EXISTING_SCHEMA_ID);
    localVeniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID);

    doReturn(false).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, NON_EXISTING_SCHEMA_ID);
    doReturn(true).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      // Verify it retrieves the offset from the OffSet Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).getLastOffset(topic, PARTITION_FOO);

      // StoreIngestionTask#checkValueSchemaAvail will keep polling for 'NON_EXISTING_SCHEMA_ID'. It blocks the
      // #putConsumerRecord and will not enter drainer queue
      verify(mockSchemaRepo, after(TEST_TIMEOUT_MS).never())
          .hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);
      verify(mockSchemaRepo, atLeastOnce()).hasValueSchema(storeNameWithoutVersionInfo, NON_EXISTING_SCHEMA_ID);
      verify(mockAbstractStorageEngine, never()).put(eq(PARTITION_FOO), any(), any(byte[].class));

      // Only two records(start_of_segment, start_of_push) offset were able to be recorded before
      // 'NON_EXISTING_SCHEMA_ID' blocks #putConsumerRecord
      verify(mockStorageMetadataService, atMost(2)).put(eq(topic), eq(PARTITION_FOO), any(OffsetRecord.class));
    }, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testReportStartWhenRestarting(boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    final long STARTING_OFFSET = 2;
    runTest(Utils.setOf(PARTITION_FOO, PARTITION_BAR), () -> {
      doReturn(getOffsetRecord(STARTING_OFFSET)).when(mockStorageMetadataService).getLastOffset(anyString(), anyInt());
    }, () -> {
      // Verify STARTED is NOT reported when offset is 0
      verify(mockLogNotifier, never()).started(topic, PARTITION_BAR);
    }, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testNotifier(boolean isActiveActiveReplicationEnabled) throws Exception {
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
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce())
          .completed(topic, PARTITION_FOO, fooLastOffset + 1, "STANDBY");
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce())
          .completed(topic, PARTITION_BAR, barLastOffset + 1, "STANDBY");
      verify(mockPartitionStatusNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce())
          .completed(topic, PARTITION_FOO, fooLastOffset + 1, "STANDBY");
      verify(mockPartitionStatusNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce())
          .completed(topic, PARTITION_BAR, barLastOffset + 1, "STANDBY");
      verify(mockLeaderFollowerStateModelNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce())
          .catchUpVersionTopicOffsetLag(topic, PARTITION_FOO);
      verify(mockLeaderFollowerStateModelNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce())
          .catchUpVersionTopicOffsetLag(topic, PARTITION_BAR);
      verify(mockLeaderFollowerStateModelNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce())
          .completed(topic, PARTITION_FOO, fooLastOffset + 1, "STANDBY");
      verify(mockLeaderFollowerStateModelNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce())
          .completed(topic, PARTITION_BAR, barLastOffset + 1, "STANDBY");
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
    }, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testReadyToServePartition(boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      Store mockStore = mock(Store.class);
      doReturn(true).when(mockStore).isHybrid();
      doReturn(Optional.of(new VersionImpl("storeName", 1))).when(mockStore).getVersion(1);
      doReturn(storeNameWithoutVersionInfo).when(mockStore).getName();
      doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeNameWithoutVersionInfo);
    }, () -> {
      verify(mockAbstractStorageEngine, never()).preparePartitionForReading(PARTITION_BAR);
      // mockAbstractStorageEngine.preparePartitionForReading will hold the lock, do not use
      // "verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS)).preparePartitionForReading(PARTITION_FOO)";
      // otherwise, the lock will be hold for time "TEST_TIMEOUT_MS"
      TestUtils.waitForNonDeterministicAssertion(
          10,
          TimeUnit.SECONDS,
          () -> verify(mockAbstractStorageEngine, atLeastOnce()).preparePartitionForReading(PARTITION_FOO));
    }, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testReadyToServePartitionValidateIngestionSuccess(boolean isActiveActiveReplicationEnabled)
      throws Exception {
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
    new StoragePartitionConfig(topic, PARTITION_FOO);

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      verify(mockAbstractStorageEngine, never()).preparePartitionForReading(PARTITION_FOO);
    }, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testReadyToServePartitionWriteOnly(boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());
    Store mockStore = mock(Store.class);

    doReturn(mockStore).when(mockMetadataRepo).getStore(storeNameWithoutVersionInfo);
    doReturn(true).when(mockStore).isHybrid();
    mockAbstractStorageEngine.addStoragePartition(PARTITION_FOO);

    doReturn(storeNameWithoutVersionInfo).when(mockStore).getName();
    StoragePartitionConfig storagePartitionConfigFoo = new StoragePartitionConfig(topic, PARTITION_FOO);
    storagePartitionConfigFoo.setWriteOnlyConfig(true);
    new StoragePartitionConfig(topic, PARTITION_BAR);

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      verify(mockAbstractStorageEngine, never()).preparePartitionForReading(PARTITION_FOO);
      verify(mockAbstractStorageEngine, never()).preparePartitionForReading(PARTITION_BAR);
    }, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testResetPartition(boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID).get();

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS))
          .put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(SCHEMA_ID, putValue).serialize()));

      storeIngestionTaskUnderTest.resetPartitionConsumptionOffset(fooTopicPartition);

      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).clearOffset(topic, PARTITION_FOO);
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS).times(2))
          .put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(SCHEMA_ID, putValue).serialize()));
    }, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testResetPartitionAfterUnsubscription(boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID).get();

    doThrow(new UnsubscribedTopicPartitionException(fooTopicPartition)).when(mockLocalKafkaConsumer)
        .resetOffset(fooTopicPartition);

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS))
          .put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(SCHEMA_ID, putValue).serialize()));
      storeIngestionTaskUnderTest.unSubscribePartition(fooTopicPartition);
      // Reset should be able to handle the scenario, when the topic partition has been unsubscribed.
      storeIngestionTaskUnderTest.resetPartitionConsumptionOffset(fooTopicPartition);
      verify(mockLocalKafkaConsumer, timeout(TEST_TIMEOUT_MS)).unSubscribe(fooTopicPartition);
      // StoreIngestionTask won't invoke consumer.resetOffset() if it already unsubscribe from that topic/partition
      verify(mockLocalKafkaConsumer, timeout(TEST_TIMEOUT_MS).times(0)).resetOffset(fooTopicPartition);
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).clearOffset(topic, PARTITION_FOO);
    }, isActiveActiveReplicationEnabled);
  }

  /**
   * In this test, partition FOO will complete successfully, but partition BAR will be missing a record.
   *
   * The {@link VeniceNotifier} should see the completion and error reported for the appropriate partitions.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testDetectionOfMissingRecord(boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    long fooLastOffset = getOffset(localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    long barOffsetToSkip = getOffset(localVeniceWriter.put(putKeyBar, putValue, SCHEMA_ID));
    localVeniceWriter.put(putKeyBar, putValue, SCHEMA_ID);
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    PollStrategy pollStrategy = new FilteringPollStrategy(
        new RandomPollStrategy(),
        Utils.setOf(new PubSubTopicPartitionOffset(barTopicPartition, barOffsetToSkip)));

    runTest(pollStrategy, Utils.setOf(PARTITION_FOO, PARTITION_BAR), () -> {}, () -> {
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce())
          .endOfPushReceived(topic, PARTITION_FOO, fooLastOffset);
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).error(
          eq(topic),
          eq(PARTITION_BAR),
          argThat(new NonEmptyStringMatcher()),
          argThat(new ExceptionClassMatcher(MissingDataException.class)));

      // After we verified that completed() and error() are called, the rest should be guaranteed to be finished, so no
      // need for timeouts

      verify(mockLogNotifier, atLeastOnce()).started(topic, PARTITION_FOO);
      verify(mockLogNotifier, atLeastOnce()).started(topic, PARTITION_BAR);
    }, isActiveActiveReplicationEnabled);
  }

  /**
   * In this test, partition FOO will complete normally, but partition BAR will contain a duplicate record. The
   * {@link VeniceNotifier} should see the completion for both partitions.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testSkippingOfDuplicateRecord(boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    long fooLastOffset = getOffset(localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    long barOffsetToDupe = getOffset(localVeniceWriter.put(putKeyBar, putValue, SCHEMA_ID));
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    PollStrategy pollStrategy = new DuplicatingPollStrategy(
        new RandomPollStrategy(),
        Utils.mutableSetOf(
            new PubSubTopicPartitionOffset(new PubSubTopicPartitionImpl(pubSubTopic, PARTITION_BAR), barOffsetToDupe)));

    runTest(pollStrategy, Utils.setOf(PARTITION_FOO, PARTITION_BAR), () -> {}, () -> {
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce())
          .endOfPushReceived(topic, PARTITION_FOO, fooLastOffset);
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce())
          .endOfPushReceived(topic, PARTITION_BAR, barOffsetToDupe);
      verify(mockLogNotifier, after(TEST_TIMEOUT_MS).never())
          .endOfPushReceived(topic, PARTITION_BAR, barOffsetToDupe + 1);

      // After we verified that completed() is called, the rest should be guaranteed to be finished, so no need for
      // timeouts

      verify(mockLogNotifier, atLeastOnce()).started(topic, PARTITION_FOO);
      verify(mockLogNotifier, atLeastOnce()).started(topic, PARTITION_BAR);
    }, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testThrottling(boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);
    localVeniceWriter.delete(deleteKeyFoo, null);

    runTest(new RandomPollStrategy(1), Utils.setOf(PARTITION_FOO), () -> {}, () -> {
      // START_OF_SEGMENT, START_OF_PUSH, PUT, DELETE
      verify(mockRecordsThrottler, timeout(TEST_TIMEOUT_MS).times(4)).maybeThrottle(1);
      verify(mockBandwidthThrottler, timeout(TEST_TIMEOUT_MS).times(4)).maybeThrottle(anyDouble());
    }, isActiveActiveReplicationEnabled);
  }

  /**
   * This test crafts a couple of invalid message types, and expects the {@link StoreIngestionTask} to fail fast. The
   * message in {@link #PARTITION_FOO} will receive a bad message type, whereas the message in {@link #PARTITION_BAR}
   * will receive a bad control message type.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testBadMessageTypesFailFast(boolean isActiveActiveReplicationEnabled) throws Exception {
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
      fail(
          "The message type " + badMessageTypeId + " is valid. "
              + "This test needs to be updated in order to send an invalid message type...");
    } catch (VeniceMessageException e) {
      // Good
    }

    try {
      ControlMessageType.valueOf(badMessageTypeId);
      fail(
          "The control message type " + badMessageTypeId + " is valid. "
              + "This test needs to be updated in order to send an invalid control message type...");
    } catch (VeniceMessageException e) {
      // Good
    }

    localVeniceWriter = getVeniceWriter(
        new TransformingProducerAdapter(
            new MockInMemoryProducerAdapter(inMemoryLocalKafkaBroker),
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
                default:
                  // do nothing
                  break;
              }

              return new TransformingProducerAdapter.SendMessageParameters(
                  topic,
                  key,
                  transformedMessageEnvelope,
                  partition);
            }));
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);
    localVeniceWriter.put(putKeyBar, putValue, SCHEMA_ID);

    runTest(Utils.setOf(PARTITION_FOO, PARTITION_BAR), () -> {
      verify(kafkaConsumerServiceStats, timeout(TEST_TIMEOUT_MS).atLeastOnce()).recordPollError();
    }, isActiveActiveReplicationEnabled);
  }

  /**
   * In this test, {@link #PARTITION_BAR} will finish a regular push, and then get some more messages afterwards,
   * including a corrupt message followed by a good one. We expect the Notifier to not report any errors after the
   * EOP.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testCorruptMessagesDoNotFailFastAfterEOP(boolean isActiveActiveReplicationEnabled) throws Exception {
    VeniceWriter veniceWriterForDataDuringPush =
        getVeniceWriter(new MockInMemoryProducerAdapter(inMemoryLocalKafkaBroker));
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

    LOGGER.info("lastOffsetBeforeEOP: {}, lastOffset: {}", lastOffsetBeforeEOP, lastOffset);

    try {
      runTest(Utils.setOf(PARTITION_BAR), () -> {

        TestUtils.waitForNonDeterministicCompletion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, () -> {
          for (Object[] args: mockNotifierProgress) {
            if (args[0].equals(topic) && args[1].equals(PARTITION_BAR) && ((long) args[2]) >= lastOffsetBeforeEOP) {
              return true;
            }
          }
          return false;
        });
        TestUtils.waitForNonDeterministicCompletion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, () -> {
          for (Object[] args: mockNotifierCompleted) {
            if (args[0].equals(topic) && args[1].equals(PARTITION_BAR) && ((long) args[2]) > lastOffsetBeforeEOP) {
              return true;
            }
          }
          return false;
        });
        for (Object[] args: mockNotifierError) {
          Assert.assertFalse(
              args[0].equals(topic) && args[1].equals(PARTITION_BAR) && ((String) args[2]).length() > 0
                  && args[3] instanceof CorruptDataException);
        }

      }, isActiveActiveReplicationEnabled);
    } catch (VerifyError e) {
      StringBuilder msg = new StringBuilder();
      ClassLoader cl = ClassLoader.getSystemClassLoader();
      URL[] urls = ((URLClassLoader) cl).getURLs();
      msg.append("VerifyError, possibly from junit or mockito version conflict. \nPrinting junit on classpath:\n");
      Arrays.asList(urls)
          .stream()
          .filter(url -> url.getFile().contains("junit"))
          .forEach(url -> msg.append(url + "\n"));
      msg.append("Printing mockito on classpath:\n");
      Arrays.asList(urls)
          .stream()
          .filter(url -> url.getFile().contains("mockito"))
          .forEach(url -> msg.append(url + "\n"));
      throw new VeniceException(msg.toString(), e);
    }
  }

  /**
   * In this test, {@link #PARTITION_FOO} will finish a regular push, and then get some more messages afterwards,
   * including a corrupt message followed by a missing message and a good one.
   * We expect the Notifier to not report any errors after the EOP.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testDIVErrorMessagesNotFailFastAfterEOP(boolean isActiveActiveReplicationEnabled) throws Exception {
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

    PollStrategy pollStrategy = new FilteringPollStrategy(
        new RandomPollStrategy(),
        Utils.setOf(
            new PubSubTopicPartitionOffset(new PubSubTopicPartitionImpl(pubSubTopic, PARTITION_FOO), fooOffsetToSkip)));

    LOGGER.info("lastOffsetBeforeEOP: {}, lastOffset: {}", lastOffsetBeforeEOP, lastOffset);

    runTest(pollStrategy, Utils.setOf(PARTITION_FOO), () -> {}, () -> {
      for (Object[] args: mockNotifierError) {
        Assert.assertFalse(
            args[0].equals(topic) && args[1].equals(PARTITION_FOO) && ((String) args[2]).length() > 0
                && args[3] instanceof FatalDataValidationException);
      }
    }, isActiveActiveReplicationEnabled);
  }

  /**
   * In this test, the {@link #PARTITION_FOO} will receive a well-formed message, while the {@link #PARTITION_BAR} will
   * receive a corrupt message. We expect the Notifier to report as such.
   * <p>
   * N.B.: There was an edge case where this test was flaky. The edge case is now fixed, but the invocationCount of 100
   * should ensure that if this test is ever made flaky again, it will be detected right away. The skipFailedInvocations
   * annotation parameter makes the test skip any invocation after the first failure.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, invocationCount = 100, skipFailedInvocations = true)
  public void testCorruptMessagesFailFast(boolean isActiveActiveReplicationEnabled) throws Exception {
    VeniceWriter veniceWriterForData = getCorruptedVeniceWriter(putValueToCorrupt, inMemoryLocalKafkaBroker);

    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    long fooLastOffset = getOffset(veniceWriterForData.put(putKeyFoo, putValue, SCHEMA_ID));
    getOffset(veniceWriterForData.put(putKeyBar, putValueToCorrupt, SCHEMA_ID));
    veniceWriterForData.close();
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    runTest(Utils.setOf(PARTITION_FOO, PARTITION_BAR), () -> {
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS))
          .completed(eq(topic), eq(PARTITION_FOO), LongEqualOrGreaterThanMatcher.get(fooLastOffset), eq("STANDBY"));

      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).error(
          eq(topic),
          eq(PARTITION_BAR),
          argThat(new NonEmptyStringMatcher()),
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
    }, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testSubscribeCompletedPartition(boolean isActiveActiveReplicationEnabled) throws Exception {
    final int offset = 100;
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    runTest(
        Utils.setOf(PARTITION_FOO),
        () -> doReturn(getOffsetRecord(offset, true)).when(mockStorageMetadataService)
            .getLastOffset(topic, PARTITION_FOO),
        () -> {
          verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).completed(topic, PARTITION_FOO, offset, "STANDBY");
        },
        isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testSubscribeCompletedPartitionUnsubscribe(boolean isActiveActiveReplicationEnabled) throws Exception {
    final int offset = 100;
    final long LONG_TEST_TIMEOUT = 2 * TEST_TIMEOUT_MS;
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    Map<String, Object> extraServerProperties = new HashMap<>();
    extraServerProperties.put(SERVER_UNSUB_AFTER_BATCHPUSH, true);

    runTest(new RandomPollStrategy(), Utils.setOf(PARTITION_FOO), () -> {
      Store mockStore = mock(Store.class);
      doReturn(1).when(mockStore).getCurrentVersion();
      doReturn(Optional.of(new VersionImpl("storeName", 1, Version.numberBasedDummyPushId(1)))).when(mockStore)
          .getVersion(1);
      doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeNameWithoutVersionInfo);
      doReturn(getOffsetRecord(offset, true)).when(mockStorageMetadataService).getLastOffset(topic, PARTITION_FOO);
    }, () -> {
      verify(mockLogNotifier, timeout(LONG_TEST_TIMEOUT)).completed(topic, PARTITION_FOO, offset, "STANDBY");
      verify(aggKafkaConsumerService, timeout(LONG_TEST_TIMEOUT))
          .batchUnsubscribeConsumerFor(pubSubTopic, Collections.singleton(fooTopicPartition));
      verify(aggKafkaConsumerService, never()).unsubscribeConsumerFor(pubSubTopic, barTopicPartition);
      verify(mockLocalKafkaConsumer, timeout(LONG_TEST_TIMEOUT))
          .batchUnsubscribe(Collections.singleton(fooTopicPartition));
      verify(mockLocalKafkaConsumer, never()).unSubscribe(barTopicPartition);
    }, this.hybridStoreConfig, false, Optional.empty(), isActiveActiveReplicationEnabled, 1, extraServerProperties);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testCompleteCalledWhenUnsubscribeAfterBatchPushDisabled(boolean isActiveActiveReplicationEnabled)
      throws Exception {
    final int offset = 10;
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      Store mockStore = mock(Store.class);
      storeIngestionTaskUnderTest.unSubscribePartition(fooTopicPartition);
      doReturn(1).when(mockStore).getCurrentVersion();
      doReturn(Optional.of(new VersionImpl("storeName", 1, Version.numberBasedDummyPushId(1)))).when(mockStore)
          .getVersion(1);
      doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeNameWithoutVersionInfo);
      doReturn(getOffsetRecord(offset, true)).when(mockStorageMetadataService).getLastOffset(topic, PARTITION_FOO);
    },
        () -> verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).completed(topic, PARTITION_FOO, offset, "STANDBY"),
        isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testUnsubscribeConsumption(boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).started(topic, PARTITION_FOO);
      // Start of push has already been consumed. Stop consumption
      storeIngestionTaskUnderTest.unSubscribePartition(fooTopicPartition);
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).stopped(anyString(), anyInt(), anyLong());
    }, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testKillConsumption(boolean isActiveActiveReplicationEnabled) throws Exception {
    final Thread writingThread = new Thread(() -> {
      while (true) {
        localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);
        localVeniceWriter.put(putKeyBar, putValue, SCHEMA_ID);
        if (Utils.sleep(READ_CYCLE_DELAY_MS)) {
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

        // Start of push has already been consumed. Stop consumption
        storeIngestionTaskUnderTest.kill();
        // task should report an error to notifier that it's killed.
        verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).error(
            eq(topic),
            eq(PARTITION_FOO),
            argThat(new NonEmptyStringMatcher()),
            argThat(new ExceptionClassMatcher(VeniceIngestionTaskKilledException.class)));
        verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).error(
            eq(topic),
            eq(PARTITION_BAR),
            argThat(new NonEmptyStringMatcher()),
            argThat(new ExceptionClassMatcher(VeniceIngestionTaskKilledException.class)));

        waitForNonDeterministicCompletion(
            TEST_TIMEOUT_MS,
            TimeUnit.MILLISECONDS,
            () -> storeIngestionTaskUnderTest.isRunning() == false);
      }, isActiveActiveReplicationEnabled);
    } finally {
      TestUtils.shutdownThread(writingThread);
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testKillActionPriority(boolean isActiveActiveReplicationEnabled) throws Exception {
    runTest(Utils.setOf(PARTITION_FOO), () -> {
      localVeniceWriter.broadcastStartOfPush(new HashMap<>());
      localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);
      // Add a reset consumer action
      storeIngestionTaskUnderTest.resetPartitionConsumptionOffset(fooTopicPartition);
      // Add a kill consumer action in higher priority than subscribe and reset.
      storeIngestionTaskUnderTest.kill();
    }, () -> {
      // verify subscribe has not been processed. Because consumption task should process kill action at first
      verify(mockStorageMetadataService, after(TEST_TIMEOUT_MS).never()).getLastOffset(topic, PARTITION_FOO);
      /**
       * Consumers are subscribed lazily; if the store ingestion task is killed before it tries to subscribe to any
       * topics, there is no consumer subscription.
       */
      waitForNonDeterministicCompletion(
          TEST_TIMEOUT_MS,
          TimeUnit.MILLISECONDS,
          () -> !storeIngestionTaskUnderTest.consumerHasAnySubscription());
      // Verify offset has not been processed. Because consumption task should process kill action at first.
      // offSetManager.clearOffset should only be invoked one time during clean up after killing this task.
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).clearOffset(topic, PARTITION_FOO);

      waitForNonDeterministicCompletion(
          TEST_TIMEOUT_MS,
          TimeUnit.MILLISECONDS,
          () -> storeIngestionTaskUnderTest.isRunning() == false);
    }, isActiveActiveReplicationEnabled);
  }

  private byte[] getNumberedKey(int number) {
    return ByteBuffer.allocate(putKeyFoo.length + Integer.BYTES).put(putKeyFoo).putInt(number).array();
  }

  private byte[] getNumberedValue(int number) {
    return ByteBuffer.allocate(putValue.length + Integer.BYTES).put(putValue).putInt(number).array();
  }

  @Test(dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testDataValidationCheckPointing(boolean sortedInput, boolean isActiveActiveReplicationEnabled)
      throws Exception {
    final Map<Integer, Long> maxOffsetPerPartition = new HashMap<>();
    final Map<Pair<Integer, ByteArray>, ByteArray> pushedRecords = new HashMap<>();
    final int totalNumberOfMessages = 1000;
    final int totalNumberOfConsumptionRestarts = 10;
    final long LONG_TEST_TIMEOUT = 2 * TEST_TIMEOUT_MS;

    setStoreVersionStateSupplier(sortedInput);
    localVeniceWriter.broadcastStartOfPush(sortedInput, new HashMap<>());
    for (int i = 0; i < totalNumberOfMessages; i++) {
      byte[] key = getNumberedKey(i);
      byte[] value = getNumberedValue(i);

      PubSubProduceResult produceResult = (PubSubProduceResult) localVeniceWriter.put(key, value, SCHEMA_ID).get();

      maxOffsetPerPartition.put(produceResult.getPartition(), produceResult.getOffset());
      pushedRecords.put(new Pair(produceResult.getPartition(), new ByteArray(key)), new ByteArray(value));
    }
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    // Basic sanity checks
    assertEquals(
        pushedRecords.size(),
        totalNumberOfMessages,
        "We did not produce as many unique records as we expected!");
    assertFalse(
        maxOffsetPerPartition.isEmpty(),
        "There should be at least one partition getting anything published into it!");

    Set<Integer> relevantPartitions = Utils.setOf(PARTITION_FOO);

    // This doesn't really need to be atomic, but it does need to be final, and int/Integer cannot be mutated.
    final AtomicInteger messagesConsumedSoFar = new AtomicInteger(0);
    PollStrategy pollStrategy =
        new BlockingObserverPollStrategy(new RandomPollStrategy(false), topicPartitionOffset -> {
          if (topicPartitionOffset == null || topicPartitionOffset.getOffset() == null) {
            LOGGER.info("Received null OffsetRecord!");
          } else if (messagesConsumedSoFar.incrementAndGet()
              % (totalNumberOfMessages / totalNumberOfConsumptionRestarts) == 0) {
            LOGGER.info("Restarting consumer after consuming {} messages so far.", messagesConsumedSoFar.get());
            relevantPartitions.stream()
                .forEach(
                    partition -> storeIngestionTaskUnderTest
                        .unSubscribePartition(new PubSubTopicPartitionImpl(pubSubTopic, partition)));
            relevantPartitions.stream()
                .forEach(
                    partition -> storeIngestionTaskUnderTest
                        .subscribePartition(new PubSubTopicPartitionImpl(pubSubTopic, partition), Optional.empty()));
          } else {
            LOGGER.info(
                "TopicPartition: {}, Offset: {}",
                topicPartitionOffset.getPubSubTopicPartition(),
                topicPartitionOffset.getOffset());
          }
        });

    runTest(pollStrategy, relevantPartitions, () -> {}, () -> {
      // Verify that all partitions reported success.
      maxOffsetPerPartition.entrySet()
          .stream()
          .filter(entry -> relevantPartitions.contains(entry.getKey()))
          .forEach(entry -> {
            int partition = entry.getKey();
            long offset = entry.getValue();
            LOGGER.info("Verifying completed was called for partition {} and offset {} or greater.", partition, offset);
            verify(mockLogNotifier, timeout(LONG_TEST_TIMEOUT).atLeastOnce())
                .completed(eq(topic), eq(partition), LongEqualOrGreaterThanMatcher.get(offset), eq("STANDBY"));
          });

      // After this, all asynchronous processing should be finished, so there's no need for time outs anymore.

      // Verify that no partitions reported errors.
      relevantPartitions.stream()
          .forEach(partition -> verify(mockLogNotifier, never()).error(eq(topic), eq(partition), anyString(), any()));

      // Verify that we really unsubscribed and re-subscribed.
      relevantPartitions.stream().forEach(partition -> {
        PubSubTopicPartition pubSubTopicPartition =
            new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), partition);
        verify(mockLocalKafkaConsumer, timeout(LONG_TEST_TIMEOUT).atLeast(totalNumberOfConsumptionRestarts + 1))
            .subscribe(eq(pubSubTopicPartition), anyLong());
        verify(mockLocalKafkaConsumer, timeout(LONG_TEST_TIMEOUT).atLeast(totalNumberOfConsumptionRestarts))
            .unSubscribe(eq(pubSubTopicPartition));

        if (sortedInput) {
          // Check database mode switches from deferred-write to transactional after EOP control message
          StoragePartitionConfig deferredWritePartitionConfig = new StoragePartitionConfig(topic, partition);
          deferredWritePartitionConfig.setDeferredWrite(true);
          // SOP control message and restart
          verify(mockAbstractStorageEngine, atLeast(1))
              .beginBatchWrite(eq(deferredWritePartitionConfig), any(), eq(Optional.empty()));
          StoragePartitionConfig transactionalPartitionConfig = new StoragePartitionConfig(topic, partition);
          // only happen after EOP control message
          verify(mockAbstractStorageEngine, times(1)).endBatchWrite(transactionalPartitionConfig);
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
        Assert.assertNotNull(storeIngestionTaskUnderTest.getPartitionConsumptionState(partition));
        PartitionConsumptionState pcs = storeIngestionTaskUnderTest.getPartitionConsumptionState(partition);
        Assert.assertTrue(pcs.getLatestProcessedUpstreamRTOffsetMap().isEmpty());
      });
    }, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testKillAfterPartitionIsCompleted(boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    long fooLastOffset = getOffset(localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      verify(mockLogNotifier, after(TEST_TIMEOUT_MS).never()).error(eq(topic), eq(PARTITION_FOO), anyString(), any());
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce()).started(topic, PARTITION_FOO);

      storeIngestionTaskUnderTest.kill();
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce())
          .endOfPushReceived(topic, PARTITION_FOO, fooLastOffset);
    }, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testNeverReportProgressBeforeStart(boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    // Read one message for each poll.
    runTest(new RandomPollStrategy(1), Utils.setOf(PARTITION_FOO), () -> {}, () -> {
      verify(mockLogNotifier, after(TEST_TIMEOUT_MS).never()).progress(topic, PARTITION_FOO, 0);
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce()).started(topic, PARTITION_FOO);
      // The current behavior is only to sync offset/report progress after processing a pre-configured amount
      // of messages in bytes, since control message is being counted as 0 bytes (no data persisted in disk),
      // then no progress will be reported during start, but only for processed messages.
      verify(mockLogNotifier, after(TEST_TIMEOUT_MS).never()).progress(any(), anyInt(), anyInt());
    }, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testOffsetPersistent(boolean isActiveActiveReplicationEnabled) throws Exception {
    // Do not persist every message.
    List<Long> offsets = new ArrayList<>();
    for (int i = 0; i < PARTITION_COUNT; i++) {
      offsets.add(5l);
    }
    databaseSyncBytesIntervalForTransactionalMode = 1000;
    try {
      localVeniceWriter.broadcastStartOfPush(new HashMap<>());
      localVeniceWriter.broadcastEndOfPush(new HashMap<>());
      /**
       * Persist for every control message except START_OF_SEGMENT and END_OF_SEGMENT:
       * START_OF_PUSH, END_OF_PUSH, START_OF_BUFFER_REPLAY
       */
      runTest(
          new RandomPollStrategy(),
          Utils.setOf(PARTITION_FOO),
          () -> {},
          () -> verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS).times(2))
              .put(eq(topic), eq(PARTITION_FOO), any()),
          Optional.of(
              new HybridStoreConfigImpl(
                  100,
                  100,
                  HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
                  DataReplicationPolicy.NON_AGGREGATE,
                  BufferReplayPolicy.REWIND_FROM_EOP)),
          false,
          Optional.empty(),
          isActiveActiveReplicationEnabled,
          1,
          Collections.emptyMap());
    } finally {
      databaseSyncBytesIntervalForTransactionalMode = 1;
    }

  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testVeniceMessagesProcessingWithSortedInput(boolean isActiveActiveReplicationEnabled) throws Exception {
    setStoreVersionStateSupplier(true);
    localVeniceWriter.broadcastStartOfPush(true, new HashMap<>());
    PubSubProduceResult putMetadata =
        (PubSubProduceResult) localVeniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID).get();
    PubSubProduceResult deleteMetadata = (PubSubProduceResult) localVeniceWriter.delete(deleteKeyFoo, null).get();
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      // Verify it retrieves the offset from the Offset Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).getLastOffset(topic, PARTITION_FOO);

      verifyPutAndDelete(1, isActiveActiveReplicationEnabled, true);

      // Verify it commits the offset to Offset Manager after receiving EOP control message
      OffsetRecord expectedOffsetRecordForDeleteMessage = getOffsetRecord(deleteMetadata.getOffset() + 1, true);
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS))
          .put(topic, PARTITION_FOO, expectedOffsetRecordForDeleteMessage);
      // Deferred write is not going to commit offset for every message, but will commit offset for every control
      // message
      // The following verification is for START_OF_PUSH control message
      verify(mockStorageMetadataService, times(1))
          .put(topic, PARTITION_FOO, getOffsetRecord(putMetadata.getOffset() - 1));
      // Check database mode switches from deferred-write to transactional after EOP control message
      StoragePartitionConfig deferredWritePartitionConfig = new StoragePartitionConfig(topic, PARTITION_FOO);
      deferredWritePartitionConfig.setDeferredWrite(true);
      verify(mockAbstractStorageEngine, times(1))
          .beginBatchWrite(eq(deferredWritePartitionConfig), any(), eq(Optional.empty()));
      StoragePartitionConfig transactionalPartitionConfig = new StoragePartitionConfig(topic, PARTITION_FOO);
      verify(mockAbstractStorageEngine, times(1)).endBatchWrite(transactionalPartitionConfig);
    }, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testVeniceMessagesProcessingWithSortedInputVerifyChecksum(boolean isActiveActiveReplicationEnabled)
      throws Exception {
    databaseChecksumVerificationEnabled = true;
    doReturn(false).when(rocksDBServerConfig).isRocksDBPlainTableFormatEnabled();
    setStoreVersionStateSupplier(true);
    localVeniceWriter.broadcastStartOfPush(true, new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID).get();
    // intentionally not sending the EOP so that expectedSSTFileChecksum calculation does not get reset.
    // veniceWriter.broadcastEndOfPush(new HashMap<>());

    CheckSum checksum = CheckSum.getInstance(CheckSumType.MD5);
    checksum.update(putKeyFoo);
    checksum.update(SCHEMA_ID);
    checksum.update(putValue);

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      // Verify it retrieves the offset from the Offset Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).getLastOffset(topic, PARTITION_FOO);

      // Verify StorageEngine#put is invoked only once and with appropriate key & value.
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS))
          .put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(SCHEMA_ID, putValue).serialize()));

      StoragePartitionConfig deferredWritePartitionConfig = new StoragePartitionConfig(topic, PARTITION_FOO);
      deferredWritePartitionConfig.setDeferredWrite(true);
      ArgumentCaptor<Optional<Supplier<byte[]>>> checksumCaptor = ArgumentCaptor.forClass(Optional.class);

      // verify the checksum matches.
      verify(mockAbstractStorageEngine, times(1))
          .beginBatchWrite(eq(deferredWritePartitionConfig), any(), checksumCaptor.capture());
      Optional<Supplier<byte[]>> checksumSupplier = checksumCaptor.getValue();
      Assert.assertTrue(checksumSupplier.isPresent());
      Assert.assertTrue(Arrays.equals(checksumSupplier.get().get(), checksum.getCheckSum()));
    }, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testDelayedTransitionToOnlineInHybridMode(boolean isActiveActiveReplicationEnabled) throws Exception {
    final long MESSAGES_BEFORE_EOP = 100;
    final long MESSAGES_AFTER_EOP = 100;
    mockStorageMetadataService = new InMemoryStorageMetadataService();
    hybridStoreConfig = Optional.of(
        new HybridStoreConfigImpl(
            10,
            20,
            HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
            DataReplicationPolicy.NON_AGGREGATE,
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

      localVeniceWriter.broadcastTopicSwitch(
          Collections.singletonList(inMemoryLocalKafkaBroker.getKafkaBootstrapServer()),
          Version.composeRealTimeTopic(storeNameWithoutVersionInfo),
          System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(10),
          Collections.emptyMap());

      for (int i = 0; i < MESSAGES_AFTER_EOP; i++) {
        try {
          localVeniceWriter.put(getNumberedKey(i), getNumberedValue(i), SCHEMA_ID).get();
        } catch (InterruptedException | ExecutionException e) {
          throw new VeniceException(e);
        }
      }

      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeast(ALL_PARTITIONS.size()))
          .completed(anyString(), anyInt(), anyLong(), anyString());
    }, isActiveActiveReplicationEnabled);
  }

  /**
   * This test writes a message to Kafka then creates a StoreIngestionTask (and StoreBufferDrainer)  It also passes a DiskUsage
   * object to the StoreIngestionTask that always reports disk full.  This means when the StoreBufferDrainer tries to persist
   * the record, it will receive a disk full error.  This test checks for that disk full error on the Notifier object.
   * @throws Exception
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testStoreIngestionTaskRespectsDiskUsage(boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID);
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());
    DiskUsage diskFullUsage = mock(DiskUsage.class);
    doReturn(true).when(diskFullUsage).isDiskFull(anyLong());
    doReturn("mock disk full disk usage").when(diskFullUsage).getDiskStatus();
    doReturn(true).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);

    runTest(
        new RandomPollStrategy(),
        Utils.setOf(PARTITION_FOO),
        () -> {},
        () -> waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
          // If the partition already got EndOfPushReceived, then all errors will be suppressed and not reported.
          // The speed for a partition to get EOP is non-deterministic, adds the if check here to make this test not
          // flaky.
          if (mockNotifierEOPReceived.isEmpty()) {
            Assert.assertFalse(mockNotifierError.isEmpty(), "Disk Usage should have triggered an ingestion error");
            String errorMessages = mockNotifierError.stream()
                .map(o -> ((Exception) o[3]).getMessage()) // elements in object array are 0:store name (String), 1:
                                                           // partition (int), 2: message (String), 3: cause (Exception)
                .collect(Collectors.joining());
            Assert.assertTrue(
                errorMessages.contains("Disk is full"),
                "Expecting disk full error, found following error messages instead: " + errorMessages);
          } else {
            LOGGER.info("EOP was received, and therefore this test cannot perform its assertions.");
          }
        }),
        Optional.empty(),
        false,
        Optional.of(diskFullUsage),
        isActiveActiveReplicationEnabled,
        1,
        Collections.emptyMap());
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, invocationCount = 10)
  public void testIncrementalPush(boolean isActiveActiveReplicationEnabled) throws Exception {
    setStoreVersionStateSupplier(true);
    localVeniceWriter.broadcastStartOfPush(true, new HashMap<>());
    long fooOffset = getOffset(localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    String version = String.valueOf(System.currentTimeMillis());
    localVeniceWriter.broadcastStartOfIncrementalPush(version, new HashMap<>());
    long fooNewOffset = getOffset(localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    localVeniceWriter.broadcastEndOfIncrementalPush(version, new HashMap<>());
    // Records order are: StartOfSeg, StartOfPush, data, EndOfPush, EndOfSeg, StartOfSeg, StartOfIncrementalPush
    // data, EndOfIncrementalPush

    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(
        100,
        100,
        HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP);

    runTest(new RandomPollStrategy(), Utils.setOf(PARTITION_FOO), () -> {}, () -> {
      waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        // sync the offset when receiving EndOfPush
        verify(mockStorageMetadataService).put(eq(topic), eq(PARTITION_FOO), eq(getOffsetRecord(fooOffset + 1, true)));
        // sync the offset when receiving StartOfIncrementalPush and EndOfIncrementalPush
        verify(mockStorageMetadataService)
            .put(eq(topic), eq(PARTITION_FOO), eq(getOffsetRecord(fooNewOffset - 1, true)));
        verify(mockStorageMetadataService)
            .put(eq(topic), eq(PARTITION_FOO), eq(getOffsetRecord(fooNewOffset + 1, true)));

        verify(mockLogNotifier, atLeastOnce()).started(topic, PARTITION_FOO);

        // since notifier reporting happens before offset update, it actually reports previous offsets
        verify(mockLogNotifier, atLeastOnce()).endOfPushReceived(topic, PARTITION_FOO, fooOffset);
        verify(mockLogNotifier, atLeastOnce())
            .endOfIncrementalPushReceived(topic, PARTITION_FOO, fooNewOffset, version);
      });
    },
        Optional.of(hybridStoreConfig),
        true,
        Optional.empty(),
        isActiveActiveReplicationEnabled,
        1,
        Collections.emptyMap());
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testSchemaCacheWarming(boolean isActiveActiveReplicationEnabled) throws Exception {
    setStoreVersionStateSupplier(true);
    localVeniceWriter.broadcastStartOfPush(true, new HashMap<>());
    long fooOffset = getOffset(localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());
    SchemaEntry schemaEntry = new SchemaEntry(1, STRING_SCHEMA);
    // Records order are: StartOfSeg, StartOfPush, data, EndOfPush, EndOfSeg
    runTest(new RandomPollStrategy(), Utils.setOf(PARTITION_FOO), () -> {
      Store mockStore = mock(Store.class);
      doReturn(true).when(mockStore).isReadComputationEnabled();
      doReturn(true).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);
      doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeNameWithoutVersionInfo);
      doReturn(schemaEntry).when(mockSchemaRepo).getValueSchema(anyString(), anyInt());
      doReturn(Optional.of(new VersionImpl("storeName", 1))).when(mockStore).getVersion(1);
    }, () -> waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      verify(mockLogNotifier, atLeastOnce()).started(topic, PARTITION_FOO);
      // since notifier reporting happens before offset update, it actually reports previous offsets
      verify(mockLogNotifier, atLeastOnce()).endOfPushReceived(topic, PARTITION_FOO, fooOffset);
      // Since the completion report will be async, the completed offset could be `END_OF_PUSH` or `END_OF_SEGMENT` for
      // batch push job.
      verify(mockLogNotifier).completed(
          eq(topic),
          eq(PARTITION_FOO),
          longThat(completionOffset -> (completionOffset == fooOffset + 1) || (completionOffset == fooOffset + 2)),
          eq("STANDBY"));
    }),
        Optional.empty(),
        false,
        Optional.empty(),
        isActiveActiveReplicationEnabled,
        1,
        Collections.singletonMap(SERVER_NUM_SCHEMA_FAST_CLASS_WARMUP, 1));
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testReportErrorWithEmptyPcsMap(boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID);
    // Dummy exception to put ingestion task into ERROR state
    doThrow(new VeniceException("fake exception")).when(mockVersionedStorageIngestionStats)
        .resetIngestionTaskPushTimeoutGauge(anyString(), anyInt());

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).error(eq(topic), eq(PARTITION_FOO), anyString(), any());
    }, isActiveActiveReplicationEnabled);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = TEST_TIMEOUT_MS * 5)
  public void testPartitionExceptionIsolation(boolean isActiveActiveReplicationEnabled) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    getOffset(localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    long barLastOffset = getOffset(localVeniceWriter.put(putKeyBar, putValue, SCHEMA_ID));
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    doThrow(new VeniceException("fake storage engine exception")).when(mockAbstractStorageEngine)
        .put(eq(PARTITION_FOO), any(), any(ByteBuffer.class));

    runTest(Utils.setOf(PARTITION_FOO, PARTITION_BAR), () -> {
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS))
          .completed(eq(topic), eq(PARTITION_BAR), LongEqualOrGreaterThanMatcher.get(barLastOffset), eq("STANDBY"));

      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).error(eq(topic), eq(PARTITION_FOO), anyString(), any());
      verify(mockLogNotifier, never()).completed(eq(topic), eq(PARTITION_FOO), anyLong());
      // Error partition should be unsubscribed
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).stopped(eq(topic), eq(PARTITION_FOO), anyLong());
      verify(mockLogNotifier, never()).error(eq(topic), eq(PARTITION_BAR), anyString(), any());
      assertTrue(storeIngestionTaskUnderTest.isRunning(), "The StoreIngestionTask should still be running");
      assertNull(
          storeIngestionTaskUnderTest.getPartitionIngestionExceptionList().get(PARTITION_FOO),
          "Exception for the errored partition should be cleared after unsubscription");
    }, isActiveActiveReplicationEnabled);
    for (int i = 0; i < 10000; ++i) {
      storeIngestionTaskUnderTest
          .setIngestionException(0, new VeniceException("new fake looooooooooooooooong exception"));
    }
  }

  private VeniceServerConfig buildVeniceServerConfig(Map<String, Object> extraProperties) {
    PropertyBuilder propertyBuilder = new PropertyBuilder();
    propertyBuilder.put(CLUSTER_NAME, "");
    propertyBuilder.put(ZOOKEEPER_ADDRESS, "");
    propertyBuilder.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 500L);
    propertyBuilder.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
    propertyBuilder.put(HYBRID_QUOTA_ENFORCEMENT_ENABLED, false);
    propertyBuilder.put(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, databaseChecksumVerificationEnabled);
    propertyBuilder.put(SERVER_LOCAL_CONSUMER_CONFIG_PREFIX, VeniceProperties.empty());
    propertyBuilder.put(SERVER_REMOTE_CONSUMER_CONFIG_PREFIX, VeniceProperties.empty());
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

    return new VeniceServerConfig(propertyBuilder.build(), kafkaClusterMap);
  }

  private void verifyPutAndDelete(
      int amplificationFactor,
      boolean isActiveActiveReplicationEnabled,
      boolean recordsInBatchPush) {
    VenicePartitioner partitioner = getVenicePartitioner(amplificationFactor);
    int targetPartitionPutKeyFoo = partitioner.getPartitionId(putKeyFoo, PARTITION_COUNT);
    int targetPartitionDeleteKeyFoo = partitioner.getPartitionId(deleteKeyFoo, PARTITION_COUNT);

    // Batch push records for Active/Active do not persist replication metadata.
    if (isActiveActiveReplicationEnabled && !recordsInBatchPush) {
      // Verify StorageEngine#putWithReplicationMetadata is invoked only once and with appropriate key & value.
      verify(mockAbstractStorageEngine, timeout(100000)).putWithReplicationMetadata(
          targetPartitionPutKeyFoo,
          putKeyFoo,
          ByteBuffer.wrap(ValueRecord.create(EXISTING_SCHEMA_ID, putValue).serialize()),
          putKeyFooReplicationMetadataWithValueSchemaIdBytes);
      // Verify StorageEngine#deleteWithReplicationMetadata is invoked only once and with appropriate key.
      verify(mockAbstractStorageEngine, timeout(100000)).deleteWithReplicationMetadata(
          targetPartitionDeleteKeyFoo,
          deleteKeyFoo,
          deleteKeyFooReplicationMetadataWithValueSchemaIdBytes);

      // Verify StorageEngine#put is never invoked for put operation.
      verify(mockAbstractStorageEngine, never())
          .put(eq(targetPartitionPutKeyFoo), eq(putKeyFoo), any(ByteBuffer.class));
      // Verify StorageEngine#Delete is never invoked for delete operation.
      verify(mockAbstractStorageEngine, never()).delete(eq(targetPartitionDeleteKeyFoo), eq(deleteKeyFoo));
    } else {
      // Verify StorageEngine#put is invoked only once and with appropriate key & value.
      verify(mockAbstractStorageEngine, timeout(100000)).put(
          targetPartitionPutKeyFoo,
          putKeyFoo,
          ByteBuffer.wrap(ValueRecord.create(EXISTING_SCHEMA_ID, putValue).serialize()));
      // Verify StorageEngine#Delete is invoked only once and with appropriate key.
      verify(mockAbstractStorageEngine, timeout(100000)).delete(targetPartitionDeleteKeyFoo, deleteKeyFoo);

      // Verify StorageEngine#putWithReplicationMetadata is never invoked for put operation.
      verify(mockAbstractStorageEngine, never()).putWithReplicationMetadata(
          targetPartitionPutKeyFoo,
          putKeyFoo,
          ByteBuffer.wrap(ValueRecord.create(EXISTING_SCHEMA_ID, putValue).serialize()),
          putKeyFooReplicationMetadataWithValueSchemaIdBytes);
      // Verify StorageEngine#deleteWithReplicationMetadata is never invoked for delete operation..
      verify(mockAbstractStorageEngine, never()).deleteWithReplicationMetadata(
          targetPartitionDeleteKeyFoo,
          deleteKeyFoo,
          deleteKeyFooReplicationMetadataWithValueSchemaIdBytes);
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

    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(
        100,
        100,
        HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP);

    MockStoreVersionConfigs storeAndVersionConfigs = setupStoreAndVersionMocks(
        partitionCount,
        partitionerConfig,
        Optional.of(hybridStoreConfig),
        false,
        false,
        true);
    Store mockStore = storeAndVersionConfigs.store;
    Version version = storeAndVersionConfigs.version;
    VeniceStoreVersionConfig storeConfig = storeAndVersionConfigs.storeVersionConfig;

    Map<String, Object> extraServerProperties = new HashMap<>();
    extraServerProperties.put(SERVER_ENABLE_LIVE_CONFIG_BASED_KAFKA_THROTTLING, true);

    StoreIngestionTaskFactory ingestionTaskFactory = getIngestionTaskFactoryBuilder(
        new RandomPollStrategy(),
        Utils.setOf(PARTITION_FOO),
        Optional.empty(),
        1,
        extraServerProperties,
        true).build();

    Properties kafkaProps = new Properties();
    kafkaProps.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryLocalKafkaBroker.getKafkaBootstrapServer());

    int leaderSubPartition = PartitionUtils.getLeaderSubPartition(PARTITION_FOO, amplificationFactor);
    storeIngestionTaskUnderTest = ingestionTaskFactory.getNewIngestionTask(
        mockStore,
        version,
        kafkaProps,
        isCurrentVersion,
        storeConfig,
        leaderSubPartition,
        false,
        Optional.empty());

    AtomicLong remoteKafkaQuota = new AtomicLong(10);

    TestMockTime testTime = new TestMockTime();
    long timeWindowMS = 1000L;
    // Unlimited
    EventThrottler localThrottler =
        new EventThrottler(testTime, -1, timeWindowMS, "local_throttler", true, EventThrottler.REJECT_STRATEGY);

    // Modifiable remote throttler
    EventThrottler remoteThrottler = new EventThrottler(
        testTime,
        remoteKafkaQuota::get,
        timeWindowMS,
        "remote_throttler",
        true,
        EventThrottler.REJECT_STRATEGY);

    kafkaUrlToRecordsThrottler.put(inMemoryLocalKafkaBroker.getKafkaBootstrapServer(), localThrottler);
    kafkaUrlToRecordsThrottler.put(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer(), remoteThrottler);

    String rtTopic = Version.composeRealTimeTopic(storeNameWithoutVersionInfo);
    PubSubTopic rtPubSubTopic = pubSubTopicRepository.getTopic(rtTopic);
    PubSubTopicPartition fooRtPartition = new PubSubTopicPartitionImpl(rtPubSubTopic, PARTITION_FOO);
    inMemoryLocalKafkaBroker.createTopic(rtTopic, partitionCount);
    inMemoryRemoteKafkaBroker.createTopic(rtTopic, partitionCount);

    aggKafkaConsumerService.subscribeConsumerFor(
        inMemoryLocalKafkaBroker.getKafkaBootstrapServer(),
        storeIngestionTaskUnderTest,
        fooRtPartition,
        0);
    aggKafkaConsumerService.subscribeConsumerFor(
        inMemoryRemoteKafkaBroker.getKafkaBootstrapServer(),
        storeIngestionTaskUnderTest,
        fooRtPartition,
        0);

    VeniceWriter localRtWriter = getVeniceWriter(rtTopic, new MockInMemoryProducerAdapter(inMemoryLocalKafkaBroker), 1);
    VeniceWriter remoteRtWriter =
        getVeniceWriter(rtTopic, new MockInMemoryProducerAdapter(inMemoryRemoteKafkaBroker), 1);

    long recordsNum = 5L;
    for (int i = 0; i < recordsNum; i++) {
      localRtWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID, PUT_KEY_FOO_TIMESTAMP, null).get();
    }

    for (int i = 0; i < recordsNum; i++) {
      remoteRtWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID, PUT_KEY_FOO_TIMESTAMP, null).get();
    }

    waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      Assert.assertEquals(localConsumedDataReceiver.receivedRecordsCount(), recordsNum);
      Assert.assertEquals(remoteConsumedDataReceiver.receivedRecordsCount(), recordsNum);
    });

    // Pause remote kafka consumption
    remoteKafkaQuota.set(0);
    for (int i = 0; i < recordsNum; i++) {
      localRtWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID, PUT_KEY_FOO_TIMESTAMP, null).get();
    }

    for (int i = 0; i < recordsNum; i++) {
      remoteRtWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID, PUT_KEY_FOO_TIMESTAMP, null).get();
    }

    Long doubleRecordsNum = recordsNum * 2;
    waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Assert.assertTrue(localConsumedDataReceiver.receivedRecordsCount() == doubleRecordsNum);
      Assert.assertTrue(remoteConsumedDataReceiver.receivedRecordsCount() == recordsNum);
    });

    // Verify resumes ingestion from remote Kafka
    int mockInteractionsBeforePoll = Mockito.mockingDetails(mockRemoteKafkaConsumer).getInvocations().size();
    // Resume remote Kafka consumption
    remoteKafkaQuota.set(10);
    testTime.sleep(timeWindowMS); // sleep so throttling window is reset and we don't run into race conditions

    int mockInteractionsAfterPoll = Mockito.mockingDetails(mockRemoteKafkaConsumer).getInvocations().size();
    Assert.assertEquals(
        mockInteractionsBeforePoll,
        mockInteractionsAfterPoll,
        "Remote consumer should not poll for new records but return previously cached records");
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testIsReadyToServe(boolean isDaVinciClient) {
    int partitionCount = 2;
    int amplificationFactor = 1;

    VenicePartitioner partitioner = getVenicePartitioner(1); // Only get base venice partitioner
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setPartitionerClass(partitioner.getClass().getName());
    partitionerConfig.setAmplificationFactor(amplificationFactor);

    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(
        100,
        100,
        100,
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP);

    MockStoreVersionConfigs storeAndVersionConfigs =
        setupStoreAndVersionMocks(partitionCount, partitionerConfig, Optional.of(hybridStoreConfig), false, true, true);
    Store mockStore = storeAndVersionConfigs.store;
    Version version = storeAndVersionConfigs.version;
    VeniceStoreVersionConfig storeConfig = storeAndVersionConfigs.storeVersionConfig;

    Map<String, Object> extraServerProperties = new HashMap<>();

    StoreIngestionTaskFactory ingestionTaskFactory = getIngestionTaskFactoryBuilder(
        new RandomPollStrategy(),
        Utils.setOf(PARTITION_FOO),
        Optional.empty(),
        1,
        extraServerProperties,
        false).setIsDaVinciClient(isDaVinciClient).setAggKafkaConsumerService(aggKafkaConsumerService).build();

    TopicManager mockTopicManagerRemoteKafka = mock(TopicManager.class);

    doReturn(mockTopicManager).when(mockTopicManagerRepository)
        .getTopicManager(inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
    doReturn(mockTopicManagerRemoteKafka).when(mockTopicManagerRepository)
        .getTopicManager(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer());

    doReturn(true).when(mockTopicManager).containsTopic(any());
    doReturn(true).when(mockTopicManagerRemoteKafka).containsTopic(any());

    Properties kafkaProps = new Properties();
    kafkaProps.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryLocalKafkaBroker.getKafkaBootstrapServer());

    int leaderSubPartition = PartitionUtils.getLeaderSubPartition(PARTITION_FOO, amplificationFactor);
    storeIngestionTaskUnderTest = ingestionTaskFactory.getNewIngestionTask(
        mockStore,
        version,
        kafkaProps,
        isCurrentVersion,
        storeConfig,
        leaderSubPartition,
        false,
        Optional.empty());

    String rtTopicName = Version.composeRealTimeTopic(mockStore.getName());
    PubSubTopic rtTopic = pubSubTopicRepository.getTopic(rtTopicName);
    TopicSwitch topicSwitchWithSourceRealTimeTopic = new TopicSwitch();
    topicSwitchWithSourceRealTimeTopic.sourceKafkaServers = new ArrayList<>();
    topicSwitchWithSourceRealTimeTopic.sourceKafkaServers.add(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer());
    topicSwitchWithSourceRealTimeTopic.sourceTopicName = rtTopicName;
    TopicSwitchWrapper topicSwitchWithSourceRealTimeTopicWrapper = new TopicSwitchWrapper(
        topicSwitchWithSourceRealTimeTopic,
        pubSubTopicRepository.getTopic(topicSwitchWithSourceRealTimeTopic.sourceTopicName.toString()));

    OffsetRecord mockOffsetRecordLagCaughtUp = mock(OffsetRecord.class);
    doReturn(5L).when(mockOffsetRecordLagCaughtUp).getLocalVersionTopicOffset();
    doReturn(rtTopic).when(mockOffsetRecordLagCaughtUp).getLeaderTopic(any());
    doReturn(5L).when(mockOffsetRecordLagCaughtUp)
        .getUpstreamOffset(inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
    doReturn(5L).when(mockOffsetRecordLagCaughtUp)
        .getUpstreamOffset(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer());

    OffsetRecord mockOffsetRecordLagCaughtUpTimestampLagging = mock(OffsetRecord.class);
    doReturn(5L).when(mockOffsetRecordLagCaughtUpTimestampLagging).getLocalVersionTopicOffset();
    doReturn(rtTopic).when(mockOffsetRecordLagCaughtUpTimestampLagging).getLeaderTopic(any());
    doReturn(5L).when(mockOffsetRecordLagCaughtUpTimestampLagging)
        .getUpstreamOffset(inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
    doReturn(5L).when(mockOffsetRecordLagCaughtUpTimestampLagging)
        .getUpstreamOffset(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer());
    doReturn(System.currentTimeMillis() - MS_PER_HOUR).when(mockOffsetRecordLagCaughtUpTimestampLagging)
        .getLatestProducerProcessingTimeInMs();

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

    // If partition is waiting for replication lag to catch up, but buffer replay has not started, it is not ready to
    // serve
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
    doReturn(topicSwitchWithSourceRealTimeTopicWrapper).when(mockPcsBufferReplayStartedLagCaughtUp).getTopicSwitch();
    doReturn(mockOffsetRecordLagCaughtUp).when(mockPcsBufferReplayStartedLagCaughtUp).getOffsetRecord();
    doReturn(5L).when(mockTopicManager).getPartitionLatestOffsetAndRetry(any(), anyInt());
    doReturn(5L).when(mockTopicManagerRemoteKafka).getPartitionLatestOffsetAndRetry(any(), anyInt());
    doReturn(0).when(mockPcsBufferReplayStartedLagCaughtUp).getPartition();
    doReturn(0).when(mockPcsBufferReplayStartedLagCaughtUp).getUserPartition();
    storeIngestionTaskUnderTest.setPartitionConsumptionState(0, mockPcsBufferReplayStartedLagCaughtUp);
    // partitionConsumptionState.getLeaderFollowerState().equals(STANDBY)
    doReturn(LEADER).when(mockPcsBufferReplayStartedLagCaughtUp).getLeaderFollowerState();
    Assert.assertTrue(storeIngestionTaskUnderTest.isReadyToServe(mockPcsBufferReplayStartedLagCaughtUp));

    // Remote replication lag has not caught up but host has caught up to lag in local VT, so DaVinci replica will be
    // marked ready to serve but not storage node replica
    PartitionConsumptionState mockPcsBufferReplayStartedRemoteLagging = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcsBufferReplayStartedRemoteLagging).isEndOfPushReceived();
    doReturn(false).when(mockPcsBufferReplayStartedRemoteLagging).isComplete();
    doReturn(true).when(mockPcsBufferReplayStartedRemoteLagging).isWaitingForReplicationLag();
    doReturn(true).when(mockPcsBufferReplayStartedRemoteLagging).isHybrid();
    doReturn(topicSwitchWithSourceRealTimeTopicWrapper).when(mockPcsBufferReplayStartedRemoteLagging).getTopicSwitch();
    doReturn(mockOffsetRecordLagCaughtUpTimestampLagging).when(mockPcsBufferReplayStartedRemoteLagging)
        .getOffsetRecord();
    doReturn(5L).when(mockTopicManager).getPartitionLatestOffsetAndRetry(any(), anyInt());
    doReturn(150L).when(mockTopicManagerRemoteKafka).getPartitionLatestOffsetAndRetry(any(), anyInt());
    doReturn(150L).when(aggKafkaConsumerService).getLatestOffsetFor(anyString(), any(), any());
    Assert.assertEquals(
        storeIngestionTaskUnderTest.isReadyToServe(mockPcsBufferReplayStartedRemoteLagging),
        isDaVinciClient);

    // If there are issues in replication from remote RT -> local VT, DaVinci client will still report ready to serve
    // since they would have consumed all available data in local VT.
    // Venice storage nodes will not be ready to serve since they will detect lag with remote colo and wait for
    // ingestion
    // to catch up before serving data.
    PartitionConsumptionState mockPcsOffsetLagCaughtUpTimestampLagging = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcsOffsetLagCaughtUpTimestampLagging).isEndOfPushReceived();
    doReturn(false).when(mockPcsOffsetLagCaughtUpTimestampLagging).isComplete();
    doReturn(true).when(mockPcsOffsetLagCaughtUpTimestampLagging).isWaitingForReplicationLag();
    doReturn(true).when(mockPcsOffsetLagCaughtUpTimestampLagging).isHybrid();
    doReturn(topicSwitchWithSourceRealTimeTopicWrapper).when(mockPcsOffsetLagCaughtUpTimestampLagging).getTopicSwitch();
    doReturn(mockOffsetRecordLagCaughtUpTimestampLagging).when(mockPcsOffsetLagCaughtUpTimestampLagging)
        .getOffsetRecord();
    doReturn(5L).when(mockTopicManager).getPartitionLatestOffsetAndRetry(any(), anyInt());
    doReturn(5L).when(mockTopicManagerRemoteKafka).getPartitionLatestOffsetAndRetry(any(), anyInt());
    doReturn(System.currentTimeMillis() - 2 * MS_PER_DAY).when(mockTopicManager)
        .getProducerTimestampOfLastDataRecord(any(), anyInt());
    doReturn(System.currentTimeMillis()).when(mockTopicManagerRemoteKafka)
        .getProducerTimestampOfLastDataRecord(any(), anyInt());
    Assert.assertEquals(
        storeIngestionTaskUnderTest.isReadyToServe(mockPcsOffsetLagCaughtUpTimestampLagging),
        isDaVinciClient);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testActiveActiveStoreIsReadyToServe(boolean isDaVinciClient) {
    int partitionCount = 2;
    int amplificationFactor = 1;
    VenicePartitioner partitioner = getVenicePartitioner(1);
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setPartitionerClass(partitioner.getClass().getName());
    partitionerConfig.setAmplificationFactor(amplificationFactor);
    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(
        100,
        100,
        -1,
        DataReplicationPolicy.ACTIVE_ACTIVE,
        BufferReplayPolicy.REWIND_FROM_EOP);

    MockStoreVersionConfigs storeAndVersionConfigs =
        setupStoreAndVersionMocks(partitionCount, partitionerConfig, Optional.of(hybridStoreConfig), false, true, true);
    Store mockStore = storeAndVersionConfigs.store;
    Version version = storeAndVersionConfigs.version;
    VeniceStoreVersionConfig storeConfig = storeAndVersionConfigs.storeVersionConfig;

    StoreIngestionTaskFactory ingestionTaskFactory = getIngestionTaskFactoryBuilder(
        new RandomPollStrategy(),
        Utils.setOf(PARTITION_FOO),
        Optional.empty(),
        1,
        new HashMap<>(),
        false).setIsDaVinciClient(isDaVinciClient).setAggKafkaConsumerService(aggKafkaConsumerService).build();

    TopicManager mockTopicManagerRemoteKafka = mock(TopicManager.class);
    doReturn(mockTopicManager).when(mockTopicManagerRepository)
        .getTopicManager(inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
    doReturn(mockTopicManagerRemoteKafka).when(mockTopicManagerRepository)
        .getTopicManager(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer());
    doReturn(true).when(mockTopicManager).containsTopic(any());
    doReturn(true).when(mockTopicManagerRemoteKafka).containsTopic(any());

    Properties kafkaProps = new Properties();
    kafkaProps.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
    int leaderSubPartition = PartitionUtils.getLeaderSubPartition(PARTITION_FOO, amplificationFactor);
    storeIngestionTaskUnderTest = ingestionTaskFactory.getNewIngestionTask(
        mockStore,
        version,
        kafkaProps,
        isCurrentVersion,
        storeConfig,
        leaderSubPartition,
        false,
        Optional.empty());

    String rtTopicName = Version.composeRealTimeTopic(mockStore.getName());
    PubSubTopic rtTopic = pubSubTopicRepository.getTopic(rtTopicName);
    TopicSwitch topicSwitchWithMultipleSourceKafkaServers = new TopicSwitch();
    topicSwitchWithMultipleSourceKafkaServers.sourceKafkaServers = new ArrayList<>();
    topicSwitchWithMultipleSourceKafkaServers.sourceKafkaServers
        .add(inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
    topicSwitchWithMultipleSourceKafkaServers.sourceKafkaServers
        .add(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer());
    topicSwitchWithMultipleSourceKafkaServers.sourceTopicName = rtTopicName;
    TopicSwitchWrapper topicSwitchWithMultipleSourceKafkaServersWrapper = new TopicSwitchWrapper(
        topicSwitchWithMultipleSourceKafkaServers,
        pubSubTopicRepository.getTopic(topicSwitchWithMultipleSourceKafkaServers.sourceTopicName.toString()));

    OffsetRecord mockOffsetRecord = mock(OffsetRecord.class);
    doReturn(5L).when(mockOffsetRecord).getLocalVersionTopicOffset();
    doReturn(rtTopic).when(mockOffsetRecord).getLeaderTopic(any());
    doReturn(5L).when(mockOffsetRecord).getUpstreamOffset(inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
    doReturn(5L).when(mockOffsetRecord).getUpstreamOffset(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer());

    // Local replication are caught up but remote replication are not. A/A storage node replica is not ready to serve
    // Since host has caught up to lag in local VT, DaVinci replica will be marked ready to serve
    PartitionConsumptionState mockPcsMultipleSourceKafkaServers = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcsMultipleSourceKafkaServers).isEndOfPushReceived();
    doReturn(false).when(mockPcsMultipleSourceKafkaServers).isComplete();
    doReturn(true).when(mockPcsMultipleSourceKafkaServers).isWaitingForReplicationLag();
    doReturn(true).when(mockPcsMultipleSourceKafkaServers).isHybrid();
    doReturn(topicSwitchWithMultipleSourceKafkaServersWrapper).when(mockPcsMultipleSourceKafkaServers).getTopicSwitch();
    doReturn(mockOffsetRecord).when(mockPcsMultipleSourceKafkaServers).getOffsetRecord();
    doReturn(5L).when(mockTopicManager).getPartitionLatestOffsetAndRetry(any(), anyInt());
    doReturn(150L).when(mockTopicManagerRemoteKafka).getPartitionLatestOffsetAndRetry(any(), anyInt());
    doReturn(150L).when(aggKafkaConsumerService).getLatestOffsetFor(anyString(), any(), any());
    doReturn(0).when(mockPcsMultipleSourceKafkaServers).getPartition();
    doReturn(0).when(mockPcsMultipleSourceKafkaServers).getUserPartition();
    storeIngestionTaskUnderTest.setPartitionConsumptionState(0, mockPcsMultipleSourceKafkaServers);
    Assert.assertEquals(storeIngestionTaskUnderTest.isReadyToServe(mockPcsMultipleSourceKafkaServers), isDaVinciClient);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testProcessTopicSwitch(boolean isDaVinciClient) {
    int amplificationFactor = 1;
    VenicePartitioner partitioner = getVenicePartitioner(amplificationFactor);
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setPartitionerClass(partitioner.getClass().getName());
    partitionerConfig.setAmplificationFactor(amplificationFactor);
    HybridStoreConfig hybridStoreConfig =
        new HybridStoreConfigImpl(100, 100, 100, DataReplicationPolicy.AGGREGATE, BufferReplayPolicy.REWIND_FROM_EOP);
    MockStoreVersionConfigs storeAndVersionConfigs =
        setupStoreAndVersionMocks(2, partitionerConfig, Optional.of(hybridStoreConfig), false, true, false);
    Store mockStore = storeAndVersionConfigs.store;
    Version version = storeAndVersionConfigs.version;
    VeniceStoreVersionConfig storeConfig = storeAndVersionConfigs.storeVersionConfig;
    doReturn(new DeepCopyStorageEngine(mockAbstractStorageEngine)).when(mockStorageEngineRepository)
        .getLocalStorageEngine(topic);
    StoreIngestionTaskFactory ingestionTaskFactory = getIngestionTaskFactoryBuilder(
        new RandomPollStrategy(),
        Utils.setOf(PARTITION_FOO),
        Optional.empty(),
        amplificationFactor,
        new HashMap<>(),
        false).setIsDaVinciClient(isDaVinciClient).build();
    int leaderSubPartition = PartitionUtils.getLeaderSubPartition(PARTITION_FOO, amplificationFactor);
    Properties kafkaProps = new Properties();
    kafkaProps.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryLocalKafkaBroker.getKafkaBootstrapServer());

    storeIngestionTaskUnderTest = ingestionTaskFactory.getNewIngestionTask(
        mockStore,
        version,
        kafkaProps,
        isCurrentVersion,
        storeConfig,
        leaderSubPartition,
        false,
        Optional.empty());

    TopicManager mockTopicManagerRemoteKafka = mock(TopicManager.class);
    doReturn(mockTopicManagerRemoteKafka).when(mockTopicManagerRepository)
        .getTopicManager(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer());

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
    doReturn(PARTITION_FOO).when(mockPcs).getUserPartition();
    doReturn(PARTITION_FOO).when(mockPcs).getPartition();
    storeIngestionTaskUnderTest.getStatusReportAdapter().initializePartitionReportStatus(PARTITION_FOO);
    storeIngestionTaskUnderTest.processTopicSwitch(controlMessage, PARTITION_FOO, 10, mockPcs);

    verify(mockTopicManagerRemoteKafka, isDaVinciClient ? never() : times(1))
        .getPartitionOffsetByTime(any(), anyLong());
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testUpdateConsumedUpstreamRTOffsetMapDuringRTSubscription(boolean activeActive) {
    String storeName = Utils.getUniqueString("store");
    String versionTopic = Version.composeKafkaTopic(storeName, 1);

    VeniceStoreVersionConfig mockVeniceStoreVersionConfig = mock(VeniceStoreVersionConfig.class);
    doReturn(versionTopic).when(mockVeniceStoreVersionConfig).getStoreVersionName();

    Version mockVersion = mock(Version.class);
    doReturn(1).when(mockVersion).getPartitionCount();
    doReturn(VersionStatus.STARTED).when(mockVersion).getStatus();

    ReadOnlyStoreRepository mockReadOnlyStoreRepository = mock(ReadOnlyStoreRepository.class);
    Store mockStore = mock(Store.class);
    doReturn(mockStore).when(mockReadOnlyStoreRepository).getStoreOrThrow(eq(storeName));
    doReturn(false).when(mockStore).isHybridStoreDiskQuotaEnabled();
    doReturn(Optional.of(mockVersion)).when(mockStore).getVersion(1);
    doReturn(activeActive).when(mockVersion).isActiveActiveReplicationEnabled();

    Properties mockKafkaConsumerProperties = mock(Properties.class);
    doReturn("localhost").when(mockKafkaConsumerProperties).getProperty(eq(KAFKA_BOOTSTRAP_SERVERS));

    VeniceServerConfig mockVeniceServerConfig = mock(VeniceServerConfig.class);
    VeniceProperties mockVeniceProperties = mock(VeniceProperties.class);
    doReturn(true).when(mockVeniceProperties).isEmpty();
    doReturn(mockVeniceProperties).when(mockVeniceServerConfig).getKafkaConsumerConfigsForLocalConsumption();
    doReturn(Object2IntMaps.emptyMap()).when(mockVeniceServerConfig).getKafkaClusterUrlToIdMap();
    doReturn(Int2ObjectMaps.emptyMap()).when(mockVeniceServerConfig).getKafkaClusterIdToUrlMap();

    StoreIngestionTaskFactory ingestionTaskFactory = TestUtils.getStoreIngestionTaskBuilder(storeName)
        .setTopicManagerRepository(mockTopicManagerRepository)
        .setStorageMetadataService(mockStorageMetadataService)
        .setMetadataRepository(mockReadOnlyStoreRepository)
        .setTopicManagerRepository(mockTopicManagerRepository)
        .setServerConfig(mockVeniceServerConfig)
        .setPubSubTopicRepository(pubSubTopicRepository)
        .build();

    LeaderFollowerStoreIngestionTask ingestionTask =
        (LeaderFollowerStoreIngestionTask) ingestionTaskFactory.getNewIngestionTask(
            mockStore,
            mockVersion,
            mockKafkaConsumerProperties,
            () -> true,
            mockVeniceStoreVersionConfig,
            0,
            false,
            Optional.empty());

    TopicSwitch topicSwitch = new TopicSwitch();
    topicSwitch.sourceKafkaServers = Collections.singletonList("localhost");
    topicSwitch.sourceTopicName = "test_rt";
    topicSwitch.rewindStartTimestamp = System.currentTimeMillis();
    TopicSwitchWrapper topicSwitchWrapper =
        new TopicSwitchWrapper(topicSwitch, pubSubTopicRepository.getTopic(topicSwitch.sourceTopicName.toString()));
    PubSubTopic newSourceTopic = pubSubTopicRepository.getTopic(topicSwitch.sourceTopicName.toString());
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(IN_TRANSITION_FROM_STANDBY_TO_LEADER).when(mockPcs).getLeaderFollowerState();
    doReturn(topicSwitchWrapper).when(mockPcs).getTopicSwitch();
    OffsetRecord mockOffsetRecord = mock(OffsetRecord.class);
    doReturn(pubSubTopicRepository.getTopic("test_rt")).when(mockOffsetRecord).getLeaderTopic(any());
    doReturn(1000L).when(mockPcs).getLeaderOffset(anyString(), any());
    doReturn(mockOffsetRecord).when(mockPcs).getOffsetRecord();
    // Test whether consumedUpstreamRTOffsetMap is updated when leader subscribes to RT after state transition
    ingestionTask.startConsumingAsLeaderInTransitionFromStandby(mockPcs);
    verify(mockPcs, times(1)).updateLeaderConsumedUpstreamRTOffset(
        eq(activeActive ? "localhost" : OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY),
        eq(1000L));

    PubSubTopic rtTopic = pubSubTopicRepository.getTopic("test_rt");
    Supplier<PartitionConsumptionState> mockPcsSupplier = () -> {
      PartitionConsumptionState mock = mock(PartitionConsumptionState.class);
      doReturn(LEADER).when(mock).getLeaderFollowerState();
      doReturn(topicSwitchWrapper).when(mock).getTopicSwitch();
      OffsetRecord mockOR = mock(OffsetRecord.class);
      doReturn(rtTopic).when(mockOR).getLeaderTopic(any());
      System.out.println(mockOR.getLeaderTopic(null));
      doReturn(1000L).when(mockOR).getUpstreamOffset(anyString());
      if (activeActive) {
        doReturn(1000L).when(mock).getLatestProcessedUpstreamRTOffsetWithNoDefault(anyString());
      } else {
        doReturn(1000L).when(mock).getLatestProcessedUpstreamRTOffset(anyString());
      }
      doReturn(mockOR).when(mock).getOffsetRecord();
      System.out.println("inside mock" + mockOR.getLeaderTopic(null));
      return mock;
    };
    mockPcs = mockPcsSupplier.get();

    // Test whether consumedUpstreamRTOffsetMap is updated when leader subscribes to RT after executing TS
    ingestionTask.leaderExecuteTopicSwitch(mockPcs, topicSwitch, newSourceTopic);
    verify(mockPcs, times(1)).updateLeaderConsumedUpstreamRTOffset(
        eq(activeActive ? "localhost" : OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY),
        eq(1000L));

    // Test alternative branch of the code
    Supplier<PartitionConsumptionState> mockPcsSupplier2 = () -> {
      PartitionConsumptionState mock = mockPcsSupplier.get();
      if (activeActive) {
        doReturn(-1L).when(mock).getLatestProcessedUpstreamRTOffsetWithNoDefault(anyString());
      } else {
        doReturn(-1L).when(mock).getLatestProcessedUpstreamRTOffset(anyString());
      }
      doReturn(new PubSubTopicPartitionImpl(rtTopic, 0)).when(mock).getSourceTopicPartition(any());
      return mock;
    };
    mockPcs = mockPcsSupplier2.get();
    ingestionTask.leaderExecuteTopicSwitch(mockPcs, topicSwitch, newSourceTopic);
    verify(mockPcs, never()).updateLeaderConsumedUpstreamRTOffset(anyString(), anyLong());

    // One more branch
    mockPcs = mockPcsSupplier2.get();
    topicSwitch.rewindStartTimestamp = 0;
    ingestionTask.leaderExecuteTopicSwitch(mockPcs, topicSwitch, newSourceTopic);
    verify(mockPcs, never()).updateLeaderConsumedUpstreamRTOffset(anyString(), anyLong());
  }

  @Test
  public void testLeaderShouldSubscribeToCorrectVTOffset() {
    StoreIngestionTaskFactory.Builder builder = mock(StoreIngestionTaskFactory.Builder.class);
    StorageEngineRepository mockStorageEngineRepository = mock(StorageEngineRepository.class);
    doReturn(new DeepCopyStorageEngine(mockAbstractStorageEngine)).when(mockStorageEngineRepository)
        .getLocalStorageEngine(anyString());
    doReturn(mockStorageEngineRepository).when(builder).getStorageEngineRepository();
    VeniceServerConfig veniceServerConfig = mock(VeniceServerConfig.class);
    doReturn(VeniceProperties.empty()).when(veniceServerConfig).getKafkaConsumerConfigsForLocalConsumption();
    doReturn(VeniceProperties.empty()).when(veniceServerConfig).getKafkaConsumerConfigsForRemoteConsumption();
    doReturn(Object2IntMaps.emptyMap()).when(veniceServerConfig).getKafkaClusterUrlToIdMap();
    doReturn(veniceServerConfig).when(builder).getServerConfig();
    doReturn(mock(ReadOnlyStoreRepository.class)).when(builder).getMetadataRepo();
    doReturn(mock(ReadOnlySchemaRepository.class)).when(builder).getSchemaRepo();
    doReturn(mock(AggKafkaConsumerService.class)).when(builder).getAggKafkaConsumerService();
    doReturn(mockAggStoreIngestionStats).when(builder).getIngestionStats();
    doReturn(pubSubTopicRepository).when(builder).getPubSubTopicRepository();

    Version version = mock(Version.class);
    doReturn(1).when(version).getPartitionCount();
    doReturn(null).when(version).getPartitionerConfig();
    doReturn(VersionStatus.ONLINE).when(version).getStatus();
    doReturn(true).when(version).isNativeReplicationEnabled();
    doReturn("localhost").when(version).getPushStreamSourceAddress();

    Store store = mock(Store.class);
    doReturn(Optional.of(version)).when(store).getVersion(eq(1));

    String versionTopicName = "testStore_v1";
    VeniceStoreVersionConfig storeConfig = mock(VeniceStoreVersionConfig.class);
    doReturn(versionTopicName).when(storeConfig).getStoreVersionName();
    LeaderFollowerStoreIngestionTask leaderFollowerStoreIngestionTask = spy(
        new LeaderFollowerStoreIngestionTask(
            builder,
            store,
            version,
            mock(Properties.class),
            mock(BooleanSupplier.class),
            storeConfig,
            -1,
            false,
            Optional.empty()));

    OffsetRecord offsetRecord = mock(OffsetRecord.class);
    doReturn(pubSubTopicRepository.getTopic(versionTopicName)).when(offsetRecord).getLeaderTopic(any());
    PartitionConsumptionState partitionConsumptionState = new PartitionConsumptionState(0, 1, offsetRecord, false);

    long localVersionTopicOffset = 100L;
    long remoteVersionTopicOffset = 200L;
    partitionConsumptionState.updateLatestProcessedLocalVersionTopicOffset(localVersionTopicOffset);
    partitionConsumptionState.updateLatestProcessedUpstreamVersionTopicOffset(remoteVersionTopicOffset);

    // Run the actual codes inside function "startConsumingAsLeader"
    doCallRealMethod().when(leaderFollowerStoreIngestionTask).startConsumingAsLeader(any());
    doReturn(false).when(leaderFollowerStoreIngestionTask).shouldNewLeaderSwitchToRemoteConsumption(any());
    Set<String> kafkaServerSet = new HashSet<>();
    kafkaServerSet.add("localhost");
    doReturn(kafkaServerSet).when(leaderFollowerStoreIngestionTask).getConsumptionSourceKafkaAddress(any());

    // Test 1: if leader is not consuming remotely, leader must subscribe to the local VT offset
    partitionConsumptionState.setConsumeRemotely(false);
    leaderFollowerStoreIngestionTask.startConsumingAsLeader(partitionConsumptionState);
    verify(leaderFollowerStoreIngestionTask, times(1))
        .consumerSubscribe(any(), eq(localVersionTopicOffset), anyString());

    // Test 2: if leader is consuming remotely, leader must subscribe to the remote VT offset
    partitionConsumptionState.setConsumeRemotely(true);
    leaderFollowerStoreIngestionTask.startConsumingAsLeader(partitionConsumptionState);
    verify(leaderFollowerStoreIngestionTask, times(1))
        .consumerSubscribe(any(), eq(remoteVersionTopicOffset), anyString());
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testWrappedInterruptExceptionDuringGracefulShutdown(boolean isActiveActiveReplicationEnabled)
      throws Exception {
    hybridStoreConfig = Optional.of(
        new HybridStoreConfigImpl(
            10,
            20,
            HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
            DataReplicationPolicy.NON_AGGREGATE,
            BufferReplayPolicy.REWIND_FROM_EOP));
    VeniceException veniceException = new VeniceException("Wrapped interruptedException", new InterruptedException());
    runTest(Utils.setOf(PARTITION_FOO), () -> {
      doReturn(getOffsetRecord(1, true)).when(mockStorageMetadataService).getLastOffset(topic, PARTITION_FOO);
      doThrow(veniceException).when(aggKafkaConsumerService).unsubscribeConsumerFor(eq(pubSubTopic), any());
    }, () -> {
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).restarted(eq(topic), eq(PARTITION_FOO), anyLong());
      storeIngestionTaskUnderTest.close();
      verify(aggKafkaConsumerService, timeout(TEST_TIMEOUT_MS)).unsubscribeConsumerFor(eq(pubSubTopic), any());
    }, isActiveActiveReplicationEnabled);
    Assert.assertEquals(mockNotifierError.size(), 0);
  }

  /**
   * Verifies that during a graceful shutdown event, the metadata of OffsetRecord will be synced up with partitionConsumptionState
   * in order to avoid re-ingesting everything, regardless of the sync bytes interval
   * Steps:
   * 1. offsetRecord and pcs has the same state at the beginning
   * 2. pcs consumes 2 records and offsetRecords doesn't sync up with pcs due to high sync interval.
   * 3. enforce to gracefully shutdown and validate offsetRecord has been synced up with pcs once.
   * @param isActiveActiveReplicationEnabled
   * @throws Exception
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testOffsetSyncBeforeGracefulShutDown(boolean isActiveActiveReplicationEnabled) throws Exception {
    // prepare to send 2 messages
    localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);
    localVeniceWriter.put(putKeyFoo2, putValue, SCHEMA_ID);

    hybridStoreConfig = Optional.of(
        new HybridStoreConfigImpl(
            10,
            20,
            HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
            DataReplicationPolicy.NON_AGGREGATE,
            BufferReplayPolicy.REWIND_FROM_EOP));
    runTest(Utils.setOf(PARTITION_FOO), () -> {
      doReturn(getOffsetRecord(0, true)).when(mockStorageMetadataService).getLastOffset(topic, PARTITION_FOO);
    }, () -> {
      // Verify it retrieves the offset from the OffSet Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).getLastOffset(topic, PARTITION_FOO);

      // Verify offsetRecord hasn't been synced yet
      PartitionConsumptionState pcs = storeIngestionTaskUnderTest.getPartitionConsumptionState(PARTITION_FOO);
      OffsetRecord offsetRecord = pcs.getOffsetRecord();
      Assert.assertEquals(pcs.getLatestProcessedLocalVersionTopicOffset(), 0L);
      Assert.assertEquals(offsetRecord.getLocalVersionTopicOffset(), 0L);

      // verify 2 messages were processed
      verify(mockStoreIngestionStats, timeout(TEST_TIMEOUT_MS).times(2)).recordTotalRecordsConsumed();
      Assert.assertEquals(pcs.getLatestProcessedLocalVersionTopicOffset(), 2L); // PCS updated
      Assert.assertEquals(offsetRecord.getLocalVersionTopicOffset(), 0L); // offsetRecord hasn't been updated yet

      storeIngestionTaskUnderTest.close();

      // Verify the OffsetRecord is synced up with pcs and get persisted only once during shutdown
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS).times(1)).put(eq(topic), eq(PARTITION_FOO), any());
      Assert.assertEquals(offsetRecord.getLocalVersionTopicOffset(), 2L);

    }, isActiveActiveReplicationEnabled, configOverride -> {
      // set very high threshold so offsetRecord isn't be synced during regular consumption
      doReturn(100_000L).when(configOverride).getDatabaseSyncBytesIntervalForTransactionalMode();
    });
    Assert.assertEquals(mockNotifierError.size(), 0);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testProduceToStoreBufferService(boolean activeActiveEnabled) throws Exception {
    byte[] keyBytes = new byte[1];
    KafkaKey kafkaKey = new KafkaKey(MessageType.PUT, keyBytes);
    KafkaMessageEnvelope kafkaMessageEnvelope = new KafkaMessageEnvelope();
    kafkaMessageEnvelope.messageType = MessageType.PUT.getValue();
    Put put = new Put();
    put.putValue = ByteBuffer.allocate(10);
    put.putValue.position(4);
    put.replicationMetadataPayload = ByteBuffer.allocate(10);
    kafkaMessageEnvelope.payloadUnion = put;
    kafkaMessageEnvelope.producerMetadata = new ProducerMetadata();
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> pubSubMessage = new ImmutablePubSubMessage(
        kafkaKey,
        kafkaMessageEnvelope,
        new PubSubTopicPartitionImpl(pubSubTopic, PARTITION_FOO),
        0,
        0,
        0);

    HostLevelIngestionStats stats = mock(HostLevelIngestionStats.class);
    when(mockAggStoreIngestionStats.getStoreStats(anyString())).thenReturn(stats);
    LeaderProducedRecordContext leaderProducedRecordContext = mock(LeaderProducedRecordContext.class);
    when(leaderProducedRecordContext.getMessageType()).thenReturn(MessageType.PUT);
    when(leaderProducedRecordContext.getValueUnion()).thenReturn(put);
    when(leaderProducedRecordContext.getKeyBytes()).thenReturn(keyBytes);

    runTest(Collections.singleton(PARTITION_FOO), () -> {
      TestUtils.waitForNonDeterministicAssertion(
          5,
          TimeUnit.SECONDS,
          () -> assertTrue(storeIngestionTaskUnderTest.hasAnySubscription()));

      Runnable produce = () -> {
        try {
          storeIngestionTaskUnderTest.produceToStoreBufferService(
              pubSubMessage,
              leaderProducedRecordContext,
              PARTITION_FOO,
              localKafkaConsumerService.kafkaUrl,
              System.nanoTime(),
              System.currentTimeMillis());
        } catch (InterruptedException e) {
          throw new VeniceException(e);
        }
      };
      int wantedInvocationsForStatsWhichCanBeDisabled = 0;
      int wantedInvocationsForAllOtherStats = 0;
      verifyStats(stats, wantedInvocationsForStatsWhichCanBeDisabled, wantedInvocationsForAllOtherStats);

      produce.run();
      verifyStats(stats, ++wantedInvocationsForStatsWhichCanBeDisabled, ++wantedInvocationsForAllOtherStats);

      storeIngestionTaskUnderTest.disableMetricsEmission();
      produce.run();
      verifyStats(stats, wantedInvocationsForStatsWhichCanBeDisabled, ++wantedInvocationsForAllOtherStats);

      storeIngestionTaskUnderTest.enableMetricsEmission();
      produce.run();
      verifyStats(stats, ++wantedInvocationsForStatsWhichCanBeDisabled, ++wantedInvocationsForAllOtherStats);

      long currentTimeMs = System.currentTimeMillis();
      long errorMargin = 10_000;
      verify(mockVersionedStorageIngestionStats, timeout(1000).times(++wantedInvocationsForStatsWhichCanBeDisabled))
          .recordConsumedRecordEndToEndProcessingLatency(
              anyString(),
              anyInt(),
              ArgumentMatchers.doubleThat(argument -> argument >= 0 && argument < 1000),
              ArgumentMatchers.longThat(
                  argument -> argument > currentTimeMs - errorMargin && argument < currentTimeMs + errorMargin));
    }, activeActiveEnabled);
  }

  private void verifyStats(
      HostLevelIngestionStats stats,
      int wantedInvocationsForStatsWhichCanBeDisabled,
      int wantedInvocationsForAllOtherStats) {
    verify(stats, times(wantedInvocationsForStatsWhichCanBeDisabled))
        .recordConsumerRecordsQueuePutLatency(anyDouble(), anyLong());
    verify(stats, timeout(1000).times(wantedInvocationsForAllOtherStats)).recordTotalRecordsConsumed();
    verify(stats, timeout(1000).times(wantedInvocationsForAllOtherStats)).recordTotalBytesConsumed(anyLong());
    verify(mockVersionedStorageIngestionStats, timeout(1000).times(wantedInvocationsForAllOtherStats))
        .recordRecordsConsumed(anyString(), anyInt());
    verify(mockVersionedStorageIngestionStats, timeout(1000).times(wantedInvocationsForAllOtherStats))
        .recordBytesConsumed(anyString(), anyInt(), anyLong());

  }

  @Test
  public void testShouldPersistRecord() throws Exception {
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> pubSubMessage =
        new ImmutablePubSubMessage(null, null, new PubSubTopicPartitionImpl(pubSubTopic, 1), 0, 0, 0);

    runTest(Collections.singleton(PARTITION_FOO), () -> {
      assertFalse(storeIngestionTaskUnderTest.shouldPersistRecord(pubSubMessage, null));
    }, false);

    Map<String, Object> serverProperties = new HashMap<>();
    serverProperties.put(FREEZE_INGESTION_IF_READY_TO_SERVE_OR_LOCAL_DATA_EXISTS, true);

    Supplier<PartitionConsumptionState> partitionConsumptionStateSupplier = () -> {
      PartitionConsumptionState partitionConsumptionState = mock(PartitionConsumptionState.class);
      when(partitionConsumptionState.isSubscribed()).thenReturn(true);
      when(partitionConsumptionState.isErrorReported()).thenReturn(false);
      when(partitionConsumptionState.isCompletionReported()).thenReturn(true);
      return partitionConsumptionState;
    };

    runTest(new RandomPollStrategy(), Collections.singleton(PARTITION_FOO), () -> {}, () -> {
      PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateSupplier.get();
      assertFalse(storeIngestionTaskUnderTest.shouldPersistRecord(pubSubMessage, partitionConsumptionState));
    }, this.hybridStoreConfig, false, Optional.empty(), false, 1, serverProperties);

    runTest(Collections.singleton(PARTITION_FOO), () -> {
      PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateSupplier.get();
      PubSubTopic wrongTopic = pubSubTopicRepository.getTopic("blah_v1");
      OffsetRecord offsetRecord = mock(OffsetRecord.class);
      when(offsetRecord.getLeaderTopic(any())).thenReturn(wrongTopic);
      when(partitionConsumptionState.getOffsetRecord()).thenReturn(offsetRecord);

      when(partitionConsumptionState.getLeaderFollowerState()).thenReturn(LEADER);
      assertFalse(storeIngestionTaskUnderTest.shouldPersistRecord(pubSubMessage, partitionConsumptionState));
    }, false);

    runTest(Collections.singleton(PARTITION_FOO), () -> {
      PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateSupplier.get();
      PubSubTopic wrongTopic = pubSubTopicRepository.getTopic("blah_v1");

      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> pubSubMessage2 =
          new ImmutablePubSubMessage(null, null, new PubSubTopicPartitionImpl(wrongTopic, 1), 0, 0, 0);

      when(partitionConsumptionState.getLeaderFollowerState()).thenReturn(STANDBY);
      assertFalse(storeIngestionTaskUnderTest.shouldPersistRecord(pubSubMessage2, partitionConsumptionState));
    }, false);
  }

  @Test
  public void testIngestionTaskForNonCurrentVersionShouldFailWhenEncounteringMemoryLimitException() throws Exception {
    doThrow(new MemoryLimitExhaustedException("mock exception")).when(mockAbstractStorageEngine)
        .put(anyInt(), any(), (ByteBuffer) any());
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID, PUT_KEY_FOO_TIMESTAMP, null).get();

    runTest(Collections.singleton(PARTITION_FOO), () -> {
      verify(mockAbstractStorageEngine, timeout(1000)).put(eq(PARTITION_FOO), any(), (ByteBuffer) any());
      verify(mockLogNotifier, timeout(1000)).error(any(), eq(PARTITION_FOO), any(), isA(VeniceException.class));
      verify(runnableForKillNonCurrentVersion, never()).run();
    }, false);
  }

  @Test
  public void testIngestionTaskForCurrentVersionShouldTryToKillOngoingPushWhenEncounteringMemoryLimitException()
      throws Exception {
    doThrow(new MemoryLimitExhaustedException("mock exception")).doNothing()
        .when(mockAbstractStorageEngine)
        .put(anyInt(), any(), (ByteBuffer) any());

    isCurrentVersion = () -> true;

    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID, PUT_KEY_FOO_TIMESTAMP, null).get();
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    runTest(Collections.singleton(PARTITION_FOO), () -> {
      verify(mockAbstractStorageEngine, timeout(5000).times(2)).put(eq(PARTITION_FOO), any(), (ByteBuffer) any());
      verify(mockAbstractStorageEngine, timeout(1000)).reopenStoragePartition(PARTITION_FOO);
      verify(mockLogNotifier, timeout(1000)).completed(anyString(), eq(PARTITION_FOO), anyLong(), anyString());
      verify(runnableForKillNonCurrentVersion, times(1)).run();
    }, false);
  }

  @Test
  public void testShouldProduceToVersionTopic() throws Exception {
    runTest(Collections.singleton(PARTITION_FOO), () -> {
      PartitionConsumptionState partitionConsumptionState = mock(PartitionConsumptionState.class);
      LeaderFollowerStoreIngestionTask lfsit = (LeaderFollowerStoreIngestionTask) storeIngestionTaskUnderTest;
      when(partitionConsumptionState.getLeaderFollowerState()).thenReturn(STANDBY);
      assertFalse(lfsit.shouldProduceToVersionTopic(partitionConsumptionState));

      when(partitionConsumptionState.getLeaderFollowerState()).thenReturn(LEADER);
      OffsetRecord offsetRecord = mock(OffsetRecord.class);
      when(offsetRecord.getLeaderTopic(any())).thenReturn(pubSubTopic);
      when(partitionConsumptionState.getOffsetRecord()).thenReturn(offsetRecord);
      assertFalse(lfsit.shouldProduceToVersionTopic(partitionConsumptionState));

      when(offsetRecord.getLeaderTopic(any())).thenReturn(pubSubTopicRepository.getTopic("blah_rt"));
      assertTrue(lfsit.shouldProduceToVersionTopic(partitionConsumptionState));

      when(offsetRecord.getLeaderTopic(any())).thenReturn(pubSubTopic);
      when(partitionConsumptionState.consumeRemotely()).thenReturn(true);
      assertTrue(lfsit.shouldProduceToVersionTopic(partitionConsumptionState));
    }, false);
  }

  @Test
  public void testBatchOnlyStoreDataRecovery() {
    Version version = mock(Version.class);
    doReturn(1).when(version).getPartitionCount();
    doReturn(VersionStatus.STARTED).when(version).getStatus();
    doReturn(true).when(version).isNativeReplicationEnabled();
    DataRecoveryVersionConfig dataRecoveryVersionConfig = new DataRecoveryVersionConfigImpl("dc-0", false, 1);
    doReturn(dataRecoveryVersionConfig).when(version).getDataRecoveryVersionConfig();

    Store store = mock(Store.class);
    doReturn(Optional.of(version)).when(store).getVersion(eq(1));

    VeniceStoreVersionConfig storeConfig = mock(VeniceStoreVersionConfig.class);
    doReturn(topic).when(storeConfig).getStoreVersionName();

    StoreIngestionTaskFactory ingestionTaskFactory = getIngestionTaskFactoryBuilder(
        new RandomPollStrategy(),
        Utils.setOf(PARTITION_FOO),
        Optional.empty(),
        1,
        Collections.emptyMap(),
        true).build();
    storeIngestionTaskUnderTest = ingestionTaskFactory.getNewIngestionTask(
        store,
        version,
        new Properties(),
        isCurrentVersion,
        storeConfig,
        1,
        false,
        Optional.empty());

    OffsetRecord offsetRecord = mock(OffsetRecord.class);
    doReturn(pubSubTopic).when(offsetRecord).getLeaderTopic(any());
    PartitionConsumptionState partitionConsumptionState = new PartitionConsumptionState(0, 1, offsetRecord, false);

    storeIngestionTaskUnderTest.updateLeaderTopicOnFollower(partitionConsumptionState);
    storeIngestionTaskUnderTest.startConsumingAsLeader(partitionConsumptionState);
    String dataRecoverySourceTopic = Version.composeKafkaTopic(storeNameWithoutVersionInfo, 1);
    verify(offsetRecord, times(1)).setLeaderTopic(pubSubTopicRepository.getTopic(dataRecoverySourceTopic));
  }

  private VeniceStoreVersionConfig getDefaultMockVeniceStoreVersionConfig(
      Consumer<VeniceStoreVersionConfig> storeVersionConfigOverride) {
    // mock the store config
    VeniceStoreVersionConfig storeConfig = mock(VeniceStoreVersionConfig.class);
    doReturn(topic).when(storeConfig).getStoreVersionName();
    doReturn(0).when(storeConfig).getTopicOffsetCheckIntervalMs();
    doReturn(READ_CYCLE_DELAY_MS).when(storeConfig).getKafkaReadCycleDelayMs();
    doReturn(EMPTY_POLL_SLEEP_MS).when(storeConfig).getKafkaEmptyPollSleepMs();
    doReturn(databaseSyncBytesIntervalForTransactionalMode).when(storeConfig)
        .getDatabaseSyncBytesIntervalForTransactionalMode();
    doReturn(databaseSyncBytesIntervalForDeferredWriteMode).when(storeConfig)
        .getDatabaseSyncBytesIntervalForDeferredWriteMode();
    doReturn(false).when(storeConfig).isReadOnlyForBatchOnlyStoreEnabled();
    storeVersionConfigOverride.accept(storeConfig);
    return storeConfig;
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
