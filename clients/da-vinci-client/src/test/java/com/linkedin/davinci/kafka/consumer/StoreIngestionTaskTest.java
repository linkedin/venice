package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.IN_TRANSITION_FROM_STANDBY_TO_LEADER;
import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.STANDBY;
import static com.linkedin.davinci.kafka.consumer.StoreIngestionTaskTest.AAConfig.AA_OFF;
import static com.linkedin.davinci.kafka.consumer.StoreIngestionTaskTest.AAConfig.AA_ON;
import static com.linkedin.davinci.kafka.consumer.StoreIngestionTaskTest.HybridConfig.HYBRID;
import static com.linkedin.davinci.kafka.consumer.StoreIngestionTaskTest.LeaderCompleteCheck.LEADER_COMPLETE_CHECK_ON;
import static com.linkedin.davinci.kafka.consumer.StoreIngestionTaskTest.NodeType.DA_VINCI;
import static com.linkedin.davinci.kafka.consumer.StoreIngestionTaskTest.NodeType.FOLLOWER;
import static com.linkedin.davinci.kafka.consumer.StoreIngestionTaskTest.NodeType.LEADER;
import static com.linkedin.davinci.kafka.consumer.StoreIngestionTaskTest.SortedInput.SORTED;
import static com.linkedin.davinci.store.AbstractStorageEngine.StoragePartitionAdjustmentTrigger.PREPARE_FOR_READ;
import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.FREEZE_INGESTION_IF_READY_TO_SERVE_OR_LOCAL_DATA_EXISTS;
import static com.linkedin.venice.ConfigKeys.HYBRID_QUOTA_ENFORCEMENT_ENABLED;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_CLUSTER_MAP_KEY_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_CLUSTER_MAP_KEY_URL;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_ENABLE_LIVE_CONFIG_BASED_KAFKA_THROTTLING;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_HEARTBEAT_INTERVAL_MS;
import static com.linkedin.venice.ConfigKeys.SERVER_LEADER_COMPLETE_STATE_CHECK_IN_FOLLOWER_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_LEADER_COMPLETE_STATE_CHECK_IN_FOLLOWER_VALID_INTERVAL_MS;
import static com.linkedin.venice.ConfigKeys.SERVER_LOCAL_CONSUMER_CONFIG_PREFIX;
import static com.linkedin.venice.ConfigKeys.SERVER_NUM_SCHEMA_FAST_CLASS_WARMUP;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_RECORD_LEVEL_METRICS_WHEN_BOOTSTRAPPING_CURRENT_VERSION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_REMOTE_CONSUMER_CONFIG_PREFIX;
import static com.linkedin.venice.ConfigKeys.SERVER_RESUBSCRIPTION_TRIGGERED_BY_VERSION_INGESTION_CONTEXT_CHANGE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_UNSUB_AFTER_BATCHPUSH;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;
import static com.linkedin.venice.utils.TestUtils.getOffsetRecord;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicCompletion;
import static com.linkedin.venice.utils.Time.MS_PER_DAY;
import static com.linkedin.venice.utils.Time.MS_PER_HOUR;
import static com.linkedin.venice.writer.LeaderCompleteState.LEADER_COMPLETED;
import static com.linkedin.venice.writer.LeaderCompleteState.LEADER_NOT_COMPLETED;
import static com.linkedin.venice.writer.VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER;
import static com.linkedin.venice.writer.VeniceWriter.generateHeartbeatMessage;
import static com.linkedin.venice.writer.VeniceWriter.getHeartbeatKME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.longThat;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.davinci.helix.StateModelIngestionProgressNotifier;
import com.linkedin.davinci.ingestion.LagType;
import com.linkedin.davinci.notifier.LogNotifier;
import com.linkedin.davinci.notifier.PushStatusNotifier;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.stats.AggHostLevelIngestionStats;
import com.linkedin.davinci.stats.AggKafkaConsumerServiceStats;
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
import com.linkedin.davinci.transformer.TestAvroRecordTransformer;
import com.linkedin.davinci.transformer.TestStringRecordTransformer;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.MemoryLimitExhaustedException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceIngestionTaskKilledException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.exceptions.validation.CorruptDataException;
import com.linkedin.venice.exceptions.validation.FatalDataValidationException;
import com.linkedin.venice.exceptions.validation.MissingDataException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.LeaderMetadata;
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
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.PubSubTopicType;
import com.linkedin.venice.pubsub.api.exceptions.PubSubUnsubscribedTopicPartitionException;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
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
import com.linkedin.venice.stats.StatsErrorCode;
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
import com.linkedin.venice.utils.ChunkingTestUtils;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.DiskUsage;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.pools.LandFillObjectPool;
import com.linkedin.venice.writer.LeaderCompleteState;
import com.linkedin.venice.writer.LeaderMetadataWrapper;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Beware that most of the test cases in this suite depend on {@link StoreIngestionTaskTest#TEST_TIMEOUT_MS}
 * Adjust it based on environment if timeout failure occurs.
 */
@Test(singleThreaded = true)
public abstract class StoreIngestionTaskTest {
  enum NodeType {
    LEADER, FOLLOWER, DA_VINCI
  }

  enum HybridConfig {
    HYBRID, BATCH_ONLY
  }

  enum AAConfig {
    AA_ON, AA_OFF
  }

  enum SortedInput {
    SORTED, UNSORTED
  }

  enum LeaderCompleteCheck {
    LEADER_COMPLETE_CHECK_ON, LEADER_COMPLETE_CHECK_OFF
  }

  @DataProvider
  public static Object[][] aaConfigProvider() {
    return DataProviderUtils.allPermutationGenerator(AAConfig.values());
  }

  @DataProvider
  public static Object[][] nodeTypeAndAAConfigAndDRPProvider() {
    return DataProviderUtils
        .allPermutationGenerator(NodeType.values(), AAConfig.values(), DataReplicationPolicy.values());
  }

  @DataProvider
  public static Object[][] hybridConfigAndNodeTypeProvider() {
    return DataProviderUtils.allPermutationGenerator(HybridConfig.values(), NodeType.values());
  }

  @DataProvider
  public static Object[][] sortedInputAndAAConfigProvider() {
    return DataProviderUtils.allPermutationGenerator(SortedInput.values(), AAConfig.values());
  }

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
  private IngestionThrottler mockIngestionThrottler;
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

  private Optional<PollStrategy> remotePollStrategy = Optional.empty();

  private boolean databaseChecksumVerificationEnabled = false;
  private AggKafkaConsumerServiceStats kafkaConsumerServiceStats = mock(AggKafkaConsumerServiceStats.class);
  private PubSubConsumerAdapterFactory mockFactory = mock(PubSubConsumerAdapterFactory.class);
  private final MetricsRepository mockMetricRepo = mock(MetricsRepository.class);

  private Supplier<StoreVersionState> storeVersionStateSupplier = () -> new StoreVersionState();
  private MockStoreVersionConfigs storeAndVersionConfigsUnderTest;

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
    replicationMetadataRecord.put(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME, Collections.singletonList(offset));

    byte[] replicationMetadata = REPLICATION_METADATA_SERIALIZER.serialize(replicationMetadataRecord);

    ByteBuffer replicationMetadataWithValueSchemaId =
        ByteUtils.prependIntHeaderToByteBuffer(ByteBuffer.wrap(replicationMetadata), valueSchemaId, false);
    replicationMetadataWithValueSchemaId
        .position(replicationMetadataWithValueSchemaId.position() - ByteUtils.SIZE_OF_INT);
    return ByteUtils.extractByteArray(replicationMetadataWithValueSchemaId);
  }

  @BeforeClass(alwaysRun = true)
  public void suiteSetUp() throws Exception {
    final Sensor mockSensor = mock(Sensor.class);
    doReturn(mockSensor).when(mockMetricRepo).sensor(anyString(), any());
    taskPollingService = Executors.newFixedThreadPool(1);
    storeBufferService =
        new StoreBufferService(3, 10000, 1000, isStoreWriterBufferAfterLeaderLogicEnabled(), mockMetricRepo, true);
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

    mockPartitionStatusNotifier = mock(PushStatusNotifier.class);
    mockLeaderFollowerStateModelNotifier = mock(StateModelIngestionProgressNotifier.class);

    mockStorageMetadataService = mock(StorageMetadataService.class);

    mockIngestionThrottler = mock(IngestionThrottler.class);
    mockSchemaRepo = mock(ReadOnlySchemaRepository.class);
    mockMetadataRepo = mock(ReadOnlyStoreRepository.class);
    mockLocalKafkaConsumer = mock(PubSubConsumerAdapter.class);
    mockRemoteKafkaConsumer = mock(PubSubConsumerAdapter.class);
    kafkaUrlToRecordsThrottler = new HashMap<>();
    kafkaClusterBasedRecordThrottler = new KafkaClusterBasedRecordThrottler(kafkaUrlToRecordsThrottler);

    mockTopicManager = mock(TopicManager.class);
    mockTopicManagerRepository = mock(TopicManagerRepository.class);
    doReturn(mockTopicManager).when(mockTopicManagerRepository).getLocalTopicManager();

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

    KafkaConsumerServiceStats regionStats = mock(KafkaConsumerServiceStats.class);
    doNothing().when(regionStats).recordByteSizePerPoll(anyDouble());
    doNothing().when(regionStats).recordPollResultNum(anyInt());
    doReturn(regionStats).when(kafkaConsumerServiceStats).getStoreStats(anyString());
  }

  private VeniceWriter getVeniceWriter(String topic, PubSubProducerAdapter producerAdapter) {
    VeniceWriterOptions veniceWriterOptions =
        new VeniceWriterOptions.Builder(topic).setKeySerializer(new DefaultSerializer())
            .setValueSerializer(new DefaultSerializer())
            .setWriteComputeSerializer(new DefaultSerializer())
            .setPartitioner(getVenicePartitioner())
            .setTime(SystemTime.INSTANCE)
            .build();
    return new VeniceWriter(veniceWriterOptions, VeniceProperties.empty(), producerAdapter);
  }

  private VenicePartitioner getVenicePartitioner() {
    return new SimplePartitioner();
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

  private void runTest(Set<Integer> partitions, Runnable assertions, AAConfig aaConfig) throws Exception {
    runTest(partitions, () -> {}, assertions, aaConfig);
  }

  private void runTest(
      Set<Integer> partitions,
      Runnable assertions,
      AAConfig aaConfig,
      Function<Integer, DaVinciRecordTransformer> getRecordTransformer) throws Exception {
    runTest(partitions, () -> {}, assertions, aaConfig, getRecordTransformer);
  }

  private void runTest(
      Set<Integer> partitions,
      Runnable beforeStartingConsumption,
      Runnable assertions,
      AAConfig aaConfig,
      Function<Integer, DaVinciRecordTransformer> getRecordTransformer) throws Exception {
    runTest(
        new RandomPollStrategy(),
        partitions,
        beforeStartingConsumption,
        assertions,
        this.hybridStoreConfig,
        false,
        Optional.empty(),
        aaConfig,
        Collections.emptyMap(),
        storeVersionConfigOverride -> {},
        getRecordTransformer);
  }

  private void runTest(
      Set<Integer> partitions,
      Runnable beforeStartingConsumption,
      Runnable assertions,
      AAConfig aaConfig) throws Exception {
    runTest(
        new RandomPollStrategy(),
        partitions,
        beforeStartingConsumption,
        assertions,
        this.hybridStoreConfig,
        false,
        Optional.empty(),
        aaConfig,
        Collections.emptyMap(),
        storeVersionConfigOverride -> {},
        null);
  }

  private void runTest(
      Set<Integer> partitions,
      Runnable beforeStartingConsumption,
      Runnable assertions,
      AAConfig aaConfig,
      Consumer<VeniceStoreVersionConfig> storeVersionConfigOverride) throws Exception {
    runTest(
        new RandomPollStrategy(),
        partitions,
        beforeStartingConsumption,
        assertions,
        this.hybridStoreConfig,
        false,
        Optional.empty(),
        aaConfig,
        Collections.emptyMap(),
        storeVersionConfigOverride,
        null);
  }

  private void runTest(
      PollStrategy pollStrategy,
      Set<Integer> partitions,
      Runnable beforeStartingConsumption,
      Runnable assertions,
      AAConfig aaConfig,
      Function<Integer, DaVinciRecordTransformer> getRecordTransformer) throws Exception {
    runTest(
        pollStrategy,
        partitions,
        beforeStartingConsumption,
        assertions,
        this.hybridStoreConfig,
        false,
        Optional.empty(),
        aaConfig,
        Collections.emptyMap(),
        storeVersionConfigOverride -> {},
        getRecordTransformer);
  }

  private void runTest(
      PollStrategy pollStrategy,
      Set<Integer> partitions,
      Runnable beforeStartingConsumption,
      Runnable assertions,
      Optional<HybridStoreConfig> hybridStoreConfig,
      boolean incrementalPushEnabled,
      Optional<DiskUsage> diskUsageForTest,
      AAConfig aaConfig,
      Map<String, Object> extraServerProperties) throws Exception {
    runTest(
        pollStrategy,
        partitions,
        beforeStartingConsumption,
        assertions,
        hybridStoreConfig,
        incrementalPushEnabled,
        diskUsageForTest,
        aaConfig,
        extraServerProperties,
        storeVersionConfigOverride -> {},
        null);
  }

  private void runTest(
      PollStrategy pollStrategy,
      Set<Integer> partitions,
      Runnable beforeStartingConsumption,
      Runnable assertions,
      Optional<HybridStoreConfig> hybridStoreConfig,
      boolean incrementalPushEnabled,
      Optional<DiskUsage> diskUsageForTest,
      AAConfig aaConfig,
      Map<String, Object> extraServerProperties,
      Consumer<VeniceStoreVersionConfig> storeVersionConfigOverride,
      Function<Integer, DaVinciRecordTransformer> getRecordTransformer) throws Exception {
    runTest(
        pollStrategy,
        partitions,
        beforeStartingConsumption,
        assertions,
        hybridStoreConfig,
        incrementalPushEnabled,
        false,
        false,
        diskUsageForTest,
        aaConfig,
        extraServerProperties,
        storeVersionConfigOverride,
        getRecordTransformer);
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
   * @param aaConfig, the flag to turn on ActiveActiveReplication for SIT
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
      boolean chunkingEnabled,
      boolean rmdChunkingEnabled,
      Optional<DiskUsage> diskUsageForTest,
      AAConfig aaConfig,
      Map<String, Object> extraServerProperties,
      Consumer<VeniceStoreVersionConfig> storeVersionConfigOverride,
      Function<Integer, DaVinciRecordTransformer> getRecordTransformer) throws Exception {

    int partitionCount = PARTITION_COUNT;
    VenicePartitioner partitioner = getVenicePartitioner(); // Only get base venice partitioner
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setPartitionerClass(partitioner.getClass().getName());

    storeAndVersionConfigsUnderTest = setupStoreAndVersionMocks(
        partitionCount,
        partitionerConfig,
        hybridStoreConfig,
        incrementalPushEnabled,
        chunkingEnabled,
        rmdChunkingEnabled,
        true,
        aaConfig,
        storeVersionConfigOverride);
    Store mockStore = storeAndVersionConfigsUnderTest.store;
    Version version = storeAndVersionConfigsUnderTest.version;
    VeniceStoreVersionConfig storeConfig = storeAndVersionConfigsUnderTest.storeVersionConfig;

    StoreIngestionTaskFactory ingestionTaskFactory =
        getIngestionTaskFactoryBuilder(pollStrategy, partitions, diskUsageForTest, extraServerProperties, false)
            .build();

    Properties kafkaProps = new Properties();
    kafkaProps.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryLocalKafkaBroker.getKafkaBootstrapServer());

    storeIngestionTaskUnderTest = spy(
        ingestionTaskFactory.getNewIngestionTask(
            mockStore,
            version,
            kafkaProps,
            isCurrentVersion,
            storeConfig,
            PARTITION_FOO,
            false,
            Optional.empty(),
            getRecordTransformer));

    Future testSubscribeTaskFuture = null;
    try {
      for (int partition: partitions) {
        storeIngestionTaskUnderTest.subscribePartition(new PubSubTopicPartitionImpl(pubSubTopic, partition));
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
      AAConfig aaConfig) {
    return setupStoreAndVersionMocks(
        partitionCount,
        partitionerConfig,
        hybridStoreConfig,
        incrementalPushEnabled,
        false,
        false,
        isNativeReplicationEnabled,
        aaConfig,
        storeVersionConfigOverride -> {});
  }

  private MockStoreVersionConfigs setupStoreAndVersionMocks(
      int partitionCount,
      PartitionerConfig partitionerConfig,
      Optional<HybridStoreConfig> hybridStoreConfig,
      boolean incrementalPushEnabled,
      boolean chunkingEnabled,
      boolean rmdChunkingEnabled,
      boolean isNativeReplicationEnabled,
      AAConfig aaConfig,
      Consumer<VeniceStoreVersionConfig> storeVersionConfigOverride) {
    boolean isHybrid = hybridStoreConfig.isPresent();
    HybridStoreConfig hybridStoreConfigValue = null;
    if (isHybrid) {
      hybridStoreConfigValue = hybridStoreConfig.get();
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

    version.setHybridStoreConfig(hybridStoreConfigValue);
    doReturn(hybridStoreConfigValue).when(mockStore).getHybridStoreConfig();
    doReturn(isHybrid).when(mockStore).isHybrid();

    version.setBufferReplayEnabledForHybrid(true);

    version.setNativeReplicationEnabled(isNativeReplicationEnabled);
    doReturn(isNativeReplicationEnabled).when(mockStore).isNativeReplicationEnabled();

    version.setChunkingEnabled(chunkingEnabled);
    doReturn(chunkingEnabled).when(mockStore).isChunkingEnabled();
    version.setRmdChunkingEnabled(rmdChunkingEnabled);
    doReturn(rmdChunkingEnabled).when(mockStore).isRmdChunkingEnabled();

    version.setPushStreamSourceAddress("");
    doReturn("").when(mockStore).getPushStreamSourceAddress();

    doReturn(false).when(mockStore).isWriteComputationEnabled();

    doReturn(false).when(mockStore).isBlobTransferEnabled();

    doReturn(1).when(mockStore).getPartitionCount();

    doReturn(VeniceWriter.UNLIMITED_MAX_RECORD_SIZE).when(mockStore).getMaxRecordSizeBytes();
    doReturn(VeniceWriter.UNLIMITED_MAX_RECORD_SIZE).when(mockStore).getMaxNearlineRecordSizeBytes();

    doReturn(false).when(mockStore).isHybridStoreDiskQuotaEnabled();
    doReturn(-1).when(mockStore).getCurrentVersion();
    doReturn(1).when(mockStore).getBootstrapToOnlineTimeoutInHours();

    version.setActiveActiveReplicationEnabled(aaConfig == AA_ON);
    doReturn(aaConfig == AA_ON).when(mockStore).isActiveActiveReplicationEnabled();
    version.setRmdVersionId(REPLICATION_METADATA_VERSION_ID);

    doReturn(version).when(mockStore).getVersion(anyInt());
    doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeNameWithoutVersionInfo);
    doReturn(mockStore).when(mockMetadataRepo).getStore(storeNameWithoutVersionInfo);

    return new MockStoreVersionConfigs(mockStore, version, storeConfig);
  }

  private StoreIngestionTaskFactory.Builder getIngestionTaskFactoryBuilder(
      PollStrategy pollStrategy,
      Set<Integer> partitions,
      Optional<DiskUsage> diskUsageForTest,
      Map<String, Object> extraServerProperties,
      Boolean isLiveConfigEnabled) {
    doReturn(new DeepCopyStorageEngine(mockAbstractStorageEngine)).when(mockStorageEngineRepository)
        .getLocalStorageEngine(topic);

    inMemoryLocalKafkaConsumer =
        new MockInMemoryConsumer(inMemoryLocalKafkaBroker, pollStrategy, mockLocalKafkaConsumer);

    inMemoryRemoteKafkaConsumer = remotePollStrategy
        .map(strategy -> new MockInMemoryConsumer(inMemoryRemoteKafkaBroker, strategy, mockRemoteKafkaConsumer))
        .orElseGet(() -> new MockInMemoryConsumer(inMemoryRemoteKafkaBroker, pollStrategy, mockRemoteKafkaConsumer));

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
      for (int partition: partitions) {
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

    Properties localKafkaProps = new Properties();
    localKafkaProps.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
    localKafkaConsumerService = getConsumerAssignmentStrategy().constructor.construct(
        ConsumerPoolType.REGULAR_POOL,
        mockFactory,
        localKafkaProps,
        10,
        1,
        mockIngestionThrottler,
        kafkaClusterBasedRecordThrottler,
        mockMetricRepo,
        inMemoryLocalKafkaBroker.getKafkaBootstrapServer(),
        1000,
        mock(TopicExistenceChecker.class),
        isLiveConfigEnabled,
        pubSubDeserializer,
        SystemTime.INSTANCE,
        kafkaConsumerServiceStats,
        false,
        mock(ReadOnlyStoreRepository.class),
        false);
    localKafkaConsumerService.start();

    Properties remoteKafkaProps = new Properties();
    remoteKafkaProps.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryRemoteKafkaBroker.getKafkaBootstrapServer());
    remoteKafkaConsumerService = getConsumerAssignmentStrategy().constructor.construct(
        ConsumerPoolType.REGULAR_POOL,
        mockFactory,
        remoteKafkaProps,
        10,
        1,
        mockIngestionThrottler,
        kafkaClusterBasedRecordThrottler,
        mockMetricRepo,
        inMemoryLocalKafkaBroker.getKafkaBootstrapServer(),
        1000,
        mock(TopicExistenceChecker.class),
        isLiveConfigEnabled,
        pubSubDeserializer,
        SystemTime.INSTANCE,
        kafkaConsumerServiceStats,
        false,
        mock(ReadOnlyStoreRepository.class),
        false);
    remoteKafkaConsumerService.start();

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
      PartitionReplicaIngestionContext partitionReplicaIngestionContext =
          invocation.getArgument(2, PartitionReplicaIngestionContext.class);
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
      StorePartitionDataReceiver dataReceiver = new StorePartitionDataReceiver(
          storeIngestionTask,
          partitionReplicaIngestionContext.getPubSubTopicPartition(),
          kafkaUrl,
          kafkaClusterId);
      kafkaConsumerService.startConsumptionIntoDataReceiver(partitionReplicaIngestionContext, offset, dataReceiver);

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

  @Test(timeOut = 10 * Time.MS_PER_SECOND)
  public void testMissingZstdDictionary() throws Exception {
    doAnswer(invocation -> {
      Function<StoreVersionState, StoreVersionState> mapFunction = invocation.getArgument(1);
      StoreVersionState result = mapFunction.apply(null);
      return result;
    }).when(mockStorageMetadataService).computeStoreVersionState(anyString(), any());

    localVeniceWriter.broadcastStartOfPush(false, false, CompressionStrategy.ZSTD_WITH_DICT, new HashMap<>());

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        assertNotNull(storeIngestionTaskUnderTest.getLastConsumerException());
        Assert.assertTrue(
            storeIngestionTaskUnderTest.getLastConsumerException()
                .getMessage()
                .contains("compression Dictionary should not be empty if CompressionStrategy is ZSTD_WITH_DICT"));
      });
    }, AA_ON);
  }

  /**
   * Verifies that the VeniceMessages from KafkaConsumer are processed appropriately as follows:
   * 1. A VeniceMessage with PUT requests leads to invoking of AbstractStorageEngine#put.
   * 2. A VeniceMessage with DELETE requests leads to invoking of AbstractStorageEngine#delete.
   * 3. A VeniceMessage with a Kafka offset that was already processed is ignored.
   */
  @Test(dataProvider = "aaConfigProvider")
  public void testVeniceMessagesProcessing(AAConfig aaConfig) throws Exception {
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
      verifyPutAndDelete(aaConfig, true);
      // Verify it commits the offset to Offset Manager
      OffsetRecord expectedOffsetRecordForDeleteMessage = getOffsetRecord(deleteMetadata.getOffset());
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS))
          .put(topic, PARTITION_FOO, expectedOffsetRecordForDeleteMessage);

      verify(mockVersionedStorageIngestionStats, timeout(TEST_TIMEOUT_MS).atLeast(3))
          .recordConsumedRecordEndToEndProcessingLatency(any(), eq(1), anyDouble(), anyLong());
    }, aaConfig, null);

    // verify the shared consumer should be detached when the ingestion task is closed.
    verify(aggKafkaConsumerService).unsubscribeAll(pubSubTopic);
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testRecordLevelMetricForCurrentVersion(boolean enableRecordLevelMetricForCurrentVersionBootstrapping)
      throws Exception {
    Map<String, Object> extraProps = new HashMap<>();
    extraProps.put(
        SERVER_RECORD_LEVEL_METRICS_WHEN_BOOTSTRAPPING_CURRENT_VERSION_ENABLED,
        enableRecordLevelMetricForCurrentVersionBootstrapping);

    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(
        -1,
        100,
        HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP);

    VeniceWriter vtWriter = getVeniceWriter(new MockInMemoryProducerAdapter(inMemoryLocalKafkaBroker));
    vtWriter.broadcastStartOfPush(Collections.emptyMap());
    vtWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID).get();
    vtWriter.broadcastEndOfPush(Collections.emptyMap());
    // Write more messages after EOP
    vtWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID).get();
    vtWriter.put(putKeyFoo2, putValue, EXISTING_SCHEMA_ID).get();

    isCurrentVersion = () -> true;

    runTest(new RandomPollStrategy(), Utils.setOf(PARTITION_FOO), () -> {}, () -> {
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS))
          .put(PARTITION_FOO, putKeyFoo2, ByteBuffer.wrap(ValueRecord.create(SCHEMA_ID, putValue).serialize()));
      // Verify host-level metrics
      if (enableRecordLevelMetricForCurrentVersionBootstrapping) {
        verify(mockStoreIngestionStats, times(3)).recordTotalBytesConsumed(anyLong());
      } else {
        verify(mockStoreIngestionStats, times(2)).recordTotalBytesConsumed(anyLong());
      }
      verify(mockStoreIngestionStats, times(3)).recordTotalRecordsConsumed();

    }, Optional.of(hybridStoreConfig), false, Optional.empty(), AA_OFF, extraProps);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testMissingMessagesForTopicWithLogCompactionEnabled(AAConfig aaConfig) throws Exception {
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
    }, aaConfig, null);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testVeniceMessagesProcessingWithExistingSchemaId(AAConfig aaConfig) throws Exception {
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
    }, aaConfig);
  }

  /**
   * Test the situation where records arrive faster than the schemas.
   * In this case, Venice would keep polling schemaRepo until schemas arrive.
   */
  @Test(dataProvider = "aaConfigProvider")
  public void testVeniceMessagesProcessingWithTemporarilyNotAvailableSchemaId(AAConfig aaConfig) throws Exception {
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
    }, aaConfig);
  }

  /**
   * Test the situation where records' schemas never arrive. In the case, the StoreIngestionTask will keep being blocked.
   */
  @Test(dataProvider = "aaConfigProvider")
  public void testVeniceMessagesProcessingWithNonExistingSchemaId(AAConfig aaConfig) throws Exception {
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
    }, aaConfig);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testReportStartWhenRestarting(AAConfig aaConfig) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    final long STARTING_OFFSET = 2;
    runTest(Utils.setOf(PARTITION_FOO, PARTITION_BAR), () -> {
      doReturn(getOffsetRecord(STARTING_OFFSET)).when(mockStorageMetadataService).getLastOffset(anyString(), anyInt());
    }, () -> {
      // Verify STARTED is NOT reported when offset is 0
      verify(mockLogNotifier, never()).started(topic, PARTITION_BAR);
    }, aaConfig);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testNotifier(AAConfig aaConfig) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    long fooLastOffset = getOffset(localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    long barLastOffset = getOffset(localVeniceWriter.put(putKeyBar, putValue, SCHEMA_ID));
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());
    doReturn(fooLastOffset + 1).when(mockTopicManager).getLatestOffsetCached(any(), eq(PARTITION_FOO));
    doReturn(barLastOffset + 1).when(mockTopicManager).getLatestOffsetCached(any(), eq(PARTITION_BAR));

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
    }, aaConfig);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testReadyToServePartition(AAConfig aaConfig) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      Store mockStore = mock(Store.class);
      doReturn(true).when(mockStore).isHybrid();
      doReturn(new VersionImpl("storeName", 1)).when(mockStore).getVersion(1);
      doReturn(storeNameWithoutVersionInfo).when(mockStore).getName();
      doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeNameWithoutVersionInfo);
    }, () -> {
      ArgumentCaptor<StoragePartitionConfig> storagePartitionConfigArgumentCaptor =
          ArgumentCaptor.forClass(StoragePartitionConfig.class);
      TestUtils.waitForNonDeterministicAssertion(
          10,
          TimeUnit.SECONDS,
          () -> verify(mockAbstractStorageEngine).adjustStoragePartition(
              eq(PARTITION_FOO),
              eq(PREPARE_FOR_READ),
              storagePartitionConfigArgumentCaptor.capture()));
      StoragePartitionConfig storagePartitionConfigParam = storagePartitionConfigArgumentCaptor.getValue();
      assertEquals(storagePartitionConfigParam.getPartitionId(), PARTITION_FOO);
      assertFalse(storagePartitionConfigParam.isWriteOnlyConfig());
      assertFalse(storagePartitionConfigParam.isReadOnly());
      assertFalse(storagePartitionConfigParam.isDeferredWrite());
      assertFalse(storagePartitionConfigParam.isReadWriteLeaderForDefaultCF());
      assertFalse(storagePartitionConfigParam.isReadWriteLeaderForRMDCF());
    }, aaConfig);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testReadyToServePartitionValidateIngestionSuccess(AAConfig aaConfig) throws Exception {
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
      verify(mockAbstractStorageEngine, never()).adjustStoragePartition(eq(PARTITION_FOO), eq(PREPARE_FOR_READ), any());
    }, aaConfig);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testReadyToServePartitionWriteOnly(AAConfig aaConfig) throws Exception {
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
      verify(mockAbstractStorageEngine, never()).adjustStoragePartition(eq(PARTITION_FOO), eq(PREPARE_FOR_READ), any());
      verify(mockAbstractStorageEngine, never()).adjustStoragePartition(eq(PARTITION_BAR), eq(PREPARE_FOR_READ), any());

    }, aaConfig);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testResetPartition(AAConfig aaConfig) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID).get();

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS))
          .put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(SCHEMA_ID, putValue).serialize()));

      storeIngestionTaskUnderTest.resetPartitionConsumptionOffset(fooTopicPartition);

      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).clearOffset(topic, PARTITION_FOO);
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS).times(2))
          .put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(SCHEMA_ID, putValue).serialize()));
    }, aaConfig);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testResetPartitionAfterUnsubscription(AAConfig aaConfig) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID).get();

    doThrow(new PubSubUnsubscribedTopicPartitionException(fooTopicPartition)).when(mockLocalKafkaConsumer)
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
    }, aaConfig);
  }

  /**
   * In this test, partition FOO will complete successfully, but partition BAR will be missing a record.
   *
   * The {@link VeniceNotifier} should see the completion and error reported for the appropriate partitions.
   */
  @Test(dataProvider = "aaConfigProvider")
  public void testDetectionOfMissingRecord(AAConfig aaConfig) throws Exception {
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
    }, aaConfig, null);
  }

  /**
   * In this test, partition FOO will complete normally, but partition BAR will contain a duplicate record. The
   * {@link VeniceNotifier} should see the completion for both partitions.
   */
  @Test(dataProvider = "aaConfigProvider")
  public void testSkippingOfDuplicateRecord(AAConfig aaConfig) throws Exception {
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
    }, aaConfig, null);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testThrottling(AAConfig aaConfig) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);
    localVeniceWriter.delete(deleteKeyFoo, null);

    runTest(new RandomPollStrategy(1), Utils.setOf(PARTITION_FOO), () -> {}, () -> {
      // START_OF_SEGMENT, START_OF_PUSH, PUT, DELETE
      verify(mockIngestionThrottler, timeout(TEST_TIMEOUT_MS).times(4))
          .maybeThrottleRecordRate(ConsumerPoolType.REGULAR_POOL, 1);
      verify(mockIngestionThrottler, timeout(TEST_TIMEOUT_MS).times(4)).maybeThrottleBandwidth(anyInt());
    }, aaConfig, null);
  }

  /**
   * This test crafts a couple of invalid message types, and expects the {@link StoreIngestionTask} to fail fast. The
   * message in {@link #PARTITION_FOO} will receive a bad message type, whereas the message in {@link #PARTITION_BAR}
   * will receive a bad control message type.
   */
  @Test(dataProvider = "aaConfigProvider")
  public void testBadMessageTypesFailFast(AAConfig aaConfig) throws Exception {
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
      verify(kafkaConsumerServiceStats, timeout(TEST_TIMEOUT_MS).atLeastOnce()).recordTotalPollError();
    }, aaConfig);
  }

  /**
   * In this test, {@link #PARTITION_BAR} will finish a regular push, and then get some more messages afterwards,
   * including a corrupt message followed by a good one. We expect the Notifier to not report any errors after the
   * EOP.
   */
  @Test(dataProvider = "aaConfigProvider")
  public void testCorruptMessagesDoNotFailFastAfterEOP(AAConfig aaConfig) throws Exception {
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

      }, aaConfig);
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
  @Test(dataProvider = "aaConfigProvider")
  public void testDIVErrorMessagesNotFailFastAfterEOP(AAConfig aaConfig) throws Exception {
    VeniceWriter veniceWriterCorrupted = getCorruptedVeniceWriter(putValueToCorrupt, inMemoryLocalKafkaBroker);

    // do a batch push
    veniceWriterCorrupted.broadcastStartOfPush(new HashMap<>());
    long lastOffsetBeforeEOP = getOffset(veniceWriterCorrupted.put(putKeyBar, putValue, SCHEMA_ID));
    veniceWriterCorrupted.broadcastEndOfPush(new HashMap<>());

    // simulate the version swap.
    isCurrentVersion = () -> true;

    // After the end of push, the venice writer continue puts a corrupt data and end the segment.
    getOffset(veniceWriterCorrupted.put(putKeyFoo, putValueToCorrupt, SCHEMA_ID));
    veniceWriterCorrupted.closePartition(PARTITION_FOO);

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
    }, aaConfig, null);
  }

  /**
   * In this test, the {@link #PARTITION_FOO} will receive a well-formed message, while the {@link #PARTITION_BAR} will
   * receive a corrupt message. We expect the Notifier to report as such.
   * <p>
   * N.B.: There was an edge case where this test was flaky. The edge case is now fixed, but the invocationCount of 100
   * should ensure that if this test is ever made flaky again, it will be detected right away. The skipFailedInvocations
   * annotation parameter makes the test skip any invocation after the first failure.
   */
  @Test(dataProvider = "aaConfigProvider", invocationCount = 100, skipFailedInvocations = true)
  public void testCorruptMessagesFailFast(AAConfig aaConfig) throws Exception {
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
    }, aaConfig);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testSubscribeCompletedPartition(AAConfig aaConfig) throws Exception {
    final int offset = 100;
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    runTest(
        Utils.setOf(PARTITION_FOO),
        () -> doReturn(getOffsetRecord(offset, true)).when(mockStorageMetadataService)
            .getLastOffset(topic, PARTITION_FOO),
        () -> {
          verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).completed(topic, PARTITION_FOO, offset, "STANDBY");
        },
        aaConfig);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testSubscribeCompletedPartitionUnsubscribe(AAConfig aaConfig) throws Exception {
    final int offset = 100;
    final long LONG_TEST_TIMEOUT = 2 * TEST_TIMEOUT_MS;
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    Map<String, Object> extraServerProperties = new HashMap<>();
    extraServerProperties.put(SERVER_UNSUB_AFTER_BATCHPUSH, true);

    runTest(new RandomPollStrategy(), Utils.setOf(PARTITION_FOO), () -> {
      Store mockStore = mock(Store.class);
      doReturn(storeNameWithoutVersionInfo).when(mockStore).getName();
      doReturn(1).when(mockStore).getCurrentVersion();
      doReturn(new VersionImpl("storeName", 1, Version.numberBasedDummyPushId(1))).when(mockStore).getVersion(1);
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
    }, this.hybridStoreConfig, false, Optional.empty(), aaConfig, extraServerProperties);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testCompleteCalledWhenUnsubscribeAfterBatchPushDisabled(AAConfig aaConfig) throws Exception {
    final int offset = 10;
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      Store mockStore = mock(Store.class);
      storeIngestionTaskUnderTest.unSubscribePartition(fooTopicPartition);
      doReturn(storeNameWithoutVersionInfo).when(mockStore).getName();
      doReturn(1).when(mockStore).getCurrentVersion();
      doReturn(new VersionImpl(storeNameWithoutVersionInfo, 1, Version.numberBasedDummyPushId(1))).when(mockStore)
          .getVersion(1);
      doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeNameWithoutVersionInfo);
      doReturn(getOffsetRecord(offset, true)).when(mockStorageMetadataService).getLastOffset(topic, PARTITION_FOO);
    },
        () -> verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).completed(topic, PARTITION_FOO, offset, "STANDBY"),
        aaConfig);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testUnsubscribeConsumption(AAConfig aaConfig) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).started(topic, PARTITION_FOO);
      // Start of push has already been consumed. Stop consumption
      storeIngestionTaskUnderTest.unSubscribePartition(fooTopicPartition);
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).stopped(anyString(), anyInt(), anyLong());
    }, aaConfig);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testKillConsumption(AAConfig aaConfig) throws Exception {
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
      }, aaConfig);
    } finally {
      TestUtils.shutdownThread(writingThread);
    }
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testKillActionPriority(AAConfig aaConfig) throws Exception {
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
    }, aaConfig);
  }

  private byte[] getNumberedKey(int number) {
    return ByteBuffer.allocate(putKeyFoo.length + Integer.BYTES).put(putKeyFoo).putInt(number).array();
  }

  private byte[] getNumberedKeyForPartitionBar(int number) {
    return ByteBuffer.allocate(putKeyBar.length + Integer.BYTES).put(putKeyBar).putInt(number).array();
  }

  private byte[] getNumberedValue(int number) {
    return ByteBuffer.allocate(putValue.length + Integer.BYTES).put(putValue).putInt(number).array();
  }

  @Test(dataProvider = "sortedInputAndAAConfigProvider")
  public void testDataValidationCheckPointing(SortedInput sortedInput, AAConfig aaConfig) throws Exception {
    final Map<Integer, Long> maxOffsetPerPartition = new HashMap<>();
    final Map<Pair<Integer, ByteArray>, ByteArray> pushedRecords = new HashMap<>();
    final int totalNumberOfMessages = 1000;
    final int totalNumberOfConsumptionRestarts = 10;
    final long LONG_TEST_TIMEOUT = 2 * TEST_TIMEOUT_MS;

    setStoreVersionStateSupplier(sortedInput == SORTED);
    localVeniceWriter.broadcastStartOfPush(sortedInput == SORTED, new HashMap<>());
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
                        .subscribePartition(new PubSubTopicPartitionImpl(pubSubTopic, partition)));
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

        if (sortedInput == SORTED) {
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
        assertNotNull(storeIngestionTaskUnderTest.getPartitionConsumptionState(partition));
        PartitionConsumptionState pcs = storeIngestionTaskUnderTest.getPartitionConsumptionState(partition);
        Assert.assertTrue(pcs.getLatestProcessedUpstreamRTOffsetMap().isEmpty());
      });
    }, aaConfig, null);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testKillAfterPartitionIsCompleted(AAConfig aaConfig) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    long fooLastOffset = getOffset(localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      verify(mockLogNotifier, after(TEST_TIMEOUT_MS).never()).error(eq(topic), eq(PARTITION_FOO), anyString(), any());
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce()).started(topic, PARTITION_FOO);

      storeIngestionTaskUnderTest.kill();
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce())
          .endOfPushReceived(topic, PARTITION_FOO, fooLastOffset);
    }, aaConfig);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testNeverReportProgressBeforeStart(AAConfig aaConfig) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    // Read one message for each poll.
    runTest(new RandomPollStrategy(1), Utils.setOf(PARTITION_FOO), () -> {}, () -> {
      verify(mockLogNotifier, after(TEST_TIMEOUT_MS).never()).progress(topic, PARTITION_FOO, 0);
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce()).started(topic, PARTITION_FOO);
      // The current behavior is only to sync offset/report progress after processing a pre-configured amount
      // of messages in bytes, since control message is being counted as 0 bytes (no data persisted in disk),
      // then no progress will be reported during start, but only for processed messages.
      verify(mockLogNotifier, after(TEST_TIMEOUT_MS).never()).progress(any(), anyInt(), anyInt());
    }, aaConfig, null);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testOffsetPersistent(AAConfig aaConfig) throws Exception {
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
          aaConfig,
          Collections.emptyMap());
    } finally {
      databaseSyncBytesIntervalForTransactionalMode = 1;
    }

  }

  @Test(dataProvider = "aaConfigProvider")
  public void testVeniceMessagesProcessingWithSortedInput(AAConfig aaConfig) throws Exception {
    setStoreVersionStateSupplier(true);
    localVeniceWriter.broadcastStartOfPush(true, new HashMap<>());
    PubSubProduceResult putMetadata =
        (PubSubProduceResult) localVeniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID).get();
    PubSubProduceResult deleteMetadata = (PubSubProduceResult) localVeniceWriter.delete(deleteKeyFoo, null).get();
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      // Verify it retrieves the offset from the Offset Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).getLastOffset(topic, PARTITION_FOO);

      verifyPutAndDelete(aaConfig, true);

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
      assertTrue(storeIngestionTaskUnderTest.hasAllPartitionReportedCompleted());
    }, aaConfig);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testVeniceMessagesProcessingWithSortedInputVerifyChecksum(AAConfig aaConfig) throws Exception {
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
    }, aaConfig);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testDelayedTransitionToOnlineInHybridMode(AAConfig aaConfig) throws Exception {
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
    long[] messageCountPerPartition = new long[PARTITION_COUNT];

    when(mockTopicManager.getLatestOffsetCached(any(), anyInt())).thenAnswer(invocation -> {
      int partitionNumber = invocation.getArgument(1);
      return messageCountPerPartition[partitionNumber];
    });

    runTest(ALL_PARTITIONS, () -> {
      localVeniceWriter.broadcastStartOfPush(Collections.emptyMap());
      for (int partition: ALL_PARTITIONS) {
        // Taking into account both the initial SOS and the SOP
        messageCountPerPartition[partition] += 2;
      }
      for (int i = 0; i < MESSAGES_BEFORE_EOP; i++) {
        try {
          CompletableFuture<PubSubProduceResult> future =
              localVeniceWriter.put(getNumberedKey(i), getNumberedValue(i), SCHEMA_ID);
          PubSubProduceResult result = future.get();
          int partition = result.getPartition();
          messageCountPerPartition[partition]++;
        } catch (InterruptedException | ExecutionException e) {
          throw new VeniceException(e);
        }
      }
      localVeniceWriter.broadcastEndOfPush(Collections.emptyMap());
      for (int partition: ALL_PARTITIONS) {
        messageCountPerPartition[partition]++;
      }

    }, () -> {
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeast(ALL_PARTITIONS.size())).started(eq(topic), anyInt());
      verify(mockLogNotifier, never()).completed(anyString(), anyInt(), anyLong());

      localVeniceWriter.broadcastTopicSwitch(
          Collections.singletonList(inMemoryLocalKafkaBroker.getKafkaBootstrapServer()),
          Version.composeRealTimeTopic(storeNameWithoutVersionInfo),
          System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(10),
          Collections.emptyMap());
      for (int partition: ALL_PARTITIONS) {
        messageCountPerPartition[partition]++;
      }

      for (int i = 0; i < MESSAGES_AFTER_EOP; i++) {
        try {
          CompletableFuture<PubSubProduceResult> future =
              localVeniceWriter.put(getNumberedKey(i), getNumberedValue(i), SCHEMA_ID);
          PubSubProduceResult result = future.get();
          int partition = result.getPartition();
          messageCountPerPartition[partition]++;
        } catch (InterruptedException | ExecutionException e) {
          throw new VeniceException(e);
        }
      }

      // HB SOS is not sent yet, so the standby replicas should not be completed
      verify(mockLogNotifier, never()).completed(anyString(), anyInt(), anyLong());

      // send HB SOS
      for (int partition: ALL_PARTITIONS) {
        PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(
            pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeNameWithoutVersionInfo, 1)),
            partition);
        localVeniceWriter.sendHeartbeat(
            topicPartition,
            null,
            DEFAULT_LEADER_METADATA_WRAPPER,
            true,
            LeaderCompleteState.getLeaderCompleteState(true),
            System.currentTimeMillis());
        messageCountPerPartition[partition]++;
      }

      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeast(ALL_PARTITIONS.size()))
          .completed(anyString(), anyInt(), anyLong(), anyString());

    }, aaConfig);
  }

  /**
   * This test writes a message to Kafka then creates a StoreIngestionTask (and StoreBufferDrainer)  It also passes a DiskUsage
   * object to the StoreIngestionTask that always reports disk full.  This means when the StoreBufferDrainer tries to persist
   * the record, it will receive a disk full error.  This test checks for that disk full error on the Notifier object.
   * @throws Exception
   */
  @Test(dataProvider = "aaConfigProvider")
  public void testStoreIngestionTaskRespectsDiskUsage(AAConfig aaConfig) throws Exception {
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
        aaConfig,
        Collections.emptyMap());
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testIncrementalPush(AAConfig aaConfig) throws Exception {
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
    }, Optional.of(hybridStoreConfig), true, Optional.empty(), aaConfig, Collections.emptyMap());
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testSchemaCacheWarming(AAConfig aaConfig) throws Exception {
    setStoreVersionStateSupplier(true);
    localVeniceWriter.broadcastStartOfPush(true, new HashMap<>());
    long fooOffset = getOffset(localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());
    SchemaEntry schemaEntry = new SchemaEntry(1, STRING_SCHEMA);
    // Records order are: StartOfSeg, StartOfPush, data, EndOfPush, EndOfSeg
    runTest(new RandomPollStrategy(), Utils.setOf(PARTITION_FOO), () -> {
      Store mockStore = mock(Store.class);
      doReturn(storeNameWithoutVersionInfo).when(mockStore).getName();
      doReturn(true).when(mockStore).isReadComputationEnabled();
      doReturn(true).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);
      doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeNameWithoutVersionInfo);
      doReturn(schemaEntry).when(mockSchemaRepo).getValueSchema(anyString(), anyInt());
      doReturn(new VersionImpl("storeName", 1)).when(mockStore).getVersion(1);
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
        aaConfig,
        Collections.singletonMap(SERVER_NUM_SCHEMA_FAST_CLASS_WARMUP, 1));
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testReportErrorWithEmptyPcsMap(AAConfig aaConfig) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID);
    // Dummy exception to put ingestion task into ERROR state
    doThrow(new VeniceException("fake exception")).when(mockVersionedStorageIngestionStats)
        .resetIngestionTaskPushTimeoutGauge(anyString(), anyInt());

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).error(eq(topic), eq(PARTITION_FOO), anyString(), any());
    }, aaConfig);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testPartitionExceptionIsolation(AAConfig aaConfig) throws Exception {
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
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, () -> {
        assertNull(
            storeIngestionTaskUnderTest.getPartitionIngestionExceptionList().get(PARTITION_FOO),
            "Exception for the errored partition should be cleared after unsubscription");
        assertEquals(
            storeIngestionTaskUnderTest.getFailedPartitions().size(),
            1,
            "Only one partition should be failed");
      });
    }, aaConfig);
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
    propertyBuilder.put(SERVER_INGESTION_HEARTBEAT_INTERVAL_MS, 1000);
    propertyBuilder.put(SERVER_LEADER_COMPLETE_STATE_CHECK_IN_FOLLOWER_VALID_INTERVAL_MS, 1000);
    propertyBuilder.put(SERVER_RESUBSCRIPTION_TRIGGERED_BY_VERSION_INGESTION_CONTEXT_CHANGE_ENABLED, true);
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

  private void verifyPutAndDelete(AAConfig aaConfig, boolean recordsInBatchPush) {
    VenicePartitioner partitioner = getVenicePartitioner();
    int targetPartitionPutKeyFoo = partitioner.getPartitionId(putKeyFoo, PARTITION_COUNT);
    int targetPartitionDeleteKeyFoo = partitioner.getPartitionId(deleteKeyFoo, PARTITION_COUNT);

    // Batch push records for Active/Active do not persist replication metadata.
    if (aaConfig == AA_ON && !recordsInBatchPush) {
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

    VenicePartitioner partitioner = getVenicePartitioner(); // Only get base venice partitioner
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setPartitionerClass(partitioner.getClass().getName());

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
        AA_ON);
    Store mockStore = storeAndVersionConfigs.store;
    Version version = storeAndVersionConfigs.version;
    VeniceStoreVersionConfig storeConfig = storeAndVersionConfigs.storeVersionConfig;

    Map<String, Object> extraServerProperties = new HashMap<>();
    extraServerProperties.put(SERVER_ENABLE_LIVE_CONFIG_BASED_KAFKA_THROTTLING, true);

    StoreIngestionTaskFactory ingestionTaskFactory = getIngestionTaskFactoryBuilder(
        new RandomPollStrategy(),
        Utils.setOf(PARTITION_FOO),
        Optional.empty(),
        extraServerProperties,
        true).build();

    Properties kafkaProps = new Properties();
    kafkaProps.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryLocalKafkaBroker.getKafkaBootstrapServer());

    storeIngestionTaskUnderTest = ingestionTaskFactory.getNewIngestionTask(
        mockStore,
        version,
        kafkaProps,
        isCurrentVersion,
        storeConfig,
        PARTITION_FOO,
        false,
        Optional.empty(),
        null);

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

    PartitionReplicaIngestionContext fooRtPartitionReplicaIngestionContext = new PartitionReplicaIngestionContext(
        pubSubTopic,
        fooRtPartition,
        PartitionReplicaIngestionContext.VersionRole.CURRENT,
        PartitionReplicaIngestionContext.WorkloadType.NON_AA_OR_WRITE_COMPUTE);
    inMemoryLocalKafkaBroker.createTopic(rtTopic, partitionCount);
    inMemoryRemoteKafkaBroker.createTopic(rtTopic, partitionCount);

    aggKafkaConsumerService.subscribeConsumerFor(
        inMemoryLocalKafkaBroker.getKafkaBootstrapServer(),
        storeIngestionTaskUnderTest,
        fooRtPartitionReplicaIngestionContext,
        0);
    aggKafkaConsumerService.subscribeConsumerFor(
        inMemoryRemoteKafkaBroker.getKafkaBootstrapServer(),
        storeIngestionTaskUnderTest,
        fooRtPartitionReplicaIngestionContext,
        0);

    VeniceWriter localRtWriter = getVeniceWriter(rtTopic, new MockInMemoryProducerAdapter(inMemoryLocalKafkaBroker));
    VeniceWriter remoteRtWriter = getVeniceWriter(rtTopic, new MockInMemoryProducerAdapter(inMemoryRemoteKafkaBroker));

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

  @Test(dataProvider = "nodeTypeAndAAConfigAndDRPProvider")
  public void testIsReadyToServe(NodeType nodeType, AAConfig aaConfig, DataReplicationPolicy dataReplicationPolicy) {
    int partitionCount = 2;

    VenicePartitioner partitioner = getVenicePartitioner(); // Only get base venice partitioner
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setPartitionerClass(partitioner.getClass().getName());

    HybridStoreConfig hybridStoreConfig =
        new HybridStoreConfigImpl(100, 100, 100, dataReplicationPolicy, BufferReplayPolicy.REWIND_FROM_EOP);

    MockStoreVersionConfigs storeAndVersionConfigs = setupStoreAndVersionMocks(
        partitionCount,
        partitionerConfig,
        Optional.of(hybridStoreConfig),
        false,
        true,
        aaConfig);
    Store mockStore = storeAndVersionConfigs.store;
    Version version = storeAndVersionConfigs.version;
    VeniceStoreVersionConfig storeConfig = storeAndVersionConfigs.storeVersionConfig;

    Map<String, Object> extraServerProperties = new HashMap<>();
    extraServerProperties.put(SERVER_INGESTION_HEARTBEAT_INTERVAL_MS, 5000L);
    extraServerProperties.put(SERVER_LEADER_COMPLETE_STATE_CHECK_IN_FOLLOWER_VALID_INTERVAL_MS, 5000L);
    extraServerProperties.put(SERVER_LEADER_COMPLETE_STATE_CHECK_IN_FOLLOWER_ENABLED, true);

    StoreIngestionTaskFactory ingestionTaskFactory = getIngestionTaskFactoryBuilder(
        new RandomPollStrategy(),
        Utils.setOf(PARTITION_FOO),
        Optional.empty(),
        extraServerProperties,
        false).setIsDaVinciClient(nodeType == DA_VINCI).setAggKafkaConsumerService(aggKafkaConsumerService).build();

    TopicManager mockTopicManagerRemoteKafka = mock(TopicManager.class);

    doReturn(mockTopicManager).when(mockTopicManagerRepository)
        .getTopicManager(inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
    doReturn(mockTopicManagerRemoteKafka).when(mockTopicManagerRepository)
        .getTopicManager(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer());

    doReturn(true).when(mockTopicManager).containsTopicCached(any());
    doReturn(true).when(mockTopicManagerRemoteKafka).containsTopicCached(any());

    Properties kafkaProps = new Properties();
    kafkaProps.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryLocalKafkaBroker.getKafkaBootstrapServer());

    storeIngestionTaskUnderTest = ingestionTaskFactory.getNewIngestionTask(
        mockStore,
        version,
        kafkaProps,
        isCurrentVersion,
        storeConfig,
        PARTITION_FOO,
        false,
        Optional.empty(),
        null);
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

    // case 1: If EOP is not received, partition is not ready to serve
    PartitionConsumptionState mockPcsEOPNotReceived = mock(PartitionConsumptionState.class);
    doReturn(false).when(mockPcsEOPNotReceived).isEndOfPushReceived();
    assertFalse(storeIngestionTaskUnderTest.isReadyToServe(mockPcsEOPNotReceived));

    // case 2: If EOP is received and partition has reported COMPLETED, then partition is ready to serve
    PartitionConsumptionState mockPcsCompleted = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcsCompleted).isEndOfPushReceived();
    doReturn(true).when(mockPcsCompleted).isComplete();
    assertTrue(storeIngestionTaskUnderTest.isReadyToServe(mockPcsCompleted));

    // case 3: If partition has not reported COMPLETED but is not waiting for replication lag to catch up,
    // it is ready to serve
    PartitionConsumptionState mockPcsNotWaitingForReplicationLag = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcsNotWaitingForReplicationLag).isEndOfPushReceived();
    doReturn(false).when(mockPcsNotWaitingForReplicationLag).isComplete();
    doReturn(false).when(mockPcsNotWaitingForReplicationLag).isWaitingForReplicationLag();
    assertTrue(storeIngestionTaskUnderTest.isReadyToServe(mockPcsNotWaitingForReplicationLag));

    // case 4: If partition is waiting for replication lag to catch up, but buffer replay has not started,
    // it is not ready to serve
    PartitionConsumptionState mockPcsHybridButBufferReplayNotStarted = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcsHybridButBufferReplayNotStarted).isEndOfPushReceived();
    doReturn(false).when(mockPcsHybridButBufferReplayNotStarted).isComplete();
    doReturn(true).when(mockPcsHybridButBufferReplayNotStarted).isWaitingForReplicationLag();
    doReturn(true).when(mockPcsHybridButBufferReplayNotStarted).isHybrid();
    doReturn(null).when(mockPcsHybridButBufferReplayNotStarted).getTopicSwitch();
    assertFalse(storeIngestionTaskUnderTest.isReadyToServe(mockPcsHybridButBufferReplayNotStarted));

    // case 5: Replication lag is caught up
    PartitionConsumptionState mockPcsBufferReplayStartedLagCaughtUp = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcsBufferReplayStartedLagCaughtUp).isEndOfPushReceived();
    doReturn(false).when(mockPcsBufferReplayStartedLagCaughtUp).isComplete();
    doReturn(true).when(mockPcsBufferReplayStartedLagCaughtUp).isWaitingForReplicationLag();
    doReturn(true).when(mockPcsBufferReplayStartedLagCaughtUp).isHybrid();
    doReturn(topicSwitchWithSourceRealTimeTopicWrapper).when(mockPcsBufferReplayStartedLagCaughtUp).getTopicSwitch();
    doReturn(mockOffsetRecordLagCaughtUp).when(mockPcsBufferReplayStartedLagCaughtUp).getOffsetRecord();
    doReturn(5L).when(mockTopicManager).getLatestOffsetCached(any(), anyInt());
    doReturn(5L).when(mockTopicManagerRemoteKafka).getLatestOffsetCached(any(), anyInt());
    doReturn(0).when(mockPcsBufferReplayStartedLagCaughtUp).getPartition();
    doReturn(0).when(mockPcsBufferReplayStartedLagCaughtUp).getPartition();
    storeIngestionTaskUnderTest.setPartitionConsumptionState(0, mockPcsBufferReplayStartedLagCaughtUp);
    if (nodeType == NodeType.LEADER) {
      // case 5a: leader replica => partition is ready to serve
      doReturn(LeaderFollowerStateType.LEADER).when(mockPcsBufferReplayStartedLagCaughtUp).getLeaderFollowerState();
      assertTrue(storeIngestionTaskUnderTest.isReadyToServe(mockPcsBufferReplayStartedLagCaughtUp));
    } else {
      // case 5b: standby replica and !LEADER_COMPLETED
      doReturn(STANDBY).when(mockPcsBufferReplayStartedLagCaughtUp).getLeaderFollowerState();
      doReturn(LEADER_NOT_COMPLETED).when(mockPcsBufferReplayStartedLagCaughtUp).getLeaderCompleteState();
      assertEquals(
          storeIngestionTaskUnderTest.isReadyToServe(mockPcsBufferReplayStartedLagCaughtUp),
          !(aaConfig == AA_ON || (aaConfig == AA_OFF && dataReplicationPolicy != DataReplicationPolicy.AGGREGATE)));
      // case 5c: standby replica and LEADER_COMPLETED => partition is ready to serve
      doReturn(LEADER_COMPLETED).when(mockPcsBufferReplayStartedLagCaughtUp).getLeaderCompleteState();
      doCallRealMethod().when(mockPcsBufferReplayStartedLagCaughtUp).isLeaderCompleted();
      doReturn(System.currentTimeMillis()).when(mockPcsBufferReplayStartedLagCaughtUp)
          .getLastLeaderCompleteStateUpdateInMs();
      assertTrue(storeIngestionTaskUnderTest.isReadyToServe(mockPcsBufferReplayStartedLagCaughtUp));
    }

    // case 6: Remote replication lag has not caught up but host has caught up to lag in local VT,
    // leader won't be marked completed, but both DaVinci replica and storage node will be marked
    // ready to serve if leader were to be completed
    PartitionConsumptionState mockPcsBufferReplayStartedRemoteLagging = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcsBufferReplayStartedRemoteLagging).isEndOfPushReceived();
    doReturn(false).when(mockPcsBufferReplayStartedRemoteLagging).isComplete();
    doReturn(true).when(mockPcsBufferReplayStartedRemoteLagging).isWaitingForReplicationLag();
    doReturn(true).when(mockPcsBufferReplayStartedRemoteLagging).isHybrid();
    doReturn(topicSwitchWithSourceRealTimeTopicWrapper).when(mockPcsBufferReplayStartedRemoteLagging).getTopicSwitch();
    doReturn(mockOffsetRecordLagCaughtUpTimestampLagging).when(mockPcsBufferReplayStartedRemoteLagging)
        .getOffsetRecord();
    doReturn(5L).when(mockTopicManager).getLatestOffsetCached(any(), anyInt());
    doReturn(150L).when(mockTopicManagerRemoteKafka).getLatestOffsetCached(any(), anyInt());
    doReturn(150L).when(aggKafkaConsumerService).getLatestOffsetBasedOnMetrics(anyString(), any(), any());
    if (nodeType == NodeType.LEADER) {
      // case 6a: leader replica => partition is not ready to serve
      doReturn(LeaderFollowerStateType.LEADER).when(mockPcsBufferReplayStartedRemoteLagging).getLeaderFollowerState();
      assertFalse(storeIngestionTaskUnderTest.isReadyToServe(mockPcsBufferReplayStartedRemoteLagging));
    } else {
      // case 6b: standby replica and !LEADER_COMPLETED
      doReturn(STANDBY).when(mockPcsBufferReplayStartedRemoteLagging).getLeaderFollowerState();
      doReturn(LEADER_NOT_COMPLETED).when(mockPcsBufferReplayStartedRemoteLagging).getLeaderCompleteState();
      assertEquals(
          storeIngestionTaskUnderTest.isReadyToServe(mockPcsBufferReplayStartedRemoteLagging),
          !(aaConfig == AA_ON || (aaConfig == AA_OFF && dataReplicationPolicy != DataReplicationPolicy.AGGREGATE)));
      // case 6c: standby replica and LEADER_COMPLETED => partition is ready to serve
      doReturn(LEADER_COMPLETED).when(mockPcsBufferReplayStartedRemoteLagging).getLeaderCompleteState();
      doCallRealMethod().when(mockPcsBufferReplayStartedRemoteLagging).isLeaderCompleted();
      doReturn(System.currentTimeMillis()).when(mockPcsBufferReplayStartedRemoteLagging)
          .getLastLeaderCompleteStateUpdateInMs();
      assertTrue(storeIngestionTaskUnderTest.isReadyToServe(mockPcsBufferReplayStartedRemoteLagging));
    }

    // case 7: If there are issues in replication from remote RT -> local VT, leader won't be marked completed,
    // but both DaVinci replica and storage node will be marked ready to serve if leader were to be completed
    PartitionConsumptionState mockPcsOffsetLagCaughtUpTimestampLagging = mock(PartitionConsumptionState.class);
    doReturn(true).when(mockPcsOffsetLagCaughtUpTimestampLagging).isEndOfPushReceived();
    doReturn(false).when(mockPcsOffsetLagCaughtUpTimestampLagging).isComplete();
    doReturn(true).when(mockPcsOffsetLagCaughtUpTimestampLagging).isWaitingForReplicationLag();
    doReturn(true).when(mockPcsOffsetLagCaughtUpTimestampLagging).isHybrid();
    doReturn(topicSwitchWithSourceRealTimeTopicWrapper).when(mockPcsOffsetLagCaughtUpTimestampLagging).getTopicSwitch();
    doReturn(mockOffsetRecordLagCaughtUpTimestampLagging).when(mockPcsOffsetLagCaughtUpTimestampLagging)
        .getOffsetRecord();
    doReturn(5L).when(mockTopicManager).getLatestOffsetCached(any(), anyInt());
    doReturn(5L).when(mockTopicManagerRemoteKafka).getLatestOffsetCached(any(), anyInt());
    doReturn(System.currentTimeMillis() - 2 * MS_PER_DAY).when(mockTopicManager)
        .getProducerTimestampOfLastDataMessageCached(any());
    doReturn(System.currentTimeMillis()).when(mockTopicManagerRemoteKafka)
        .getProducerTimestampOfLastDataMessageCached(any());
    if (nodeType == NodeType.LEADER) {
      // case 7a: leader replica => partition is not ready to serve
      doReturn(LeaderFollowerStateType.LEADER).when(mockPcsOffsetLagCaughtUpTimestampLagging).getLeaderFollowerState();
      assertFalse(storeIngestionTaskUnderTest.isReadyToServe(mockPcsOffsetLagCaughtUpTimestampLagging));
    } else {
      // case 7b: standby replica and !LEADER_COMPLETED
      doReturn(STANDBY).when(mockPcsOffsetLagCaughtUpTimestampLagging).getLeaderFollowerState();
      doReturn(LEADER_NOT_COMPLETED).when(mockPcsOffsetLagCaughtUpTimestampLagging).getLeaderCompleteState();
      assertEquals(
          storeIngestionTaskUnderTest.isReadyToServe(mockPcsOffsetLagCaughtUpTimestampLagging),
          !(aaConfig == AA_ON || (aaConfig == AA_OFF && dataReplicationPolicy != DataReplicationPolicy.AGGREGATE)));
      // case 7c: standby replica and LEADER_COMPLETED => partition is ready to serve
      doReturn(LEADER_COMPLETED).when(mockPcsOffsetLagCaughtUpTimestampLagging).getLeaderCompleteState();
      doCallRealMethod().when(mockPcsOffsetLagCaughtUpTimestampLagging).isLeaderCompleted();
      doReturn(System.currentTimeMillis()).when(mockPcsOffsetLagCaughtUpTimestampLagging)
          .getLastLeaderCompleteStateUpdateInMs();
      Assert.assertTrue(storeIngestionTaskUnderTest.isReadyToServe(mockPcsOffsetLagCaughtUpTimestampLagging));
    }
  }

  @Test(dataProvider = "hybridConfigAndNodeTypeProvider")
  public void testActiveActiveStoreIsReadyToServe(HybridConfig hybridConfig, NodeType nodeType) {
    int partitionCount = 2;
    VenicePartitioner partitioner = getVenicePartitioner();
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setPartitionerClass(partitioner.getClass().getName());
    HybridStoreConfig hybridStoreConfig = null;
    if (hybridConfig == HYBRID) {
      hybridStoreConfig = new HybridStoreConfigImpl(
          100,
          100,
          -1,
          DataReplicationPolicy.NON_AGGREGATE,
          BufferReplayPolicy.REWIND_FROM_EOP);
    }

    MockStoreVersionConfigs storeAndVersionConfigs = setupStoreAndVersionMocks(
        partitionCount,
        partitionerConfig,
        Optional.ofNullable(hybridStoreConfig),
        false,
        true,
        AA_ON);
    Store mockStore = storeAndVersionConfigs.store;
    Version version = storeAndVersionConfigs.version;
    VeniceStoreVersionConfig storeConfig = storeAndVersionConfigs.storeVersionConfig;

    StoreIngestionTaskFactory ingestionTaskFactory = getIngestionTaskFactoryBuilder(
        new RandomPollStrategy(),
        Utils.setOf(PARTITION_FOO),
        Optional.empty(),
        new HashMap<>(),
        false).setIsDaVinciClient(nodeType == DA_VINCI).setAggKafkaConsumerService(aggKafkaConsumerService).build();

    TopicManager mockTopicManagerRemoteKafka = mock(TopicManager.class);
    doReturn(mockTopicManager).when(mockTopicManagerRepository)
        .getTopicManager(inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
    doReturn(mockTopicManagerRemoteKafka).when(mockTopicManagerRepository)
        .getTopicManager(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer());
    doReturn(true).when(mockTopicManager).containsTopic(any());
    doReturn(true).when(mockTopicManagerRemoteKafka).containsTopic(any());

    Properties kafkaProps = new Properties();
    kafkaProps.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
    storeIngestionTaskUnderTest = ingestionTaskFactory.getNewIngestionTask(
        mockStore,
        version,
        kafkaProps,
        isCurrentVersion,
        storeConfig,
        PARTITION_FOO,
        false,
        Optional.empty(),
        null);

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
    doReturn(hybridConfig != HYBRID).when(mockPcsMultipleSourceKafkaServers).isComplete();
    doReturn(true).when(mockPcsMultipleSourceKafkaServers).isWaitingForReplicationLag();
    doReturn(hybridConfig == HYBRID).when(mockPcsMultipleSourceKafkaServers).isHybrid();
    doReturn(topicSwitchWithMultipleSourceKafkaServersWrapper).when(mockPcsMultipleSourceKafkaServers).getTopicSwitch();
    doReturn(mockOffsetRecord).when(mockPcsMultipleSourceKafkaServers).getOffsetRecord();
    doReturn(5L).when(mockTopicManager).getLatestOffsetCached(any(), anyInt());
    doReturn(150L).when(mockTopicManagerRemoteKafka).getLatestOffsetCached(any(), anyInt());
    doReturn(150L).when(aggKafkaConsumerService).getLatestOffsetBasedOnMetrics(anyString(), any(), any());
    doReturn(0).when(mockPcsMultipleSourceKafkaServers).getPartition();
    doReturn(0).when(mockPcsMultipleSourceKafkaServers).getPartition();
    doReturn(5L).when(mockPcsMultipleSourceKafkaServers).getLatestProcessedLocalVersionTopicOffset();
    if (nodeType == NodeType.LEADER) {
      doReturn(LeaderFollowerStateType.LEADER).when(mockPcsMultipleSourceKafkaServers).getLeaderFollowerState();
    } else {
      doReturn(STANDBY).when(mockPcsMultipleSourceKafkaServers).getLeaderFollowerState();
      doReturn(LEADER_COMPLETED).when(mockPcsMultipleSourceKafkaServers).getLeaderCompleteState();
      doCallRealMethod().when(mockPcsMultipleSourceKafkaServers).isLeaderCompleted();
      doReturn(System.currentTimeMillis()).when(mockPcsMultipleSourceKafkaServers)
          .getLastLeaderCompleteStateUpdateInMs();
    }
    storeIngestionTaskUnderTest.setPartitionConsumptionState(0, mockPcsMultipleSourceKafkaServers);
    if (hybridConfig == HYBRID && nodeType == NodeType.LEADER) {
      assertFalse(storeIngestionTaskUnderTest.isReadyToServe(mockPcsMultipleSourceKafkaServers));
    } else {
      assertTrue(storeIngestionTaskUnderTest.isReadyToServe(mockPcsMultipleSourceKafkaServers));
    }
  }

  @DataProvider
  public static Object[][] testCheckAndLogIfLagIsAcceptableForHybridStoreProvider() {
    return DataProviderUtils.allPermutationGenerator(
        LagType.values(),
        new NodeType[] { DA_VINCI, FOLLOWER },
        AAConfig.values(),
        LeaderCompleteCheck.values(),
        DataReplicationPolicy.values());
  }

  /**
   * @param lagType N.B. this only affects cosmetic logging details at the level where we mock it
   * @param nodeType Can be either DVC or follower
   * @param aaConfig AA on/off
   * @param leaderCompleteCheck Whether followers/DVC should wait for the leader to be complete
   */
  @Test(dataProvider = "testCheckAndLogIfLagIsAcceptableForHybridStoreProvider")
  public void testCheckAndLogIfLagIsAcceptableForHybridStore(
      LagType lagType,
      NodeType nodeType,
      AAConfig aaConfig,
      LeaderCompleteCheck leaderCompleteCheck,
      DataReplicationPolicy dataReplicationPolicy) {
    int partitionCount = 2;
    VenicePartitioner partitioner = getVenicePartitioner();
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setPartitionerClass(partitioner.getClass().getName());

    HybridStoreConfig hybridStoreConfig =
        new HybridStoreConfigImpl(100, 100, 100, dataReplicationPolicy, BufferReplayPolicy.REWIND_FROM_EOP);

    MockStoreVersionConfigs storeAndVersionConfigs = setupStoreAndVersionMocks(
        partitionCount,
        partitionerConfig,
        Optional.of(hybridStoreConfig),
        false,
        true,
        aaConfig);
    Store mockStore = storeAndVersionConfigs.store;
    Version version = storeAndVersionConfigs.version;
    VeniceStoreVersionConfig storeConfig = storeAndVersionConfigs.storeVersionConfig;

    Map<String, Object> serverProperties = new HashMap<>();
    serverProperties.put(SERVER_INGESTION_HEARTBEAT_INTERVAL_MS, 5000L);
    serverProperties.put(SERVER_LEADER_COMPLETE_STATE_CHECK_IN_FOLLOWER_VALID_INTERVAL_MS, 5000L);
    serverProperties
        .put(SERVER_LEADER_COMPLETE_STATE_CHECK_IN_FOLLOWER_ENABLED, leaderCompleteCheck == LEADER_COMPLETE_CHECK_ON);

    StoreIngestionTaskFactory ingestionTaskFactory = getIngestionTaskFactoryBuilder(
        new RandomPollStrategy(),
        Utils.setOf(PARTITION_FOO),
        Optional.empty(),
        serverProperties,
        false).setIsDaVinciClient(nodeType == DA_VINCI).setAggKafkaConsumerService(aggKafkaConsumerService).build();

    TopicManager mockTopicManagerRemoteKafka = mock(TopicManager.class);
    doReturn(mockTopicManager).when(mockTopicManagerRepository)
        .getTopicManager(inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
    doReturn(mockTopicManagerRemoteKafka).when(mockTopicManagerRepository)
        .getTopicManager(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer());
    doReturn(true).when(mockTopicManager).containsTopic(any());
    doReturn(true).when(mockTopicManagerRemoteKafka).containsTopic(any());

    Properties kafkaProps = new Properties();
    kafkaProps.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
    storeIngestionTaskUnderTest = ingestionTaskFactory.getNewIngestionTask(
        mockStore,
        version,
        kafkaProps,
        isCurrentVersion,
        storeConfig,
        PARTITION_FOO,
        false,
        Optional.empty(),
        null);

    PartitionConsumptionState mockPartitionConsumptionState = mock(PartitionConsumptionState.class);
    doCallRealMethod().when(mockPartitionConsumptionState).isLeaderCompleted();

    // Case 1: offsetLag > offsetThreshold and instance is leader
    long offsetLag = 100;
    long offsetThreshold = 50;
    if (nodeType != DA_VINCI) {
      doReturn(LeaderFollowerStateType.LEADER).when(mockPartitionConsumptionState).getLeaderFollowerState();
      assertFalse(
          storeIngestionTaskUnderTest.checkAndLogIfLagIsAcceptableForHybridStore(
              mockPartitionConsumptionState,
              offsetLag,
              offsetThreshold,
              true,
              lagType,
              0));
    }

    // case 2: offsetLag > offsetThreshold and instance is not a leader
    doReturn(STANDBY).when(mockPartitionConsumptionState).getLeaderFollowerState();
    assertFalse(
        storeIngestionTaskUnderTest.checkAndLogIfLagIsAcceptableForHybridStore(
            mockPartitionConsumptionState,
            offsetLag,
            offsetThreshold,
            false,
            lagType,
            0));

    // Case 3: offsetLag <= offsetThreshold and instance is not a standby or DaVinciClient
    offsetLag = 50;
    offsetThreshold = 100;
    if (nodeType != DA_VINCI) {
      doReturn(LeaderFollowerStateType.LEADER).when(mockPartitionConsumptionState).getLeaderFollowerState();
      assertTrue(
          storeIngestionTaskUnderTest.checkAndLogIfLagIsAcceptableForHybridStore(
              mockPartitionConsumptionState,
              offsetLag,
              offsetThreshold,
              false,
              lagType,
              0));
    }

    // Case 4: offsetLag <= offsetThreshold and instance is a standby or DaVinciClient
    doReturn(STANDBY).when(mockPartitionConsumptionState).getLeaderFollowerState();
    assertEquals(
        storeIngestionTaskUnderTest.checkAndLogIfLagIsAcceptableForHybridStore(
            mockPartitionConsumptionState,
            offsetLag,
            offsetThreshold,
            false,
            lagType,
            0),
        !(leaderCompleteCheck == LEADER_COMPLETE_CHECK_ON && (aaConfig == AA_ON
            || (aaConfig == AA_OFF && dataReplicationPolicy != DataReplicationPolicy.AGGREGATE))));

    // Case 5: offsetLag <= offsetThreshold and instance is a standby or DaVinciClient
    // and leaderCompleteState is LEADER_COMPLETED and last update time is within threshold
    doReturn(STANDBY).when(mockPartitionConsumptionState).getLeaderFollowerState();
    doReturn(LEADER_COMPLETED).when(mockPartitionConsumptionState).getLeaderCompleteState();
    doReturn(System.currentTimeMillis() - 1000).when(mockPartitionConsumptionState)
        .getLastLeaderCompleteStateUpdateInMs();
    assertTrue(
        storeIngestionTaskUnderTest.checkAndLogIfLagIsAcceptableForHybridStore(
            mockPartitionConsumptionState,
            offsetLag,
            offsetThreshold,
            false,
            lagType,
            0));

    // Case 6: offsetLag <= offsetThreshold and instance is a standby or DaVinciClient.
    // and leaderCompleteState is LEADER_COMPLETED and last update time is more than threshold
    doReturn(System.currentTimeMillis() - 6000).when(mockPartitionConsumptionState)
        .getLastLeaderCompleteStateUpdateInMs();
    assertEquals(
        storeIngestionTaskUnderTest.checkAndLogIfLagIsAcceptableForHybridStore(
            mockPartitionConsumptionState,
            offsetLag,
            offsetThreshold,
            false,
            lagType,
            0),
        !(leaderCompleteCheck == LEADER_COMPLETE_CHECK_ON && (aaConfig == AA_ON
            || (aaConfig == AA_OFF && dataReplicationPolicy != DataReplicationPolicy.AGGREGATE))));

    // Case 7: offsetLag <= offsetThreshold and instance is a standby or DaVinciClient
    // and leaderCompleteState is LEADER_NOT_COMPLETED and leader is not completed
    doReturn(LEADER_NOT_COMPLETED).when(mockPartitionConsumptionState).getLeaderCompleteState();
    assertEquals(
        storeIngestionTaskUnderTest.checkAndLogIfLagIsAcceptableForHybridStore(
            mockPartitionConsumptionState,
            offsetLag,
            offsetThreshold,
            false,
            lagType,
            0),
        !(leaderCompleteCheck == LEADER_COMPLETE_CHECK_ON && (aaConfig == AA_ON
            || (aaConfig == AA_OFF && dataReplicationPolicy != DataReplicationPolicy.AGGREGATE))));
  }

  @DataProvider
  public static Object[][] testGetAndUpdateLeaderCompletedStateProvider() {
    return DataProviderUtils.allPermutationGenerator(HybridConfig.values(), new NodeType[] { DA_VINCI, FOLLOWER });
  }

  @Test(dataProvider = "testGetAndUpdateLeaderCompletedStateProvider")
  public void testGetAndUpdateLeaderCompletedState(HybridConfig hybridConfig, NodeType nodeType) {
    int partitionCount = 2;
    VenicePartitioner partitioner = getVenicePartitioner();
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setPartitionerClass(partitioner.getClass().getName());

    HybridStoreConfig hybridStoreConfig = null;
    if (hybridConfig == HYBRID) {
      hybridStoreConfig =
          new HybridStoreConfigImpl(100, 100, 100, DataReplicationPolicy.AGGREGATE, BufferReplayPolicy.REWIND_FROM_EOP);
    }
    MockStoreVersionConfigs storeAndVersionConfigs = setupStoreAndVersionMocks(
        partitionCount,
        partitionerConfig,
        Optional.ofNullable(hybridStoreConfig),
        false,
        true,
        AA_ON);
    Store mockStore = storeAndVersionConfigs.store;
    Version version = storeAndVersionConfigs.version;
    VeniceStoreVersionConfig storeConfig = storeAndVersionConfigs.storeVersionConfig;

    StoreIngestionTaskFactory ingestionTaskFactory = getIngestionTaskFactoryBuilder(
        new RandomPollStrategy(),
        Utils.setOf(PARTITION_FOO),
        Optional.empty(),
        new HashMap<>(),
        false).setIsDaVinciClient(nodeType == DA_VINCI).setAggKafkaConsumerService(aggKafkaConsumerService).build();

    Properties kafkaProps = new Properties();
    kafkaProps.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
    LeaderFollowerStoreIngestionTask ingestionTask =
        (LeaderFollowerStoreIngestionTask) ingestionTaskFactory.getNewIngestionTask(
            mockStore,
            version,
            kafkaProps,
            isCurrentVersion,
            storeConfig,
            PARTITION_FOO,
            false,
            Optional.empty(),
            null);

    OffsetRecord mockOffsetRecord = mock(OffsetRecord.class);
    PartitionConsumptionState partitionConsumptionState =
        new PartitionConsumptionState(Utils.getReplicaId(topic, PARTITION_FOO), PARTITION_FOO, mockOffsetRecord, true);

    long producerTimestamp = System.currentTimeMillis();
    LeaderMetadataWrapper mockLeaderMetadataWrapper = mock(LeaderMetadataWrapper.class);
    KafkaMessageEnvelope kafkaMessageEnvelope =
        getHeartbeatKME(producerTimestamp, mockLeaderMetadataWrapper, generateHeartbeatMessage(CheckSumType.NONE), "0");

    PubSubMessageHeaders pubSubMessageHeaders = new PubSubMessageHeaders();
    pubSubMessageHeaders.add(VeniceWriter.getLeaderCompleteStateHeader(LEADER_COMPLETED));
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> pubSubMessage = new ImmutablePubSubMessage(
        KafkaKey.HEART_BEAT,
        kafkaMessageEnvelope,
        new PubSubTopicPartitionImpl(pubSubTopic, PARTITION_FOO),
        0,
        0,
        0,
        pubSubMessageHeaders);

    assertEquals(partitionConsumptionState.getLeaderCompleteState(), LEADER_NOT_COMPLETED);
    assertEquals(partitionConsumptionState.getLastLeaderCompleteStateUpdateInMs(), 0L);

    KafkaKey kafkaKey = pubSubMessage.getKey();
    KafkaMessageEnvelope kafkaValue = pubSubMessage.getValue();
    ControlMessage controlMessage = (ControlMessage) kafkaValue.payloadUnion;

    if (nodeType != DA_VINCI) {
      partitionConsumptionState.setLeaderFollowerState(LeaderFollowerStateType.LEADER);
      ingestionTask.getAndUpdateLeaderCompletedState(
          kafkaKey,
          kafkaValue,
          controlMessage,
          pubSubMessage.getPubSubMessageHeaders(),
          partitionConsumptionState);
      assertEquals(partitionConsumptionState.getLeaderCompleteState(), LEADER_NOT_COMPLETED);
      assertEquals(partitionConsumptionState.getLastLeaderCompleteStateUpdateInMs(), 0L);
    }

    partitionConsumptionState.setLeaderFollowerState(STANDBY);
    ingestionTask.getAndUpdateLeaderCompletedState(
        kafkaKey,
        kafkaValue,
        controlMessage,
        pubSubMessage.getPubSubMessageHeaders(),
        partitionConsumptionState);
    if (hybridConfig == HYBRID) {
      assertEquals(partitionConsumptionState.getLeaderCompleteState(), LEADER_COMPLETED);
      assertEquals(partitionConsumptionState.getLastLeaderCompleteStateUpdateInMs(), producerTimestamp);
    } else {
      assertEquals(partitionConsumptionState.getLeaderCompleteState(), LEADER_NOT_COMPLETED);
      assertEquals(partitionConsumptionState.getLastLeaderCompleteStateUpdateInMs(), 0L);
    }
  }

  @DataProvider
  public static Object[][] testProcessTopicSwitchProvider() {
    return new Object[][] { { LEADER }, { DA_VINCI } };
  }

  @Test(dataProvider = "testProcessTopicSwitchProvider")
  public void testProcessTopicSwitch(NodeType nodeType) {
    VenicePartitioner partitioner = getVenicePartitioner();
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setPartitionerClass(partitioner.getClass().getName());
    HybridStoreConfig hybridStoreConfig =
        new HybridStoreConfigImpl(100, 100, 100, DataReplicationPolicy.AGGREGATE, BufferReplayPolicy.REWIND_FROM_EOP);
    MockStoreVersionConfigs storeAndVersionConfigs =
        setupStoreAndVersionMocks(2, partitionerConfig, Optional.of(hybridStoreConfig), false, true, AA_OFF);
    Store mockStore = storeAndVersionConfigs.store;
    Version version = storeAndVersionConfigs.version;
    VeniceStoreVersionConfig storeConfig = storeAndVersionConfigs.storeVersionConfig;
    doReturn(new DeepCopyStorageEngine(mockAbstractStorageEngine)).when(mockStorageEngineRepository)
        .getLocalStorageEngine(topic);
    StoreIngestionTaskFactory ingestionTaskFactory = getIngestionTaskFactoryBuilder(
        new RandomPollStrategy(),
        Utils.setOf(PARTITION_FOO),
        Optional.empty(),
        new HashMap<>(),
        false).setIsDaVinciClient(nodeType == DA_VINCI).build();
    Properties kafkaProps = new Properties();
    kafkaProps.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryLocalKafkaBroker.getKafkaBootstrapServer());

    storeIngestionTaskUnderTest = ingestionTaskFactory.getNewIngestionTask(
        mockStore,
        version,
        kafkaProps,
        isCurrentVersion,
        storeConfig,
        PARTITION_FOO,
        false,
        Optional.empty(),
        null);
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
    doReturn(PARTITION_FOO).when(mockPcs).getPartition();
    doReturn(PARTITION_FOO).when(mockPcs).getPartition();
    storeIngestionTaskUnderTest.processTopicSwitch(controlMessage, PARTITION_FOO, 10, mockPcs);
    verify(mockTopicManagerRemoteKafka, never()).getOffsetByTime(any(), anyLong());
    verify(mockOffsetRecord, never()).setLeaderUpstreamOffset(anyString(), anyLong());
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testUpdateConsumedUpstreamRTOffsetMapDuringRTSubscription(AAConfig aaConfig) {
    String storeName = Utils.getUniqueString("store");
    String versionTopic = Version.composeKafkaTopic(storeName, 1);

    VeniceStoreVersionConfig mockVeniceStoreVersionConfig = mock(VeniceStoreVersionConfig.class);
    doReturn(versionTopic).when(mockVeniceStoreVersionConfig).getStoreVersionName();

    Version mockVersion = mock(Version.class);
    doReturn(1).when(mockVersion).getPartitionCount();
    doReturn(VersionStatus.STARTED).when(mockVersion).getStatus();

    ReadOnlyStoreRepository mockReadOnlyStoreRepository = mock(ReadOnlyStoreRepository.class);
    Store mockStore = mock(Store.class);
    doReturn(storeName).when(mockStore).getName();
    doReturn(mockStore).when(mockReadOnlyStoreRepository).getStoreOrThrow(eq(storeName));
    doReturn(false).when(mockStore).isHybridStoreDiskQuotaEnabled();
    doReturn(mockVersion).when(mockStore).getVersion(1);
    doReturn(aaConfig == AA_ON).when(mockVersion).isActiveActiveReplicationEnabled();

    Properties mockKafkaConsumerProperties = mock(Properties.class);
    doReturn("localhost").when(mockKafkaConsumerProperties).getProperty(eq(KAFKA_BOOTSTRAP_SERVERS));

    VeniceServerConfig mockVeniceServerConfig = mock(VeniceServerConfig.class);
    doReturn(VeniceProperties.empty()).when(mockVeniceServerConfig).getClusterProperties();
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
            Optional.empty(),
            null);

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
        eq(aaConfig == AA_ON ? "localhost" : OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY),
        eq(1000L));

    PubSubTopic rtTopic = pubSubTopicRepository.getTopic("test_rt");
    Supplier<PartitionConsumptionState> mockPcsSupplier = () -> {
      PartitionConsumptionState mock = mock(PartitionConsumptionState.class);
      doReturn(LeaderFollowerStateType.LEADER).when(mock).getLeaderFollowerState();
      doReturn(topicSwitchWrapper).when(mock).getTopicSwitch();
      OffsetRecord mockOR = mock(OffsetRecord.class);
      doReturn(rtTopic).when(mockOR).getLeaderTopic(any());
      System.out.println(mockOR.getLeaderTopic(null));
      doReturn(1000L).when(mockOR).getUpstreamOffset(anyString());
      if (aaConfig == AA_ON) {
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
        eq(aaConfig == AA_ON ? "localhost" : OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY),
        eq(1000L));

    // Test alternative branch of the code
    Supplier<PartitionConsumptionState> mockPcsSupplier2 = () -> {
      PartitionConsumptionState mock = mockPcsSupplier.get();
      if (aaConfig == AA_ON) {
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
    doReturn(VeniceProperties.empty()).when(veniceServerConfig).getClusterProperties();
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
    doReturn(version).when(store).getVersion(eq(1));

    String versionTopicName = "testStore_v1";
    VeniceStoreVersionConfig storeConfig = mock(VeniceStoreVersionConfig.class);
    doReturn(Version.parseStoreFromVersionTopic(versionTopicName)).when(store).getName();
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
            Optional.empty(),
            null));

    OffsetRecord offsetRecord = mock(OffsetRecord.class);
    doReturn(pubSubTopicRepository.getTopic(versionTopicName)).when(offsetRecord).getLeaderTopic(any());
    PartitionConsumptionState partitionConsumptionState =
        new PartitionConsumptionState(Utils.getReplicaId(versionTopicName, 0), 0, offsetRecord, false);

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

  private void produceRecordsUsingSpecificWriter(
      VeniceWriter veniceWriter,
      int startIndex,
      int numberOfMessages,
      Function<Integer, byte[]> randKeyGen) {
    for (int i = startIndex; i < startIndex + numberOfMessages; i++) {
      byte[] value = getNumberedValue(i);
      byte[] randKey = randKeyGen.apply(i);
      try {
        veniceWriter.put(randKey, value, SCHEMA_ID).get();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private Consumer<PubSubTopicPartitionOffset> getObserver(
      List<Long> resubscriptionOffsetForVT,
      List<Long> resubscriptionOffsetForRT) {
    return topicPartitionOffset -> {

      if (topicPartitionOffset == null || topicPartitionOffset.getOffset() == null) {
        LOGGER.info("Received null OffsetRecord!");
      } else {
        PubSubTopicPartition pubSubTopicPartition = topicPartitionOffset.getPubSubTopicPartition();
        Long offset = topicPartitionOffset.getOffset();
        LOGGER.info(
            "TopicPartition: {}, Offset: {}",
            topicPartitionOffset.getPubSubTopicPartition(),
            topicPartitionOffset.getOffset());
        if (pubSubTopicPartition.getPubSubTopic().isVersionTopic() && resubscriptionOffsetForVT.contains(offset)) {
          storeIngestionTaskUnderTest.setVersionRole(PartitionReplicaIngestionContext.VersionRole.BACKUP);
          LOGGER.info(
              "Trigger re-subscription after consuming message for {} at offset {} ",
              pubSubTopicPartition,
              offset);
        } else if (pubSubTopicPartition.getPubSubTopic().isRealTime() && resubscriptionOffsetForRT.contains(offset)) {
          storeIngestionTaskUnderTest.setVersionRole(PartitionReplicaIngestionContext.VersionRole.BACKUP);
          LOGGER.info(
              "Trigger re-subscription after consuming message for {} at offset {}.",
              pubSubTopicPartition,
              offset);
        }
      }
    };
  }

  @Test
  public void testResubscribeAfterRoleChange() throws Exception {
    PubSubTopic realTimeTopic =
        pubSubTopicRepository.getTopic(Version.composeRealTimeTopic(storeNameWithoutVersionInfo));
    // Prepare both local and remote real-time topics
    inMemoryLocalKafkaBroker.createTopic(Version.composeRealTimeTopic(storeNameWithoutVersionInfo), PARTITION_COUNT);
    inMemoryRemoteKafkaBroker.createTopic(Version.composeRealTimeTopic(storeNameWithoutVersionInfo), PARTITION_COUNT);
    mockStorageMetadataService = new InMemoryStorageMetadataService();

    AbstractStoragePartition mockStoragePartition = mock(AbstractStoragePartition.class);
    doReturn(mockStoragePartition).when(mockAbstractStorageEngine).getPartitionOrThrow(anyInt());
    doReturn(new ReentrantReadWriteLock()).when(mockAbstractStorageEngine).getRWLockForPartitionOrThrow(anyInt());

    doReturn(putKeyFooReplicationMetadataWithValueSchemaIdBytesDefault).when(mockStoragePartition)
        .getReplicationMetadata(ByteBuffer.wrap(putKeyFoo));
    doReturn(deleteKeyFooReplicationMetadataWithValueSchemaIdBytes).when(mockStoragePartition)
        .getReplicationMetadata(ByteBuffer.wrap(deleteKeyFoo));

    VeniceWriter vtWriter = getVeniceWriter(topic, new MockInMemoryProducerAdapter(inMemoryLocalKafkaBroker));
    VeniceWriter localRtWriter = getVeniceWriter(
        Version.composeRealTimeTopic(storeNameWithoutVersionInfo),
        new MockInMemoryProducerAdapter(inMemoryLocalKafkaBroker));
    VeniceWriter remoteRtWriter = getVeniceWriter(
        Version.composeRealTimeTopic(storeNameWithoutVersionInfo),
        new MockInMemoryProducerAdapter(inMemoryRemoteKafkaBroker));
    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(
        100,
        100,
        HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
        DataReplicationPolicy.ACTIVE_ACTIVE,
        BufferReplayPolicy.REWIND_FROM_EOP);

    final int batchMessagesNum = 100;
    final List<Long> resubscriptionOffsetForLocalVT = Arrays.asList(30L, 70L);
    final List<Long> resubscriptionOffsetForLocalRT = Arrays.asList(40L);
    final List<Long> resubscriptionOffsetForRemoteRT = Arrays.asList(50L);

    // Prepare resubscription number to be verified after ingestion.
    int totalResubscriptionTriggered = resubscriptionOffsetForLocalVT.size() + resubscriptionOffsetForLocalRT.size()
        + resubscriptionOffsetForRemoteRT.size();
    int totalLocalVtResubscriptionTriggered = resubscriptionOffsetForLocalVT.size();
    int totalLocalRtResubscriptionTriggered =
        resubscriptionOffsetForRemoteRT.size() + resubscriptionOffsetForLocalRT.size();
    int totalRemoteRtResubscriptionTriggered =
        resubscriptionOffsetForRemoteRT.size() + resubscriptionOffsetForLocalRT.size();

    vtWriter.broadcastStartOfPush(new HashMap<>());

    // Produce batchMessagesNum messages to local Venice version topic
    produceRecordsUsingSpecificWriter(localVeniceWriter, 0, batchMessagesNum, this::getNumberedKeyForPartitionBar);

    // Set two observers for both local and remote consumer thread, these observers will trigger resubscription by
    // setting
    // the version role to Backup when the offset reaches the specified value.
    Consumer<PubSubTopicPartitionOffset> localObserver =
        getObserver(resubscriptionOffsetForLocalVT, resubscriptionOffsetForLocalRT);
    Consumer<PubSubTopicPartitionOffset> remoteObserver =
        getObserver(Collections.emptyList(), resubscriptionOffsetForRemoteRT);
    PollStrategy localPollStrategy = new BlockingObserverPollStrategy(new RandomPollStrategy(false), localObserver);
    remotePollStrategy = Optional.of(new BlockingObserverPollStrategy(new RandomPollStrategy(false), remoteObserver));

    TopicManager mockTopicManagerRemoteKafka = mock(TopicManager.class);
    doReturn(mockTopicManagerRemoteKafka).when(mockTopicManagerRepository)
        .getTopicManager(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer());

    runTest(localPollStrategy, Utils.setOf(PARTITION_FOO, PARTITION_BAR), () -> {}, () -> {
      doReturn(vtWriter).when(mockWriterFactory).createVeniceWriter(any(VeniceWriterOptions.class));
      verify(mockLogNotifier, never()).completed(anyString(), anyInt(), anyLong());
      List<CharSequence> kafkaBootstrapServers = new ArrayList<>();
      kafkaBootstrapServers.add(inMemoryLocalKafkaBroker.getKafkaBootstrapServer());
      kafkaBootstrapServers.add(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer());

      // Verify ingestion of Venice version topic batchMessagesNum messages
      verify(mockAbstractStorageEngine, timeout(10000).times(batchMessagesNum))
          .put(eq(PARTITION_BAR), any(), (ByteBuffer) any());

      vtWriter.broadcastEndOfPush(new HashMap<>());
      vtWriter.broadcastTopicSwitch(
          kafkaBootstrapServers,
          Version.composeRealTimeTopic(storeNameWithoutVersionInfo),
          System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(10),
          new HashMap<>());
      storeIngestionTaskUnderTest.promoteToLeader(
          fooTopicPartition,
          new LeaderFollowerPartitionStateModel.LeaderSessionIdChecker(1, new AtomicLong(1)));

      // Both Colo RT ingestion, avoid DCR collision intentionally. Each rt will be produced batchMessagesNum messages.
      produceRecordsUsingSpecificWriter(localRtWriter, 0, batchMessagesNum, this::getNumberedKey);
      produceRecordsUsingSpecificWriter(remoteRtWriter, batchMessagesNum, batchMessagesNum, this::getNumberedKey);

      verify(mockAbstractStorageEngine, timeout(10000).times(batchMessagesNum * 2))
          .putWithReplicationMetadata(eq(PARTITION_FOO), any(), any(), any());
      try {
        verify(storeIngestionTaskUnderTest, times(totalResubscriptionTriggered)).resubscribeForAllPartitions();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      verify(mockLocalKafkaConsumer, atLeast(totalLocalVtResubscriptionTriggered)).unSubscribe(eq(fooTopicPartition));
      verify(mockLocalKafkaConsumer, atLeast(totalLocalVtResubscriptionTriggered)).unSubscribe(eq(barTopicPartition));
      PubSubTopicPartition fooRtTopicPartition = new PubSubTopicPartitionImpl(realTimeTopic, PARTITION_FOO);
      verify(mockLocalKafkaConsumer, atLeast(totalLocalRtResubscriptionTriggered)).unSubscribe(fooRtTopicPartition);
      verify(mockRemoteKafkaConsumer, atLeast(totalRemoteRtResubscriptionTriggered)).unSubscribe(fooRtTopicPartition);
      verify(mockLocalKafkaConsumer, atLeast(totalLocalVtResubscriptionTriggered))
          .subscribe(eq(fooTopicPartition), anyLong());
      verify(mockLocalKafkaConsumer, atLeast(totalLocalRtResubscriptionTriggered))
          .subscribe(eq(fooRtTopicPartition), anyLong());
      verify(mockRemoteKafkaConsumer, atLeast(totalRemoteRtResubscriptionTriggered))
          .subscribe(eq(fooRtTopicPartition), anyLong());
    },
        Optional.of(hybridStoreConfig),
        false,
        Optional.empty(),
        AA_ON,
        Collections.singletonMap(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 3L));
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testWrappedInterruptExceptionDuringGracefulShutdown(AAConfig aaConfig) throws Exception {
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
    }, aaConfig);
    Assert.assertEquals(mockNotifierError.size(), 0);
  }

  /**
   * Verifies that during a graceful shutdown event, the metadata of OffsetRecord will be synced up with partitionConsumptionState
   * in order to avoid re-ingesting everything, regardless of the sync bytes interval
   * Steps:
   * 1. offsetRecord and pcs has the same state at the beginning
   * 2. pcs consumes 2 records and offsetRecords doesn't sync up with pcs due to high sync interval.
   * 3. enforce to gracefully shutdown and validate offsetRecord has been synced up with pcs once.
   */
  @Test(dataProvider = "aaConfigProvider")
  public void testOffsetSyncBeforeGracefulShutDown(AAConfig aaConfig) throws Exception {
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
      if (pcs == null) {
        LOGGER.info(
            "pcs for PARTITION_FOO is null, which is an indication that it was never synced before, so we carry on.");
      } else {
        // If the pcs is non-null, then we perform additional checks to ensure that it was not synced
        Assert.assertEquals(
            pcs.getLatestProcessedLocalVersionTopicOffset(),
            0L,
            "pcs.getLatestProcessedLocalVersionTopicOffset() for PARTITION_FOO is expected to be zero!");
        OffsetRecord offsetRecord = pcs.getOffsetRecord();
        assertNotNull(offsetRecord);
        Assert.assertEquals(offsetRecord.getLocalVersionTopicOffset(), 0L);
      }

      // verify 2 messages were processed
      verify(mockStoreIngestionStats, timeout(TEST_TIMEOUT_MS).times(2)).recordTotalRecordsConsumed();
      pcs = storeIngestionTaskUnderTest.getPartitionConsumptionState(PARTITION_FOO); // We re-fetch in case it was null
      assertNotNull(pcs, "pcs for PARTITION_FOO is null!");
      OffsetRecord offsetRecord = pcs.getOffsetRecord();
      assertNotNull(offsetRecord);
      Assert.assertEquals(pcs.getLatestProcessedLocalVersionTopicOffset(), 2L); // PCS updated
      Assert.assertEquals(offsetRecord.getLocalVersionTopicOffset(), 0L); // offsetRecord hasn't been updated yet

      storeIngestionTaskUnderTest.close();

      // Verify the OffsetRecord is synced up with pcs and get persisted only once during shutdown
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS).times(1)).put(eq(topic), eq(PARTITION_FOO), any());
      Assert.assertEquals(offsetRecord.getLocalVersionTopicOffset(), 2L);

      // Verify that the underlying storage engine sync function is invoked.
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS).times(1)).sync(eq(PARTITION_FOO));
    }, aaConfig, configOverride -> {
      // set very high threshold so offsetRecord isn't be synced during regular consumption
      doReturn(100_000L).when(configOverride).getDatabaseSyncBytesIntervalForTransactionalMode();
    });
    Assert.assertEquals(mockNotifierError.size(), 0);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testProduceToStoreBufferService(AAConfig aaConfig) throws Exception {
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
    }, aaConfig);
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
    }, AA_OFF);

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
    }, this.hybridStoreConfig, false, Optional.empty(), AA_OFF, serverProperties);

    runTest(Collections.singleton(PARTITION_FOO), () -> {
      PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateSupplier.get();
      PubSubTopic wrongTopic = pubSubTopicRepository.getTopic("blah_v1");
      OffsetRecord offsetRecord = mock(OffsetRecord.class);
      when(offsetRecord.getLeaderTopic(any())).thenReturn(wrongTopic);
      when(partitionConsumptionState.getOffsetRecord()).thenReturn(offsetRecord);

      when(partitionConsumptionState.getLeaderFollowerState()).thenReturn(LeaderFollowerStateType.LEADER);
      assertFalse(storeIngestionTaskUnderTest.shouldPersistRecord(pubSubMessage, partitionConsumptionState));
    }, AA_OFF);

    runTest(Collections.singleton(PARTITION_FOO), () -> {
      PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateSupplier.get();
      PubSubTopic wrongTopic = pubSubTopicRepository.getTopic("blah_v1");

      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> pubSubMessage2 =
          new ImmutablePubSubMessage(null, null, new PubSubTopicPartitionImpl(wrongTopic, 1), 0, 0, 0);

      when(partitionConsumptionState.getLeaderFollowerState()).thenReturn(STANDBY);
      assertFalse(storeIngestionTaskUnderTest.shouldPersistRecord(pubSubMessage2, partitionConsumptionState));
    }, AA_OFF);
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
    }, AA_OFF);
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
    }, AA_OFF);
  }

  @Test
  public void testShouldProduceToVersionTopic() throws Exception {
    runTest(Collections.singleton(PARTITION_FOO), () -> {
      PartitionConsumptionState partitionConsumptionState = mock(PartitionConsumptionState.class);
      LeaderFollowerStoreIngestionTask lfsit = (LeaderFollowerStoreIngestionTask) storeIngestionTaskUnderTest;
      when(partitionConsumptionState.getLeaderFollowerState()).thenReturn(STANDBY);
      assertFalse(lfsit.shouldProduceToVersionTopic(partitionConsumptionState));

      when(partitionConsumptionState.getLeaderFollowerState()).thenReturn(LeaderFollowerStateType.LEADER);
      OffsetRecord offsetRecord = mock(OffsetRecord.class);
      when(offsetRecord.getLeaderTopic(any())).thenReturn(pubSubTopic);
      when(partitionConsumptionState.getOffsetRecord()).thenReturn(offsetRecord);
      assertFalse(lfsit.shouldProduceToVersionTopic(partitionConsumptionState));

      when(offsetRecord.getLeaderTopic(any())).thenReturn(pubSubTopicRepository.getTopic("blah_rt"));
      assertTrue(lfsit.shouldProduceToVersionTopic(partitionConsumptionState));

      when(offsetRecord.getLeaderTopic(any())).thenReturn(pubSubTopic);
      when(partitionConsumptionState.consumeRemotely()).thenReturn(true);
      assertTrue(lfsit.shouldProduceToVersionTopic(partitionConsumptionState));
    }, AA_OFF);
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

    doReturn(version).when(store).getVersion(eq(1));

    VeniceStoreVersionConfig storeConfig = mock(VeniceStoreVersionConfig.class);
    doReturn(topic).when(storeConfig).getStoreVersionName();

    StoreIngestionTaskFactory ingestionTaskFactory = getIngestionTaskFactoryBuilder(
        new RandomPollStrategy(),
        Utils.setOf(PARTITION_FOO),
        Optional.empty(),
        Collections.emptyMap(),
        true).build();
    doReturn(Version.parseStoreFromVersionTopic(topic)).when(store).getName();
    storeIngestionTaskUnderTest = ingestionTaskFactory.getNewIngestionTask(
        store,
        version,
        new Properties(),
        isCurrentVersion,
        storeConfig,
        1,
        false,
        Optional.empty(),
        null);
    OffsetRecord offsetRecord = mock(OffsetRecord.class);
    doReturn(pubSubTopic).when(offsetRecord).getLeaderTopic(any());
    PartitionConsumptionState partitionConsumptionState =
        new PartitionConsumptionState(Utils.getReplicaId(pubSubTopic, 0), 0, offsetRecord, false);

    storeIngestionTaskUnderTest.updateLeaderTopicOnFollower(partitionConsumptionState);
    storeIngestionTaskUnderTest.startConsumingAsLeader(partitionConsumptionState);
    String dataRecoverySourceTopic = Version.composeKafkaTopic(storeNameWithoutVersionInfo, 1);
    verify(offsetRecord, times(1)).setLeaderTopic(pubSubTopicRepository.getTopic(dataRecoverySourceTopic));
  }

  @Test
  public void testCheckIngestionTaskActiveness() {
    StoreIngestionTask storeIngestionTask = mock(StoreIngestionTask.class);
    AtomicBoolean result = new AtomicBoolean(true);
    when(storeIngestionTask.getIsRunning()).thenReturn(result);
    doCallRealMethod().when(storeIngestionTask).close();
    doCallRealMethod().when(storeIngestionTask).isRunning();

    // Case 1: idle time pass, close(), then startConsumption(), SIT should be closed
    when(storeIngestionTask.getMaxIdleCounter()).thenReturn(10);
    when(storeIngestionTask.getIdleCounter()).thenReturn(11);
    when(storeIngestionTask.maybeSetIngestionTaskActiveState(anyBoolean())).thenCallRealMethod();
    storeIngestionTask.maybeSetIngestionTaskActiveState(false);
    Assert.assertFalse(storeIngestionTask.maybeSetIngestionTaskActiveState(true));

    // Case 2: idle time pass, startConsumption() then close(), SIT should keep active.
    result.set(true);
    when(storeIngestionTask.getIsRunning()).thenReturn(result);
    storeIngestionTask.maybeSetIngestionTaskActiveState(true);
    when(storeIngestionTask.getIdleCounter()).thenReturn(0);
    Assert.assertTrue(storeIngestionTask.maybeSetIngestionTaskActiveState(false));
  }

  @DataProvider
  public static Object[][] testMaybeSendIngestionHeartbeatProvider() {
    return DataProviderUtils.allPermutationGenerator(
        AAConfig.values(),
        DataProviderUtils.BOOLEAN,
        new NodeType[] { FOLLOWER, NodeType.LEADER },
        HybridConfig.values());
  }

  @Test(dataProvider = "testMaybeSendIngestionHeartbeatProvider")
  public void testMaybeSendIngestionHeartbeat(
      AAConfig aaConfig,
      boolean isRealTimeTopic,
      NodeType nodeType,
      HybridConfig hybridConfig) {
    String storeName = Utils.getUniqueString("store");
    Store mockStore = mock(Store.class);
    doReturn(storeName).when(mockStore).getName();
    String versionTopic = Version.composeKafkaTopic(storeName, 1);
    VeniceStoreVersionConfig mockVeniceStoreVersionConfig = mock(VeniceStoreVersionConfig.class);
    doReturn(versionTopic).when(mockVeniceStoreVersionConfig).getStoreVersionName();
    Version mockVersion = mock(Version.class);
    doReturn(1).when(mockVersion).getPartitionCount();
    doReturn(VersionStatus.STARTED).when(mockVersion).getStatus();
    doReturn(true).when(mockVersion).isUseVersionLevelHybridConfig();
    if (hybridConfig == HYBRID) {
      HybridStoreConfig mockHybridConfig = mock(HybridStoreConfig.class);
      doReturn(mockHybridConfig).when(mockVersion).getHybridStoreConfig();
    } else {
      doReturn(null).when(mockVersion).getHybridStoreConfig();
    }
    Properties mockKafkaConsumerProperties = mock(Properties.class);
    doReturn("localhost").when(mockKafkaConsumerProperties).getProperty(eq(KAFKA_BOOTSTRAP_SERVERS));
    ReadOnlyStoreRepository mockReadOnlyStoreRepository = mock(ReadOnlyStoreRepository.class);
    doReturn(mockStore).when(mockReadOnlyStoreRepository).getStoreOrThrow(eq(storeName));
    doReturn(false).when(mockStore).isHybridStoreDiskQuotaEnabled();
    doReturn(mockVersion).when(mockStore).getVersion(1);
    doReturn(aaConfig == AA_ON).when(mockVersion).isActiveActiveReplicationEnabled();
    VeniceServerConfig mockVeniceServerConfig = mock(VeniceServerConfig.class);
    VeniceProperties mockVeniceProperties = mock(VeniceProperties.class);
    doReturn(true).when(mockVeniceProperties).isEmpty();
    doReturn(mockVeniceProperties).when(mockVeniceServerConfig).getKafkaConsumerConfigsForLocalConsumption();
    doReturn(Object2IntMaps.emptyMap()).when(mockVeniceServerConfig).getKafkaClusterUrlToIdMap();
    doReturn(Int2ObjectMaps.emptyMap()).when(mockVeniceServerConfig).getKafkaClusterIdToUrlMap();
    doReturn(TimeUnit.MINUTES.toMillis(1)).when(mockVeniceServerConfig).getIngestionHeartbeatIntervalMs();
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    OffsetRecord offsetRecord = mock(OffsetRecord.class);
    doReturn(offsetRecord).when(pcs).getOffsetRecord();
    doReturn(nodeType == NodeType.LEADER ? LeaderFollowerStateType.LEADER : STANDBY).when(pcs).getLeaderFollowerState();
    PubSubTopic pubsubTopic = mock(PubSubTopic.class);
    doReturn(pubsubTopic).when(offsetRecord).getLeaderTopic(any());
    doReturn(isRealTimeTopic).when(pubsubTopic).isRealTime();

    VeniceWriter veniceWriter = mock(VeniceWriter.class);
    VeniceWriterFactory veniceWriterFactory = mock(VeniceWriterFactory.class);
    CompletableFuture heartBeatFuture = new CompletableFuture();
    heartBeatFuture.complete(null);
    doReturn(heartBeatFuture).when(veniceWriter).sendHeartbeat(any(), any(), any(), anyBoolean(), any(), anyLong());
    doReturn(veniceWriter).when(veniceWriterFactory).createVeniceWriter(any());

    StoreIngestionTaskFactory ingestionTaskFactory = TestUtils.getStoreIngestionTaskBuilder(storeName)
        .setStorageMetadataService(mockStorageMetadataService)
        .setMetadataRepository(mockReadOnlyStoreRepository)
        .setTopicManagerRepository(mockTopicManagerRepository)
        .setServerConfig(mockVeniceServerConfig)
        .setPubSubTopicRepository(pubSubTopicRepository)
        .setVeniceWriterFactory(veniceWriterFactory)
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
            Optional.empty(),
            null);

    ingestionTask.setPartitionConsumptionState(0, pcs);
    ingestionTask.maybeSendIngestionHeartbeat();
    // Second invocation should be skipped since it shouldn't be time for another heartbeat yet.
    ingestionTask.maybeSendIngestionHeartbeat();
    if (hybridConfig == HYBRID && isRealTimeTopic && nodeType == NodeType.LEADER) {
      verify(veniceWriter, times(1)).sendHeartbeat(any(), any(), any(), anyBoolean(), any(), anyLong());
    } else {
      verify(veniceWriter, never()).sendHeartbeat(any(), any(), any(), anyBoolean(), any(), anyLong());
    }

    /**
     * Leverage the same test to validate {@link StoreIngestionTask#isProducingVersionTopicHealthy()}
     */
    when(mockTopicManager.containsTopic(eq(ingestionTask.getVersionTopic()))).thenReturn(true);
    Assert.assertTrue(ingestionTask.isProducingVersionTopicHealthy());

    when(mockTopicManager.containsTopic(eq(ingestionTask.getVersionTopic()))).thenReturn(false);
    Assert.assertFalse(ingestionTask.isProducingVersionTopicHealthy());
  }

  @Test
  public void testMaybeSendIngestionHeartbeatWithHBSuccessOrFailure() throws InterruptedException {
    String storeName = Utils.getUniqueString("store");
    Store mockStore = mock(Store.class);
    doReturn(storeName).when(mockStore).getName();
    String versionTopic = Version.composeKafkaTopic(storeName, 1);
    VeniceStoreVersionConfig mockVeniceStoreVersionConfig = mock(VeniceStoreVersionConfig.class);
    doReturn(versionTopic).when(mockVeniceStoreVersionConfig).getStoreVersionName();
    Version mockVersion = mock(Version.class);
    doReturn(2).when(mockVersion).getPartitionCount();
    doReturn(VersionStatus.STARTED).when(mockVersion).getStatus();
    doReturn(true).when(mockVersion).isUseVersionLevelHybridConfig();
    HybridStoreConfig mockHybridConfig = mock(HybridStoreConfig.class);
    doReturn(mockHybridConfig).when(mockVersion).getHybridStoreConfig();
    Properties mockKafkaConsumerProperties = mock(Properties.class);
    doReturn("localhost").when(mockKafkaConsumerProperties).getProperty(eq(KAFKA_BOOTSTRAP_SERVERS));
    ReadOnlyStoreRepository mockReadOnlyStoreRepository = mock(ReadOnlyStoreRepository.class);
    doReturn(mockStore).when(mockReadOnlyStoreRepository).getStoreOrThrow(eq(storeName));
    doReturn(false).when(mockStore).isHybridStoreDiskQuotaEnabled();
    doReturn(mockVersion).when(mockStore).getVersion(1);
    doReturn(true).when(mockVersion).isActiveActiveReplicationEnabled();
    VeniceServerConfig mockVeniceServerConfig = mock(VeniceServerConfig.class);
    VeniceProperties mockVeniceProperties = mock(VeniceProperties.class);
    doReturn(true).when(mockVeniceProperties).isEmpty();
    doReturn(mockVeniceProperties).when(mockVeniceServerConfig).getKafkaConsumerConfigsForLocalConsumption();
    doReturn(Object2IntMaps.emptyMap()).when(mockVeniceServerConfig).getKafkaClusterUrlToIdMap();
    doReturn(Int2ObjectMaps.emptyMap()).when(mockVeniceServerConfig).getKafkaClusterIdToUrlMap();
    doReturn(1000L).when(mockVeniceServerConfig).getIngestionHeartbeatIntervalMs();
    OffsetRecord offsetRecord = mock(OffsetRecord.class);
    PartitionConsumptionState pcs0 = mock(PartitionConsumptionState.class);
    doReturn(offsetRecord).when(pcs0).getOffsetRecord();
    doReturn(LeaderFollowerStateType.LEADER).when(pcs0).getLeaderFollowerState();
    PartitionConsumptionState pcs1 = mock(PartitionConsumptionState.class);
    doReturn(offsetRecord).when(pcs1).getOffsetRecord();
    doReturn(LeaderFollowerStateType.LEADER).when(pcs1).getLeaderFollowerState();
    doReturn(1).when(pcs1).getPartition();
    PubSubTopic pubsubTopic = mock(PubSubTopic.class);
    doReturn(pubsubTopic).when(offsetRecord).getLeaderTopic(any());
    doReturn(true).when(pubsubTopic).isRealTime();

    VeniceWriter veniceWriter = mock(VeniceWriter.class);
    VeniceWriterFactory veniceWriterFactory = mock(VeniceWriterFactory.class);
    doReturn(veniceWriter).when(veniceWriterFactory).createVeniceWriter(any());

    StoreIngestionTaskFactory ingestionTaskFactory = TestUtils.getStoreIngestionTaskBuilder(storeName)
        .setStorageMetadataService(mockStorageMetadataService)
        .setMetadataRepository(mockReadOnlyStoreRepository)
        .setTopicManagerRepository(mockTopicManagerRepository)
        .setServerConfig(mockVeniceServerConfig)
        .setPubSubTopicRepository(pubSubTopicRepository)
        .setVeniceWriterFactory(veniceWriterFactory)
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
            Optional.empty(),
            null);

    ingestionTask.setPartitionConsumptionState(0, pcs0);
    ingestionTask.setPartitionConsumptionState(1, pcs1);

    CompletableFuture heartBeatFuture = new CompletableFuture();
    heartBeatFuture.complete(null);
    PubSubTopicPartition pubSubTopicPartition0 = new PubSubTopicPartitionImpl(pubsubTopic, 0);
    PubSubTopicPartition pubSubTopicPartition1 = new PubSubTopicPartitionImpl(pubsubTopic, 1);

    // all succeeded
    doReturn(heartBeatFuture).when(veniceWriter).sendHeartbeat(any(), any(), any(), anyBoolean(), any(), anyLong());
    AtomicReference<Set<String>> failedPartitions = new AtomicReference<>(null);
    failedPartitions.set(ingestionTask.maybeSendIngestionHeartbeat());
    assertEquals(failedPartitions.get().size(), 0);

    // 1 partition throws exception
    doReturn(heartBeatFuture).when(veniceWriter)
        .sendHeartbeat(eq(pubSubTopicPartition0), any(), any(), anyBoolean(), any(), anyLong());
    doAnswer(invocation -> {
      throw new Exception("mock exception");
    }).when(veniceWriter).sendHeartbeat(eq(pubSubTopicPartition1), any(), any(), anyBoolean(), any(), anyLong());
    // wait for SERVER_INGESTION_HEARTBEAT_INTERVAL_MS
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      failedPartitions.set(ingestionTask.maybeSendIngestionHeartbeat());
      assertNotNull(failedPartitions.get());
    });
    // wait for the futures to complete that populates failedPartitions
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      assertEquals(failedPartitions.get().size(), 1);
      assertFalse(failedPartitions.get().contains("0"));
      assertTrue(failedPartitions.get().contains("1"));
    });

    // both partition throws exception
    doAnswer(invocation -> {
      throw new Exception("mock exception");
    }).when(veniceWriter).sendHeartbeat(eq(pubSubTopicPartition0), any(), any(), anyBoolean(), any(), anyLong());
    // wait for SERVER_INGESTION_HEARTBEAT_INTERVAL_MS
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      failedPartitions.set(ingestionTask.maybeSendIngestionHeartbeat());
      assertNotNull(failedPartitions.get());
    });
    // wait for the futures to complete that populates failedPartitions
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      assertEquals(failedPartitions.get().size(), 2);
      assertTrue(failedPartitions.get().contains("0"));
      assertTrue(failedPartitions.get().contains("1"));
    });
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testStoreIngestionRecordTransformer(AAConfig aaConfig) throws Exception {
    KafkaKey kafkaKey = new KafkaKey(MessageType.PUT, putKeyFoo);
    KafkaMessageEnvelope kafkaMessageEnvelope = new KafkaMessageEnvelope();
    kafkaMessageEnvelope.messageType = MessageType.PUT.getValue();
    Put put = new Put();

    put.putValue = ByteBuffer.wrap(putValue);
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

    LeaderProducedRecordContext leaderProducedRecordContext = mock(LeaderProducedRecordContext.class);
    when(leaderProducedRecordContext.getMessageType()).thenReturn(MessageType.PUT);
    when(leaderProducedRecordContext.getValueUnion()).thenReturn(put);
    when(leaderProducedRecordContext.getKeyBytes()).thenReturn(putKeyFoo);

    Schema keySchema = Schema.create(Schema.Type.INT);
    SchemaEntry keySchemaEntry = mock(SchemaEntry.class);
    when(keySchemaEntry.getSchema()).thenReturn(keySchema);
    when(mockSchemaRepo.getKeySchema(storeNameWithoutVersionInfo)).thenReturn(keySchemaEntry);

    Schema valueSchema = Schema.create(Schema.Type.STRING);
    SchemaEntry valueSchemaEntry = mock(SchemaEntry.class);
    when(valueSchemaEntry.getSchema()).thenReturn(valueSchema);
    when(mockSchemaRepo.getValueSchema(eq(storeNameWithoutVersionInfo), anyInt())).thenReturn(valueSchemaEntry);

    runTest(Collections.singleton(PARTITION_FOO), () -> {
      TestUtils.waitForNonDeterministicAssertion(
          5,
          TimeUnit.SECONDS,
          () -> assertTrue(storeIngestionTaskUnderTest.hasAnySubscription()));

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
    }, aaConfig, (storeVersion) -> new TestAvroRecordTransformer(storeVersion));
  }

  // Test to throw type error when performing record transformation with incompatible types
  // @Test(dataProvider = "aaConfigProvider", expectedExceptions = { VeniceException.class, VeniceMessageException.class
  // })
  @Test(dataProvider = "aaConfigProvider")
  public void testStoreIngestionRecordTransformerError(AAConfig aaConfig) throws Exception {
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

    LeaderProducedRecordContext leaderProducedRecordContext = mock(LeaderProducedRecordContext.class);
    when(leaderProducedRecordContext.getMessageType()).thenReturn(MessageType.PUT);
    when(leaderProducedRecordContext.getValueUnion()).thenReturn(put);
    when(leaderProducedRecordContext.getKeyBytes()).thenReturn(keyBytes);

    Schema keySchema = Schema.create(Schema.Type.INT);
    SchemaEntry keySchemaEntry = mock(SchemaEntry.class);
    when(keySchemaEntry.getSchema()).thenReturn(keySchema);
    when(mockSchemaRepo.getKeySchema(storeNameWithoutVersionInfo)).thenReturn(keySchemaEntry);

    Schema valueSchema = Schema.create(Schema.Type.INT);
    SchemaEntry valueSchemaEntry = mock(SchemaEntry.class);
    when(valueSchemaEntry.getSchema()).thenReturn(valueSchema);
    when(mockSchemaRepo.getValueSchema(eq(storeNameWithoutVersionInfo), anyInt())).thenReturn(valueSchemaEntry);

    runTest(Collections.singleton(PARTITION_FOO), () -> {
      TestUtils.waitForNonDeterministicAssertion(
          5,
          TimeUnit.SECONDS,
          () -> assertTrue(storeIngestionTaskUnderTest.hasAnySubscription()));

      try {
        storeIngestionTaskUnderTest.produceToStoreBufferService(
            pubSubMessage,
            leaderProducedRecordContext,
            PARTITION_FOO,
            localKafkaConsumerService.kafkaUrl,
            System.nanoTime(),
            System.currentTimeMillis());
      } catch (Exception e) {
        e.printStackTrace();
      }
      // Verify transformer error was recorded
      verify(mockVersionedStorageIngestionStats, timeout(1000))
          .recordTransformerError(eq(storeNameWithoutVersionInfo), anyInt(), anyDouble(), anyLong());
    }, aaConfig, TestStringRecordTransformer::new);
  }

  public enum RmdState {
    NO_RMD, NON_CHUNKED, CHUNKED
  }

  @DataProvider
  public static Object[][] testAssembledValueSizeProvider() {
    Object[] testSchemaIds =
        { VeniceWriter.VENICE_DEFAULT_VALUE_SCHEMA_ID, AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion(),
            AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion() };
    Object[] rmdState = { RmdState.NO_RMD, RmdState.NON_CHUNKED, RmdState.CHUNKED };
    return DataProviderUtils.allPermutationGenerator(AAConfig.values(), testSchemaIds, rmdState);
  }

  /**
   * Create messages for a chunked record (in multiple chunks and a manifest), and verify that the drainer
   * records the size of the chunked record as described by the size field of the manifest + size of key.
   * Ensure that RMD size is also recorded when included in the manifest.
   * Also, verify that metrics are only emitted when the correct schemaId=-20 is on the manifest message,
   * and not emitted on any other invalid schemaId values.
   */
  @Test(dataProvider = "testAssembledValueSizeProvider")
  public void testAssembledValueSizeSensor(AAConfig aaConfig, int testSchemaId, RmdState rmdState) throws Exception {
    int numChunks = 10;
    long expectedRecordSize = (long) numChunks * ChunkingTestUtils.CHUNK_LENGTH + putKeyFoo.length;
    int rmdSize = 5 * ByteUtils.BYTES_PER_KB; // arbitrary size
    PubSubTopicPartition tp = new PubSubTopicPartitionImpl(pubSubTopic, PARTITION_FOO);
    List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> messages = new ArrayList<>(numChunks + 1); // + manifest
    for (int i = 0; i < numChunks; i++) {
      messages.add(ChunkingTestUtils.createChunkedRecord(putKeyFoo, 1, 1, i, 0, tp));
    }
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> manifestMessage =
        ChunkingTestUtils.createChunkValueManifestRecord(putKeyFoo, messages.get(0), numChunks, tp);
    messages.add(manifestMessage);

    runTest(new RandomPollStrategy(), Collections.singleton(PARTITION_FOO), () -> {}, () -> {
      TestUtils.waitForNonDeterministicAssertion(
          5,
          TimeUnit.SECONDS,
          () -> assertTrue(storeIngestionTaskUnderTest.hasAnySubscription()));

      for (PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> message: messages) {
        try {
          Put put = (Put) message.getValue().getPayloadUnion();
          if (put.schemaId == AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion()) {
            put.schemaId = testSchemaId; // set manifest schemaId to testSchemaId to see if metrics are still recorded
            switch (rmdState) {
              case NON_CHUNKED:
                put.replicationMetadataPayload = ByteBuffer.allocate(rmdSize + ByteUtils.SIZE_OF_INT);
                put.replicationMetadataPayload.position(ByteUtils.SIZE_OF_INT); // for getIntHeaderFromByteBuffer()
                break;
              case CHUNKED:
                put.replicationMetadataPayload = ChunkingTestUtils.createReplicationMetadataPayload(rmdSize);
                break;
              default:
                put.replicationMetadataPayload = VeniceWriter.EMPTY_BYTE_BUFFER;
                break;
            }
          }
          LeaderProducedRecordContext leaderProducedRecordContext = mock(LeaderProducedRecordContext.class);
          when(leaderProducedRecordContext.getMessageType()).thenReturn(MessageType.PUT);
          when(leaderProducedRecordContext.getValueUnion()).thenReturn(put);
          when(leaderProducedRecordContext.getKeyBytes()).thenReturn(putKeyFoo);

          storeIngestionTaskUnderTest.produceToStoreBufferService(
              message,
              leaderProducedRecordContext,
              PARTITION_FOO,
              localKafkaConsumerService.kafkaUrl,
              System.nanoTime(),
              System.currentTimeMillis());
        } catch (InterruptedException e) {
          throw new VeniceException(e);
        }
      }

      // Verify that the assembled record metrics are only recorded if schemaId=-20 which indicates a manifest
      HostLevelIngestionStats stats = storeIngestionTaskUnderTest.hostLevelIngestionStats;
      if (testSchemaId == AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion()) {
        ArgumentCaptor<Long> sizeCaptor = ArgumentCaptor.forClass(long.class);
        verify(stats, timeout(1000).times(1)).recordAssembledRecordSize(sizeCaptor.capture(), anyLong());
        assertEquals(sizeCaptor.getValue().longValue(), expectedRecordSize);
        verify(stats, timeout(1000).times(1)).recordAssembledRecordSizeRatio(anyDouble(), anyLong());

        if (rmdState != RmdState.NO_RMD) {
          verify(stats, timeout(1000).times(1)).recordAssembledRmdSize(sizeCaptor.capture(), anyLong());
          assertEquals(sizeCaptor.getValue().longValue(), rmdSize);
        } else {
          verify(stats, times(0)).recordAssembledRmdSize(anyLong(), anyLong());
        }
      } else {
        verify(stats, times(0)).recordAssembledRmdSize(anyLong(), anyLong());
        verify(stats, times(0)).recordAssembledRecordSize(anyLong(), anyLong());
        verify(stats, times(0)).recordAssembledRecordSizeRatio(anyDouble(), anyLong());
      }
    },
        hybridStoreConfig,
        false,
        true,
        rmdState == RmdState.CHUNKED,
        Optional.empty(),
        aaConfig,
        Collections.emptyMap(),
        storeVersionConfigOverride -> {},
        null);
  }

  @Test
  public void testGetOffsetToOnlineLagThresholdPerPartition() {
    ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
    String storeName = "test-store";
    int partitionCount = 10;

    // Non-hybrid store should throw
    assertThrows(
        VeniceException.class,
        () -> StoreIngestionTask
            .getOffsetToOnlineLagThresholdPerPartition(Optional.empty(), storeName, partitionCount));

    // Negative threshold
    HybridStoreConfigImpl hybridStoreConfig1 = new HybridStoreConfigImpl(
        100L,
        -1L,
        100L,
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_SOP);
    assertEquals(
        StoreIngestionTask
            .getOffsetToOnlineLagThresholdPerPartition(Optional.of(hybridStoreConfig1), storeName, partitionCount),
        -1L);

    // For current version, the partition-level offset lag threshold should be divided by partition count
    HybridStoreConfigImpl hybridStoreConfig2 = new HybridStoreConfigImpl(
        100L,
        100L,
        100L,
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_SOP);
    Store store = mock(Store.class);
    doReturn(10).when(store).getCurrentVersion();
    doReturn(store).when(storeRepository).getStore(storeName);
    assertEquals(
        StoreIngestionTask
            .getOffsetToOnlineLagThresholdPerPartition(Optional.of(hybridStoreConfig2), storeName, partitionCount),
        10L);
  }

  @Test
  public void testCheckAndHandleUpstreamOffsetRewind() {
    String storeName = "test_store";
    int version = 1;
    // No rewind, then nothing would happen
    LeaderFollowerStoreIngestionTask.checkAndHandleUpstreamOffsetRewind(
        mock(PartitionConsumptionState.class),
        mock(PubSubMessage.class),
        11,
        10,
        mock(LeaderFollowerStoreIngestionTask.class));

    // Rewind with batch only store, nothing would happen
    LeaderFollowerStoreIngestionTask mockTask1 = mock(LeaderFollowerStoreIngestionTask.class);
    when(mockTask1.isHybridMode()).thenReturn(false);
    AggVersionedDIVStats mockStats1 = mock(AggVersionedDIVStats.class);
    when(mockTask1.getVersionedDIVStats()).thenReturn(mockStats1);
    when(mockTask1.getStoreName()).thenReturn(storeName);
    when(mockTask1.getVersionNumber()).thenReturn(version);

    LeaderFollowerStoreIngestionTask.checkAndHandleUpstreamOffsetRewind(
        mock(PartitionConsumptionState.class),
        mock(PubSubMessage.class),
        10,
        11,
        mockTask1);
    verify(mockStats1).recordBenignLeaderOffsetRewind(storeName, version);

    // Benign rewind
    final long messageOffset = 10;
    KafkaKey key = new KafkaKey(MessageType.PUT, "test_key".getBytes());
    KafkaMessageEnvelope messsageEnvelope = new KafkaMessageEnvelope();
    LeaderMetadata leaderMetadata = new LeaderMetadata();
    leaderMetadata.upstreamOffset = 10;
    leaderMetadata.hostName = "new_leader";
    messsageEnvelope.leaderMetadataFooter = leaderMetadata;
    ProducerMetadata producerMetadata = new ProducerMetadata();
    producerMetadata.producerGUID = GuidUtils.getGuidFromCharSequence("new_leader_guid");
    messsageEnvelope.producerMetadata = producerMetadata;
    Put put = new Put();
    put.putValue = ByteBuffer.wrap("test_value_suffix".getBytes(), 0, 10); // With trailing suffix.
    put.schemaId = 1;
    messsageEnvelope.payloadUnion = put;
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumedRecord = new ImmutablePubSubMessage<>(
        key,
        messsageEnvelope,
        new PubSubTopicPartitionImpl(
            new TestPubSubTopic("test_store_v1", "test_store", PubSubTopicType.VERSION_TOPIC),
            1),
        messageOffset,
        -1,
        1000);
    AbstractStorageEngine mockStorageEngine2 = mock(AbstractStorageEngine.class);
    ByteBuffer actualValueBuffer = ByteBuffer.allocate(100);
    actualValueBuffer.putInt(1); // schema id
    actualValueBuffer.put("test_value".getBytes());
    actualValueBuffer.flip();
    when(mockStorageEngine2.get(eq(1), eq("test_key".getBytes())))
        .thenReturn(ByteUtils.extractByteArray(actualValueBuffer));
    PartitionConsumptionState mockState2 = mock(PartitionConsumptionState.class);
    when(mockState2.getLeaderHostId()).thenReturn("old_leader");
    when(mockState2.getLeaderGUID()).thenReturn(GuidUtils.getGuidFromCharSequence("old_leader_guid"));
    when(mockState2.isCompletionReported()).thenReturn(false);

    LeaderFollowerStoreIngestionTask mockTask2 = mock(LeaderFollowerStoreIngestionTask.class);
    when(mockTask2.isHybridMode()).thenReturn(true);
    when(mockTask2.getIngestionTaskName()).thenReturn("test_store_v1_task");
    when(mockTask2.getStoreName()).thenReturn(storeName);
    when(mockTask2.getVersionNumber()).thenReturn(version);
    when(mockTask2.getStorageEngine()).thenReturn(mockStorageEngine2);
    AggVersionedDIVStats mockStats2 = mock(AggVersionedDIVStats.class);
    when(mockTask2.getVersionedDIVStats()).thenReturn(mockStats2);
    IngestionNotificationDispatcher ingestionNotificationDispatcher = mock(IngestionNotificationDispatcher.class);
    when(mockTask2.getIngestionNotificationDispatcher()).thenReturn(ingestionNotificationDispatcher);
    LeaderFollowerStoreIngestionTask.checkAndHandleUpstreamOffsetRewind(mockState2, consumedRecord, 10, 11, mockTask2);
    verify(mockStats2).recordBenignLeaderOffsetRewind("test_store", 1);
    verify(mockStats2, never()).recordPotentiallyLossyLeaderOffsetRewind(storeName, version);

    // If current storage engine returns a different value for rewound key, exception should be thrown
    actualValueBuffer = ByteBuffer.allocate(100);
    actualValueBuffer.putInt(1); // schema id
    actualValueBuffer.put("test_value_old".getBytes());
    actualValueBuffer.flip();
    when(mockStorageEngine2.get(eq(1), eq("test_key".getBytes())))
        .thenReturn(ByteUtils.extractByteArray(actualValueBuffer));
    VeniceException exception = Assert.expectThrows(
        VeniceException.class,
        () -> LeaderFollowerStoreIngestionTask
            .checkAndHandleUpstreamOffsetRewind(mockState2, consumedRecord, 10, 11, mockTask2));
    assertTrue(
        exception.getMessage().contains("Failing the job because lossy rewind happens before receiving EndOfPush."));
    // Verify that the VT offset is also in the error message
    assertTrue(exception.getMessage().contains("received message at offset: " + messageOffset));
    verify(mockStats2).recordPotentiallyLossyLeaderOffsetRewind(storeName, version);
  }

  @Test
  public void testMeasureLagWithCallToPubSub() {
    final int PARTITION_UNABLE_TO_GET_END_OFFSET = 0;
    final int EMPTY_PARTITION = 1;
    final int PARTITION_WITH_SOME_MESSAGES_IN_IT = 2;
    final long MESSAGE_COUNT = 10;
    final long INVALID_CURRENT_OFFSET = -2;
    final long CURRENT_OFFSET_NOTHING_CONSUMED = OffsetRecord.LOWEST_OFFSET;
    final long CURRENT_OFFSET_SOME_CONSUMED = 3;
    final String PUB_SUB_SERVER_NAME = "blah";
    doReturn((long) StatsErrorCode.LAG_MEASUREMENT_FAILURE.code).when(mockTopicManager)
        .getLatestOffsetCached(pubSubTopic, PARTITION_UNABLE_TO_GET_END_OFFSET);
    doReturn(0L).when(mockTopicManager).getLatestOffsetCached(pubSubTopic, EMPTY_PARTITION);
    doReturn(MESSAGE_COUNT).when(mockTopicManager)
        .getLatestOffsetCached(pubSubTopic, PARTITION_WITH_SOME_MESSAGES_IN_IT);

    assertEquals(
        StoreIngestionTask.measureLagWithCallToPubSub(
            PUB_SUB_SERVER_NAME,
            pubSubTopic,
            PARTITION_UNABLE_TO_GET_END_OFFSET,
            CURRENT_OFFSET_NOTHING_CONSUMED,
            s -> mockTopicManager),
        Long.MAX_VALUE,
        "If unable to get the end offset, we expect Long.MAX_VALUE (infinite lag).");

    assertEquals(
        StoreIngestionTask.measureLagWithCallToPubSub(
            PUB_SUB_SERVER_NAME,
            pubSubTopic,
            EMPTY_PARTITION,
            INVALID_CURRENT_OFFSET,
            s -> mockTopicManager),
        Long.MAX_VALUE,
        "If the current offset is invalid (less than -1), we expect Long.MAX_VALUE (infinite lag).");

    assertEquals(
        StoreIngestionTask.measureLagWithCallToPubSub(
            PUB_SUB_SERVER_NAME,
            pubSubTopic,
            EMPTY_PARTITION,
            CURRENT_OFFSET_NOTHING_CONSUMED,
            s -> mockTopicManager),
        0,
        "If the partition is empty, we expect no lag.");

    assertEquals(
        StoreIngestionTask.measureLagWithCallToPubSub(
            PUB_SUB_SERVER_NAME,
            pubSubTopic,
            PARTITION_WITH_SOME_MESSAGES_IN_IT,
            CURRENT_OFFSET_NOTHING_CONSUMED,
            s -> mockTopicManager),
        MESSAGE_COUNT,
        "If the partition has messages in it, but we consumed nothing, we expect lag to equal the message count.");

    assertEquals(
        StoreIngestionTask.measureLagWithCallToPubSub(
            PUB_SUB_SERVER_NAME,
            pubSubTopic,
            PARTITION_WITH_SOME_MESSAGES_IN_IT,
            CURRENT_OFFSET_SOME_CONSUMED,
            s -> mockTopicManager),
        MESSAGE_COUNT - 1 - CURRENT_OFFSET_SOME_CONSUMED,
        "If the partition has messages in it, and we consumed some of them, we expect lag to equal the unconsumed message count.");
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
