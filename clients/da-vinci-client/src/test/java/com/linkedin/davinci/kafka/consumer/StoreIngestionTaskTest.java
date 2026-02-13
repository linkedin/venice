package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.IN_TRANSITION_FROM_STANDBY_TO_LEADER;
import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.STANDBY;
import static com.linkedin.davinci.kafka.consumer.StoreIngestionTaskTest.AAConfig.AA_OFF;
import static com.linkedin.davinci.kafka.consumer.StoreIngestionTaskTest.AAConfig.AA_ON;
import static com.linkedin.davinci.kafka.consumer.StoreIngestionTaskTest.HybridConfig.HYBRID;
import static com.linkedin.davinci.kafka.consumer.StoreIngestionTaskTest.NodeType.DA_VINCI;
import static com.linkedin.davinci.kafka.consumer.StoreIngestionTaskTest.NodeType.FOLLOWER;
import static com.linkedin.davinci.kafka.consumer.StoreIngestionTaskTest.NodeType.LEADER;
import static com.linkedin.davinci.kafka.consumer.StoreIngestionTaskTest.SortedInput.SORTED;
import static com.linkedin.davinci.store.StoragePartitionAdjustmentTrigger.PREPARE_FOR_READ;
import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.FREEZE_INGESTION_IF_READY_TO_SERVE_OR_LOCAL_DATA_EXISTS;
import static com.linkedin.venice.ConfigKeys.HYBRID_QUOTA_ENFORCEMENT_ENABLED;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_CLUSTER_MAP_KEY_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_CLUSTER_MAP_KEY_URL;
import static com.linkedin.venice.ConfigKeys.KEY_URN_COMPRESSION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_AA_WC_WORKLOAD_PARALLEL_PROCESSING_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_ENABLE_LIVE_CONFIG_BASED_KAFKA_THROTTLING;
import static com.linkedin.venice.ConfigKeys.SERVER_IDLE_INGESTION_TASK_CLEANUP_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_HEARTBEAT_INTERVAL_MS;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_TASK_MAX_IDLE_COUNT;
import static com.linkedin.venice.ConfigKeys.SERVER_LEADER_COMPLETE_STATE_CHECK_IN_FOLLOWER_VALID_INTERVAL_MS;
import static com.linkedin.venice.ConfigKeys.SERVER_LOCAL_CONSUMER_CONFIG_PREFIX;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_RECORD_LEVEL_METRICS_WHEN_BOOTSTRAPPING_CURRENT_VERSION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_REMOTE_CONSUMER_CONFIG_PREFIX;
import static com.linkedin.venice.ConfigKeys.SERVER_RESET_ERROR_REPLICA_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_RESUBSCRIPTION_CHECK_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_RESUBSCRIPTION_TRIGGERED_BY_VERSION_INGESTION_CONTEXT_CHANGE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_UNSUB_AFTER_BATCHPUSH;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.pubsub.mock.adapter.producer.MockInMemoryProducerAdapter.getPosition;
import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicCompletion;
import static com.linkedin.venice.utils.Time.MS_PER_HOUR;
import static com.linkedin.venice.writer.LeaderCompleteState.LEADER_COMPLETED;
import static com.linkedin.venice.writer.LeaderCompleteState.LEADER_NOT_COMPLETED;
import static com.linkedin.venice.writer.VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER;
import static com.linkedin.venice.writer.VeniceWriter.LEADER_COMPLETE_STATE_HEADERS;
import static com.linkedin.venice.writer.VeniceWriter.generateHeartbeatMessage;
import static com.linkedin.venice.writer.VeniceWriter.getHeartbeatKME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
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

import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerFunctionalInterface;
import com.linkedin.davinci.client.InternalDaVinciRecordTransformerConfig;
import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerDaVinciRecordTransformerImpl;
import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.davinci.helix.StateModelIngestionProgressNotifier;
import com.linkedin.davinci.ingestion.LagType;
import com.linkedin.davinci.ingestion.utils.IngestionTaskReusableObjects;
import com.linkedin.davinci.notifier.LogNotifier;
import com.linkedin.davinci.notifier.PushStatusNotifier;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.stats.AggHostLevelIngestionStats;
import com.linkedin.davinci.stats.AggKafkaConsumerServiceStats;
import com.linkedin.davinci.stats.AggVersionedDIVStats;
import com.linkedin.davinci.stats.AggVersionedDaVinciRecordTransformerStats;
import com.linkedin.davinci.stats.AggVersionedIngestionStats;
import com.linkedin.davinci.stats.HostLevelIngestionStats;
import com.linkedin.davinci.stats.KafkaConsumerServiceStats;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatMonitoringService;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.AbstractStorageIterator;
import com.linkedin.davinci.store.AbstractStoragePartition;
import com.linkedin.davinci.store.DelegatingStorageEngine;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.StorageEngineNoOpStats;
import com.linkedin.davinci.store.StoragePartitionConfig;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.davinci.store.rocksdb.RocksDBServerConfig;
import com.linkedin.davinci.transformer.TestStringRecordTransformer;
import com.linkedin.davinci.validation.DataIntegrityValidator;
import com.linkedin.venice.compression.CompressionStrategy;
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
import com.linkedin.venice.kafka.protocol.state.GlobalRtDivState;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.kafka.validation.checksum.CheckSum;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataRecoveryVersionConfig;
import com.linkedin.venice.meta.DataRecoveryVersionConfigImpl;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.offsets.DeepCopyStorageMetadataService;
import com.linkedin.venice.offsets.InMemoryStorageMetadataService;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubContext;
import com.linkedin.venice.pubsub.PubSubPositionDeserializer;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicImpl;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.PubSubTopicType;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubUnsubscribedTopicPartitionException;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import com.linkedin.venice.pubsub.mock.InMemoryPubSubBroker;
import com.linkedin.venice.pubsub.mock.InMemoryPubSubPosition;
import com.linkedin.venice.pubsub.mock.InMemoryPubSubPositionFactory;
import com.linkedin.venice.pubsub.mock.SimplePartitioner;
import com.linkedin.venice.pubsub.mock.adapter.MockInMemoryPartitionPosition;
import com.linkedin.venice.pubsub.mock.adapter.consumer.MockInMemoryConsumerAdapter;
import com.linkedin.venice.pubsub.mock.adapter.consumer.poll.AbstractPollStrategy;
import com.linkedin.venice.pubsub.mock.adapter.consumer.poll.ArbitraryOrderingPollStrategy;
import com.linkedin.venice.pubsub.mock.adapter.consumer.poll.BlockingObserverPollStrategy;
import com.linkedin.venice.pubsub.mock.adapter.consumer.poll.CompositePollStrategy;
import com.linkedin.venice.pubsub.mock.adapter.consumer.poll.DuplicatingPollStrategy;
import com.linkedin.venice.pubsub.mock.adapter.consumer.poll.FilteringPollStrategy;
import com.linkedin.venice.pubsub.mock.adapter.consumer.poll.PollStrategy;
import com.linkedin.venice.pubsub.mock.adapter.consumer.poll.RandomPollStrategy;
import com.linkedin.venice.pubsub.mock.adapter.producer.MockInMemoryProducerAdapter;
import com.linkedin.venice.pubsub.mock.adapter.producer.MockInMemoryTransformingProducerAdapter;
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
import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.unit.matchers.ExceptionClassMatcher;
import com.linkedin.venice.unit.matchers.NonEmptyStringMatcher;
import com.linkedin.venice.utils.ByteArray;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.ChunkingTestUtils;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.DiskUsage;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.ReferenceCounted;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.utils.pools.LandFillObjectPool;
import com.linkedin.venice.views.MaterializedView;
import com.linkedin.venice.views.VeniceView;
import com.linkedin.venice.writer.DeleteMetadata;
import com.linkedin.venice.writer.LeaderCompleteState;
import com.linkedin.venice.writer.LeaderMetadataWrapper;
import com.linkedin.venice.writer.PutMetadata;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import java.lang.reflect.Field;
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
import java.util.concurrent.CountDownLatch;
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
import org.apache.helix.HelixException;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
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
    return DataProviderUtils.allPermutationGenerator(NodeType.values(), AAConfig.values());
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

  private PubSubContext pubSubContext;
  private InMemoryPubSubBroker inMemoryLocalKafkaBroker;
  private InMemoryPubSubBroker inMemoryRemoteKafkaBroker;
  private MockInMemoryConsumerAdapter inMemoryLocalKafkaConsumer;
  private MockInMemoryConsumerAdapter inMemoryRemoteKafkaConsumer;
  private VeniceWriterFactory mockWriterFactory;
  private VeniceWriter localVeniceWriter;
  private StorageService mockStorageService;
  private VeniceNotifier mockLogNotifier, mockPartitionStatusNotifier, mockLeaderFollowerStateModelNotifier;
  private List<Object[]> mockNotifierProgress;
  private List<Object[]> mockNotifierEOPReceived;
  private List<Object[]> mockNotifierCompleted;
  private List<Object[]> mockNotifierError;
  private StorageMetadataService mockStorageMetadataService;
  private AbstractStorageEngine mockAbstractStorageEngine;
  private DeepCopyStorageEngine mockDeepCopyStorageEngine;
  private IngestionThrottler mockIngestionThrottler;
  private Map<String, EventThrottler> kafkaUrlToRecordsThrottler;
  private KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler;
  private ReadOnlySchemaRepository mockSchemaRepo;
  private ReadOnlyStoreRepository mockMetadataRepo;
  /** N.B.: This mock can be used to verify() calls, but not to return arbitrary things. */
  private PubSubConsumerAdapter mockLocalKafkaConsumer;
  private PubSubConsumerAdapter mockRemoteKafkaConsumer;
  private TopicManager mockTopicManager;
  private TopicManager mockTopicManagerRemote;
  private TopicManagerRepository mockTopicManagerRepository;
  private AggHostLevelIngestionStats mockAggStoreIngestionStats;
  private HostLevelIngestionStats mockStoreIngestionStats;
  private AggVersionedDIVStats mockVersionedDIVStats;
  private AggVersionedIngestionStats mockVersionedStorageIngestionStats;
  private AggVersionedDaVinciRecordTransformerStats mockDaVinciRecordTransformerStats;
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
  private StoreInfo storeInfo;
  private String topic;
  private PubSubTopic pubSubTopic;
  private PubSubTopicPartition fooTopicPartition;
  private PubSubTopicPartition barTopicPartition;
  private PubSubPosition mockedPubSubPosition;

  private ZKHelixAdmin zkHelixAdmin;

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

  protected boolean isAaWCParallelProcessingEnabled() {
    return false;
  }

  @BeforeClass(alwaysRun = true)
  public void suiteSetUp() throws Exception {
    final Sensor mockSensor = mock(Sensor.class);
    doReturn(mockSensor).when(mockMetricRepo).sensor(anyString(), any());
    taskPollingService = Executors.newFixedThreadPool(1, new DaemonThreadFactory("SIT"));
    storeBufferService = new StoreBufferService(
        3,
        10000,
        1000,
        isStoreWriterBufferAfterLeaderLogicEnabled(),
        null,
        mockMetricRepo,
        true);
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
    mockedPubSubPosition = InMemoryPubSubPosition.of(10);

    inMemoryLocalKafkaBroker = new InMemoryPubSubBroker("local");
    inMemoryLocalKafkaBroker.createTopic(topic, PARTITION_COUNT);
    inMemoryRemoteKafkaBroker = new InMemoryPubSubBroker("remote");
    inMemoryRemoteKafkaBroker.createTopic(topic, PARTITION_COUNT);
    zkHelixAdmin = mock(ZKHelixAdmin.class);
    localVeniceWriter = getVeniceWriter(new MockInMemoryProducerAdapter(inMemoryLocalKafkaBroker));

    mockStorageService = mock(StorageService.class);
    storeInfo = mock(StoreInfo.class, RETURNS_DEEP_STUBS);
    when(storeInfo.getName()).thenReturn(storeNameWithoutVersionInfo);
    when(storeInfo.getHybridStoreConfig().getRealTimeTopicName())
        .thenReturn(Utils.composeRealTimeTopic(storeNameWithoutVersionInfo));
    doReturn(new ReferenceCounted<>(mock(DelegatingStorageEngine.class), se -> {})).when(mockStorageService)
        .getRefCountedStorageEngine(anyString());

    mockLogNotifier = mock(LogNotifier.class);
    mockNotifierProgress = new ArrayList<>();
    doAnswer(invocation -> {
      Object[] args = invocation.getArguments();
      mockNotifierProgress.add(args);
      return null;
    }).when(mockLogNotifier).progress(anyString(), anyInt(), any());
    mockNotifierEOPReceived = new ArrayList<>();
    doAnswer(invocation -> {
      Object[] args = invocation.getArguments();
      mockNotifierEOPReceived.add(args);
      return null;
    }).when(mockLogNotifier).endOfPushReceived(anyString(), anyInt(), any());
    mockNotifierCompleted = new ArrayList<>();
    doAnswer(invocation -> {
      Object[] args = invocation.getArguments();
      mockNotifierCompleted.add(args);
      return null;
    }).when(mockLogNotifier).completed(anyString(), anyInt(), any(), anyString());
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

    mockTopicManagerRemote = mock(TopicManager.class);
    doReturn(mockTopicManager).when(mockTopicManagerRepository)
        .getTopicManager(inMemoryLocalKafkaBroker.getPubSubBrokerAddress());
    doReturn(mockTopicManagerRemote).when(mockTopicManagerRepository)
        .getTopicManager(inMemoryRemoteKafkaBroker.getPubSubBrokerAddress());

    doAnswer(inv -> {
      InMemoryPubSubPosition end = inv.getArgument(1);
      return end.getInternalOffset();
    }).when(mockTopicManager).countRecordsUntil(any(), any());

    doAnswer(inv -> {
      InMemoryPubSubPosition a = convertToInMemoryPosition(inv.getArgument(1));
      InMemoryPubSubPosition b = convertToInMemoryPosition(inv.getArgument(2));
      return a.getInternalOffset() - b.getInternalOffset();
    }).when(mockTopicManager).diffPosition(any(), any(), any());

    doAnswer(inv -> {
      InMemoryPubSubPosition end = inv.getArgument(1);
      return end.getInternalOffset();
    }).when(mockTopicManagerRemote).countRecordsUntil(any(), any());

    doAnswer(inv -> {
      InMemoryPubSubPosition a = convertToInMemoryPosition(inv.getArgument(1));
      InMemoryPubSubPosition b = convertToInMemoryPosition(inv.getArgument(2));
      return a.getInternalOffset() - b.getInternalOffset();
    }).when(mockTopicManagerRemote).diffPosition(any(), any(), any());

    PubSubPositionTypeRegistry positionTypeRegistry =
        InMemoryPubSubPositionFactory.getPositionTypeRegistryWithInMemoryPosition();
    PubSubPositionDeserializer pubSubPositionDeserializer = new PubSubPositionDeserializer(positionTypeRegistry);

    pubSubContext = new PubSubContext.Builder().setPubSubTopicRepository(pubSubTopicRepository)
        .setTopicManagerRepository(mockTopicManagerRepository)
        .setPubSubPositionTypeRegistry(positionTypeRegistry)
        .setPubSubPositionDeserializer(pubSubPositionDeserializer)
        .build();

    mockAggStoreIngestionStats = mock(AggHostLevelIngestionStats.class);
    mockStoreIngestionStats = mock(HostLevelIngestionStats.class);
    doReturn(mockStoreIngestionStats).when(mockAggStoreIngestionStats).getStoreStats(anyString());

    mockVersionedDIVStats = mock(AggVersionedDIVStats.class);
    mockVersionedStorageIngestionStats = mock(AggVersionedIngestionStats.class);
    mockDaVinciRecordTransformerStats = mock(AggVersionedDaVinciRecordTransformerStats.class);

    isCurrentVersion = () -> false;
    hybridStoreConfig = Optional.empty();

    databaseChecksumVerificationEnabled = false;
    rocksDBServerConfig = mock(RocksDBServerConfig.class);

    doReturn(true).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);
    doReturn(false).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, NON_EXISTING_SCHEMA_ID);

    doReturn(new RmdSchemaEntry(EXISTING_SCHEMA_ID, REPLICATION_METADATA_VERSION_ID, REPLICATION_METADATA_SCHEMA))
        .when(mockSchemaRepo)
        .getReplicationMetadataSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID, REPLICATION_METADATA_VERSION_ID);

    doReturn(new SchemaEntry(1, STRING_SCHEMA)).when(mockSchemaRepo).getKeySchema(any());

    setDefaultStoreVersionStateSupplier();

    KafkaConsumerServiceStats regionStats = mock(KafkaConsumerServiceStats.class);
    doNothing().when(regionStats).recordByteSizePerPoll(anyDouble());
    doNothing().when(regionStats).recordPollResultNum(anyInt());
    doReturn(regionStats).when(kafkaConsumerServiceStats).getStoreStats(anyString());
  }

  private InMemoryPubSubPosition convertToInMemoryPosition(Object position) {
    if (position instanceof InMemoryPubSubPosition) {
      return (InMemoryPubSubPosition) position;
    } else if (PubSubSymbolicPosition.EARLIEST.equals(position)) {
      return InMemoryPubSubPosition.of(-1L);
    } else {
      return InMemoryPubSubPosition.of(Long.MAX_VALUE);
    }
  }

  private VeniceWriter getVeniceWriter(String topic, PubSubProducerAdapter producerAdapter) {
    VeniceWriterOptions veniceWriterOptions =
        new VeniceWriterOptions.Builder(topic).setKeyPayloadSerializer(new DefaultSerializer())
            .setValuePayloadSerializer(new DefaultSerializer())
            .setWriteComputePayloadSerializer(new DefaultSerializer())
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
        new VeniceWriterOptions.Builder(topic).setKeyPayloadSerializer(new DefaultSerializer())
            .setValuePayloadSerializer(new DefaultSerializer())
            .setWriteComputePayloadSerializer(new DefaultSerializer())
            .setPartitioner(new SimplePartitioner())
            .setTime(SystemTime.INSTANCE)
            .build();
    return new VeniceWriter(veniceWriterOptions, VeniceProperties.empty(), producerAdapter);
  }

  private VeniceWriter getCorruptedVeniceWriter(byte[] valueToCorrupt, InMemoryPubSubBroker kafkaBroker) {
    return getVeniceWriter(
        new CorruptedKafkaProducerAdapterMockInMemory(
            new MockInMemoryProducerAdapter(kafkaBroker),
            topic,
            valueToCorrupt));
  }

  static class CorruptedKafkaProducerAdapterMockInMemory extends MockInMemoryTransformingProducerAdapter {
    public CorruptedKafkaProducerAdapterMockInMemory(
        PubSubProducerAdapter baseProducer,
        String topic,
        byte[] valueToCorrupt) {
      super(baseProducer, (topicName, key, value, partition) -> {
        KafkaMessageEnvelope transformedMessageEnvelope = value;

        if (MessageType.valueOf(transformedMessageEnvelope) == MessageType.PUT) {
          Put put = (Put) transformedMessageEnvelope.payloadUnion;
          if (put.putValue.array() == valueToCorrupt) {
            put.putValue = ByteBuffer.wrap("CORRUPT_VALUE".getBytes());
            transformedMessageEnvelope.payloadUnion = put;
          }
        }

        return new MockInMemoryTransformingProducerAdapter.SendMessageParameters(
            topic,
            key,
            transformedMessageEnvelope,
            partition);
      });
    }
  }

  private void runTest(Set<Integer> partitions, Runnable assertions, AAConfig aaConfig) throws Exception {
    StoreIngestionTaskTestConfig config = new StoreIngestionTaskTestConfig(partitions, assertions, aaConfig);
    runTest(config);
  }

  public static class StoreIngestionTaskTestConfig {
    private final AAConfig aaConfig;
    private final Set<Integer> partitions;
    private final Runnable assertions;
    private PollStrategy pollStrategy = new RandomPollStrategy();
    private Runnable beforeStartingConsumption = () -> {};
    private Optional<HybridStoreConfig> hybridStoreConfig = Optional.empty();
    private boolean incrementalPushEnabled = false;
    private boolean chunkingEnabled = false;
    private boolean rmdChunkingEnabled = false;
    private Optional<DiskUsage> diskUsageForTest = Optional.empty();
    private Map<String, Object> extraServerProperties = new HashMap<>();
    private Consumer<VeniceStoreVersionConfig> storeVersionConfigOverride = storeVersionConfigOverride -> {};
    private DaVinciRecordTransformerConfig recordTransformerConfig = null;
    private OffsetRecord offsetRecord = null;
    private boolean isDaVinci = false;

    public StoreIngestionTaskTestConfig(Set<Integer> partitions, Runnable assertions, AAConfig aaConfig) {
      this.partitions = partitions;
      this.assertions = assertions;
      this.aaConfig = aaConfig;
    }

    public boolean isChunkingEnabled() {
      return chunkingEnabled;
    }

    public StoreIngestionTaskTestConfig setChunkingEnabled(boolean chunkingEnabled) {
      this.chunkingEnabled = chunkingEnabled;
      return this;
    }

    public Set<Integer> getPartitions() {
      return partitions;
    }

    public Runnable getAssertions() {
      return assertions;
    }

    public PollStrategy getPollStrategy() {
      return pollStrategy;
    }

    public StoreIngestionTaskTestConfig setPollStrategy(PollStrategy pollStrategy) {
      this.pollStrategy = pollStrategy;
      return this;
    }

    public Runnable getBeforeStartingConsumption() {
      return beforeStartingConsumption;
    }

    public StoreIngestionTaskTestConfig setBeforeStartingConsumption(Runnable beforeStartingConsumption) {
      this.beforeStartingConsumption = beforeStartingConsumption;
      return this;
    }

    public Optional<HybridStoreConfig> getHybridStoreConfig() {
      return hybridStoreConfig;
    }

    public StoreIngestionTaskTestConfig setHybridStoreConfig(Optional<HybridStoreConfig> hybridStoreConfig) {
      this.hybridStoreConfig = hybridStoreConfig;
      return this;
    }

    public boolean isIncrementalPushEnabled() {
      return incrementalPushEnabled;
    }

    public StoreIngestionTaskTestConfig setIncrementalPushEnabled(boolean incrementalPushEnabled) {
      this.incrementalPushEnabled = incrementalPushEnabled;
      return this;
    }

    public boolean isRmdChunkingEnabled() {
      return rmdChunkingEnabled;
    }

    public StoreIngestionTaskTestConfig setRmdChunkingEnabled(boolean rmdChunkingEnabled) {
      this.rmdChunkingEnabled = rmdChunkingEnabled;
      return this;
    }

    public Optional<DiskUsage> getDiskUsageForTest() {
      return diskUsageForTest;
    }

    public StoreIngestionTaskTestConfig setDiskUsageForTest(Optional<DiskUsage> diskUsageForTest) {
      this.diskUsageForTest = diskUsageForTest;
      return this;
    }

    public AAConfig getAaConfig() {
      return aaConfig;
    }

    public Map<String, Object> getExtraServerProperties() {
      return extraServerProperties;
    }

    public StoreIngestionTaskTestConfig setExtraServerProperties(Map<String, Object> extraServerProperties) {
      this.extraServerProperties = extraServerProperties;
      return this;
    }

    public Consumer<VeniceStoreVersionConfig> getStoreVersionConfigOverride() {
      return storeVersionConfigOverride;
    }

    public StoreIngestionTaskTestConfig setStoreVersionConfigOverride(
        Consumer<VeniceStoreVersionConfig> storeVersionConfigOverride) {
      this.storeVersionConfigOverride = storeVersionConfigOverride;
      return this;
    }

    public DaVinciRecordTransformerConfig getRecordTransformerConfig() {
      return recordTransformerConfig;
    }

    public StoreIngestionTaskTestConfig setRecordTransformerConfig(
        DaVinciRecordTransformerConfig recordTransformerConfig) {
      this.recordTransformerConfig = recordTransformerConfig;
      return this;
    }

    public OffsetRecord getOffsetRecord() {
      return offsetRecord;
    }

    public StoreIngestionTaskTestConfig setOffsetRecord(OffsetRecord offsetRecord) {
      this.offsetRecord = offsetRecord;
      return this;
    }

    public boolean isDaVinci() {
      return isDaVinci;
    }

    public StoreIngestionTaskTestConfig setDaVinci(boolean daVinci) {
      isDaVinci = daVinci;
      return this;
    }
  }

  private void runTest(StoreIngestionTaskTestConfig config) throws Exception {
    runTest(
        config.getPollStrategy(),
        config.getPartitions(),
        config.getBeforeStartingConsumption(),
        config.getAssertions(),
        config.getHybridStoreConfig(),
        config.isIncrementalPushEnabled(),
        config.isChunkingEnabled(),
        config.isRmdChunkingEnabled(),
        config.getDiskUsageForTest(),
        config.getAaConfig(),
        config.getExtraServerProperties(),
        config.getStoreVersionConfigOverride(),
        config.getRecordTransformerConfig(),
        config.getOffsetRecord(),
        config.isDaVinci());
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
   * @param offsetRecord, an override for what offsetRecord should be returned when building PCS in storeIngestionTask
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
      DaVinciRecordTransformerConfig recordTransformerConfig,
      OffsetRecord offsetRecord,
      boolean isDaVinci) throws Exception {

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

    StoreIngestionTaskFactory ingestionTaskFactory = getIngestionTaskFactoryBuilder(
        pollStrategy,
        partitions,
        diskUsageForTest,
        extraServerProperties,
        false,
        recordTransformerConfig,
        offsetRecord,
        this.mockStorageService).setIsDaVinciClient(isDaVinci).build();

    Properties kafkaProps = new Properties();
    kafkaProps.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryLocalKafkaBroker.getPubSubBrokerAddress());

    InternalDaVinciRecordTransformerConfig internalDaVinciRecordTransformerConfig = null;
    if (recordTransformerConfig != null) {
      internalDaVinciRecordTransformerConfig =
          new InternalDaVinciRecordTransformerConfig(recordTransformerConfig, mockDaVinciRecordTransformerStats);
    }

    storeIngestionTaskUnderTest = spy(
        ingestionTaskFactory.getNewIngestionTask(
            this.mockStorageService,
            mockStore,
            version,
            kafkaProps,
            isCurrentVersion,
            storeConfig,
            PARTITION_FOO,
            false,
            Optional.empty(),
            internalDaVinciRecordTransformerConfig,
            Lazy.of(() -> zkHelixAdmin)));

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

    // Enbable URN compression and this won't be enabled unless the server-level config is enabled in DaVinci.
    version.setKeyUrnCompressionEnabled(true);

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
      Boolean isLiveConfigEnabled,
      DaVinciRecordTransformerConfig recordTransformerConfig,
      OffsetRecord optionalOffsetRecord,
      StorageService storageService) {
    DelegatingStorageEngine storageEngineToUse;
    if (recordTransformerConfig != null && recordTransformerConfig.getRecordTransformerFunction() != null) {
      LOGGER.info("Storage engine to use is the mockAbstractStorageEngine");
      storageEngineToUse = new DelegatingStorageEngine(this.mockAbstractStorageEngine);

      AbstractStorageIterator iterator = mock(AbstractStorageIterator.class);
      when(iterator.isValid()).thenReturn(true).thenReturn(false);

      RecordSerializer recordSerializer =
          FastSerializerDeserializerFactory.getFastAvroGenericSerializer(Schema.create(Schema.Type.STRING));
      when(iterator.key()).thenReturn(recordSerializer.serialize("mockKey"));

      byte[] valueByteArray = recordSerializer.serialize("mockValue");
      ByteBuffer valueBytes = ByteBuffer.wrap(valueByteArray);
      ByteBuffer newValueBytes = ByteBuffer.allocate(Integer.BYTES + valueBytes.remaining());
      newValueBytes.putInt(SCHEMA_ID);
      newValueBytes.put(valueBytes);
      newValueBytes.flip();

      when(iterator.value()).thenReturn(newValueBytes.array());
      when(this.mockAbstractStorageEngine.getIterator(anyInt())).thenReturn(iterator);
    } else {
      this.mockDeepCopyStorageEngine = spy(new DeepCopyStorageEngine(this.mockAbstractStorageEngine));
      LOGGER.info("Storage engine to use is the mockDeepCopyStorageEngine");
      storageEngineToUse = new DelegatingStorageEngine(this.mockDeepCopyStorageEngine);
    }
    assertNotNull(
        storageEngineToUse,
        "Either mockDeepCopyStorageEngine or mockAbstractStorageEngine should be non-null!");
    ReferenceCounted<StorageEngine> refCountedSE = new ReferenceCounted<>(storageEngineToUse, se -> {});
    doReturn(refCountedSE).when(storageService).getRefCountedStorageEngine(this.topic);

    inMemoryLocalKafkaConsumer =
        new MockInMemoryConsumerAdapter(inMemoryLocalKafkaBroker, pollStrategy, mockLocalKafkaConsumer);

    inMemoryRemoteKafkaConsumer = remotePollStrategy
        .map(strategy -> new MockInMemoryConsumerAdapter(inMemoryRemoteKafkaBroker, strategy, mockRemoteKafkaConsumer))
        .orElseGet(
            () -> new MockInMemoryConsumerAdapter(inMemoryRemoteKafkaBroker, pollStrategy, mockRemoteKafkaConsumer));

    doAnswer(invocation -> {
      PubSubConsumerAdapterContext consumerContext = invocation.getArgument(0, PubSubConsumerAdapterContext.class);
      if (consumerContext.getPubSubBrokerAddress().equals(inMemoryRemoteKafkaBroker.getPubSubBrokerAddress())) {
        return inMemoryRemoteKafkaConsumer;
      }
      return inMemoryLocalKafkaConsumer;
    }).when(mockFactory).create(any(PubSubConsumerAdapterContext.class));

    mockWriterFactory = mock(VeniceWriterFactory.class);
    doReturn(null).when(mockWriterFactory).createVeniceWriter(any());
    StorageMetadataService offsetManager;
    LOGGER.info(
        "mockStorageMetadataService: {} pubSubContext: {}",
        mockStorageMetadataService.getClass().getName(),
        pubSubContext);
    final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    if (mockStorageMetadataService.getClass() != InMemoryStorageMetadataService.class) {
      for (int partition: partitions) {
        OffsetRecord record = optionalOffsetRecord != null
            ? optionalOffsetRecord
            : new OffsetRecord(partitionStateSerializer, pubSubContext);
        doReturn(record).when(mockStorageMetadataService).getLastOffset(topic, partition, pubSubContext);
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

    PubSubClientsFactory mockPubSubClientsFactory = mock(PubSubClientsFactory.class);
    doReturn(mockFactory).when(mockPubSubClientsFactory).getConsumerAdapterFactory();

    PubSubContext mockPubSubContext = mock(PubSubContext.class);
    doReturn(pubSubDeserializer).when(mockPubSubContext).getPubSubMessageDeserializer();
    doReturn(mockPubSubClientsFactory).when(mockPubSubContext).getPubSubClientsFactory();

    Properties localKafkaProps = new Properties();
    localKafkaProps.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryLocalKafkaBroker.getPubSubBrokerAddress());
    localKafkaConsumerService = getConsumerAssignmentStrategy().constructor.construct(
        ConsumerPoolType.REGULAR_POOL,
        localKafkaProps,
        10,
        1,
        mockIngestionThrottler,
        kafkaClusterBasedRecordThrottler,
        mockMetricRepo,
        inMemoryLocalKafkaBroker.getPubSubBrokerAddress(),
        1000,
        mock(StaleTopicChecker.class),
        isLiveConfigEnabled,
        SystemTime.INSTANCE,
        kafkaConsumerServiceStats,
        false,
        mock(ReadOnlyStoreRepository.class),
        false,
        veniceServerConfig,
        mockPubSubContext,
        null);
    localKafkaConsumerService.start();

    Properties remoteKafkaProps = new Properties();
    remoteKafkaProps.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryRemoteKafkaBroker.getPubSubBrokerAddress());
    remoteKafkaConsumerService = getConsumerAssignmentStrategy().constructor.construct(
        ConsumerPoolType.REGULAR_POOL,
        remoteKafkaProps,
        10,
        1,
        mockIngestionThrottler,
        kafkaClusterBasedRecordThrottler,
        mockMetricRepo,
        inMemoryLocalKafkaBroker.getPubSubBrokerAddress(),
        1000,
        mock(StaleTopicChecker.class),
        isLiveConfigEnabled,
        SystemTime.INSTANCE,
        kafkaConsumerServiceStats,
        false,
        mock(ReadOnlyStoreRepository.class),
        false,
        veniceServerConfig,
        mockPubSubContext,
        null);
    remoteKafkaConsumerService.start();

    prepareAggKafkaConsumerServiceMock();
    return StoreIngestionTaskFactory.builder()
        .setHeartbeatMonitoringService(mock(HeartbeatMonitoringService.class))
        .setVeniceWriterFactory(mockWriterFactory)
        .setStorageMetadataService(offsetManager)
        .setLeaderFollowerNotifiersQueue(leaderFollowerNotifiers)
        .setSchemaRepository(mockSchemaRepo)
        .setMetadataRepository(mockMetadataRepo)
        .setPubSubContext(pubSubContext)
        .setHostLevelIngestionStats(mockAggStoreIngestionStats)
        .setVersionedDIVStats(mockVersionedDIVStats)
        .setVersionedIngestionStats(mockVersionedStorageIngestionStats)
        .setStoreBufferService(storeBufferService)
        .setServerConfig(veniceServerConfig)
        .setDiskUsage(diskUsage)
        .setAggKafkaConsumerService(aggKafkaConsumerService)
        .setCompressorFactory(new StorageEngineBackedCompressorFactory(mockStorageMetadataService))
        .setPartitionStateSerializer(partitionStateSerializer)
        .setReusableObjectsSupplier(IngestionTaskReusableObjects.Strategy.SINGLETON_THREAD_LOCAL.supplier())
        .setAAWCWorkLoadProcessingThreadPool(
            Executors.newFixedThreadPool(2, new DaemonThreadFactory("AA_WC_PARALLEL_PROCESSING")))
        .setAAWCIngestionStorageLookupThreadPool(
            Executors.newFixedThreadPool(1, new DaemonThreadFactory("AA_WC_INGESTION_STORAGE_LOOKUP")));
  }

  abstract KafkaConsumerService.ConsumerAssignmentStrategy getConsumerAssignmentStrategy();

  abstract boolean isStoreWriterBufferAfterLeaderLogicEnabled();

  private void prepareAggKafkaConsumerServiceMock() {
    doAnswer(invocation -> {
      String kafkaUrl = invocation.getArgument(0, String.class);
      StoreIngestionTask storeIngestionTask = invocation.getArgument(1, StoreIngestionTask.class);
      PartitionReplicaIngestionContext partitionReplicaIngestionContext =
          invocation.getArgument(2, PartitionReplicaIngestionContext.class);
      PubSubPosition offset = invocation.getArgument(3, PubSubPosition.class);
      KafkaConsumerService kafkaConsumerService;
      int kafkaClusterId;
      boolean local = kafkaUrl.equals(inMemoryLocalKafkaBroker.getPubSubBrokerAddress());
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
      kafkaConsumerService
          .startConsumptionIntoDataReceiver(partitionReplicaIngestionContext, offset, dataReceiver, false);

      if (local) {
        localConsumedDataReceiver = dataReceiver;
      } else {
        remoteConsumedDataReceiver = dataReceiver;
      }

      return null;
    }).when(aggKafkaConsumerService)
        .subscribeConsumerFor(anyString(), any(), any(), any(PubSubPosition.class), anyBoolean());

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
      if (kafkaUrl.equals(inMemoryLocalKafkaBroker.getPubSubBrokerAddress())) {
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
      PubSubTopic versionTopic = invocation.getArgument(0, PubSubTopic.class);
      PubSubTopicPartition pubSubTopicPartition = invocation.getArgument(1, PubSubTopicPartition.class);
      Long timeoutMs = invocation.getArgument(2, Long.class);
      /**
       * The internal {@link SharedKafkaConsumer} has special logic for unsubscription to avoid some race condition
       * between the fast unsubscribe and re-subscribe.
       * Please check {@link SharedKafkaConsumer#unSubscribe} to find more details.
       *
       * We shouldn't use {@link #mockLocalKafkaConsumer} or {@link #inMemoryRemoteKafkaConsumer} here since
       * they don't have the proper synchronization.
       */
      if (inMemoryLocalKafkaConsumer.hasSubscription(pubSubTopicPartition)) {
        localKafkaConsumerService.unSubscribe(versionTopic, pubSubTopicPartition, timeoutMs.longValue());
      }
      if (inMemoryRemoteKafkaConsumer.hasSubscription(pubSubTopicPartition)) {
        remoteKafkaConsumerService.unSubscribe(versionTopic, pubSubTopicPartition, timeoutMs.longValue());
      }
      return null;
    }).when(aggKafkaConsumerService).unsubscribeConsumerFor(any(), any(), anyLong());

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
        return Collections.singleton(inMemoryLocalKafkaBroker.getPubSubBrokerAddress());
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
    doReturn(StorageEngineNoOpStats.SINGLETON).when(mockAbstractStorageEngine).getStats();
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
    doReturn(svs).when(mockStorageMetadataService).computeStoreVersionState(eq(topic), any());
  }

  void setStoreVersionStateSupplier(boolean sorted) {
    StoreVersionState storeVersionState = new StoreVersionState();
    storeVersionState.sorted = sorted;
    setStoreVersionStateSupplier(storeVersionState);
  }

  private MockInMemoryPartitionPosition getTopicPartitionOffsetPair(PubSubProduceResult produceResult) {
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(
        pubSubTopicRepository.getTopic(produceResult.getTopic()),
        produceResult.getPartition());
    return new MockInMemoryPartitionPosition(pubSubTopicPartition, produceResult.getPubSubPosition());
  }

  private MockInMemoryPartitionPosition getTopicPartitionOffsetPair(
      String topic,
      int partition,
      PubSubPosition offset) {
    return new MockInMemoryPartitionPosition(
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
    PubSubProduceResult putProduceResult;
    PubSubProduceResult deleteProduceResult;
    if (aaConfig.equals(AA_ON)) {
      ByteBuffer dummyRmd = ByteBuffer.wrap(new byte[] { 0xa, 0xb });
      PutMetadata putMetadata = new PutMetadata(1, dummyRmd);
      putProduceResult =
          (PubSubProduceResult) localVeniceWriter
              .put(
                  putKeyFoo,
                  putValue,
                  EXISTING_SCHEMA_ID,
                  null,
                  DEFAULT_LEADER_METADATA_WRAPPER,
                  PUT_KEY_FOO_TIMESTAMP,
                  putMetadata,
                  null,
                  null)
              .get();
      DeleteMetadata delMetadata = new DeleteMetadata(1, 1, dummyRmd);
      deleteProduceResult =
          (PubSubProduceResult) localVeniceWriter
              .delete(
                  deleteKeyFoo,
                  null,
                  DEFAULT_LEADER_METADATA_WRAPPER,
                  DELETE_KEY_FOO_TIMESTAMP,
                  delMetadata,
                  null,
                  null)
              .get();
    } else {
      putProduceResult = (PubSubProduceResult) localVeniceWriter
          .put(putKeyFoo, putValue, EXISTING_SCHEMA_ID, PUT_KEY_FOO_TIMESTAMP, null)
          .get();
      deleteProduceResult =
          (PubSubProduceResult) localVeniceWriter.delete(deleteKeyFoo, DELETE_KEY_FOO_TIMESTAMP, null).get();
    }

    Queue<AbstractPollStrategy> pollStrategies = new LinkedList<>();
    pollStrategies.add(new RandomPollStrategy());

    // We re-deliver the old put out of order, so we can make sure it's ignored.
    Queue<MockInMemoryPartitionPosition> pollDeliveryOrder = new LinkedList<>();
    pollDeliveryOrder.add(getTopicPartitionOffsetPair(putProduceResult));
    pollStrategies.add(new ArbitraryOrderingPollStrategy(pollDeliveryOrder));

    PollStrategy pollStrategy = new CompositePollStrategy(pollStrategies);

    StoreIngestionTaskTestConfig config = new StoreIngestionTaskTestConfig(Utils.setOf(PARTITION_FOO), () -> {
      // Verify it retrieves the offset from the OffSet Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).getLastOffset(topic, PARTITION_FOO, pubSubContext);
      verifyPut(aaConfig, true);
      verifyDelete(aaConfig, true, false);
      // Verify it commits the offset to Offset Manager
      OffsetRecord expectedOffsetRecordForDeleteMessage =
          TestUtils.getOffsetRecord(deleteProduceResult.getPubSubPosition(), Optional.empty(), pubSubContext);
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS))
          .put(topic, PARTITION_FOO, expectedOffsetRecordForDeleteMessage);

      verify(mockVersionedStorageIngestionStats, timeout(TEST_TIMEOUT_MS).atLeast(3))
          .recordConsumedRecordEndToEndProcessingLatency(any(), eq(1), anyDouble(), anyLong());
    }, aaConfig);
    config.setPollStrategy(pollStrategy);
    runTest(config);

    // verify the shared consumer should be detached when the ingestion task is closed.
    verify(aggKafkaConsumerService).unsubscribeAll(pubSubTopic);
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False", timeOut = 60_000)
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
        BufferReplayPolicy.REWIND_FROM_EOP);

    VeniceWriter vtWriter = getVeniceWriter(new MockInMemoryProducerAdapter(inMemoryLocalKafkaBroker));
    vtWriter.broadcastStartOfPush(Collections.emptyMap());
    vtWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID).get();
    vtWriter.broadcastEndOfPush(Collections.emptyMap());
    // Write more messages after EOP
    vtWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID).get();
    vtWriter.put(putKeyFoo2, putValue, EXISTING_SCHEMA_ID).get();

    isCurrentVersion = () -> true;

    // Keeping this because the original test assumed lag was always 0 (reason unclear).
    // This causes the replica to flip RTS status based only on EOP.
    doReturn(InMemoryPubSubPosition.of(0)).when(mockTopicManager)
        .getLatestPositionCached(new PubSubTopicPartitionImpl(pubSubTopic, PARTITION_FOO));

    StoreIngestionTaskTestConfig config = new StoreIngestionTaskTestConfig(Utils.setOf(PARTITION_FOO), () -> {
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS))
          .put(PARTITION_FOO, putKeyFoo2, ByteBuffer.wrap(ValueRecord.create(SCHEMA_ID, putValue).serialize()));
      /**
       * Verify host-level metrics
       *
       * N.B.: the below verification for {@link HostLevelIngestionStats#recordTotalBytesConsumed(long)} is flaky, and
       *       sometimes comes up with 1 fewer invocation than desired (in both branches of the if). The retries mask
       *       the issue as the rate of flakiness is low. But there does seem to be something going on here...
       */
      if (enableRecordLevelMetricForCurrentVersionBootstrapping) {
        verify(mockStoreIngestionStats, timeout(TEST_TIMEOUT_MS).times(3)).recordTotalBytesConsumed(anyLong());
      } else {
        // When record level metric is disabled for current version bootstrapping, the store ingestion stats
        verify(mockStoreIngestionStats, timeout(TEST_TIMEOUT_MS).times(2)).recordTotalBytesConsumed(anyLong());
      }
      verify(mockStoreIngestionStats, timeout(TEST_TIMEOUT_MS).times(3)).recordTotalRecordsConsumed();

    }, AA_OFF);
    config.setHybridStoreConfig(Optional.of(hybridStoreConfig)).setExtraServerProperties(extraProps);
    runTest(config);
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testKeyUrnCompression(boolean enableKeyUrnCompression) throws Exception {
    Map<String, Object> extraProps = new HashMap<>();
    byte[] urnKey =
        FastSerializerDeserializerFactory.getFastAvroGenericSerializer(STRING_SCHEMA).serialize("urn:li:record:123");
    int partition = new SimplePartitioner().getPartitionId(urnKey, PARTITION_COUNT);
    extraProps.put(KEY_URN_COMPRESSION_ENABLED, enableKeyUrnCompression);

    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(
        -1,
        100,
        HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
        BufferReplayPolicy.REWIND_FROM_EOP);

    VeniceWriter vtWriter = getVeniceWriter(new MockInMemoryProducerAdapter(inMemoryLocalKafkaBroker));
    vtWriter.broadcastStartOfPush(Collections.emptyMap());
    vtWriter.put(urnKey, putValue, EXISTING_SCHEMA_ID).get();

    vtWriter.broadcastEndOfPush(Collections.emptyMap());
    // Write more messages after EOP
    vtWriter.put(urnKey, putValue, EXISTING_SCHEMA_ID).get();

    isCurrentVersion = () -> true;

    // Make sure the internal lag measurement won't fail.
    doReturn(InMemoryPubSubPosition.of(0)).when(mockTopicManager)
        .getLatestPositionCachedNonBlocking(eq(new PubSubTopicPartitionImpl(pubSubTopic, partition)));

    StoreIngestionTaskTestConfig config = new StoreIngestionTaskTestConfig(Utils.setOf(partition), () -> {
      if (!enableKeyUrnCompression) {
        verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS).times(2))
            .put(partition, urnKey, ByteBuffer.wrap(ValueRecord.create(SCHEMA_ID, putValue).serialize()));
      } else {
        verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS).atLeastOnce())
            .put(eq(partition), any(), any(ByteBuffer.class));
        verify(mockAbstractStorageEngine, never())
            .put(partition, urnKey, ByteBuffer.wrap(ValueRecord.create(SCHEMA_ID, putValue).serialize()));
      }
    }, AA_OFF);
    config.setHybridStoreConfig(Optional.of(hybridStoreConfig)).setExtraServerProperties(extraProps).setDaVinci(true);
    runTest(config);
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

    Queue<MockInMemoryPartitionPosition> pollDeliveryOrder = new LinkedList<>();
    /**
     * The reason to put offset -1 and offset 0 in the deliveryOrder queue is that the SOS and SOP need to be polled.
     */
    pollDeliveryOrder.add(getTopicPartitionOffsetPair(topic, PARTITION_FOO, InMemoryPubSubPosition.of(-1)));
    pollDeliveryOrder.add(getTopicPartitionOffsetPair(topic, PARTITION_FOO, InMemoryPubSubPosition.of(0)));
    /**
     * The reason to put "putMetadata1" and "putMetadata3" in the deliveryOrder queue is that
     * {@link AbstractPollStrategy#poll(InMemoryPubSubBroker, Map, InMemoryPubSubPosition)} is always trying to return the next message
     * after whats in the queue. One at a time. Here we want to only deliver the unique entries after compaction:
     * putMetadata2 and putMetadata4
     */
    pollDeliveryOrder.add(getTopicPartitionOffsetPair(putMetadata1));
    pollDeliveryOrder.add(getTopicPartitionOffsetPair(putMetadata3));
    PollStrategy pollStrategy = new ArbitraryOrderingPollStrategy(pollDeliveryOrder);

    StoreIngestionTaskTestConfig config = new StoreIngestionTaskTestConfig(Utils.setOf(PARTITION_FOO), () -> {
      // Verify it retrieves the offset from the OffSet Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).getLastOffset(topic, PARTITION_FOO, pubSubContext);

      // Verify StorageEngine#put is invoked only once and with appropriate key & value.
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS))
          .put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(SCHEMA_ID, putValue).serialize()));

      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS))
          .put(PARTITION_FOO, putKeyFoo2, ByteBuffer.wrap(ValueRecord.create(SCHEMA_ID, putValue).serialize()));

      // Verify it commits the offset to Offset Manager
      OffsetRecord expectedOffsetRecordForLastMessage =
          TestUtils.getOffsetRecord(putMetadata4.getPubSubPosition(), Optional.empty(), pubSubContext);
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS))
          .put(topic, PARTITION_FOO, expectedOffsetRecordForLastMessage);
    }, aaConfig);
    config.setPollStrategy(pollStrategy);
    runTest(config);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testVeniceMessagesProcessingWithExistingSchemaId(AAConfig aaConfig) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    InMemoryPubSubPosition fooLastOffset = getPosition(localVeniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID));

    doReturn(true).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      // Verify it retrieves the offset from the OffSet Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).getLastOffset(topic, PARTITION_FOO, pubSubContext);

      // Verify StorageEngine#put is invoked only once and with appropriate key & value.
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS))
          .put(PARTITION_FOO, putKeyFoo, ByteBuffer.wrap(ValueRecord.create(EXISTING_SCHEMA_ID, putValue).serialize()));
      verify(mockSchemaRepo, timeout(TEST_TIMEOUT_MS)).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);

      // Verify it commits the offset to Offset Manager
      long currentOffset = fooLastOffset.getInternalOffset();
      OffsetRecord expected =
          TestUtils.getOffsetRecord(InMemoryPubSubPosition.of(currentOffset), Optional.empty(), pubSubContext);
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
    InMemoryPubSubPosition existingSchemaOffset =
        getPosition(localVeniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID));

    when(mockSchemaRepo.hasValueSchema(storeNameWithoutVersionInfo, NON_EXISTING_SCHEMA_ID))
        .thenReturn(false, false, true);
    doReturn(true).when(mockSchemaRepo).hasValueSchema(storeNameWithoutVersionInfo, EXISTING_SCHEMA_ID);

    StoreIngestionTaskTestConfig config = new StoreIngestionTaskTestConfig(Utils.setOf(PARTITION_FOO), () -> {
      // Verify it retrieves the offset from the OffSet Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).getLastOffset(topic, PARTITION_FOO, pubSubContext);

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

      long currentOffset = existingSchemaOffset.getInternalOffset();
      OffsetRecord expected =
          TestUtils.getOffsetRecord(InMemoryPubSubPosition.of(currentOffset), Optional.empty(), pubSubContext);
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).put(topic, PARTITION_FOO, expected);
    }, aaConfig);
    runTest(config);
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
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).getLastOffset(topic, PARTITION_FOO, pubSubContext);

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
    StoreIngestionTaskTestConfig config =
        new StoreIngestionTaskTestConfig(Utils.setOf(PARTITION_FOO, PARTITION_BAR), () -> {
          // Verify STARTED is NOT reported when offset is 0
          verify(mockLogNotifier, never()).started(topic, PARTITION_BAR);
        }, aaConfig);
    config.setBeforeStartingConsumption(() -> {
      doReturn(TestUtils.getOffsetRecord(InMemoryPubSubPosition.of(STARTING_OFFSET), Optional.empty(), pubSubContext))
          .when(mockStorageMetadataService)
          .getLastOffset(anyString(), anyInt(), eq(pubSubContext));
    });
    runTest(config);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testNotifier(AAConfig aaConfig) throws Exception {
    localVeniceWriter.broadcastStartOfPush(Collections.emptyMap());
    InMemoryPubSubPosition fooLastPosition = getPosition(localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    InMemoryPubSubPosition fooNextPosition = fooLastPosition.getNextPosition();
    InMemoryPubSubPosition barLastPosition = getPosition(localVeniceWriter.put(putKeyBar, putValue, SCHEMA_ID));
    InMemoryPubSubPosition barNextPosition = barLastPosition.getNextPosition();

    doReturn(fooNextPosition).when(mockTopicManager)
        .getLatestPositionCachedNonBlocking(argThat(tp -> tp.getPartitionNumber() == PARTITION_FOO));
    doReturn(barNextPosition).when(mockTopicManager)
        .getLatestPositionCachedNonBlocking(argThat(tp -> tp.getPartitionNumber() == PARTITION_BAR));
    localVeniceWriter.broadcastEndOfPush(Collections.emptyMap());

    runTest(Utils.setOf(PARTITION_FOO, PARTITION_BAR), () -> {
      Utils.sleep(1000);
      /**
       * Considering that the {@link VeniceWriter} will send an {@link ControlMessageType#END_OF_PUSH},
       * we need to add 1 to last data message offset.
       */
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce())
          .completed(topic, PARTITION_FOO, fooNextPosition, "STANDBY");
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce())
          .completed(topic, PARTITION_BAR, barNextPosition, "STANDBY");
      verify(mockPartitionStatusNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce())
          .completed(topic, PARTITION_FOO, fooNextPosition, "STANDBY");
      verify(mockPartitionStatusNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce())
          .completed(topic, PARTITION_BAR, barNextPosition, "STANDBY");
      verify(mockLeaderFollowerStateModelNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce())
          .completed(topic, PARTITION_FOO, fooNextPosition, "STANDBY");
      verify(mockLeaderFollowerStateModelNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce())
          .completed(topic, PARTITION_BAR, barNextPosition, "STANDBY");
      verify(mockStorageMetadataService)
          .put(eq(topic), eq(PARTITION_FOO), eq(getOffsetRecord(fooNextPosition, true, pubSubContext)));
      verify(mockStorageMetadataService)
          .put(eq(topic), eq(PARTITION_BAR), eq(getOffsetRecord(barNextPosition, true, pubSubContext)));
      verify(mockLogNotifier, atLeastOnce()).started(topic, PARTITION_FOO);
      verify(mockLogNotifier, atLeastOnce()).started(topic, PARTITION_BAR);
      verify(mockLogNotifier, atLeastOnce()).endOfPushReceived(topic, PARTITION_FOO, fooLastPosition);
      verify(mockLogNotifier, atLeastOnce()).endOfPushReceived(topic, PARTITION_BAR, barLastPosition);
      verify(mockPartitionStatusNotifier, atLeastOnce()).started(topic, PARTITION_FOO);
      verify(mockPartitionStatusNotifier, atLeastOnce()).started(topic, PARTITION_BAR);
      verify(mockPartitionStatusNotifier, atLeastOnce()).endOfPushReceived(topic, PARTITION_FOO, fooLastPosition);
      verify(mockPartitionStatusNotifier, atLeastOnce()).endOfPushReceived(topic, PARTITION_BAR, barLastPosition);
    }, aaConfig);
  }

  @Test(dataProvider = "aaConfigProvider", timeOut = 60_000)
  public void testReadyToServePartition(AAConfig aaConfig) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    StoreIngestionTaskTestConfig config = new StoreIngestionTaskTestConfig(Utils.setOf(PARTITION_FOO), () -> {
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
    config.setBeforeStartingConsumption(() -> {
      Store mockStore = mock(Store.class);
      doReturn(true).when(mockStore).isHybrid();
      doReturn(new VersionImpl("storeName", 1)).when(mockStore).getVersion(1);
      doReturn(storeNameWithoutVersionInfo).when(mockStore).getName();
      doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeNameWithoutVersionInfo);
    });

    runTest(config);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testReadyToServePartitionValidateIngestionSuccess(AAConfig aaConfig) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());
    Store mockStore = mock(Store.class);
    doReturn(mockStore).when(mockMetadataRepo).getStore(storeNameWithoutVersionInfo);
    doReturn(false).when(mockStore).isHybrid();
    doReturn(storeNameWithoutVersionInfo).when(mockStore).getName();
    mockAbstractStorageEngine.addStoragePartitionIfAbsent(PARTITION_FOO);
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
    mockAbstractStorageEngine.addStoragePartitionIfAbsent(PARTITION_FOO);

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
  public void testReadyToServePartitionValidateIngestionSuccessWithPriorState(AAConfig aaConfig) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());
    Store mockStore = mock(Store.class);
    doReturn(mockStore).when(mockMetadataRepo).getStore(storeNameWithoutVersionInfo);
    doReturn(true).when(mockStore).isHybrid();
    doReturn(storeNameWithoutVersionInfo).when(mockStore).getName();
    mockAbstractStorageEngine.addStoragePartitionIfAbsent(PARTITION_FOO);
    AbstractStoragePartition mockPartition = mock(AbstractStoragePartition.class);
    doReturn(mockPartition).when(mockAbstractStorageEngine).getPartitionOrThrow(PARTITION_FOO);
    doReturn(true).when(mockPartition).validateBatchIngestion();
    new StoragePartitionConfig(topic, PARTITION_FOO);

    StoreIngestionTaskTestConfig testConfig = new StoreIngestionTaskTestConfig(Utils.setOf(PARTITION_FOO), () -> {
      verify(mockAbstractStorageEngine, never()).adjustStoragePartition(eq(PARTITION_FOO), eq(PREPARE_FOR_READ), any());
    }, aaConfig);

    testConfig
        .setHybridStoreConfig(Optional.of(new HybridStoreConfigImpl(1L, 1L, 1L, BufferReplayPolicy.REWIND_FROM_SOP)));
    final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    OffsetRecord offsetRecord = new OffsetRecord(partitionStateSerializer, pubSubContext);
    offsetRecord.endOfPushReceived();
    offsetRecord.setPreviousStatusesEntry("previouslyReadyToServe", "true");
    testConfig.setOffsetRecord(offsetRecord);
    runTest(testConfig);
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
    InMemoryPubSubPosition fooLastOffset = getPosition(localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    InMemoryPubSubPosition barOffsetToSkip = getPosition(localVeniceWriter.put(putKeyBar, putValue, SCHEMA_ID));
    localVeniceWriter.put(putKeyBar, putValue, SCHEMA_ID);
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    PollStrategy pollStrategy = new FilteringPollStrategy(
        new RandomPollStrategy(),
        Utils.setOf(new MockInMemoryPartitionPosition(barTopicPartition, barOffsetToSkip)));

    StoreIngestionTaskTestConfig config =
        new StoreIngestionTaskTestConfig(Utils.setOf(PARTITION_FOO, PARTITION_BAR), () -> {
          verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce())
              .endOfPushReceived(topic, PARTITION_FOO, fooLastOffset);
          verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).error(
              eq(topic),
              eq(PARTITION_BAR),
              argThat(new NonEmptyStringMatcher()),
              argThat(new ExceptionClassMatcher(MissingDataException.class)));

          // After we verified that completed() and error() are called, the rest should be guaranteed to be finished, so
          // no
          // need for timeouts

          verify(mockLogNotifier, atLeastOnce()).started(topic, PARTITION_FOO);
          verify(mockLogNotifier, atLeastOnce()).started(topic, PARTITION_BAR);
        }, aaConfig);
    config.setPollStrategy(pollStrategy);
    runTest(config);
  }

  /**
   * In this test, partition FOO will complete normally, but partition BAR will contain a duplicate record. The
   * {@link VeniceNotifier} should see the completion for both partitions.
   */
  @Test(dataProvider = "aaConfigProvider")
  public void testSkippingOfDuplicateRecord(AAConfig aaConfig) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    InMemoryPubSubPosition fooLastOffset = getPosition(localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    InMemoryPubSubPosition barOffsetToDupe = getPosition(localVeniceWriter.put(putKeyBar, putValue, SCHEMA_ID));
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    PollStrategy pollStrategy = new DuplicatingPollStrategy(
        new RandomPollStrategy(),
        Utils.mutableSetOf(
            new MockInMemoryPartitionPosition(
                new PubSubTopicPartitionImpl(pubSubTopic, PARTITION_BAR),
                barOffsetToDupe)));

    StoreIngestionTaskTestConfig config =
        new StoreIngestionTaskTestConfig(Utils.setOf(PARTITION_FOO, PARTITION_BAR), () -> {
          verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce())
              .endOfPushReceived(topic, PARTITION_FOO, fooLastOffset);
          verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce())
              .endOfPushReceived(topic, PARTITION_BAR, barOffsetToDupe);
          verify(mockLogNotifier, after(TEST_TIMEOUT_MS).never())
              .endOfPushReceived(topic, PARTITION_BAR, barOffsetToDupe.getNextPosition());

          // After we verified that completed() is called, the rest should be guaranteed to be finished, so no need for
          // timeouts

          verify(mockLogNotifier, atLeastOnce()).started(topic, PARTITION_FOO);
          verify(mockLogNotifier, atLeastOnce()).started(topic, PARTITION_BAR);
        }, aaConfig);
    config.setPollStrategy(pollStrategy);
    runTest(config);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testThrottling(AAConfig aaConfig) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);
    localVeniceWriter.delete(deleteKeyFoo, null);

    StoreIngestionTaskTestConfig config = new StoreIngestionTaskTestConfig(Utils.setOf(PARTITION_FOO), () -> {
      // START_OF_SEGMENT, START_OF_PUSH, PUT, DELETE
      verify(mockIngestionThrottler, timeout(TEST_TIMEOUT_MS).times(4))
          .maybeThrottleRecordRate(ConsumerPoolType.REGULAR_POOL, 1);
      verify(mockIngestionThrottler, timeout(TEST_TIMEOUT_MS).times(4)).maybeThrottleBandwidth(anyInt());
    }, aaConfig);
    config.setPollStrategy(new RandomPollStrategy(1));
    runTest(config);
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
        new MockInMemoryTransformingProducerAdapter(
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

              return new MockInMemoryTransformingProducerAdapter.SendMessageParameters(
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
    InMemoryPubSubPosition lastOffsetBeforeEOP =
        getPosition(veniceWriterForDataDuringPush.put(putKeyBar, putValue, SCHEMA_ID));
    veniceWriterForDataDuringPush.close();
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    // We simulate the version swap...
    isCurrentVersion = () -> true;

    // After the end of push, we simulate a nearline writer, which somehow pushes corrupt data.
    getPosition(veniceWriterForDataAfterPush.put(putKeyBar, putValueToCorrupt, SCHEMA_ID));
    InMemoryPubSubPosition lastOffset = getPosition(veniceWriterForDataAfterPush.put(putKeyBar, putValue, SCHEMA_ID));
    veniceWriterForDataAfterPush.close();

    LOGGER.info("lastOffsetBeforeEOP: {}, lastOffset: {}", lastOffsetBeforeEOP, lastOffset);

    try {
      runTest(Utils.setOf(PARTITION_BAR), () -> {

        TestUtils.waitForNonDeterministicCompletion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, () -> {
          for (Object[] args: mockNotifierProgress) {
            InMemoryPubSubPosition position = (InMemoryPubSubPosition) args[2];
            if (args[0].equals(topic) && args[1].equals(PARTITION_BAR)
                && position.getInternalOffset() >= lastOffsetBeforeEOP.getInternalOffset()) {
              return true;
            }
          }
          return false;
        });
        TestUtils.waitForNonDeterministicCompletion(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS, () -> {
          for (Object[] args: mockNotifierCompleted) {
            InMemoryPubSubPosition position = (InMemoryPubSubPosition) args[2];
            if (args[0].equals(topic) && args[1].equals(PARTITION_BAR)
                && position.getInternalOffset() > lastOffsetBeforeEOP.getInternalOffset()) {
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
  @Test(dataProvider = "aaConfigProvider", timeOut = 60_000)
  public void testDIVErrorMessagesNotFailFastAfterEOP(AAConfig aaConfig) throws Exception {
    VeniceWriter veniceWriterCorrupted = getCorruptedVeniceWriter(putValueToCorrupt, inMemoryLocalKafkaBroker);

    // do a batch push
    veniceWriterCorrupted.broadcastStartOfPush(new HashMap<>());
    InMemoryPubSubPosition lastOffsetBeforeEOP = getPosition(veniceWriterCorrupted.put(putKeyBar, putValue, SCHEMA_ID));
    veniceWriterCorrupted.broadcastEndOfPush(new HashMap<>());

    // simulate the version swap.
    isCurrentVersion = () -> true;

    // After the end of push, the venice writer continue puts a corrupt data and end the segment.
    getPosition(veniceWriterCorrupted.put(putKeyFoo, putValueToCorrupt, SCHEMA_ID));
    veniceWriterCorrupted.closePartition(PARTITION_FOO);

    // a missing msg
    InMemoryPubSubPosition fooOffsetToSkip = getPosition(veniceWriterCorrupted.put(putKeyFoo, putValue, SCHEMA_ID));
    // a normal msg
    InMemoryPubSubPosition lastOffset = getPosition(veniceWriterCorrupted.put(putKeyFoo2, putValue, SCHEMA_ID));
    veniceWriterCorrupted.close();

    PollStrategy pollStrategy = new FilteringPollStrategy(
        new RandomPollStrategy(),
        Utils.setOf(
            new MockInMemoryPartitionPosition(
                new PubSubTopicPartitionImpl(pubSubTopic, PARTITION_FOO),
                fooOffsetToSkip)));

    LOGGER.info("lastOffsetBeforeEOP: {}, lastOffset: {}", lastOffsetBeforeEOP, lastOffset);

    StoreIngestionTaskTestConfig config = new StoreIngestionTaskTestConfig(Utils.setOf(PARTITION_FOO), () -> {
      for (Object[] args: mockNotifierError) {
        Assert.assertFalse(
            args[0].equals(topic) && args[1].equals(PARTITION_FOO) && ((String) args[2]).length() > 0
                && args[3] instanceof FatalDataValidationException);
      }
    }, aaConfig);
    config.setPollStrategy(pollStrategy);
    runTest(config);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testIngoreFatalDivForSeekingClient(AAConfig aaConfig) throws Exception {
    VeniceWriter veniceWriterForData = getCorruptedVeniceWriter(putValueToCorrupt, inMemoryLocalKafkaBroker);

    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    InMemoryPubSubPosition fooLastPosition = getPosition(veniceWriterForData.put(putKeyFoo, putValue, SCHEMA_ID));
    getPosition(veniceWriterForData.put(putKeyBar, putValueToCorrupt, SCHEMA_ID));
    veniceWriterForData.close();
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    runTest(Utils.setOf(PARTITION_FOO, PARTITION_BAR), () -> {
      storeIngestionTaskUnderTest.setSkipValidationsForDaVinciClientEnabled();
      ArgumentCaptor<InMemoryPubSubPosition> positionCaptor = ArgumentCaptor.forClass(InMemoryPubSubPosition.class);
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS))
          .completed(eq(topic), eq(PARTITION_FOO), positionCaptor.capture(), eq("STANDBY"));
      InMemoryPubSubPosition completedPosition = positionCaptor.getValue();

      assertTrue(completedPosition.getInternalOffset() >= fooLastPosition.getInternalOffset());

      verify(mockLogNotifier, never()).error(
          eq(topic),
          eq(PARTITION_BAR),
          argThat(new NonEmptyStringMatcher()),
          argThat(new ExceptionClassMatcher(CorruptDataException.class)));
      verify(mockLogNotifier, never()).completed(eq(topic), eq(PARTITION_BAR), any());
    }, aaConfig);
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
    InMemoryPubSubPosition fooLastPosition = getPosition(veniceWriterForData.put(putKeyFoo, putValue, SCHEMA_ID));
    getPosition(veniceWriterForData.put(putKeyBar, putValueToCorrupt, SCHEMA_ID));
    veniceWriterForData.close();
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    runTest(Utils.setOf(PARTITION_FOO, PARTITION_BAR), () -> {
      ArgumentCaptor<InMemoryPubSubPosition> positionCaptor = ArgumentCaptor.forClass(InMemoryPubSubPosition.class);
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS))
          .completed(eq(topic), eq(PARTITION_FOO), positionCaptor.capture(), eq("STANDBY"));
      InMemoryPubSubPosition completedPosition = positionCaptor.getValue();
      assertTrue(completedPosition.getInternalOffset() >= fooLastPosition.getInternalOffset());

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
      verify(mockLogNotifier, never()).completed(eq(topic), eq(PARTITION_BAR), any());
    }, aaConfig);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testSubscribeCompletedPartition(AAConfig aaConfig) throws Exception {
    final InMemoryPubSubPosition p100 = InMemoryPubSubPosition.of(100L);
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());

    StoreIngestionTaskTestConfig config = new StoreIngestionTaskTestConfig(Utils.setOf(PARTITION_FOO), () -> {
      ArgumentCaptor<PubSubPosition> positionCaptor = ArgumentCaptor.forClass(PubSubPosition.class);
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS))
          .completed(eq(topic), eq(PARTITION_FOO), positionCaptor.capture(), eq("STANDBY"));
      assertEquals(positionCaptor.getValue(), p100);
    }, aaConfig);
    config.setBeforeStartingConsumption(
        () -> doReturn(getOffsetRecord(p100, true, pubSubContext)).when(mockStorageMetadataService)
            .getLastOffset(topic, PARTITION_FOO, pubSubContext));

    runTest(config);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testSubscribeCompletedPartitionUnsubscribe(AAConfig aaConfig) throws Exception {
    final InMemoryPubSubPosition p100 = InMemoryPubSubPosition.of(100L);
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    Map<String, Object> extraServerProperties = new HashMap<>();
    extraServerProperties.put(SERVER_UNSUB_AFTER_BATCHPUSH, true);
    extraServerProperties.put(SERVER_INGESTION_TASK_MAX_IDLE_COUNT, 0);

    StoreIngestionTaskTestConfig config = new StoreIngestionTaskTestConfig(Utils.setOf(PARTITION_FOO), () -> {
      ArgumentCaptor<PubSubPosition> positionCaptor = ArgumentCaptor.forClass(PubSubPosition.class);
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS))
          .completed(eq(topic), eq(PARTITION_FOO), positionCaptor.capture(), eq("STANDBY"));
      assertEquals(positionCaptor.getValue(), p100);
      verify(aggKafkaConsumerService, timeout(TEST_TIMEOUT_MS))
          .batchUnsubscribeConsumerFor(pubSubTopic, Collections.singleton(fooTopicPartition));
      verify(aggKafkaConsumerService, never()).unsubscribeConsumerFor(pubSubTopic, barTopicPartition);
      verify(mockLocalKafkaConsumer, timeout(TEST_TIMEOUT_MS))
          .batchUnsubscribe(Collections.singleton(fooTopicPartition));
      verify(mockLocalKafkaConsumer, never()).unSubscribe(barTopicPartition);
      HostLevelIngestionStats stats = storeIngestionTaskUnderTest.hostLevelIngestionStats;
      verify(stats, timeout(TEST_TIMEOUT_MS).atLeast(3)).recordStorageQuotaUsed(anyDouble());
    }, aaConfig);

    config.setBeforeStartingConsumption(() -> {
      Store mockStore = mock(Store.class);
      doReturn(storeNameWithoutVersionInfo).when(mockStore).getName();
      doReturn(1).when(mockStore).getCurrentVersion();
      doReturn(new VersionImpl("storeName", 1, Version.numberBasedDummyPushId(1))).when(mockStore).getVersion(1);
      doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeNameWithoutVersionInfo);
      doReturn(getOffsetRecord(p100, true, pubSubContext)).when(mockStorageMetadataService)
          .getLastOffset(topic, PARTITION_FOO, pubSubContext);
      doAnswer(invocation -> false).when(aggKafkaConsumerService).hasAnyConsumerAssignedForVersionTopic(any());
    }).setHybridStoreConfig(this.hybridStoreConfig).setExtraServerProperties(extraServerProperties);
    runTest(config);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testCompleteCalledWhenUnsubscribeAfterBatchPushDisabled(AAConfig aaConfig) throws Exception {
    final InMemoryPubSubPosition p10 = InMemoryPubSubPosition.of(10L);
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());

    StoreIngestionTaskTestConfig config = new StoreIngestionTaskTestConfig(Utils.setOf(PARTITION_FOO), () -> {
      ArgumentCaptor<PubSubPosition> positionCaptor = ArgumentCaptor.forClass(PubSubPosition.class);
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS))
          .completed(eq(topic), eq(PARTITION_FOO), positionCaptor.capture(), eq("STANDBY"));
      assertEquals(positionCaptor.getValue(), p10);
    }, aaConfig);
    config.setBeforeStartingConsumption(() -> {
      Store mockStore = mock(Store.class);
      storeIngestionTaskUnderTest.unSubscribePartition(fooTopicPartition);
      doReturn(storeNameWithoutVersionInfo).when(mockStore).getName();
      doReturn(1).when(mockStore).getCurrentVersion();
      doReturn(new VersionImpl(storeNameWithoutVersionInfo, 1, Version.numberBasedDummyPushId(1))).when(mockStore)
          .getVersion(1);
      doReturn(mockStore).when(mockMetadataRepo).getStoreOrThrow(storeNameWithoutVersionInfo);
      doReturn(getOffsetRecord(p10, true, pubSubContext)).when(mockStorageMetadataService)
          .getLastOffset(topic, PARTITION_FOO, pubSubContext);
    });

    runTest(config);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testUnsubscribeConsumption(AAConfig aaConfig) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);

    runTest(Utils.setOf(PARTITION_FOO), () -> {
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).started(topic, PARTITION_FOO);
      // Start of push has already been consumed. Stop consumption
      storeIngestionTaskUnderTest.unSubscribePartition(fooTopicPartition);
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).stopped(anyString(), anyInt(), any());
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

      StoreIngestionTaskTestConfig config =
          new StoreIngestionTaskTestConfig(Utils.setOf(PARTITION_FOO, PARTITION_BAR), () -> {
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
      config.setBeforeStartingConsumption(() -> {
        localVeniceWriter.broadcastStartOfPush(new HashMap<>());
        writingThread.start();
      });

      runTest(config);
    } finally {
      TestUtils.shutdownThread(writingThread);
    }
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testKillActionPriority(AAConfig aaConfig) throws Exception {

    StoreIngestionTaskTestConfig config = new StoreIngestionTaskTestConfig(Utils.setOf(PARTITION_FOO), () -> {
      // verify subscribe has not been processed. Because consumption task should process kill action at first
      verify(mockStorageMetadataService, after(TEST_TIMEOUT_MS).never())
          .getLastOffset(topic, PARTITION_FOO, pubSubContext);
      /**
       * Consumers are subscribed lazily; if the store ingestion task is killed before it tries to subscribe to any
       * topics, there is no consumer subscription.
       */
      waitForNonDeterministicCompletion(
          TEST_TIMEOUT_MS,
          TimeUnit.MILLISECONDS,
          () -> !storeIngestionTaskUnderTest.hasAnySubscription());
      // Verify offset has not been processed. Because consumption task should process kill action at first.
      // offSetManager.clearOffset should only be invoked one time during clean up after killing this task.
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).clearOffset(topic, PARTITION_FOO);

      waitForNonDeterministicCompletion(
          TEST_TIMEOUT_MS,
          TimeUnit.MILLISECONDS,
          () -> storeIngestionTaskUnderTest.isRunning() == false);
    }, aaConfig);
    config.setBeforeStartingConsumption(() -> {
      localVeniceWriter.broadcastStartOfPush(new HashMap<>());
      localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID);
      // Add a reset consumer action
      storeIngestionTaskUnderTest.resetPartitionConsumptionOffset(fooTopicPartition);
      // Add a kill consumer action in higher priority than subscribe and reset.
      storeIngestionTaskUnderTest.kill();
    });

    runTest(config);
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
    final Map<Integer, InMemoryPubSubPosition> maxOffsetPerPartition = new HashMap<>();
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
      InMemoryPubSubPosition dataRecordPosition = (InMemoryPubSubPosition) produceResult.getPubSubPosition();

      maxOffsetPerPartition.put(produceResult.getPartition(), dataRecordPosition);
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
          if (topicPartitionOffset == null || topicPartitionOffset.getPubSubPosition() == null) {
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
                topicPartitionOffset.getPubSubPosition());
          }
        });

    StoreIngestionTaskTestConfig config = new StoreIngestionTaskTestConfig(relevantPartitions, () -> {
      Utils.sleep(1000);
      // Verify that all partitions reported success.
      maxOffsetPerPartition.entrySet()
          .stream()
          .filter(entry -> relevantPartitions.contains(entry.getKey()))
          .forEach(entry -> {
            int partition = entry.getKey();
            long offset = entry.getValue().getInternalOffset();
            LOGGER.info("Verifying completed was called for partition {} and offset {} or greater.", partition, offset);

            ArgumentCaptor<InMemoryPubSubPosition> positionCaptor =
                ArgumentCaptor.forClass(InMemoryPubSubPosition.class);
            verify(mockLogNotifier, timeout(LONG_TEST_TIMEOUT).atLeastOnce())
                .completed(eq(topic), eq(partition), positionCaptor.capture(), eq("STANDBY"));
            InMemoryPubSubPosition completedPosition = positionCaptor.getValue();
            assertTrue(completedPosition.getInternalOffset() >= offset);
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
            .subscribe(eq(pubSubTopicPartition), any(PubSubPosition.class));
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
        Assert.assertTrue(pcs.getLatestProcessedRtPositions().isEmpty());
      });
    }, aaConfig);
    config.setPollStrategy(pollStrategy);
    runTest(config);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testKillAfterPartitionIsCompleted(AAConfig aaConfig) throws Exception {
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    InMemoryPubSubPosition fooLastOffset = getPosition(localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
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
    StoreIngestionTaskTestConfig config = new StoreIngestionTaskTestConfig(Utils.setOf(PARTITION_FOO), () -> {
      verify(mockLogNotifier, after(TEST_TIMEOUT_MS).never())
          .progress(eq(topic), eq(PARTITION_FOO), eq(InMemoryPubSubPosition.of(0L)));
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeastOnce()).started(topic, PARTITION_FOO);
      // The current behavior is only to sync offset/report progress after processing a pre-configured amount
      // of messages in bytes, since control message is being counted as 0 bytes (no data persisted in disk),
      // then no progress will be reported during start, but only for processed messages.
      verify(mockLogNotifier, after(TEST_TIMEOUT_MS).never()).progress(any(), anyInt(), any());
    }, aaConfig);
    config.setPollStrategy(new RandomPollStrategy(1));
    runTest(config);
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
      StoreIngestionTaskTestConfig config = new StoreIngestionTaskTestConfig(
          Utils.setOf(PARTITION_FOO),
          () -> verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS).times(2))
              .put(eq(topic), eq(PARTITION_FOO), any()),
          aaConfig);
      config.setHybridStoreConfig(
          Optional.of(
              new HybridStoreConfigImpl(
                  100,
                  100,
                  HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
                  BufferReplayPolicy.REWIND_FROM_EOP)));
      runTest(config);
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
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).getLastOffset(topic, PARTITION_FOO, pubSubContext);

      verifyPut(aaConfig, false);
      verifyDelete(aaConfig, false, true);

      // Verify it commits the offset to Offset Manager after receiving EOP control message
      InMemoryPubSubPosition positionAfterDelete =
          ((InMemoryPubSubPosition) deleteMetadata.getPubSubPosition()).getNextPosition();
      OffsetRecord expectedOffsetRecordForDeleteMessage = getOffsetRecord(positionAfterDelete, true, pubSubContext);
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS))
          .put(topic, PARTITION_FOO, expectedOffsetRecordForDeleteMessage);
      // Deferred write is not going to commit offset for every message, but will commit offset for every control
      // message
      // The following verification is for START_OF_PUSH control message
      InMemoryPubSubPosition currentPosition =
          ((InMemoryPubSubPosition) putMetadata.getPubSubPosition()).getPreviousPosition();
      verify(mockStorageMetadataService, times(1))
          .put(topic, PARTITION_FOO, TestUtils.getOffsetRecord(currentPosition, Optional.empty(), pubSubContext));
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

  @Test(dataProvider = "Boolean-and-Optional-Boolean", dataProviderClass = DataProviderUtils.class)
  public void testVeniceMessagesProcessingWithSortedInputWithBlobMode(boolean blobMode, Boolean sortedFlagInSVS)
      throws Exception {
    if (sortedFlagInSVS != null) {
      setStoreVersionStateSupplier(sortedFlagInSVS);
    } else {
      doReturn(null).when(mockStorageMetadataService).getStoreVersionState(any());
    }
    doAnswer((Answer<StoreVersionState>) invocationOnMock -> {
      String topicName = invocationOnMock.getArgument(0, String.class);
      Function<StoreVersionState, StoreVersionState> mapFunction = invocationOnMock.getArgument(1, Function.class);
      StoreVersionState updatedStoreVersionState =
          mapFunction.apply(mockStorageMetadataService.getStoreVersionState(topicName));
      doReturn(updatedStoreVersionState).when(mockStorageMetadataService).getStoreVersionState(any());
      return updatedStoreVersionState;
    }).when(mockStorageMetadataService).computeStoreVersionState(anyString(), any());

    localVeniceWriter.broadcastStartOfPush(true, new HashMap<>());
    PubSubProduceResult putMetadata =
        (PubSubProduceResult) localVeniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID).get();
    PubSubProduceResult deleteMetadata = (PubSubProduceResult) localVeniceWriter.delete(deleteKeyFoo, null).get();
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    StoreIngestionTaskTestConfig testConfig = new StoreIngestionTaskTestConfig(Utils.setOf(PARTITION_FOO), () -> {
      // Verify it retrieves the offset from the Offset Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).getLastOffset(topic, PARTITION_FOO, pubSubContext);

      // Verify it commits the offset to Offset Manager after receiving EOP control message
      InMemoryPubSubPosition nextToDeletePosition =
          ((InMemoryPubSubPosition) deleteMetadata.getPubSubPosition()).getNextPosition();
      OffsetRecord expectedOffsetRecordForDeleteMessage = getOffsetRecord(nextToDeletePosition, true, pubSubContext);
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS))
          .put(topic, PARTITION_FOO, expectedOffsetRecordForDeleteMessage);
      // Deferred write is not going to commit offset for every message, but will commit offset for every control
      // message
      // The following verification is for START_OF_PUSH control message
      InMemoryPubSubPosition currentPosition =
          ((InMemoryPubSubPosition) putMetadata.getPubSubPosition()).getPreviousPosition();
      verify(mockStorageMetadataService, times(1))
          .put(topic, PARTITION_FOO, TestUtils.getOffsetRecord(currentPosition, Optional.empty(), pubSubContext));
      // Check database mode switches from deferred-write to transactional after EOP control message
      StoragePartitionConfig deferredWritePartitionConfig = new StoragePartitionConfig(topic, PARTITION_FOO);
      boolean deferredWrite;
      if (!blobMode) {
        deferredWrite = sortedFlagInSVS != null ? sortedFlagInSVS : true;
      } else {
        deferredWrite = sortedFlagInSVS != null ? sortedFlagInSVS : false;
      }
      deferredWritePartitionConfig.setDeferredWrite(deferredWrite);
      verify(mockAbstractStorageEngine, times(1))
          .beginBatchWrite(eq(deferredWritePartitionConfig), any(), eq(Optional.empty()));
      StoragePartitionConfig transactionalPartitionConfig = new StoragePartitionConfig(topic, PARTITION_FOO);
      verify(mockAbstractStorageEngine, times(1)).endBatchWrite(transactionalPartitionConfig);
    }, null);
    testConfig.setHybridStoreConfig(
        Optional.of(
            new HybridStoreConfigImpl(
                10,
                20,
                HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
                BufferReplayPolicy.REWIND_FROM_EOP)));
    testConfig.setExtraServerProperties(
        Collections.singletonMap(RocksDBServerConfig.ROCKSDB_BLOB_FILES_ENABLED, Boolean.toString(blobMode)));
    runTest(testConfig);
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
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).getLastOffset(topic, PARTITION_FOO, pubSubContext);

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
  public void testRecordTransformerDatabaseChecksumDefaultSettings(AAConfig aaConfig) throws Exception {
    databaseChecksumVerificationEnabled = true;
    doReturn(false).when(rocksDBServerConfig).isRocksDBPlainTableFormatEnabled();
    setStoreVersionStateSupplier(true);

    StoreIngestionTaskTestConfig config = new StoreIngestionTaskTestConfig(Collections.singleton(PARTITION_FOO), () -> {
      localVeniceWriter.broadcastStartOfPush(true, new HashMap<>());
      try {
        localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID).get();
      } catch (Exception e) {
        throw new VeniceException(e);
      }

      // Verify it retrieves the offset from the Offset Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).getLastOffset(topic, PARTITION_FOO, pubSubContext);

      StoragePartitionConfig deferredWritePartitionConfig = new StoragePartitionConfig(topic, PARTITION_FOO);
      deferredWritePartitionConfig.setDeferredWrite(true);

      waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        // When DVRT is enabled with default settings, checksum shouldn't be calculated
        verify(mockAbstractStorageEngine)
            .beginBatchWrite(eq(deferredWritePartitionConfig), any(), eq(Optional.empty()));
      });
    }, aaConfig);

    config.setRecordTransformerConfig(buildRecordTransformerConfig(true));
    runTest(config);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testRecordTransformerDatabaseChecksumRecordTransformationDisabled(AAConfig aaConfig) throws Exception {
    databaseChecksumVerificationEnabled = true;
    doReturn(false).when(rocksDBServerConfig).isRocksDBPlainTableFormatEnabled();
    setStoreVersionStateSupplier(true);

    StoreIngestionTaskTestConfig config = new StoreIngestionTaskTestConfig(Collections.singleton(PARTITION_FOO), () -> {
      localVeniceWriter.broadcastStartOfPush(true, new HashMap<>());
      try {
        localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID).get();
      } catch (Exception e) {
        throw new VeniceException(e);
      }

      CheckSum checksum = CheckSum.getInstance(CheckSumType.MD5);
      checksum.update(putKeyFoo);
      checksum.update(SCHEMA_ID);
      checksum.update(putValue);
      ArgumentCaptor<Optional<Supplier<byte[]>>> checksumCaptor = ArgumentCaptor.forClass(Optional.class);

      // Verify it retrieves the offset from the Offset Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).getLastOffset(topic, PARTITION_FOO, pubSubContext);

      StoragePartitionConfig deferredWritePartitionConfig = new StoragePartitionConfig(topic, PARTITION_FOO);
      deferredWritePartitionConfig.setDeferredWrite(true);

      waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        // When DVRT is enabled with record transformation disabled, checksum should be calculated
        verify(mockAbstractStorageEngine)
            .beginBatchWrite(eq(deferredWritePartitionConfig), any(), checksumCaptor.capture());
        Optional<Supplier<byte[]>> checksumSupplier = checksumCaptor.getValue();
        Assert.assertTrue(checksumSupplier.isPresent());
        Assert.assertTrue(Arrays.equals(checksumSupplier.get().get(), checksum.getCheckSum()));
      });
    }, aaConfig);

    config.setRecordTransformerConfig(buildRecordTransformerConfig(false));
    runTest(config);
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
            BufferReplayPolicy.REWIND_FROM_EOP));
    long[] messageCountPerPartition = new long[PARTITION_COUNT];

    when(mockTopicManager.getLatestPositionCachedNonBlocking(any(PubSubTopicPartition.class)))
        .thenAnswer(invocation -> {
          PubSubTopicPartition topicPartition = invocation.getArgument(0);
          int partitionNumber = topicPartition.getPartitionNumber();
          return InMemoryPubSubPosition.of(messageCountPerPartition[partitionNumber]);
        });

    StoreIngestionTaskTestConfig config = new StoreIngestionTaskTestConfig(ALL_PARTITIONS, () -> {
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS).atLeast(ALL_PARTITIONS.size())).started(eq(topic), anyInt());
      verify(mockLogNotifier, never()).completed(anyString(), anyInt(), any());

      localVeniceWriter.broadcastTopicSwitch(
          Collections.singletonList(inMemoryLocalKafkaBroker.getPubSubBrokerAddress()),
          Utils.getRealTimeTopicName(storeInfo),
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
      verify(mockLogNotifier, never()).completed(anyString(), anyInt(), any());

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
          .completed(anyString(), anyInt(), any(), anyString());

    }, aaConfig);
    config.setBeforeStartingConsumption(() -> {
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

    });

    runTest(config);
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

    StoreIngestionTaskTestConfig config = new StoreIngestionTaskTestConfig(
        Utils.setOf(PARTITION_FOO),
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
        aaConfig);
    config.setDiskUsageForTest(Optional.of(diskFullUsage));
    runTest(config);
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testIncrementalPush(AAConfig aaConfig) throws Exception {
    setStoreVersionStateSupplier(true);
    localVeniceWriter.broadcastStartOfPush(true, new HashMap<>());
    InMemoryPubSubPosition fooOffset = getPosition(localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    String version = String.valueOf(System.currentTimeMillis());
    localVeniceWriter.broadcastStartOfIncrementalPush(version, new HashMap<>());
    InMemoryPubSubPosition fooNewOffset = getPosition(localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    localVeniceWriter.broadcastEndOfIncrementalPush(version, new HashMap<>());
    // Records order are: StartOfSeg, StartOfPush, data, EndOfPush, EndOfSeg, StartOfSeg, StartOfIncrementalPush
    // data, EndOfIncrementalPush

    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(
        100,
        100,
        HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
        BufferReplayPolicy.REWIND_FROM_EOP);

    StoreIngestionTaskTestConfig config = new StoreIngestionTaskTestConfig(Utils.setOf(PARTITION_FOO), () -> {
      waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        // sync the offset when receiving EndOfPush
        verify(mockStorageMetadataService)
            .put(eq(topic), eq(PARTITION_FOO), eq(getOffsetRecord(fooOffset.getNextPosition(), true, pubSubContext)));
        // sync the offset when receiving StartOfIncrementalPush and EndOfIncrementalPush
        verify(mockStorageMetadataService).put(
            eq(topic),
            eq(PARTITION_FOO),
            eq(getOffsetRecord(fooNewOffset.getPreviousPosition(), true, pubSubContext)));
        verify(mockStorageMetadataService).put(
            eq(topic),
            eq(PARTITION_FOO),
            eq(getOffsetRecord(fooNewOffset.getNextPosition(), true, pubSubContext)));

        verify(mockLogNotifier, atLeastOnce()).started(topic, PARTITION_FOO);

        // since notifier reporting happens before offset update, it actually reports previous offsets
        verify(mockLogNotifier, atLeastOnce()).endOfPushReceived(topic, PARTITION_FOO, fooOffset);
        verify(mockLogNotifier, atLeastOnce())
            .endOfIncrementalPushReceived(topic, PARTITION_FOO, fooNewOffset, version);
      });
    }, aaConfig);
    config.setHybridStoreConfig(Optional.of(hybridStoreConfig)).setIncrementalPushEnabled(true);
    runTest(config);
  }

  @Test
  public void testSchemaCacheWarming() {
    VenicePartitioner partitioner = getVenicePartitioner();
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setPartitionerClass(partitioner.getClass().getName());
    MockStoreVersionConfigs storeAndVersionConfigs =
        setupStoreAndVersionMocks(2, partitionerConfig, Optional.empty(), false, true, AA_OFF);
    Store mockStore = storeAndVersionConfigs.store;
    Version version = storeAndVersionConfigs.version;
    VeniceStoreVersionConfig storeConfig = storeAndVersionConfigs.storeVersionConfig;

    StoreIngestionTaskFactory ingestionTaskFactory = getIngestionTaskFactoryBuilder(
        new RandomPollStrategy(),
        Utils.setOf(PARTITION_FOO),
        Optional.empty(),
        new HashMap<>(),
        false,
        null,
        null,
        this.mockStorageService).build();
    Properties kafkaProps = new Properties();
    kafkaProps.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryLocalKafkaBroker.getPubSubBrokerAddress());

    storeIngestionTaskUnderTest = spy(
        ingestionTaskFactory.getNewIngestionTask(
            this.mockStorageService,
            mockStore,
            version,
            kafkaProps,
            isCurrentVersion,
            storeConfig,
            PARTITION_FOO,
            false,
            Optional.empty(),
            null,
            null));

    Schema schema1 = Schema.parse(
        "{" + "\"fields\": ["
            + "   {\"default\": \"\", \"doc\": \"test field\", \"name\": \"testField1\", \"type\": \"string\"},"
            + "   {\"default\": 0, \"doc\": \"test field two\", \"name\": \"testField2\", \"type\": \"float\"}"
            + "   ]," + " \"name\": \"testObject\", \"type\": \"record\"" + "}");
    Schema schema2 = Schema.parse(
        "{" + "\"fields\": ["
            + "   {\"default\": \"\", \"doc\": \"test field\", \"name\": \"testField1\", \"type\": \"string\"},"
            + "   {\"default\": -1, \"doc\": \"test field two\", \"name\": \"testField2\", \"type\": \"float\"}"
            + "   ]," + " \"name\": \"testObject\", \"type\": \"record\"" + "}");
    Schema schema5 = Schema.parse(
        "{" + "\"fields\": ["
            + "   {\"default\": \"\", \"doc\": \"test field\", \"name\": \"testField1\", \"type\": \"string\"},"
            + "   {\"default\": -1, \"doc\": \"test field three\", \"name\": \"testField2\", \"type\": \"float\"}"
            + "   ]," + " \"name\": \"testObject\", \"type\": \"record\"" + "}");
    doReturn(true).when(mockStore).isReadComputationEnabled();
    doReturn(true).when(mockSchemaRepo).hasValueSchema(anyString(), anyInt());
    SchemaEntry schemaEntry1 = new SchemaEntry(1, schema1);
    SchemaEntry schemaEntry2 = new SchemaEntry(2, schema2);
    SchemaEntry schemaEntry5 = new SchemaEntry(5, schema5);
    doReturn(schemaEntry1).when(mockSchemaRepo).getValueSchema(anyString(), anyInt());
    doReturn(Arrays.asList(schemaEntry1, schemaEntry5, schemaEntry2)).when(mockSchemaRepo).getValueSchemas(anyString());
    storeIngestionTaskUnderTest.setValueSchemaId(2);
    storeIngestionTaskUnderTest.warmupSchemaCache(mockStore);
    verify(storeIngestionTaskUnderTest, times(1)).cacheFastAvroGenericDeserializer(schema1, schema1, 120000L);
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
    getPosition(localVeniceWriter.put(putKeyFoo, putValue, SCHEMA_ID));
    InMemoryPubSubPosition barLastPosition = getPosition(localVeniceWriter.put(putKeyBar, putValue, SCHEMA_ID));
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    doThrow(new VeniceException("fake storage engine exception")).when(mockAbstractStorageEngine)
        .put(eq(PARTITION_FOO), any(), any(ByteBuffer.class));

    runTest(Utils.setOf(PARTITION_FOO, PARTITION_BAR), () -> {
      ArgumentCaptor<InMemoryPubSubPosition> captor = ArgumentCaptor.forClass(InMemoryPubSubPosition.class);
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS))
          .completed(eq(topic), eq(PARTITION_BAR), captor.capture(), eq("STANDBY"));
      InMemoryPubSubPosition capturedPosition = captor.getValue();
      assertTrue(capturedPosition.getInternalOffset() >= barLastPosition.getInternalOffset());

      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).error(eq(topic), eq(PARTITION_FOO), anyString(), any());
      verify(mockLogNotifier, never()).completed(eq(topic), eq(PARTITION_FOO), any());
      // Error partition should be unsubscribed
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).stopped(eq(topic), eq(PARTITION_FOO), any());
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
    propertyBuilder.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryLocalKafkaBroker.getPubSubBrokerAddress());
    propertyBuilder.put(HYBRID_QUOTA_ENFORCEMENT_ENABLED, false);
    propertyBuilder.put(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, databaseChecksumVerificationEnabled);
    propertyBuilder.put(SERVER_LOCAL_CONSUMER_CONFIG_PREFIX, VeniceProperties.empty());
    propertyBuilder.put(SERVER_REMOTE_CONSUMER_CONFIG_PREFIX, VeniceProperties.empty());
    propertyBuilder.put(SERVER_INGESTION_HEARTBEAT_INTERVAL_MS, 1000);
    propertyBuilder.put(SERVER_LEADER_COMPLETE_STATE_CHECK_IN_FOLLOWER_VALID_INTERVAL_MS, 1000);
    propertyBuilder.put(SERVER_RESUBSCRIPTION_TRIGGERED_BY_VERSION_INGESTION_CONTEXT_CHANGE_ENABLED, true);
    propertyBuilder.put(SERVER_RESET_ERROR_REPLICA_ENABLED, true);
    propertyBuilder.put(SERVER_IDLE_INGESTION_TASK_CLEANUP_INTERVAL_IN_SECONDS, -1);
    extraProperties.forEach(propertyBuilder::put);

    Map<String, Map<String, String>> kafkaClusterMap = new HashMap<>();
    Map<String, String> localKafkaMapping = new HashMap<>();
    localKafkaMapping.put(KAFKA_CLUSTER_MAP_KEY_NAME, "dev");
    localKafkaMapping.put(KAFKA_CLUSTER_MAP_KEY_URL, inMemoryLocalKafkaBroker.getPubSubBrokerAddress());
    kafkaClusterMap.put(String.valueOf(0), localKafkaMapping);

    Map<String, String> remoteKafkaMapping = new HashMap<>();
    remoteKafkaMapping.put(KAFKA_CLUSTER_MAP_KEY_NAME, "remote");
    remoteKafkaMapping.put(KAFKA_CLUSTER_MAP_KEY_URL, inMemoryRemoteKafkaBroker.getPubSubBrokerAddress());
    kafkaClusterMap.put(String.valueOf(1), remoteKafkaMapping);

    propertyBuilder.put(SERVER_AA_WC_WORKLOAD_PARALLEL_PROCESSING_ENABLED, isAaWCParallelProcessingEnabled());

    return new VeniceServerConfig(propertyBuilder.build(), kafkaClusterMap);
  }

  private void verifyPut(AAConfig aaConfig, boolean operationWithMetadata) {
    VenicePartitioner partitioner = getVenicePartitioner();
    int targetPartitionPutKeyFoo = partitioner.getPartitionId(putKeyFoo, PARTITION_COUNT);

    if (aaConfig == AA_ON) {
      if (!operationWithMetadata) {
        // Verify StorageEngine#put is invoked only once and with appropriate key & value.
        verify(mockAbstractStorageEngine, timeout(2000)).put(
            targetPartitionPutKeyFoo,
            putKeyFoo,
            ByteBuffer.wrap(ValueRecord.create(EXISTING_SCHEMA_ID, putValue).serialize()));
        verify(mockAbstractStorageEngine, never())
            .putWithReplicationMetadata(eq(targetPartitionPutKeyFoo), eq(putKeyFoo), any(ByteBuffer.class), any());
      } else {
        // Verify StorageEngine#putWithReplicationMetadata is invoked only once and with appropriate key & value.
        verify(mockAbstractStorageEngine, timeout(2000))
            .putWithReplicationMetadata(eq(targetPartitionPutKeyFoo), eq(putKeyFoo), any(ByteBuffer.class), any());
        verify(mockAbstractStorageEngine, never())
            .put(eq(targetPartitionPutKeyFoo), eq(putKeyFoo), any(ByteBuffer.class));
      }
    } else {
      // Verify StorageEngine#put is invoked only once and with appropriate key & value.
      verify(mockAbstractStorageEngine, timeout(2000)).put(
          targetPartitionPutKeyFoo,
          putKeyFoo,
          ByteBuffer.wrap(ValueRecord.create(EXISTING_SCHEMA_ID, putValue).serialize()));

      // Verify StorageEngine#putWithReplicationMetadata is never invoked for put operation.
      verify(mockAbstractStorageEngine, never()).putWithReplicationMetadata(
          targetPartitionPutKeyFoo,
          putKeyFoo,
          ByteBuffer.wrap(ValueRecord.create(EXISTING_SCHEMA_ID, putValue).serialize()),
          putKeyFooReplicationMetadataWithValueSchemaIdBytes);
    }
  }

  private void verifyDelete(AAConfig aaConfig, boolean operationWithMetadata, boolean isBatchWrite) {
    VenicePartitioner partitioner = getVenicePartitioner();
    int targetPartitionDeleteKeyFoo = partitioner.getPartitionId(deleteKeyFoo, PARTITION_COUNT);

    if (aaConfig == AA_ON) {
      if (!operationWithMetadata && isBatchWrite) {
        // Verify StorageEngine#deleteWithReplicationMetadata is invoked only once and with appropriate key.
        verify(mockAbstractStorageEngine, timeout(2000)).delete(eq(targetPartitionDeleteKeyFoo), eq(deleteKeyFoo));
      } else {
        // Verify StorageEngine#deleteWithReplicationMetadata is invoked only once and with appropriate key.
        verify(mockAbstractStorageEngine, timeout(2000))
            .deleteWithReplicationMetadata(eq(targetPartitionDeleteKeyFoo), eq(deleteKeyFoo), any());
      }
    } else {
      // Verify StorageEngine#Delete is invoked only once and with appropriate key.
      verify(mockAbstractStorageEngine, timeout(2000)).delete(targetPartitionDeleteKeyFoo, deleteKeyFoo);
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
        true,
        null,
        null,
        this.mockStorageService).build();

    Properties kafkaProps = new Properties();
    kafkaProps.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryLocalKafkaBroker.getPubSubBrokerAddress());

    storeIngestionTaskUnderTest = ingestionTaskFactory.getNewIngestionTask(
        this.mockStorageService,
        mockStore,
        version,
        kafkaProps,
        isCurrentVersion,
        storeConfig,
        PARTITION_FOO,
        false,
        Optional.empty(),
        null,
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

    kafkaUrlToRecordsThrottler.put(inMemoryLocalKafkaBroker.getPubSubBrokerAddress(), localThrottler);
    kafkaUrlToRecordsThrottler.put(inMemoryRemoteKafkaBroker.getPubSubBrokerAddress(), remoteThrottler);

    String rtTopic = Utils.getRealTimeTopicName(storeInfo);
    PubSubTopic rtPubSubTopic = pubSubTopicRepository.getTopic(rtTopic);
    PubSubTopicPartition fooRtPartition = new PubSubTopicPartitionImpl(rtPubSubTopic, PARTITION_FOO);

    PartitionReplicaIngestionContext fooRtPartitionReplicaIngestionContext = new PartitionReplicaIngestionContext(
        pubSubTopic,
        fooRtPartition,
        VersionRole.CURRENT,
        PartitionReplicaIngestionContext.WorkloadType.NON_AA_OR_WRITE_COMPUTE);
    inMemoryLocalKafkaBroker.createTopic(rtTopic, partitionCount);
    inMemoryRemoteKafkaBroker.createTopic(rtTopic, partitionCount);

    PubSubPosition p0 = InMemoryPubSubPosition.of(0);
    aggKafkaConsumerService.subscribeConsumerFor(
        inMemoryLocalKafkaBroker.getPubSubBrokerAddress(),
        storeIngestionTaskUnderTest,
        fooRtPartitionReplicaIngestionContext,
        p0,
        false);
    aggKafkaConsumerService.subscribeConsumerFor(
        inMemoryRemoteKafkaBroker.getPubSubBrokerAddress(),
        storeIngestionTaskUnderTest,
        fooRtPartitionReplicaIngestionContext,
        p0,
        false);

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
  public void testIsReadyToServe(NodeType nodeType, AAConfig aaConfig) {
    int partitionCount = 2;

    VenicePartitioner partitioner = getVenicePartitioner(); // Only get base venice partitioner
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setPartitionerClass(partitioner.getClass().getName());

    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(100, 100, 100, BufferReplayPolicy.REWIND_FROM_EOP);

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

    StoreIngestionTaskFactory ingestionTaskFactory = getIngestionTaskFactoryBuilder(
        new RandomPollStrategy(),
        Utils.setOf(PARTITION_FOO),
        Optional.empty(),
        extraServerProperties,
        false,
        null,
        null,
        this.mockStorageService).setIsDaVinciClient(nodeType == DA_VINCI)
            .setAggKafkaConsumerService(aggKafkaConsumerService)
            .build();

    doReturn(true).when(mockTopicManager).containsTopicCached(any());
    doReturn(true).when(mockTopicManagerRemote).containsTopicCached(any());

    Properties kafkaProps = new Properties();
    kafkaProps.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryLocalKafkaBroker.getPubSubBrokerAddress());

    storeIngestionTaskUnderTest = ingestionTaskFactory.getNewIngestionTask(
        this.mockStorageService,
        mockStore,
        version,
        kafkaProps,
        isCurrentVersion,
        storeConfig,
        PARTITION_FOO,
        false,
        Optional.empty(),
        null,
        null);
    PubSubTopicPartition partition0 = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    String rtTopicName = Utils.getRealTimeTopicName(mockStore);
    PubSubTopic rtTopic = pubSubTopicRepository.getTopic(rtTopicName);
    TopicSwitch topicSwitchWithSourceRealTimeTopic = new TopicSwitch();
    topicSwitchWithSourceRealTimeTopic.sourceKafkaServers = new ArrayList<>();
    topicSwitchWithSourceRealTimeTopic.sourceKafkaServers.add(inMemoryRemoteKafkaBroker.getPubSubBrokerAddress());
    topicSwitchWithSourceRealTimeTopic.sourceTopicName = rtTopicName;
    TopicSwitchWrapper topicSwitchWithSourceRealTimeTopicWrapper = new TopicSwitchWrapper(
        topicSwitchWithSourceRealTimeTopic,
        pubSubTopicRepository.getTopic(topicSwitchWithSourceRealTimeTopic.sourceTopicName.toString()));

    InMemoryPubSubPosition p5 = InMemoryPubSubPosition.of(5);
    OffsetRecord mockOffsetRecordLagCaughtUp = mock(OffsetRecord.class);
    doReturn(p5).when(mockOffsetRecordLagCaughtUp).getCheckpointedLocalVtPosition();
    doReturn(rtTopic).when(mockOffsetRecordLagCaughtUp).getLeaderTopic(any());
    doReturn(p5).when(mockOffsetRecordLagCaughtUp)
        .getCheckpointedRtPosition(inMemoryLocalKafkaBroker.getPubSubBrokerAddress());
    doReturn(p5).when(mockOffsetRecordLagCaughtUp)
        .getCheckpointedRtPosition(inMemoryRemoteKafkaBroker.getPubSubBrokerAddress());

    OffsetRecord mockOffsetRecordLagCaughtUpTimestampLagging = mock(OffsetRecord.class);
    doReturn(p5).when(mockOffsetRecordLagCaughtUpTimestampLagging).getCheckpointedLocalVtPosition();
    doReturn(rtTopic).when(mockOffsetRecordLagCaughtUpTimestampLagging).getLeaderTopic(any());
    doReturn(p5).when(mockOffsetRecordLagCaughtUpTimestampLagging)
        .getCheckpointedRtPosition(inMemoryLocalKafkaBroker.getPubSubBrokerAddress());
    doReturn(p5).when(mockOffsetRecordLagCaughtUpTimestampLagging)
        .getCheckpointedRtPosition(inMemoryRemoteKafkaBroker.getPubSubBrokerAddress());
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

    doReturn(p5).when(mockTopicManager).getLatestPositionCachedNonBlocking(any(PubSubTopicPartition.class));
    doReturn(p5).when(mockTopicManager).getLatestPositionCached(any(PubSubTopicPartition.class));
    doReturn(p5).when(mockTopicManagerRemote).getLatestPositionCachedNonBlocking(any(PubSubTopicPartition.class));
    doReturn(p5).when(mockTopicManagerRemote).getLatestPositionCached(any(PubSubTopicPartition.class));

    doReturn(0).when(mockPcsBufferReplayStartedLagCaughtUp).getPartition();
    doReturn(PubSubSymbolicPosition.EARLIEST).when(mockPcsBufferReplayStartedLagCaughtUp)
        .getLatestProcessedRtPosition(anyString());
    doReturn(PubSubSymbolicPosition.EARLIEST).when(mockPcsBufferReplayStartedLagCaughtUp)
        .getLatestProcessedVtPosition();

    doReturn(partition0).when(mockPcsBufferReplayStartedLagCaughtUp).getReplicaTopicPartition();
    storeIngestionTaskUnderTest.setPartitionConsumptionState(0, mockPcsBufferReplayStartedLagCaughtUp);

    if (nodeType == NodeType.LEADER) {
      // case 5a: leader replica => partition is ready to serve
      doReturn(LeaderFollowerStateType.LEADER).when(mockPcsBufferReplayStartedLagCaughtUp).getLeaderFollowerState();
      assertTrue(storeIngestionTaskUnderTest.isReadyToServe(mockPcsBufferReplayStartedLagCaughtUp));
    } else {
      // case 5b: standby replica and !LEADER_COMPLETED
      doReturn(STANDBY).when(mockPcsBufferReplayStartedLagCaughtUp).getLeaderFollowerState();
      doReturn(LEADER_NOT_COMPLETED).when(mockPcsBufferReplayStartedLagCaughtUp).getLeaderCompleteState();
      assertFalse(storeIngestionTaskUnderTest.isReadyToServe(mockPcsBufferReplayStartedLagCaughtUp));
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
    InMemoryPubSubPosition p150 = InMemoryPubSubPosition.of(150L);
    PartitionConsumptionState mockPcsBufferReplayStartedRemoteLagging = mock(PartitionConsumptionState.class);
    doReturn(partition0).when(mockPcsBufferReplayStartedRemoteLagging).getReplicaTopicPartition();
    doReturn(true).when(mockPcsBufferReplayStartedRemoteLagging).isEndOfPushReceived();
    doReturn(false).when(mockPcsBufferReplayStartedRemoteLagging).isComplete();
    doReturn(true).when(mockPcsBufferReplayStartedRemoteLagging).isWaitingForReplicationLag();
    doReturn(true).when(mockPcsBufferReplayStartedRemoteLagging).isHybrid();
    doReturn(topicSwitchWithSourceRealTimeTopicWrapper).when(mockPcsBufferReplayStartedRemoteLagging).getTopicSwitch();
    doReturn(mockOffsetRecordLagCaughtUpTimestampLagging).when(mockPcsBufferReplayStartedRemoteLagging)
        .getOffsetRecord();
    doReturn(p150).when(mockTopicManager).getLatestPositionCachedNonBlocking(any(PubSubTopicPartition.class));
    doReturn(p150).when(mockTopicManagerRemote).getLatestPositionCachedNonBlocking(any(PubSubTopicPartition.class));
    doReturn(p150.getInternalOffset()).when(aggKafkaConsumerService)
        .getLatestOffsetBasedOnMetrics(anyString(), any(), any());
    doReturn(PubSubSymbolicPosition.EARLIEST).when(mockPcsBufferReplayStartedRemoteLagging)
        .getLatestProcessedRtPosition(anyString());
    doReturn(PubSubSymbolicPosition.EARLIEST).when(mockPcsBufferReplayStartedRemoteLagging)
        .getLatestProcessedVtPosition();

    LOGGER.info("localTM: {}, remoteTM: {}", mockTopicManager, mockTopicManagerRemote);

    PubSubPosition endPosition = storeIngestionTaskUnderTest
        .getTopicPartitionEndPosition(localKafkaConsumerService.kafkaUrl, new PubSubTopicPartitionImpl(pubSubTopic, 1));
    // Lag based metrics returns hard-coded ApacheKafkaOffsetPosition to make APIs compatible with PubSubPosition
    assertEquals(((InMemoryPubSubPosition) endPosition).getInternalOffset(), p150.getInternalOffset());
    if (nodeType == NodeType.LEADER) {
      // case 6a: leader replica => partition is not ready to serve
      doReturn(LeaderFollowerStateType.LEADER).when(mockPcsBufferReplayStartedRemoteLagging).getLeaderFollowerState();
      assertFalse(storeIngestionTaskUnderTest.isReadyToServe(mockPcsBufferReplayStartedRemoteLagging));
    } else {
      // case 6b: standby replica and !LEADER_COMPLETED
      doReturn(STANDBY).when(mockPcsBufferReplayStartedRemoteLagging).getLeaderFollowerState();
      doReturn(LEADER_NOT_COMPLETED).when(mockPcsBufferReplayStartedRemoteLagging).getLeaderCompleteState();
      assertFalse(storeIngestionTaskUnderTest.isReadyToServe(mockPcsBufferReplayStartedRemoteLagging));
      // case 6c: standby replica and LEADER_COMPLETED => partition is ready to serve
      doReturn(LEADER_COMPLETED).when(mockPcsBufferReplayStartedRemoteLagging).getLeaderCompleteState();
      doReturn(p150).when(mockPcsBufferReplayStartedRemoteLagging).getLatestProcessedVtPosition();
      doCallRealMethod().when(mockPcsBufferReplayStartedRemoteLagging).isLeaderCompleted();
      doReturn(System.currentTimeMillis()).when(mockPcsBufferReplayStartedRemoteLagging)
          .getLastLeaderCompleteStateUpdateInMs();
      assertTrue(storeIngestionTaskUnderTest.isReadyToServe(mockPcsBufferReplayStartedRemoteLagging));
    }

    // case 7: If there are issues in replication from remote RT -> local VT, leader won't be marked completed,
    // but both DaVinci replica and storage node will be marked ready to serve if leader were to be completed
    PartitionConsumptionState mockPcsOffsetLagCaughtUpTimestampLagging = mock(PartitionConsumptionState.class);
    doReturn(partition0).when(mockPcsOffsetLagCaughtUpTimestampLagging).getReplicaTopicPartition();
    doReturn(PubSubSymbolicPosition.EARLIEST).when(mockPcsOffsetLagCaughtUpTimestampLagging)
        .getLatestProcessedRtPosition(anyString());
    doReturn(PubSubSymbolicPosition.EARLIEST).when(mockPcsOffsetLagCaughtUpTimestampLagging)
        .getLatestProcessedVtPosition();
    doReturn(true).when(mockPcsOffsetLagCaughtUpTimestampLagging).isEndOfPushReceived();
    doReturn(false).when(mockPcsOffsetLagCaughtUpTimestampLagging).isComplete();
    doReturn(true).when(mockPcsOffsetLagCaughtUpTimestampLagging).isWaitingForReplicationLag();
    doReturn(true).when(mockPcsOffsetLagCaughtUpTimestampLagging).isHybrid();
    doReturn(topicSwitchWithSourceRealTimeTopicWrapper).when(mockPcsOffsetLagCaughtUpTimestampLagging).getTopicSwitch();
    doReturn(mockOffsetRecordLagCaughtUpTimestampLagging).when(mockPcsOffsetLagCaughtUpTimestampLagging)
        .getOffsetRecord();
    doReturn(p150).when(mockTopicManager).getLatestPositionCachedNonBlocking(any(PubSubTopicPartition.class));
    doReturn(p150).when(mockTopicManagerRemote).getLatestPositionCachedNonBlocking(any(PubSubTopicPartition.class));
    if (nodeType == NodeType.LEADER) {
      // case 7a: leader replica => partition is not ready to serve
      doReturn(LeaderFollowerStateType.LEADER).when(mockPcsOffsetLagCaughtUpTimestampLagging).getLeaderFollowerState();
      assertFalse(storeIngestionTaskUnderTest.isReadyToServe(mockPcsOffsetLagCaughtUpTimestampLagging));
    } else {
      // case 7b: standby replica and !LEADER_COMPLETED
      doReturn(STANDBY).when(mockPcsOffsetLagCaughtUpTimestampLagging).getLeaderFollowerState();
      doReturn(LEADER_NOT_COMPLETED).when(mockPcsOffsetLagCaughtUpTimestampLagging).getLeaderCompleteState();
      assertFalse(storeIngestionTaskUnderTest.isReadyToServe(mockPcsOffsetLagCaughtUpTimestampLagging));
      // case 7c: standby replica and LEADER_COMPLETED => partition is ready to serve
      doReturn(LEADER_COMPLETED).when(mockPcsOffsetLagCaughtUpTimestampLagging).getLeaderCompleteState();
      doCallRealMethod().when(mockPcsOffsetLagCaughtUpTimestampLagging).isLeaderCompleted();
      doReturn(System.currentTimeMillis()).when(mockPcsOffsetLagCaughtUpTimestampLagging)
          .getLastLeaderCompleteStateUpdateInMs();
      doReturn(p150).when(mockPcsOffsetLagCaughtUpTimestampLagging).getLatestProcessedVtPosition();
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
      hybridStoreConfig = new HybridStoreConfigImpl(100, 100, -1, BufferReplayPolicy.REWIND_FROM_EOP);
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
        false,
        null,
        null,
        this.mockStorageService).setIsDaVinciClient(nodeType == DA_VINCI)
            .setAggKafkaConsumerService(aggKafkaConsumerService)
            .build();

    doReturn(true).when(mockTopicManager).containsTopic(any());
    doReturn(true).when(mockTopicManagerRemote).containsTopic(any());

    Properties kafkaProps = new Properties();
    kafkaProps.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryLocalKafkaBroker.getPubSubBrokerAddress());
    storeIngestionTaskUnderTest = ingestionTaskFactory.getNewIngestionTask(
        this.mockStorageService,
        mockStore,
        version,
        kafkaProps,
        isCurrentVersion,
        storeConfig,
        PARTITION_FOO,
        false,
        Optional.empty(),
        null,
        null);

    if (hybridConfig.equals(HYBRID) && nodeType.equals(LEADER) && isAaWCParallelProcessingEnabled()) {
      assertTrue(storeIngestionTaskUnderTest instanceof ActiveActiveStoreIngestionTask);
      ActiveActiveStoreIngestionTask activeActiveStoreIngestionTask =
          (ActiveActiveStoreIngestionTask) storeIngestionTaskUnderTest;
      assertNotNull(activeActiveStoreIngestionTask.getIngestionBatchProcessor());
      assertNotNull(activeActiveStoreIngestionTask.getIngestionBatchProcessor().getLockManager());
    }

    PubSubTopic rtTopic = pubSubTopicRepository.getTopic(Utils.getRealTimeTopicName(mockStore));
    // Create a TopicSwitch with multiple source PubSub servers
    TopicSwitch topicSwitchWithMultipleSourceKafkaServers = new TopicSwitch();
    topicSwitchWithMultipleSourceKafkaServers.sourceKafkaServers = new ArrayList<>();
    topicSwitchWithMultipleSourceKafkaServers.sourceKafkaServers.add(inMemoryLocalKafkaBroker.getPubSubBrokerAddress());
    topicSwitchWithMultipleSourceKafkaServers.sourceKafkaServers
        .add(inMemoryRemoteKafkaBroker.getPubSubBrokerAddress());
    topicSwitchWithMultipleSourceKafkaServers.sourceTopicName = rtTopic.getName();
    TopicSwitchWrapper topicSwitchWithMultipleSourceKafkaServersWrapper =
        new TopicSwitchWrapper(topicSwitchWithMultipleSourceKafkaServers, rtTopic);

    InMemoryPubSubPosition p5 = InMemoryPubSubPosition.of(5);
    InMemoryPubSubPosition p150 = InMemoryPubSubPosition.of(150);

    OffsetRecord mockOffsetRecord = mock(OffsetRecord.class);
    doReturn(p5).when(mockOffsetRecord).getCheckpointedLocalVtPosition();
    doReturn(rtTopic).when(mockOffsetRecord).getLeaderTopic(any());
    doReturn(p5).when(mockOffsetRecord).getCheckpointedRtPosition(inMemoryLocalKafkaBroker.getPubSubBrokerAddress());
    doReturn(p5).when(mockOffsetRecord).getCheckpointedRtPosition(inMemoryRemoteKafkaBroker.getPubSubBrokerAddress());

    // Local replication are caught up but remote replication are not. A/A storage node replica is not ready to serve
    // Since host has caught up to lag in local VT, DaVinci replica will be marked ready to serve
    PartitionConsumptionState pcs0 = mock(PartitionConsumptionState.class);
    doReturn(true).when(pcs0).isEndOfPushReceived();
    doReturn(hybridConfig != HYBRID).when(pcs0).isComplete();
    doReturn(true).when(pcs0).isWaitingForReplicationLag();
    doReturn(hybridConfig == HYBRID).when(pcs0).isHybrid();
    doReturn(topicSwitchWithMultipleSourceKafkaServersWrapper).when(pcs0).getTopicSwitch();
    doReturn(mockOffsetRecord).when(pcs0).getOffsetRecord();
    doReturn(p5).when(mockTopicManager).getLatestPositionCached(any(PubSubTopicPartition.class));
    doReturn(p150).when(mockTopicManagerRemote).getLatestPositionCached(any(PubSubTopicPartition.class));
    doReturn(0).when(pcs0).getPartition();
    PubSubTopicPartition vtP0 = new PubSubTopicPartitionImpl(storeIngestionTaskUnderTest.getVersionTopic(), 0);
    doReturn(vtP0).when(pcs0).getReplicaTopicPartition();
    doReturn(vtP0.toString()).when(pcs0).getReplicaId();
    doReturn(p5).when(pcs0).getLatestProcessedVtPosition();
    if (nodeType == NodeType.LEADER) {
      doReturn(LeaderFollowerStateType.LEADER).when(pcs0).getLeaderFollowerState();
    } else {
      doReturn(STANDBY).when(pcs0).getLeaderFollowerState();
      doReturn(LEADER_COMPLETED).when(pcs0).getLeaderCompleteState();
      doCallRealMethod().when(pcs0).isLeaderCompleted();
      doReturn(System.currentTimeMillis()).when(pcs0).getLastLeaderCompleteStateUpdateInMs();
    }
    doReturn(p5).when(pcs0).getLatestProcessedRtPosition(anyString());

    storeIngestionTaskUnderTest.setPartitionConsumptionState(0, pcs0);
    if (hybridConfig == HYBRID && nodeType == NodeType.LEADER) {
      assertFalse(storeIngestionTaskUnderTest.isReadyToServe(pcs0));
    } else {
      assertTrue(storeIngestionTaskUnderTest.isReadyToServe(pcs0));
    }

    // TODO: Move the following code into a separate test case since it is not related to isReadyToServe()
    // The following code is trying to verify that getTopicPartitionEndPosition() is non-blocking
    LOGGER.info("Case2: Verify non-blocking behavior of getTopicPartitionEndPosition()");
    InMemoryPubSubPosition p10 = InMemoryPubSubPosition.of(10L);
    doReturn(p10.getInternalOffset()).when(aggKafkaConsumerService)
        .getLatestOffsetBasedOnMetrics(anyString(), any(), any());
    doReturn(p10).when(mockTopicManager).getLatestPositionCachedNonBlocking(any(PubSubTopicPartition.class));
    PubSubPosition endPosition = storeIngestionTaskUnderTest
        .getTopicPartitionEndPosition(localKafkaConsumerService.kafkaUrl, new PubSubTopicPartitionImpl(pubSubTopic, 0));
    assertEquals(((InMemoryPubSubPosition) endPosition).getInternalOffset(), p10.getInternalOffset());
    doReturn(-1L).when(aggKafkaConsumerService).getLatestOffsetBasedOnMetrics(anyString(), any(), any());
    doReturn(PubSubSymbolicPosition.LATEST).when(mockTopicManager)
        .getLatestPositionCachedNonBlocking(any(PubSubTopicPartition.class));
    long startTime = System.currentTimeMillis();
    endPosition = storeIngestionTaskUnderTest
        .getTopicPartitionEndPosition(localKafkaConsumerService.kafkaUrl, new PubSubTopicPartitionImpl(pubSubTopic, 0));
    long elapsedTime = System.currentTimeMillis() - startTime;
    // verify getLatestPositionCachedNonBlocking was called multiple times (retries with exponential backoff).
    // The actual count varies (5-10) due to exponential backoff timing within the 5-second max duration.
    verify(mockTopicManager, atLeast(5)).getLatestPositionCachedNonBlocking(any(PubSubTopicPartition.class));
    // elapsed time should be less than 10 seconds (10 retries with 1 second interval)
    assertTrue(elapsedTime < 10000, "elapsed time: " + elapsedTime);
    assertEquals(endPosition, PubSubSymbolicPosition.LATEST);
  }

  @DataProvider
  public static Object[][] testCheckAndLogIfLagIsAcceptableForHybridStoreProvider() {
    return DataProviderUtils
        .allPermutationGenerator(LagType.values(), new NodeType[] { DA_VINCI, FOLLOWER }, AAConfig.values());
  }

  /**
   * @param lagType N.B. this only affects cosmetic logging details at the level where we mock it
   * @param nodeType Can be either DVC or follower
   * @param aaConfig AA on/off
   */
  @Test(dataProvider = "testCheckAndLogIfLagIsAcceptableForHybridStoreProvider")
  public void testCheckAndLogIfLagIsAcceptableForHybridStore(LagType lagType, NodeType nodeType, AAConfig aaConfig) {
    int partitionCount = 2;
    VenicePartitioner partitioner = getVenicePartitioner();
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setPartitionerClass(partitioner.getClass().getName());

    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(100, 100, 100, BufferReplayPolicy.REWIND_FROM_EOP);

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

    StoreIngestionTaskFactory ingestionTaskFactory = getIngestionTaskFactoryBuilder(
        new RandomPollStrategy(),
        Utils.setOf(PARTITION_FOO),
        Optional.empty(),
        serverProperties,
        false,
        null,
        null,
        this.mockStorageService).setIsDaVinciClient(nodeType == DA_VINCI)
            .setAggKafkaConsumerService(aggKafkaConsumerService)
            .build();

    doReturn(true).when(mockTopicManager).containsTopic(any());
    doReturn(true).when(mockTopicManagerRemote).containsTopic(any());

    Properties kafkaProps = new Properties();
    kafkaProps.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryLocalKafkaBroker.getPubSubBrokerAddress());
    storeIngestionTaskUnderTest = ingestionTaskFactory.getNewIngestionTask(
        this.mockStorageService,
        mockStore,
        version,
        kafkaProps,
        isCurrentVersion,
        storeConfig,
        PARTITION_FOO,
        false,
        Optional.empty(),
        null,
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
              lagType));
    }

    // case 2: offsetLag > offsetThreshold and instance is not a leader
    doReturn(STANDBY).when(mockPartitionConsumptionState).getLeaderFollowerState();
    assertFalse(
        storeIngestionTaskUnderTest.checkAndLogIfLagIsAcceptableForHybridStore(
            mockPartitionConsumptionState,
            offsetLag,
            offsetThreshold,
            false,
            lagType));

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
              lagType));
    }

    // Case 4: offsetLag <= offsetThreshold and instance is a standby or DaVinciClient
    doReturn(STANDBY).when(mockPartitionConsumptionState).getLeaderFollowerState();
    assertFalse(
        storeIngestionTaskUnderTest.checkAndLogIfLagIsAcceptableForHybridStore(
            mockPartitionConsumptionState,
            offsetLag,
            offsetThreshold,
            false,
            lagType));

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
            lagType));

    // Case 6: offsetLag <= offsetThreshold and instance is a standby or DaVinciClient.
    // and leaderCompleteState is LEADER_COMPLETED and last update time is more than threshold
    doReturn(System.currentTimeMillis() - 6000).when(mockPartitionConsumptionState)
        .getLastLeaderCompleteStateUpdateInMs();
    assertFalse(
        storeIngestionTaskUnderTest.checkAndLogIfLagIsAcceptableForHybridStore(
            mockPartitionConsumptionState,
            offsetLag,
            offsetThreshold,
            false,
            lagType));

    // Case 7: offsetLag <= offsetThreshold and instance is a standby or DaVinciClient
    // and leaderCompleteState is LEADER_NOT_COMPLETED and leader is not completed
    doReturn(LEADER_NOT_COMPLETED).when(mockPartitionConsumptionState).getLeaderCompleteState();
    assertFalse(
        storeIngestionTaskUnderTest.checkAndLogIfLagIsAcceptableForHybridStore(
            mockPartitionConsumptionState,
            offsetLag,
            offsetThreshold,
            false,
            lagType));
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
      hybridStoreConfig = new HybridStoreConfigImpl(100, 100, 100, BufferReplayPolicy.REWIND_FROM_EOP);
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
        false,
        null,
        null,
        this.mockStorageService).setIsDaVinciClient(nodeType == DA_VINCI)
            .setAggKafkaConsumerService(aggKafkaConsumerService)
            .build();

    Properties kafkaProps = new Properties();
    kafkaProps.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryLocalKafkaBroker.getPubSubBrokerAddress());
    LeaderFollowerStoreIngestionTask ingestionTask =
        (LeaderFollowerStoreIngestionTask) ingestionTaskFactory.getNewIngestionTask(
            this.mockStorageService,
            mockStore,
            version,
            kafkaProps,
            isCurrentVersion,
            storeConfig,
            PARTITION_FOO,
            false,
            Optional.empty(),
            null,
            null);

    OffsetRecord mockOffsetRecord = mock(OffsetRecord.class);
    PartitionConsumptionState partitionConsumptionState = new PartitionConsumptionState(
        new PubSubTopicPartitionImpl(pubSubTopic, PARTITION_FOO),
        mockOffsetRecord,
        pubSubContext,
        true,
        Schema.create(Schema.Type.STRING));

    long producerTimestamp = System.currentTimeMillis();
    LeaderMetadataWrapper mockLeaderMetadataWrapper = mock(LeaderMetadataWrapper.class);
    InMemoryPubSubPosition p10 = InMemoryPubSubPosition.of(10L);
    doReturn(p10).when(mockLeaderMetadataWrapper).getUpstreamPosition();
    KafkaMessageEnvelope kafkaMessageEnvelope =
        getHeartbeatKME(producerTimestamp, mockLeaderMetadataWrapper, generateHeartbeatMessage(CheckSumType.NONE), "0");

    PubSubMessageHeaders pubSubMessageHeaders = new PubSubMessageHeaders();
    pubSubMessageHeaders.add(LEADER_COMPLETE_STATE_HEADERS.get(LEADER_COMPLETED));
    DefaultPubSubMessage pubSubMessage = new ImmutablePubSubMessage(
        KafkaKey.HEART_BEAT,
        kafkaMessageEnvelope,
        new PubSubTopicPartitionImpl(pubSubTopic, PARTITION_FOO),
        mockedPubSubPosition,
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
    return DataProviderUtils.allPermutationGenerator(new NodeType[] { DA_VINCI, LEADER });
  }

  @Test(dataProvider = "testProcessTopicSwitchProvider")
  public void testProcessTopicSwitch(NodeType nodeType) {
    VenicePartitioner partitioner = getVenicePartitioner();
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setPartitionerClass(partitioner.getClass().getName());
    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(100, 100, 100, BufferReplayPolicy.REWIND_FROM_EOP);
    MockStoreVersionConfigs storeAndVersionConfigs =
        setupStoreAndVersionMocks(2, partitionerConfig, Optional.of(hybridStoreConfig), false, true, AA_OFF);
    Store mockStore = storeAndVersionConfigs.store;
    Version version = storeAndVersionConfigs.version;
    VeniceStoreVersionConfig storeConfig = storeAndVersionConfigs.storeVersionConfig;
    StoreIngestionTaskFactory ingestionTaskFactory = getIngestionTaskFactoryBuilder(
        new RandomPollStrategy(),
        Utils.setOf(PARTITION_FOO),
        Optional.empty(),
        new HashMap<>(),
        false,
        null,
        null,
        this.mockStorageService).setIsDaVinciClient(nodeType == DA_VINCI).build();
    Properties kafkaProps = new Properties();
    kafkaProps.put(KAFKA_BOOTSTRAP_SERVERS, inMemoryLocalKafkaBroker.getPubSubBrokerAddress());

    storeIngestionTaskUnderTest = ingestionTaskFactory.getNewIngestionTask(
        this.mockStorageService,
        mockStore,
        version,
        kafkaProps,
        isCurrentVersion,
        storeConfig,
        PARTITION_FOO,
        false,
        Optional.empty(),
        null,
        null);
    TopicSwitch topicSwitchWithRemoteRealTimeTopic = new TopicSwitch();
    topicSwitchWithRemoteRealTimeTopic.sourceKafkaServers = new ArrayList<>();
    topicSwitchWithRemoteRealTimeTopic.sourceKafkaServers.add(inMemoryRemoteKafkaBroker.getPubSubBrokerAddress());
    topicSwitchWithRemoteRealTimeTopic.sourceTopicName = Utils.getRealTimeTopicName(mockStore);
    topicSwitchWithRemoteRealTimeTopic.rewindStartTimestamp = System.currentTimeMillis();
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageUnion = topicSwitchWithRemoteRealTimeTopic;
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    OffsetRecord mockOffsetRecord = mock(OffsetRecord.class);
    doReturn(mockOffsetRecord).when(mockPcs).getOffsetRecord();
    doReturn(PARTITION_FOO).when(mockPcs).getPartition();
    doReturn(PARTITION_FOO).when(mockPcs).getPartition();
    PubSubPosition p10 = InMemoryPubSubPosition.of(10L);
    storeIngestionTaskUnderTest.processTopicSwitch(controlMessage, PARTITION_FOO, p10, mockPcs);
    verify(mockTopicManagerRemote, never()).getPositionByTime(any(), anyLong());
    verify(mockOffsetRecord, never()).checkpointRtPosition(anyString(), any(PubSubPosition.class));
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testUpdateConsumedUpstreamRTOffsetMapDuringRTSubscription(AAConfig aaConfig) {
    String storeName = Utils.getUniqueString("store");
    String versionTopic = Version.composeKafkaTopic(storeName, 1);

    VeniceStoreVersionConfig mockVeniceStoreVersionConfig = mock(VeniceStoreVersionConfig.class);
    doReturn(versionTopic).when(mockVeniceStoreVersionConfig).getStoreVersionName();

    Version mockVersion = mock(Version.class);
    doReturn(storeName).when(mockVersion).getStoreName();
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
    doReturn(-1).when(mockVeniceServerConfig).getIdleIngestionTaskCleanupIntervalInSeconds();
    doReturn(Object2IntMaps.singleton("localhost", 0)).when(mockVeniceServerConfig).getKafkaClusterUrlToIdMap();
    doReturn(Int2ObjectMaps.singleton(0, "localhost")).when(mockVeniceServerConfig).getKafkaClusterIdToUrlMap();
    doReturn(new ReferenceCounted<>(new DeepCopyStorageEngine(this.mockAbstractStorageEngine), se -> {}))
        .when(this.mockStorageService)
        .getRefCountedStorageEngine(anyString());

    StoreIngestionTaskFactory ingestionTaskFactory = TestUtils.getStoreIngestionTaskBuilder(storeName)
        .setPubSubContext(pubSubContext)
        .setStorageMetadataService(mockStorageMetadataService)
        .setMetadataRepository(mockReadOnlyStoreRepository)
        .setServerConfig(mockVeniceServerConfig)
        .build();

    LeaderFollowerStoreIngestionTask ingestionTask =
        (LeaderFollowerStoreIngestionTask) ingestionTaskFactory.getNewIngestionTask(
            this.mockStorageService,
            mockStore,
            mockVersion,
            mockKafkaConsumerProperties,
            () -> true,
            mockVeniceStoreVersionConfig,
            0,
            false,
            Optional.empty(),
            null,
            null);

    TopicSwitch topicSwitch = new TopicSwitch();
    topicSwitch.sourceKafkaServers = Collections.singletonList("localhost");
    topicSwitch.sourceTopicName = "test_rt";
    topicSwitch.rewindStartTimestamp = System.currentTimeMillis();
    TopicSwitchWrapper topicSwitchWrapper =
        new TopicSwitchWrapper(topicSwitch, pubSubTopicRepository.getTopic(topicSwitch.sourceTopicName.toString()));
    PubSubTopic newSourceTopic = pubSubTopicRepository.getTopic(topicSwitch.sourceTopicName.toString());
    PartitionConsumptionState mockPcs = mock(PartitionConsumptionState.class);
    doReturn(PubSubSymbolicPosition.EARLIEST).when(mockPcs).getLatestConsumedRtPosition(anyString());
    doReturn(PubSubSymbolicPosition.EARLIEST).when(mockPcs).getLatestProcessedRtPosition(anyString());
    doReturn(IN_TRANSITION_FROM_STANDBY_TO_LEADER).when(mockPcs).getLeaderFollowerState();
    doReturn(topicSwitchWrapper).when(mockPcs).getTopicSwitch();
    OffsetRecord mockOffsetRecord = mock(OffsetRecord.class);
    doReturn(pubSubTopicRepository.getTopic("test_rt")).when(mockOffsetRecord).getLeaderTopic(any());

    PubSubPosition p1000 = InMemoryPubSubPosition.of(1000L);
    doReturn(p1000).when(mockPcs).getLeaderPosition(anyString(), anyBoolean());
    doReturn(mockOffsetRecord).when(mockPcs).getOffsetRecord();
    // Test whether consumedUpstreamRTOffsetMap is updated when leader subscribes to RT after state transition
    ingestionTask.startConsumingAsLeader(mockPcs);
    verify(mockPcs, times(1)).setLatestConsumedRtPosition(
        eq(aaConfig == AA_ON ? "localhost" : OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY),
        eq(p1000));

    PubSubTopic rtTopic = pubSubTopicRepository.getTopic("test_rt");
    Supplier<PartitionConsumptionState> mockPcsSupplier = () -> {
      PartitionConsumptionState mock = mock(PartitionConsumptionState.class);
      doReturn(PubSubSymbolicPosition.EARLIEST).when(mock).getLatestConsumedRtPosition(anyString());
      doReturn(PubSubSymbolicPosition.EARLIEST).when(mock).getLatestProcessedRtPosition(anyString());
      doReturn(LeaderFollowerStateType.LEADER).when(mock).getLeaderFollowerState();
      doReturn(topicSwitchWrapper).when(mock).getTopicSwitch();
      OffsetRecord mockOR = mock(OffsetRecord.class);
      doReturn(rtTopic).when(mockOR).getLeaderTopic(any());
      doReturn(p1000).when(mock).getLeaderPosition(anyString(), anyBoolean());
      System.out.println(mockOR.getLeaderTopic(null));
      doReturn(p1000).when(mockOR).getCheckpointedRtPosition(anyString());
      doReturn(p1000).when(mock).getLatestProcessedRtPosition(anyString());
      doReturn(mockOR).when(mock).getOffsetRecord();
      System.out.println("inside mock" + mockOR.getLeaderTopic(null));
      return mock;
    };
    mockPcs = mockPcsSupplier.get();

    // Test whether consumedUpstreamRTOffsetMap is updated when leader subscribes to RT after executing TS
    ingestionTask.leaderExecuteTopicSwitch(mockPcs, topicSwitch, newSourceTopic);
    verify(mockPcs, times(1)).setLatestConsumedRtPosition(
        eq(aaConfig == AA_ON ? "localhost" : OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY),
        eq(p1000));

    // Test alternative branch of the code
    Supplier<PartitionConsumptionState> mockPcsSupplier2 = () -> {
      PartitionConsumptionState mock = mockPcsSupplier.get();
      doReturn(versionTopic + "-0").when(mock).getReplicaId();
      doReturn(PubSubSymbolicPosition.EARLIEST).when(mock).getLeaderPosition(anyString(), anyBoolean());
      doReturn(PubSubSymbolicPosition.EARLIEST).when(mock).getLatestProcessedRtPosition(anyString());
      doReturn(new PubSubTopicPartitionImpl(rtTopic, 0)).when(mock).getSourceTopicPartition(any());
      return mock;
    };
    mockPcs = mockPcsSupplier2.get();

    when(mockTopicManagerRepository.getTopicManager("localhost")).thenReturn(mockTopicManager);
    doReturn(PubSubSymbolicPosition.EARLIEST).when(mockTopicManager).getPositionByTime(any(), anyLong());

    ingestionTask.leaderExecuteTopicSwitch(mockPcs, topicSwitch, newSourceTopic);
    verify(mockPcs, never()).setLatestConsumedRtPosition(anyString(), any());

    // One more branch
    mockPcs = mockPcsSupplier2.get();
    topicSwitch.rewindStartTimestamp = 0;
    ingestionTask.leaderExecuteTopicSwitch(mockPcs, topicSwitch, newSourceTopic);
    verify(mockPcs, never()).setLatestConsumedRtPosition(anyString(), any());
  }

  @Test
  public void testLeaderShouldSubscribeToCorrectVTOffset() {
    StoreIngestionTaskFactory.Builder builder = mock(StoreIngestionTaskFactory.Builder.class);
    doReturn(new ReferenceCounted<>(new DeepCopyStorageEngine(this.mockAbstractStorageEngine), se -> {}))
        .when(this.mockStorageService)
        .getRefCountedStorageEngine(anyString());
    VeniceServerConfig veniceServerConfig = mock(VeniceServerConfig.class);
    doReturn(VeniceProperties.empty()).when(veniceServerConfig).getClusterProperties();
    doReturn(VeniceProperties.empty()).when(veniceServerConfig).getKafkaConsumerConfigsForLocalConsumption();
    doReturn(VeniceProperties.empty()).when(veniceServerConfig).getKafkaConsumerConfigsForRemoteConsumption();
    doReturn(Object2IntMaps.emptyMap()).when(veniceServerConfig).getKafkaClusterUrlToIdMap();
    doReturn(-1).when(veniceServerConfig).getIdleIngestionTaskCleanupIntervalInSeconds();
    doReturn(veniceServerConfig).when(builder).getServerConfig();
    doReturn(mock(ReadOnlyStoreRepository.class)).when(builder).getMetadataRepo();
    doReturn(mock(ReadOnlySchemaRepository.class)).when(builder).getSchemaRepo();
    doReturn(mock(AggKafkaConsumerService.class)).when(builder).getAggKafkaConsumerService();
    doReturn(mockAggStoreIngestionStats).when(builder).getIngestionStats();
    doReturn(pubSubContext).when(builder).getPubSubContext();

    Version version = mock(Version.class);
    doReturn(1).when(version).getPartitionCount();
    doReturn("store").when(version).getStoreName();
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
            this.mockStorageService,
            builder,
            store,
            version,
            mock(Properties.class),
            mock(BooleanSupplier.class),
            storeConfig,
            -1,
            false,
            Optional.empty(),
            null,
            null));

    OffsetRecord offsetRecord = mock(OffsetRecord.class);
    PubSubTopic versionTopic = pubSubTopicRepository.getTopic(versionTopicName);
    doReturn(versionTopic).when(offsetRecord).getLeaderTopic(any());
    PartitionConsumptionState partitionConsumptionState = new PartitionConsumptionState(
        new PubSubTopicPartitionImpl(versionTopic, 0),
        offsetRecord,
        pubSubContext,
        false,
        Schema.create(Schema.Type.STRING));
    PubSubPosition localVersionTopicOffset = InMemoryPubSubPosition.of(100L);
    PubSubPosition remoteVersionTopicOffset = InMemoryPubSubPosition.of(200L);
    partitionConsumptionState.setLatestProcessedVtPosition(localVersionTopicOffset);
    partitionConsumptionState.setLatestProcessedRemoteVtPosition(remoteVersionTopicOffset);

    // Run the actual codes inside function "startConsumingAsLeader"
    doCallRealMethod().when(leaderFollowerStoreIngestionTask).startConsumingAsLeader(any());

    doCallRealMethod().when(leaderFollowerStoreIngestionTask)
        .resolveRtTopicPartitionWithPubSubBrokerAddress(any(), any(), any());
    doReturn(false).when(leaderFollowerStoreIngestionTask).shouldNewLeaderSwitchToRemoteConsumption(any());
    Set<String> kafkaServerSet = new HashSet<>();
    kafkaServerSet.add("localhost");
    doReturn(kafkaServerSet).when(leaderFollowerStoreIngestionTask).getConsumptionSourceKafkaAddress(any());

    // Test 1: if leader is not consuming remotely, leader must subscribe to the local VT offset
    partitionConsumptionState.setConsumeRemotely(false);
    leaderFollowerStoreIngestionTask.startConsumingAsLeader(partitionConsumptionState);
    verify(leaderFollowerStoreIngestionTask, times(1))
        .consumerSubscribe(any(), any(), eq(localVersionTopicOffset), anyString());

    // Test 2: if leader is consuming remotely, leader must subscribe to the remote VT offset
    partitionConsumptionState.setConsumeRemotely(true);
    leaderFollowerStoreIngestionTask.startConsumingAsLeader(partitionConsumptionState);
    verify(leaderFollowerStoreIngestionTask, times(1))
        .consumerSubscribe(any(), any(), eq(remoteVersionTopicOffset), anyString());
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

  private Consumer<MockInMemoryPartitionPosition> getObserver(
      List<InMemoryPubSubPosition> resubscriptionOffsetForVT,
      List<InMemoryPubSubPosition> resubscriptionOffsetForRT,
      CountDownLatch resubscriptionLatch) {
    return topicPartitionOffset -> {

      if (topicPartitionOffset == null || topicPartitionOffset.getPubSubPosition() == null) {
        LOGGER.info("Received null OffsetRecord!");
      } else {
        PubSubTopicPartition pubSubTopicPartition = topicPartitionOffset.getPubSubTopicPartition();
        InMemoryPubSubPosition position = topicPartitionOffset.getPubSubPosition();
        LOGGER.info(
            "Topic-partition: {}, position: {}",
            topicPartitionOffset.getPubSubTopicPartition(),
            topicPartitionOffset.getPubSubPosition());
        if (pubSubTopicPartition.getPubSubTopic().isVersionTopic() && resubscriptionOffsetForVT.contains(position)) {
          storeIngestionTaskUnderTest.setVersionRole(VersionRole.BACKUP);
          resubscriptionLatch.countDown();
          LOGGER.info(
              "Trigger re-subscription after consuming message for {} at position {} ",
              pubSubTopicPartition,
              position);
        } else if (pubSubTopicPartition.getPubSubTopic().isRealTime() && resubscriptionOffsetForRT.contains(position)) {
          storeIngestionTaskUnderTest.setVersionRole(VersionRole.BACKUP);
          resubscriptionLatch.countDown();
          LOGGER.info(
              "Trigger re-subscription after consuming message for {} at position {}.",
              pubSubTopicPartition,
              position);
        }
      }
    };
  }

  @Test(timeOut = 120000)
  public void testResubscribeAfterRoleChange() throws Exception {
    String realTimeTopicName = Utils.composeRealTimeTopic(storeNameWithoutVersionInfo);
    PubSubTopic realTimeTopic = pubSubTopicRepository.getTopic(realTimeTopicName);
    // Prepare both local and remote real-time topics
    inMemoryLocalKafkaBroker.createTopic(realTimeTopicName, PARTITION_COUNT);
    inMemoryRemoteKafkaBroker.createTopic(realTimeTopicName, PARTITION_COUNT);
    mockStorageMetadataService = new InMemoryStorageMetadataService();

    AbstractStoragePartition mockStoragePartition = mock(AbstractStoragePartition.class);
    doReturn(mockStoragePartition).when(mockAbstractStorageEngine).getPartitionOrThrow(anyInt());
    doReturn(new ReentrantReadWriteLock()).when(mockAbstractStorageEngine).getRWLockForPartitionOrThrow(anyInt());

    doReturn(putKeyFooReplicationMetadataWithValueSchemaIdBytesDefault).when(mockStoragePartition)
        .getReplicationMetadata(ByteBuffer.wrap(putKeyFoo));
    doReturn(deleteKeyFooReplicationMetadataWithValueSchemaIdBytes).when(mockStoragePartition)
        .getReplicationMetadata(ByteBuffer.wrap(deleteKeyFoo));
    doReturn(InMemoryPubSubPosition.of(0L)).when(mockTopicManager)
        .getLatestPositionCachedNonBlocking(any(PubSubTopicPartition.class));

    VeniceWriter vtWriter = getVeniceWriter(topic, new MockInMemoryProducerAdapter(inMemoryLocalKafkaBroker));
    VeniceWriter localRtWriter =
        getVeniceWriter(realTimeTopicName, new MockInMemoryProducerAdapter(inMemoryLocalKafkaBroker));
    VeniceWriter remoteRtWriter =
        getVeniceWriter(realTimeTopicName, new MockInMemoryProducerAdapter(inMemoryRemoteKafkaBroker));
    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(
        100,
        100,
        HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
        BufferReplayPolicy.REWIND_FROM_EOP);

    final int batchMessagesNum = 100;
    final List<InMemoryPubSubPosition> resubscriptionOffsetForLocalVT =
        Arrays.asList(InMemoryPubSubPosition.of(30L), InMemoryPubSubPosition.of(70L));
    final List<InMemoryPubSubPosition> resubscriptionOffsetForLocalRT =
        Collections.singletonList(InMemoryPubSubPosition.of(40L));
    final List<InMemoryPubSubPosition> resubscriptionOffsetForRemoteRT =
        Collections.singletonList(InMemoryPubSubPosition.of(50L));

    // Prepare resubscription number to be verified after ingestion.
    int totalResubscriptionTriggered = resubscriptionOffsetForLocalVT.size() + resubscriptionOffsetForLocalRT.size()
        + resubscriptionOffsetForRemoteRT.size();
    int totalLocalVtResubscriptionTriggered = resubscriptionOffsetForLocalVT.size();
    int totalLocalRtResubscriptionTriggered =
        resubscriptionOffsetForRemoteRT.size() + resubscriptionOffsetForLocalRT.size();
    int totalRemoteRtResubscriptionTriggered =
        resubscriptionOffsetForRemoteRT.size() + resubscriptionOffsetForLocalRT.size();

    // Create CountDownLatch for synchronization between observer threads and main test thread
    CountDownLatch resubscriptionLatch = new CountDownLatch(totalResubscriptionTriggered);

    vtWriter.broadcastStartOfPush(new HashMap<>());

    // Produce batchMessagesNum messages to local Venice version topic
    produceRecordsUsingSpecificWriter(localVeniceWriter, 0, batchMessagesNum, this::getNumberedKeyForPartitionBar);

    // Set two observers for both local and remote consumer thread, these observers will trigger resubscription by
    // setting
    // the version role to Backup when the offset reaches the specified value.
    Consumer<MockInMemoryPartitionPosition> localObserver =
        getObserver(resubscriptionOffsetForLocalVT, resubscriptionOffsetForLocalRT, resubscriptionLatch);
    Consumer<MockInMemoryPartitionPosition> remoteObserver =
        getObserver(Collections.emptyList(), resubscriptionOffsetForRemoteRT, resubscriptionLatch);
    PollStrategy localPollStrategy = new BlockingObserverPollStrategy(new RandomPollStrategy(false), localObserver);
    remotePollStrategy = Optional.of(new BlockingObserverPollStrategy(new RandomPollStrategy(false), remoteObserver));

    doReturn(PubSubSymbolicPosition.EARLIEST).when(mockTopicManager)
        .getPositionByTime(any(PubSubTopicPartition.class), anyLong());
    doReturn(PubSubSymbolicPosition.EARLIEST).when(mockTopicManagerRemote)
        .getPositionByTime(any(PubSubTopicPartition.class), anyLong());

    StoreIngestionTaskTestConfig config =
        new StoreIngestionTaskTestConfig(Utils.setOf(PARTITION_FOO, PARTITION_BAR), () -> {
          doReturn(vtWriter).when(mockWriterFactory).createVeniceWriter(any(VeniceWriterOptions.class));
          verify(mockLogNotifier, never()).completed(anyString(), anyInt(), any());
          List<CharSequence> kafkaBootstrapServers = new ArrayList<>();
          kafkaBootstrapServers.add(inMemoryLocalKafkaBroker.getPubSubBrokerAddress());
          kafkaBootstrapServers.add(inMemoryRemoteKafkaBroker.getPubSubBrokerAddress());

          // Verify ingestion of Venice version topic batchMessagesNum messages
          verify(mockAbstractStorageEngine, timeout(10000).times(batchMessagesNum))
              .put(eq(PARTITION_BAR), any(), (ByteBuffer) any());

          vtWriter.broadcastEndOfPush(new HashMap<>());
          vtWriter.broadcastTopicSwitch(
              kafkaBootstrapServers,
              Utils.composeRealTimeTopic(storeNameWithoutVersionInfo),
              System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(10),
              new HashMap<>());
          storeIngestionTaskUnderTest.promoteToLeader(
              fooTopicPartition,
              new LeaderFollowerPartitionStateModel.LeaderSessionIdChecker(1, new AtomicLong(1)));

          // Both Colo RT ingestion, avoid DCR collision intentionally. Each rt will be produced batchMessagesNum
          // messages.
          produceRecordsUsingSpecificWriter(localRtWriter, 0, batchMessagesNum, this::getNumberedKey);
          produceRecordsUsingSpecificWriter(remoteRtWriter, batchMessagesNum, batchMessagesNum, this::getNumberedKey);

          verify(mockAbstractStorageEngine, timeout(10000).times(batchMessagesNum * 2))
              .putWithReplicationMetadata(eq(PARTITION_FOO), any(), any(), any());

          // Wait for all resubscriptions to complete before verifying mock interactions
          try {
            assertTrue(
                resubscriptionLatch.await(30, TimeUnit.SECONDS),
                "Timed out waiting for all resubscriptions to complete");
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }

          // Use waitForNonDeterministicAssertion with atLeast() for all mock verifications
          // Use longer timeout (60s) since resubscribeForAllPartitions() is called asynchronously
          // by the SIT thread after setVersionRole() triggers, and there can be delays
          waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
            try {
              verify(storeIngestionTaskUnderTest, atLeast(totalResubscriptionTriggered)).resubscribeForAllPartitions();
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          });

          PubSubTopicPartition fooRtTopicPartition = new PubSubTopicPartitionImpl(realTimeTopic, PARTITION_FOO);

          waitForNonDeterministicAssertion(120, TimeUnit.SECONDS, () -> {
            // Verify unsubscribe calls
            verify(mockLocalKafkaConsumer, atLeast(totalLocalVtResubscriptionTriggered))
                .unSubscribe(eq(fooTopicPartition));
            verify(mockLocalKafkaConsumer, atLeast(totalLocalVtResubscriptionTriggered))
                .unSubscribe(eq(barTopicPartition));
            verify(mockLocalKafkaConsumer, atLeast(totalLocalRtResubscriptionTriggered))
                .unSubscribe(fooRtTopicPartition);
            verify(mockRemoteKafkaConsumer, atLeast(totalRemoteRtResubscriptionTriggered))
                .unSubscribe(fooRtTopicPartition);

            // Verify subscribe calls
            verify(mockLocalKafkaConsumer, atLeast(totalLocalVtResubscriptionTriggered))
                .subscribe(eq(fooTopicPartition), any(PubSubPosition.class));
            verify(mockLocalKafkaConsumer, atLeast(totalLocalRtResubscriptionTriggered))
                .subscribe(eq(fooRtTopicPartition), any(PubSubPosition.class));
            verify(mockRemoteKafkaConsumer, atLeast(totalRemoteRtResubscriptionTriggered))
                .subscribe(eq(fooRtTopicPartition), any(PubSubPosition.class));
          });
        }, AA_ON);
    /**
     * This test requires checking for every round of SIT thread run loop. To maintain the goal of the check, disable
     * the check interval feature here. We should consider how to make the check more relaxed while still checking the
     * critical part of the feature.
     */
    Map<String, Object> extraServerProperties = new HashMap<>();
    extraServerProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 3L);
    extraServerProperties.put(SERVER_RESUBSCRIPTION_CHECK_INTERVAL_IN_SECONDS, 0);
    config.setPollStrategy(localPollStrategy)
        .setHybridStoreConfig(Optional.of(hybridStoreConfig))
        .setExtraServerProperties(extraServerProperties);
    runTest(config);
  }

  public void testResubscribeForStaleVersion() throws Exception {
    // Set up the environment.
    StoreIngestionTaskFactory.Builder builder = mock(StoreIngestionTaskFactory.Builder.class);
    doReturn(new ReferenceCounted<>(new DeepCopyStorageEngine(this.mockAbstractStorageEngine), se -> {}))
        .when(this.mockStorageService)
        .getRefCountedStorageEngine(anyString());

    VeniceServerConfig veniceServerConfig = mock(VeniceServerConfig.class);
    doReturn(VeniceProperties.empty()).when(veniceServerConfig).getClusterProperties();
    doReturn(VeniceProperties.empty()).when(veniceServerConfig).getKafkaConsumerConfigsForLocalConsumption();
    doReturn(VeniceProperties.empty()).when(veniceServerConfig).getKafkaConsumerConfigsForRemoteConsumption();
    doReturn(Object2IntMaps.emptyMap()).when(veniceServerConfig).getKafkaClusterUrlToIdMap();
    doReturn(-1).when(veniceServerConfig).getIdleIngestionTaskCleanupIntervalInSeconds();
    doReturn(veniceServerConfig).when(builder).getServerConfig();
    doReturn(mock(ReadOnlyStoreRepository.class)).when(builder).getMetadataRepo();
    doReturn(mock(ReadOnlySchemaRepository.class)).when(builder).getSchemaRepo();
    doReturn(mock(AggKafkaConsumerService.class)).when(builder).getAggKafkaConsumerService();
    doReturn(mockAggStoreIngestionStats).when(builder).getIngestionStats();
    doReturn(pubSubContext).when(builder).getPubSubContext();

    // Prepare the meaningful store version
    Version version = mock(Version.class);
    doReturn(1).when(version).getPartitionCount();
    doReturn(null).when(version).getPartitionerConfig();
    doReturn(VersionStatus.ONLINE).when(version).getStatus();
    doReturn(true).when(version).isNativeReplicationEnabled();
    doReturn("localhost").when(version).getPushStreamSourceAddress();
    doReturn(true).when(version).isActiveActiveReplicationEnabled();

    String versionTopicName = "testStore_v1";
    doReturn(versionTopicName).when(version).kafkaTopicName();
    Store store = mock(Store.class);
    doReturn(version).when(store).getVersion(eq(1));
    doReturn(Version.parseStoreFromVersionTopic(versionTopicName)).when(store).getName();
    doReturn(Version.parseStoreFromVersionTopic(versionTopicName)).when(version).getStoreName();

    VeniceStoreVersionConfig storeConfig = mock(VeniceStoreVersionConfig.class);
    doReturn(Version.parseStoreFromVersionTopic(versionTopicName)).when(store).getName();
    doReturn(versionTopicName).when(storeConfig).getStoreVersionName();

    LeaderFollowerStoreIngestionTask ingestionTask = spy(
        new LeaderFollowerStoreIngestionTask(
            this.mockStorageService,
            builder,
            store,
            version,
            mock(Properties.class),
            mock(BooleanSupplier.class),
            storeConfig,
            -1,
            false,
            Optional.empty(),
            null,
            null));

    // Simulate the version has been deleted.
    ingestionTask.setVersionRole(VersionRole.BACKUP);
    doReturn(null).when(store).getVersion(eq(1));
    ingestionTask.refreshIngestionContextIfChanged(store);
    verify(ingestionTask, never()).resubscribeForAllPartitions();
    doReturn(Store.NON_EXISTING_VERSION).when(store).getCurrentVersion();
    ingestionTask.refreshIngestionContextIfChanged(store);
    verify(ingestionTask, never()).resubscribeForAllPartitions();
  }

  @Test
  public void testResubscribeForCompletedCurrentVersionPartition() throws InterruptedException {
    StoreIngestionTask storeIngestionTask = mock(StoreIngestionTask.class);
    doCallRealMethod().when(storeIngestionTask).resubscribeForCompletedCurrentVersionPartition();
    Map<Integer, PartitionConsumptionState> partitionConsumptionStateMap = new HashMap<>();
    doReturn(partitionConsumptionStateMap).when(storeIngestionTask).getPartitionConsumptionStateMap();
    PartitionConsumptionState pcs1 = mock(PartitionConsumptionState.class);
    doReturn(false).when(pcs1).isComplete();
    doReturn(false).when(pcs1).hasResubscribedAfterBootstrapAsCurrentVersion();
    PartitionConsumptionState pcs2 = mock(PartitionConsumptionState.class);
    doReturn(false).when(pcs2).isComplete();
    doReturn(true).when(pcs2).hasResubscribedAfterBootstrapAsCurrentVersion();
    PartitionConsumptionState pcs3 = mock(PartitionConsumptionState.class);
    doReturn(true).when(pcs3).isComplete();
    doReturn(false).when(pcs3).hasResubscribedAfterBootstrapAsCurrentVersion();
    PartitionConsumptionState pcs4 = mock(PartitionConsumptionState.class);
    doReturn(true).when(pcs4).isComplete();
    doReturn(true).when(pcs4).hasResubscribedAfterBootstrapAsCurrentVersion();
    partitionConsumptionStateMap.put(0, pcs1);
    partitionConsumptionStateMap.put(1, pcs2);
    partitionConsumptionStateMap.put(2, pcs3);
    partitionConsumptionStateMap.put(3, pcs4);

    // Non-current version don't do resubscription.
    doReturn(false).when(storeIngestionTask).isCurrentVersion();
    storeIngestionTask.resubscribeForCompletedCurrentVersionPartition();
    verify(storeIngestionTask, never()).resubscribe(any());

    // Only completed replica will flip once.
    doReturn(true).when(storeIngestionTask).isCurrentVersion();
    storeIngestionTask.resubscribeForCompletedCurrentVersionPartition();
    verify(storeIngestionTask, times(1)).resubscribe(any());
    verify(pcs3, times(1)).setHasResubscribedAfterBootstrapAsCurrentVersion(eq(true));

  }

  @Test(dataProvider = "aaConfigProvider")
  public void testWrappedInterruptExceptionDuringGracefulShutdown(AAConfig aaConfig) throws Exception {
    hybridStoreConfig = Optional.of(
        new HybridStoreConfigImpl(
            10,
            20,
            HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
            BufferReplayPolicy.REWIND_FROM_EOP));
    VeniceException veniceException = new VeniceException("Wrapped interruptedException", new InterruptedException());

    StoreIngestionTaskTestConfig config = new StoreIngestionTaskTestConfig(Utils.setOf(PARTITION_FOO), () -> {
      verify(mockLogNotifier, timeout(TEST_TIMEOUT_MS)).restarted(eq(topic), eq(PARTITION_FOO), any());
      storeIngestionTaskUnderTest.close();
      verify(aggKafkaConsumerService, timeout(TEST_TIMEOUT_MS)).unsubscribeConsumerFor(eq(pubSubTopic), any());
    }, aaConfig);
    config.setBeforeStartingConsumption(() -> {
      doReturn(getOffsetRecord(InMemoryPubSubPosition.of(1L), true, pubSubContext)).when(mockStorageMetadataService)
          .getLastOffset(topic, PARTITION_FOO, pubSubContext);
      doThrow(veniceException).when(aggKafkaConsumerService).unsubscribeConsumerFor(eq(pubSubTopic), any());
    });
    runTest(config);
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
            BufferReplayPolicy.REWIND_FROM_EOP));

    InMemoryPubSubPosition p0 = InMemoryPubSubPosition.of(0L);
    InMemoryPubSubPosition p2 = InMemoryPubSubPosition.of(2L);
    StoreIngestionTaskTestConfig testConfig = new StoreIngestionTaskTestConfig(Utils.setOf(PARTITION_FOO), () -> {
      // Verify it retrieves the offset from the OffSet Manager
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS)).getLastOffset(topic, PARTITION_FOO, pubSubContext);

      // Verify offsetRecord hasn't been synced yet
      PartitionConsumptionState pcs = storeIngestionTaskUnderTest.getPartitionConsumptionState(PARTITION_FOO);
      if (pcs == null) {
        LOGGER.info(
            "pcs for PARTITION_FOO is null, which is an indication that it was never synced before, so we carry on.");
      } else {
        // If the pcs is non-null, then we perform additional checks to ensure that it was not synced
        Assert.assertEquals(
            pcs.getLatestProcessedVtPosition(),
            p0,
            "pcs.getLatestProcessedLocalVersionTopicOffset() for PARTITION_FOO is expected to be zero!");
        OffsetRecord offsetRecord = pcs.getOffsetRecord();
        assertNotNull(offsetRecord);
        Assert.assertEquals(
            offsetRecord.getCheckpointedLocalVtPosition(),
            p0,
            "offsetRecord.getCheckpointedLocalVtPosition() for PARTITION_FOO is expected to be zero!");
      }

      // verify 2 messages were processed
      verify(mockStoreIngestionStats, timeout(TEST_TIMEOUT_MS).times(2)).recordTotalRecordsConsumed();
      pcs = storeIngestionTaskUnderTest.getPartitionConsumptionState(PARTITION_FOO); // We re-fetch in case it was null
      assertNotNull(pcs, "pcs for PARTITION_FOO is null!");
      OffsetRecord offsetRecord = pcs.getOffsetRecord();
      assertNotNull(offsetRecord);
      Assert.assertEquals(pcs.getLatestProcessedVtPosition(), p2); // PCS updated
      // offsetRecord hasn't been updated yet
      Assert.assertEquals(offsetRecord.getCheckpointedLocalVtPosition(), p0);
      storeIngestionTaskUnderTest.close();

      // Verify the OffsetRecord is synced up with pcs and get persisted only once during shutdown
      verify(mockStorageMetadataService, timeout(TEST_TIMEOUT_MS).times(1)).put(eq(topic), eq(PARTITION_FOO), any());
      Assert.assertEquals(
          offsetRecord.getCheckpointedLocalVtPosition(),
          p2,
          "offsetRecord.getCheckpointedLocalVtPosition() for PARTITION_FOO is expected to be 2!");

      // Verify that the underlying storage engine sync function is invoked.
      verify(mockAbstractStorageEngine, timeout(TEST_TIMEOUT_MS).times(1)).sync(eq(PARTITION_FOO));
    }, aaConfig);

    testConfig.setHybridStoreConfig(this.hybridStoreConfig).setBeforeStartingConsumption(() -> {
      doReturn(getOffsetRecord(InMemoryPubSubPosition.of(0L), true, pubSubContext)).when(mockStorageMetadataService)
          .getLastOffset(topic, PARTITION_FOO, pubSubContext);
    }).setStoreVersionConfigOverride(configOverride -> {
      // set very high threshold so offsetRecord isn't be synced during regular consumption
      doReturn(100_000L).when(configOverride).getDatabaseSyncBytesIntervalForTransactionalMode();
    });
    runTest(testConfig);
    Assert.assertEquals(mockNotifierError.size(), 0);
  }

  @Test(dataProvider = "aaConfigProvider", timeOut = 60_000)
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
    DefaultPubSubMessage pubSubMessage = new ImmutablePubSubMessage(
        kafkaKey,
        kafkaMessageEnvelope,
        new PubSubTopicPartitionImpl(pubSubTopic, PARTITION_FOO),
        mockedPubSubPosition,
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
          () -> assertFalse(storeIngestionTaskUnderTest.getPartitionConsumptionStateMap().isEmpty()));

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
    verify(stats, timeout(10000).times(wantedInvocationsForAllOtherStats)).recordTotalRecordsConsumed();
    verify(stats, timeout(10000).times(wantedInvocationsForAllOtherStats)).recordTotalBytesConsumed(anyLong());
    verify(mockVersionedStorageIngestionStats, timeout(10000).times(wantedInvocationsForAllOtherStats))
        .recordRecordsConsumed(anyString(), anyInt());
    verify(mockVersionedStorageIngestionStats, timeout(10000).times(wantedInvocationsForAllOtherStats))
        .recordBytesConsumed(anyString(), anyInt(), anyLong());

  }

  @Test
  public void testShouldPersistRecord() throws Exception {
    DefaultPubSubMessage pubSubMessage = new ImmutablePubSubMessage(
        null,
        null,
        new PubSubTopicPartitionImpl(pubSubTopic, 1),
        mockedPubSubPosition,
        0,
        0);

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

    StoreIngestionTaskTestConfig testConfig =
        new StoreIngestionTaskTestConfig(Collections.singleton(PARTITION_FOO), () -> {
          PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateSupplier.get();
          assertFalse(storeIngestionTaskUnderTest.shouldPersistRecord(pubSubMessage, partitionConsumptionState));
        }, AA_OFF);

    testConfig.setHybridStoreConfig(this.hybridStoreConfig).setExtraServerProperties(serverProperties);

    runTest(testConfig);

    // runTest(new RandomPollStrategy(), Collections.singleton(PARTITION_FOO), () -> {}, () -> {
    // PartitionConsumptionState partitionConsumptionState = partitionConsumptionStateSupplier.get();
    // assertFalse(storeIngestionTaskUnderTest.shouldPersistRecord(pubSubMessage, partitionConsumptionState));
    // }, this.hybridStoreConfig, false, Optional.empty(), AA_OFF, serverProperties);

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

      DefaultPubSubMessage pubSubMessage2 = new ImmutablePubSubMessage(
          null,
          null,
          new PubSubTopicPartitionImpl(wrongTopic, 1),
          mockedPubSubPosition,
          0,
          0);

      when(partitionConsumptionState.getLeaderFollowerState()).thenReturn(STANDBY);
      assertFalse(storeIngestionTaskUnderTest.shouldPersistRecord(pubSubMessage2, partitionConsumptionState));
    }, AA_OFF);
  }

  public void testIngestionTaskForCurrentVersionResetExceptionReportError() throws Exception {
    doThrow(new VeniceException("mock exception")).doNothing()
        .when(mockAbstractStorageEngine)
        .put(anyInt(), any(), (ByteBuffer) any());
    isCurrentVersion = () -> true;

    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID, PUT_KEY_FOO_TIMESTAMP, null).get();
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    doThrow(new HelixException("a")).when(zkHelixAdmin)
        .setPartitionsToError(anyString(), anyString(), anyString(), anyList());
    StoreIngestionTaskTestConfig testConfig =
        new StoreIngestionTaskTestConfig(Collections.singleton(PARTITION_FOO), () -> {
          verify(mockAbstractStorageEngine, timeout(10000).times(1)).put(eq(PARTITION_FOO), any(), (ByteBuffer) any());
          verify(zkHelixAdmin, timeout(1000).atLeast(1))
              .setPartitionsToError(anyString(), anyString(), anyString(), anyList());
          verify(storeIngestionTaskUnderTest, timeout(TEST_TIMEOUT_MS).times(1))
              .reportIngestionNotifier(any(PartitionConsumptionState.class), any(VeniceException.class));
        }, AA_OFF);
    testConfig.setStoreVersionConfigOverride(configOverride -> {
      doReturn(true).when(configOverride).isResetErrorReplicaEnabled();
    });
    runTest(testConfig);
  }

  @Test
  public void testIngestionTaskForCurrentVersionResetException() throws Exception {
    doThrow(new VeniceException("mock exception")).doNothing()
        .when(mockAbstractStorageEngine)
        .put(anyInt(), any(), (ByteBuffer) any());
    isCurrentVersion = () -> true;

    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID, PUT_KEY_FOO_TIMESTAMP, null).get();
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());
    doNothing().when(zkHelixAdmin).setPartitionsToError(anyString(), anyString(), anyString(), anyList());
    StoreIngestionTaskTestConfig testConfig =
        new StoreIngestionTaskTestConfig(Collections.singleton(PARTITION_FOO), () -> {
          verify(mockAbstractStorageEngine, timeout(10000).times(1)).put(eq(PARTITION_FOO), any(), (ByteBuffer) any());
          verify(zkHelixAdmin, timeout(1000).atLeast(1))
              .setPartitionsToError(anyString(), anyString(), anyString(), anyList());
        }, AA_OFF);
    testConfig.setStoreVersionConfigOverride(configOverride -> {
      doReturn(true).when(configOverride).isResetErrorReplicaEnabled();
    });
    runTest(testConfig);
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
    doReturn("store").when(version).getStoreName();
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
        true,
        null,
        null,
        this.mockStorageService).build();
    doReturn(Version.parseStoreFromVersionTopic(topic)).when(store).getName();
    storeIngestionTaskUnderTest = ingestionTaskFactory.getNewIngestionTask(
        this.mockStorageService,
        store,
        version,
        new Properties(),
        isCurrentVersion,
        storeConfig,
        1,
        false,
        Optional.empty(),
        null,
        null);
    OffsetRecord offsetRecord = mock(OffsetRecord.class);
    doReturn(pubSubTopic).when(offsetRecord).getLeaderTopic(any());
    PartitionConsumptionState partitionConsumptionState = new PartitionConsumptionState(
        new PubSubTopicPartitionImpl(pubSubTopic, 0),
        offsetRecord,
        pubSubContext,
        false,
        Schema.create(Schema.Type.STRING));
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
    doCallRealMethod().when(storeIngestionTask).isIdleOverThreshold();

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
    doReturn(storeName).when(mockVersion).getStoreName();
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
    doReturn(-1).when(mockVeniceServerConfig).getIdleIngestionTaskCleanupIntervalInSeconds();
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

    doReturn(Lazy.of(() -> veniceWriter)).when(pcs).getVeniceWriterLazyRef();
    doReturn(new ReferenceCounted<>(new DeepCopyStorageEngine(this.mockAbstractStorageEngine), se -> {}))
        .when(this.mockStorageService)
        .getRefCountedStorageEngine(anyString());

    StoreIngestionTaskFactory ingestionTaskFactory = TestUtils.getStoreIngestionTaskBuilder(storeName)
        .setStorageMetadataService(mockStorageMetadataService)
        .setMetadataRepository(mockReadOnlyStoreRepository)
        .setPubSubContext(pubSubContext)
        .setServerConfig(mockVeniceServerConfig)
        .setVeniceWriterFactory(veniceWriterFactory)
        .build();
    LeaderFollowerStoreIngestionTask ingestionTask =
        (LeaderFollowerStoreIngestionTask) ingestionTaskFactory.getNewIngestionTask(
            this.mockStorageService,
            mockStore,
            mockVersion,
            mockKafkaConsumerProperties,
            () -> true,
            mockVeniceStoreVersionConfig,
            0,
            false,
            Optional.empty(),
            null,
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
  public void testMaybeSendIngestionHeartbeatWithHBSuccessOrFailure() {
    String storeName = Utils.getUniqueString("store");
    Store mockStore = mock(Store.class);
    doReturn(storeName).when(mockStore).getName();
    String versionTopic = Version.composeKafkaTopic(storeName, 1);
    VeniceStoreVersionConfig mockVeniceStoreVersionConfig = mock(VeniceStoreVersionConfig.class);
    doReturn(versionTopic).when(mockVeniceStoreVersionConfig).getStoreVersionName();
    Version mockVersion = mock(Version.class);
    doReturn(storeName).when(mockVersion).getStoreName();
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
    doReturn(true).when(mockVersion).isSeparateRealTimeTopicEnabled();
    VeniceServerConfig mockVeniceServerConfig = mock(VeniceServerConfig.class);
    VeniceProperties mockVeniceProperties = mock(VeniceProperties.class);
    doReturn(true).when(mockVeniceProperties).isEmpty();
    doReturn(mockVeniceProperties).when(mockVeniceServerConfig).getKafkaConsumerConfigsForLocalConsumption();
    doReturn(Object2IntMaps.emptyMap()).when(mockVeniceServerConfig).getKafkaClusterUrlToIdMap();
    doReturn(Int2ObjectMaps.emptyMap()).when(mockVeniceServerConfig).getKafkaClusterIdToUrlMap();
    doReturn(-1).when(mockVeniceServerConfig).getIdleIngestionTaskCleanupIntervalInSeconds();
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

    PubSubTopic pubsubTopicSepRT = mock(PubSubTopic.class);
    doReturn(true).when(pubsubTopicSepRT).isRealTime();

    VeniceWriter veniceWriter = mock(VeniceWriter.class);
    VeniceWriterFactory veniceWriterFactory = mock(VeniceWriterFactory.class);
    doReturn(veniceWriter).when(veniceWriterFactory).createVeniceWriter(any());

    doReturn(Lazy.of(() -> veniceWriter)).when(pcs0).getVeniceWriterLazyRef();
    doReturn(Lazy.of(() -> veniceWriter)).when(pcs1).getVeniceWriterLazyRef();
    doReturn(new ReferenceCounted<>(new DeepCopyStorageEngine(this.mockAbstractStorageEngine), se -> {}))
        .when(this.mockStorageService)
        .getRefCountedStorageEngine(anyString());

    StoreIngestionTaskFactory ingestionTaskFactory = TestUtils.getStoreIngestionTaskBuilder(storeName)
        .setStorageMetadataService(mockStorageMetadataService)
        .setMetadataRepository(mockReadOnlyStoreRepository)
        .setPubSubContext(pubSubContext)
        .setServerConfig(mockVeniceServerConfig)
        .setVeniceWriterFactory(veniceWriterFactory)
        .build();
    LeaderFollowerStoreIngestionTask ingestionTask =
        (LeaderFollowerStoreIngestionTask) ingestionTaskFactory.getNewIngestionTask(
            this.mockStorageService,
            mockStore,
            mockVersion,
            mockKafkaConsumerProperties,
            () -> true,
            mockVeniceStoreVersionConfig,
            0,
            false,
            Optional.empty(),
            null,
            null);

    ingestionTask.setPartitionConsumptionState(0, pcs0);
    ingestionTask.setPartitionConsumptionState(1, pcs1);

    CompletableFuture heartBeatFuture = new CompletableFuture();
    heartBeatFuture.complete(null);
    PubSubTopicPartition pubSubTopicPartition0 = new PubSubTopicPartitionImpl(pubsubTopic, 0);
    PubSubTopicPartition pubSubTopicPartition1 = new PubSubTopicPartitionImpl(pubsubTopic, 1);
    PubSubTopic sepRTtopic = pubSubTopicRepository.getTopic(Utils.getSeparateRealTimeTopicName(storeName));
    PubSubTopicPartition pubSubTopicPartition1sep = new PubSubTopicPartitionImpl(sepRTtopic, 1);

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

    // 1 partition throws exception
    doReturn(heartBeatFuture).when(veniceWriter)
        .sendHeartbeat(eq(pubSubTopicPartition0), any(), any(), anyBoolean(), any(), anyLong());
    doAnswer(invocation -> {
      throw new Exception("mock exception");
    }).when(veniceWriter).sendHeartbeat(eq(pubSubTopicPartition1sep), any(), any(), anyBoolean(), any(), anyLong());
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
  public void testSITRecordTransformer(AAConfig aaConfig) throws Exception {
    KafkaKey kafkaKey = new KafkaKey(MessageType.PUT, putKeyFoo);
    KafkaMessageEnvelope kafkaMessageEnvelope = new KafkaMessageEnvelope();
    kafkaMessageEnvelope.messageType = MessageType.PUT.getValue();
    Put put = new Put();

    put.putValue = ByteBuffer.wrap(putValue);
    put.replicationMetadataPayload = ByteBuffer.allocate(10);
    kafkaMessageEnvelope.payloadUnion = put;
    kafkaMessageEnvelope.producerMetadata = new ProducerMetadata();
    DefaultPubSubMessage pubSubMessage = new ImmutablePubSubMessage(
        kafkaKey,
        kafkaMessageEnvelope,
        new PubSubTopicPartitionImpl(pubSubTopic, PARTITION_FOO),
        mockedPubSubPosition,
        0,
        0);

    LeaderProducedRecordContext leaderProducedRecordContext = mock(LeaderProducedRecordContext.class);
    when(leaderProducedRecordContext.getMessageType()).thenReturn(MessageType.PUT);
    when(leaderProducedRecordContext.getValueUnion()).thenReturn(put);
    when(leaderProducedRecordContext.getKeyBytes()).thenReturn(putKeyFoo);
    when(leaderProducedRecordContext.getConsumedPosition()).thenReturn(mockedPubSubPosition);

    StoreIngestionTaskTestConfig config = new StoreIngestionTaskTestConfig(Collections.singleton(PARTITION_FOO), () -> {
      TestUtils.waitForNonDeterministicAssertion(
          5,
          TimeUnit.SECONDS,
          () -> assertFalse(storeIngestionTaskUnderTest.getPartitionConsumptionStateMap().isEmpty()));

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
    }, aaConfig);

    config.setRecordTransformerConfig(buildRecordTransformerConfig(true));
    runTest(config);

    // Metrics that should have been recorded
    verify(mockDaVinciRecordTransformerStats, atLeastOnce())
        .recordPutLatency(eq(storeNameWithoutVersionInfo), anyInt(), anyDouble(), anyLong());

    // Metrics that shouldn't have been recorded
    verify(mockDaVinciRecordTransformerStats, never())
        .recordPutError(eq(storeNameWithoutVersionInfo), anyInt(), anyLong());
    verify(mockDaVinciRecordTransformerStats, never())
        .recordDeleteError(eq(storeNameWithoutVersionInfo), anyInt(), anyLong());
    verify(mockDaVinciRecordTransformerStats, never())
        .recordDeleteLatency(eq(storeNameWithoutVersionInfo), anyInt(), anyDouble(), anyLong());
  }

  @Test(dataProvider = "aaConfigProvider")
  public void testSITRecordTransformerUndefinedOutputValueClassAndSchema(AAConfig aaConfig) throws Exception {
    KafkaKey kafkaKey = new KafkaKey(MessageType.PUT, putKeyFoo);
    KafkaMessageEnvelope kafkaMessageEnvelope = new KafkaMessageEnvelope();
    kafkaMessageEnvelope.messageType = MessageType.PUT.getValue();
    Put put = new Put();

    put.putValue = ByteBuffer.wrap(putValue);
    put.replicationMetadataPayload = ByteBuffer.allocate(10);
    kafkaMessageEnvelope.payloadUnion = put;
    kafkaMessageEnvelope.producerMetadata = new ProducerMetadata();
    DefaultPubSubMessage pubSubMessage = new ImmutablePubSubMessage(
        kafkaKey,
        kafkaMessageEnvelope,
        new PubSubTopicPartitionImpl(pubSubTopic, PARTITION_FOO),
        mockedPubSubPosition,
        0,
        0);

    LeaderProducedRecordContext leaderProducedRecordContext = mock(LeaderProducedRecordContext.class);
    when(leaderProducedRecordContext.getMessageType()).thenReturn(MessageType.PUT);
    when(leaderProducedRecordContext.getValueUnion()).thenReturn(put);
    when(leaderProducedRecordContext.getKeyBytes()).thenReturn(putKeyFoo);

    Schema myKeySchema = Schema.create(Schema.Type.INT);
    SchemaEntry keySchemaEntry = mock(SchemaEntry.class);
    when(keySchemaEntry.getSchema()).thenReturn(myKeySchema);
    when(mockSchemaRepo.getKeySchema(storeNameWithoutVersionInfo)).thenReturn(keySchemaEntry);

    Schema myValueSchema = Schema.create(Schema.Type.STRING);
    SchemaEntry valueSchemaEntry = mock(SchemaEntry.class);
    when(valueSchemaEntry.getSchema()).thenReturn(myValueSchema);
    when(mockSchemaRepo.getValueSchema(eq(storeNameWithoutVersionInfo), anyInt())).thenReturn(valueSchemaEntry);
    when(mockSchemaRepo.getSupersetOrLatestValueSchema(eq(storeNameWithoutVersionInfo))).thenReturn(valueSchemaEntry);

    StoreIngestionTaskTestConfig config = new StoreIngestionTaskTestConfig(Collections.singleton(PARTITION_FOO), () -> {
      TestUtils.waitForNonDeterministicAssertion(
          5,
          TimeUnit.SECONDS,
          () -> assertFalse(storeIngestionTaskUnderTest.getPartitionConsumptionStateMap().isEmpty()));

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
    }, aaConfig);

    DaVinciRecordTransformerConfig recordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestStringRecordTransformer::new)
            .build();
    config.setRecordTransformerConfig(recordTransformerConfig);

    runTest(config);

    // Transformer put and delete error should never be recorded
    verify(mockDaVinciRecordTransformerStats, never())
        .recordPutError(eq(storeNameWithoutVersionInfo), anyInt(), anyLong());
    verify(mockDaVinciRecordTransformerStats, never())
        .recordDeleteError(eq(storeNameWithoutVersionInfo), anyInt(), anyLong());
  }

  // Test to throw type error when performing record transformation with incompatible types
  @Test(dataProvider = "aaConfigProvider")
  public void testSITRecordTransformerError(AAConfig aaConfig) throws Exception {
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
    DefaultPubSubMessage pubSubMessage = new ImmutablePubSubMessage(
        kafkaKey,
        kafkaMessageEnvelope,
        new PubSubTopicPartitionImpl(pubSubTopic, PARTITION_FOO),
        mockedPubSubPosition,
        0,
        0);

    LeaderProducedRecordContext leaderProducedRecordContext = mock(LeaderProducedRecordContext.class);
    when(leaderProducedRecordContext.getMessageType()).thenReturn(MessageType.PUT);
    when(leaderProducedRecordContext.getValueUnion()).thenReturn(put);
    when(leaderProducedRecordContext.getKeyBytes()).thenReturn(keyBytes);

    Schema myKeySchema = Schema.create(Schema.Type.INT);
    SchemaEntry keySchemaEntry = mock(SchemaEntry.class);
    when(keySchemaEntry.getSchema()).thenReturn(myKeySchema);
    when(mockSchemaRepo.getKeySchema(storeNameWithoutVersionInfo)).thenReturn(keySchemaEntry);

    Schema myValueSchema = Schema.create(Schema.Type.INT);
    SchemaEntry valueSchemaEntry = mock(SchemaEntry.class);
    when(valueSchemaEntry.getSchema()).thenReturn(myValueSchema);
    when(mockSchemaRepo.getValueSchema(eq(storeNameWithoutVersionInfo), anyInt())).thenReturn(valueSchemaEntry);
    when(mockSchemaRepo.getSupersetOrLatestValueSchema(eq(storeNameWithoutVersionInfo))).thenReturn(valueSchemaEntry);

    StoreIngestionTaskTestConfig config = new StoreIngestionTaskTestConfig(Collections.singleton(PARTITION_FOO), () -> {
      TestUtils.waitForNonDeterministicAssertion(
          5,
          TimeUnit.SECONDS,
          () -> assertFalse(storeIngestionTaskUnderTest.getPartitionConsumptionStateMap().isEmpty()));

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
      // Verify transformer put error was recorded
      verify(mockDaVinciRecordTransformerStats, timeout(1000))
          .recordPutError(eq(storeNameWithoutVersionInfo), anyInt(), anyLong());
    }, aaConfig);

    DaVinciRecordTransformerConfig recordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestStringRecordTransformer::new)
            .build();
    config.setRecordTransformerConfig(recordTransformerConfig);
    runTest(config);
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
    List<DefaultPubSubMessage> messages = new ArrayList<>(numChunks + 1); // +
                                                                          // manifest
    for (int i = 0; i < numChunks; i++) {
      messages.add(ChunkingTestUtils.createChunkedRecord(putKeyFoo, 1, 1, i, 0, tp));
    }
    DefaultPubSubMessage manifestMessage =
        ChunkingTestUtils.createChunkValueManifestRecord(putKeyFoo, messages.get(0), numChunks, tp);
    messages.add(manifestMessage);

    StoreIngestionTaskTestConfig testConfig =
        new StoreIngestionTaskTestConfig(Collections.singleton(PARTITION_FOO), () -> {
          TestUtils.waitForNonDeterministicAssertion(
              5,
              TimeUnit.SECONDS,
              () -> assertFalse(storeIngestionTaskUnderTest.getPartitionConsumptionStateMap().isEmpty()));

          for (DefaultPubSubMessage message: messages) {
            try {
              Put put = (Put) message.getValue().getPayloadUnion();
              if (put.schemaId == AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion()) {
                put.schemaId = testSchemaId; // set manifest schemaId to testSchemaId to see if metrics are still
                                             // recorded
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
        }, aaConfig);

    testConfig.setPollStrategy(new RandomPollStrategy())
        .setHybridStoreConfig(hybridStoreConfig)
        .setChunkingEnabled(true)
        .setRmdChunkingEnabled(rmdState == RmdState.CHUNKED);

    runTest(testConfig);
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
    HybridStoreConfigImpl hybridStoreConfig1 =
        new HybridStoreConfigImpl(100L, -1L, 100L, BufferReplayPolicy.REWIND_FROM_SOP);
    assertEquals(
        StoreIngestionTask
            .getOffsetToOnlineLagThresholdPerPartition(Optional.of(hybridStoreConfig1), storeName, partitionCount),
        -1L);

    // For current version, the partition-level offset lag threshold should be divided by partition count
    HybridStoreConfigImpl hybridStoreConfig2 =
        new HybridStoreConfigImpl(100L, 100L, 100L, BufferReplayPolicy.REWIND_FROM_SOP);
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
    PubSubPosition p10 = InMemoryPubSubPosition.of(10);
    PubSubPosition p11 = InMemoryPubSubPosition.of(11);
    LeaderFollowerStoreIngestionTask.checkAndHandleUpstreamOffsetRewind(
        mockTopicManager,
        mock(PubSubTopicPartition.class),
        mock(PartitionConsumptionState.class),
        mock(DefaultPubSubMessage.class),
        p11,
        p10,
        mock(LeaderFollowerStoreIngestionTask.class));

    // Rewind with batch only store, nothing would happen
    LeaderFollowerStoreIngestionTask mockTask1 = mock(LeaderFollowerStoreIngestionTask.class);
    when(mockTask1.isHybridMode()).thenReturn(false);
    AggVersionedDIVStats mockStats1 = mock(AggVersionedDIVStats.class);
    when(mockTask1.getVersionedDIVStats()).thenReturn(mockStats1);
    when(mockTask1.getStoreName()).thenReturn(storeName);
    when(mockTask1.getVersionNumber()).thenReturn(version);

    LeaderFollowerStoreIngestionTask.checkAndHandleUpstreamOffsetRewind(
        mockTopicManager,
        mock(PubSubTopicPartition.class),
        mock(PartitionConsumptionState.class),
        mock(DefaultPubSubMessage.class),
        p10,
        p11,
        mockTask1);
    verify(mockStats1).recordBenignLeaderOffsetRewind(storeName, version);

    // Benign rewind
    final long messageOffset = 10;
    PubSubPosition messagePosition = InMemoryPubSubPosition.of(messageOffset);
    KafkaKey key = new KafkaKey(MessageType.PUT, "test_key".getBytes());
    KafkaMessageEnvelope messsageEnvelope = new KafkaMessageEnvelope();
    LeaderMetadata leaderMetadata = new LeaderMetadata();
    leaderMetadata.upstreamPubSubPosition = InMemoryPubSubPosition.of(10L).toWireFormatBuffer();
    leaderMetadata.hostName = "new_leader";
    messsageEnvelope.leaderMetadataFooter = leaderMetadata;
    ProducerMetadata producerMetadata = new ProducerMetadata();
    producerMetadata.producerGUID = GuidUtils.getGuidFromCharSequence("new_leader_guid");
    messsageEnvelope.producerMetadata = producerMetadata;
    Put put = new Put();
    put.putValue = ByteBuffer.wrap("test_value_suffix".getBytes(), 0, 10); // With trailing suffix.
    put.schemaId = 1;
    messsageEnvelope.payloadUnion = put;
    DefaultPubSubMessage consumedRecord = new ImmutablePubSubMessage(
        key,
        messsageEnvelope,
        new PubSubTopicPartitionImpl(
            new TestPubSubTopic("test_store_v1", "test_store", PubSubTopicType.VERSION_TOPIC),
            1),
        messagePosition,
        -1,
        1000);
    StorageEngine mockStorageEngine2 = mock(AbstractStorageEngine.class);
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
    LeaderFollowerStoreIngestionTask.checkAndHandleUpstreamOffsetRewind(
        mockTopicManager,
        mock(PubSubTopicPartition.class),
        mockState2,
        consumedRecord,
        p10,
        p11,
        mockTask2);
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
        () -> LeaderFollowerStoreIngestionTask.checkAndHandleUpstreamOffsetRewind(
            mockTopicManager,
            mock(PubSubTopicPartition.class),
            mockState2,
            consumedRecord,
            p10,
            p11,
            mockTask2));
    assertTrue(
        exception.getMessage().contains("Failing the job because lossy rewind happens before receiving EndOfPush."));
    // Verify that the VT offset is also in the error message
    assertTrue(
        exception.getMessage().contains("received message at offset: " + messagePosition),
        "Actual message: " + exception.getMessage());
    verify(mockStats2).recordPotentiallyLossyLeaderOffsetRewind(storeName, version);
  }

  @Test
  public void testMeasureLagWithCallToPubSub() {
    final PubSubTopicPartition PARTITION_UNABLE_TO_GET_END_POSITION = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    final PubSubTopicPartition EMPTY_PARTITION = new PubSubTopicPartitionImpl(pubSubTopic, 1);
    final PubSubTopicPartition PARTITION_WITH_SOME_MESSAGES_IN_IT = new PubSubTopicPartitionImpl(pubSubTopic, 2);

    final long MESSAGE_COUNT = 10;
    final PubSubPosition INVALID_CURRENT_POSITION = null;
    final PubSubPosition CURRENT_OFFSET_NOTHING_CONSUMED = PubSubSymbolicPosition.EARLIEST;
    final InMemoryPubSubPosition CURRENT_POSITION_SOME_CONSUMED = InMemoryPubSubPosition.of(3L);
    final String PUB_SUB_SERVER_NAME = "blah";

    InMemoryPubSubPosition p0 = InMemoryPubSubPosition.of(0L);
    InMemoryPubSubPosition p10 = InMemoryPubSubPosition.of(MESSAGE_COUNT);
    doReturn(PubSubSymbolicPosition.LATEST).when(mockTopicManager)
        .getLatestPositionCached(PARTITION_UNABLE_TO_GET_END_POSITION);
    doReturn(p0).when(mockTopicManager).getLatestPositionCached(EMPTY_PARTITION);
    doReturn(p10).when(mockTopicManager).getLatestPositionCached(PARTITION_WITH_SOME_MESSAGES_IN_IT);

    assertEquals(
        StoreIngestionTask.measureLagWithCallToPubSub(
            PUB_SUB_SERVER_NAME,
            PARTITION_UNABLE_TO_GET_END_POSITION,
            CURRENT_OFFSET_NOTHING_CONSUMED,
            s -> mockTopicManager),
        Long.MAX_VALUE,
        "If unable to get the end position, we expect Long.MAX_VALUE (infinite lag).");

    assertEquals(
        StoreIngestionTask.measureLagWithCallToPubSub(
            PUB_SUB_SERVER_NAME,
            EMPTY_PARTITION,
            INVALID_CURRENT_POSITION,
            s -> mockTopicManager),
        Long.MAX_VALUE,
        "If the current position is invalid (less than -1), we expect Long.MAX_VALUE (infinite lag).");

    assertEquals(
        StoreIngestionTask.measureLagWithCallToPubSub(
            PUB_SUB_SERVER_NAME,
            EMPTY_PARTITION,
            CURRENT_OFFSET_NOTHING_CONSUMED,
            s -> mockTopicManager),
        0,
        "If the partition is empty, we expect no lag.");

    assertEquals(
        StoreIngestionTask.measureLagWithCallToPubSub(
            PUB_SUB_SERVER_NAME,
            PARTITION_WITH_SOME_MESSAGES_IN_IT,
            CURRENT_OFFSET_NOTHING_CONSUMED,
            s -> mockTopicManager),
        MESSAGE_COUNT,
        "If the partition has messages in it, but we consumed nothing, we expect lag to equal the message count.");

    assertEquals(
        StoreIngestionTask.measureLagWithCallToPubSub(
            PUB_SUB_SERVER_NAME,
            PARTITION_WITH_SOME_MESSAGES_IN_IT,
            CURRENT_POSITION_SOME_CONSUMED,
            s -> mockTopicManager),
        MESSAGE_COUNT - 1 - CURRENT_POSITION_SOME_CONSUMED.getInternalOffset(),
        "If the partition has messages in it, and we consumed some of them, we expect lag to equal the unconsumed message count.");
  }

  @Test
  public void testMeasureLagWithCallToPubSubWhenTopicDoesNotExist() {
    final PubSubTopicPartition partition = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    final InMemoryPubSubPosition endPosition = InMemoryPubSubPosition.of(10L);
    final String PUB_SUB_SERVER_NAME = "blah";

    TopicManager throwingTopicManager = mock(TopicManager.class);
    doReturn(endPosition).when(throwingTopicManager).getLatestPositionCached(partition);
    doThrow(new PubSubTopicDoesNotExistException("topic deleted")).when(throwingTopicManager)
        .diffPosition(any(), any(), any());
    doThrow(new PubSubTopicDoesNotExistException("topic deleted")).when(throwingTopicManager)
        .countRecordsUntil(any(), any());

    // Case 1: Non-EARLIEST position -> diffPosition throws
    assertEquals(
        StoreIngestionTask.measureLagWithCallToPubSub(
            PUB_SUB_SERVER_NAME,
            partition,
            InMemoryPubSubPosition.of(3L),
            s -> throwingTopicManager),
        Long.MAX_VALUE,
        "When diffPosition throws PubSubTopicDoesNotExistException, we expect Long.MAX_VALUE.");

    // Case 2: EARLIEST position -> countRecordsUntil throws
    assertEquals(
        StoreIngestionTask.measureLagWithCallToPubSub(
            PUB_SUB_SERVER_NAME,
            partition,
            PubSubSymbolicPosition.EARLIEST,
            s -> throwingTopicManager),
        Long.MAX_VALUE,
        "When countRecordsUntil throws PubSubTopicDoesNotExistException, we expect Long.MAX_VALUE.");
  }

  /**
   * When SIT encounters a corrupted {@link OffsetRecord} in {@link StoreIngestionTask#processCommonConsumerAction} and
   * {@link StorageMetadataService#getLastOffset} throws an exception due to a deserialization error,
   * {@link StoreIngestionTask#reportError(String, int, Exception)} from
   * {@link StoreIngestionTask#handleConsumerActionsError(Throwable, ConsumerAction, long)} should be called in
   * order to trigger a Helix state transition without waiting 24+ hours for the Helix state transition timeout.
   */
  @Test
  public void testHandleConsumerActionsError() throws Exception {
    final int p = 0; // partition number
    runTest(Collections.singleton(p), () -> {
      // This is an actual exception thrown when deserializing a corrupted OffsetRecord
      String msg = "Received Magic Byte '6' which is not supported by InternalAvroSpecificSerializer. "
          + "The only supported Magic Byte for this implementation is '24'.";
      Throwable e = new VeniceException(msg);
      CompletableFuture<Void> future = new CompletableFuture<>();// CompletableFuture(null);
      ConsumerAction action = mock(ConsumerAction.class);
      when(action.getPartition()).thenReturn(p);
      when(action.getFuture()).thenReturn(future);
      when(action.getAttemptsCount()).thenReturn(StoreIngestionTask.MAX_CONSUMER_ACTION_ATTEMPTS + 1);
      assertFalse(future.isCompletedExceptionally());
      storeIngestionTaskUnderTest.handleConsumerActionsError(e, action, 0L);
      verify(storeIngestionTaskUnderTest, times(1)).reportError(anyString(), eq(p), any(VeniceException.class));
      assertTrue(future.isCompletedExceptionally());
    }, AA_OFF);
  }

  @Test
  public void testGetTopicManager() throws Exception {
    String localKafkaBootstrapServer = inMemoryLocalKafkaBroker.getPubSubBrokerAddress();
    String remoteKafkaBootstrapServer = inMemoryRemoteKafkaBroker.getPubSubBrokerAddress();
    runTest(Collections.singleton(PARTITION_FOO), () -> {
      // local url returns the local manager
      Assert.assertSame(mockTopicManager, storeIngestionTaskUnderTest.getTopicManager(localKafkaBootstrapServer));
      Assert.assertSame(
          mockTopicManager,
          storeIngestionTaskUnderTest.getTopicManager(localKafkaBootstrapServer + "_sep"));
      // remote url returns the remote manager
      Assert
          .assertSame(mockTopicManagerRemote, storeIngestionTaskUnderTest.getTopicManager(remoteKafkaBootstrapServer));
      Assert.assertSame(
          mockTopicManagerRemote,
          storeIngestionTaskUnderTest.getTopicManager(remoteKafkaBootstrapServer + "_sep"));
    }, AA_OFF);
  }

  @Test
  public void testShouldProcessRecordForGlobalRtDivMessage() {
    // Set up the environment.
    doReturn(new ReferenceCounted<>(new DeepCopyStorageEngine(this.mockAbstractStorageEngine), se -> {}))
        .when(this.mockStorageService)
        .getRefCountedStorageEngine(anyString());
    StoreIngestionTaskFactory.Builder builder = mock(StoreIngestionTaskFactory.Builder.class);
    VeniceServerConfig veniceServerConfig = mock(VeniceServerConfig.class);
    doReturn(VeniceProperties.empty()).when(veniceServerConfig).getClusterProperties();
    doReturn(VeniceProperties.empty()).when(veniceServerConfig).getKafkaConsumerConfigsForLocalConsumption();
    doReturn(VeniceProperties.empty()).when(veniceServerConfig).getKafkaConsumerConfigsForRemoteConsumption();
    doReturn(Object2IntMaps.emptyMap()).when(veniceServerConfig).getKafkaClusterUrlToIdMap();
    doReturn(-1).when(veniceServerConfig).getIdleIngestionTaskCleanupIntervalInSeconds();
    doReturn(veniceServerConfig).when(builder).getServerConfig();
    doReturn(mock(ReadOnlyStoreRepository.class)).when(builder).getMetadataRepo();
    doReturn(mock(ReadOnlySchemaRepository.class)).when(builder).getSchemaRepo();
    doReturn(mock(AggKafkaConsumerService.class)).when(builder).getAggKafkaConsumerService();
    doReturn(mockAggStoreIngestionStats).when(builder).getIngestionStats();
    doReturn(pubSubContext).when(builder).getPubSubContext();

    Version version = mock(Version.class);
    doReturn(1).when(version).getPartitionCount();
    doReturn(null).when(version).getPartitionerConfig();
    doReturn(VersionStatus.ONLINE).when(version).getStatus();
    doReturn(true).when(version).isNativeReplicationEnabled();
    doReturn("localhost").when(version).getPushStreamSourceAddress();

    Store store = mock(Store.class);
    doReturn(version).when(store).getVersion(eq(1));

    String versionTopicName = "testStore_v1";
    String rtTopicName = "testStore_rt";
    VeniceStoreVersionConfig storeConfig = mock(VeniceStoreVersionConfig.class);
    doReturn(Version.parseStoreFromVersionTopic(versionTopicName)).when(store).getName();
    doReturn(versionTopicName).when(storeConfig).getStoreVersionName();

    LeaderFollowerStoreIngestionTask ingestionTask = spy(
        new LeaderFollowerStoreIngestionTask(
            this.mockStorageService,
            builder,
            store,
            version,
            mock(Properties.class),
            mock(BooleanSupplier.class),
            storeConfig,
            -1,
            false,
            Optional.empty(),
            null,
            null));

    // Create a DIV record.
    KafkaKey key = new KafkaKey(MessageType.GLOBAL_RT_DIV, "test_key".getBytes());
    KafkaMessageEnvelope value = new KafkaMessageEnvelope();
    value.payloadUnion = new Put();
    value.messageType = MessageType.PUT.getValue();
    PubSubTopic versionTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic("testStore", 1));
    PubSubTopic rtTopic = pubSubTopicRepository.getTopic(Utils.composeRealTimeTopic("testStore", 1));

    PubSubTopicPartition versionTopicPartition = new PubSubTopicPartitionImpl(versionTopic, PARTITION_FOO);
    PubSubTopicPartition rtPartition = new PubSubTopicPartitionImpl(rtTopic, PARTITION_FOO);
    DefaultPubSubMessage vtRecord =
        new ImmutablePubSubMessage(key, value, versionTopicPartition, InMemoryPubSubPosition.of(1), 0, 0);

    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    when(pcs.getLatestProcessedVtPosition()).thenReturn(PubSubSymbolicPosition.EARLIEST);
    when(pcs.getLeaderFollowerState()).thenReturn(LeaderFollowerStateType.LEADER);
    doReturn(true).when(pcs).consumeRemotely();
    doReturn(false).when(pcs).skipKafkaMessage();

    OffsetRecord offsetRecord = mock(OffsetRecord.class);
    doReturn(offsetRecord).when(pcs).getOffsetRecord();
    doReturn(pubSubTopicRepository.getTopic(versionTopicName)).when(offsetRecord).getLeaderTopic(any());
    ingestionTask.setPartitionConsumptionState(PARTITION_FOO, pcs);

    assertFalse(ingestionTask.shouldProcessRecord(vtRecord), "RT DIV From remote VT should not be processed");

    when(pcs.getLeaderFollowerState()).thenReturn(LeaderFollowerStateType.STANDBY);
    doReturn(false).when(pcs).consumeRemotely();
    assertTrue(ingestionTask.shouldProcessRecord(vtRecord), "RT DIV from local VT should be processed");

    doReturn(pubSubTopicRepository.getTopic(rtTopicName)).when(offsetRecord).getLeaderTopic(any());
    DefaultPubSubMessage rtRecord =
        new ImmutablePubSubMessage(key, value, rtPartition, InMemoryPubSubPosition.of(0), 0, 0);
    assertFalse(ingestionTask.shouldProcessRecord(rtRecord), "RT DIV from RT should not be processed");
  }

  /**
   * Tests that the {@link StoreIngestionTask#isGlobalRtDivEnabled} feature flag stops the drainer from syncing
   * OffsetRecord from {@link StoreIngestionTask#updateOffsetMetadataAndSyncOffset}, and the {@link ConsumptionTask}
   * should send Global RT DIV in {@link LeaderFollowerStoreIngestionTask#sendGlobalRtDivMessage} instead.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testShouldSendGlobalRtDiv(boolean isGlobalRtDivEnabled) {
    String brokerUrl = "localhost:1234";
    StoreIngestionTask storeIngestionTask = mock(StoreIngestionTask.class);
    doCallRealMethod().when(storeIngestionTask).shouldSyncOffset(any(), any(), any());
    doCallRealMethod().when(storeIngestionTask).shouldSendGlobalRtDiv(any(), any(), any());
    doReturn(isGlobalRtDivEnabled).when(storeIngestionTask).isGlobalRtDivEnabled();
    doReturn(1L).when(storeIngestionTask).getSyncBytesInterval(any()); // just needs to be greater than 0
    DefaultPubSubMessage message = mock(DefaultPubSubMessage.class);
    KafkaKey key = mock(KafkaKey.class);
    doReturn(false).when(key).isControlMessage();
    doReturn(key).when(message).getKey();
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    doReturn(100L).when(pcs).getProcessedRecordSizeSinceLastSync(); // just needs to be greater than syncBytesInterval
    VeniceConcurrentHashMap<String, Long> lastProcessedMap = new VeniceConcurrentHashMap<>();
    doReturn(lastProcessedMap).when(storeIngestionTask).getConsumedBytesSinceLastSync();

    // Two sanity tests: empty map should not cause divide by zero, and host not present in map should return false
    storeIngestionTask.shouldSendGlobalRtDiv(message, pcs, brokerUrl);
    assertFalse(storeIngestionTask.shouldSendGlobalRtDiv(message, pcs, "fakehost:5678"));

    lastProcessedMap.put(brokerUrl, 100L); // just needs to be greater than syncBytesInterval
    boolean shouldSendGlobalRtDiv = storeIngestionTask.shouldSendGlobalRtDiv(message, pcs, brokerUrl);
    boolean shouldSyncOffset = storeIngestionTask.shouldSyncOffset(pcs, message, null);

    // Feature flag should stop drainer from syncing OffsetRecord, and ConsumptionTask should send Global RT DIV
    if (isGlobalRtDivEnabled) {
      assertTrue(shouldSendGlobalRtDiv && !shouldSyncOffset);
    } else {
      assertTrue(shouldSyncOffset && !shouldSendGlobalRtDiv);
    }
  }

  /**
   * Verify what happens when globalRtDiv() is called and simulate loading a GlobalRtDivState object from disk.
   */
  @Test
  public void testLoadGlobalRtDiv() {
    LeaderFollowerStoreIngestionTask ingestionTask = mock(LeaderFollowerStoreIngestionTask.class);
    doCallRealMethod().when(ingestionTask).restoreProducerStatesForLeaderConsumption(anyInt());
    doCallRealMethod().when(ingestionTask).loadGlobalRtDiv(anyInt());
    doCallRealMethod().when(ingestionTask).loadGlobalRtDiv(anyInt(), anyString());
    doReturn(true).when(ingestionTask).isGlobalRtDivEnabled();

    InMemoryPubSubPosition p1 = InMemoryPubSubPosition.of(11);
    GlobalRtDivState globalRtDivState =
        new GlobalRtDivState("localhost:1234", Collections.emptyMap(), p1.toWireFormatBuffer());
    doReturn(globalRtDivState).when(ingestionTask).readGlobalRtDivState(any(), anyInt(), any(), any());
    DataIntegrityValidator consumerDiv = mock(DataIntegrityValidator.class);
    doReturn(consumerDiv).when(ingestionTask).getConsumerDiv();
    Int2ObjectMap<String> brokerIdToUrlMap = new Int2ObjectOpenHashMap<>();
    brokerIdToUrlMap.put(0, "localhost:1234");
    brokerIdToUrlMap.put(1, "localhost:4567");
    brokerIdToUrlMap.put(2, "localhost:8910");
    doReturn(brokerIdToUrlMap).when(ingestionTask).getKafkaClusterIdToUrlMap();
    OffsetRecord offsetRecord = mock(OffsetRecord.class);
    doReturn(pubSubTopic).when(offsetRecord).getLeaderTopic(any());
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    doReturn(offsetRecord).when(pcs).getOffsetRecord();
    doReturn(pcs).when(ingestionTask).getPartitionConsumptionState(PARTITION_FOO);
    doReturn(pubSubContext).when(ingestionTask).getPubSubContext();

    ingestionTask.restoreProducerStatesForLeaderConsumption(PARTITION_FOO);
    verify(consumerDiv, times(1)).clearRtSegments(eq(PARTITION_FOO));
    verify(ingestionTask, times(1)).loadGlobalRtDiv(eq(PARTITION_FOO));
    brokerIdToUrlMap.forEach((brokerId, url) -> {
      verify(ingestionTask, times(1)).loadGlobalRtDiv(eq(PARTITION_FOO), eq(url));
    });
    ArgumentCaptor<PubSubPosition> positionCaptor = ArgumentCaptor.forClass(PubSubPosition.class);
    ArgumentCaptor<String> urlCaptor = ArgumentCaptor.forClass(String.class);
    verify(pcs, times(brokerIdToUrlMap.size()))
        .setDivRtCheckpointPosition(urlCaptor.capture(), positionCaptor.capture());
    List<PubSubPosition> capturedPositions = positionCaptor.getAllValues();
    List<String> capturedUrls = urlCaptor.getAllValues();
    assertEquals(capturedPositions.size(), brokerIdToUrlMap.size());
    assertEquals(capturedUrls.size(), brokerIdToUrlMap.size());

    for (int i = 0; i < capturedUrls.size(); i++) {
      PubSubPosition position = capturedPositions.get(i);
      assertEquals(position, p1);
      assertTrue(position instanceof InMemoryPubSubPosition);
      InMemoryPubSubPosition p1Prime = (InMemoryPubSubPosition) position;
      assertEquals(p1Prime.getInternalOffset(), p1.getInternalOffset());
    }
  }

  @Test
  public void testResolveRtTopicPartitionWithPubSubBrokerAddress() throws NoSuchFieldException, IllegalAccessException {
    StoreIngestionTask storeIngestionTask = mock(StoreIngestionTask.class);
    Function<String, String> resolver = Utils::resolveKafkaUrlForSepTopic;
    doCallRealMethod().when(storeIngestionTask)
        .resolveRtTopicPartitionWithPubSubBrokerAddress(any(), any(), anyString());
    doCallRealMethod().when(storeIngestionTask).resolveRtTopicWithPubSubBrokerAddress(any(), anyString());
    doReturn(pubSubTopicRepository).when(storeIngestionTask).getPubSubTopicRepository();
    doReturn(resolver).when(storeIngestionTask).getKafkaClusterUrlResolver();
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    doReturn(1).when(pcs).getPartition();
    doCallRealMethod().when(pcs).getSourceTopicPartition(any());
    String store = "test_store";
    String kafkaUrl = "localhost:1234";
    PubSubTopic realTimeTopic = pubSubTopicRepository.getTopic(Utils.composeRealTimeTopic(store));
    PubSubTopic separateRealTimeTopic =
        pubSubTopicRepository.getTopic(Utils.getSeparateRealTimeTopicName(realTimeTopic.getName()));
    PubSubTopic versionTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(store, 1));
    doReturn(new PubSubTopicPartitionImpl(versionTopic, 1)).when(pcs).getReplicaTopicPartition();
    Field field = storeIngestionTask.getClass().getSuperclass().getDeclaredField("separateRealTimeTopic");
    field.setAccessible(true);
    field.set(storeIngestionTask, separateRealTimeTopic);

    PubSubTopicPartition resolvedRtTopicPartition =
        storeIngestionTask.resolveRtTopicPartitionWithPubSubBrokerAddress(realTimeTopic, pcs, kafkaUrl);
    Assert.assertEquals(resolvedRtTopicPartition.getPubSubTopic(), realTimeTopic);
    Assert.assertEquals(
        storeIngestionTask.resolveRtTopicPartitionWithPubSubBrokerAddress(versionTopic, pcs, kafkaUrl).getPubSubTopic(),
        versionTopic);
    Assert.assertEquals(
        storeIngestionTask
            .resolveRtTopicPartitionWithPubSubBrokerAddress(realTimeTopic, pcs, kafkaUrl + Utils.SEPARATE_TOPIC_SUFFIX)
            .getPubSubTopic(),
        separateRealTimeTopic);
  }

  @Test
  public void testUnsubscribeFromTopic() throws IllegalAccessException, NoSuchFieldException {
    StoreIngestionTask storeIngestionTask = mock(StoreIngestionTask.class);
    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    doCallRealMethod().when(storeIngestionTask).unsubscribeFromTopic(any(), any());
    doReturn(pubSubTopicRepository).when(storeIngestionTask).getPubSubTopicRepository();
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    String store = "test_store";
    String kafkaUrl = "localhost:1234";
    PubSubTopic realTimeTopic = pubSubTopicRepository.getTopic(Utils.composeRealTimeTopic(store));
    PubSubTopic separateRealTimeTopic =
        pubSubTopicRepository.getTopic(Utils.getSeparateRealTimeTopicName(realTimeTopic.getName()));
    PubSubTopic versionTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(store, 1));
    Field field = storeIngestionTask.getClass().getSuperclass().getDeclaredField("separateRealTimeTopic");
    field.setAccessible(true);
    field.set(storeIngestionTask, separateRealTimeTopic);

    doReturn(true).when(storeIngestionTask).isSeparatedRealtimeTopicEnabled();
    storeIngestionTask.unsubscribeFromTopic(realTimeTopic, pcs);
    verify(storeIngestionTask, times(1)).consumerUnSubscribeForStateTransition(realTimeTopic, pcs);
    verify(storeIngestionTask, times(1)).consumerUnSubscribeForStateTransition(separateRealTimeTopic, pcs);
    storeIngestionTask.unsubscribeFromTopic(versionTopic, pcs);
    verify(storeIngestionTask, times(1)).consumerUnSubscribeForStateTransition(versionTopic, pcs);

    doReturn(false).when(storeIngestionTask).isSeparatedRealtimeTopicEnabled();
    storeIngestionTask.unsubscribeFromTopic(realTimeTopic, pcs);
    verify(storeIngestionTask, times(2)).consumerUnSubscribeForStateTransition(realTimeTopic, pcs);
    verify(storeIngestionTask, times(1)).consumerUnSubscribeForStateTransition(separateRealTimeTopic, pcs);
    storeIngestionTask.unsubscribeFromTopic(versionTopic, pcs);
    verify(storeIngestionTask, times(2)).consumerUnSubscribeForStateTransition(versionTopic, pcs);
  }

  @Test(timeOut = 10 * Time.MS_PER_SECOND)
  public void testStoreIngestionInternalClose() throws Exception {
    AtomicReference<CompletableFuture<Void>> dropPartitionFuture = new AtomicReference<>();
    StoreIngestionTaskTestConfig config = new StoreIngestionTaskTestConfig(Utils.setOf(PARTITION_FOO), () -> {
      dropPartitionFuture.set(storeIngestionTaskUnderTest.dropStoragePartitionGracefully(fooTopicPartition));
    }, AA_OFF);
    // Mock out processConsumerActions to ensure the consumer action queue is not processed
    config.setBeforeStartingConsumption(() -> {
      try {
        doNothing().when(storeIngestionTaskUnderTest).processConsumerActions(any());
      } catch (InterruptedException e) {
        // ignored
      }
    });
    runTest(config);
    // The drop partition consumer action should still be handled as part of internalClose
    dropPartitionFuture.get().get();
  }

  /**
   * Test that {@link LeaderFollowerStoreIngestionTask#reportIfCatchUpVersionTopicOffset(PartitionConsumptionState)}
   * only executes if the latch was created and not released. Previously, it would not check if the latch was created.
   * Latch creation is at the start of ingestion {@link LeaderFollowerPartitionStateModel#onBecomeStandbyFromOffline}
   * only if the version is current, but {@link LeaderFollowerPartitionStateModel} is not tested in this unit test.
   */
  @Test
  public void testReportIfCatchUpVersionTopicOffset() throws Exception {
    // Push a key-value pair to kick start the SIT and populate the PCS data structure
    localVeniceWriter.broadcastStartOfPush(new HashMap<>());
    localVeniceWriter.put(putKeyFoo, putValue, EXISTING_SCHEMA_ID, PUT_KEY_FOO_TIMESTAMP, null).get();
    localVeniceWriter.broadcastEndOfPush(new HashMap<>());

    // Use a test config so we can stub the spy BEFORE the task starts (avoiding UnfinishedStubbingException)
    final PubSubTopicPartition BAR_TP = new PubSubTopicPartitionImpl(pubSubTopic, PARTITION_BAR);
    StoreIngestionTaskTestConfig config = new StoreIngestionTaskTestConfig(Collections.singleton(PARTITION_FOO), () -> {
      // Wait for a real PCS to be populated after topic subscription in processCommonConsumerAction()
      verify(mockStoreIngestionStats, timeout(TEST_TIMEOUT_MS).times(1)).recordTotalRecordsConsumed();

      // Intentionally use a mock PCS with a different partition to avoid the SIT test interfering with the test
      PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
      when(pcs.getReplicaTopicPartition()).thenReturn(BAR_TP);
      when(pcs.isHybrid()).thenReturn(true);

      // Case 1: Latch was not created or released, so reportIfCatchUpVersionTopicOffset() shouldn't do anything
      when(pcs.isEndOfPushReceived()).thenReturn(true);
      when(pcs.isLatchCreated()).thenReturn(false);
      when(pcs.isLatchReleased()).thenReturn(false);
      when(pcs.getLatestProcessedVtPosition()).thenReturn(PubSubSymbolicPosition.EARLIEST);
      storeIngestionTaskUnderTest.reportIfCatchUpVersionTopicOffset(pcs);
      verify(storeIngestionTaskUnderTest, never())
          .measureLagWithCallToPubSub(anyString(), eq(BAR_TP), any(PubSubPosition.class));

      // Case 2: Latch was created, so reportIfCatchUpVersionTopicOffset() should execute
      when(pcs.isLatchCreated()).thenReturn(true);
      storeIngestionTaskUnderTest.reportIfCatchUpVersionTopicOffset(pcs);
      verify(storeIngestionTaskUnderTest, times(1))
          .measureLagWithCallToPubSub(anyString(), eq(BAR_TP), any(PubSubPosition.class));

      // Case 3: Latch was created and released, so reportIfCatchUpVersionTopicOffset() shouldn't do anything
      when(pcs.isLatchReleased()).thenReturn(true);

      storeIngestionTaskUnderTest.reportIfCatchUpVersionTopicOffset(pcs);
      verify(storeIngestionTaskUnderTest, times(1))
          .measureLagWithCallToPubSub(anyString(), eq(BAR_TP), any(PubSubPosition.class));
    }, AA_OFF);

    // Stub measureLagWithCallToPubSub BEFORE starting consumption to avoid UnfinishedStubbingException
    // from concurrent mock access by the SIT thread
    config.setBeforeStartingConsumption(() -> {
      doReturn(0L).when(storeIngestionTaskUnderTest)
          .measureLagWithCallToPubSub(anyString(), any(PubSubTopicPartition.class), any(PubSubPosition.class));
    });

    runTest(config);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testResubscribeAsLeader(boolean aaEnabled) throws InterruptedException {
    LeaderFollowerStoreIngestionTask ingestionTask =
        aaEnabled ? mock(ActiveActiveStoreIngestionTask.class) : mock(LeaderFollowerStoreIngestionTask.class);
    doCallRealMethod().when(ingestionTask).resubscribeAsLeader(any());
    doCallRealMethod().when(ingestionTask)
        .preparePositionCheckpointAndStartConsumptionAsLeader(any(), any(), anyBoolean());
    PubSubTopicRepository topicRepository = new PubSubTopicRepository();
    when(ingestionTask.getPubSubTopicRepository()).thenReturn(topicRepository);
    OffsetRecord offsetRecord = mock(OffsetRecord.class);
    PubSubTopic pubSubTopic = topicRepository.getTopic("test_rt");
    when(offsetRecord.getLeaderTopic(any())).thenReturn(pubSubTopic);
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    when(pcs.getOffsetRecord()).thenReturn(offsetRecord);
    when(pcs.getReplicaId()).thenReturn("test_v1-1");
    when(pcs.getPartition()).thenReturn(1);
    when(pcs.getPubSubContext()).thenReturn(pubSubContext);
    when(ingestionTask.isActiveActiveReplicationEnabled()).thenReturn(aaEnabled);
    Set<String> upstreamUrlSet = new HashSet<>();
    upstreamUrlSet.add("dc-1");
    if (aaEnabled) {
      upstreamUrlSet.add("dc-2");
      upstreamUrlSet.add("dc-3");
    }
    Map<String, PubSubPosition> upstreamOffsetMap = new HashMap<>();
    if (aaEnabled) {
      upstreamOffsetMap.put("dc-1", InMemoryPubSubPosition.of(100L));
      upstreamOffsetMap.put("dc-2", InMemoryPubSubPosition.of(20L));
      upstreamOffsetMap.put("dc-3", InMemoryPubSubPosition.of(3L));
    } else {
      upstreamOffsetMap.put(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY, InMemoryPubSubPosition.of(1000L));
    }
    when(ingestionTask.getConsumptionSourceKafkaAddress(pcs)).thenReturn(upstreamUrlSet);
    when(pcs.getLatestProcessedRtPositions()).thenReturn(upstreamOffsetMap);
    doCallRealMethod().when(pcs).getLatestProcessedRtPosition(anyString());
    doCallRealMethod().when(pcs).getLeaderPosition(anyString(), anyBoolean());
    ingestionTask.resubscribeAsLeader(pcs);

    ArgumentCaptor<String> urlCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<PubSubPosition> offsetCaptor = ArgumentCaptor.forClass(PubSubPosition.class);
    verify(ingestionTask, times(aaEnabled ? 3 : 1))
        .consumerSubscribe(any(), any(), offsetCaptor.capture(), urlCaptor.capture());
    List<String> urls = urlCaptor.getAllValues();
    List<PubSubPosition> offsets = offsetCaptor.getAllValues();
    if (aaEnabled) {
      Assert.assertEquals(urls.size(), 3);
      Assert.assertEquals(offsets.size(), 3);
      InMemoryPubSubPosition p1 = (InMemoryPubSubPosition) offsets.get(0);
      InMemoryPubSubPosition p2 = (InMemoryPubSubPosition) offsets.get(1);
      InMemoryPubSubPosition p3 = (InMemoryPubSubPosition) offsets.get(2);
      long sum = p1.getInternalOffset() + p2.getInternalOffset() + p3.getInternalOffset();
      Assert.assertEquals(
          sum,
          123L,
          "The sum of offsets should be equal to the sum of upstream offsets: 100 + 20 + 3 = 123 but got " + sum);
    } else {
      InMemoryPubSubPosition p1 = (InMemoryPubSubPosition) offsets.get(0);
      Assert.assertEquals(urls.size(), 1);
      Assert.assertEquals(offsets.size(), 1);
      Assert.assertEquals(p1.getInternalOffset(), 1000L);
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testResubscribeAsLeaderFromVersionTopic(boolean aaEnabled) throws InterruptedException {
    LeaderFollowerStoreIngestionTask ingestionTask =
        aaEnabled ? mock(ActiveActiveStoreIngestionTask.class) : mock(LeaderFollowerStoreIngestionTask.class);
    doCallRealMethod().when(ingestionTask)
        .preparePositionCheckpointAndStartConsumptionAsLeader(any(), any(), anyBoolean());
    PubSubTopicRepository topicRepository = new PubSubTopicRepository();
    when(ingestionTask.getPubSubTopicRepository()).thenReturn(topicRepository);
    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    OffsetRecord offsetRecord = new OffsetRecord(partitionStateSerializer, pubSubContext);
    PubSubTopic pubSubTopic = topicRepository.getTopic("test_v1");
    offsetRecord.setLeaderTopic(pubSubTopic);
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    when(pcs.getOffsetRecord()).thenReturn(offsetRecord);
    when(pcs.getReplicaId()).thenReturn("test_v1-1");
    when(pcs.getPartition()).thenReturn(1);
    when(ingestionTask.isActiveActiveReplicationEnabled()).thenReturn(aaEnabled);
    Set<String> upstreamUrlSet = new HashSet<>();
    upstreamUrlSet.add("dc-1");
    if (aaEnabled) {
      upstreamUrlSet.add("dc-2");
      upstreamUrlSet.add("dc-3");
    }
    Map<String, PubSubPosition> upstreamOffsetMap = new HashMap<>();
    when(ingestionTask.getConsumptionSourceKafkaAddress(pcs)).thenReturn(upstreamUrlSet);
    when(pcs.getLatestProcessedRtPositions()).thenReturn(upstreamOffsetMap);
    doCallRealMethod().when(pcs).getLatestProcessedRtPosition(anyString());
    PubSubPosition p100 = InMemoryPubSubPosition.of(100L);
    when(pcs.getLatestProcessedRemoteVtPosition()).thenReturn(p100);
    when(pcs.consumeRemotely()).thenReturn(true);
    ingestionTask.preparePositionCheckpointAndStartConsumptionAsLeader(pubSubTopic, pcs, false);

    if (aaEnabled) {
      Assert.assertEquals(offsetRecord.getCheckpointedRtPosition("dc-1"), PubSubSymbolicPosition.EARLIEST);
      Assert.assertEquals(offsetRecord.getCheckpointedRtPosition("dc-2"), PubSubSymbolicPosition.EARLIEST);
      Assert.assertEquals(offsetRecord.getCheckpointedRtPosition("dc-3"), PubSubSymbolicPosition.EARLIEST);
    } else {
      Assert.assertEquals(
          offsetRecord.getCheckpointedRtPosition(OffsetRecord.NON_AA_REPLICATION_UPSTREAM_OFFSET_MAP_KEY),
          PubSubSymbolicPosition.EARLIEST);
    }

  }

  @Test
  public void testParallelShutdown() throws InterruptedException {
    // Setup test data
    StoreIngestionTask storeIngestionTask = mock(StoreIngestionTask.class);
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    List<CompletableFuture<Void>> shutdownFutures = new ArrayList<>();
    ExecutorService shutdownExecutor = Executors.newSingleThreadExecutor();

    // Mock server config to enable checkpointing during shutdown
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    when(serverConfig.isServerIngestionCheckpointDuringGracefulShutdownEnabled()).thenReturn(true);
    when(storeIngestionTask.getServerConfig()).thenReturn(serverConfig);

    // Mock store buffer service
    StoreBufferService storeBufferService = mock(StoreBufferService.class);
    when(storeIngestionTask.getStoreBufferService()).thenReturn(storeBufferService);
    when(storeBufferService.execSyncOffsetCommandAsync(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    doCallRealMethod().when(storeIngestionTask).executeShutdownRunnable(any(), anyList(), any());

    // Set up test data
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(new PubSubTopicImpl("test_topic_v1"), 0);
    when(pcs.getReplicaTopicPartition()).thenReturn(topicPartition);

    // Call the method under test
    storeIngestionTask.executeShutdownRunnable(pcs, shutdownFutures, shutdownExecutor);

    // Wait for async operation to complete
    Assert.assertEquals(shutdownFutures.size(), 1);
    shutdownFutures.forEach(CompletableFuture::join);

    // Verify behavior
    verify(storeIngestionTask).consumerUnSubscribeAllTopics(pcs);
    verify(storeBufferService).execSyncOffsetCommandAsync(topicPartition, storeIngestionTask);
    verify(storeIngestionTask).waitForAllMessageToBeProcessedFromTopicPartition(topicPartition, pcs);

    // Test with null executor (synchronous execution)
    shutdownFutures.clear();
    storeIngestionTask.executeShutdownRunnable(pcs, shutdownFutures, null);
    assertTrue(shutdownFutures.isEmpty(), "No futures should be added when executor is null");
    verify(storeIngestionTask, times(2)).consumerUnSubscribeAllTopics(pcs);

    // Test when checkpointing is disabled
    when(serverConfig.isServerIngestionCheckpointDuringGracefulShutdownEnabled()).thenReturn(false);
    storeIngestionTask.executeShutdownRunnable(pcs, shutdownFutures, shutdownExecutor);
    Assert.assertEquals(shutdownFutures.size(), 1);
    shutdownFutures.forEach(CompletableFuture::join);
    verify(storeIngestionTask, times(3)).consumerUnSubscribeAllTopics(pcs);

    // Clean up
    shutdownExecutor.shutdown();
  }

  @Test
  public void testSkipValidationForSeekableClientEnabled() throws Exception {
    // Test 1: Non-CDC client with non-view topic should NOT skip validation
    runTest(Collections.singleton(PARTITION_FOO), () -> {
      assertFalse(
          storeIngestionTaskUnderTest.shouldSkipValidationsForDaVinciClientEnabled(),
          "Non-CDC client with regular topic should not skip validation");
    }, AA_OFF);

    // Test 2: CDC client with non-view topic should NOT skip validation
    StoreIngestionTaskTestConfig cdcNonViewConfig =
        new StoreIngestionTaskTestConfig(Collections.singleton(PARTITION_FOO), () -> {
          assertFalse(
              storeIngestionTaskUnderTest.shouldSkipValidationsForDaVinciClientEnabled(),
              "CDC client with non-view topic should not skip validation");
        }, AA_OFF);

    DaVinciRecordTransformerConfig cdcRecordTransformerConfig = buildCdcRecordTransformerConfig();
    cdcNonViewConfig.setRecordTransformerConfig(cdcRecordTransformerConfig);

    runTest(cdcNonViewConfig);

    // Test 3: CDC client with materialized view topic SHOULD skip validation
    // This test uses a real materialized view topic name by overriding the store version name in config
    StoreIngestionTaskTestConfig cdcMaterializedViewConfig =
        new StoreIngestionTaskTestConfig(Collections.singleton(PARTITION_FOO), () -> {
          assertTrue(
              storeIngestionTaskUnderTest.shouldSkipValidationsForDaVinciClientEnabled(),
              "CDC client with materialized view topic should skip validation");
        }, AA_OFF);

    cdcMaterializedViewConfig.setRecordTransformerConfig(cdcRecordTransformerConfig).setDaVinci(true);
    // Override the store version config to use a materialized view topic name
    cdcMaterializedViewConfig.setStoreVersionConfigOverride(storeVersionConfig -> {
      String materializedViewTopicName = storeNameWithoutVersionInfo + "_v1" + VeniceView.VIEW_NAME_SEPARATOR
          + "testView" + MaterializedView.MATERIALIZED_VIEW_TOPIC_SUFFIX;
      doReturn(materializedViewTopicName).when(storeVersionConfig).getStoreVersionName();
    });

    runTest(cdcMaterializedViewConfig);
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

  private DaVinciRecordTransformerConfig buildRecordTransformerConfig(boolean isRecordTransformationEnabled) {
    Schema myKeySchema = Schema.create(Schema.Type.INT);
    SchemaEntry keySchemaEntry = new SchemaEntry(SCHEMA_ID, myKeySchema);
    when(mockSchemaRepo.getKeySchema(storeNameWithoutVersionInfo)).thenReturn(keySchemaEntry);

    Schema myValueSchema = Schema.create(Schema.Type.STRING);
    SchemaEntry valueSchemaEntry = new SchemaEntry(SCHEMA_ID, myValueSchema);
    when(mockSchemaRepo.getValueSchema(eq(storeNameWithoutVersionInfo), anyInt())).thenReturn(valueSchemaEntry);
    when(mockSchemaRepo.getSupersetOrLatestValueSchema(eq(storeNameWithoutVersionInfo))).thenReturn(valueSchemaEntry);

    return new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestStringRecordTransformer::new)
        .setOutputValueClass(String.class)
        .setOutputValueSchema(myValueSchema)
        .setRecordTransformationEnabled(isRecordTransformationEnabled)
        .build();
  }

  /**
   * Builds a CDC record transformer config that returns a mock CDC transformer.
   * This is used to test logic that depends on isCDCRecordTransformer() returning true.
   */
  private DaVinciRecordTransformerConfig buildCdcRecordTransformerConfig() {
    Schema myKeySchema = Schema.create(Schema.Type.INT);
    SchemaEntry keySchemaEntry = new SchemaEntry(SCHEMA_ID, myKeySchema);
    when(mockSchemaRepo.getKeySchema(storeNameWithoutVersionInfo)).thenReturn(keySchemaEntry);

    Schema myValueSchema = Schema.create(Schema.Type.STRING);
    SchemaEntry valueSchemaEntry = new SchemaEntry(SCHEMA_ID, myValueSchema);
    when(mockSchemaRepo.getValueSchema(eq(storeNameWithoutVersionInfo), anyInt())).thenReturn(valueSchemaEntry);
    when(mockSchemaRepo.getSupersetOrLatestValueSchema(eq(storeNameWithoutVersionInfo))).thenReturn(valueSchemaEntry);

    // Create a mock CDC transformer that will return true for isCDCRecordTransformer()
    VeniceChangelogConsumerDaVinciRecordTransformerImpl.DaVinciRecordTransformerChangelogConsumer mockCdcTransformer =
        mock(VeniceChangelogConsumerDaVinciRecordTransformerImpl.DaVinciRecordTransformerChangelogConsumer.class);

    // Mock the function to return the CDC transformer
    DaVinciRecordTransformerFunctionalInterface cdcTransformerFunction =
        (storeName, storeVersion, keySchema, inputValueSchema, outputValueSchema, config) -> mockCdcTransformer;

    return new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(cdcTransformerFunction)
        .setOutputValueClass(String.class)
        .setOutputValueSchema(myValueSchema)
        .setRecordTransformationEnabled(false)
        .build();
  }

  public static OffsetRecord getOffsetRecord(
      PubSubPosition currentPosition,
      boolean complete,
      PubSubContext pubSubContext) {
    return TestUtils.getOffsetRecord(
        currentPosition,
        complete ? Optional.of(InMemoryPubSubPosition.of(1000L)) : Optional.of(InMemoryPubSubPosition.of(0L)),
        pubSubContext);
  }
}
