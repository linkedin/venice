package com.linkedin.venice.controller;

import static com.linkedin.venice.controller.VeniceHelixAdmin.VERSION_ID_UNSET;
import static com.linkedin.venice.controller.kafka.consumer.AdminConsumptionTask.IGNORED_CURRENT_VERSION;
import static com.linkedin.venice.controller.util.ParentControllerConfigUpdateUtils.addUpdateSchemaForStore;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ACCESS_CONTROLLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ACTIVE_ACTIVE_REPLICATION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.AMPLIFICATION_FACTOR;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.AUTO_SCHEMA_REGISTER_FOR_PUSHJOB_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.BACKUP_STRATEGY;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.BACKUP_VERSION_RETENTION_MS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.BATCH_GET_LIMIT;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.BLOB_TRANSFER_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.BUFFER_REPLAY_POLICY;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.CHUNKING_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLIENT_DECOMPRESSION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.COMPRESSION_STRATEGY;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.DATA_REPLICATION_POLICY;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.DISABLE_DAVINCI_PUSH_STATUS_STORE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.DISABLE_META_STORE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ENABLE_READS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ENABLE_STORE_MIGRATION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ENABLE_WRITES;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ETLED_PROXY_USER_ACCOUNT;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.FUTURE_VERSION_ETL_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.HYBRID_STORE_DISK_QUOTA_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.INCREMENTAL_PUSH_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.IS_DAVINCI_HEARTBEAT_REPORTED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.LARGEST_USED_VERSION_NUMBER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.LATEST_SUPERSET_SCHEMA_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.MAX_COMPACTION_LAG_SECONDS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.MAX_NEARLINE_RECORD_SIZE_BYTES;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.MAX_RECORD_SIZE_BYTES;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.MIGRATION_DUPLICATE_STORE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.MIN_COMPACTION_LAG_SECONDS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NATIVE_REPLICATION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NATIVE_REPLICATION_SOURCE_FABRIC;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NEARLINE_PRODUCER_COMPRESSION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NEARLINE_PRODUCER_COUNT_PER_WRITER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NUM_VERSIONS_TO_PRESERVE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.OFFSET_LAG_TO_GO_ONLINE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.OWNER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PARTITIONER_CLASS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PARTITIONER_PARAMS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PARTITION_COUNT;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PERSONA_NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PUSH_STREAM_SOURCE_ADDRESS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.READ_COMPUTATION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.READ_QUOTA_IN_CU;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REAL_TIME_TOPIC_NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REGULAR_VERSION_ETL_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REPLICATION_FACTOR;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REPLICATION_METADATA_PROTOCOL_VERSION_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REWIND_TIME_IN_SECONDS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.RMD_CHUNKING_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.SEPARATE_REAL_TIME_TOPIC_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORAGE_NODE_READ_QUOTA_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORAGE_QUOTA_IN_BYTE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_MIGRATION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_VIEW;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.TARGET_SWAP_REGION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.TARGET_SWAP_REGION_WAIT_TIME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.TIME_LAG_TO_GO_ONLINE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.UNUSED_SCHEMA_DELETION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VERSION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.WRITE_COMPUTATION_ENABLED;
import static com.linkedin.venice.meta.HybridStoreConfigImpl.DEFAULT_HYBRID_OFFSET_LAG_THRESHOLD;
import static com.linkedin.venice.meta.HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD;
import static com.linkedin.venice.meta.HybridStoreConfigImpl.DEFAULT_REAL_TIME_TOPIC_NAME;
import static com.linkedin.venice.meta.HybridStoreConfigImpl.DEFAULT_REWIND_TIME_IN_SECONDS;
import static com.linkedin.venice.meta.Version.VERSION_SEPARATOR;
import static com.linkedin.venice.meta.VersionStatus.ONLINE;
import static com.linkedin.venice.meta.VersionStatus.PUSHED;
import static com.linkedin.venice.serialization.avro.AvroProtocolDefinition.BATCH_JOB_HEARTBEAT;
import static com.linkedin.venice.serialization.avro.AvroProtocolDefinition.PUSH_JOB_DETAILS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.venice.ConfigConstants;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.acl.AclException;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.authorization.AceEntry;
import com.linkedin.venice.authorization.AclBinding;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.authorization.IdentityParser;
import com.linkedin.venice.authorization.Method;
import com.linkedin.venice.authorization.Permission;
import com.linkedin.venice.authorization.Principal;
import com.linkedin.venice.authorization.Resource;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.authorization.SystemStoreAclSynchronizationTask;
import com.linkedin.venice.controller.init.DelegatingClusterLeaderInitializationRoutine;
import com.linkedin.venice.controller.init.SharedInternalRTStoreInitializationRoutine;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumerService;
import com.linkedin.venice.controller.kafka.protocol.admin.AbortMigration;
import com.linkedin.venice.controller.kafka.protocol.admin.AddVersion;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.ConfigureActiveActiveReplicationForCluster;
import com.linkedin.venice.controller.kafka.protocol.admin.CreateStoragePersona;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteAllVersions;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteOldVersion;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteStoragePersona;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteStore;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteUnusedValueSchemas;
import com.linkedin.venice.controller.kafka.protocol.admin.DerivedSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.DisableStoreRead;
import com.linkedin.venice.controller.kafka.protocol.admin.ETLStoreConfigRecord;
import com.linkedin.venice.controller.kafka.protocol.admin.EnableStoreRead;
import com.linkedin.venice.controller.kafka.protocol.admin.HybridStoreConfigRecord;
import com.linkedin.venice.controller.kafka.protocol.admin.KillOfflinePushJob;
import com.linkedin.venice.controller.kafka.protocol.admin.MetaSystemStoreAutoCreationValidation;
import com.linkedin.venice.controller.kafka.protocol.admin.MetadataSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.MigrateStore;
import com.linkedin.venice.controller.kafka.protocol.admin.PartitionerConfigRecord;
import com.linkedin.venice.controller.kafka.protocol.admin.PauseStore;
import com.linkedin.venice.controller.kafka.protocol.admin.PushStatusSystemStoreAutoCreationValidation;
import com.linkedin.venice.controller.kafka.protocol.admin.ResumeStore;
import com.linkedin.venice.controller.kafka.protocol.admin.RollForwardCurrentVersion;
import com.linkedin.venice.controller.kafka.protocol.admin.RollbackCurrentVersion;
import com.linkedin.venice.controller.kafka.protocol.admin.SchemaMeta;
import com.linkedin.venice.controller.kafka.protocol.admin.SetStoreOwner;
import com.linkedin.venice.controller.kafka.protocol.admin.SetStorePartitionCount;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreViewConfigRecord;
import com.linkedin.venice.controller.kafka.protocol.admin.SupersetSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.UpdateStoragePersona;
import com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore;
import com.linkedin.venice.controller.kafka.protocol.admin.ValueSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.enums.SchemaType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controller.lingeringjob.DefaultLingeringStoreVersionChecker;
import com.linkedin.venice.controller.lingeringjob.LingeringStoreVersionChecker;
import com.linkedin.venice.controller.migration.MigrationPushStrategyZKAccessor;
import com.linkedin.venice.controller.supersetschema.DefaultSupersetSchemaGenerator;
import com.linkedin.venice.controller.supersetschema.SupersetSchemaGenerator;
import com.linkedin.venice.controller.util.ParentControllerConfigUpdateUtils;
import com.linkedin.venice.controllerapi.AdminCommandExecution;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.MultiStoreInfoResponse;
import com.linkedin.venice.controllerapi.MultiStoreStatusResponse;
import com.linkedin.venice.controllerapi.NodeReplicasReadinessState;
import com.linkedin.venice.controllerapi.ReadyForDataRecoveryResponse;
import com.linkedin.venice.controllerapi.RegionPushDetailsResponse;
import com.linkedin.venice.controllerapi.RepushInfo;
import com.linkedin.venice.controllerapi.SchemaUsageResponse;
import com.linkedin.venice.controllerapi.StoreComparisonInfo;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateClusterConfigQueryParams;
import com.linkedin.venice.controllerapi.UpdateStoragePersonaQueryParams;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.exceptions.ConcurrentBatchPushException;
import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.PartitionerSchemaMismatchException;
import com.linkedin.venice.exceptions.ResourceStillExistsException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSystemStoreRepository;
import com.linkedin.venice.helix.ParentHelixOfflinePushAccessor;
import com.linkedin.venice.helix.Replica;
import com.linkedin.venice.helix.StoragePersonaRepository;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.ETLStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RegionPushDetails;
import com.linkedin.venice.meta.RoutersClusterConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.StoreDataAudit;
import com.linkedin.venice.meta.StoreGraveyard;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.VeniceUserStoreType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.meta.ViewConfigImpl;
import com.linkedin.venice.meta.ViewParameterKeys;
import com.linkedin.venice.persona.StoragePersona;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.GeneratedSchemaID;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatKey;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatValue;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.system.store.MetaStoreReader;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.CollectionUtils;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.RegionUtils;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import com.linkedin.venice.views.MaterializedView;
import com.linkedin.venice.views.VeniceView;
import com.linkedin.venice.views.ViewUtils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.io.IOException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is a wrapper of {@link VeniceHelixAdmin}, which will be used in parent controller.
 * There should be only one single Parent Controller, which is the endpoint for all the admin data
 * update.
 * For every admin update operation, it will first push admin operation messages to Kafka,
 * then wait for the admin consumer to consume the message.
 * All validations on the updates should be done before the admin operation message is published to Kafka.
 */
public class VeniceParentHelixAdmin implements Admin {
  private static final long SLEEP_INTERVAL_FOR_DATA_CONSUMPTION_IN_MS = 1000;
  private static final Logger LOGGER = LogManager.getLogger(VeniceParentHelixAdmin.class);
  // Store version number to retain in Parent Controller to limit 'Store' ZNode size.
  static final int STORE_VERSION_RETENTION_COUNT = 5;
  private static final StackTraceElement[] EMPTY_STACK_TRACE = new StackTraceElement[0];

  private static final long TOPIC_DELETION_DELAY_MS = 5 * Time.MS_PER_MINUTE;

  final Map<String, Boolean> asyncSetupEnabledMap;
  private final VeniceHelixAdmin veniceHelixAdmin;
  private final Map<String, VeniceWriter<byte[], byte[], byte[]>> veniceWriterMap;
  private final AdminTopicMetadataAccessor adminTopicMetadataAccessor;
  private final byte[] emptyKeyByteArr = new byte[0];
  private final AdminOperationSerializer adminOperationSerializer = new AdminOperationSerializer();
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final Map<String, Map<String, ReentrantLock>> perStoreAdminLocks = new ConcurrentHashMap<>();
  private final Map<String, ReentrantLock> perClusterAdminLocks = new ConcurrentHashMap<>();
  private final Map<String, AdminCommandExecutionTracker> adminCommandExecutionTrackers;
  private final Set<String> executionIdValidatedClusters = new HashSet<>();
  // Only used for setup work which are intended to be short lived and is bounded by the number of venice clusters.
  // Based on JavaDoc "Threads that have not been used for sixty seconds are terminated and removed from the cache."
  private final ExecutorService asyncSetupExecutor = Executors.newCachedThreadPool();
  private final ExecutorService topicCheckerExecutor = Executors.newSingleThreadExecutor();
  private final TerminalStateTopicCheckerForParentController terminalStateTopicChecker;
  private final SystemStoreAclSynchronizationTask systemStoreAclSynchronizationTask;
  private final UserSystemStoreLifeCycleHelper systemStoreLifeCycleHelper;
  private final WriteComputeSchemaConverter writeComputeSchemaConverter;

  private Time timer = new SystemTime();
  private Optional<SSLFactory> sslFactory = Optional.empty();

  private final MigrationPushStrategyZKAccessor pushStrategyZKAccessor;

  private final PubSubTopicRepository pubSubTopicRepository;

  private ParentHelixOfflinePushAccessor offlinePushAccessor;

  /**
   * Here is the way how Parent Controller is keeping errored topics when {@link #maxErroredTopicNumToKeep} > 0:
   * 1. For errored topics, {@link #getOffLineJobStatus} won't truncate them;
   * 2. For errored topics, {@link #killOfflinePush(String, String, boolean)} won't truncate them;
   * 3. {@link #getTopicForCurrentPushJob(String, String, boolean, boolean)} will truncate the errored topics based on
   * {@link #maxErroredTopicNumToKeep};
   *
   * It means error topic retiring is only be triggered by next push.
   *
   * When {@link #maxErroredTopicNumToKeep} is 0, errored topics will be truncated right away when job is finished.
   */
  private int maxErroredTopicNumToKeep;

  private final int waitingTimeForConsumptionMs;

  private Optional<DynamicAccessController> accessController;

  private final Optional<AuthorizerService> authorizerService;

  private final ExecutorService systemStoreAclSynchronizationExecutor;

  private final LingeringStoreVersionChecker lingeringStoreVersionChecker;

  private final Optional<SupersetSchemaGenerator> externalSupersetSchemaGenerator;

  private final SupersetSchemaGenerator defaultSupersetSchemaGenerator = new DefaultSupersetSchemaGenerator();

  private final IdentityParser identityParser;

  // New fabric controller client map per cluster per fabric
  private final Map<String, Map<String, ControllerClient>> newFabricControllerClientMap =
      new VeniceConcurrentHashMap<>();

  // Visible for testing
  public VeniceParentHelixAdmin(
      VeniceHelixAdmin veniceHelixAdmin,
      VeniceControllerMultiClusterConfig multiClusterConfigs) {
    this(veniceHelixAdmin, multiClusterConfigs, false, Optional.empty(), Optional.empty());
  }

  // Visible for testing
  public VeniceParentHelixAdmin(
      VeniceHelixAdmin veniceHelixAdmin,
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      boolean sslEnabled,
      Optional<SSLConfig> sslConfig,
      Optional<AuthorizerService> authorizerService) {
    this(
        veniceHelixAdmin,
        multiClusterConfigs,
        sslEnabled,
        sslConfig,
        Optional.empty(),
        authorizerService,
        new DefaultLingeringStoreVersionChecker());
  }

  // Visible for testing
  public VeniceParentHelixAdmin(
      VeniceHelixAdmin veniceHelixAdmin,
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      boolean sslEnabled,
      Optional<SSLConfig> sslConfig,
      Optional<DynamicAccessController> accessController,
      Optional<AuthorizerService> authorizerService,
      LingeringStoreVersionChecker lingeringStoreVersionChecker) {
    this(
        veniceHelixAdmin,
        multiClusterConfigs,
        sslEnabled,
        sslConfig,
        accessController,
        authorizerService,
        lingeringStoreVersionChecker,
        WriteComputeSchemaConverter.getInstance(), // TODO: make it an input param
        Optional.empty(),
        new PubSubTopicRepository(),
        null,
        null);
  }

  public VeniceParentHelixAdmin(
      VeniceHelixAdmin veniceHelixAdmin,
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      boolean sslEnabled,
      Optional<SSLConfig> sslConfig,
      Optional<DynamicAccessController> accessController,
      Optional<AuthorizerService> authorizerService,
      LingeringStoreVersionChecker lingeringStoreVersionChecker,
      WriteComputeSchemaConverter writeComputeSchemaConverter,
      Optional<SupersetSchemaGenerator> externalSupersetSchemaGenerator,
      PubSubTopicRepository pubSubTopicRepository,
      DelegatingClusterLeaderInitializationRoutine initRoutineForPushJobDetailsSystemStore,
      DelegatingClusterLeaderInitializationRoutine initRoutineForHeartbeatSystemStore) {
    Validate.notNull(lingeringStoreVersionChecker);
    Validate.notNull(writeComputeSchemaConverter);
    this.veniceHelixAdmin = veniceHelixAdmin;
    this.multiClusterConfigs = multiClusterConfigs;
    this.waitingTimeForConsumptionMs = this.multiClusterConfigs.getParentControllerWaitingTimeForConsumptionMs();
    this.veniceWriterMap = new ConcurrentHashMap<>();
    this.adminTopicMetadataAccessor = new ZkAdminTopicMetadataAccessor(
        this.veniceHelixAdmin.getZkClient(),
        this.veniceHelixAdmin.getAdapterSerializer());
    this.adminCommandExecutionTrackers = new HashMap<>();
    this.asyncSetupEnabledMap = new VeniceConcurrentHashMap<>();
    this.accessController = accessController;
    this.authorizerService = authorizerService;
    this.externalSupersetSchemaGenerator = externalSupersetSchemaGenerator;
    this.pubSubTopicRepository = pubSubTopicRepository;
    this.systemStoreAclSynchronizationExecutor =
        authorizerService.map(service -> Executors.newSingleThreadExecutor()).orElse(null);
    if (sslEnabled) {
      try {
        String sslFactoryClassName = this.multiClusterConfigs.getSslFactoryClassName();
        Properties sslProperties = sslConfig.get().getSslProperties();
        sslFactory = Optional.of(SslUtils.getSSLFactory(sslProperties, sslFactoryClassName));
      } catch (Exception e) {
        LOGGER.error("Failed to create SSL engine", e);
        throw new VeniceException(e);
      }
    }
    for (String cluster: this.multiClusterConfigs.getClusters()) {
      VeniceControllerClusterConfig config = this.multiClusterConfigs.getControllerConfig(cluster);
      adminCommandExecutionTrackers.put(
          cluster,
          new AdminCommandExecutionTracker(
              config.getClusterName(),
              this.veniceHelixAdmin.getExecutionIdAccessor(),
              this.veniceHelixAdmin.getControllerClientMap(config.getClusterName())));
      perStoreAdminLocks.put(cluster, new ConcurrentHashMap<>());
      perClusterAdminLocks.put(cluster, new ReentrantLock());
    }
    this.pushStrategyZKAccessor = new MigrationPushStrategyZKAccessor(
        this.veniceHelixAdmin.getZkClient(),
        this.veniceHelixAdmin.getAdapterSerializer());
    this.maxErroredTopicNumToKeep = this.multiClusterConfigs.getParentControllerMaxErroredTopicNumToKeep();
    this.offlinePushAccessor = new ParentHelixOfflinePushAccessor(
        this.veniceHelixAdmin.getZkClient(),
        this.veniceHelixAdmin.getAdapterSerializer());
    terminalStateTopicChecker = new TerminalStateTopicCheckerForParentController(
        this,
        this.veniceHelixAdmin.getStoreConfigRepo(),
        this.multiClusterConfigs.getTerminalStateTopicCheckerDelayMs());
    topicCheckerExecutor.submit(terminalStateTopicChecker);
    systemStoreAclSynchronizationTask =
        authorizerService
            .map(
                service -> new SystemStoreAclSynchronizationTask(
                    service,
                    this,
                    this.multiClusterConfigs.getSystemStoreAclSynchronizationDelayMs()))
            .orElse(null);
    if (systemStoreAclSynchronizationTask != null) {
      systemStoreAclSynchronizationExecutor.submit(systemStoreAclSynchronizationTask);
    }
    this.lingeringStoreVersionChecker = lingeringStoreVersionChecker;
    systemStoreLifeCycleHelper = new UserSystemStoreLifeCycleHelper(this, authorizerService, multiClusterConfigs);
    this.writeComputeSchemaConverter = writeComputeSchemaConverter;
    Class<IdentityParser> identityParserClass =
        ReflectUtils.loadClass(multiClusterConfigs.getCommonConfig().getIdentityParserClassName());
    this.identityParser = ReflectUtils.callConstructor(identityParserClass, new Class[0], new Object[0]);

    String pushJobDetailsStoreClusterName = getMultiClusterConfigs().getPushJobStatusStoreClusterName();
    boolean initializePushJobDetailsStore = !StringUtils.isEmpty(pushJobDetailsStoreClusterName);
    if (initRoutineForPushJobDetailsSystemStore != null) {
      if (initializePushJobDetailsStore) {
        // TODO: When we plan to enable active-active push details store in future, we need to enable it by default.
        UpdateStoreQueryParams updateStoreQueryParamsForPushJobDetails =
            new UpdateStoreQueryParams().setHybridDataReplicationPolicy(DataReplicationPolicy.AGGREGATE);
        initRoutineForPushJobDetailsSystemStore.setDelegate(
            new SharedInternalRTStoreInitializationRoutine(
                pushJobDetailsStoreClusterName,
                VeniceSystemStoreUtils.getPushJobDetailsStoreName(),
                PUSH_JOB_DETAILS,
                multiClusterConfigs,
                this,
                PushJobStatusRecordKey.getClassSchema(),
                updateStoreQueryParamsForPushJobDetails));
      } else {
        initRoutineForPushJobDetailsSystemStore.setAllowEmptyDelegateInitializationToSucceed();
      }
    }

    String batchJobHeartbeatStoreClusterName = getMultiClusterConfigs().getBatchJobHeartbeatStoreCluster();
    boolean initializeBatchJobHeartbeatStore = !StringUtils.isEmpty(batchJobHeartbeatStoreClusterName);
    if (initRoutineForHeartbeatSystemStore != null) {
      if (initializeBatchJobHeartbeatStore) {
        UpdateStoreQueryParams updateStoreQueryParamsForHeartbeatSystemStore =
            new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true);
        initRoutineForHeartbeatSystemStore.setDelegate(
            new SharedInternalRTStoreInitializationRoutine(
                batchJobHeartbeatStoreClusterName,
                BATCH_JOB_HEARTBEAT.getSystemStoreName(),
                BATCH_JOB_HEARTBEAT,
                multiClusterConfigs,
                this,
                BatchJobHeartbeatKey.getClassSchema(),
                updateStoreQueryParamsForHeartbeatSystemStore));
      } else {
        initRoutineForHeartbeatSystemStore.setAllowEmptyDelegateInitializationToSucceed();
      }
    }
  }

  // For testing purpose.
  void setMaxErroredTopicNumToKeep(int maxErroredTopicNumToKeep) {
    this.maxErroredTopicNumToKeep = maxErroredTopicNumToKeep;
  }

  void setVeniceWriterForCluster(String clusterName, VeniceWriter writer) {
    veniceWriterMap.putIfAbsent(clusterName, writer);
  }

  /**
   * Initialize Venice storage cluster in Helix by:
   * <ul>
   *  <li> creating and configuring required properties in Helix.</li>
   *  <li> waiting resource's (partial) partition to appear in the external view.</li>
   *  <li> making sure admin Kafka topics is created.</li>
   *  <li> creating a Venice writer for the cluster.</li>
   * </ul>
   * @param clusterName Venice cluster name.
   */
  @Override
  public synchronized void initStorageCluster(String clusterName) {
    /*
     * We might not be able to call a lot of functions of veniceHelixAdmin since
     * current controller might not be the leader controller for the given clusterName
     * Even current controller is leader controller, it will take some time to become 'leader'
     * since VeniceHelixAdmin.start won't wait for state becomes 'Leader', but a lot of
     * VeniceHelixAdmin functions have 'leadership' check.
     */

    // Check whether the admin topic exists or not.
    PubSubTopic topicName = pubSubTopicRepository.getTopic(AdminTopicUtils.getTopicNameFromClusterName(clusterName));
    TopicManager topicManager = getTopicManager();
    if (topicManager.containsTopicAndAllPartitionsAreOnline(topicName)) {
      LOGGER.info("Admin topic: {} for cluster: {} already exists.", topicName, clusterName);
    } else {
      // Create Kafka topic.
      topicManager.createTopic(
          topicName,
          AdminTopicUtils.PARTITION_NUM_FOR_ADMIN_TOPIC,
          getMultiClusterConfigs().getControllerConfig(clusterName).getAdminTopicReplicationFactor(),
          true,
          false,
          getMultiClusterConfigs().getControllerConfig(clusterName).getMinInSyncReplicasAdminTopics());
      LOGGER.info("Created admin topic: {} for cluster: {}", topicName, clusterName);
    }

    // Initialize producer.
    veniceWriterMap.computeIfAbsent(clusterName, (key) -> {
      /**
       * Venice just needs to check seq id in {@link com.linkedin.venice.controller.kafka.consumer.AdminConsumptionTask} to catch the following scenarios:
       * 1. Data missing;
       * 2. Data out of order;
       * 3. Data duplication;
       */
      return getVeniceWriterFactory().createVeniceWriter(
          new VeniceWriterOptions.Builder(topicName.getName()).setTime(getTimer())
              .setPartitionCount(AdminTopicUtils.PARTITION_NUM_FOR_ADMIN_TOPIC)
              .build());
    });

    getVeniceHelixAdmin().initStorageCluster(clusterName);
    asyncSetupEnabledMap.put(clusterName, true);
  }

  /**
   * Test if a cluster is valid (in Helix cluster list).
   * @param clusterName Venice cluster name.
   * @return <code>true</code> if input cluster is in Helix cluster list;
   *         <code>false</code> otherwise.
   */
  @Override
  public boolean isClusterValid(String clusterName) {
    return getVeniceHelixAdmin().isClusterValid(clusterName);
  }

  private void sendAdminMessageAndWaitForConsumed(String clusterName, String storeName, AdminOperation message) {
    if (!veniceWriterMap.containsKey(clusterName)) {
      throw new VeniceException("Cluster: " + clusterName + " is not started yet!");
    }
    acquireAdminMessageExecutionIdLock(clusterName);
    try {
      checkAndRepairCorruptedExecutionId(clusterName);
      try (AutoCloseableLock ignore = veniceHelixAdmin.getHelixVeniceClusterResources(clusterName)
          .getClusterLockManager()
          .createClusterReadLock()) {
        // Obtain the cluster level read lock so during a graceful shutdown or leadership handover there will be no
        // execution id gap (execution id is generated but the message is not sent).
        AdminCommandExecutionTracker adminCommandExecutionTracker = adminCommandExecutionTrackers.get(clusterName);
        AdminCommandExecution execution =
            adminCommandExecutionTracker.createExecution(AdminMessageType.valueOf(message).name());
        message.executionId = execution.getExecutionId();
        VeniceWriter<byte[], byte[], byte[]> veniceWriter = veniceWriterMap.get(clusterName);
        byte[] serializedValue = adminOperationSerializer.serialize(message);
        try {
          Future<PubSubProduceResult> future = veniceWriter
              .put(emptyKeyByteArr, serializedValue, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
          PubSubProduceResult produceResult = future.get();

          LOGGER.info("Sent message: {} to kafka, offset: {}", message, produceResult.getOffset());
        } catch (Exception e) {
          throw new VeniceException("Got exception during sending message to Kafka -- " + e.getMessage(), e);
        }
        // TODO Remove the admin command execution tracking code since no one is using it (might not even be working).
        adminCommandExecutionTracker.startTrackingExecution(execution);
      }
    } finally {
      releaseAdminMessageExecutionIdLock(clusterName);
    }
    waitingMessageToBeConsumed(clusterName, storeName, message.executionId);
  }

  @Override
  public void deleteValueSchemas(String clusterName, String storeName, Set<Integer> unusedValueSchemaIds) {
    Set<Integer> inuseValueSchemaIds = getInUseValueSchemaIds(clusterName, storeName);
    if (inuseValueSchemaIds.isEmpty()) {
      return;
    }
    boolean isCommon = unusedValueSchemaIds.stream().anyMatch(inuseValueSchemaIds::contains);
    if (isCommon) {
      LOGGER
          .error("For store {} cannot delete value schema ids {} as they being used.", storeName, unusedValueSchemaIds);
      return;
    }
    getVeniceHelixAdmin().checkControllerLeadershipFor(clusterName);
    DeleteUnusedValueSchemas deleteValueSchemas =
        (DeleteUnusedValueSchemas) AdminMessageType.DELETE_UNUSED_VALUE_SCHEMA.getNewInstance();
    deleteValueSchemas.setClusterName(clusterName);
    deleteValueSchemas.setStoreName(storeName);
    deleteValueSchemas.setSchemaIds(new ArrayList<>(unusedValueSchemaIds));

    AdminOperation message = new AdminOperation();
    message.operationType = AdminMessageType.DELETE_UNUSED_VALUE_SCHEMA.getValue();
    message.payloadUnion = deleteValueSchemas;

    sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);
  }

  @Override
  public Set<Integer> getInUseValueSchemaIds(String clusterName, String storeName) {
    Map<String, ControllerClient> controllerClients = getVeniceHelixAdmin().getControllerClientMap(clusterName);
    Set<Integer> result = new HashSet<>();
    for (Map.Entry<String, ControllerClient> entry: controllerClients.entrySet()) {
      String region = entry.getKey();
      ControllerClient controllerClient = entry.getValue();
      SchemaUsageResponse response = controllerClient.getInUseSchemaIds(storeName);
      if (response.isError()) {
        if (response.isError()) {
          LOGGER.error(
              "Could not query store from region: " + region + " for cluster: " + clusterName + ". "
                  + response.getError());
        }
        return Collections.emptySet();
      } else {
        // make union of all used schemas
        result.addAll(response.getInUseValueSchemaIds());
      }
    }
    return result;
  }

  private void checkAndRepairCorruptedExecutionId(String clusterName) {
    if (!executionIdValidatedClusters.contains(clusterName)) {
      ExecutionIdAccessor executionIdAccessor = getVeniceHelixAdmin().getExecutionIdAccessor();
      long lastGeneratedExecutionId = executionIdAccessor.getLastGeneratedExecutionId(clusterName);
      long lastConsumedExecutionId =
          AdminTopicMetadataAccessor.getExecutionId(adminTopicMetadataAccessor.getMetadata(clusterName));
      if (lastGeneratedExecutionId < lastConsumedExecutionId) {
        // Invalid state, resetting the last generated execution id to last consumed execution id.
        LOGGER.warn(
            "Invalid executionId state detected, last generated execution id: {}, last consumed execution id: {}. "
                + "Resetting last generated execution id to: {}",
            lastGeneratedExecutionId,
            lastConsumedExecutionId,
            lastConsumedExecutionId);
        executionIdAccessor.updateLastGeneratedExecutionId(clusterName, lastConsumedExecutionId);
      }
      executionIdValidatedClusters.add(clusterName);
    }
  }

  private void waitingMessageToBeConsumed(String clusterName, String storeName, long executionId) {
    // Blocking until consumer consumes the new message or timeout
    long startTime = SystemTime.INSTANCE.getMilliseconds();
    while (true) {
      Long consumedExecutionId = getVeniceHelixAdmin().getLastSucceededExecutionId(clusterName, storeName);
      if (consumedExecutionId != null && consumedExecutionId >= executionId) {
        break;
      }
      // Check whether timeout
      long currentTime = SystemTime.INSTANCE.getMilliseconds();
      if (currentTime - startTime > waitingTimeForConsumptionMs) {
        Exception lastException =
            (storeName == null) ? null : getVeniceHelixAdmin().getLastExceptionForStore(clusterName, storeName);
        String errMsg =
            "Timed out after waiting for " + waitingTimeForConsumptionMs + "ms for admin consumption to catch up.";
        errMsg += " Consumed execution id: " + consumedExecutionId + ", waiting to be consumed id: " + executionId;
        errMsg += (lastException == null) ? "" : " Last exception: " + lastException.getMessage();
        throw new VeniceException(errMsg, lastException);
      }

      LOGGER.info("Waiting execution id: {} to be consumed, currently at: {}", executionId, consumedExecutionId);
      Utils.sleep(SLEEP_INTERVAL_FOR_DATA_CONSUMPTION_IN_MS);
    }
    LOGGER.info("The message has been consumed, execution id: {}", executionId);
  }

  /**
   * Acquire the cluster level lock used to ensure no duplicate admin message execution id is generated and admin
   * messages are written to the admin topic in the correct order (with incrementing execution id).
   * This lock is held when generating the new execution and writing the admin message with the new execution id to the
   * admin topic.
   */
  private void acquireAdminMessageExecutionIdLock(String clusterName) {
    try {
      if (clusterName == null) {
        throw new VeniceException("Cannot acquire admin message execution id lock with a null cluster name");
      }
      boolean acquired =
          perClusterAdminLocks.get(clusterName).tryLock(waitingTimeForConsumptionMs, TimeUnit.MILLISECONDS);
      if (!acquired) {
        throw new VeniceException(
            "Failed to acquire cluster level admin message execution id lock after waiting for "
                + waitingTimeForConsumptionMs
                + "ms. Another ongoing admin operation might be holding up the lock for cluster:" + clusterName);
      }
    } catch (InterruptedException e) {
      throw new VeniceException("Got interrupted during acquiring lock", e);
    }
  }

  private void releaseAdminMessageExecutionIdLock(String clusterName) {
    if (clusterName == null) {
      throw new VeniceException("Cannot release admin message execution id lock with null cluster name");
    }
    perClusterAdminLocks.get(clusterName).unlock();
  }

  /**
   * Acquire the store level lock used to ensure no other admin operation is performed on the same store while the
   * ongoing admin operation is being performed.
   * This lock is held when generating, writing and processing the admin messages for the given store.
   */
  private void acquireAdminMessageLock(String clusterName, String storeName) {
    try {
      if (clusterName == null) {
        throw new VeniceException("Cannot acquire admin message lock with a null cluster name");
      }
      if (storeName == null) {
        throw new VeniceException("Cannot acquire admin message lock with a null name");
      }
      // First check whether an exception already exist in the admin channel for the given store
      Exception lastException = getVeniceHelixAdmin().getLastExceptionForStore(clusterName, storeName);
      if (lastException != null) {
        throw new VeniceException(
            "Unable to start new admin operations for store: " + storeName + " in cluster: " + clusterName
                + " due to existing exception: " + lastException.getMessage(),
            lastException);
      }
      Lock storeAdminLock = perStoreAdminLocks.get(clusterName).computeIfAbsent(storeName, k -> new ReentrantLock());
      boolean acquired = storeAdminLock.tryLock(waitingTimeForConsumptionMs, TimeUnit.MILLISECONDS);
      if (!acquired) {
        throw new VeniceException(
            "Failed to acquire store level admin message lock after waiting for " + waitingTimeForConsumptionMs
                + "ms. Another ongoing admin operation might be holding up the lock for store:" + storeName);
      }
    } catch (InterruptedException e) {
      throw new VeniceException("Got interrupted during acquiring lock", e);
    }
  }

  private void releaseAdminMessageLock(String clusterName, String storeName) {
    if (clusterName == null) {
      throw new VeniceException("Cannot release admin message lock with null cluster name");
    }
    if (storeName == null) {
      throw new VeniceException("Cannot release admin message lock with null store name");
    }
    Lock storeAdminMessageLock = perStoreAdminLocks.get(clusterName).get(storeName);
    if (storeAdminMessageLock != null) {
      storeAdminMessageLock.unlock();
    }
  }

  /**
   * Create a store by sending {@link AdminMessageType#STORE_CREATION STORE_CREATION} admin message to the Kafka admin topic,
   * sending {@link AdminMessageType#META_SYSTEM_STORE_AUTO_CREATION_VALIDATION META_SYSTEM_STORE_AUTO_CREATION_VALIDATION} admin message,
   * and performing initialization steps for using authorize server to manage ACLs for the input store.
   */
  @Override
  public void createStore(
      String clusterName,
      String storeName,
      String owner,
      String keySchema,
      String valueSchema,
      boolean isSystemStore,
      Optional<String> accessPermissions) {
    acquireAdminMessageLock(clusterName, storeName);
    try {
      getVeniceHelixAdmin()
          .checkPreConditionForCreateStore(clusterName, storeName, keySchema, valueSchema, isSystemStore, false);
      LOGGER.info("Adding store: {} to cluster: {}", storeName, clusterName);

      // Provisioning ACL needs to be the first step in store creation process.
      provisionAclsForStore(storeName, accessPermissions, Collections.emptyList());
      sendStoreCreationAdminMessage(clusterName, storeName, owner, keySchema, valueSchema);
      /**
       * If the newly created store operation is triggered by store migration, Parent Controller will skip the system store
       * auto-materialization since the system stores will be taken care by store migration logic.
       * Otherwise, for each user store level system store, we will send admin message to validate the creation is successful.
       */
      boolean isStoreMigrating = false;
      ZkStoreConfigAccessor storeConfigAccessor =
          getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName).getStoreConfigAccessor();
      if (storeConfigAccessor.containsConfig(storeName)) {
        StoreConfig storeConfig = storeConfigAccessor.getStoreConfig(storeName);
        isStoreMigrating = !storeConfig.isDeleting() && clusterName.equals(storeConfig.getMigrationDestCluster());
        if (isStoreMigrating) {
          LOGGER.info(
              "Store: {} is migrating to cluster: {}, will skip system store auto-materialization",
              storeName,
              clusterName);
        }
      }
      // Don't materialize system stores for system stores.
      if (!isStoreMigrating && !isSystemStore) {
        for (VeniceSystemStoreType systemStoreType: getSystemStoreLifeCycleHelper()
            .materializeSystemStoresForUserStore(clusterName, storeName)) {
          LOGGER.info(
              "Materializing system store: {} for store: {} in cluster: {}",
              systemStoreType,
              storeName,
              clusterName);
          sendUserSystemStoreCreationValidationAdminMessage(clusterName, storeName, systemStoreType);
        }
      }
      if (VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE.getPrefix().equals(storeName)) {
        setupResourceForBatchJobHeartbeatStore(storeName);
      }

    } finally {
      releaseAdminMessageLock(clusterName, storeName);
    }
  }

  private void sendStoreCreationAdminMessage(
      String clusterName,
      String storeName,
      String owner,
      String keySchema,
      String valueSchema) {
    // Write store creation message to Kafka
    final StoreCreation storeCreation = (StoreCreation) AdminMessageType.STORE_CREATION.getNewInstance();
    storeCreation.clusterName = clusterName;
    storeCreation.storeName = storeName;
    storeCreation.owner = owner;
    storeCreation.keySchema = new SchemaMeta();
    storeCreation.keySchema.schemaType = SchemaType.AVRO_1_4.getValue();
    storeCreation.keySchema.definition = keySchema;
    storeCreation.valueSchema = new SchemaMeta();
    storeCreation.valueSchema.schemaType = SchemaType.AVRO_1_4.getValue();
    storeCreation.valueSchema.definition = valueSchema;

    final AdminOperation message = new AdminOperation();
    message.operationType = AdminMessageType.STORE_CREATION.getValue();
    message.payloadUnion = storeCreation;
    sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);
  }

  private void sendUserSystemStoreCreationValidationAdminMessage(
      String clusterName,
      String storeName,
      VeniceSystemStoreType systemStoreType) {
    final AdminOperation message = new AdminOperation();
    switch (systemStoreType) {
      case META_STORE:
        MetaSystemStoreAutoCreationValidation metaSystemStoreAutoCreationValidation =
            (MetaSystemStoreAutoCreationValidation) AdminMessageType.META_SYSTEM_STORE_AUTO_CREATION_VALIDATION
                .getNewInstance();
        metaSystemStoreAutoCreationValidation.clusterName = clusterName;
        metaSystemStoreAutoCreationValidation.storeName = storeName;
        message.operationType = AdminMessageType.META_SYSTEM_STORE_AUTO_CREATION_VALIDATION.getValue();
        message.payloadUnion = metaSystemStoreAutoCreationValidation;
        break;
      case DAVINCI_PUSH_STATUS_STORE:
        PushStatusSystemStoreAutoCreationValidation pushStatusSystemStoreAutoCreationValidation =
            (PushStatusSystemStoreAutoCreationValidation) AdminMessageType.PUSH_STATUS_SYSTEM_STORE_AUTO_CREATION_VALIDATION
                .getNewInstance();
        pushStatusSystemStoreAutoCreationValidation.clusterName = clusterName;
        pushStatusSystemStoreAutoCreationValidation.storeName = storeName;
        message.operationType = AdminMessageType.PUSH_STATUS_SYSTEM_STORE_AUTO_CREATION_VALIDATION.getValue();
        message.payloadUnion = pushStatusSystemStoreAutoCreationValidation;
        break;
      default:
        LOGGER.warn(
            "System store type: {} is not a user store level system store, will not send store creation "
                + "validation message.",
            systemStoreType);
        return;
    }
    LOGGER.info(
        "Sending system store creation validation message for user store: {}, system store type: {}",
        storeName,
        systemStoreType);
    sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);
  }

  private void setupResourceForBatchJobHeartbeatStore(String batchJobHeartbeatStoreName) {
    if (authorizerService.isPresent()) {
      authorizerService.get().setupResource(new Resource(batchJobHeartbeatStoreName));
      LOGGER.info("Set up wildcard ACL regex for store: {}", batchJobHeartbeatStoreName);
    } else {
      LOGGER.warn(
          "Skip setting up wildcard ACL regex for store: {} since the authorizer service is not provided",
          batchJobHeartbeatStoreName);
    }
  }

  /**
   * Delete a store by sending {@link AdminMessageType#DELETE_STORE DELETE_STORE} admin message to the Kafka admin topic and clearing all ACLs and release
   * resource for the target store from authorize service.
   */
  @Override
  public void deleteStore(
      String clusterName,
      String storeName,
      int largestUsedVersionNumber,
      boolean waitOnRTTopicDeletion) {
    acquireAdminMessageLock(clusterName, storeName);
    try {
      LOGGER.info("Deleting store: {} from cluster: {}", storeName, clusterName);
      Store store = null;
      try {
        store = getVeniceHelixAdmin().checkPreConditionForDeletion(clusterName, storeName);
      } catch (VeniceNoStoreException e) {
        // It's possible for a store to partially exist due to partial delete/creation failures.
        LOGGER.warn("Store object is missing for store: {} will proceed with the rest of store deletion", storeName);
      }
      DeleteStore deleteStore = (DeleteStore) AdminMessageType.DELETE_STORE.getNewInstance();
      deleteStore.clusterName = clusterName;
      deleteStore.storeName = storeName;
      // Tell each prod colo the largest used version number in corp to make it consistent.
      deleteStore.largestUsedVersionNumber = store == null ? Store.IGNORE_VERSION : store.getLargestUsedVersionNumber();
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.DELETE_STORE.getValue();
      message.payloadUnion = deleteStore;

      sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);

      // Deleting ACL needs to be the last step in store deletion process.
      if (store != null) {
        if (!store.isMigrating()) {
          cleanUpAclsForStore(storeName, VeniceSystemStoreType.getEnabledSystemStoreTypes(store));
        } else {
          LOGGER.info("Store: {} is migrating! Skipping acl deletion!", storeName);
        }
      } else {
        LOGGER.warn("Store object for {} is missing! Skipping acl deletion!", storeName);
      }
    } finally {
      releaseAdminMessageLock(clusterName, storeName);
    }
  }

  /**
   * @see Admin#addVersionAndStartIngestion(String, String, String, int, int, Version.PushType, String, long, int, boolean, int)
   */
  @Override
  public void addVersionAndStartIngestion(
      String clusterName,
      String storeName,
      String pushJobId,
      int versionNumber,
      int numberOfPartitions,
      Version.PushType pushType,
      String remoteKafkaBootstrapServers,
      long rewindTimeInSecondsOverride,
      int ignoredRmdVersionID,
      boolean versionSwapDeferred,
      int repushSourceVersion) {
    // Parent controller will always pick the replicationMetadataVersionId from configs.
    final int replicationMetadataVersionId = getRmdVersionID(storeName, clusterName);
    Version version = getVeniceHelixAdmin().addVersionOnly(
        clusterName,
        storeName,
        pushJobId,
        versionNumber,
        numberOfPartitions,
        pushType,
        remoteKafkaBootstrapServers,
        rewindTimeInSecondsOverride,
        replicationMetadataVersionId);
    if (version.isActiveActiveReplicationEnabled()) {
      updateReplicationMetadataSchemaForAllValueSchema(clusterName, storeName);
    }
    acquireAdminMessageLock(clusterName, storeName);
    try {
      sendAddVersionAdminMessage(clusterName, storeName, pushJobId, version, numberOfPartitions, pushType, null, -1);
    } finally {
      releaseAdminMessageLock(clusterName, storeName);
    }
  }

  private int getRmdVersionID(final String storeName, final String clusterName) {
    final Store store = getVeniceHelixAdmin().getStore(clusterName, storeName);
    if (store == null) {
      LOGGER.warn(
          "No store found in the store repository. Will get store-level RMD version ID from cluster config. "
              + "Store name: {}, cluster: {}",
          storeName,
          clusterName);
    } else if (store.getRmdVersion() == ConfigConstants.UNSPECIFIED_REPLICATION_METADATA_VERSION) {
      LOGGER.info("No store-level RMD version ID found for store {} in cluster {}", storeName, clusterName);
    } else {
      LOGGER.info(
          "Found store-level RMD version ID {} for store {} in cluster {}",
          store.getRmdVersion(),
          storeName,
          clusterName);
      return store.getRmdVersion();
    }

    final VeniceControllerClusterConfig controllerConfig = getMultiClusterConfigs().getControllerConfig(clusterName);
    if (controllerConfig == null) {
      throw new VeniceException("No controller cluster config found for cluster " + clusterName);
    }
    final int rmdVersionID = controllerConfig.getReplicationMetadataVersion();
    LOGGER.info("Use RMD version ID {} for cluster {}", rmdVersionID, clusterName);
    return rmdVersionID;
  }

  /**
   * Since there is no offline push running in Parent Controller,
   * the old store versions won't be cleaned up by job completion action, so Parent Controller chooses
   * to clean it up when the new store version gets created.
   * It is OK to clean up the old store versions in Parent Controller without notifying Child Controller since
   * store version in Parent Controller doesn't maintain actual version status, and only for tracking
   * the store version creation history.
   */
  void cleanupHistoricalVersions(String clusterName, String storeName) {
    HelixVeniceClusterResources resources = getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
      ReadWriteStoreRepository storeRepo = resources.getStoreMetadataRepository();
      Store store = storeRepo.getStore(storeName);
      if (store == null) {
        LOGGER.info("The store to clean up: {} doesn't exist", storeName);
        return;
      }
      List<Version> versions = store.getVersions();
      final int versionCount = versions.size();
      if (versionCount <= STORE_VERSION_RETENTION_COUNT) {
        return;
      }
      Map<String, Integer> currentVersionsMap = getCurrentVersionsForMultiColos(clusterName, storeName);
      List<Version> clonedVersions = new ArrayList<>(versions);
      clonedVersions.stream()
          .sorted()
          .filter(v -> !currentVersionsMap.containsValue(v.getNumber()))
          .limit(versionCount - STORE_VERSION_RETENTION_COUNT)
          .forEach(v -> store.deleteVersion(v.getNumber()));
      storeRepo.updateStore(store);
    }
  }

  /**
  * Check whether any topic for this store exists or not.
  * The existing topic could be introduced by two cases:
  * 1. The previous job push is still running;
  * 2. The previous job push fails to delete this topic;
  *
  * For the 1st case, it is expected to refuse the new data push,
  * and for the 2nd case, customer should reach out Venice team to fix this issue for now.
  **/
  List<PubSubTopic> existingVersionTopicsForStore(String storeName) {
    List<PubSubTopic> outputList = new ArrayList<>();
    TopicManager topicManager = getTopicManager();
    Set<PubSubTopic> topics = topicManager.listTopics();
    String storeNameForCurrentTopic;
    for (PubSubTopic topic: topics) {
      if (AdminTopicUtils.isAdminTopic(topic.getName()) || AdminTopicUtils.isKafkaInternalTopic(topic.getName())
          || topic.isRealTime() || VeniceView.isViewTopic(topic.getName())) {
        continue;
      }
      try {
        storeNameForCurrentTopic = Version.parseStoreFromKafkaTopicName(topic.getName());
      } catch (Exception e) {
        LOGGER.debug("Failed to parse StoreName from topic: {}, and error message: {}", topic, e.getMessage());
        continue;
      }
      if (storeNameForCurrentTopic.equals(storeName)) {
        outputList.add(topic);
      }
    }
    return outputList;
  }

  /**
   * Get the version topics list for the specified store in freshness order; the first
   * topic in the list is the latest topic and the last topic is the oldest one.
   * @param storeName
   * @return the version topics in freshness order
   */
  List<PubSubTopic> getKafkaTopicsByAge(String storeName) {
    List<PubSubTopic> existingTopics = existingVersionTopicsForStore(storeName);
    if (!existingTopics.isEmpty()) {
      existingTopics.sort((t1, t2) -> {
        int v1 = Version.parseVersionFromKafkaTopicName(t1.getName());
        int v2 = Version.parseVersionFromKafkaTopicName(t2.getName());
        return v2 - v1;
      });
    }
    return existingTopics;
  }

  /**
   * If there is no ongoing push for specified store currently, this function will return {@link Optional#empty()},
   * else will return the ongoing Kafka topic. It will also try to clean up legacy topics.
   */
  Optional<String> getTopicForCurrentPushJob(
      String clusterName,
      String storeName,
      boolean isIncrementalPush,
      boolean isRepush) {
    // The first/last topic in the list is the latest/oldest version topic
    List<PubSubTopic> versionTopics = getKafkaTopicsByAge(storeName);
    Optional<PubSubTopic> latestTopic = Optional.empty();
    if (!versionTopics.isEmpty()) {
      latestTopic = Optional.of(versionTopics.get(0));
    }

    /**
     * Check current topic retention to decide whether the previous job is already done or not
     */
    if (latestTopic.isPresent()) {
      LOGGER.debug("Latest kafka topic for store: {} is {}", storeName, latestTopic.get());
      final String latestTopicName = latestTopic.get().getName();
      int versionNumber = Version.parseVersionFromKafkaTopicName(latestTopicName);
      Store store = getStore(clusterName, storeName);
      Version version = store.getVersion(versionNumber);
      if (version != null && version.isVersionSwapDeferred()) {
        LOGGER.error(
            "There is already future version {} exists for store {}, please wait till the future version is made current.",
            versionNumber,
            storeName);
        return Optional.of(latestTopic.get().getName());
      }

      if (!isTopicTruncated(latestTopicName)) {
        /**
         * Check whether the corresponding version exists or not, since it is possible that last push
         * meets Kafka topic creation timeout.
         * When Kafka topic creation timeout happens, topic/job could be still running, but the version
         * should not exist according to the logic in {@link VeniceHelixAdmin#addVersion}.
         * However, it is possible that a different request enters this code section when the topic has been created but
         * either the version information has not been persisted to Zk or the in-memory Store object. In this case, it
         * is desirable to add a delay to topic deletion.
         *
         * If the corresponding version doesn't exist, this function will issue command to kill job to deprecate
         * the incomplete topic/job.
         */
        Pair<Store, Version> storeVersionPair =
            getVeniceHelixAdmin().waitVersion(clusterName, storeName, versionNumber, Duration.ofSeconds(30));
        if (storeVersionPair.getSecond() == null) {
          // TODO: Guard this topic deletion code using a store-level lock instead.
          Long inMemoryTopicCreationTime = getVeniceHelixAdmin().getInMemoryTopicCreationTime(latestTopicName);
          if (inMemoryTopicCreationTime != null
              && SystemTime.INSTANCE.getMilliseconds() < (inMemoryTopicCreationTime + TOPIC_DELETION_DELAY_MS)) {
            throw new VeniceException(
                "Failed to get version information but the topic exists and has been created recently. Try again after some time.");
          }

          killOfflinePush(clusterName, latestTopicName, true);
          LOGGER.info("Found topic: {} without the corresponding version, will kill it", latestTopicName);
          return Optional.empty();
        }

        /**
         * If Parent Controller could not infer the job status from topic retention policy, it will check the actual
         * job status by sending requests to each individual datacenter.
         * If the job is still running, Parent Controller will block current push.
         */
        final long SLEEP_MS_BETWEEN_RETRY = TimeUnit.SECONDS.toMillis(10);
        ExecutionStatus jobStatus = ExecutionStatus.PROGRESS;
        Map<String, String> extraInfo = new HashMap<>();

        int retryTimes = 5;
        int current = 0;
        while (current++ < retryTimes) {
          OfflinePushStatusInfo offlineJobStatus = getOffLinePushStatus(clusterName, latestTopicName);
          jobStatus = offlineJobStatus.getExecutionStatus();
          extraInfo = offlineJobStatus.getExtraInfo();
          if (!extraInfo.containsValue(ExecutionStatus.UNKNOWN.toString())) {
            break;
          }
          // Retry since there is a connection failure when querying job status against each datacenter
          try {
            timer.sleep(SLEEP_MS_BETWEEN_RETRY);
          } catch (InterruptedException e) {
            throw new VeniceException(
                "Received InterruptedException during sleep between 'getOffLinePushStatus' calls");
          }
        }
        if (extraInfo.containsValue(ExecutionStatus.UNKNOWN.toString())) {
          // TODO: Do we need to throw exception here??
          LOGGER.error(
              "Failed to get job status for topic: {} after retrying {} times, extra info: {}",
              latestTopicName,
              retryTimes,
              extraInfo);
        }
        if (!jobStatus.isTerminal()) {
          LOGGER.info(
              "Job status: {} for Kafka topic: {} is not terminal, extra info: {}",
              jobStatus,
              latestTopicName,
              extraInfo);
          if (latestTopic.isPresent()) {
            return Optional.of(latestTopic.get().getName());
          }
          return Optional.empty();
        } else {
          /**
           * If the job status of latestKafkaTopic is terminal and it is not an incremental push,
           * it will be truncated in {@link #getOffLinePushStatus(String, String)}.
           */
          if (!isIncrementalPush) {
            Map<String, Integer> currentVersionsMap = getCurrentVersionsForMultiColos(clusterName, storeName);
            truncateTopicsBasedOnMaxErroredTopicNumToKeep(
                versionTopics.stream().map(vt -> vt.getName()).collect(Collectors.toList()),
                isRepush,
                currentVersionsMap);
          }
        }
      }
    }
    return Optional.empty();
  }

  /**
   * Only keep {@link #maxErroredTopicNumToKeep} non-truncated topics ordered by version. It works as a general method
   * for cleaning up leaking topics. ({@link #maxErroredTopicNumToKeep} is always 0.)
   */
  void truncateTopicsBasedOnMaxErroredTopicNumToKeep(
      List<String> topics,
      boolean isRepush,
      Map<String, Integer> currentVersionsMap) {
    // Based on current logic, only 'errored' topics were not truncated.
    List<String> sortedNonTruncatedTopics =
        topics.stream().filter(topic -> !isTopicTruncated(topic)).sorted((t1, t2) -> {
          int v1 = Version.parseVersionFromKafkaTopicName(t1);
          int v2 = Version.parseVersionFromKafkaTopicName(t2);
          return v1 - v2;
        }).collect(Collectors.toList());
    Set<String> streamReprocessingTopics =
        sortedNonTruncatedTopics.stream().filter(Version::isStreamReprocessingTopic).collect(Collectors.toSet());
    List<String> sortedNonTruncatedVersionTopics = sortedNonTruncatedTopics.stream()
        .filter(topic -> !Version.isStreamReprocessingTopic(topic))
        .collect(Collectors.toList());
    if (sortedNonTruncatedVersionTopics.size() <= maxErroredTopicNumToKeep) {
      LOGGER.info(
          "Non-truncated version topics size: {} isn't bigger than maxErroredTopicNumToKeep: {}, so no topic "
              + "will be truncated this time",
          sortedNonTruncatedTopics.size(),
          maxErroredTopicNumToKeep);
      return;
    }
    int topicNumToTruncate = sortedNonTruncatedVersionTopics.size() - maxErroredTopicNumToKeep;
    int truncatedTopicCnt = 0;
    for (String topic: sortedNonTruncatedVersionTopics) {
      /**
       * If Venice repush somehow failed and we delete the version topic for the current version here, future incremental
       * pushes will fail; therefore, keep Venice repush transparent and don't delete any VTs; future regular batch pushes
       * from users will delete the VT we retain here.
       * Potential improvement: After the Venice repush completes, we can automatically deletes VT from previous version,
       * at the risk of not being able to roll back to previous version though, so not recommend to do such automation.
       */
      if (isRepush && currentVersionsMap.containsValue(Version.parseVersionFromVersionTopicName(topic))) {
        LOGGER.info(
            "Do not delete the current version topic: {} since the incoming push is a Venice internal re-push.",
            topic);
        continue;
      }
      if (++truncatedTopicCnt > topicNumToTruncate) {
        break;
      }
      truncateKafkaTopic(topic);
      LOGGER.info("Errored topic: {} got truncated", topic);
      String correspondingStreamReprocessingTopic = Version.composeStreamReprocessingTopicFromVersionTopic(topic);
      if (streamReprocessingTopics.contains(correspondingStreamReprocessingTopic)) {
        truncateKafkaTopic(correspondingStreamReprocessingTopic);
        LOGGER.info(
            "Corresponding stream reprocessing topic: {} also got truncated.",
            correspondingStreamReprocessingTopic);
      }
    }
  }

  /**
   * Test if the given certificate has the write-access permission for the given batch-job heartbeat store.
   * @param requesterCert X.509 certificate object.
   * @param batchJobHeartbeatStoreName name of the batch-job heartbeat store.
   * @return <code>true</code> if input certificate has write-access permission for the given store;
   *         <code>false</code> otherwise.
   * @throws AclException
   */
  @Override
  public boolean hasWritePermissionToBatchJobHeartbeatStore(
      X509Certificate requesterCert,
      String batchJobHeartbeatStoreName) throws AclException {
    if (!accessController.isPresent()) {
      throw new VeniceException(
          String.format(
              "Cannot check write permission on store %s since the access controller " + "does not present for cert %s",
              batchJobHeartbeatStoreName,
              requesterCert));
    }
    final String accessMethodName = Method.Write.name();
    // Currently write access on a Venice store needs to be checked using this hasAccessToTopic method
    final boolean hasAccess =
        accessController.get().hasAccessToTopic(requesterCert, batchJobHeartbeatStoreName, accessMethodName);
    StringBuilder sb = new StringBuilder();
    sb.append("Requester");
    sb.append(hasAccess ? " has " : " does not have ");
    sb.append(accessMethodName + " access on " + batchJobHeartbeatStoreName);
    sb.append(" with identity: ");
    sb.append(identityParser.parseIdentityFromCert(requesterCert));
    LOGGER.info(sb.toString());
    return hasAccess;
  }

  /**
   * @see Admin#isActiveActiveReplicationEnabledInAllRegion(String, String, boolean)
   */
  @Override
  public boolean isActiveActiveReplicationEnabledInAllRegion(
      String clusterName,
      String storeName,
      boolean checkCurrentVersion) {
    Map<String, ControllerClient> controllerClients = getVeniceHelixAdmin().getControllerClientMap(clusterName);
    Store store = getVeniceHelixAdmin().getStore(clusterName, storeName);

    if (!store.isActiveActiveReplicationEnabled()) {
      LOGGER.info(
          "isActiveActiveReplicationEnabledInAllRegion: {} store is not enabled for Active/Active in parent region",
          storeName);
      return false;
    }

    for (Map.Entry<String, ControllerClient> entry: controllerClients.entrySet()) {
      String region = entry.getKey();
      ControllerClient controllerClient = entry.getValue();
      StoreResponse response = controllerClient.retryableRequest(5, c -> c.getStore(storeName));
      if (response.isError()) {
        LOGGER.warn(
            "isActiveActiveReplicationEnabledInAllRegion: Could not query store from region: {} for cluster: {}. "
                + "{}. Default child AA config to true, since AA is already enabled in parent.",
            region,
            clusterName,
            response.getError());
      } else {
        if (!response.getStore().isActiveActiveReplicationEnabled()) {
          if (store.isActiveActiveReplicationEnabled()) {
            throw new VeniceException(
                String.format(
                    "Store %s doesn't have Active/Active enabled in region %s, but A/A is "
                        + "enabled in parent which indicates A/A is fully ramped",
                    storeName,
                    region));
          }
          LOGGER.info(
              "isActiveActiveReplicationEnabledInAllRegion: store: {} is not enabled for Active/Active in region: {}",
              storeName,
              region);
          return false;
        }

        /**
         * check version level config as well. In case there is no version it should be fine to return true.
         */
        if (checkCurrentVersion) {
          int currentVersion = response.getStore().getCurrentVersion();
          for (Version version: response.getStore().getVersions()) {
            if (currentVersion == version.getNumber()) {
              if (!version.isActiveActiveReplicationEnabled()) {
                LOGGER.info(
                    "isActiveActiveReplicationEnabledInAllRegion: store: {} current version: {} is not enabled "
                        + "for Active/Active in region: {}",
                    storeName,
                    version.getNumber(),
                    region);
                return false;
              }
            }
          }
        }
      }
    }
    return true;
  }

  /**
   * @see Admin#incrementVersionIdempotent(String, String, String, int, int)
   */
  @Override
  public Version incrementVersionIdempotent(
      String clusterName,
      String storeName,
      String pushJobId,
      int numberOfPartitions,
      int replicationFactor,
      Version.PushType pushType,
      boolean sendStartOfPush,
      boolean sorted,
      String compressionDictionary,
      Optional<String> sourceGridFabric,
      Optional<X509Certificate> requesterCert,
      long rewindTimeInSecondsOverride,
      Optional<String> emergencySourceRegion,
      boolean versionSwapDeferred,
      String targetedRegions,
      int repushSourceVersion) {
    Optional<String> currentPushTopic =
        getTopicForCurrentPushJob(clusterName, storeName, pushType.isIncremental(), Version.isPushIdRePush(pushJobId));

    if (currentPushTopic.isPresent()) {
      int currentPushVersion = Version.parseVersionFromKafkaTopicName(currentPushTopic.get());
      Store store = getStore(clusterName, storeName);
      Version version = store.getVersion(currentPushVersion);
      if (version == null) {
        throw new VeniceException(
            "A corresponding version should exist with the ongoing push with topic " + currentPushTopic);
      }
      String existingPushJobId = version.getPushJobId();
      if (existingPushJobId.equals(pushJobId)) {
        return version;
      }

      boolean isExistingPushJobARepush = Version.isPushIdRePush(existingPushJobId);
      boolean isIncomingPushJobARepush = Version.isPushIdRePush(pushJobId);

      // If version swap is enabled, do not check for lingering push as user may swap at much later time.
      if (!version.isVersionSwapDeferred() && getLingeringStoreVersionChecker()
          .isStoreVersionLingering(store, version, timer, this, requesterCert, identityParser)) {
        if (pushType.isIncremental()) {
          /**
           * Incremental push shouldn't kill the previous full push, there could be a transient issue that parents couldn't
           * get the right job states from child colos; once child colos recover, next incremental push should succeed.
           *
           * If the previous full push is indeed lingering, users should issue to full push to clean up the lingering job
           * instead of running incremental push.
           */
          throw new VeniceException(
              "Version " + version.getNumber() + " is not healthy in Venice backend; please "
                  + "consider running a full batch push for your store: " + storeName
                  + " before running incremental push, " + "or reach out to Venice team.");
        } else {
          // Kill the lingering version and allow the new push to start.
          LOGGER.info(
              "Found lingering topic: {} with push id: {}. Killing the lingering version that was created at: {}",
              currentPushTopic.get(),
              existingPushJobId,
              version.getCreatedTime());
          killOfflinePush(clusterName, currentPushTopic.get(), true);
        }
      } else if (isExistingPushJobARepush && !pushType.isIncremental() && !isIncomingPushJobARepush) {
        // Inc push policy INCREMENTAL_PUSH_SAME_AS_REAL_TIME with target version filtering is deprecated and not going
        // to be used.

        // Kill the existing job if incoming push type is not an inc push and also not a repush job.
        LOGGER.info(
            "Found running repush job with push id: {} and incoming push is a batch job or stream reprocessing "
                + "job with push id: {}. Killing the repush job for store: {}",
            existingPushJobId,
            pushJobId,
            storeName);
        killOfflinePush(clusterName, currentPushTopic.get(), true);
      } else if (pushType.isIncremental()) {
        // No op. Allow concurrent inc push to RT to continue when there is an ongoing batch push
        LOGGER.info(
            "Found a running batch push job: {} and incoming push: {} is an incremental push. "
                + "Letting the push continue for the store: {}",
            existingPushJobId,
            pushJobId,
            storeName);
      } else {
        String msg = version.isVersionSwapDeferred()
            ? ". There is already a future version " + version.getNumber() + " exists for the store " + storeName
                + " please make that version current before starting a next push."
            : ". An ongoing push with pushJobId " + existingPushJobId + " and topic " + currentPushTopic.get()
                + " is found and it must be terminated before another push can be started.";
        VeniceException e = new ConcurrentBatchPushException(
            "Unable to start the push with pushJobId " + pushJobId + " for store " + storeName + msg);
        e.setStackTrace(EMPTY_STACK_TRACE);
        throw e;
      }
    }

    Version newVersion;
    if (pushType.isIncremental()) {
      newVersion = getVeniceHelixAdmin().getIncrementalPushVersion(clusterName, storeName);
    } else {
      validateTargetedRegions(targetedRegions, clusterName);

      newVersion = addVersionAndTopicOnly(
          clusterName,
          storeName,
          pushJobId,
          VERSION_ID_UNSET,
          numberOfPartitions,
          replicationFactor,
          pushType,
          sendStartOfPush,
          sorted,
          compressionDictionary,
          sourceGridFabric,
          rewindTimeInSecondsOverride,
          emergencySourceRegion,
          versionSwapDeferred,
          targetedRegions,
          repushSourceVersion);
    }
    cleanupHistoricalVersions(clusterName, storeName);
    if (VeniceSystemStoreType.getSystemStoreType(storeName) == null) {
      if (pushType.isBatch()) {
        getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName)
            .getVeniceAdminStats()
            .recordSuccessfullyStartedUserBatchPushParentAdminCount();
      } else if (pushType.isIncremental()) {
        getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName)
            .getVeniceAdminStats()
            .recordSuccessfullyStartedUserIncrementalPushParentAdminCount();
      }
    }

    return newVersion;
  }

  /**
   * Validate the given targeted regions are all valid. A valid region should have a controller client present in the cluster.
   * @param targetedRegions
   * @param clusterName
   */
  private void validateTargetedRegions(String targetedRegions, String clusterName) throws VeniceException {
    if (StringUtils.isEmpty(targetedRegions)) {
      return;
    }
    Set<String> targetedRegionSet = RegionUtils.parseRegionsFilterList(targetedRegions);
    Map<String, ControllerClient> clientMap = getVeniceHelixAdmin().getControllerClientMap(clusterName);
    for (String region: targetedRegionSet) {
      if (!clientMap.containsKey(region)) {
        throw new VeniceException(
            "One of the targeted region " + region + " is not a valid region in cluster " + clusterName);
      }
    }
  }

  Version addVersionAndTopicOnly(
      String clusterName,
      String storeName,
      String pushJobId,
      int versionNumber,
      int numberOfPartitions,
      int replicationFactor,
      Version.PushType pushType,
      boolean sendStartOfPush,
      boolean sorted,
      String compressionDictionary,
      Optional<String> sourceGridFabric,
      long rewindTimeInSecondsOverride,
      Optional<String> emergencySourceRegion,
      boolean versionSwapDeferred,
      String targetedRegions,
      int repushSourceVersion) {
    final int replicationMetadataVersionId = getRmdVersionID(storeName, clusterName);
    Pair<Boolean, Version> result = getVeniceHelixAdmin().addVersionAndTopicOnly(
        clusterName,
        storeName,
        pushJobId,
        versionNumber,
        numberOfPartitions,
        replicationFactor,
        sendStartOfPush,
        sorted,
        pushType,
        compressionDictionary,
        null,
        sourceGridFabric,
        rewindTimeInSecondsOverride,
        replicationMetadataVersionId,
        emergencySourceRegion,
        versionSwapDeferred,
        targetedRegions,
        repushSourceVersion);
    Version newVersion = result.getSecond();
    if (result.getFirst()) {
      if (newVersion.isActiveActiveReplicationEnabled()) {
        updateReplicationMetadataSchemaForAllValueSchema(clusterName, storeName);
      }
      // Send admin message if the version is newly created.
      acquireAdminMessageLock(clusterName, storeName);
      try {
        sendAddVersionAdminMessage(
            clusterName,
            storeName,
            pushJobId,
            newVersion,
            numberOfPartitions,
            pushType,
            targetedRegions,
            repushSourceVersion);
      } finally {
        releaseAdminMessageLock(clusterName, storeName);
      }
      getSystemStoreLifeCycleHelper().maybeCreateSystemStoreWildcardAcl(storeName);
    }
    return newVersion;
  }

  void sendAddVersionAdminMessage(
      String clusterName,
      String storeName,
      String pushJobId,
      Version version,
      int numberOfPartitions,
      Version.PushType pushType,
      String targetedRegions,
      int repushSourceVersion) {
    AdminOperation message = new AdminOperation();
    message.operationType = AdminMessageType.ADD_VERSION.getValue();
    message.payloadUnion = getAddVersionMessage(
        clusterName,
        storeName,
        pushJobId,
        version,
        numberOfPartitions,
        pushType,
        targetedRegions,
        repushSourceVersion);
    sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);
  }

  private AddVersion getAddVersionMessage(
      String clusterName,
      String storeName,
      String pushJobId,
      Version version,
      int numberOfPartitions,
      Version.PushType pushType,
      String targetedRegions,
      int repushSourceVersion) {
    AddVersion addVersion = (AddVersion) AdminMessageType.ADD_VERSION.getNewInstance();
    addVersion.clusterName = clusterName;
    addVersion.storeName = storeName;
    addVersion.pushJobId = pushJobId;
    addVersion.versionNum = version.getNumber();
    addVersion.numberOfPartitions = numberOfPartitions;
    addVersion.pushType = pushType.getValue();
    // Check whether native replication is enabled
    if (version.isNativeReplicationEnabled()) {
      addVersion.pushStreamSourceAddress = version.getPushStreamSourceAddress();
    }
    if (version.getHybridStoreConfig() != null) {
      addVersion.rewindTimeInSecondsOverride = version.getHybridStoreConfig().getRewindTimeInSeconds();
    } else {
      // Default value, unused for non hybrid store
      addVersion.rewindTimeInSecondsOverride = -1;
    }
    if (StringUtils.isNotEmpty(targetedRegions)) {
      addVersion.targetedRegions = new ArrayList<>(RegionUtils.parseRegionsFilterList(targetedRegions));
    }
    addVersion.timestampMetadataVersionId = version.getRmdVersionId();
    addVersion.versionSwapDeferred = version.isVersionSwapDeferred();
    addVersion.repushSourceVersion = repushSourceVersion;
    return addVersion;
  }

  /**
   * @see VeniceHelixAdmin#getRealTimeTopic(String, String)
   */
  @Override
  public String getRealTimeTopic(String clusterName, String storeName) {
    return getVeniceHelixAdmin().getRealTimeTopic(clusterName, storeName);
  }

  /**
   * @see VeniceHelixAdmin#getRealTimeTopic(String, Store)
   */
  @Override
  public String getRealTimeTopic(String clusterName, Store store) {
    return getVeniceHelixAdmin().getRealTimeTopic(clusterName, store);
  }

  @Override
  public String getSeparateRealTimeTopic(String clusterName, String storeName) {
    return getVeniceHelixAdmin().getSeparateRealTimeTopic(clusterName, storeName);
  }

  /**
   * A couple of extra checks are needed in parent controller
   * 1. check batch job statuses across child controllers. (We cannot only check the version status
   * in parent controller since they are marked as STARTED)
   * 2. check if the topic is marked to be truncated or not. (This could be removed if we don't
   * preserve incremental push topic in parent Kafka anymore
   */
  @Override
  public Version getIncrementalPushVersion(String clusterName, String storeName) {
    Version incrementalPushVersion = getVeniceHelixAdmin().getIncrementalPushVersion(clusterName, storeName);
    String incrementalPushTopic = incrementalPushVersion.kafkaTopicName();
    ExecutionStatus status = getOffLinePushStatus(clusterName, incrementalPushTopic).getExecutionStatus();

    return getIncrementalPushVersion(incrementalPushVersion, status);
  }

  // This method is only for internal / test use case
  Version getIncrementalPushVersion(Version incrementalPushVersion, ExecutionStatus status) {
    String storeName = incrementalPushVersion.getStoreName();
    if (!status.isTerminal()) {
      throw new VeniceException("Cannot start incremental push since batch push is on going." + " store: " + storeName);
    }

    String incrementalPushTopic = Version.composeRealTimeTopic(storeName);
    if (status.isError() || getVeniceHelixAdmin().isTopicTruncated(incrementalPushTopic)) {
      throw new VeniceException(
          "Cannot start incremental push since previous batch push has failed. Please run another bash job."
              + " store: " + storeName);
    }
    return incrementalPushVersion;
  }

  /**
   * Unsupported operation in the parent controller.
   */
  @Override
  public int getCurrentVersion(String clusterName, String storeName) {
    throw new VeniceUnsupportedOperationException(
        "getCurrentVersion",
        "Please use getCurrentVersionsForMultiColos in Parent controller.");
  }

  /**
   * Query the current version for the given store. In parent colo, Venice do not update the current version because
   * there is not offline push monitor. So parent controller will query each prod controller and return the map.
   */
  @Override
  public Map<String, Integer> getCurrentVersionsForMultiColos(String clusterName, String storeName) {
    Map<String, ControllerClient> controllerClients = getVeniceHelixAdmin().getControllerClientMap(clusterName);
    return getCurrentVersionForMultiRegions(clusterName, storeName, controllerClients);
  }

  /**
   * @return a RepushInfo object with store information retrieved from the specified cluster and fabric.
   */
  @Override
  public RepushInfo getRepushInfo(String clusterName, String storeName, Optional<String> fabricName) {
    Map<String, ControllerClient> controllerClients = getVeniceHelixAdmin().getControllerClientMap(clusterName);
    String systemSchemaClusterName = multiClusterConfigs.getSystemSchemaClusterName();
    VeniceControllerClusterConfig systemSchemaClusterConfig =
        multiClusterConfigs.getControllerConfig(systemSchemaClusterName);

    if (fabricName.isPresent()) {
      StoreResponse response = controllerClients.get(fabricName.get()).getStore(storeName);
      if (response.isError()) {
        throw new VeniceException(
            "Could not query store from colo: " + fabricName.get() + " for cluster: " + clusterName + ". "
                + response.getError());
      }
      return RepushInfo.createRepushInfo(
          response.getStore().getVersion(response.getStore().getCurrentVersion()).get(),
          response.getStore().getKafkaBrokerUrl(),
          systemSchemaClusterConfig.getClusterToD2Map().get(systemSchemaClusterName),
          systemSchemaClusterConfig.getChildControllerD2ZkHost(fabricName.get()));
    }
    // fabricName not present, get the largest version info among the child colos.
    Map<String, Integer> currentVersionsMap =
        getCurrentVersionForMultiRegions(clusterName, storeName, controllerClients);
    int largestVersion = Integer.MIN_VALUE;
    String colo = null;
    for (Map.Entry<String, Integer> mapEntry: currentVersionsMap.entrySet()) {
      if (mapEntry.getValue() > largestVersion) {
        largestVersion = mapEntry.getValue();
        colo = mapEntry.getKey();
      }
    }
    StoreResponse response = controllerClients.get(colo).getStore(storeName);
    if (response.isError()) {
      throw new VeniceException(
          "Could not query store from largest version colo: " + fabricName.get() + " for cluster: " + clusterName + ". "
              + response.getError());
    }
    return RepushInfo.createRepushInfo(
        response.getStore().getVersion((response.getStore().getCurrentVersion())).get(),
        response.getStore().getKafkaBrokerUrl(),
        systemSchemaClusterConfig.getClusterToD2Map().get(systemSchemaClusterName),
        systemSchemaClusterConfig.getChildControllerD2ZkHost(colo));
  }

  /**
   * @see Admin#getFutureVersionsForMultiColos(String, String)
   */
  @Override
  public Map<String, String> getFutureVersionsForMultiColos(String clusterName, String storeName) {
    Map<String, ControllerClient> controllerClients = getVeniceHelixAdmin().getControllerClientMap(clusterName);
    Map<String, String> result = new HashMap<>();
    for (Map.Entry<String, ControllerClient> entry: controllerClients.entrySet()) {
      String region = entry.getKey();
      ControllerClient controllerClient = entry.getValue();
      MultiStoreStatusResponse response =
          ControllerClient.retryableRequest(controllerClient, 5, c -> c.getFutureVersions(clusterName, storeName));
      if (response.isError()) {
        LOGGER.error(
            "Could not query store from region: {} for cluster: {}. Error: {}",
            region,
            clusterName,
            response.getError());
        result.put(region, String.valueOf(IGNORED_CURRENT_VERSION));
      } else {
        result.put(region, response.getStoreStatusMap().get(storeName));
      }
    }
    return result;
  }

  @Override
  public Map<String, String> getBackupVersionsForMultiColos(String clusterName, String storeName) {
    Map<String, ControllerClient> controllerClients = getVeniceHelixAdmin().getControllerClientMap(clusterName);
    Map<String, String> result = new HashMap<>();
    for (Map.Entry<String, ControllerClient> entry: controllerClients.entrySet()) {
      String region = entry.getKey();
      ControllerClient controllerClient = entry.getValue();
      MultiStoreStatusResponse response = controllerClient.getBackupVersions(clusterName, storeName);
      if (response.isError()) {
        LOGGER.error(
            "Could not query store from region: {} for cluster: {}. Error: {}",
            region,
            clusterName,
            response.getError());
        result.put(region, String.valueOf(IGNORED_CURRENT_VERSION));
      } else {
        result.put(region, response.getStoreStatusMap().get(storeName));
      }
    }
    return result;
  }

  /**
   * Unsupported operation in the parent controller and returns {@linkplain Store#NON_EXISTING_VERSION}.
   */
  @Override
  public int getFutureVersion(String clusterName, String storeName) {
    return Store.NON_EXISTING_VERSION;
  }

  @Override
  public int getBackupVersion(String clusterName, String storeName) {
    return Store.NON_EXISTING_VERSION;
  }

  Map<String, Integer> getCurrentVersionForMultiRegions(
      String clusterName,
      String storeName,
      Map<String, ControllerClient> controllerClients) {
    Map<String, Integer> result = new HashMap<>();
    for (Map.Entry<String, ControllerClient> entry: controllerClients.entrySet()) {
      String region = entry.getKey();
      ControllerClient controllerClient = entry.getValue();
      StoreResponse response = controllerClient.getStore(storeName);
      if (response.isError()) {
        LOGGER.error(
            "Could not query store from region: {} for cluster: {}. Error: {}",
            region,
            clusterName,
            response.getError());
        result.put(region, IGNORED_CURRENT_VERSION);
      } else {
        result.put(region, response.getStore().getCurrentVersion());
      }
    }
    return result;
  }

  /**
   * Unsupported operation in the parent controller.
   */
  @Override
  public Version peekNextVersion(String clusterName, String storeName) {
    throw new VeniceUnsupportedOperationException("peekNextVersion");
  }

  /**
   * @see Admin#deleteAllVersionsInStore(String, String)
   */
  @Override
  public List<Version> deleteAllVersionsInStore(String clusterName, String storeName) {
    acquireAdminMessageLock(clusterName, storeName);
    try {
      getVeniceHelixAdmin().checkPreConditionForDeletion(clusterName, storeName);

      DeleteAllVersions deleteAllVersions = (DeleteAllVersions) AdminMessageType.DELETE_ALL_VERSIONS.getNewInstance();
      deleteAllVersions.clusterName = clusterName;
      deleteAllVersions.storeName = storeName;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.DELETE_ALL_VERSIONS.getValue();
      message.payloadUnion = deleteAllVersions;

      sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);
      return Collections.emptyList();
    } finally {
      releaseAdminMessageLock(clusterName, storeName);
    }
  }

  /**
   * @see Admin#deleteOldVersionInStore(String, String, int)
   */
  @Override
  public void deleteOldVersionInStore(String clusterName, String storeName, int versionNum) {
    acquireAdminMessageLock(clusterName, storeName);
    try {
      getVeniceHelixAdmin().checkPreConditionForSingleVersionDeletion(clusterName, storeName, versionNum);

      DeleteOldVersion deleteOldVersion = (DeleteOldVersion) AdminMessageType.DELETE_OLD_VERSION.getNewInstance();
      deleteOldVersion.clusterName = clusterName;
      deleteOldVersion.storeName = storeName;
      deleteOldVersion.versionNum = versionNum;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.DELETE_OLD_VERSION.getValue();
      message.payloadUnion = deleteOldVersion;

      sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);
    } finally {
      releaseAdminMessageLock(clusterName, storeName);
    }
  }

  /**
   * @return all versions of the specified store from a cluster.
   */
  @Override
  public List<Version> versionsForStore(String clusterName, String storeName) {
    return getVeniceHelixAdmin().versionsForStore(clusterName, storeName);
  }

  /**
   * @return all stores in the specified cluster.
   */
  @Override
  public List<Store> getAllStores(String clusterName) {
    return getVeniceHelixAdmin().getAllStores(clusterName);
  }

  /**
   * Unsupported operation in the parent controller.
   */
  @Override
  public Map<String, String> getAllStoreStatuses(String clusterName) {
    throw new VeniceUnsupportedOperationException("getAllStoreStatuses");
  }

  /**
   * @return <code>Store</code> object reference from the input store name.
   */
  @Override
  public Store getStore(String clusterName, String storeName) {
    return getVeniceHelixAdmin().getStore(clusterName, storeName);
  }

  /**
   * @see VeniceHelixAdmin#hasStore(String, String)
   */
  @Override
  public boolean hasStore(String clusterName, String storeName) {
    return getVeniceHelixAdmin().hasStore(clusterName, storeName);
  }

  /**
   * Unsupported operation in the parent controller.
   */
  @Override
  public void setStoreCurrentVersion(String clusterName, String storeName, int versionNumber) {
    throw new VeniceUnsupportedOperationException(
        "setStoreCurrentVersion",
        "Please use set-version only on child controllers, "
            + "setting version on parent is not supported, since the version list could be different fabric by fabric");
  }

  @Override
  public void rollForwardToFutureVersion(String clusterName, String storeName, String regionFilter) {
    acquireAdminMessageLock(clusterName, storeName);
    try {
      getVeniceHelixAdmin().checkPreConditionForUpdateStoreMetadata(clusterName, storeName);
      // Send admin message to set backup version as current version. Child controllers will execute the admin message.
      RollForwardCurrentVersion rollForwardCurrentVersion =
          (RollForwardCurrentVersion) AdminMessageType.ROLLFORWARD_CURRENT_VERSION.getNewInstance();
      rollForwardCurrentVersion.clusterName = clusterName;
      rollForwardCurrentVersion.storeName = storeName;
      rollForwardCurrentVersion.regionsFilter = regionFilter;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.ROLLFORWARD_CURRENT_VERSION.getValue();
      message.payloadUnion = rollForwardCurrentVersion;

      Map<String, String> futureVersions = getFutureVersionsForMultiColos(clusterName, storeName);
      int futureVersion = 0;
      for (Map.Entry<String, String> entry: futureVersions.entrySet()) {
        futureVersion = Integer.parseInt(entry.getValue());
        if (futureVersion > 0) {
          break;
        }
      }
      sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);
      LOGGER.info("Truncating topic {} after rollforward", Version.composeKafkaTopic(storeName, futureVersion));
      truncateKafkaTopic(Version.composeKafkaTopic(storeName, futureVersion));
    } finally {
      releaseAdminMessageLock(clusterName, storeName);
    }
  }

  @FunctionalInterface
  interface VersionProvider {
    int getVersion(StoreInfo storeInfo);
  }

  /**
   * Set backup version as current version in all child regions.
   */
  @Override
  public void rollbackToBackupVersion(String clusterName, String storeName, String regionFilter) {
    acquireAdminMessageLock(clusterName, storeName);
    try {
      getVeniceHelixAdmin().checkPreConditionForUpdateStoreMetadata(clusterName, storeName);
      // Send admin message to set backup version as current version. Child controllers will execute the admin message.
      RollbackCurrentVersion rollbackCurrentVersion =
          (RollbackCurrentVersion) AdminMessageType.ROLLBACK_CURRENT_VERSION.getNewInstance();
      rollbackCurrentVersion.clusterName = clusterName;
      rollbackCurrentVersion.storeName = storeName;
      rollbackCurrentVersion.regionsFilter = regionFilter;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.ROLLBACK_CURRENT_VERSION.getValue();
      message.payloadUnion = rollbackCurrentVersion;

      sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);
    } finally {
      releaseAdminMessageLock(clusterName, storeName);
    }
  }

  /**
   * Unsupported operation in the parent controller.
   */
  @Override
  public void setStoreLargestUsedVersion(String clusterName, String storeName, int versionNumber) {
    throw new VeniceUnsupportedOperationException(
        "setStoreLargestUsedVersion",
        "This is only supported in the Child Controller.");
  }

  /**
   * Update the owner of a specified store by sending {@link AdminMessageType#SET_STORE_OWNER SET_STORE_OWNER} admin message
   * to the admin topic.
   */
  @Override
  public void setStoreOwner(String clusterName, String storeName, String owner) {
    acquireAdminMessageLock(clusterName, storeName);
    try {
      getVeniceHelixAdmin().checkPreConditionForUpdateStoreMetadata(clusterName, storeName);

      SetStoreOwner setStoreOwner = (SetStoreOwner) AdminMessageType.SET_STORE_OWNER.getNewInstance();
      setStoreOwner.clusterName = clusterName;
      setStoreOwner.storeName = storeName;
      setStoreOwner.owner = owner;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.SET_STORE_OWNER.getValue();
      message.payloadUnion = setStoreOwner;

      sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);
    } finally {
      releaseAdminMessageLock(clusterName, storeName);
    }
  }

  /**
   * Update the partition count of a specified store by sending {@link AdminMessageType#SET_STORE_PARTITION SET_STORE_PARTITION}
   * admin message to the admin topic.
   */
  @Override
  public void setStorePartitionCount(String clusterName, String storeName, int partitionCount) {
    acquireAdminMessageLock(clusterName, storeName);
    try {
      getVeniceHelixAdmin().checkPreConditionForUpdateStoreMetadata(clusterName, storeName);

      int maxPartitionNum =
          getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName).getConfig().getMaxNumberOfPartitions();
      if (partitionCount > maxPartitionNum) {
        throw new ConfigurationException(
            "Partition count: " + partitionCount + " should be less than max: " + maxPartitionNum);
      }
      if (partitionCount < 0) {
        throw new ConfigurationException("Partition count: " + partitionCount + " should NOT be negative");
      }

      SetStorePartitionCount setStorePartition =
          (SetStorePartitionCount) AdminMessageType.SET_STORE_PARTITION.getNewInstance();
      setStorePartition.clusterName = clusterName;
      setStorePartition.storeName = storeName;
      setStorePartition.partitionNum = partitionCount;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.SET_STORE_PARTITION.getValue();
      message.payloadUnion = setStorePartition;

      sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);
    } finally {
      releaseAdminMessageLock(clusterName, storeName);
    }
  }

  /**
   * Update the readability of a specified store by sending {@link AdminMessageType#ENABLE_STORE_READ ENABLE_STORE_READ}
   * or {@link AdminMessageType#DISABLE_STORE_READ DISABLE_STORE_READ} admin message.
   */
  @Override
  public void setStoreReadability(String clusterName, String storeName, boolean desiredReadability) {
    acquireAdminMessageLock(clusterName, storeName);
    try {
      getVeniceHelixAdmin().checkPreConditionForUpdateStoreMetadata(clusterName, storeName);

      AdminOperation message = new AdminOperation();

      if (desiredReadability) {
        message.operationType = AdminMessageType.ENABLE_STORE_READ.getValue();
        EnableStoreRead enableStoreRead = (EnableStoreRead) AdminMessageType.ENABLE_STORE_READ.getNewInstance();
        enableStoreRead.clusterName = clusterName;
        enableStoreRead.storeName = storeName;
        message.payloadUnion = enableStoreRead;
      } else {
        message.operationType = AdminMessageType.DISABLE_STORE_READ.getValue();
        DisableStoreRead disableStoreRead = (DisableStoreRead) AdminMessageType.DISABLE_STORE_READ.getNewInstance();
        disableStoreRead.clusterName = clusterName;
        disableStoreRead.storeName = storeName;
        message.payloadUnion = disableStoreRead;
      }

      sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);
    } finally {
      releaseAdminMessageLock(clusterName, storeName);
    }
  }

  /**
   * Update the writability of a specified store by sending {@link AdminMessageType#ENABLE_STORE_WRITE ENABLE_STORE_WRITE}
   * or {@link AdminMessageType#DISABLE_STORE_WRITE DISABLE_STORE_WRITE} admin message.
   */
  @Override
  public void setStoreWriteability(String clusterName, String storeName, boolean desiredWriteability) {
    acquireAdminMessageLock(clusterName, storeName);
    try {
      getVeniceHelixAdmin().checkPreConditionForUpdateStoreMetadata(clusterName, storeName);

      AdminOperation message = new AdminOperation();

      if (desiredWriteability) {
        message.operationType = AdminMessageType.ENABLE_STORE_WRITE.getValue();
        ResumeStore resumeStore = (ResumeStore) AdminMessageType.ENABLE_STORE_WRITE.getNewInstance();
        resumeStore.clusterName = clusterName;
        resumeStore.storeName = storeName;
        message.payloadUnion = resumeStore;
      } else {
        message.operationType = AdminMessageType.DISABLE_STORE_WRITE.getValue();
        PauseStore pauseStore = (PauseStore) AdminMessageType.DISABLE_STORE_WRITE.getNewInstance();
        pauseStore.clusterName = clusterName;
        pauseStore.storeName = storeName;
        message.payloadUnion = pauseStore;
      }

      sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);
    } finally {
      releaseAdminMessageLock(clusterName, storeName);
    }
  }

  /**
   * Update both readability and writability of a specified store.
   */
  @Override
  public void setStoreReadWriteability(String clusterName, String storeName, boolean isAccessible) {
    setStoreReadability(clusterName, storeName, isAccessible);
    setStoreWriteability(clusterName, storeName, isAccessible);
  }

  /**
   * Update a target store properties by first applying the provided deltas and then sending
   * {@link AdminMessageType#UPDATE_STORE UPDATE_STORE} admin message.
   * @param clusterName name of the Venice cluster.
   * @param storeName name of the to-be-updated store.
   * @param params to-be-updated store properties.
   */
  @Override
  public void updateStore(String clusterName, String storeName, UpdateStoreQueryParams params) {
    acquireAdminMessageLock(clusterName, storeName);
    try {
      Optional<String> owner = params.getOwner();
      Optional<Boolean> readability = params.getEnableReads();
      Optional<Boolean> writeability = params.getEnableWrites();
      Optional<Integer> partitionCount = params.getPartitionCount();
      Optional<String> partitionerClass = params.getPartitionerClass();
      Optional<Map<String, String>> partitionerParams = params.getPartitionerParams();
      Optional<Integer> amplificationFactor = params.getAmplificationFactor();
      Optional<Long> storageQuotaInByte = params.getStorageQuotaInByte();
      Optional<Long> readQuotaInCU = params.getReadQuotaInCU();
      Optional<Integer> currentVersion = params.getCurrentVersion();
      Optional<Integer> largestUsedVersionNumber = params.getLargestUsedVersionNumber();
      Optional<Long> hybridRewindSeconds = params.getHybridRewindSeconds();
      Optional<Long> hybridOffsetLagThreshold = params.getHybridOffsetLagThreshold();
      Optional<Long> hybridTimeLagThreshold = params.getHybridTimeLagThreshold();
      Optional<DataReplicationPolicy> hybridDataReplicationPolicy = params.getHybridDataReplicationPolicy();
      Optional<BufferReplayPolicy> hybridBufferReplayPolicy = params.getHybridBufferReplayPolicy();
      Optional<String> realTimeTopicName = params.getRealTimeTopicName();
      Optional<Boolean> accessControlled = params.getAccessControlled();
      Optional<CompressionStrategy> compressionStrategy = params.getCompressionStrategy();
      Optional<Boolean> clientDecompressionEnabled = params.getClientDecompressionEnabled();
      Optional<Boolean> chunkingEnabled = params.getChunkingEnabled();
      Optional<Boolean> rmdChunkingEnabled = params.getRmdChunkingEnabled();
      Optional<Integer> batchGetLimit = params.getBatchGetLimit();
      Optional<Integer> numVersionsToPreserve = params.getNumVersionsToPreserve();
      Optional<Boolean> incrementalPushEnabled = params.getIncrementalPushEnabled();
      Optional<Boolean> separateRealTimeTopicEnabled = params.getSeparateRealTimeTopicEnabled();
      Optional<Boolean> storeMigration = params.getStoreMigration();
      Optional<Boolean> writeComputationEnabled = params.getWriteComputationEnabled();
      Optional<Integer> replicationMetadataVersionID = params.getReplicationMetadataVersionID();
      Optional<Boolean> readComputationEnabled = params.getReadComputationEnabled();
      Optional<Integer> bootstrapToOnlineTimeoutInHours = params.getBootstrapToOnlineTimeoutInHours();
      Optional<BackupStrategy> backupStrategy = params.getBackupStrategy();
      Optional<Boolean> autoSchemaRegisterPushJobEnabled = params.getAutoSchemaRegisterPushJobEnabled();
      Optional<Boolean> hybridStoreDiskQuotaEnabled = params.getHybridStoreDiskQuotaEnabled();
      Optional<Boolean> regularVersionETLEnabled = params.getRegularVersionETLEnabled();
      Optional<Boolean> futureVersionETLEnabled = params.getFutureVersionETLEnabled();
      Optional<String> etledUserProxyAccount = params.getETLedProxyUserAccount();
      Optional<Boolean> nativeReplicationEnabled = params.getNativeReplicationEnabled();
      Optional<String> pushStreamSourceAddress = params.getPushStreamSourceAddress();
      Optional<Long> backupVersionRetentionMs = params.getBackupVersionRetentionMs();
      Optional<Integer> replicationFactor = params.getReplicationFactor();
      Optional<Boolean> migrationDuplicateStore = params.getMigrationDuplicateStore();
      Optional<String> nativeReplicationSourceFabric = params.getNativeReplicationSourceFabric();
      Optional<Boolean> activeActiveReplicationEnabled = params.getActiveActiveReplicationEnabled();
      Optional<String> regionsFilter = params.getRegionsFilter();
      Optional<String> personaName = params.getStoragePersona();
      Optional<Map<String, String>> storeViewConfig = params.getStoreViews();
      Optional<String> viewName = params.getViewName();
      Optional<String> viewClassName = params.getViewClassName();
      Optional<Map<String, String>> viewParams = params.getViewClassParams();
      Optional<Boolean> removeView = params.getDisableStoreView();
      Optional<Integer> latestSupersetSchemaId = params.getLatestSupersetSchemaId();
      Optional<Boolean> unusedSchemaDeletionEnabled = params.getUnusedSchemaDeletionEnabled();

      /**
       * Check whether parent controllers will only propagate the update configs to child controller, or all unchanged
       * configs should be replicated to children too.
       */
      Optional<Boolean> replicateAll = params.getReplicateAllConfigs();
      Optional<Boolean> storageNodeReadQuotaEnabled = params.getStorageNodeReadQuotaEnabled();
      Optional<Long> minCompactionLagSeconds = params.getMinCompactionLagSeconds();
      Optional<Long> maxCompactionLagSeconds = params.getMaxCompactionLagSeconds();
      Optional<Integer> maxRecordSizeBytes = params.getMaxRecordSizeBytes();
      Optional<Integer> maxNearlineRecordSizeBytes = params.getMaxNearlineRecordSizeBytes();

      boolean replicateAllConfigs = replicateAll.isPresent() && replicateAll.get();
      List<CharSequence> updatedConfigsList = new LinkedList<>();
      String errorMessagePrefix = "Store update error for " + storeName + " in cluster: " + clusterName + ": ";

      Store currStore = getVeniceHelixAdmin().getStore(clusterName, storeName);
      if (currStore == null) {
        LOGGER.error(errorMessagePrefix + "store does not exist, and thus cannot be updated.");
        throw new VeniceNoStoreException(storeName, clusterName);
      }
      UpdateStore setStore = (UpdateStore) AdminMessageType.UPDATE_STORE.getNewInstance();
      setStore.clusterName = clusterName;
      setStore.storeName = storeName;
      setStore.owner = owner.map(addToUpdatedConfigList(updatedConfigsList, OWNER)).orElseGet(currStore::getOwner);

      boolean isUpdateForStoreMigration = storeMigration.orElse(false);
      if (!isUpdateForStoreMigration && !currStore.isHybrid()
          && (hybridRewindSeconds.isPresent() || hybridOffsetLagThreshold.isPresent())) {
        // Today target colo pushjob cannot handle hybrid stores, so if a batch push is running, fail the request
        Optional<String> currentPushTopic = getTopicForCurrentPushJob(clusterName, storeName, false, false);
        if (currentPushTopic.isPresent()) {
          String errorMessage =
              "Cannot convert to hybrid as there is already a pushjob running with topic " + currentPushTopic.get();
          LOGGER.error(errorMessage);
          throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, errorMessage, ErrorType.BAD_REQUEST);
        }
      }
      // Invalid config update on hybrid will not be populated to admin channel so subsequent updates on the store won't
      // be blocked by retry mechanism.
      if (currStore.isHybrid() && (partitionerClass.isPresent() || partitionerParams.isPresent())) {
        String errorMessage = errorMessagePrefix + "Cannot change partitioner class and parameters for hybrid stores";
        LOGGER.error(errorMessage);
        throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, errorMessage, ErrorType.BAD_REQUEST);
      }

      if (partitionCount.isPresent()) {
        getVeniceHelixAdmin().preCheckStorePartitionCountUpdate(clusterName, currStore, partitionCount.get());
        setStore.partitionNum = partitionCount.get();
        updatedConfigsList.add(PARTITION_COUNT);
      } else {
        setStore.partitionNum = currStore.getPartitionCount();
      }

      /**
       * TODO: We should build an UpdateStoreHelper that takes current store config and update command as input, and
       *       return whether the update command is valid.
       */
      validateActiveActiveReplicationEnableConfigs(activeActiveReplicationEnabled, nativeReplicationEnabled, currStore);

      setStore.nativeReplicationEnabled =
          nativeReplicationEnabled.map(addToUpdatedConfigList(updatedConfigsList, NATIVE_REPLICATION_ENABLED))
              .orElseGet(currStore::isNativeReplicationEnabled);
      setStore.pushStreamSourceAddress =
          pushStreamSourceAddress.map(addToUpdatedConfigList(updatedConfigsList, PUSH_STREAM_SOURCE_ADDRESS))
              .orElseGet(currStore::getPushStreamSourceAddress);

      if (storeViewConfig.isPresent() && viewName.isPresent()) {
        throw new VeniceException("Cannot update a store view and overwrite store view setup together!");
      }
      if (viewName.isPresent()) {
        Map<String, StoreViewConfigRecord> updatedViewSettings;
        if (!removeView.isPresent()) {
          if (!viewClassName.isPresent()) {
            throw new VeniceException("View class name is required when configuring a view.");
          }
          // If View parameter is not provided, use emtpy map instead. It does not inherit from existing config.
          ViewConfig viewConfig = new ViewConfigImpl(viewClassName.get(), viewParams.orElse(Collections.emptyMap()));
          ViewConfig validatedViewConfig = validateAndDecorateStoreViewConfig(currStore, viewConfig, viewName.get());
          updatedViewSettings =
              VeniceHelixAdmin.addNewViewConfigsIntoOldConfigs(currStore, viewName.get(), validatedViewConfig);
        } else {
          updatedViewSettings = VeniceHelixAdmin.removeViewConfigFromStoreViewConfigMap(currStore, viewName.get());
        }
        setStore.views = updatedViewSettings;
        updatedConfigsList.add(STORE_VIEW);
      }

      if (storeViewConfig.isPresent()) {
        // Validate and overwrite store views if they're getting set
        Map<String, ViewConfig> validatedViewConfigs =
            validateAndDecorateStoreViewConfigs(storeViewConfig.get(), currStore);
        setStore.views = StoreViewUtils.convertViewConfigMapToStoreViewRecordMap(validatedViewConfigs);
        updatedConfigsList.add(STORE_VIEW);
      }

      // Only update fields that are set, other fields will be read from the original store's partitioner config.
      PartitionerConfig updatedPartitionerConfig = VeniceHelixAdmin.mergeNewSettingsIntoOldPartitionerConfig(
          currStore,
          partitionerClass,
          partitionerParams,
          amplificationFactor);
      if (partitionerClass.isPresent() || partitionerParams.isPresent() || amplificationFactor.isPresent()) {
        // Update updatedConfigsList.
        partitionerClass.ifPresent(p -> updatedConfigsList.add(PARTITIONER_CLASS));
        partitionerParams.ifPresent(p -> updatedConfigsList.add(PARTITIONER_PARAMS));
        amplificationFactor.ifPresent(p -> updatedConfigsList.add(AMPLIFICATION_FACTOR));
        // Create PartitionConfigRecord for admin channel transmission.
        PartitionerConfigRecord partitionerConfigRecord = new PartitionerConfigRecord();
        partitionerConfigRecord.partitionerClass = updatedPartitionerConfig.getPartitionerClass();
        partitionerConfigRecord.partitionerParams =
            CollectionUtils.getCharSequenceMapFromStringMap(updatedPartitionerConfig.getPartitionerParams());
        partitionerConfigRecord.amplificationFactor = updatedPartitionerConfig.getAmplificationFactor();
        // Before setting partitioner config, verify the updated partitionerConfig can be built
        try {
          PartitionUtils.getVenicePartitioner(
              partitionerConfigRecord.partitionerClass.toString(),
              new VeniceProperties(partitionerConfigRecord.partitionerParams),
              getKeySchema(clusterName, storeName).getSchema());
        } catch (PartitionerSchemaMismatchException e) {
          String errorMessage = errorMessagePrefix + e.getMessage();
          LOGGER.error(errorMessage);
          throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, errorMessage, ErrorType.INVALID_SCHEMA);
        } catch (Exception e) {
          String errorMessage = errorMessagePrefix + "Partitioner Configs invalid, please verify that partitioner "
              + "configs like classpath and parameters are correct!";
          LOGGER.error(errorMessage);
          throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, errorMessage, ErrorType.INVALID_CONFIG);
        }
        setStore.partitionerConfig = partitionerConfigRecord;
      }

      setStore.enableReads =
          readability.map(addToUpdatedConfigList(updatedConfigsList, ENABLE_READS)).orElseGet(currStore::isEnableReads);
      setStore.enableWrites = writeability.map(addToUpdatedConfigList(updatedConfigsList, ENABLE_WRITES))
          .orElseGet(currStore::isEnableWrites);

      setStore.readQuotaInCU = readQuotaInCU.map(addToUpdatedConfigList(updatedConfigsList, READ_QUOTA_IN_CU))
          .orElseGet(currStore::getReadQuotaInCU);

      // We need to be careful when handling currentVersion.
      // Since it is not synced between parent and local controller,
      // It is very likely to override local values unintentionally.
      setStore.currentVersion =
          currentVersion.map(addToUpdatedConfigList(updatedConfigsList, VERSION)).orElse(IGNORED_CURRENT_VERSION);

      hybridRewindSeconds.map(addToUpdatedConfigList(updatedConfigsList, REWIND_TIME_IN_SECONDS));
      hybridOffsetLagThreshold.map(addToUpdatedConfigList(updatedConfigsList, OFFSET_LAG_TO_GO_ONLINE));
      hybridTimeLagThreshold.map(addToUpdatedConfigList(updatedConfigsList, TIME_LAG_TO_GO_ONLINE));
      hybridDataReplicationPolicy.map(addToUpdatedConfigList(updatedConfigsList, DATA_REPLICATION_POLICY));
      hybridBufferReplayPolicy.map(addToUpdatedConfigList(updatedConfigsList, BUFFER_REPLAY_POLICY));
      realTimeTopicName.map(addToUpdatedConfigList(updatedConfigsList, REAL_TIME_TOPIC_NAME));
      HybridStoreConfig updatedHybridStoreConfig = VeniceHelixAdmin.mergeNewSettingsIntoOldHybridStoreConfig(
          currStore,
          hybridRewindSeconds,
          hybridOffsetLagThreshold,
          hybridTimeLagThreshold,
          hybridDataReplicationPolicy,
          hybridBufferReplayPolicy,
          realTimeTopicName);

      // Get VeniceControllerClusterConfig for the cluster
      VeniceControllerClusterConfig controllerConfig =
          veniceHelixAdmin.getHelixVeniceClusterResources(clusterName).getConfig();
      // Check if the store is being converted to a hybrid store
      boolean storeBeingConvertedToHybrid = !currStore.isHybrid() && updatedHybridStoreConfig != null
          && veniceHelixAdmin.isHybrid(updatedHybridStoreConfig);
      // Check if the store is being converted to a batch store
      boolean storeBeingConvertedToBatch = currStore.isHybrid() && !veniceHelixAdmin.isHybrid(updatedHybridStoreConfig);
      if (storeBeingConvertedToBatch && activeActiveReplicationEnabled.orElse(false)) {
        throw new VeniceHttpException(
            HttpStatus.SC_BAD_REQUEST,
            "Cannot convert store to batch-only and enable Active/Active together.",
            ErrorType.BAD_REQUEST);
      }
      if (storeBeingConvertedToBatch && incrementalPushEnabled.orElse(false)) {
        throw new VeniceHttpException(
            HttpStatus.SC_BAD_REQUEST,
            "Cannot convert store to batch-only and enable incremental push together.",
            ErrorType.BAD_REQUEST);
      }
      // Update active-active replication config.
      setStore.activeActiveReplicationEnabled = activeActiveReplicationEnabled
          .map(addToUpdatedConfigList(updatedConfigsList, ACTIVE_ACTIVE_REPLICATION_ENABLED))
          .orElseGet(currStore::isActiveActiveReplicationEnabled);
      // Enable active-active replication automatically when batch user store being converted to hybrid store and
      // active-active replication is enabled for all hybrid store via the cluster config
      if (storeBeingConvertedToHybrid && !setStore.activeActiveReplicationEnabled && !currStore.isSystemStore()
          && controllerConfig.isActiveActiveReplicationEnabledAsDefaultForHybrid()) {
        setStore.activeActiveReplicationEnabled = true;
        updatedConfigsList.add(ACTIVE_ACTIVE_REPLICATION_ENABLED);
      }
      // When turning off hybrid store, we will also turn off A/A store config.
      if (storeBeingConvertedToBatch && setStore.activeActiveReplicationEnabled) {
        setStore.activeActiveReplicationEnabled = false;
        updatedConfigsList.add(ACTIVE_ACTIVE_REPLICATION_ENABLED);
      }

      // Update incremental push config.
      setStore.incrementalPushEnabled =
          incrementalPushEnabled.map(addToUpdatedConfigList(updatedConfigsList, INCREMENTAL_PUSH_ENABLED))
              .orElseGet(currStore::isIncrementalPushEnabled);
      // Enable incremental push automatically when batch user store being converted to hybrid store and active-active
      // replication is enabled or being and the cluster config allows it.
      if (!setStore.incrementalPushEnabled && !currStore.isSystemStore() && storeBeingConvertedToHybrid
          && setStore.activeActiveReplicationEnabled
          && controllerConfig.enabledIncrementalPushForHybridActiveActiveUserStores()) {
        setStore.incrementalPushEnabled = true;
        updatedConfigsList.add(INCREMENTAL_PUSH_ENABLED);
      }
      // Enable separate real-time topic automatically when incremental push is enabled and cluster config allows it.
      if (setStore.incrementalPushEnabled
          && controllerConfig.enabledSeparateRealTimeTopicForStoreWithIncrementalPush()) {
        setStore.separateRealTimeTopicEnabled = true;
        updatedConfigsList.add(SEPARATE_REAL_TIME_TOPIC_ENABLED);
      }

      // When turning off hybrid store, we will also turn off incremental store config.
      if (storeBeingConvertedToBatch && setStore.incrementalPushEnabled) {
        setStore.incrementalPushEnabled = false;
        updatedConfigsList.add(INCREMENTAL_PUSH_ENABLED);
      }

      if (updatedHybridStoreConfig == null) {
        setStore.hybridStoreConfig = null;
      } else {
        HybridStoreConfigRecord hybridStoreConfigRecord = new HybridStoreConfigRecord();
        hybridStoreConfigRecord.offsetLagThresholdToGoOnline =
            updatedHybridStoreConfig.getOffsetLagThresholdToGoOnline();
        hybridStoreConfigRecord.rewindTimeInSeconds = updatedHybridStoreConfig.getRewindTimeInSeconds();
        hybridStoreConfigRecord.producerTimestampLagThresholdToGoOnlineInSeconds =
            updatedHybridStoreConfig.getProducerTimestampLagThresholdToGoOnlineInSeconds();
        hybridStoreConfigRecord.dataReplicationPolicy = updatedHybridStoreConfig.getDataReplicationPolicy().getValue();
        hybridStoreConfigRecord.bufferReplayPolicy = updatedHybridStoreConfig.getBufferReplayPolicy().getValue();
        hybridStoreConfigRecord.realTimeTopicName = updatedHybridStoreConfig.getRealTimeTopicName();
        setStore.hybridStoreConfig = hybridStoreConfigRecord;
      }

      if (incrementalPushEnabled.orElse(currStore.isIncrementalPushEnabled())
          && !veniceHelixAdmin.isHybrid(currStore.getHybridStoreConfig())
          && !veniceHelixAdmin.isHybrid(updatedHybridStoreConfig)) {
        LOGGER.info(
            "Enabling incremental push for a batch store:{}. Converting it to a hybrid store with default configs.",
            storeName);
        HybridStoreConfigRecord hybridStoreConfigRecord = new HybridStoreConfigRecord();
        hybridStoreConfigRecord.rewindTimeInSeconds = DEFAULT_REWIND_TIME_IN_SECONDS;
        updatedConfigsList.add(REWIND_TIME_IN_SECONDS);
        hybridStoreConfigRecord.offsetLagThresholdToGoOnline = DEFAULT_HYBRID_OFFSET_LAG_THRESHOLD;
        updatedConfigsList.add(OFFSET_LAG_TO_GO_ONLINE);
        hybridStoreConfigRecord.producerTimestampLagThresholdToGoOnlineInSeconds = DEFAULT_HYBRID_TIME_LAG_THRESHOLD;
        updatedConfigsList.add(TIME_LAG_TO_GO_ONLINE);
        hybridStoreConfigRecord.dataReplicationPolicy = DataReplicationPolicy.NON_AGGREGATE.getValue();
        updatedConfigsList.add(DATA_REPLICATION_POLICY);
        hybridStoreConfigRecord.bufferReplayPolicy = BufferReplayPolicy.REWIND_FROM_EOP.getValue();
        updatedConfigsList.add(BUFFER_REPLAY_POLICY);
        hybridStoreConfigRecord.realTimeTopicName = DEFAULT_REAL_TIME_TOPIC_NAME;
        setStore.hybridStoreConfig = hybridStoreConfigRecord;
      }

      /**
       * Set storage quota according to store properties. For hybrid stores, rocksDB has the overhead ratio as we
       * do append-only and compaction will happen later.
       * We expose actual disk usage to users, instead of multiplying/dividing the overhead ratio by situations.
       */
      setStore.storageQuotaInByte =
          storageQuotaInByte.map(addToUpdatedConfigList(updatedConfigsList, STORAGE_QUOTA_IN_BYTE))
              .orElseGet(currStore::getStorageQuotaInByte);

      setStore.accessControlled = accessControlled.map(addToUpdatedConfigList(updatedConfigsList, ACCESS_CONTROLLED))
          .orElseGet(currStore::isAccessControlled);
      setStore.compressionStrategy =
          compressionStrategy.map(addToUpdatedConfigList(updatedConfigsList, COMPRESSION_STRATEGY))
              .map(CompressionStrategy::getValue)
              .orElse(currStore.getCompressionStrategy().getValue());
      setStore.clientDecompressionEnabled =
          clientDecompressionEnabled.map(addToUpdatedConfigList(updatedConfigsList, CLIENT_DECOMPRESSION_ENABLED))
              .orElseGet(currStore::getClientDecompressionEnabled);
      setStore.batchGetLimit = batchGetLimit.map(addToUpdatedConfigList(updatedConfigsList, BATCH_GET_LIMIT))
          .orElseGet(currStore::getBatchGetLimit);
      setStore.numVersionsToPreserve =
          numVersionsToPreserve.map(addToUpdatedConfigList(updatedConfigsList, NUM_VERSIONS_TO_PRESERVE))
              .orElseGet(currStore::getNumVersionsToPreserve);
      setStore.isMigrating =
          storeMigration.map(addToUpdatedConfigList(updatedConfigsList, STORE_MIGRATION, ENABLE_STORE_MIGRATION))

              .orElseGet(currStore::isMigrating);
      setStore.replicationMetadataVersionID = replicationMetadataVersionID
          .map(addToUpdatedConfigList(updatedConfigsList, REPLICATION_METADATA_PROTOCOL_VERSION_ID))
          .orElse(currStore.getRmdVersion());
      setStore.readComputationEnabled =
          readComputationEnabled.map(addToUpdatedConfigList(updatedConfigsList, READ_COMPUTATION_ENABLED))
              .orElseGet(currStore::isReadComputationEnabled);
      setStore.bootstrapToOnlineTimeoutInHours = bootstrapToOnlineTimeoutInHours
          .map(addToUpdatedConfigList(updatedConfigsList, BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS))
          .orElseGet(currStore::getBootstrapToOnlineTimeoutInHours);
      setStore.leaderFollowerModelEnabled = true; // do not mess up during upgrades
      setStore.backupStrategy = (backupStrategy.map(addToUpdatedConfigList(updatedConfigsList, BACKUP_STRATEGY))
          .orElse(currStore.getBackupStrategy())).ordinal();

      setStore.schemaAutoRegisterFromPushJobEnabled = autoSchemaRegisterPushJobEnabled
          .map(addToUpdatedConfigList(updatedConfigsList, AUTO_SCHEMA_REGISTER_FOR_PUSHJOB_ENABLED))
          .orElse(currStore.isSchemaAutoRegisterFromPushJobEnabled());

      setStore.hybridStoreDiskQuotaEnabled =
          hybridStoreDiskQuotaEnabled.map(addToUpdatedConfigList(updatedConfigsList, HYBRID_STORE_DISK_QUOTA_ENABLED))
              .orElse(currStore.isHybridStoreDiskQuotaEnabled());

      regularVersionETLEnabled.map(addToUpdatedConfigList(updatedConfigsList, REGULAR_VERSION_ETL_ENABLED));
      futureVersionETLEnabled.map(addToUpdatedConfigList(updatedConfigsList, FUTURE_VERSION_ETL_ENABLED));
      etledUserProxyAccount.map(addToUpdatedConfigList(updatedConfigsList, ETLED_PROXY_USER_ACCOUNT));
      setStore.ETLStoreConfig = mergeNewSettingIntoOldETLStoreConfig(
          currStore,
          regularVersionETLEnabled,
          futureVersionETLEnabled,
          etledUserProxyAccount);

      setStore.largestUsedVersionNumber =
          largestUsedVersionNumber.map(addToUpdatedConfigList(updatedConfigsList, LARGEST_USED_VERSION_NUMBER))
              .orElseGet(currStore::getLargestUsedVersionNumber);

      setStore.backupVersionRetentionMs =
          backupVersionRetentionMs.map(addToUpdatedConfigList(updatedConfigsList, BACKUP_VERSION_RETENTION_MS))
              .orElseGet(currStore::getBackupVersionRetentionMs);
      setStore.replicationFactor = replicationFactor.map(addToUpdatedConfigList(updatedConfigsList, REPLICATION_FACTOR))
          .orElseGet(currStore::getReplicationFactor);
      setStore.migrationDuplicateStore =
          migrationDuplicateStore.map(addToUpdatedConfigList(updatedConfigsList, MIGRATION_DUPLICATE_STORE))
              .orElseGet(currStore::isMigrationDuplicateStore);
      setStore.nativeReplicationSourceFabric = nativeReplicationSourceFabric
          .map(addToUpdatedConfigList(updatedConfigsList, NATIVE_REPLICATION_SOURCE_FABRIC))
          .orElseGet((currStore::getNativeReplicationSourceFabric));

      setStore.disableMetaStore =
          params.disableMetaStore().map(addToUpdatedConfigList(updatedConfigsList, DISABLE_META_STORE)).orElse(false);

      setStore.disableDavinciPushStatusStore = params.disableDavinciPushStatusStore()
          .map(addToUpdatedConfigList(updatedConfigsList, DISABLE_DAVINCI_PUSH_STATUS_STORE))
          .orElse(false);

      setStore.storagePersona = personaName.map(addToUpdatedConfigList(updatedConfigsList, PERSONA_NAME)).orElse(null);

      setStore.blobTransferEnabled = params.getBlobTransferEnabled()
          .map(addToUpdatedConfigList(updatedConfigsList, BLOB_TRANSFER_ENABLED))
          .orElseGet(currStore::isBlobTransferEnabled);

      setStore.separateRealTimeTopicEnabled =
          separateRealTimeTopicEnabled.map(addToUpdatedConfigList(updatedConfigsList, SEPARATE_REAL_TIME_TOPIC_ENABLED))
              .orElseGet(currStore::isSeparateRealTimeTopicEnabled);

      setStore.nearlineProducerCompressionEnabled = params.getNearlineProducerCompressionEnabled()
          .map(addToUpdatedConfigList(updatedConfigsList, NEARLINE_PRODUCER_COMPRESSION_ENABLED))
          .orElseGet(currStore::isNearlineProducerCompressionEnabled);

      setStore.nearlineProducerCountPerWriter = params.getNearlineProducerCountPerWriter()
          .map(addToUpdatedConfigList(updatedConfigsList, NEARLINE_PRODUCER_COUNT_PER_WRITER))
          .orElseGet(currStore::getNearlineProducerCountPerWriter);

      setStore.targetSwapRegion = params.getTargetSwapRegion()
          .map(addToUpdatedConfigList(updatedConfigsList, TARGET_SWAP_REGION))
          .orElseGet(currStore::getTargetSwapRegion);

      setStore.targetSwapRegionWaitTime = params.getTargetRegionSwapWaitTime()
          .map(addToUpdatedConfigList(updatedConfigsList, TARGET_SWAP_REGION_WAIT_TIME))
          .orElseGet((currStore::getTargetSwapRegionWaitTime));

      setStore.isDaVinciHeartBeatReported = params.getIsDavinciHeartbeatReported()
          .map(addToUpdatedConfigList(updatedConfigsList, IS_DAVINCI_HEARTBEAT_REPORTED))
          .orElseGet((currStore::getIsDavinciHeartbeatReported));

      // Check whether the passed param is valid or not
      if (latestSupersetSchemaId.isPresent()) {
        if (latestSupersetSchemaId.get() != SchemaData.INVALID_VALUE_SCHEMA_ID) {
          if (veniceHelixAdmin.getValueSchema(clusterName, storeName, latestSupersetSchemaId.get()) == null) {
            throw new VeniceException(
                "Unknown value schema id: " + latestSupersetSchemaId.get() + " in store: " + storeName);
          }
        }
      }
      setStore.latestSuperSetValueSchemaId =
          latestSupersetSchemaId.map(addToUpdatedConfigList(updatedConfigsList, LATEST_SUPERSET_SCHEMA_ID))
              .orElseGet(currStore::getLatestSuperSetValueSchemaId);
      setStore.storageNodeReadQuotaEnabled =
          storageNodeReadQuotaEnabled.map(addToUpdatedConfigList(updatedConfigsList, STORAGE_NODE_READ_QUOTA_ENABLED))
              .orElseGet(currStore::isStorageNodeReadQuotaEnabled);
      setStore.unusedSchemaDeletionEnabled =
          unusedSchemaDeletionEnabled.map(addToUpdatedConfigList(updatedConfigsList, UNUSED_SCHEMA_DELETION_ENABLED))
              .orElseGet(currStore::isUnusedSchemaDeletionEnabled);
      setStore.minCompactionLagSeconds =
          minCompactionLagSeconds.map(addToUpdatedConfigList(updatedConfigsList, MIN_COMPACTION_LAG_SECONDS))
              .orElseGet(currStore::getMinCompactionLagSeconds);
      setStore.maxCompactionLagSeconds =
          maxCompactionLagSeconds.map(addToUpdatedConfigList(updatedConfigsList, MAX_COMPACTION_LAG_SECONDS))
              .orElseGet(currStore::getMaxCompactionLagSeconds);
      if (setStore.maxCompactionLagSeconds < setStore.minCompactionLagSeconds) {
        throw new VeniceException(
            "Store's max compaction lag seconds: " + setStore.maxCompactionLagSeconds + " shouldn't be smaller than "
                + "store's min compaction lag seconds: " + setStore.minCompactionLagSeconds);
      }
      setStore.maxRecordSizeBytes =
          maxRecordSizeBytes.map(addToUpdatedConfigList(updatedConfigsList, MAX_RECORD_SIZE_BYTES))
              .orElseGet(currStore::getMaxRecordSizeBytes);
      setStore.maxNearlineRecordSizeBytes =
          maxNearlineRecordSizeBytes.map(addToUpdatedConfigList(updatedConfigsList, MAX_NEARLINE_RECORD_SIZE_BYTES))
              .orElseGet(currStore::getMaxNearlineRecordSizeBytes);

      StoragePersonaRepository repository =
          getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName).getStoragePersonaRepository();
      StoragePersona personaToValidate = null;
      StoragePersona existingPersona = repository.getPersonaContainingStore(currStore.getName());

      if (params.getStoragePersona().isPresent()) {
        personaToValidate = getVeniceHelixAdmin().getStoragePersona(clusterName, params.getStoragePersona().get());
        if (personaToValidate == null) {
          String errMsg = "UpdateStore command failed for store " + storeName + ".  The provided StoragePersona "
              + params.getStoragePersona().get() + " does not exist.";
          throw new VeniceException(errMsg);
        }
      } else if (existingPersona != null) {
        personaToValidate = existingPersona;
      }

      if (personaToValidate != null) {
        /**
         * Create a new copy of the store with an updated quota, and validate this.
         */
        Store updatedQuotaStore = getVeniceHelixAdmin().getStore(clusterName, storeName);
        updatedQuotaStore.setStorageQuotaInByte(setStore.getStorageQuotaInByte());
        repository.validateAddUpdatedStore(personaToValidate, Optional.of(updatedQuotaStore));
      }

      /**
       * Fabrics filter is not a store config, so we don't need to add it into {@link UpdateStore#updatedConfigsList}
       */
      setStore.regionsFilter = regionsFilter.orElse(null);

      // Update Partial Update config.
      boolean partialUpdateConfigUpdated = ParentControllerConfigUpdateUtils.checkAndMaybeApplyPartialUpdateConfig(
          this,
          clusterName,
          storeName,
          writeComputationEnabled,
          setStore,
          storeBeingConvertedToHybrid);
      if (partialUpdateConfigUpdated) {
        updatedConfigsList.add(WRITE_COMPUTATION_ENABLED);
      }
      boolean partialUpdateJustEnabled = setStore.writeComputationEnabled && !currStore.isWriteComputationEnabled();
      // Update Chunking config.
      boolean chunkingConfigUpdated = ParentControllerConfigUpdateUtils
          .checkAndMaybeApplyChunkingConfigChange(this, clusterName, storeName, chunkingEnabled, setStore);
      if (chunkingConfigUpdated) {
        updatedConfigsList.add(CHUNKING_ENABLED);
      }

      // Update RMD Chunking config.
      boolean rmdChunkingConfigUpdated = ParentControllerConfigUpdateUtils
          .checkAndMaybeApplyRmdChunkingConfigChange(this, clusterName, storeName, rmdChunkingEnabled, setStore);
      if (rmdChunkingConfigUpdated) {
        updatedConfigsList.add(RMD_CHUNKING_ENABLED);
      }

      // Validate Amplification Factor config based on latest A/A and partial update status.
      if ((setStore.getActiveActiveReplicationEnabled() || setStore.getWriteComputationEnabled())
          && updatedPartitionerConfig.getAmplificationFactor() > 1) {
        throw new VeniceHttpException(
            HttpStatus.SC_BAD_REQUEST,
            "Non-default amplification factor is not compatible with active-active replication and/or partial update.",
            ErrorType.BAD_REQUEST);
      }

      if (!getVeniceHelixAdmin().isHybrid(currStore.getHybridStoreConfig())
          && getVeniceHelixAdmin().isHybrid(setStore.getHybridStoreConfig()) && setStore.getPartitionNum() == 0) {
        // This is a new hybrid store and partition count is not specified.
        VeniceControllerClusterConfig config =
            getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName).getConfig();
        setStore.setPartitionNum(
            PartitionUtils.calculatePartitionCount(
                storeName,
                setStore.getStorageQuotaInByte(),
                0,
                config.getPartitionSize(),
                config.getMinNumberOfPartitionsForHybrid(),
                config.getMaxNumberOfPartitions(),
                config.isPartitionCountRoundUpEnabled(),
                config.getPartitionCountRoundUpSize()));
        LOGGER.info(
            "Enforcing default hybrid partition count:{} for a new hybrid store:{}.",
            setStore.getPartitionNum(),
            storeName);
        updatedConfigsList.add(PARTITION_COUNT);
      }

      /**
       * By default, parent controllers will not try to replicate the unchanged store configs to child controllers;
       * an updatedConfigsList will be used to represent which configs are updated by users.
       */
      setStore.replicateAllConfigs = replicateAllConfigs;
      if (!replicateAllConfigs) {
        if (updatedConfigsList.isEmpty()) {
          String errMsg =
              "UpdateStore command failed for store " + storeName + ". The command didn't change any specific"
                  + " store config and didn't specify \"--replicate-all-configs\" flag.";
          LOGGER.error(errMsg);
          throw new VeniceException(errMsg);
        }
        setStore.updatedConfigsList = new ArrayList<>(updatedConfigsList);
      } else {
        setStore.updatedConfigsList = Collections.emptyList();
      }

      final boolean readComputeJustEnabled =
          readComputationEnabled.orElse(false) && !currStore.isReadComputationEnabled();
      boolean needToGenerateSupersetSchema =
          !currStore.isSystemStore() && (readComputeJustEnabled || partialUpdateJustEnabled);
      if (needToGenerateSupersetSchema) {
        // dry run to make sure superset schema generation can work
        getSupersetSchemaGenerator(clusterName)
            .generateSupersetSchemaFromSchemas(getValueSchemas(clusterName, storeName));
      }

      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.UPDATE_STORE.getValue();
      message.payloadUnion = setStore;
      sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);

      if (needToGenerateSupersetSchema) {
        addSupersetSchemaForStore(clusterName, storeName, currStore.isActiveActiveReplicationEnabled());
      }
      if (partialUpdateJustEnabled) {
        LOGGER.info("Enabling partial update for the first time on store: {} in cluster: {}", storeName, clusterName);
        addUpdateSchemaForStore(this, clusterName, storeName, false);
      }

      /**
       * If active-active replication is getting enabled for the store, generate and register the Replication metadata schema
       * for all existing value schemas.
       */
      final boolean activeActiveReplicationJustEnabled =
          activeActiveReplicationEnabled.orElse(false) && !currStore.isActiveActiveReplicationEnabled();
      if (activeActiveReplicationJustEnabled) {
        updateReplicationMetadataSchemaForAllValueSchema(clusterName, storeName);
      }
    } finally {
      releaseAdminMessageLock(clusterName, storeName);
    }
  }

  private Map<String, ViewConfig> validateAndDecorateStoreViewConfigs(Map<String, String> stringMap, Store store) {
    Map<String, ViewConfig> configs = StoreViewUtils.convertStringMapViewToViewConfigMap(stringMap);
    Map<String, ViewConfig> validatedConfigs = new HashMap<>();
    for (Map.Entry<String, ViewConfig> viewConfigEntry: configs.entrySet()) {
      ViewConfig validatedViewConfig =
          validateAndDecorateStoreViewConfig(store, viewConfigEntry.getValue(), viewConfigEntry.getKey());
      validatedConfigs.put(viewConfigEntry.getKey(), validatedViewConfig);
    }
    return validatedConfigs;
  }

  private ViewConfig validateAndDecorateStoreViewConfig(Store store, ViewConfig viewConfig, String viewName) {
    // TODO: Pass a proper properties object here. Today this isn't used in this context
    if (viewConfig.getViewClassName().equals(MaterializedView.class.getCanonicalName())) {
      if (viewName.contains(VERSION_SEPARATOR)) {
        throw new VeniceException(
            String.format("Materialized View name cannot contain version separator: %s", VERSION_SEPARATOR));
      }
      Map<String, String> viewParams = viewConfig.getViewParameters();
      viewParams.put(ViewParameterKeys.MATERIALIZED_VIEW_NAME.name(), viewName);
      if (!viewParams.containsKey(ViewParameterKeys.MATERIALIZED_VIEW_PARTITIONER.name())) {
        viewParams.put(
            ViewParameterKeys.MATERIALIZED_VIEW_PARTITIONER.name(),
            store.getPartitionerConfig().getPartitionerClass());
        if (!store.getPartitionerConfig().getPartitionerParams().isEmpty()) {
          try {
            viewParams.put(
                ViewParameterKeys.MATERIALIZED_VIEW_PARTITIONER_PARAMS.name(),
                ObjectMapperFactory.getInstance()
                    .writeValueAsString(store.getPartitionerConfig().getPartitionerParams()));
          } catch (JsonProcessingException e) {
            throw new VeniceException("Failed to convert store partitioner params to string", e);
          }
        }
      }
      if (!viewParams.containsKey(ViewParameterKeys.MATERIALIZED_VIEW_PARTITION_COUNT.name())) {
        viewParams.put(
            ViewParameterKeys.MATERIALIZED_VIEW_PARTITION_COUNT.name(),
            Integer.toString(store.getPartitionCount()));
      }
      viewConfig.setViewParameters(viewParams);
    }
    VeniceView view =
        ViewUtils.getVeniceView(viewConfig.getViewClassName(), new Properties(), store, viewConfig.getViewParameters());
    view.validateConfigs();
    return viewConfig;
  }

  private SupersetSchemaGenerator getSupersetSchemaGenerator(String clusterName) {
    if (externalSupersetSchemaGenerator.isPresent() && getMultiClusterConfigs().getControllerConfig(clusterName)
        .isParentExternalSupersetSchemaGenerationEnabled()) {
      return externalSupersetSchemaGenerator.get();
    }
    return defaultSupersetSchemaGenerator;
  }

  private void addSupersetSchemaForStore(String clusterName, String storeName, boolean activeActiveReplicationEnabled) {
    // Generate a superset schema and add it.
    SchemaEntry supersetSchemaEntry = getSupersetSchemaGenerator(clusterName)
        .generateSupersetSchemaFromSchemas(getValueSchemas(clusterName, storeName));
    final Schema supersetSchema = supersetSchemaEntry.getSchema();
    final int supersetSchemaID = supersetSchemaEntry.getId();
    addValueSchemaEntry(clusterName, storeName, supersetSchema.toString(), supersetSchemaID, true);

    if (activeActiveReplicationEnabled) {
      updateReplicationMetadataSchema(clusterName, storeName, supersetSchema, supersetSchemaID);
    }
  }

  /**
   * @see VeniceHelixAdmin#updateClusterConfig(String, UpdateClusterConfigQueryParams)
   */
  @Override
  public void updateClusterConfig(String clusterName, UpdateClusterConfigQueryParams params) {
    getVeniceHelixAdmin().updateClusterConfig(clusterName, params);
  }

  private void validateActiveActiveReplicationEnableConfigs(
      Optional<Boolean> activeActiveReplicationEnabledOptional,
      Optional<Boolean> nativeReplicationEnabledOptional,
      Store store) {
    final boolean activeActiveReplicationEnabled = activeActiveReplicationEnabledOptional.orElse(false);
    if (!activeActiveReplicationEnabled) {
      return;
    }

    final boolean nativeReplicationEnabled = nativeReplicationEnabledOptional.isPresent()
        ? nativeReplicationEnabledOptional.get()
        : store.isNativeReplicationEnabled();

    if (!nativeReplicationEnabled) {
      throw new VeniceHttpException(
          HttpStatus.SC_BAD_REQUEST,
          "Active/Active Replication cannot be enabled for store " + store.getName()
              + " since Native Replication is not enabled on it.",
          ErrorType.INVALID_CONFIG);
    }
  }

  /**
   * @see VeniceHelixAdmin#getStorageEngineOverheadRatio(String)
   */
  @Override
  public double getStorageEngineOverheadRatio(String clusterName) {
    return getVeniceHelixAdmin().getStorageEngineOverheadRatio(clusterName);
  }

  /**
   * @see VeniceHelixAdmin#getKeySchema(String, String)
   */
  @Override
  public SchemaEntry getKeySchema(String clusterName, String storeName) {
    return getVeniceHelixAdmin().getKeySchema(clusterName, storeName);
  }

  /**
   * @see VeniceHelixAdmin#getValueSchemas(String, String)
   */
  @Override
  public Collection<SchemaEntry> getValueSchemas(String clusterName, String storeName) {
    return getVeniceHelixAdmin().getValueSchemas(clusterName, storeName);
  }

  /**
   * @see VeniceHelixAdmin#getDerivedSchemas(String, String)
   */
  @Override
  public Collection<DerivedSchemaEntry> getDerivedSchemas(String clusterName, String storeName) {
    return getVeniceHelixAdmin().getDerivedSchemas(clusterName, storeName);
  }

  /**
   * @see VeniceHelixAdmin#getValueSchemaId(String, String, String)
   */
  @Override
  public int getValueSchemaId(String clusterName, String storeName, String valueSchemaStr) {
    return getVeniceHelixAdmin().getValueSchemaId(clusterName, storeName, valueSchemaStr);
  }

  /**
   * @see VeniceHelixAdmin#getDerivedSchemaId(String, String, String)
   */
  @Override
  public GeneratedSchemaID getDerivedSchemaId(String clusterName, String storeName, String schemaStr) {
    return getVeniceHelixAdmin().getDerivedSchemaId(clusterName, storeName, schemaStr);
  }

  /**
   * @see VeniceHelixAdmin#getValueSchema(String, String, int)
   */
  @Override
  public SchemaEntry getValueSchema(String clusterName, String storeName, int id) {
    return getVeniceHelixAdmin().getValueSchema(clusterName, storeName, id);
  }

  /**
   * Add a new value schema for the given store with all specified properties by sending a
   * {@link AdminMessageType#VALUE_SCHEMA_CREATION VALUE_SCHEMA_CREATION} admin message.
   * @return an <code>SchemaEntry</code> object composed of a schema and its corresponding id.
   */
  @Override
  public SchemaEntry addValueSchema(
      String clusterName,
      String storeName,
      String newValueSchemaStr,
      DirectionalSchemaCompatibilityType expectedCompatibilityType) {
    acquireAdminMessageLock(clusterName, storeName);
    try {
      final int newValueSchemaId = getVeniceHelixAdmin().checkPreConditionForAddValueSchemaAndGetNewSchemaId(
          clusterName,
          storeName,
          newValueSchemaStr,
          expectedCompatibilityType);

      /**
       * If we find this is an exactly duplicate schema, return the existing schema id;
       * else add the schema with possible doc field change.
       */
      if (newValueSchemaId == SchemaData.DUPLICATE_VALUE_SCHEMA_CODE) {
        return new SchemaEntry(
            getVeniceHelixAdmin().getValueSchemaId(clusterName, storeName, newValueSchemaStr),
            newValueSchemaStr);
      }

      return addValueSchema(clusterName, storeName, newValueSchemaStr, newValueSchemaId, expectedCompatibilityType);
    } finally {
      releaseAdminMessageLock(clusterName, storeName);
    }
  }

  private SchemaEntry addValueAndSupersetSchemaEntries(
      String clusterName,
      String storeName,
      SchemaEntry newValueSchemaEntry,
      SchemaEntry newSupersetSchemaEntry,
      final boolean isWriteComputationEnabled) {
    validateNewSupersetAndValueSchemaEntries(storeName, clusterName, newValueSchemaEntry, newSupersetSchemaEntry);
    LOGGER.info(
        "Adding value schema {} and superset schema {} to store: {} in cluster: {}",
        newValueSchemaEntry,
        newSupersetSchemaEntry,
        storeName,
        clusterName);

    SupersetSchemaCreation supersetSchemaCreation =
        (SupersetSchemaCreation) AdminMessageType.SUPERSET_SCHEMA_CREATION.getNewInstance();
    supersetSchemaCreation.clusterName = clusterName;
    supersetSchemaCreation.storeName = storeName;
    SchemaMeta valueSchemaMeta = new SchemaMeta();
    valueSchemaMeta.definition = newValueSchemaEntry.getSchemaStr();
    valueSchemaMeta.schemaType = SchemaType.AVRO_1_4.getValue();
    supersetSchemaCreation.valueSchema = valueSchemaMeta;
    supersetSchemaCreation.valueSchemaId = newValueSchemaEntry.getId();

    SchemaMeta supersetSchemaMeta = new SchemaMeta();
    supersetSchemaMeta.definition = newSupersetSchemaEntry.getSchemaStr();
    supersetSchemaMeta.schemaType = SchemaType.AVRO_1_4.getValue();
    supersetSchemaCreation.supersetSchema = supersetSchemaMeta;
    supersetSchemaCreation.supersetSchemaId = newSupersetSchemaEntry.getId();

    AdminOperation message = new AdminOperation();
    message.operationType = AdminMessageType.SUPERSET_SCHEMA_CREATION.getValue();
    message.payloadUnion = supersetSchemaCreation;

    sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);
    // Need to add RMD schemas for both new value schema and new superset schema.
    updateReplicationMetadataSchema(
        clusterName,
        storeName,
        newValueSchemaEntry.getSchema(),
        newValueSchemaEntry.getId());
    updateReplicationMetadataSchema(
        clusterName,
        storeName,
        newSupersetSchemaEntry.getSchema(),
        newSupersetSchemaEntry.getId());
    if (isWriteComputationEnabled) {
      Schema newValueWriteComputeSchema =
          writeComputeSchemaConverter.convertFromValueRecordSchema(newValueSchemaEntry.getSchema());
      Schema newSuperSetWriteComputeSchema =
          writeComputeSchemaConverter.convertFromValueRecordSchema(newSupersetSchemaEntry.getSchema());
      addDerivedSchema(clusterName, storeName, newValueSchemaEntry.getId(), newValueWriteComputeSchema.toString());
      addDerivedSchema(
          clusterName,
          storeName,
          newSupersetSchemaEntry.getId(),
          newSuperSetWriteComputeSchema.toString());
    }
    updateStore(
        clusterName,
        storeName,
        new UpdateStoreQueryParams().setLatestSupersetSchemaId(newSupersetSchemaEntry.getId()));
    return newValueSchemaEntry;
  }

  private void validateNewSupersetAndValueSchemaEntries(
      String storeName,
      String clusterName,
      SchemaEntry newValueSchemaEntry,
      SchemaEntry newSupersetSchemaEntry) {
    if (newValueSchemaEntry.getId() == newSupersetSchemaEntry.getId()) {
      throw new IllegalArgumentException(
          String.format(
              "Superset schema ID and value schema ID are expected to be different for store %s in cluster %s. "
                  + "Got ID: %d",
              storeName,
              clusterName,
              newValueSchemaEntry.getId()));
    }
    if (AvroSchemaUtils
        .compareSchemaIgnoreFieldOrder(newValueSchemaEntry.getSchema(), newSupersetSchemaEntry.getSchema())) {
      throw new IllegalArgumentException(
          String.format(
              "Superset and value schemas are expected to be different for store %s in cluster %s. Got schema: %s",
              storeName,
              clusterName,
              newValueSchemaEntry.getSchema()));
    }
  }

  private SchemaEntry addValueSchemaEntry(
      String clusterName,
      String storeName,
      String valueSchemaStr,
      final int newValueSchemaId,
      final boolean doUpdateSupersetSchemaID) {
    LOGGER.info("Adding value schema: {} to store: {} in cluster: {}", valueSchemaStr, storeName, clusterName);

    ValueSchemaCreation valueSchemaCreation =
        (ValueSchemaCreation) AdminMessageType.VALUE_SCHEMA_CREATION.getNewInstance();
    valueSchemaCreation.clusterName = clusterName;
    valueSchemaCreation.storeName = storeName;
    SchemaMeta schemaMeta = new SchemaMeta();
    schemaMeta.definition = valueSchemaStr;
    schemaMeta.schemaType = SchemaType.AVRO_1_4.getValue();
    valueSchemaCreation.schema = schemaMeta;
    valueSchemaCreation.schemaId = newValueSchemaId;

    AdminOperation message = new AdminOperation();
    message.operationType = AdminMessageType.VALUE_SCHEMA_CREATION.getValue();
    message.payloadUnion = valueSchemaCreation;
    sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);

    // defensive code checking
    int actualValueSchemaId = getValueSchemaId(clusterName, storeName, valueSchemaStr);
    if (actualValueSchemaId != newValueSchemaId) {
      throw new VeniceException(
          "Something bad happens, the expected new value schema id is: " + newValueSchemaId + ", but got: "
              + actualValueSchemaId);
    }

    if (doUpdateSupersetSchemaID) {
      updateStore(clusterName, storeName, new UpdateStoreQueryParams().setLatestSupersetSchemaId(newValueSchemaId));
    }

    return new SchemaEntry(actualValueSchemaId, valueSchemaStr);
  }

  /**
   * Unsupported operation in the parent controller.
   */
  @Override
  public SchemaEntry addSupersetSchema(
      String clusterName,
      String storeName,
      String valueSchemaStr,
      int valueSchemaId,
      String supersetSchemaStr,
      int supersetSchemaId) {
    throw new VeniceUnsupportedOperationException("addSupersetSchema");
  }

  @Override
  public SchemaEntry addValueSchema(
      String clusterName,
      String storeName,
      String newValueSchemaStr,
      int schemaId,
      DirectionalSchemaCompatibilityType expectedCompatibilityType) {
    acquireAdminMessageLock(clusterName, storeName);
    try {
      Schema newValueSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(newValueSchemaStr);

      final Store store = getVeniceHelixAdmin().getStore(clusterName, storeName);
      Schema existingValueSchema = getVeniceHelixAdmin().getSupersetOrLatestValueSchema(clusterName, store);

      final boolean doUpdateSupersetSchemaID;
      if (existingValueSchema != null && (store.isReadComputationEnabled() || store.isWriteComputationEnabled())) {
        SupersetSchemaGenerator supersetSchemaGenerator = getSupersetSchemaGenerator(clusterName);
        Schema newSuperSetSchema = supersetSchemaGenerator.generateSupersetSchema(existingValueSchema, newValueSchema);
        String newSuperSetSchemaStr = newSuperSetSchema.toString();
        if (supersetSchemaGenerator.compareSchema(newSuperSetSchema, newValueSchema)) {
          doUpdateSupersetSchemaID = true;

        } else if (supersetSchemaGenerator.compareSchema(newSuperSetSchema, existingValueSchema)) {
          doUpdateSupersetSchemaID = false;

        } else if (store.isSystemStore()) {
          /**
           * Do not register superset schema for system store for now. Because some system stores specify the schema ID
           * explicitly, which may conflict with the superset schema generated internally, the new value schema registration
           * could fail.
           *
           * TODO: Design a long-term plan.
           */
          doUpdateSupersetSchemaID = false;

        } else {
          // Register superset schema only if it does not match with existing or new schema.

          // validate compatibility of the new superset schema
          getVeniceHelixAdmin().checkPreConditionForAddValueSchemaAndGetNewSchemaId(
              clusterName,
              storeName,
              newSuperSetSchemaStr,
              expectedCompatibilityType);
          // Check if the superset schema already exists or not. If exists use the same ID, else bump the value ID by
          // one.
          int supersetSchemaId = getVeniceHelixAdmin().getValueSchemaIdIgnoreFieldOrder(
              clusterName,
              storeName,
              newSuperSetSchemaStr,
              (s1, s2) -> supersetSchemaGenerator.compareSchema(s1, s2) ? 0 : 1);
          if (supersetSchemaId == SchemaData.INVALID_VALUE_SCHEMA_ID) {
            supersetSchemaId = schemaId + 1;
          }
          return addValueAndSupersetSchemaEntries(
              clusterName,
              storeName,
              new SchemaEntry(schemaId, newValueSchema),
              new SchemaEntry(supersetSchemaId, newSuperSetSchema),
              store.isWriteComputationEnabled());
        }
      } else {
        doUpdateSupersetSchemaID = false;
      }
      SchemaEntry addedSchemaEntry =
          addValueSchemaEntry(clusterName, storeName, newValueSchemaStr, schemaId, doUpdateSupersetSchemaID);

      /**
       * if active-active replication is enabled for the store then generate and register the new Replication metadata schema
       * for this newly added value schema.
       */
      if (store.isActiveActiveReplicationEnabled()) {
        Schema latestValueSchema = getVeniceHelixAdmin().getSupersetOrLatestValueSchema(clusterName, store);
        final int valueSchemaId = getValueSchemaId(clusterName, storeName, latestValueSchema.toString());
        updateReplicationMetadataSchema(clusterName, storeName, latestValueSchema, valueSchemaId);
      }
      if (store.isWriteComputationEnabled()) {
        Schema newWriteComputeSchema =
            writeComputeSchemaConverter.convertFromValueRecordSchema(addedSchemaEntry.getSchema());
        addDerivedSchema(clusterName, storeName, addedSchemaEntry.getId(), newWriteComputeSchema.toString());
      }

      return addedSchemaEntry;
    } finally {
      releaseAdminMessageLock(clusterName, storeName);
    }
  }

  /**
   * Add a new superset schema for the given store with all specified properties by sending a
   * {@link AdminMessageType#DERIVED_SCHEMA_CREATION DERIVED_SCHEMA_CREATION} admin message.
   * @return an <code>DerivedSchemaEntry</code> object composed of a derived schema and its corresponding id.
   */
  @Override
  public DerivedSchemaEntry addDerivedSchema(
      String clusterName,
      String storeName,
      int valueSchemaId,
      String derivedSchemaStr) {
    acquireAdminMessageLock(clusterName, storeName);
    try {
      int newDerivedSchemaId = veniceHelixAdmin.checkPreConditionForAddDerivedSchemaAndGetNewSchemaId(
          clusterName,
          storeName,
          valueSchemaId,
          derivedSchemaStr);

      // if we find this is a duplicate schema, return the existing schema id
      if (newDerivedSchemaId == SchemaData.DUPLICATE_VALUE_SCHEMA_CODE) {
        return new DerivedSchemaEntry(
            valueSchemaId,
            getVeniceHelixAdmin().getDerivedSchemaId(clusterName, storeName, derivedSchemaStr)
                .getGeneratedSchemaVersion(),
            derivedSchemaStr);
      }

      LOGGER.info(
          "Adding derived schema: {} to store: {}, version: {} in cluster: {}",
          derivedSchemaStr,
          storeName,
          valueSchemaId,
          clusterName);

      DerivedSchemaCreation derivedSchemaCreation =
          (DerivedSchemaCreation) AdminMessageType.DERIVED_SCHEMA_CREATION.getNewInstance();
      derivedSchemaCreation.clusterName = clusterName;
      derivedSchemaCreation.storeName = storeName;
      SchemaMeta schemaMeta = new SchemaMeta();
      schemaMeta.definition = derivedSchemaStr;
      schemaMeta.schemaType = SchemaType.AVRO_1_4.getValue();
      derivedSchemaCreation.schema = schemaMeta;
      derivedSchemaCreation.valueSchemaId = valueSchemaId;
      derivedSchemaCreation.derivedSchemaId = newDerivedSchemaId;

      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.DERIVED_SCHEMA_CREATION.getValue();
      message.payloadUnion = derivedSchemaCreation;

      sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);

      return new DerivedSchemaEntry(valueSchemaId, newDerivedSchemaId, derivedSchemaStr);
    } finally {
      releaseAdminMessageLock(clusterName, storeName);
    }
  }

  /**
   * Unsupported operation in the parent controller.
   */
  @Override
  public DerivedSchemaEntry addDerivedSchema(
      String clusterName,
      String storeName,
      int valueSchemaId,
      int derivedSchemaId,
      String derivedSchemaStr) {
    throw new VeniceUnsupportedOperationException("addDerivedSchema");
  }

  /**
   * Unsupported operation in the parent controller.
   */
  @Override
  public DerivedSchemaEntry removeDerivedSchema(
      String clusterName,
      String storeName,
      int valueSchemaId,
      int derivedSchemaId) {
    throw new VeniceUnsupportedOperationException("removeDerivedSchema");
  }

  /**
   * @see VeniceHelixAdmin#getReplicationMetadataSchemas(String, String)
   */
  @Override
  public Collection<RmdSchemaEntry> getReplicationMetadataSchemas(String clusterName, String storeName) {
    return getVeniceHelixAdmin().getReplicationMetadataSchemas(clusterName, storeName);
  }

  /**
   * @see VeniceHelixAdmin#getReplicationMetadataSchema(String, String, int, int)
   */
  @Override
  public Optional<Schema> getReplicationMetadataSchema(
      String clusterName,
      String storeName,
      int valueSchemaID,
      int rmdVersionID) {
    return getVeniceHelixAdmin().getReplicationMetadataSchema(clusterName, storeName, valueSchemaID, rmdVersionID);
  }

  /**
   * Create a new <code>ReplicationMetadataSchemaEntry</code> object with the given properties and add it into schema
   * repository by sending {@link AdminMessageType#REPLICATION_METADATA_SCHEMA_CREATION REPLICATION_METADATA_SCHEMA_CREATION} admin message.
   */
  @Override
  public RmdSchemaEntry addReplicationMetadataSchema(
      String clusterName,
      String storeName,
      int valueSchemaId,
      int replicationMetadataVersionId,
      String replicationMetadataSchemaStr) {
    acquireAdminMessageLock(clusterName, storeName);
    try {
      RmdSchemaEntry rmdSchemaEntry =
          new RmdSchemaEntry(valueSchemaId, replicationMetadataVersionId, replicationMetadataSchemaStr);
      final boolean replicationMetadataSchemaAlreadyPresent = getVeniceHelixAdmin()
          .checkIfMetadataSchemaAlreadyPresent(clusterName, storeName, valueSchemaId, rmdSchemaEntry);
      if (replicationMetadataSchemaAlreadyPresent) {
        LOGGER.info(
            "Replication metadata schema already exists for store: {} in cluster: {} metadataSchema: {} "
                + "replicationMetadataVersionId: {} valueSchemaId: {}",
            storeName,
            clusterName,
            replicationMetadataSchemaStr,
            replicationMetadataVersionId,
            valueSchemaId);
        return rmdSchemaEntry;
      }

      LOGGER.info(
          "Adding Replication metadata schema for store: {} in cluster: {} metadataSchema: {} "
              + "replicationMetadataVersionId: {} valueSchemaId: {}",
          storeName,
          clusterName,
          replicationMetadataSchemaStr,
          replicationMetadataVersionId,
          valueSchemaId);

      MetadataSchemaCreation replicationMetadataSchemaCreation =
          (MetadataSchemaCreation) AdminMessageType.REPLICATION_METADATA_SCHEMA_CREATION.getNewInstance();
      replicationMetadataSchemaCreation.clusterName = clusterName;
      replicationMetadataSchemaCreation.storeName = storeName;
      replicationMetadataSchemaCreation.valueSchemaId = valueSchemaId;
      SchemaMeta schemaMeta = new SchemaMeta();
      schemaMeta.definition = replicationMetadataSchemaStr;
      schemaMeta.schemaType = SchemaType.AVRO_1_4.getValue();
      replicationMetadataSchemaCreation.metadataSchema = schemaMeta;
      replicationMetadataSchemaCreation.timestampMetadataVersionId = replicationMetadataVersionId;

      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.REPLICATION_METADATA_SCHEMA_CREATION.getValue();
      message.payloadUnion = replicationMetadataSchemaCreation;

      sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);

      // Be defensive and check that RMD schema has been added indeed. Do a loose validation parsing as stores can have
      // older schemas considered wrong with respect to the current avro version
      final Schema expectedRmdSchema =
          AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(replicationMetadataSchemaStr);
      validateRmdSchemaIsAddedAsExpected(
          clusterName,
          storeName,
          valueSchemaId,
          replicationMetadataVersionId,
          expectedRmdSchema);
      return new RmdSchemaEntry(valueSchemaId, replicationMetadataVersionId, replicationMetadataSchemaStr);
    } catch (Exception e) {
      LOGGER.error(
          "Error when adding replication metadata schema for store: {}, value schema id: {}",
          storeName,
          valueSchemaId,
          e);
      throw e;
    } finally {
      releaseAdminMessageLock(clusterName, storeName);
    }
  }

  private void validateRmdSchemaIsAddedAsExpected(
      String clusterName,
      String storeName,
      int valueSchemaID,
      int rmdVersionID,
      Schema expectedRmdSchema) {
    final Schema addedRmdSchema =
        getReplicationMetadataSchema(clusterName, storeName, valueSchemaID, rmdVersionID).orElse(null);
    if (addedRmdSchema == null) {
      throw new VeniceException(
          String.format(
              "No replication metadata schema found for store %s in cluster %s with value "
                  + "schema ID %s and RMD protocol version ID %d",
              storeName,
              clusterName,
              valueSchemaID,
              rmdVersionID));
    }
    if (!AvroSchemaUtils.compareSchemaIgnoreFieldOrder(addedRmdSchema, expectedRmdSchema)) {
      throw new VeniceException(
          String.format(
              "For store %s in cluster %s with value schema ID %d and RMD protocol"
                  + " version ID %d. Expected RMD schema %s. But got RMD schema: %s",
              storeName,
              clusterName,
              valueSchemaID,
              rmdVersionID,
              expectedRmdSchema.toString(true),
              addedRmdSchema.toString(true)));
    }
  }

  /**
   * Unsupported operation in the parent controller.
   */
  @Override
  public void validateAndMaybeRetrySystemStoreAutoCreation(
      String clusterName,
      String storeName,
      VeniceSystemStoreType veniceSystemStoreType) {
    throw new VeniceUnsupportedOperationException("validateAndMaybeRetrySystemStoreAutoCreation");
  }

  private void updateReplicationMetadataSchemaForAllValueSchema(String clusterName, String storeName) {
    final Collection<SchemaEntry> valueSchemas = getValueSchemas(clusterName, storeName);
    for (SchemaEntry valueSchemaEntry: valueSchemas) {
      updateReplicationMetadataSchema(clusterName, storeName, valueSchemaEntry.getSchema(), valueSchemaEntry.getId());
    }
  }

  private void updateReplicationMetadataSchema(
      String clusterName,
      String storeName,
      Schema valueSchema,
      int valueSchemaId) {
    final int rmdVersionId = getRmdVersionID(storeName, clusterName);
    final boolean valueSchemaAlreadyHasRmdSchema = getVeniceHelixAdmin()
        .checkIfValueSchemaAlreadyHasRmdSchema(clusterName, storeName, valueSchemaId, rmdVersionId);
    if (valueSchemaAlreadyHasRmdSchema) {
      LOGGER.info(
          "Store {} in cluster {} already has a replication metadata schema for its value schema with ID {} and "
              + "replication metadata version ID {}. So skip updating this value schema's RMD schema.",
          storeName,
          clusterName,
          valueSchemaId,
          rmdVersionId);
      return;
    }
    String replicationMetadataSchemaStr =
        RmdSchemaGenerator.generateMetadataSchema(valueSchema, rmdVersionId).toString();
    addReplicationMetadataSchema(clusterName, storeName, valueSchemaId, rmdVersionId, replicationMetadataSchemaStr);
  }

  /**
   * Unsupported operation in the parent controller.
   */
  @Override
  public List<String> getStorageNodes(String clusterName) {
    throw new VeniceUnsupportedOperationException("getStorageNodes");
  }

  /**
   * Unsupported operation in the parent controller.
   */
  @Override
  public Map<String, String> getStorageNodesStatus(String clusterName, boolean enableReplica) {
    throw new VeniceUnsupportedOperationException("getStorageNodesStatus");
  }

  /**
   * Unsupported operation in the parent controller.
   */
  @Override
  public void removeStorageNode(String clusterName, String instanceId) {
    throw new VeniceUnsupportedOperationException("removeStorageNode");
  }

  /**
   * Queries child clusters for status.
   * Of all responses, return highest of (in order) NOT_CREATED, NEW, STARTED, PROGRESS.
   * If all responses are COMPLETED, returns COMPLETED.
   * If any response is ERROR and all responses are terminal (COMPLETED or ERROR), returns ERROR
   * If any response is ERROR and any response is not terminal, returns PROGRESS
   * ARCHIVED is treated as NOT_CREATED
   *
   * If error in querying half or more of clusters, returns PROGRESS. (so that polling will continue)
   *
   * @param clusterName
   * @param kafkaTopic
   * @return
   */
  @Override
  public OfflinePushStatusInfo getOffLinePushStatus(String clusterName, String kafkaTopic) {
    Map<String, ControllerClient> controllerClients = getVeniceHelixAdmin().getControllerClientMap(clusterName);
    return getOffLineJobStatus(clusterName, kafkaTopic, controllerClients);
  }

  /**
   * @see Admin#getOffLinePushStatus(String, String)
   */
  @Override
  public OfflinePushStatusInfo getOffLinePushStatus(
      String clusterName,
      String kafkaTopic,
      Optional<String> incrementalPushVersion,
      String region,
      String targetedRegions) {
    Map<String, ControllerClient> controllerClients = getVeniceHelixAdmin().getControllerClientMap(clusterName);
    if (region != null) {
      if (!controllerClients.containsKey(region)) {
        throw new VeniceException("Region " + region + " does not exist in " + controllerClients.keySet());
      }
      JobStatusQueryResponse response = controllerClients.get(region).queryDetailedJobStatus(kafkaTopic, region);
      if (response.isError()) {
        throw new VeniceException(
            "Couldn't query " + region + " for job " + kafkaTopic + " status: " + response.getError());
      }
      ExecutionStatus status = ExecutionStatus.valueOf(response.getStatus());
      String statusDetails = response.getOptionalStatusDetails().orElse(null);
      OfflinePushStatusInfo offlinePushStatusInfo =
          new OfflinePushStatusInfo(status, response.getStatusUpdateTimestamp(), statusDetails);
      offlinePushStatusInfo.setUncompletedPartitions(response.getUncompletedPartitions());
      return offlinePushStatusInfo;
    }
    return getOffLineJobStatus(clusterName, kafkaTopic, controllerClients, incrementalPushVersion, targetedRegions);
  }

  OfflinePushStatusInfo getOffLineJobStatus(
      String clusterName,
      String kafkaTopic,
      Map<String, ControllerClient> controllerClients) {
    return getOffLineJobStatus(clusterName, kafkaTopic, controllerClients, Optional.empty(), null);
  }

  /**
   * Querying child controllers for status in different regions and then aggregate them.
   * @param clusterName
   * @param kafkaTopic
   * @param controllerClients
   * @param incrementalPushVersion
   * @param targetedRegions
   * @return
   */
  private OfflinePushStatusInfo getOffLineJobStatus(
      String clusterName,
      String kafkaTopic,
      Map<String, ControllerClient> controllerClients,
      Optional<String> incrementalPushVersion,
      String targetedRegions) {
    Set<String> childRegions = controllerClients.keySet();
    Map<String, ExecutionStatus> statuses = new HashMap<>();
    Map<String, String> extraInfo = new HashMap<>();
    Map<String, String> extraDetails = new HashMap<>();
    Map<String, Long> extraInfoUpdateTimestamp = new HashMap<>();
    int numChildRegionsFailedToFetchStatus = 0;
    Set<String> targetedRegionSet = RegionUtils.parseRegionsFilterList(targetedRegions);

    for (Map.Entry<String, ControllerClient> entry: controllerClients.entrySet()) {
      String region = entry.getKey();
      // if targetedRegions is present, only query the targeted regions
      if (!targetedRegionSet.isEmpty() && !targetedRegionSet.contains(region)) {
        continue;
      }
      ControllerClient controllerClient = entry.getValue();
      String leaderControllerUrl;
      try {
        leaderControllerUrl = controllerClient.getLeaderControllerUrl();
      } catch (VeniceException exception) {
        LOGGER.warn("Couldn't query {} for job status of {}", region, kafkaTopic, exception);
        statuses.put(region, ExecutionStatus.UNKNOWN);
        extraInfo.put(region, ExecutionStatus.UNKNOWN.toString());
        extraDetails.put(region, "Failed to get leader controller url " + exception.getMessage());
        continue;
      }
      JobStatusQueryResponse response = controllerClient.queryJobStatus(kafkaTopic, incrementalPushVersion);
      if (response.isError()) {
        numChildRegionsFailedToFetchStatus += 1;
        LOGGER.warn("Couldn't query {} for job {} status: {}", region, kafkaTopic, response.getError());
        statuses.put(region, ExecutionStatus.UNKNOWN);
        extraInfo.put(region, ExecutionStatus.UNKNOWN.toString());
        extraDetails.put(region, leaderControllerUrl + " " + response.getError());
      } else {
        ExecutionStatus status = ExecutionStatus.valueOf(response.getStatus());
        statuses.put(region, status);
        extraInfo.put(region, response.getStatus());
        if (response.getStatusUpdateTimestamp() != null) {
          extraInfoUpdateTimestamp.put(region, response.getStatusUpdateTimestamp());
        }
        Optional<String> statusDetails = response.getOptionalStatusDetails();
        statusDetails.ifPresent(s -> extraDetails.put(region, leaderControllerUrl + " " + s));
      }
    }

    StringBuilder currentReturnStatusDetails = new StringBuilder();

    ExecutionStatus currentReturnStatus =
        getFinalReturnStatus(statuses, childRegions, numChildRegionsFailedToFetchStatus, currentReturnStatusDetails);

    // Do not delete parent Kafka if its part of targeted colo push to prevent concurrent pushes
    if (currentReturnStatus.isTerminal()) {
      String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
      int versionNum = Version.parseVersionFromKafkaTopicName(kafkaTopic);
      HelixVeniceClusterResources resources = getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName);

      try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
        ReadWriteStoreRepository repository = resources.getStoreMetadataRepository();
        Store parentStore = repository.getStore(storeName);
        Version version = parentStore.getVersion(versionNum);
        boolean isDeferredSwap = version != null && version.isVersionSwapDeferred();
        if (!isDeferredSwap) {
          // targetedRegions is non-empty for target region push of batch store
          boolean isTargetRegionPush = !StringUtils.isEmpty(targetedRegions);
          Version storeVersion = parentStore.getVersion(versionNum);
          boolean isVersionPushed = storeVersion != null && storeVersion.getStatus().equals(PUSHED);
          boolean isHybridStore = storeVersion != null && storeVersion.getHybridStoreConfig() != null;
          // Truncate topic after push is in terminal state if
          // 1. Its a hybrid store or regular push. (Hybrid store target push uses repush where isTargetRegionPush is
          // false)
          // 2. If target region push is enabled and job to push data only to target region completed (status == PUSHED)
          if (!isTargetRegionPush // regular push
              || isVersionPushed // target region push
              || isHybridStore) {
            LOGGER
                .info("Truncating parent VT {} after push status {}", kafkaTopic, currentReturnStatus.getRootStatus());
            truncateTopicsOptionally(
                clusterName,
                kafkaTopic,
                incrementalPushVersion,
                currentReturnStatus,
                currentReturnStatusDetails);
          }
          // status PUSHED is set when batch store's target region push is completed, but other region are yet to
          // complete
          if (isTargetRegionPush && !isVersionPushed) {
            parentStore.updateVersionStatus(versionNum, PUSHED);
            repository.updateStore(parentStore);
          } else { // status ONLINE is set when all region finishes ingestion for either regular or target region push.
            parentStore.updateVersionStatus(versionNum, ONLINE);
            repository.updateStore(parentStore);
          }
        }
      }
    }

    return new OfflinePushStatusInfo(
        currentReturnStatus,
        null,
        extraInfo,
        currentReturnStatusDetails.toString(),
        extraDetails,
        extraInfoUpdateTimestamp);
  }

  /**
   * Based on the global information, start determining the final status to return
   * @param statuses
   * @param childRegions
   * @param numChildRegionsFailedToFetchStatus
   * @param currentReturnStatusDetails
   * @return
   */
  protected static ExecutionStatus getFinalReturnStatus(
      Map<String, ExecutionStatus> statuses,
      Set<String> childRegions,
      int numChildRegionsFailedToFetchStatus,
      StringBuilder currentReturnStatusDetails) {
    ExecutionStatus currentReturnStatus = ExecutionStatus.NEW;

    // Sort the per-datacenter status in this order, and return the first one in the list
    // Edge case example: if one cluster is stuck in NOT_CREATED, then
    // as another cluster goes from PROGRESS to COMPLETED
    // the aggregate status will go from PROGRESS back down to NOT_CREATED.
    List<ExecutionStatus> sortedStatuses = statuses.values()
        .stream()
        .sorted(Comparator.comparingInt(VeniceHelixAdmin.STATUS_PRIORITIES::indexOf))
        .collect(Collectors.toList());

    if (!sortedStatuses.isEmpty()) {
      currentReturnStatus = sortedStatuses.get(0);
    }

    int successCount = childRegions.size() - numChildRegionsFailedToFetchStatus;
    if (successCount < (childRegions.size() / 2) + 1) {
      // Strict majority must be reachable, otherwise keep polling
      currentReturnStatus = ExecutionStatus.PROGRESS;
    }

    if (currentReturnStatus.isTerminal()) {
      // If there is a temporary datacenter connection failure, we want VPJ to report failure while allowing the push
      // to succeed in remaining datacenters. If we want to allow the push to succeed in async in the remaining
      // datacenter, then put the topic delete into an else block under `if (numChildRegionsFailedToFetchStatus > 0)`
      if (numChildRegionsFailedToFetchStatus > 0) {
        currentReturnStatus = ExecutionStatus.ERROR;
        currentReturnStatusDetails.append(numChildRegionsFailedToFetchStatus)
            .append("/")
            .append(childRegions.size())
            .append(" DCs unreachable. ");
      }
    }

    return currentReturnStatus;
  }

  /**
   * Based on the control configs and push information to decide whether to truncate the Kafka topic or not.
   * @param clusterName
   * @param kafkaTopic， the kafka topic in the parent region
   * @param incrementalPushVersion
   * @param currentReturnStatus
   * @param currentReturnStatusDetails
   */
  private void truncateTopicsOptionally(
      String clusterName,
      String kafkaTopic,
      Optional<String> incrementalPushVersion,
      ExecutionStatus currentReturnStatus,
      StringBuilder currentReturnStatusDetails) {
    // TODO: Set parent controller's version status based on currentReturnStatus
    // COMPLETED -> ONLINE
    // ERROR -> ERROR
    // TODO: remove this if statement since it was only for debugging purpose
    if (maxErroredTopicNumToKeep > 0 && currentReturnStatus.isError()) {
      currentReturnStatusDetails.append("Parent Kafka topic won't be truncated");
      LOGGER.info(
          "The errored kafka topic {} won't be truncated since it will be used to investigate some Kafka related issue",
          kafkaTopic);
    } else {
      /**
       * truncate the topic if either
       * 1. the store is not incremental push enabled and the push completed (no ERROR)
       * 2. this is a failed batch push
       * 3. the store is incremental push enabled and same incPushToRT and batch push finished
       */
      Store store = getVeniceHelixAdmin().getStore(clusterName, Version.parseStoreFromKafkaTopicName(kafkaTopic));
      boolean failedBatchPush = !incrementalPushVersion.isPresent() && currentReturnStatus.isError();
      Version version = store.getVersion(Version.parseVersionFromKafkaTopicName(kafkaTopic));

      boolean incPushEnabledBatchPushSuccess = !incrementalPushVersion.isPresent() && store.isIncrementalPushEnabled();
      boolean nonIncPushBatchSuccess = !store.isIncrementalPushEnabled() && !currentReturnStatus.isError();
      boolean isDeferredVersionSwap = version != null && version.isVersionSwapDeferred();

      if ((failedBatchPush || nonIncPushBatchSuccess && !isDeferredVersionSwap || incPushEnabledBatchPushSuccess)
          && !getMultiClusterConfigs().getCommonConfig().disableParentTopicTruncationUponCompletion()) {
        LOGGER.info("Truncating kafka topic: {} with job status: {}", kafkaTopic, currentReturnStatus);
        truncateKafkaTopic(kafkaTopic);
        if (version != null && version.getPushType().isStreamReprocessing()) {
          truncateKafkaTopic(Version.composeStreamReprocessingTopic(store.getName(), version.getNumber()));
        }
        currentReturnStatusDetails.append("Parent Kafka topic truncated");
      }
    }
  }

  /**
   * @see VeniceHelixAdmin#getKafkaBootstrapServers(boolean)
   */
  @Override
  public String getKafkaBootstrapServers(boolean isSSL) {
    return getVeniceHelixAdmin().getKafkaBootstrapServers(isSSL);
  }

  @Override
  public String getRegionName() {
    return getVeniceHelixAdmin().getRegionName();
  }

  /**
   * @see VeniceHelixAdmin#getNativeReplicationKafkaBootstrapServerAddress(String)
   */
  @Override
  public String getNativeReplicationKafkaBootstrapServerAddress(String sourceFabric) {
    return getVeniceHelixAdmin().getNativeReplicationKafkaBootstrapServerAddress(sourceFabric);
  }

  /**
   * @see VeniceHelixAdmin#getNativeReplicationSourceFabric(String, Store, Optional, Optional, String)
   */
  @Override
  public String getNativeReplicationSourceFabric(
      String clusterName,
      Store store,
      Optional<String> sourceGridFabric,
      Optional<String> emergencySourceRegion,
      String targetedRegions) {
    return getVeniceHelixAdmin()
        .getNativeReplicationSourceFabric(clusterName, store, sourceGridFabric, emergencySourceRegion, targetedRegions);
  }

  /**
   * @see VeniceHelixAdmin#isSSLEnabledForPush(String, String)
   */
  @Override
  public boolean isSSLEnabledForPush(String clusterName, String storeName) {
    return getVeniceHelixAdmin().isSSLEnabledForPush(clusterName, storeName);
  }

  /**
   * @see VeniceHelixAdmin#isSslToKafka()
   */
  @Override
  public boolean isSslToKafka() {
    return getVeniceHelixAdmin().isSslToKafka();
  }

  /**
   * @see VeniceHelixAdmin#getTopicManager()
   */
  @Override
  public TopicManager getTopicManager() {
    return getVeniceHelixAdmin().getTopicManager();
  }

  /**
   * @see VeniceHelixAdmin#getTopicManager(String)
   */
  @Override
  public TopicManager getTopicManager(String pubSubServerAddress) {
    return getVeniceHelixAdmin().getTopicManager(pubSubServerAddress);
  }

  @Override
  public InstanceRemovableStatuses getAggregatedHealthStatus(
      String cluster,
      List<String> instances,
      List<String> toBeStoppedInstances,
      boolean isSSLEnabled) {
    throw new VeniceUnsupportedOperationException("getAggregatedHealthStatus");
  }

  @Override
  public boolean isRTTopicDeletionPermittedByAllControllers(String clusterName, String storeName) {
    return false;
  }

  /**
   * @see VeniceHelixAdmin#isLeaderControllerFor(String)
   */
  @Override
  public boolean isLeaderControllerFor(String clusterName) {
    return getVeniceHelixAdmin().isLeaderControllerFor(clusterName);
  }

  /**
   * @see Admin#calculateNumberOfPartitions(String, String)
   */
  @Override
  public int calculateNumberOfPartitions(String clusterName, String storeName) {
    return getVeniceHelixAdmin().calculateNumberOfPartitions(clusterName, storeName);
  }

  /**
   * @see VeniceHelixAdmin#getReplicationFactor(String, String)
   */
  @Override
  public int getReplicationFactor(String clusterName, String storeName) {
    return getVeniceHelixAdmin().getReplicationFactor(clusterName, storeName);
  }

  /**
   * @see VeniceHelixAdmin#getDatacenterCount(String)
   */
  @Override
  public int getDatacenterCount(String clusterName) {
    return getMultiClusterConfigs().getControllerConfig(clusterName).getChildDataCenterControllerUrlMap().size();
  }

  /**
   * @see VeniceHelixAdmin#getReplicas(String, String)
   */
  @Override
  public List<Replica> getReplicas(String clusterName, String kafkaTopic) {
    throw new VeniceException("getReplicas is not supported!");
  }

  /**
   * Unsupported operation in the parent controller.
   */
  @Override
  public List<Replica> getReplicasOfStorageNode(String clusterName, String instanceId) {
    throw new VeniceException("getReplicasOfStorageNode is not supported!");
  }

  /**
   * Unsupported operation in the parent controller.
   */
  @Override
  public NodeRemovableResult isInstanceRemovable(String clusterName, String instanceId, List<String> lockedNodes) {
    throw new VeniceException("isInstanceRemovable is not supported!");
  }

  /**
   * Unsupported operation in the parent controller.
   */
  @Override
  public Pair<NodeReplicasReadinessState, List<Replica>> nodeReplicaReadiness(String cluster, String helixNodeId) {
    throw new VeniceUnsupportedOperationException("nodeReplicaReadiness is not supported");
  }

  private StoreInfo getStoreInChildRegion(String regionName, String clusterName, String storeName) {
    ControllerClient childControllerClient = getFabricBuildoutControllerClient(clusterName, regionName);
    StoreResponse storeResponse = childControllerClient.getStore(storeName);
    if (storeResponse.isError()) {
      throw new VeniceException(
          "Error when getting store " + storeName + " from region " + regionName + ": " + storeResponse.getError());
    }
    return storeResponse.getStore();
  }

  private boolean whetherToCreateNewDataRecoveryVersion(
      String destFabric,
      String clusterName,
      StoreInfo destStore,
      int versionNumber) {
    /**
     * Creating a new data recovery version on the destination colo when satisfying:
     * 1. New version data recovery is only supported for batch-only store.
     * 2. For the existing destination data center, a new version is needed if
     *    2.1. srcVersionNumber equals to the current version in dest colo, as current version is serving read requests.
     *    2.2. srcVersionNumber is less than the current version in dest colo, because Venice normally assumes that a
     *         new version always have a larger version number than previous ones
     *         e.g. {@link StoreBackupVersionCleanupService#cleanupBackupVersion(Store, String)}.
     */
    return destStore.getHybridStoreConfig() == null && versionNumber <= destStore.getCurrentVersion()
        && multiClusterConfigs.getControllerConfig(clusterName).getChildDatacenters().contains(destFabric);
  }

  /**
   * @see Admin#initiateDataRecovery(String, String, int, String, String, boolean, Optional)
   */
  @Override
  public void initiateDataRecovery(
      String clusterName,
      String storeName,
      int version,
      String sourceFabric,
      String destinationFabric,
      boolean copyAllVersionConfigs,
      Optional<Version> ignored) {
    if (Objects.equals(sourceFabric, destinationFabric)) {
      throw new VeniceException(
          String.format(
              "Source ({}) and destination ({}) cannot be the same data center",
              sourceFabric,
              destinationFabric));
    }
    StoreInfo srcStore = getStoreInChildRegion(sourceFabric, clusterName, storeName);
    if (version == VERSION_ID_UNSET) {
      version = srcStore.getCurrentVersion();
    }
    Optional<Version> srcVersion = srcStore.getVersion(version);
    if (!srcVersion.isPresent()) {
      throw new VeniceException(
          "Version " + version + " does not exist in source fabric " + sourceFabric + " store " + storeName);
    }
    StoreInfo destStore = getStoreInChildRegion(destinationFabric, clusterName, storeName);
    if (whetherToCreateNewDataRecoveryVersion(destinationFabric, clusterName, destStore, version)) {
      getVeniceHelixAdmin().checkPreConditionForUpdateStoreMetadata(clusterName, storeName);
      HelixVeniceClusterResources resources = getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName);
      try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
        ReadWriteStoreRepository repository = resources.getStoreMetadataRepository();
        Store parentStore = repository.getStore(storeName);
        int newVersion = parentStore.peekNextVersion().getNumber();
        parentStore.setLargestUsedVersionNumber(newVersion);
        repository.updateStore(parentStore);
        LOGGER.info(
            "version {} is less or equal to in the current version of {} in {}. Copying data to a new version {}.",
            version,
            storeName,
            destinationFabric,
            newVersion);
        version = newVersion;
      }
    }
    ControllerClient destFabricChildControllerClient =
        getFabricBuildoutControllerClient(clusterName, destinationFabric);
    ControllerResponse destinationFabricResponse = destFabricChildControllerClient
        .dataRecovery(sourceFabric, destinationFabric, storeName, version, true, copyAllVersionConfigs, srcVersion);
    if (destinationFabricResponse.isError()) {
      throw new VeniceException(
          "Failed to initiate data recovery in destination fabric, error: " + destinationFabricResponse.getError());
    }
  }

  /**
   * @see Admin#prepareDataRecovery(String, String, int, String, String, Optional)
   */
  @Override
  public void prepareDataRecovery(
      String clusterName,
      String storeName,
      int version,
      String sourceFabric,
      String destinationFabric,
      Optional<Integer> ignored) {
    if (Objects.equals(sourceFabric, destinationFabric)) {
      throw new VeniceException(
          String.format(
              "Source ({}) and destination ({}) cannot be the same data center",
              sourceFabric,
              destinationFabric));
    }
    StoreInfo srcStore = getStoreInChildRegion(sourceFabric, clusterName, storeName);
    if (version == VERSION_ID_UNSET) {
      version = srcStore.getCurrentVersion();
    }
    StoreInfo destStore = getStoreInChildRegion(destinationFabric, clusterName, storeName);
    if (whetherToCreateNewDataRecoveryVersion(destinationFabric, clusterName, destStore, version)) {
      LOGGER.info("Skip cleanup for store: {}, version:{} in {}", storeName, version, destinationFabric);
      return;
    }
    int amplificationFactor = srcStore.getPartitionerConfig().getAmplificationFactor();
    ControllerClient destFabricChildControllerClient =
        getFabricBuildoutControllerClient(clusterName, destinationFabric);
    ControllerResponse destFabricResponse = destFabricChildControllerClient
        .prepareDataRecovery(sourceFabric, destinationFabric, storeName, version, Optional.of(amplificationFactor));
    if (destFabricResponse.isError()) {
      throw new VeniceException(
          "Error when preparing data recovery for store " + storeName + " in destination fabric " + destinationFabric
              + ": " + destFabricResponse.getError());
    }
  }

  /**
   * @see Admin#isStoreVersionReadyForDataRecovery(String, String, int, String, String, Optional)
   */
  @Override
  public Pair<Boolean, String> isStoreVersionReadyForDataRecovery(
      String clusterName,
      String storeName,
      int version,
      String sourceFabric,
      String destinationFabric,
      Optional<Integer> ignored) {
    try {
      ControllerClient srcFabricChildControllerClient = getFabricBuildoutControllerClient(clusterName, sourceFabric);
      ControllerClient destFabricChildControllerClient =
          getFabricBuildoutControllerClient(clusterName, destinationFabric);
      StoreInfo sourceStoreInfo = srcFabricChildControllerClient.getStore(storeName).getStore();
      int amplificationFactor = sourceStoreInfo.getPartitionerConfig().getAmplificationFactor();
      ReadyForDataRecoveryResponse destinationFabricResponse =
          destFabricChildControllerClient.isStoreVersionReadyForDataRecovery(
              sourceFabric,
              destinationFabric,
              storeName,
              version,
              Optional.of(amplificationFactor));
      return new Pair<>(destinationFabricResponse.isReady(), destinationFabricResponse.getReason());
    } catch (Exception e) {
      return new Pair<>(false, e.getMessage());
    }
  }

  /**
   * @see Admin#getLeaderController(String)
   */
  @Override
  public Instance getLeaderController(String clusterName) {
    return getVeniceHelixAdmin().getLeaderController(clusterName);
  }

  /**
   * Unsupported operation in the parent controller.
   */
  @Override
  public void addInstanceToAllowlist(String clusterName, String helixNodeId) {
    throw new VeniceException("addInstanceToAllowlist is not supported!");
  }

  /**
   * Unsupported operation in the parent controller.
   */
  @Override
  public void removeInstanceFromAllowList(String clusterName, String helixNodeId) {
    throw new VeniceException("removeInstanceFromAllowList is not supported!");
  }

  /**
   * Unsupported operation in the parent controller.
   */
  @Override
  public Set<String> getAllowlist(String clusterName) {
    throw new VeniceException("getAllowlist is not supported!");
  }

  /**
   * @see Admin#killOfflinePush(String, String, boolean)
   */
  @Override
  public void killOfflinePush(String clusterName, String kafkaTopic, boolean isForcedKill) {
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    if (getStore(clusterName, storeName) == null) {
      throw new VeniceNoStoreException(storeName, clusterName);
    }
    acquireAdminMessageLock(clusterName, storeName);
    try {
      getVeniceHelixAdmin().checkPreConditionForKillOfflinePush(clusterName, kafkaTopic);
      LOGGER.info("Killing offline push job for topic: {} in cluster: {}", kafkaTopic, clusterName);
      /**
       * When parent controller wants to keep some errored topics, this function won't remove topic,
       * but relying on the next push to clean up this topic if it hasn't been removed by {@link #getOffLineJobStatus}.
       *
       * The reason is that every errored push will call this function.
       */
      if (maxErroredTopicNumToKeep == 0) {
        // Truncate Kafka topic
        LOGGER.info("Truncating topic when kill offline push job, topic: {}", kafkaTopic);
        truncateKafkaTopic(kafkaTopic);
        PubSubTopic correspondingStreamReprocessingTopic =
            pubSubTopicRepository.getTopic(Version.composeStreamReprocessingTopicFromVersionTopic(kafkaTopic));
        if (getTopicManager().containsTopic(correspondingStreamReprocessingTopic)) {
          truncateKafkaTopic(correspondingStreamReprocessingTopic.getName());
        }
      }

      HelixVeniceClusterResources resources = getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName);
      try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
        ReadWriteStoreRepository repository = resources.getStoreMetadataRepository();
        Store parentStore = repository.getStore(storeName);
        int version = Version.parseVersionFromKafkaTopicName(kafkaTopic);
        parentStore.updateVersionStatus(version, VersionStatus.KILLED);
        repository.updateStore(parentStore);
      }

      KillOfflinePushJob killJob = (KillOfflinePushJob) AdminMessageType.KILL_OFFLINE_PUSH_JOB.getNewInstance();
      killJob.clusterName = clusterName;
      killJob.kafkaTopic = kafkaTopic;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.KILL_OFFLINE_PUSH_JOB.getValue();
      message.payloadUnion = killJob;

      sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);
    } finally {
      releaseAdminMessageLock(clusterName, storeName);
    }
  }

  /**
   * Unsupported operation in the parent controller.
   */
  @Override
  public StorageNodeStatus getStorageNodesStatus(String clusterName, String instanceId) {
    throw new VeniceUnsupportedOperationException("getStorageNodesStatus");
  }

  /**
   * Unsupported operation in the parent controller.
   */
  @Override
  public boolean isStorageNodeNewerOrEqualTo(String clusterName, String instanceId, StorageNodeStatus oldServerStatus) {
    throw new VeniceUnsupportedOperationException("isStorageNodeNewerOrEqualTo");
  }

  /**
   * @see Admin#setAdminConsumerService(String, AdminConsumerService)
   */
  @Override
  public void setAdminConsumerService(String clusterName, AdminConsumerService service) {
    getVeniceHelixAdmin().setAdminConsumerService(clusterName, service);
  }

  /**
   * @see Admin#skipAdminMessage(String, long, boolean)
   */
  @Override
  public void skipAdminMessage(String clusterName, long offset, boolean skipDIV) {
    getVeniceHelixAdmin().skipAdminMessage(clusterName, offset, skipDIV);
  }

  /**
   * @see Admin#getLastSucceedExecutionId(String)
   */
  @Override
  public Long getLastSucceedExecutionId(String clustername) {
    return getVeniceHelixAdmin().getLastSucceedExecutionId(clustername);
  }

  private Time getTimer() {
    return timer;
  }

  // Visible for testing
  void setTimer(Time timer) {
    this.timer = timer;
  }

  /**
   * @see Admin#getAdminCommandExecutionTracker(String)
   */
  @Override
  public Optional<AdminCommandExecutionTracker> getAdminCommandExecutionTracker(String clusterName) {
    if (adminCommandExecutionTrackers.containsKey(clusterName)) {
      return Optional.of(adminCommandExecutionTrackers.get(clusterName));
    } else {
      return Optional.empty();
    }
  }

  /**
   * Unsupported operation in the parent controller.
   */
  @Override
  public Map<String, Long> getAdminTopicMetadata(String clusterName, Optional<String> storeName) {
    throw new VeniceUnsupportedOperationException("getAdminTopicMetadata");
  }

  /**
   * Unsupported operation in the parent controller.
   */
  @Override
  public void updateAdminTopicMetadata(
      String clusterName,
      long executionId,
      Optional<String> storeName,
      Optional<Long> offset,
      Optional<Long> upstreamOffset) {
    throw new VeniceUnsupportedOperationException("updateAdminTopicMetadata");
  }

  /**
   * Unsupported operation in the parent controller.
   */
  @Override
  public RoutersClusterConfig getRoutersClusterConfig(String clusterName) {
    throw new VeniceUnsupportedOperationException("getRoutersClusterConfig");
  }

  /**
   * Unsupported operation in the parent controller.
   */
  @Override
  public void updateRoutersClusterConfig(
      String clusterName,
      Optional<Boolean> isThrottlingEnable,
      Optional<Boolean> isQuotaRebalancedEnable,
      Optional<Boolean> isMaxCapacityProtectionEnabled,
      Optional<Integer> expectedRouterCount) {
    throw new VeniceUnsupportedOperationException("updateRoutersClusterConfig");
  }

  /**
   * @return all push-strategies defined in the ZK path {@link MigrationPushStrategyZKAccessor#MIGRATION_PUSH_STRATEGY_PATH}
   */
  @Override
  public Map<String, String> getAllStorePushStrategyForMigration() {
    return pushStrategyZKAccessor.getAllPushStrategies();
  }

  /**
   * Set a push-strategy in the ZK path {@link MigrationPushStrategyZKAccessor#MIGRATION_PUSH_STRATEGY_PATH}.
   */
  @Override
  public void setStorePushStrategyForMigration(String voldemortStoreName, String strategy) {
    pushStrategyZKAccessor.setPushStrategy(voldemortStoreName, strategy);
  }

  /**
   * @see Admin#discoverCluster(String)
   */
  @Override
  public Pair<String, String> discoverCluster(String storeName) {
    return getVeniceHelixAdmin().discoverCluster(storeName);
  }

  /**
   * @see Admin#getServerD2Service(String)
   */
  @Override
  public String getServerD2Service(String clusterName) {
    return getVeniceHelixAdmin().getServerD2Service(clusterName);
  }

  /**
   * Unsupported operation in the parent controller.
   */
  @Override
  public Map<String, String> findAllBootstrappingVersions(String clusterName) {
    throw new VeniceUnsupportedOperationException("findAllBootstrappingVersions");
  }

  /**
   * @return a <code>VeniceWriterFactory</code> object used by the Venice controller to create the venice writer.
   */
  @Override
  public VeniceWriterFactory getVeniceWriterFactory() {
    return getVeniceHelixAdmin().getVeniceWriterFactory();
  }

  /**
   * @see VeniceHelixAdmin#getPubSubConsumerAdapterFactory()
   */
  @Override
  public PubSubConsumerAdapterFactory getPubSubConsumerAdapterFactory() {
    return getVeniceHelixAdmin().getPubSubConsumerAdapterFactory();
  }

  @Override
  public VeniceProperties getPubSubSSLProperties(String pubSubBrokerAddress) {
    return getVeniceHelixAdmin().getPubSubSSLProperties(pubSubBrokerAddress);
  }

  /**
   * @see Admin#stop(String)
   */
  @Override
  public synchronized void stop(String clusterName) {
    getVeniceHelixAdmin().stop(clusterName);
    // Close the admin producer for this cluster
    VeniceWriter<byte[], byte[], byte[]> veniceWriter = veniceWriterMap.get(clusterName);
    if (veniceWriter != null) {
      veniceWriter.close();
    }
    asyncSetupEnabledMap.put(clusterName, false);
  }

  /**
   * @see Admin#stopVeniceController()
   */
  @Override
  public void stopVeniceController() {
    getVeniceHelixAdmin().stopVeniceController();
  }

  /**
   * Cause {@link VeniceParentHelixAdmin} and its associated services to stop executing.
   */
  @Override
  public synchronized void close() {
    veniceWriterMap.keySet().forEach(this::stop);

    getVeniceHelixAdmin().close();
    terminalStateTopicChecker.close();
    if (systemStoreAclSynchronizationTask != null) {
      systemStoreAclSynchronizationTask.close();
    }
    topicCheckerExecutor.shutdownNow();
    asyncSetupExecutor.shutdownNow();
    if (systemStoreAclSynchronizationExecutor != null) {
      systemStoreAclSynchronizationExecutor.shutdownNow();
    }
    try {
      topicCheckerExecutor.awaitTermination(30, TimeUnit.SECONDS);
      asyncSetupExecutor.awaitTermination(30, TimeUnit.SECONDS);
      if (systemStoreAclSynchronizationExecutor != null) {
        systemStoreAclSynchronizationExecutor.awaitTermination(30, TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    newFabricControllerClientMap.forEach(
        (clusterName, controllerClientMap) -> controllerClientMap.values().forEach(Utils::closeQuietlyWithErrorLogged));
  }

  /**
   * @see VeniceHelixAdmin#isLeaderControllerOfControllerCluster()
   */
  @Override
  public boolean isLeaderControllerOfControllerCluster() {
    return getVeniceHelixAdmin().isLeaderControllerOfControllerCluster();
  }

  /**
   * @see VeniceHelixAdmin#isTopicTruncated(String)
   */
  @Override
  public boolean isTopicTruncated(String kafkaTopicName) {
    return getVeniceHelixAdmin().isTopicTruncated(kafkaTopicName);
  }

  /**
   * @see VeniceHelixAdmin#isTopicTruncatedBasedOnRetention(long)
   */
  @Override
  public boolean isTopicTruncatedBasedOnRetention(long retention) {
    return getVeniceHelixAdmin().isTopicTruncatedBasedOnRetention(retention);
  }

  @Override
  public boolean isTopicTruncatedBasedOnRetention(String kafkaTopicName, long retentionTime) {
    return getVeniceHelixAdmin().isTopicTruncatedBasedOnRetention(kafkaTopicName, retentionTime);
  }

  /**
   * @see VeniceHelixAdmin#getMinNumberOfUnusedKafkaTopicsToPreserve()
   */
  @Override
  public int getMinNumberOfUnusedKafkaTopicsToPreserve() {
    return getVeniceHelixAdmin().getMinNumberOfUnusedKafkaTopicsToPreserve();
  }

  /**
   * @see VeniceHelixAdmin#truncateKafkaTopic(String)
   */
  @Override
  public boolean truncateKafkaTopic(String kafkaTopicName) {
    return getVeniceHelixAdmin().truncateKafkaTopic(kafkaTopicName);
  }

  /**
   * @see Admin#truncateKafkaTopic(String, long)
   */
  @Override
  public boolean truncateKafkaTopic(String kafkaTopicName, long retentionTime) {
    return getVeniceHelixAdmin().truncateKafkaTopic(kafkaTopicName, retentionTime);
  }

  /**
   * Unsupported operation in the parent controller.
   */
  @Override
  public boolean isResourceStillAlive(String resourceName) {
    throw new VeniceException("VeniceParentHelixAdmin#isResourceStillAlive is not supported!");
  }

  // Used by test only.
  void setOfflinePushAccessor(ParentHelixOfflinePushAccessor offlinePushAccessor) {
    this.offlinePushAccessor = offlinePushAccessor;
  }

  /**
   * @see Admin#updateClusterDiscovery(String, String, String, String)
   */
  @Override
  public void updateClusterDiscovery(String storeName, String oldCluster, String newCluster, String initiatingCluster) {
    getVeniceHelixAdmin().updateClusterDiscovery(storeName, oldCluster, newCluster, initiatingCluster);
  }

  /**
   * @see VeniceHelixAdmin#sendPushJobDetails(PushJobStatusRecordKey, PushJobDetails)
   */
  @Override
  public void sendPushJobDetails(PushJobStatusRecordKey key, PushJobDetails value) {
    getVeniceHelixAdmin().sendPushJobDetails(key, value);
  }

  /**
   * @see VeniceHelixAdmin#getPushJobDetails(PushJobStatusRecordKey)
   */
  @Override
  public PushJobDetails getPushJobDetails(PushJobStatusRecordKey key) {
    return getVeniceHelixAdmin().getPushJobDetails(key);
  }

  /**
   * @see VeniceHelixAdmin#getBatchJobHeartbeatValue(BatchJobHeartbeatKey)
   */
  @Override
  public BatchJobHeartbeatValue getBatchJobHeartbeatValue(BatchJobHeartbeatKey batchJobHeartbeatKey) {
    return getVeniceHelixAdmin().getBatchJobHeartbeatValue(batchJobHeartbeatKey);
  }

  /**
   * @see VeniceHelixAdmin#writeEndOfPush(String, String, int, boolean)
   */
  @Override
  public void writeEndOfPush(String clusterName, String storeName, int versionNumber, boolean alsoWriteStartOfPush) {
    getVeniceHelixAdmin().writeEndOfPush(clusterName, storeName, versionNumber, alsoWriteStartOfPush);
  }

  @Override
  public boolean whetherEnableBatchPushFromAdmin(String storeName) {
    /**
     * Batch push to Parent Cluster is always enabled.
     */
    return true;
  }

  /**
   * @see VeniceHelixAdmin#isStoreMigrationAllowed(String)
   */
  @Override
  public boolean isStoreMigrationAllowed(String clusterName) {
    return getVeniceHelixAdmin().isStoreMigrationAllowed(clusterName);
  }

  /**
   * Migrate a store from its source cluster to a new destination cluster by sending a
   * {@link AdminMessageType#MIGRATE_STORE MIGRATE_STORE} admin message.
   *
   */
  @Override
  public void migrateStore(String srcClusterName, String destClusterName, String storeName) {
    if (srcClusterName.equals(destClusterName)) {
      throw new VeniceException("Source cluster and destination cluster cannot be the same!");
    }

    MigrateStore migrateStore = (MigrateStore) AdminMessageType.MIGRATE_STORE.getNewInstance();
    migrateStore.srcClusterName = srcClusterName;
    migrateStore.destClusterName = destClusterName;
    migrateStore.storeName = storeName;

    // Set src store migration flag
    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setStoreMigration(true);
    this.updateStore(srcClusterName, storeName, params);

    // Update migration src and dest cluster in storeConfig
    getVeniceHelixAdmin().setStoreConfigForMigration(storeName, srcClusterName, destClusterName);

    // Trigger store migration operation
    AdminOperation message = new AdminOperation();
    message.operationType = AdminMessageType.MIGRATE_STORE.getValue();
    message.payloadUnion = migrateStore;
    sendAdminMessageAndWaitForConsumed(srcClusterName, storeName, message);
  }

  /**
   * @see VeniceHelixAdmin#updateClusterDiscovery(String, String, String, String)
   */
  @Override
  public void completeMigration(String srcClusterName, String destClusterName, String storeName) {
    getVeniceHelixAdmin().updateClusterDiscovery(storeName, srcClusterName, destClusterName, srcClusterName);
  }

  /**
   * Abort store migration by sending a {@link AdminMessageType#ABORT_MIGRATION ABORT_MIGRATION} admin message.
   */
  @Override
  public void abortMigration(String srcClusterName, String destClusterName, String storeName) {
    if (srcClusterName.equals(destClusterName)) {
      throw new VeniceException("Source cluster and destination cluster cannot be the same!");
    }

    AbortMigration abortMigration = (AbortMigration) AdminMessageType.ABORT_MIGRATION.getNewInstance();
    abortMigration.srcClusterName = srcClusterName;
    abortMigration.destClusterName = destClusterName;
    abortMigration.storeName = storeName;

    // Trigger store migration operation
    AdminOperation message = new AdminOperation();
    message.operationType = AdminMessageType.ABORT_MIGRATION.getValue();
    message.payloadUnion = abortMigration;
    sendAdminMessageAndWaitForConsumed(srcClusterName, storeName, message);
  }

  @Override
  public StoreMetaValue getMetaStoreValue(StoreMetaKey metaKey, String storeName) {
    throw new VeniceException("Not implemented in parent");
  }

  /**
   * Check if etled proxy account is set before enabling any ETL and return a {@link ETLStoreConfigRecord}
   */
  private ETLStoreConfigRecord mergeNewSettingIntoOldETLStoreConfig(
      Store store,
      Optional<Boolean> regularVersionETLEnabled,
      Optional<Boolean> futureVersionETLEnabled,
      Optional<String> etledUserProxyAccount) {
    ETLStoreConfig etlStoreConfig = store.getEtlStoreConfig();
    /**
     * If etl enabled is true (either current version or future version), then account name must be specified in the command
     * and it's not empty, or the store metadata already contains a non-empty account name.
     */
    if (regularVersionETLEnabled.orElse(false) || futureVersionETLEnabled.orElse(false)) {
      if ((!etledUserProxyAccount.isPresent() || etledUserProxyAccount.get().isEmpty())
          && (etlStoreConfig.getEtledUserProxyAccount() == null
              || etlStoreConfig.getEtledUserProxyAccount().isEmpty())) {
        throw new VeniceException("Cannot enable ETL for this store because etled user proxy account is not set");
      }
    }
    ETLStoreConfigRecord etlStoreConfigRecord = new ETLStoreConfigRecord();
    etlStoreConfigRecord.etledUserProxyAccount =
        etledUserProxyAccount.orElse(etlStoreConfig.getEtledUserProxyAccount());
    etlStoreConfigRecord.regularVersionETLEnabled =
        regularVersionETLEnabled.orElse(etlStoreConfig.isRegularVersionETLEnabled());
    etlStoreConfigRecord.futureVersionETLEnabled =
        futureVersionETLEnabled.orElse(etlStoreConfig.isFutureVersionETLEnabled());
    return etlStoreConfigRecord;
  }

  /**
   * This parses the input accessPermission string to create ACL's and provision them using the authorizerService interface.
   *
   * The json interface to acl API always accepts a list of "principal" which are "ALLOWED" to perform "method"
   * "read" or "write" on the "resource". We assume a failclose implementation here where only explicitly listed
   * "principals" are "ALLOWED" to perform "method" on "resource".
   * So we always pass acls with "ALLOW" permission to underlying implementation.
   *
   * @param storeName store being provisioned.
   * @param accessPermissions json string respresenting the accesspermissions.
   * @param enabledVeniceSystemStores list of enabled Venice system stores for the given store.
   */
  private void provisionAclsForStore(
      String storeName,
      Optional<String> accessPermissions,
      List<VeniceSystemStoreType> enabledVeniceSystemStores) {
    // provision the ACL's needed to read/write venice store and kafka topic
    if (authorizerService.isPresent() && accessPermissions.isPresent()) {
      Resource resource = new Resource(storeName);
      Iterator<JsonNode> readPermissions = null;
      Iterator<JsonNode> writePermissions = null;
      ObjectMapper mapper = ObjectMapperFactory.getInstance();
      try {
        JsonNode root = mapper.readTree(accessPermissions.get());
        JsonNode perms = root.path("AccessPermissions");
        if (perms.has("Read")) {
          readPermissions = perms.path("Read").elements();
        }
        if (perms.has("Write")) {
          writePermissions = perms.path("Write").elements();
        }
      } catch (Exception e) {
        LOGGER.error("ACLProvisioning: invalid accessPermission schema for store: {}", storeName, e);
        throw new VeniceException(e);
      }

      try {
        AclBinding aclBinding = new AclBinding(resource);
        if (readPermissions != null) {
          while (readPermissions.hasNext()) {
            String readPerm = readPermissions.next().textValue();
            Principal principal = new Principal(readPerm);
            AceEntry readAceEntry = new AceEntry(principal, Method.Read, Permission.ALLOW);
            aclBinding.addAceEntry(readAceEntry);
          }
        }
        if (writePermissions != null) {
          while (writePermissions.hasNext()) {
            String writePerm = writePermissions.next().textValue();
            Principal principal = new Principal(writePerm);
            AceEntry writeAceEntry = new AceEntry(principal, Method.Write, Permission.ALLOW);
            aclBinding.addAceEntry(writeAceEntry);
          }
        }
        authorizerService.get().setAcls(aclBinding);
        // Provision the ACL's needed to read/write corresponding venice system stores if any are specified.
        for (VeniceSystemStoreType veniceSystemStoreType: enabledVeniceSystemStores) {
          AclBinding systemStoreAclBinding = veniceSystemStoreType.generateSystemStoreAclBinding(aclBinding);
          authorizerService.get().setAcls(systemStoreAclBinding);
        }
      } catch (Exception e) {
        LOGGER.error("ACLProvisioning: failure when setting ACL's for store: {}", storeName, e);
        throw new VeniceException(e);
      }
    }
  }

  /**
   * This fetches currently provisioned ACL's using authorizerService interface and converts them to output json.
   *
   * The json interface to acl API always return a list of "principal" which are "ALLOWED" to perform "method"
   * "read" or "write" on the "resource". We assume a failclose implementation here where only explicitly listed
   * "principals" are "ALLOWED" to perform "method" on "resource"
   * So even if the underlying implementation return "DENY" acls, filter them out here.
   *
   * @param storeName store name
   * @return a json string represnting currently provisioned ACL's or empty string if co ACL's are present currently.
   */
  private String fetchAclsForStore(String storeName) {
    String result = "";
    try {
      Resource resource = new Resource(storeName);
      AclBinding aclBinding = authorizerService.get().describeAcls(resource);
      if (aclBinding == null) {
        LOGGER.error("ACLProvisioning: null ACL returned for store: {}", storeName);
        return result;
      }

      // return empty string in case there is no ACL's present currently.
      if (aclBinding.countAceEntries() == 0) {
        return "";
      }

      JsonNodeFactory factory = JsonNodeFactory.instance;
      ObjectMapper mapper = ObjectMapperFactory.getInstance();
      ObjectNode root = factory.objectNode();
      ObjectNode perms = factory.objectNode();
      ArrayNode readP = factory.arrayNode();
      ArrayNode writeP = factory.arrayNode();
      for (AceEntry aceEntry: aclBinding.getAceEntries()) {
        if (aceEntry.getPermission() != Permission.ALLOW) {
          continue;
        }
        if (aceEntry.getMethod() == Method.Read) {
          readP.add(aceEntry.getPrincipal().getName());
        } else if (aceEntry.getMethod() == Method.Write) {
          writeP.add(aceEntry.getPrincipal().getName());
        }
      }
      perms.replace("Read", readP);
      perms.replace("Write", writeP);
      root.replace("AccessPermissions", perms);
      result = mapper.writeValueAsString(root);
      return result;
    } catch (Exception e) {
      LOGGER.error("ACLProvisioning: failure in getting ACL's for store: {}", storeName, e);
      throw new VeniceException(e);
    }
  }

  /**
   * This deletes all existing ACL's for a store using the authorizerService interface.
   * @param storeName store being provisioned.
   */
  private void cleanUpAclsForStore(String storeName, List<VeniceSystemStoreType> enabledVeniceSystemStores) {
    if (authorizerService.isPresent()) {
      Resource resource = new Resource(storeName);
      try {
        authorizerService.get().clearAcls(resource);
        for (VeniceSystemStoreType veniceSystemStoreType: enabledVeniceSystemStores) {
          Resource systemStoreResource = new Resource(veniceSystemStoreType.getSystemStoreName(storeName));
          authorizerService.get().clearAcls(systemStoreResource);
          authorizerService.get().clearResource(systemStoreResource);
        }
      } catch (Exception e) {
        LOGGER.error("ACLProvisioning: failure in deleting ACL's for store: {}", storeName, e);
        throw new VeniceException(e);
      }
    }
  }

  /**
   * @see Admin#updateAclForStore(String, String, String)
   */
  @Override
  public void updateAclForStore(String clusterName, String storeName, String accessPermissions) {
    HelixVeniceClusterResources resources = getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
      LOGGER.info("ACLProvisioning: UpdateAcl for store: {} in cluster: {}", storeName, clusterName);
      if (!authorizerService.isPresent()) {
        throw new VeniceUnsupportedOperationException("updateAclForStore is not supported yet!");
      }
      Store store = getVeniceHelixAdmin().checkPreConditionForAclOp(clusterName, storeName);
      provisionAclsForStore(
          storeName,
          Optional.of(accessPermissions),
          VeniceSystemStoreType.getEnabledSystemStoreTypes(store));
    }
  }

  /**
   * Set the AceEntries in provided AclBinding object to be the current set of ACL's for the resource.
   */
  public void updateSystemStoreAclForStore(
      String clusterName,
      String regularStoreName,
      AclBinding systemStoreAclBinding) {
    HelixVeniceClusterResources resources = getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(regularStoreName)) {
      if (!authorizerService.isPresent()) {
        throw new VeniceUnsupportedOperationException("updateAclForStore is not supported yet!");
      }
      getVeniceHelixAdmin().checkPreConditionForAclOp(clusterName, regularStoreName);
      authorizerService.get().setAcls(systemStoreAclBinding);
    }
  }

  /**
   * @see Admin#getAclForStore(String, String)
   */
  @Override
  public String getAclForStore(String clusterName, String storeName) {
    HelixVeniceClusterResources resources = getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreReadLock(storeName)) {
      LOGGER.info("ACLProvisioning: GetAcl for store: {} in cluster: {}", storeName, clusterName);
      if (!authorizerService.isPresent()) {
        throw new VeniceUnsupportedOperationException("getAclForStore is not supported yet!");
      }
      getVeniceHelixAdmin().checkPreConditionForAclOp(clusterName, storeName);
      String accessPerms = fetchAclsForStore(storeName);
      return accessPerms;
    }
  }

  /**
   * @see Admin#deleteAclForStore(String, String)
   */
  @Override
  public void deleteAclForStore(String clusterName, String storeName) {
    HelixVeniceClusterResources resources = getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
      LOGGER.info("ACLProvisioning: DeleteAcl for store: {} in cluster: {}", storeName, clusterName);
      if (!authorizerService.isPresent()) {
        throw new VeniceUnsupportedOperationException("deleteAclForStore is not supported yet!");
      }
      Store store = getVeniceHelixAdmin().checkPreConditionForAclOp(clusterName, storeName);
      if (!store.isMigrating()) {
        cleanUpAclsForStore(storeName, VeniceSystemStoreType.getEnabledSystemStoreTypes(store));
      } else {
        LOGGER.info("Store {} is migrating! Skipping acl deletion!", storeName);
      }
    }
  }

  /**
   * @see Admin#configureActiveActiveReplication(String, VeniceUserStoreType, Optional, boolean, Optional)
   */
  @Override
  public void configureActiveActiveReplication(
      String clusterName,
      VeniceUserStoreType storeType,
      Optional<String> storeName,
      boolean enableNativeReplicationForCluster,
      Optional<String> regionsFilter) {
    ConfigureActiveActiveReplicationForCluster migrateClusterToActiveActiveReplication =
        (ConfigureActiveActiveReplicationForCluster) AdminMessageType.CONFIGURE_ACTIVE_ACTIVE_REPLICATION_FOR_CLUSTER
            .getNewInstance();
    migrateClusterToActiveActiveReplication.clusterName = clusterName;
    migrateClusterToActiveActiveReplication.storeType = storeType.toString();
    migrateClusterToActiveActiveReplication.enabled = enableNativeReplicationForCluster;
    migrateClusterToActiveActiveReplication.regionsFilter = regionsFilter.orElse(null);

    AdminOperation message = new AdminOperation();
    message.operationType = AdminMessageType.CONFIGURE_ACTIVE_ACTIVE_REPLICATION_FOR_CLUSTER.getValue();
    message.payloadUnion = migrateClusterToActiveActiveReplication;
    sendAdminMessageAndWaitForConsumed(clusterName, null, message);
  }

  /**
   * This function will iterate over all of Helix Parent Admin's child controllers,
   * in order to ask about stale stores.
   */
  @Override
  public Map<String, StoreDataAudit> getClusterStaleStores(String clusterName) {
    Map<String, StoreDataAudit> dataMap = new HashMap<>();
    Map<String, StoreDataAudit> retMap = new HashMap<>();
    try {
      Map<String, ControllerClient> childControllers = getVeniceHelixAdmin().getControllerClientMap(clusterName);

      // iterate through child controllers
      for (Map.Entry<String, ControllerClient> controller: childControllers.entrySet()) {
        MultiStoreInfoResponse response = controller.getValue().getClusterStores(clusterName); // get all stores from
                                                                                               // child
        response.getStoreInfoList().forEach((storeInfo) -> {
          dataMap.putIfAbsent(storeInfo.getName(), new StoreDataAudit());
          dataMap.get(storeInfo.getName()).setStoreName(storeInfo.getName());
          dataMap.get(storeInfo.getName()).insert(controller.getKey(), storeInfo); // StoreDataAudit.insert manages
                                                                                   // version, and healthy/stale region
                                                                                   // delineation
        });
      }
      // filter out
      for (Map.Entry<String, StoreDataAudit> store: dataMap.entrySet()) {
        StoreDataAudit audit = store.getValue();
        Optional<String> currentPushJobTopic =
            getTopicForCurrentPushJob(clusterName, store.getValue().getStoreName(), false, false);
        if (!audit.getStaleRegions().isEmpty() && !currentPushJobTopic.isPresent()) {
          retMap.put(store.getKey(), audit);
        }
      }
    } catch (Exception e) {
      throw new VeniceException("Something went wrong trying to fetch stale stores.", e);
    }
    return retMap;
  }

  /**
   * @return the largest used version number for the given store from the store graveyard.
   */
  @Override
  public int getLargestUsedVersionFromStoreGraveyard(String clusterName, String storeName) {
    Map<String, ControllerClient> childControllers = getVeniceHelixAdmin().getControllerClientMap(clusterName);
    int aggregatedLargestUsedVersionNumber =
        getVeniceHelixAdmin().getStoreGraveyard().getLargestUsedVersionNumber(storeName);
    for (Map.Entry<String, ControllerClient> controller: childControllers.entrySet()) {
      VersionResponse response = controller.getValue().getStoreLargestUsedVersion(clusterName, storeName);
      if (response.getVersion() > aggregatedLargestUsedVersionNumber) {
        aggregatedLargestUsedVersionNumber = response.getVersion();
      }
    }
    return aggregatedLargestUsedVersionNumber;
  }

  /**
   * Unsupported operation in the parent controller.
   */
  public ArrayList<StoreInfo> getClusterStores(String clusterName) {
    throw new UnsupportedOperationException("This function has no implementation.");
  }

  /**
   * Unsupported operation in the parent controller.
   */
  @Override
  public RegionPushDetails getRegionPushDetails(String clusterName, String storeName, boolean isPartitionDetailAdded) {
    throw new UnsupportedOperationException("This function has no implementation.");
  }

  /**
   * This function will look for a single store, given a name and cluster name, and return information about the current
   * push jobs for that store across all regions.
   */
  @Override
  public Map<String, RegionPushDetails> listStorePushInfo(
      String clusterName,
      String storeName,
      boolean isPartitionDetailEnabled) {
    Map<String, RegionPushDetails> retMap = new HashMap<>();
    try {
      Map<String, ControllerClient> controllerClientMap = getVeniceHelixAdmin().getControllerClientMap(clusterName);
      for (Map.Entry<String, ControllerClient> entry: controllerClientMap.entrySet()) {
        RegionPushDetailsResponse detailsResp =
            entry.getValue().getRegionPushDetails(storeName, isPartitionDetailEnabled);
        if (detailsResp != null && detailsResp.getRegionPushDetails() != null) {
          detailsResp.getRegionPushDetails().setRegionName(entry.getKey());
          retMap.put(entry.getKey(), detailsResp.getRegionPushDetails());
        }
      }

    } catch (Exception e) {
      throw new VeniceException("Something went wrong trying to get store push info. ", e);
    }
    return retMap;
  }

  /**
   * This function will check whether there are still resources left for the requested store in the requested
   * cluster.
   * This function will check both parent colo and all prod colos.
   */
  @Override
  public void checkResourceCleanupBeforeStoreCreation(String clusterName, String storeName) {
    try {
      // Check local parent colo first
      getVeniceHelixAdmin().checkResourceCleanupBeforeStoreCreation(clusterName, storeName, false);
      // Check all the prod colos to see whether there are still resources left from the previous store.
      Map<String, ControllerClient> controllerClientMap = getVeniceHelixAdmin().getControllerClientMap(clusterName);
      controllerClientMap.forEach((coloName, cc) -> {
        ControllerResponse controllerResponse = cc.checkResourceCleanupForStoreCreation(storeName);
        if (controllerResponse.isError()) {
          throw new VeniceException(controllerResponse.getError() + " in colo: " + coloName);
        }
      });
    } catch (VeniceException e) {
      throw new VeniceException(
          "Encountered the following error during re-creation check, please try to recreate" + " your store later: "
              + e.getMessage());
    }
  }

  /**
   * @see Admin#isParent()
   */
  @Override
  public boolean isParent() {
    return getVeniceHelixAdmin().isParent();
  }

  /**
   * @see Admin#getParentControllerRegionState()
   */
  @Override
  public ParentControllerRegionState getParentControllerRegionState() {
    return getVeniceHelixAdmin().getParentControllerRegionState();
  }

  /**
   * @see Admin#getChildDataCenterControllerUrlMap(String)
   */
  @Override
  public Map<String, String> getChildDataCenterControllerUrlMap(String clusterName) {
    return getVeniceHelixAdmin().getChildDataCenterControllerUrlMap(clusterName);
  }

  /**
   * @see Admin#getChildDataCenterControllerD2Map(String)
   */
  @Override
  public Map<String, String> getChildDataCenterControllerD2Map(String clusterName) {
    return getVeniceHelixAdmin().getChildDataCenterControllerD2Map(clusterName);
  }

  /**
   * @see Admin#getChildControllerD2ServiceName(String)
   */
  @Override
  public String getChildControllerD2ServiceName(String clusterName) {
    return getVeniceHelixAdmin().getChildControllerD2ServiceName(clusterName);
  }

  /**
   * @see Admin#getStoreConfigRepo()
   */
  @Override
  public HelixReadOnlyStoreConfigRepository getStoreConfigRepo() {
    return getVeniceHelixAdmin().getStoreConfigRepo();
  }

  /**
   * @see Admin#getReadOnlyZKSharedSystemStoreRepository()
   */
  @Override
  public HelixReadOnlyZKSharedSystemStoreRepository getReadOnlyZKSharedSystemStoreRepository() {
    return getVeniceHelixAdmin().getReadOnlyZKSharedSystemStoreRepository();
  }

  /**
   * @see Admin#getReadOnlyZKSharedSystemStoreRepository()
   */
  @Override
  public HelixReadOnlyZKSharedSchemaRepository getReadOnlyZKSharedSchemaRepository() {
    return getVeniceHelixAdmin().getReadOnlyZKSharedSchemaRepository();
  }

  /**
   * @see Admin#getMetaStoreWriter()
   */
  @Override
  public MetaStoreWriter getMetaStoreWriter() {
    return getVeniceHelixAdmin().getMetaStoreWriter();
  }

  @Override
  public MetaStoreReader getMetaStoreReader() {
    return getVeniceHelixAdmin().getMetaStoreReader();
  }

  /**
   * @see Admin#getEmergencySourceRegion(String)
   */
  @Override
  public Optional<String> getEmergencySourceRegion(@Nonnull String clusterName) {
    String emergencySourceRegion = multiClusterConfigs.getEmergencySourceRegion(clusterName);
    if (StringUtils.isNotEmpty(emergencySourceRegion)) {
      return Optional.of(emergencySourceRegion);
    } else {
      return Optional.empty();
    }
  }

  /**
   * @see Admin#getAggregateRealTimeTopicSource(String)
   */
  @Override
  public Optional<String> getAggregateRealTimeTopicSource(String clusterName) {
    return getVeniceHelixAdmin().getAggregateRealTimeTopicSource(clusterName);
  }

  /**
   * @see Admin#getClustersLeaderOf()
   */
  @Override
  public List<String> getClustersLeaderOf() {
    return getVeniceHelixAdmin().getClustersLeaderOf();
  }

  public VeniceHelixAdmin getVeniceHelixAdmin() {
    return veniceHelixAdmin;
  }

  private <T> Function<T, T> addToUpdatedConfigList(List<CharSequence> updatedConfigList, String config) {
    return (configValue) -> {
      updatedConfigList.add(config);
      return configValue;
    };
  }

  static private <T> Function<T, T> addToUpdatedConfigList(
      List<CharSequence> updatedConfigList,
      String config,
      String legacyConfigName) {
    return (configValue) -> {
      updatedConfigList.add(config);
      updatedConfigList.add(legacyConfigName);
      return configValue;
    };
  }

  /**
   * @see Admin#getBackupVersionDefaultRetentionMs()
   */
  @Override
  public long getBackupVersionDefaultRetentionMs() {
    return getVeniceHelixAdmin().getBackupVersionDefaultRetentionMs();
  }

  /** @see Admin#getDefaultMaxRecordSizeBytes() */
  @Override
  public int getDefaultMaxRecordSizeBytes() {
    return getVeniceHelixAdmin().getDefaultMaxRecordSizeBytes();
  }

  /**
   * Delete stores from the cluster by sending a {@link ControllerClient#wipeCluster(String, Optional, Optional)} request.
   */
  @Override
  public void wipeCluster(String clusterName, String fabric, Optional<String> storeName, Optional<Integer> versionNum) {
    ControllerClient childControllerClient = getFabricBuildoutControllerClient(clusterName, fabric);
    ControllerResponse response = childControllerClient.wipeCluster(fabric, storeName, versionNum);
    if (response.isError()) {
      throw new VeniceException(
          "Could not wipe cluster " + clusterName + " in colo: " + fabric + ". " + response.getError());
    }
  }

  /**
   * @see Admin#compareStore(String, String, String, String)
   */
  @Override
  public StoreComparisonInfo compareStore(String clusterName, String storeName, String fabricA, String fabricB)
      throws IOException {
    ControllerClient controllerClientA = getFabricBuildoutControllerClient(clusterName, fabricA);
    ControllerClient controllerClientB = getFabricBuildoutControllerClient(clusterName, fabricB);

    StoreComparisonInfo result = new StoreComparisonInfo();
    compareStoreProperties(storeName, fabricA, fabricB, controllerClientA, controllerClientB, result);
    compareStoreSchemas(storeName, fabricA, fabricB, controllerClientA, controllerClientB, result);
    compareStoreVersions(storeName, fabricA, fabricB, controllerClientA, controllerClientB, result);

    return result;
  }

  private static void compareStoreProperties(
      String storeName,
      String fabricA,
      String fabricB,
      ControllerClient controllerClientA,
      ControllerClient controllerClientB,
      StoreComparisonInfo result) {
    StoreInfo storeA = checkControllerResponse(controllerClientA.getStore(storeName), fabricA).getStore();
    StoreInfo storeB = checkControllerResponse(controllerClientB.getStore(storeName), fabricB).getStore();
    ObjectMapper mapper = ObjectMapperFactory.getInstance();
    Map<String, Object> storePropertiesA = mapper.convertValue(storeA, Map.class);
    Map<String, Object> storePropertiesB = mapper.convertValue(storeB, Map.class);

    for (Map.Entry<String, Object> entry: storePropertiesA.entrySet()) {
      // Filter out non-store-level configs
      if (entry.getKey().equals("coloToCurrentVersions") || entry.getKey().equals("versions")
          || entry.getKey().equals("kafkaBrokerUrl")) {
        continue;
      }
      if (!Objects.equals(entry.getValue(), storePropertiesB.get(entry.getKey()))) {
        result.addPropertyDiff(
            fabricA,
            fabricB,
            entry.getKey(),
            entry.getValue().toString(),
            storePropertiesB.get(entry.getKey()).toString());
      }
    }
  }

  private static void compareStoreSchemas(
      String storeName,
      String fabricA,
      String fabricB,
      ControllerClient controllerClientA,
      ControllerClient controllerClientB,
      StoreComparisonInfo result) {
    String keySchemaA = checkControllerResponse(controllerClientA.getKeySchema(storeName), fabricA).getSchemaStr();
    String keySchemaB = checkControllerResponse(controllerClientB.getKeySchema(storeName), fabricB).getSchemaStr();

    if (!Objects.equals(keySchemaA, keySchemaB)) {
      result.addSchemaDiff(fabricA, fabricB, "key-schema", keySchemaA, keySchemaB);
    }
    populateSchemaDiff(
        fabricA,
        fabricB,
        checkControllerResponse(controllerClientA.getAllValueAndDerivedSchema(storeName), fabricA).getSchemas(),
        checkControllerResponse(controllerClientB.getAllValueAndDerivedSchema(storeName), fabricB).getSchemas(),
        "derived-schema",
        result);
    populateSchemaDiff(
        fabricA,
        fabricB,
        checkControllerResponse(controllerClientA.getAllReplicationMetadataSchemas(storeName), fabricA).getSchemas(),
        checkControllerResponse(controllerClientB.getAllReplicationMetadataSchemas(storeName), fabricB).getSchemas(),
        "timestamp-metadata-schema",
        result);
  }

  private static void compareStoreVersions(
      String storeName,
      String fabricA,
      String fabricB,
      ControllerClient controllerClientA,
      ControllerClient controllerClientB,
      StoreComparisonInfo result) {
    StoreInfo storeA = checkControllerResponse(controllerClientA.getStore(storeName), fabricA).getStore();
    StoreInfo storeB = checkControllerResponse(controllerClientB.getStore(storeName), fabricB).getStore();
    List<Version> versionsB = storeB.getVersions();

    for (Version version: storeA.getVersions()) {
      int versionNum = version.getNumber();
      Optional<Version> versionB = storeB.getVersion(versionNum);
      if (versionB.isPresent()) {
        if (!version.getStatus().equals(versionB.get().getStatus())) {
          result.addVersionStateDiff(fabricA, fabricB, versionNum, version.getStatus(), versionB.get().getStatus());
        }
        versionsB.remove(versionB.get());
      } else {
        if (storeB.getLargestUsedVersionNumber() >= versionNum) {
          // Version was added but then deleted due to errors
          result.addVersionStateDiff(fabricA, fabricB, versionNum, version.getStatus(), VersionStatus.ERROR);
        } else {
          result.addVersionStateDiff(fabricA, fabricB, versionNum, version.getStatus(), VersionStatus.NOT_CREATED);
        }
      }
    }
    for (Version version: versionsB) {
      if (storeA.getLargestUsedVersionNumber() >= version.getNumber()) {
        result.addVersionStateDiff(fabricA, fabricB, version.getNumber(), VersionStatus.ERROR, version.getStatus());
      } else {
        result
            .addVersionStateDiff(fabricA, fabricB, version.getNumber(), VersionStatus.NOT_CREATED, version.getStatus());
      }
    }
  }

  private static <T extends ControllerResponse> T checkControllerResponse(T controllerResponse, String fabric) {
    if (controllerResponse.isError()) {
      throw new VeniceException("ControllerResponse from fabric " + fabric + " has error " + controllerResponse);
    }
    return controllerResponse;
  }

  private static void populateSchemaDiff(
      String fabricA,
      String fabricB,
      MultiSchemaResponse.Schema[] schemasA,
      MultiSchemaResponse.Schema[] schemasB,
      String derivedSchemaName,
      StoreComparisonInfo storeComparisonInfo) {
    Map<String, String> schemaMapB = new HashMap<>();
    for (MultiSchemaResponse.Schema schema: schemasB) {
      String key = schema.getDerivedSchemaId() == -1
          ? "value-schema-" + schema.getId()
          : derivedSchemaName + "-" + schema.getId() + "-" + schema.getDerivedSchemaId();
      schemaMapB.put(key, schema.getSchemaStr());
    }
    for (MultiSchemaResponse.Schema schema: schemasA) {
      String key = schema.getDerivedSchemaId() == -1
          ? "value-schema-" + schema.getId()
          : derivedSchemaName + "-" + schema.getId() + "-" + schema.getDerivedSchemaId();
      if (schemaMapB.containsKey(key)) {
        if (!schema.getSchemaStr().equals(schemaMapB.get(key))) {
          storeComparisonInfo.addSchemaDiff(fabricA, fabricB, key, schema.getSchemaStr(), schemaMapB.get(key));
        }
        schemaMapB.remove(key);
      } else {
        storeComparisonInfo.addSchemaDiff(fabricA, fabricB, key, schema.getSchemaStr(), "N/A");
      }
    }
    for (Map.Entry<String, String> entry: schemaMapB.entrySet()) {
      storeComparisonInfo.addSchemaDiff(fabricA, fabricB, entry.getKey(), "N/A", entry.getValue());
    }
  }

  /**
   * @see Admin#copyOverStoreSchemasAndConfigs(String, String, String, String)
   */
  @Override
  public StoreInfo copyOverStoreSchemasAndConfigs(
      String clusterName,
      String srcFabric,
      String destFabric,
      String storeName) {
    try {
      ControllerClient srcFabricChildControllerClient = getFabricBuildoutControllerClient(clusterName, srcFabric);
      ControllerClient destFabricChildControllerClient = getFabricBuildoutControllerClient(clusterName, destFabric);
      long storeExecutionId;
      StoreInfo storeInfo;
      String keySchema;
      MultiSchemaResponse.Schema[] valueAndDerivedSchemas;
      // Acquire a lock to guarantee parent controller cannot send new admin messages for the store during metadata dump
      acquireAdminMessageLock(clusterName, storeName);
      try {
        // Src fabric local controller dumps out the store's execution id, configs and schemas
        storeExecutionId =
            srcFabricChildControllerClient.getAdminTopicMetadata(Optional.of(storeName)).getExecutionId();
        storeInfo = srcFabricChildControllerClient.getStore(storeName).getStore();
        keySchema = srcFabricChildControllerClient.getKeySchema(storeName).getSchemaStr();
        valueAndDerivedSchemas = srcFabricChildControllerClient.getAllValueAndDerivedSchema(storeName).getSchemas();
      } catch (Exception e) {
        throw new VeniceException(
            "Error when getting store " + storeName + " metadata from source fabric " + srcFabric + " Exception: "
                + e.getMessage());
      } finally {
        releaseAdminMessageLock(clusterName, storeName);
      }

      // sort schemas with sorted value schemas first, and then sorted derived schemas.
      Arrays.sort(valueAndDerivedSchemas, new Comparator<MultiSchemaResponse.Schema>() {
        @Override
        public int compare(MultiSchemaResponse.Schema o1, MultiSchemaResponse.Schema o2) {
          int distance = o1.getDerivedSchemaId() - o2.getDerivedSchemaId();
          if (distance == 0) {
            return o1.getId() - o2.getId();
          }
          return distance;
        }
      });

      // Dest fabric controller creates the stores and copies over value schemas and configs
      destFabricChildControllerClient.createNewStore(
          storeInfo.getName(),
          storeInfo.getOwner(),
          keySchema,
          valueAndDerivedSchemas[0].getSchemaStr());
      for (int i = 1; i < valueAndDerivedSchemas.length; i++) {
        MultiSchemaResponse.Schema schema = valueAndDerivedSchemas[i];
        if (schema.getDerivedSchemaId() == -1) {
          destFabricChildControllerClient.addValueSchema(storeInfo.getName(), schema.getSchemaStr(), schema.getId());
        } else {
          destFabricChildControllerClient.addDerivedSchema(
              storeInfo.getName(),
              schema.getId(),
              schema.getSchemaStr(),
              schema.getDerivedSchemaId());
        }
      }
      UpdateStoreQueryParams params = new UpdateStoreQueryParams(storeInfo, false);
      ControllerResponse response = destFabricChildControllerClient.updateStore(storeInfo.getName(), params);
      if (response.isError()) {
        throw new VeniceException("Failed to update store " + response.getError());
      }

      response = destFabricChildControllerClient
          .updateAdminTopicMetadata(storeExecutionId, Optional.of(storeName), Optional.empty(), Optional.empty());
      if (response.isError()) {
        throw new VeniceException("Failed to update store's execution id " + response.getError());
      }

      return storeInfo;
    } catch (Exception e) {
      throw new VeniceException("Error copying src fabric's metadata to dest fabric.", e.getCause());
    }
  }

  // Allow overriding in tests
  LingeringStoreVersionChecker getLingeringStoreVersionChecker() {
    return lingeringStoreVersionChecker;
  }

  VeniceControllerMultiClusterConfig getMultiClusterConfigs() {
    return multiClusterConfigs;
  }

  UserSystemStoreLifeCycleHelper getSystemStoreLifeCycleHelper() {
    return systemStoreLifeCycleHelper;
  }

  private ControllerClient getFabricBuildoutControllerClient(String clusterName, String fabric) {
    Map<String, ControllerClient> controllerClients = getVeniceHelixAdmin().getControllerClientMap(clusterName);
    if (controllerClients.containsKey(fabric)) {
      return controllerClients.get(fabric);
    }

    // For fabrics not in allowlist, build controller clients using child cluster configs and cache them in another map
    ControllerClient value =
        newFabricControllerClientMap.computeIfAbsent(clusterName, cn -> new VeniceConcurrentHashMap<>())
            .computeIfAbsent(fabric, f -> {
              VeniceControllerClusterConfig controllerConfig = multiClusterConfigs.getControllerConfig(clusterName);
              String d2ZkHost = controllerConfig.getChildControllerD2ZkHost(fabric);
              String d2ServiceName = controllerConfig.getD2ServiceName();
              if (StringUtils.isNotBlank(d2ZkHost) && StringUtils.isNotBlank(d2ServiceName)) {
                return new D2ControllerClient(d2ServiceName, clusterName, d2ZkHost, sslFactory);
              }
              String url = controllerConfig.getChildControllerUrl(fabric);
              if (StringUtils.isNotBlank(url)) {
                return ControllerClient.constructClusterControllerClient(clusterName, url, sslFactory);
              }
              return null;
            });

    if (value == null) {
      throw new VeniceException(
          "Could not construct child controller client for cluster " + clusterName + " fabric " + fabric
              + ". child.cluster.d2 or child.cluster.url value is missing in parent controller");
    }
    return value;
  }

  /**
   * Creates a new persona with the given parameters.
   * @see StoragePersonaRepository#addPersona(String, long, Set, Set)
   */
  @Override
  public void createStoragePersona(
      String clusterName,
      String name,
      long quotaNumber,
      Set<String> storesToEnforce,
      Set<String> owners) {
    getVeniceHelixAdmin().checkControllerLeadershipFor(clusterName);

    CreateStoragePersona createStoragePersona =
        (CreateStoragePersona) AdminMessageType.CREATE_STORAGE_PERSONA.getNewInstance();
    createStoragePersona.setClusterName(clusterName);
    createStoragePersona.setName(name);
    createStoragePersona.setQuotaNumber(quotaNumber);
    createStoragePersona.setStoresToEnforce(new ArrayList<>(storesToEnforce));
    createStoragePersona.setOwners(new ArrayList<>(owners));

    AdminOperation message = new AdminOperation();
    message.operationType = AdminMessageType.CREATE_STORAGE_PERSONA.getValue();
    message.payloadUnion = createStoragePersona;

    StoragePersonaRepository repository =
        getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName).getStoragePersonaRepository();
    if (repository.hasPersona(name)) {
      throw new VeniceException("Persona with name " + name + " already exists");
    }
    repository.validatePersona(name, quotaNumber, storesToEnforce, owners);
    sendAdminMessageAndWaitForConsumed(clusterName, null, message);
  }

  /**
   * @see VeniceHelixAdmin#getStoragePersona(String, String)
   */
  @Override
  public StoragePersona getStoragePersona(String clusterName, String name) {
    return getVeniceHelixAdmin().getStoragePersona(clusterName, name);
  }

  /**
   * Deletes the persona with the given name. If no persona is found, this method does nothing.
   */
  @Override
  public void deleteStoragePersona(String clusterName, String name) {
    getVeniceHelixAdmin().checkControllerLeadershipFor(clusterName);
    DeleteStoragePersona deleteStoragePersona =
        (DeleteStoragePersona) AdminMessageType.DELETE_STORAGE_PERSONA.getNewInstance();
    deleteStoragePersona.setClusterName(clusterName);
    deleteStoragePersona.setName(name);

    AdminOperation message = new AdminOperation();
    message.operationType = AdminMessageType.DELETE_STORAGE_PERSONA.getValue();
    message.payloadUnion = deleteStoragePersona;

    sendAdminMessageAndWaitForConsumed(clusterName, null, message);
  }

  /**
   * Updates a persona with the given parameters by sending a
   * {@link AdminMessageType#UPDATE_STORAGE_PERSONA UPDATE_STORAGE_PERSONA} admin message.
   */
  @Override
  public void updateStoragePersona(String clusterName, String name, UpdateStoragePersonaQueryParams queryParams) {
    getVeniceHelixAdmin().checkControllerLeadershipFor(clusterName);
    UpdateStoragePersona updateStoragePersona =
        (UpdateStoragePersona) AdminMessageType.UPDATE_STORAGE_PERSONA.getNewInstance();
    updateStoragePersona.setClusterName(clusterName);
    updateStoragePersona.setName(name);
    updateStoragePersona.setQuotaNumber(queryParams.getQuota().orElse(null));
    updateStoragePersona.setStoresToEnforce(queryParams.getStoresToEnforceAsList().orElse(null));
    updateStoragePersona.setOwners(queryParams.getOwnersAsList().orElse(null));
    AdminOperation message = new AdminOperation();
    message.operationType = AdminMessageType.UPDATE_STORAGE_PERSONA.getValue();
    message.payloadUnion = updateStoragePersona;

    StoragePersonaRepository repository =
        getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName).getStoragePersonaRepository();
    repository.validatePersonaUpdate(name, queryParams);
    sendAdminMessageAndWaitForConsumed(clusterName, null, message);
  }

  /**
   * @see VeniceHelixAdmin#getPersonaAssociatedWithStore(String, String)
   */
  @Override
  public StoragePersona getPersonaAssociatedWithStore(String clusterName, String storeName) {
    return getVeniceHelixAdmin().getPersonaAssociatedWithStore(clusterName, storeName);
  }

  /**
   * @see VeniceHelixAdmin#getClusterStoragePersonas(String)
   */
  @Override
  public List<StoragePersona> getClusterStoragePersonas(String clusterName) {
    return getVeniceHelixAdmin().getClusterStoragePersonas(clusterName);
  }

  @Override
  public List<String> cleanupInstanceCustomizedStates(String clusterName) {
    throw new VeniceUnsupportedOperationException("cleanupInstanceCustomizedStates");
  }

  @Override
  public StoreGraveyard getStoreGraveyard() {
    return getVeniceHelixAdmin().getStoreGraveyard();
  }

  @Override
  public void removeStoreFromGraveyard(String clusterName, String storeName) {
    // Check parent data center to make sure store could be removed from graveyard.
    getVeniceHelixAdmin().checkKafkaTopicAndHelixResource(clusterName, storeName, true, false, false);
    // Check all child data centers to make sure store is removed from graveyard there.
    Map<String, ControllerClient> controllerClientMap = getVeniceHelixAdmin().getControllerClientMap(clusterName);
    controllerClientMap.forEach((coloName, cc) -> {
      ControllerResponse response = cc.removeStoreFromGraveyard(storeName);
      if (response.isError()) {
        if (ErrorType.RESOURCE_STILL_EXISTS.equals(response.getErrorType())) {
          throw new ResourceStillExistsException(
              "Store graveyard " + storeName + " is not ready for removal in colo: " + coloName);
        }
        throw new VeniceException(
            "Error when removing store graveyard " + storeName + " in colo: " + coloName + ". " + response.getError());
      }
    });
    getStoreGraveyard().removeStoreFromGraveyard(clusterName, storeName);
  }

  @Override
  public PushStatusStoreReader getPushStatusStoreReader() {
    throw new VeniceUnsupportedOperationException("Parent controller does not have Da Vinci push status store reader");
  }

  @Override
  public PushStatusStoreWriter getPushStatusStoreWriter() {
    return getVeniceHelixAdmin().getPushStatusStoreWriter();
  }

  @Override
  public void sendHeartbeatToSystemStore(String clusterName, String systemStoreName, long heartbeatTimestamp) {
    throw new VeniceUnsupportedOperationException("sendHeartbeatToSystemStore");
  }

  @Override
  public long getHeartbeatFromSystemStore(String clusterName, String storeName) {
    throw new VeniceUnsupportedOperationException("getHeartbeatFromSystemStore");
  }

  @Override
  public HelixVeniceClusterResources getHelixVeniceClusterResources(String cluster) {
    return getVeniceHelixAdmin().getHelixVeniceClusterResources(cluster);
  }

  @Override
  public PubSubTopicRepository getPubSubTopicRepository() {
    return pubSubTopicRepository;
  }
}
