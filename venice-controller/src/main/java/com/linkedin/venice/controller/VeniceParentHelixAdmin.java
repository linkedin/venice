package com.linkedin.venice.controller;

import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.acl.AclException;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.authorization.AceEntry;
import com.linkedin.venice.authorization.AclBinding;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.authorization.Method;
import com.linkedin.venice.authorization.Permission;
import com.linkedin.venice.authorization.Principal;
import com.linkedin.venice.authorization.Resource;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.authorization.SystemStoreAclSynchronizationTask;
import com.linkedin.venice.controller.datarecovery.DataRecoveryManager;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumerService;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumptionTask;
import com.linkedin.venice.controller.kafka.consumer.ControllerKafkaClientFactory;
import com.linkedin.venice.controller.kafka.protocol.admin.AbortMigration;
import com.linkedin.venice.controller.kafka.protocol.admin.AddVersion;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.ConfigureActiveActiveReplicationForCluster;
import com.linkedin.venice.controller.kafka.protocol.admin.ConfigureIncrementalPushForCluster;
import com.linkedin.venice.controller.kafka.protocol.admin.ConfigureNativeReplicationForCluster;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteAllVersions;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteOldVersion;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteStore;
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
import com.linkedin.venice.controller.kafka.protocol.admin.SchemaMeta;
import com.linkedin.venice.controller.kafka.protocol.admin.SetStoreOwner;
import com.linkedin.venice.controller.kafka.protocol.admin.SetStorePartitionCount;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.SupersetSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore;
import com.linkedin.venice.controller.kafka.protocol.admin.ValueSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.enums.SchemaType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controller.lingeringjob.DefaultLingeringStoreVersionChecker;
import com.linkedin.venice.controller.lingeringjob.IdentityParser;
import com.linkedin.venice.controller.lingeringjob.IdentityParserImpl;
import com.linkedin.venice.controller.lingeringjob.LingeringStoreVersionChecker;
import com.linkedin.venice.controller.migration.MigrationPushStrategyZKAccessor;
import com.linkedin.venice.controllerapi.AdminCommandExecution;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.IncrementalPushVersionsResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.MultiStoreInfoResponse;
import com.linkedin.venice.controllerapi.MultiStoreStatusResponse;
import com.linkedin.venice.controllerapi.NodeReplicasReadinessState;
import com.linkedin.venice.controllerapi.ReadyForDataRecoveryResponse;
import com.linkedin.venice.controllerapi.RegionPushDetailsResponse;
import com.linkedin.venice.controllerapi.RepushInfo;
import com.linkedin.venice.controllerapi.StoreComparisonInfo;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateClusterConfigQueryParams;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.exceptions.ExceptionType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSystemStoreRepository;
import com.linkedin.venice.helix.ParentHelixOfflinePushAccessor;
import com.linkedin.venice.helix.Replica;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.ETLStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.IncrementalPushPolicy;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RegionPushDetails;
import com.linkedin.venice.meta.RoutersClusterConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataAudit;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.VeniceUserStoreType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushstatushelper.PushStatusStoreRecordDeleter;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.schema.rmd.ReplicationMetadataSchemaEntry;
import com.linkedin.venice.schema.rmd.ReplicationMetadataSchemaGenerator;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatKey;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatValue;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.AvroSupersetSchemaUtils;
import com.linkedin.venice.utils.CollectionUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
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
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import static com.linkedin.venice.controller.VeniceHelixAdmin.*;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.meta.IncrementalPushPolicy.*;


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
  private static final long SLEEP_INTERVAL_FOR_ASYNC_SETUP_MS = 3000;
  private static final int MAX_ASYNC_SETUP_RETRY_COUNT = 10;
  private static final Logger logger = LogManager.getLogger(VeniceParentHelixAdmin.class);
  private static final String VENICE_INTERNAL_STORE_OWNER = "venice-internal";
  private static final String PUSH_JOB_DETAILS_STORE_DESCRIPTOR = "push job details store: ";
  private static final String BATCH_JOB_HEARTBEAT_STORE_DESCRIPTOR = "batch job liveness heartbeat store: ";
  //Store version number to retain in Parent Controller to limit 'Store' ZNode size.
  protected static final int STORE_VERSION_RETENTION_COUNT = 5;

  private static final long TOPIC_DELETION_DELAY_MS = 5 * Time.MS_PER_MINUTE;

  protected final Map<String, Boolean> asyncSetupEnabledMap;
  private final VeniceHelixAdmin veniceHelixAdmin;
  private final Map<String, VeniceWriter<byte[], byte[], byte[]>> veniceWriterMap;
  private final AdminTopicMetadataAccessor adminTopicMetadataAccessor;
  private final byte[] emptyKeyByteArr = new byte[0];
  private final AdminOperationSerializer adminOperationSerializer = new AdminOperationSerializer();
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final Map<String, Lock> perClusterAdminLocks = new ConcurrentHashMap<>();
  private final Map<String, AdminCommandExecutionTracker> adminCommandExecutionTrackers;
  private final Set<String> executionIdValidatedClusters = new HashSet<>();
  // Only used for setup work which are intended to be short lived and is bounded by the number of venice clusters.
  // Based on JavaDoc "Threads that have not been used for sixty seconds are terminated and removed from the cache."
  private final ExecutorService asyncSetupExecutor = Executors.newCachedThreadPool();
  private final ExecutorService topicCheckerExecutor = Executors.newSingleThreadExecutor();
  private final TerminalStateTopicCheckerForParentController terminalStateTopicChecker;
  private final SystemStoreAclSynchronizationTask systemStoreAclSynchronizationTask;
  private final UserSystemStoreLifeCycleHelper systemStoreLifeCycleHelper;
  private Time timer = new SystemTime();
  private Optional<SSLFactory> sslFactory = Optional.empty();

  private final MigrationPushStrategyZKAccessor pushStrategyZKAccessor;

  private ParentHelixOfflinePushAccessor offlinePushAccessor;

  /**
   * Here is the way how Parent Controller is keeping errored topics when {@link #maxErroredTopicNumToKeep} > 0:
   * 1. For errored topics, {@link #getOfflineJobProgress(String, String, Map)} won't truncate them;
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

  private final boolean batchJobHeartbeatEnabled;

  Optional<DynamicAccessController> accessController;

  private final Optional<AuthorizerService> authorizerService;

  private final ExecutorService systemStoreAclSynchronizationExecutor;

  private final LingeringStoreVersionChecker lingeringStoreVersionChecker;

  // Visible for testing
  public VeniceParentHelixAdmin(VeniceHelixAdmin veniceHelixAdmin, VeniceControllerMultiClusterConfig multiClusterConfigs) {
    this(veniceHelixAdmin, multiClusterConfigs, false, Optional.empty(), Optional.empty());
  }

  public VeniceParentHelixAdmin(VeniceHelixAdmin veniceHelixAdmin, VeniceControllerMultiClusterConfig multiClusterConfigs,
      boolean sslEnabled, Optional<SSLConfig> sslConfig, Optional<AuthorizerService> authorizerService) {
    this(veniceHelixAdmin, multiClusterConfigs, sslEnabled, sslConfig, Optional.empty(), authorizerService, new DefaultLingeringStoreVersionChecker());
  }

  public VeniceParentHelixAdmin(VeniceHelixAdmin veniceHelixAdmin, VeniceControllerMultiClusterConfig multiClusterConfigs,
      boolean sslEnabled, Optional<SSLConfig> sslConfig, Optional<DynamicAccessController> accessController,
      Optional<AuthorizerService> authorizerService, @Nonnull LingeringStoreVersionChecker lingeringStoreVersionChecker) {
    Validate.notNull(lingeringStoreVersionChecker);
    this.veniceHelixAdmin = veniceHelixAdmin;
    this.multiClusterConfigs = multiClusterConfigs;
    this.waitingTimeForConsumptionMs = getMultiClusterConfigs().getParentControllerWaitingTimeForConsumptionMs();
    this.batchJobHeartbeatEnabled = getMultiClusterConfigs().getBatchJobHeartbeatEnabled();
    this.veniceWriterMap = new ConcurrentHashMap<>();
    this.adminTopicMetadataAccessor = new ZkAdminTopicMetadataAccessor(this.getVeniceHelixAdmin().getZkClient(),
        this.getVeniceHelixAdmin().getAdapterSerializer());
    this.adminCommandExecutionTrackers = new HashMap<>();
    this.asyncSetupEnabledMap = new VeniceConcurrentHashMap<>();
    this.accessController = accessController;
    this.authorizerService = authorizerService;
    this.systemStoreAclSynchronizationExecutor =
        authorizerService.map(service -> Executors.newSingleThreadExecutor()).orElse(null);
    if (sslEnabled) {
      try {
        String sslFactoryClassName = getMultiClusterConfigs().getSslFactoryClassName();
        Properties sslProperties = sslConfig.get().getSslProperties();
        sslFactory = Optional.of(SslUtils.getSSLFactory(sslProperties, sslFactoryClassName));
      } catch (Exception e) {
        logger.error("Failed to create SSL engine", e);
        throw new VeniceException(e);
      }
    }
    for (String cluster : getMultiClusterConfigs().getClusters()) {
      VeniceControllerConfig config = getMultiClusterConfigs().getControllerConfig(cluster);
      adminCommandExecutionTrackers.put(cluster,
          new AdminCommandExecutionTracker(config.getClusterName(), getVeniceHelixAdmin().getExecutionIdAccessor(),
              getVeniceHelixAdmin().getControllerClientMap(config.getClusterName())));
      perClusterAdminLocks.put(cluster, new ReentrantLock());
    }
    this.pushStrategyZKAccessor = new MigrationPushStrategyZKAccessor(getVeniceHelixAdmin().getZkClient(),
        getVeniceHelixAdmin().getAdapterSerializer());
    this.maxErroredTopicNumToKeep = getMultiClusterConfigs().getParentControllerMaxErroredTopicNumToKeep();
    this.offlinePushAccessor =
        new ParentHelixOfflinePushAccessor(getVeniceHelixAdmin().getZkClient(), getVeniceHelixAdmin().getAdapterSerializer());
    terminalStateTopicChecker = new TerminalStateTopicCheckerForParentController(this,
        getVeniceHelixAdmin().getStoreConfigRepo(), getMultiClusterConfigs().getTerminalStateTopicCheckerDelayMs());
    topicCheckerExecutor.submit(terminalStateTopicChecker);
    systemStoreAclSynchronizationTask = authorizerService.map(
        service -> new SystemStoreAclSynchronizationTask(service, this,
            getMultiClusterConfigs().getSystemStoreAclSynchronizationDelayMs())).orElse(null);
    if (systemStoreAclSynchronizationTask != null) {
      systemStoreAclSynchronizationExecutor.submit(systemStoreAclSynchronizationTask);
    }
    this.lingeringStoreVersionChecker = lingeringStoreVersionChecker;
    systemStoreLifeCycleHelper = new UserSystemStoreLifeCycleHelper(this, authorizerService, multiClusterConfigs);
  }

  // For testing purpose
  protected void setMaxErroredTopicNumToKeep(int maxErroredTopicNumToKeep) {
    this.maxErroredTopicNumToKeep = maxErroredTopicNumToKeep;
  }

  public void setVeniceWriterForCluster(String clusterName, VeniceWriter writer) {
    veniceWriterMap.putIfAbsent(clusterName, writer);
  }

  @Override
  public synchronized void initStorageCluster(String clusterName) {
    getVeniceHelixAdmin().initStorageCluster(clusterName);
    asyncSetupEnabledMap.put(clusterName, true);
    // We might not be able to call a lot of functions of veniceHelixAdmin since
    // current controller might not be the leader controller for the given clusterName
    // Even current controller is leader controller, it will take some time to become 'leader'
    // since VeniceHelixAdmin.start won't wait for state becomes 'Leader', but a lot of
    // VeniceHelixAdmin functions have 'leadership' check.

    // Check whether the admin topic exists or not
    String topicName = AdminTopicUtils.getTopicNameFromClusterName(clusterName);
    TopicManager topicManager = getTopicManager();
    if (topicManager.containsTopicAndAllPartitionsAreOnline(topicName)) {
      logger.info("Admin topic: " + topicName + " for cluster: " + clusterName + " already exists.");
    } else {
      // Create Kafka topic
      topicManager.createTopic(topicName, AdminTopicUtils.PARTITION_NUM_FOR_ADMIN_TOPIC, getMultiClusterConfigs().getKafkaReplicaFactor());
      logger.info("Created admin topic: " + topicName + " for cluster: " + clusterName);
    }

    // Initialize producer
    veniceWriterMap.computeIfAbsent(clusterName, (key) -> {
      /**
       * Venice just needs to check seq id in {@link com.linkedin.venice.controller.kafka.consumer.AdminConsumptionTask} to catch the following scenarios:
       * 1. Data missing;
       * 2. Data out of order;
       * 3. Data duplication;
       */
      return getVeniceWriterFactory().createBasicVeniceWriter(topicName, getTimer());
    });

    if (!getMultiClusterConfigs().getPushJobStatusStoreClusterName().isEmpty()
        && clusterName.equals(getMultiClusterConfigs().getPushJobStatusStoreClusterName())) {
      asyncSetupForInternalRTStore(
          getMultiClusterConfigs().getPushJobStatusStoreClusterName(),
          VeniceSystemStoreUtils.getPushJobDetailsStoreName(),
          PUSH_JOB_DETAILS_STORE_DESCRIPTOR + VeniceSystemStoreUtils.getPushJobDetailsStoreName(),
          PushJobStatusRecordKey.SCHEMA$.toString(), PushJobDetails.SCHEMA$.toString(),
          getMultiClusterConfigs().getControllerConfig(clusterName).getNumberOfPartition());
    }

    maybeSetupBatchJobLivenessHeartbeatStore(clusterName);
  }

  private void maybeSetupBatchJobLivenessHeartbeatStore(String currClusterName) {
    final String batchJobHeartbeatStoreCluster = getMultiClusterConfigs().getBatchJobHeartbeatStoreCluster();
    final String batchJobHeartbeatStoreName = AvroProtocolDefinition.BATCH_JOB_HEARTBEAT.getSystemStoreName();

    if (Objects.equals(currClusterName, batchJobHeartbeatStoreCluster)) {
      asyncSetupForInternalRTStore(
              currClusterName,
              batchJobHeartbeatStoreName,
              BATCH_JOB_HEARTBEAT_STORE_DESCRIPTOR + batchJobHeartbeatStoreName,
              BatchJobHeartbeatKey.SCHEMA$.toString(),
              BatchJobHeartbeatValue.SCHEMA$.toString(),
              getMultiClusterConfigs().getControllerConfig(currClusterName).getNumberOfPartition());
    } else {
      logger.info(String.format("Skip creating the batch job liveness heartbeat store %s in cluster %s since the expected" +
              " store cluster is %s", batchJobHeartbeatStoreName, currClusterName, batchJobHeartbeatStoreCluster));
    }
  }

  /**
   * Setup the venice RT store used internally for hosting push job status records or participant messages.
   * If the store already exists and is in the correct state then only verification is performed.
   * TODO replace this with {@link com.linkedin.venice.controller.init.ClusterLeaderInitializationRoutine}
   */
  private void asyncSetupForInternalRTStore(String clusterName, String storeName, String storeDescriptor,
      String keySchema, String valueSchema, int partitionCount) {

    asyncSetupExecutor.submit(() -> {
      int retryCount = 0;
      boolean isStoreReady = false;
      while (!isStoreReady && asyncSetupEnabledMap.get(clusterName) && retryCount < MAX_ASYNC_SETUP_RETRY_COUNT) {
        try {
          if (retryCount > 0) {
            timer.sleep(SLEEP_INTERVAL_FOR_ASYNC_SETUP_MS);
          }
          isStoreReady = createOrVerifyInternalStore(clusterName, storeName, storeDescriptor, keySchema, valueSchema,
              partitionCount);
        } catch (VeniceException e) {
          // Verification attempts (i.e. a controller running this routine but is not the leader of the cluster) do not
          // count towards the retry count.
          logger.info("VeniceException occurred during " + storeDescriptor + " setup with store " + storeName
              + " in cluster " + clusterName, e);
          logger.info("Async setup for " + storeDescriptor + " attempts: " + retryCount + "/" + MAX_ASYNC_SETUP_RETRY_COUNT);
        } catch (Exception e) {
          logger.warn(
              "Exception occurred aborting " + storeDescriptor + " setup with store " + storeName + " in cluster " + clusterName, e);
          break;
        } finally {
          retryCount++;
        }
      }
      if (isStoreReady) {
        logger.info(storeDescriptor + " has been successfully created or it already exists in cluster " + clusterName);
      } else {
        logger.error("Unable to create or verify the " + storeDescriptor +  " in cluster " + clusterName);
      }
    });
  }

  /**
   * Verify the state of the system store. The leader controller will also create and configure the store if the
   * desired state is not met.
   * @param clusterName the name of the cluster that push status store belongs to.
   * @param storeName the name of the push status store.
   * @return {@code true} if the store is ready, {@code false} otherwise.
   */
  private boolean createOrVerifyInternalStore(String clusterName, String storeName, String storeDescriptor,
                                              String keySchema, String valueSchema, int partitionCount) {
    boolean storeReady = false;
    if (isLeaderControllerFor(clusterName)) {
      // We should only perform the store validation if the current controller is the leader controller of the requested cluster.
      Store store = getStore(clusterName, storeName);
      if (store == null) {
        createStore(clusterName, storeName, VENICE_INTERNAL_STORE_OWNER, keySchema, valueSchema, true);
        store = getStore(clusterName, storeName);
        if (store == null) {
          throw new VeniceException("Unable to create or fetch the " + storeDescriptor);
        }
      } else {
        logger.info("Internal store " + storeName + " already exists in cluster " + clusterName);
      }

      if (!store.isHybrid()) {
        UpdateStoreQueryParams updateStoreQueryParams;
        updateStoreQueryParams = new UpdateStoreQueryParams();
        updateStoreQueryParams.setHybridOffsetLagThreshold(100L);
        updateStoreQueryParams.setHybridRewindSeconds(TimeUnit.DAYS.toSeconds(7));
        updateStoreQueryParams.setHybridDataReplicationPolicy(DataReplicationPolicy.AGGREGATE);
        updateStore(clusterName, storeName, updateStoreQueryParams);
        store = getStore(clusterName, storeName);
        if (!store.isHybrid()) {
          throw new VeniceException("Unable to update the " + storeDescriptor + " to a hybrid store");
        }
        logger.info("Enabled hybrid for internal store " + storeName + " in cluster " + clusterName);
      }

      if (store.getVersions().isEmpty()) {
        int replicationFactor = getReplicationFactor(clusterName, storeName);
        Version version =
            incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), partitionCount,
                replicationFactor);
        writeEndOfPush(clusterName, storeName, version.getNumber(), true);
        store = getStore(clusterName, storeName);
        if (store.getVersions().isEmpty()) {
          throw new VeniceException("Unable to initialize a version for the " + storeDescriptor);
        }
        logger.info("Created a version for internal store " + storeName + " in cluster " + clusterName);
      }

      final String existingRtTopic = getRealTimeTopic(clusterName, storeName);
      if (!existingRtTopic.equals(Version.composeRealTimeTopic(storeName))) {
        throw new VeniceException("Unexpected real time topic name for the " + storeDescriptor);
      }
      storeReady = true;
    } else {
      // Verify that the store is indeed created by another controller. This is to prevent if the initial leader fails
      // or when the cluster happens to be leaderless for a bit.
      try (ControllerClient controllerClient =
           ControllerClient.constructClusterControllerClient(clusterName, getLeaderController(clusterName).getUrl(false), sslFactory)) {
        StoreResponse storeResponse = controllerClient.getStore(storeName);
        if (storeResponse.isError()) {
          logger.warn("Failed to verify if " + storeDescriptor + " exists from the controller with URL: " +
              controllerClient.getControllerDiscoveryUrls());
          return false;
        }
        StoreInfo storeInfo = storeResponse.getStore();

        if (storeInfo.getHybridStoreConfig() != null
            && !storeInfo.getVersions().isEmpty()
            && storeInfo.getVersions().get(storeInfo.getLargestUsedVersionNumber()).getPartitionCount() == partitionCount
            && getTopicManager().containsTopicAndAllPartitionsAreOnline(Version.composeRealTimeTopic(storeName))) {
          storeReady = true;
        }
      }
    }
    return storeReady;
  }

  @Override
  public boolean isClusterValid(String clusterName) {
    return getVeniceHelixAdmin().isClusterValid(clusterName);
  }

  @Override
  public boolean isBatchJobHeartbeatEnabled() {
    return batchJobHeartbeatEnabled;
  }

  private void sendAdminMessageAndWaitForConsumed(String clusterName, String storeName, AdminOperation message) {
    if (!veniceWriterMap.containsKey(clusterName)) {
      throw new VeniceException("Cluster: " + clusterName + " is not started yet!");
    }
    if (!executionIdValidatedClusters.contains(clusterName)) {
      ExecutionIdAccessor executionIdAccessor = getVeniceHelixAdmin().getExecutionIdAccessor();
      long lastGeneratedExecutionId = executionIdAccessor.getLastGeneratedExecutionId(clusterName);
      long lastConsumedExecutionId =
          AdminTopicMetadataAccessor.getExecutionId(adminTopicMetadataAccessor.getMetadata(clusterName));
      if (lastGeneratedExecutionId < lastConsumedExecutionId) {
        // Invalid state, resetting the last generated execution id to last consumed execution id.
        logger.warn("Invalid executionId state detected, last generated execution id: " + lastGeneratedExecutionId
            + ", last consumed execution id: " + lastConsumedExecutionId
            + ". Resetting last generated execution id to: " + lastConsumedExecutionId);
        executionIdAccessor.updateLastGeneratedExecutionId(clusterName, lastConsumedExecutionId);
      }
      executionIdValidatedClusters.add(clusterName);
    }
    try (AutoCloseableLock ignore = veniceHelixAdmin.getHelixVeniceClusterResources(clusterName)
        .getClusterLockManager().createClusterReadLock()) {
      // Obtain the cluster level read lock so during a graceful shutdown or leadership handover there will be no
      // execution id gap (execution id is generated but the message is not sent).
      AdminCommandExecutionTracker adminCommandExecutionTracker = adminCommandExecutionTrackers.get(clusterName);
      AdminCommandExecution execution = adminCommandExecutionTracker.createExecution(AdminMessageType.valueOf(message).name());
      message.executionId = execution.getExecutionId();
      VeniceWriter<byte[], byte[], byte[]> veniceWriter = veniceWriterMap.get(clusterName);
      byte[] serializedValue = adminOperationSerializer.serialize(message);
      try {
        Future<RecordMetadata> future = veniceWriter.put(emptyKeyByteArr, serializedValue, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
        RecordMetadata meta = future.get();

        logger.info("Sent message: " + message + " to kafka, offset: " + meta.offset());
        waitingMessageToBeConsumed(clusterName, storeName, message.executionId);
        adminCommandExecutionTracker.startTrackingExecution(execution);
      } catch (Exception e) {
        throw new VeniceException("Got exception during sending message to Kafka -- " + e.getMessage(), e);
      }
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
        Exception lastException = (null == storeName) ? null : getVeniceHelixAdmin().getLastExceptionForStore(clusterName, storeName);
        String errMsg = "Timed out after waiting for " + waitingTimeForConsumptionMs + "ms for admin consumption to catch up.";
        errMsg += " Consumed execution id: " + consumedExecutionId + ", waiting to be consumed id: " + executionId;
        errMsg += (null == lastException) ? "" : " Last exception: " + lastException.getMessage();
        throw new VeniceException(errMsg, lastException);
      }

      logger.info("Waiting execution id: " + executionId + " to be consumed, currently at " + consumedExecutionId);
      Utils.sleep(SLEEP_INTERVAL_FOR_DATA_CONSUMPTION_IN_MS);
    }
    logger.info("The message has been consumed, execution id: " + executionId);
  }

  /**
   * Acquire the cluster level lock used to ensure admin messages in the admin topic (per cluster) have the correct order.
   * This lock is needed only when generating and writing admin messages.
   */
  protected void acquireAdminMessageLock(String clusterName, String storeName) {
    try {
      if (storeName != null) {
        // First check whether an exception already exist in the admin channel for the given store
        Exception lastException = getVeniceHelixAdmin().getLastExceptionForStore(clusterName, storeName);
        if (lastException != null) {
          throw new VeniceException(
              "Unable to start new admin operations for store: " + storeName + " in cluster: " + clusterName + " due to existing exception: " + lastException.getMessage(), lastException);
        }
      }
      boolean acquired = perClusterAdminLocks.get(clusterName).tryLock(waitingTimeForConsumptionMs, TimeUnit.MILLISECONDS);
      if (!acquired) {
        throw new VeniceException("Failed to acquire lock after waiting for " + waitingTimeForConsumptionMs
            + "ms. Another ongoing admin operation might be holding up the lock");
      }
    } catch (InterruptedException e) {
      throw new VeniceException("Got interrupted during acquiring lock", e);
    }
  }

  protected void releaseAdminMessageLock(String clusterName) {
    perClusterAdminLocks.get(clusterName).unlock();
  }

  @Override
  public void createStore(String clusterName, String storeName, String owner, String keySchema, String valueSchema,
                          boolean isSystemStore, Optional<String> accessPermissions) {
    acquireAdminMessageLock(clusterName, storeName);
    try {
      getVeniceHelixAdmin().checkPreConditionForCreateStore(clusterName, storeName, keySchema, valueSchema, isSystemStore, false);
      logger.info("Adding store: " + storeName + " to cluster: " + clusterName);

      // Provisioning ACL needs to be the first step in store creation process.
      provisionAclsForStore(storeName, accessPermissions, Collections.emptyList());
      sendStoreCreationAdminMessage(clusterName, storeName, owner, keySchema, valueSchema);
      // For each user store level system store, we will send admin message to validate the creation is successful.
      for (VeniceSystemStoreType systemStoreType : getSystemStoreLifeCycleHelper().maybeMaterializeSystemStoresForUserStore(clusterName, storeName)) {
        sendUserSystemStoreCreationValidationAdminMessage(clusterName, storeName, systemStoreType);
      }
      if (VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE.getPrefix().equals(storeName)) {
        setupResourceForBatchJobHeartbeatStore(storeName);
      }

    } finally {
      releaseAdminMessageLock(clusterName);
    }
  }

  private void sendStoreCreationAdminMessage(String clusterName, String storeName, String owner, String keySchema, String valueSchema) {
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

  private void sendUserSystemStoreCreationValidationAdminMessage(String clusterName, String storeName, VeniceSystemStoreType systemStoreType) {
    final AdminOperation message = new AdminOperation();
    switch (systemStoreType) {
      case META_STORE:
        MetaSystemStoreAutoCreationValidation metaSystemStoreAutoCreationValidation =
            (MetaSystemStoreAutoCreationValidation) AdminMessageType.META_SYSTEM_STORE_AUTO_CREATION_VALIDATION.getNewInstance();
        metaSystemStoreAutoCreationValidation.clusterName = clusterName;
        metaSystemStoreAutoCreationValidation.storeName = storeName;
        message.operationType = AdminMessageType.META_SYSTEM_STORE_AUTO_CREATION_VALIDATION.getValue();
        message.payloadUnion = metaSystemStoreAutoCreationValidation;
        break;
      case DAVINCI_PUSH_STATUS_STORE:
        PushStatusSystemStoreAutoCreationValidation pushStatusSystemStoreAutoCreationValidation =
            (PushStatusSystemStoreAutoCreationValidation) AdminMessageType.PUSH_STATUS_SYSTEM_STORE_AUTO_CREATION_VALIDATION.getNewInstance();
        pushStatusSystemStoreAutoCreationValidation.clusterName = clusterName;
        pushStatusSystemStoreAutoCreationValidation.storeName = storeName;
        message.operationType = AdminMessageType.PUSH_STATUS_SYSTEM_STORE_AUTO_CREATION_VALIDATION.getValue();
        message.payloadUnion = pushStatusSystemStoreAutoCreationValidation;
        break;
      default:
        logger.warn("System store type: " + systemStoreType + " is not a user store level system store, will not send store creation validation message.");
        return;
    }
    logger.info("Sending system store creation validation message for user store:" + storeName + ", system store type: " + systemStoreType);
    sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);
  }

  private void setupResourceForBatchJobHeartbeatStore(String batchJobHeartbeatStoreName) {
    if (authorizerService.isPresent()) {
      authorizerService.get().setupResource(new Resource(batchJobHeartbeatStoreName));
      logger.info("Set up wildcard ACL regex for " + batchJobHeartbeatStoreName);
    } else {
      logger.warn("Skip setting up wildcard ACL regex for " + batchJobHeartbeatStoreName + " since the authorizer service is not provided");
    }
  }

  @Override
  public void deleteStore(String clusterName, String storeName, int largestUsedVersionNumber, boolean waitOnRTTopicDeletion) {
    acquireAdminMessageLock(clusterName, storeName);
    try {
      logger.info("Deleting store: " + storeName + " from cluster: " + clusterName);
      Store store = null;
      try {
        store = getVeniceHelixAdmin().checkPreConditionForDeletion(clusterName, storeName);
      } catch (VeniceNoStoreException e) {
        // It's possible for a store to partially exist due to partial delete/creation failures.
        logger.warn("Store object is missing for store: " + storeName
            + " will proceed with the rest of store deletion");
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

      //Deleting ACL needs to be the last step in store deletion process.
      if (store != null) {
        if (!store.isMigrating()) {
          cleanUpAclsForStore(storeName, VeniceSystemStoreType.getEnabledSystemStoreTypes(store));
        } else {
          logger.info("Store " + storeName + " is migrating! Skipping acl deletion!");
        }
      } else {
        logger.warn("Store object for " + storeName + " is missing! Skipping acl deletion!");
      }
    } finally {
      releaseAdminMessageLock(clusterName);
    }
  }

  @Override
  public void addVersionAndStartIngestion(String clusterName, String storeName, String pushJobId, int versionNumber,
      int numberOfPartitions, Version.PushType pushType, String remoteKafkaBootstrapServers,
      long rewindTimeInSecondsOverride, int replicationMetadataVersionId, boolean versionSwapDeferred) {
    // Parent controller will always pick the replicationMetadataVersionId from configs.
    replicationMetadataVersionId = getMultiClusterConfigs().getCommonConfig().getReplicationMetadataVersionId();
    Version version = getVeniceHelixAdmin().addVersionOnly(clusterName, storeName, pushJobId, versionNumber, numberOfPartitions, pushType,
        remoteKafkaBootstrapServers, rewindTimeInSecondsOverride, replicationMetadataVersionId);
    if (version.isActiveActiveReplicationEnabled()) {
      updateReplicationMetadataSchemaForAllValueSchema(clusterName, storeName);
    }
    acquireAdminMessageLock(clusterName, storeName);
    try {
      sendAddVersionAdminMessage(clusterName, storeName, pushJobId, version, numberOfPartitions, pushType);
    } finally {
      releaseAdminMessageLock(clusterName);
    }
  }

  /**
   * Since there is no offline push running in Parent Controller,
   * the old store versions won't be cleaned up by job completion action, so Parent Controller chooses
   * to clean it up when the new store version gets created.
   * It is OK to clean up the old store versions in Parent Controller without notifying Child Controller since
   * store version in Parent Controller doesn't maintain actual version status, and only for tracking
   * the store version creation history.
   */
  protected void cleanupHistoricalVersions(String clusterName, String storeName) {
    HelixVeniceClusterResources resources = getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
      ReadWriteStoreRepository storeRepo = resources.getStoreMetadataRepository();
      Store store = storeRepo.getStore(storeName);
      if (null == store) {
        logger.info("The store to clean up: " + storeName + " doesn't exist");
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
  protected List<String> existingTopicsForStore(String storeName) {
    List<String> outputList = new ArrayList<>();
    TopicManager topicManager = getTopicManager();
    Set<String> topics = topicManager.listTopics();
    String storeNameForCurrentTopic;
    for (String topic: topics) {
      if (AdminTopicUtils.isAdminTopic(topic) || AdminTopicUtils.isKafkaInternalTopic(topic) || Version.isRealTimeTopic(topic)) {
        continue;
      }
      try {
        storeNameForCurrentTopic = Version.parseStoreFromKafkaTopicName(topic);
      } catch (Exception e) {
        if (logger.isDebugEnabled()) {
          logger.debug("Failed to parse StoreName from topic: " + topic + ", and error message: " + e.getMessage());
        }
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
  protected List<String> getKafkaTopicsByAge(String storeName) {
    List<String> existingTopics = existingTopicsForStore(storeName);
    if (!existingTopics.isEmpty()) {
      existingTopics.sort((t1, t2) -> {
        int v1 = Version.parseVersionFromKafkaTopicName(t1);
        int v2 = Version.parseVersionFromKafkaTopicName(t2);
        return v2 - v1;
      });
    }
    return existingTopics;
  }

  /**
   * If there is no ongoing push for specified store currently, this function will return {@link Optional#empty()},
   * else will return the ongoing Kafka topic. It will also try to clean up legacy topics.
   */
  protected Optional<String> getTopicForCurrentPushJob(String clusterName, String storeName, boolean isIncrementalPush, boolean isRepush) {
    // The first/last topic in the list is the latest/oldest version topic
    List<String> versionTopics = getKafkaTopicsByAge(storeName);
    Optional<String> latestKafkaTopic = Optional.empty();
    if (!versionTopics.isEmpty()) {
      latestKafkaTopic = Optional.of(versionTopics.get(0));
    }

    /**
     * Check current topic retention to decide whether the previous job is already done or not
     */
    if (latestKafkaTopic.isPresent()) {
      logger.debug("Latest kafka topic for store: " + storeName + " is " + latestKafkaTopic.get());

      final String latestKafkaTopicName = latestKafkaTopic.get();
      if (!isTopicTruncated(latestKafkaTopicName)) {
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
        int versionNumber = Version.parseVersionFromKafkaTopicName(latestKafkaTopicName);
        Pair<Store, Version> storeVersionPair = getVeniceHelixAdmin().waitVersion(clusterName, storeName, versionNumber, Duration.ofSeconds(30));
        if (storeVersionPair.getSecond() == null) {
          // TODO: Guard this topic deletion code using a store-level lock instead.
          Long inMemoryTopicCreationTime = getVeniceHelixAdmin().getInMemoryTopicCreationTime(latestKafkaTopicName);
          if (inMemoryTopicCreationTime != null && SystemTime.INSTANCE.getMilliseconds() < (inMemoryTopicCreationTime + TOPIC_DELETION_DELAY_MS)) {
            throw new VeniceException("Failed to get version information but the topic exists and has been created recently. Try again after some time.");
          }

          killOfflinePush(clusterName, latestKafkaTopicName, true);
          logger.info("Found topic: " + latestKafkaTopicName + " without the corresponding version, will kill it");
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
          OfflinePushStatusInfo offlineJobStatus = getOffLinePushStatus(clusterName, latestKafkaTopicName);
          jobStatus = offlineJobStatus.getExecutionStatus();
          extraInfo = offlineJobStatus.getExtraInfo();
          if (!extraInfo.containsValue(ExecutionStatus.UNKNOWN.toString())) {
            break;
          }
          // Retry since there is a connection failure when querying job status against each datacenter
          try {
            timer.sleep(SLEEP_MS_BETWEEN_RETRY);
          } catch (InterruptedException e) {
            throw new VeniceException("Received InterruptedException during sleep between 'getOffLinePushStatus' calls");
          }
        }
        if (extraInfo.containsValue(ExecutionStatus.UNKNOWN.toString())) {
          // TODO: Do we need to throw exception here??
          logger.error("Failed to get job status for topic: " + latestKafkaTopicName + " after retrying " + retryTimes
              + " times, extra info: " + extraInfo);
        }
        if (!jobStatus.isTerminal()) {
          logger.info(
              "Job status: " + jobStatus + " for Kafka topic: " + latestKafkaTopicName + " is not terminal, extra info: " + extraInfo);
          return latestKafkaTopic;
        } else {
          /**
           * If the job status of latestKafkaTopic is terminal and it is not an incremental push,
           * it will be truncated in {@link #getOffLinePushStatus(String, String)}.
           */
          if (!isIncrementalPush) {
            Map<String, Integer> currentVersionsMap = getCurrentVersionsForMultiColos(clusterName, storeName);
            truncateTopicsBasedOnMaxErroredTopicNumToKeep(versionTopics, isRepush, currentVersionsMap);
          }
        }
      }
    }
    return Optional.empty();
  }

  /**
   * Only keep {@link #maxErroredTopicNumToKeep} non-truncated topics ordered by version
   * N.B. This method was originally introduced to debug KMM issues. But now it works
   * as a general method for cleaning up leaking topics. ({@link #maxErroredTopicNumToKeep}
   * is always 0.)
   *
   * TODO: rename the method once we remove the rest of KMM debugging logic.
   */
  protected void truncateTopicsBasedOnMaxErroredTopicNumToKeep(List<String> topics, boolean isRepush, Map<String, Integer> currentVersionsMap) {
    // Based on current logic, only 'errored' topics were not truncated.
    List<String> sortedNonTruncatedTopics = topics.stream().filter(topic -> !isTopicTruncated(topic)).sorted((t1, t2) -> {
      int v1 = Version.parseVersionFromKafkaTopicName(t1);
      int v2 = Version.parseVersionFromKafkaTopicName(t2);
      return v1 - v2;
    }).collect(Collectors.toList());
    Set<String> streamReprocessingTopics = sortedNonTruncatedTopics.stream().filter(Version::isStreamReprocessingTopic)
        .collect(Collectors.toSet());
    List<String> sortedNonTruncatedVersionTopics = sortedNonTruncatedTopics.stream().filter(topic ->
        !Version.isStreamReprocessingTopic(topic)).collect(Collectors.toList());
    if (sortedNonTruncatedVersionTopics.size() <= maxErroredTopicNumToKeep) {
      logger.info("Non-truncated version topics size: " + sortedNonTruncatedVersionTopics.size() +
          " isn't bigger than maxErroredTopicNumToKeep: " + maxErroredTopicNumToKeep + ", so no topic will be truncated this time");
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
        logger.info("Do not delete topic " + topic + " since the incoming batch push is a Venice internal repush and"
            + "" + topic + " is a current version topic.");
        continue;
      }
      if (++truncatedTopicCnt > topicNumToTruncate) {
        break;
      }
      truncateKafkaTopic(topic);
      logger.info("Errored topic: " + topic + " got truncated");
      String correspondingStreamReprocessingTopic = Version.composeStreamReprocessingTopicFromVersionTopic(topic);
      if (streamReprocessingTopics.contains(correspondingStreamReprocessingTopic)) {
        truncateKafkaTopic(correspondingStreamReprocessingTopic);
        logger.info("Corresponding stream reprocessing topic: " + correspondingStreamReprocessingTopic + " also got truncated.");
      }
    }
  }

  @Override
  public boolean hasWritePermissionToBatchJobHeartbeatStore(
      X509Certificate requesterCert,
      String batchJobHeartbeatStoreName,
      IdentityParser identityParser
  ) throws AclException {
    if (!accessController.isPresent()) {
      throw new VeniceException(String.format("Cannot check write permission on store %s since the access controller "
          + "does not present for cert %s", batchJobHeartbeatStoreName, requesterCert));
    }
    final String accessMethodName = Method.Write.name();
    // Currently write access on a Venice store needs to be checked using this hasAccessToTopic method
    final boolean hasAccess = accessController.get().hasAccessToTopic(requesterCert, batchJobHeartbeatStoreName, accessMethodName);
    StringBuilder sb = new StringBuilder();
    sb.append("Requester");
    sb.append(hasAccess ? " has " : " does not have ");
    sb.append(accessMethodName + " access on " + batchJobHeartbeatStoreName);
    sb.append(" with identity: ");
    sb.append(identityParser.parseIdentityFromCert(requesterCert));
    logger.info(sb.toString());
    return hasAccess;
  }

  @Override
  public boolean isActiveActiveReplicationEnabledInAllRegion(String clusterName, String storeName, boolean checkCurrentVersion) {
    Map<String, ControllerClient> controllerClients = getVeniceHelixAdmin().getControllerClientMap(clusterName);
    Store store = getVeniceHelixAdmin().getStore(clusterName, storeName);

    if (!store.isActiveActiveReplicationEnabled()) {
      logger.info("isActiveActiveReplicationEnabledInAllRegion: " + storeName + " store is not enabled for Active/Active in parent region");
      return false;
    }

    for (Map.Entry<String, ControllerClient> entry: controllerClients.entrySet()) {
      String region = entry.getKey();
      ControllerClient controllerClient = entry.getValue();
      StoreResponse response = controllerClient.retryableRequest(10, c -> c.getStore(storeName));
      if (response.isError()) {
        logger.warn("isActiveActiveReplicationEnabledInAllRegion: Could not query store from region: " + region + " for cluster: " + clusterName + ". " + response.getError()
            + ". Default child AA config to true, since AA is already enabled in parent.");
      } else {
        if (!response.getStore().isActiveActiveReplicationEnabled()) {
          if (store.isActiveActiveReplicationEnabled()) {
            throw new VeniceException(String.format("Store %s doesn't have Active/Active enabled in region %s, but A/A is "
                + "enabled in parent which indicates A/A is fully ramped", storeName, region));
          }
          logger.info("isActiveActiveReplicationEnabledInAllRegion:" + storeName + " store is not enabled for Active/Active in region: " + region);
          return false;
        }

        /**
         * check version level config as well. In case there is no version it should be fine to return true.
         */
        if (checkCurrentVersion) {
          int currentVersion = response.getStore().getCurrentVersion();
          for (Version version : response.getStore().getVersions()) {
            if (currentVersion == version.getNumber()) {
              if (!version.isActiveActiveReplicationEnabled()) {
                logger.info(
                    "isActiveActiveReplicationEnabledInAllRegion:" + storeName + " current version: " + version.getNumber() + " is not enabled for Active/Active in region: " + region);
                return false;
              }
            }
          }
        }
      }
    }
    return true;
  }

  @Override
  public Version incrementVersionIdempotent(String clusterName, String storeName, String pushJobId,
      int numberOfPartitions, int replicationFactor, Version.PushType pushType, boolean sendStartOfPush,
      boolean sorted, String compressionDictionary, Optional<String> sourceGridFabric,
      Optional<X509Certificate> requesterCert, long rewindTimeInSecondsOverride, Optional<String> emergencySourceRegion, boolean versionSwapDeferred) {
    Optional<String> currentPushTopic = getTopicForCurrentPushJob(clusterName, storeName, pushType.isIncremental(), Version.isPushIdRePush(pushJobId));
    if (currentPushTopic.isPresent()) {
      int currentPushVersion = Version.parseVersionFromKafkaTopicName(currentPushTopic.get());
      Store store = getStore(clusterName, storeName);
      Optional<Version> version = store.getVersion(currentPushVersion);
      if (!version.isPresent()) {
        throw new VeniceException("A corresponding version should exist with the ongoing push with topic "
            + currentPushTopic);
      }
      String existingPushJobId = version.get().getPushJobId();
      if (existingPushJobId.equals(pushJobId)) {
         return version.get();
      }

      boolean isExistingPushJobARepush = Version.isPushIdRePush(existingPushJobId);
      boolean isIncomingPushJobARepush = Version.isPushIdRePush(pushJobId);

      if (getLingeringStoreVersionChecker().isStoreVersionLingering(
          store, version.get(), timer, this, requesterCert, new IdentityParserImpl())) {
        if (pushType.isIncremental()) {
          /**
           * Incremental push shouldn't kill the previous full push, there could be a transient issue that parents couldn't
           * get the right job states from child colos; once child colos recover, next incremental push should succeed.
           *
           * If the previous full push is indeed lingering, users should issue to full push to clean up the lingering job
           * instead of running incremental push.
           */
          throw new VeniceException("Version " + version.get().getNumber() + " is not healthy in Venice backend; please "
              + "consider running a full batch push for your store: " + storeName + " before running incremental push, "
              + "or reach out to Venice team.");
        } else {
          // Kill the lingering version and allow the new push to start.
          logger.info("Found lingering topic: " + currentPushTopic.get() + " with push id: " + existingPushJobId + ". Killing the lingering version that was created at: " + version.get().getCreatedTime());
          killOfflinePush(clusterName, currentPushTopic.get(), true);
        }
      } else if (isExistingPushJobARepush && !pushType.isIncremental() && !isIncomingPushJobARepush) {
        // Repush jobs for inc push enabled versions with inc push policy PUSH_TO_VERSION_TOPIC are blocked by VPJ.
        // Inc push policy INCREMENTAL_PUSH_SAME_AS_REAL_TIME with target version filtering is deprecated and not going
        // to be used.

        // Kill the existing job if incoming push type is not an inc push and also not a repush job.
        logger.info("Found running repush job with push id: " + existingPushJobId + " and incoming push is a batch "
            + "job or stream reprocessing job with push id: " + pushJobId + ". Killing the repush job for store " + storeName);
        killOfflinePush(clusterName, currentPushTopic.get(), true);
      } else if (isExistingPushJobARepush && pushType.isIncremental()
          && INCREMENTAL_PUSH_SAME_AS_REAL_TIME.equals(version.get().getIncrementalPushPolicy())) {
        // No op. Allow push to continue if existing push is a repush, incoming push is an inc push with inc push policy
        // as INCREMENTAL_PUSH_SAME_AS_REAL_TIME.
        logger.info("Found running repush job with push id: " + existingPushJobId + " and incoming push is an "
            + "incremental push with push id: " + pushJobId + " with incremental push policy: " +
            INCREMENTAL_PUSH_SAME_AS_REAL_TIME + ". Letting the push continue for store " + storeName);
      } else {
        throw new VeniceException("Unable to start the push with pushJobId " + pushJobId + " for store " + storeName
            + ". An ongoing push with pushJobId " + existingPushJobId + " and topic " + currentPushTopic
            + " is found and it must be terminated before another push can be started.");
      }
    }

    Version newVersion;
    if (pushType.isIncremental()) {
      newVersion = getVeniceHelixAdmin().getIncrementalPushVersion(clusterName, storeName);
    } else {
      newVersion = addVersionAndTopicOnly(clusterName, storeName, pushJobId, VERSION_ID_UNSET, numberOfPartitions,
          replicationFactor, pushType, sendStartOfPush, sorted, compressionDictionary, sourceGridFabric, rewindTimeInSecondsOverride, emergencySourceRegion, versionSwapDeferred);
    }
    cleanupHistoricalVersions(clusterName, storeName);
    if (VeniceSystemStoreType.getSystemStoreType(storeName) == null) {
      if (pushType.isBatch()) {
        getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName).getVeniceAdminStats()
            .recordSuccessfullyStartedUserBatchPushParentAdminCount();
      }
      else if (pushType.isIncremental()) {
        getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName).getVeniceAdminStats()
            .recordSuccessfullyStartedUserIncrementalPushParentAdminCount();
      }
    }

    return newVersion;
  }

  public Version addVersionAndTopicOnly(String clusterName, String storeName, String pushJobId, int versionNumber,
      int numberOfPartitions, int replicationFactor, Version.PushType pushType, boolean sendStartOfPush, boolean sorted,
      String compressionDictionary, Optional<String> sourceGridFabric, long rewindTimeInSecondsOverride, Optional<String> emergencySourceRegion,
      boolean versionSwapDeferred) {
    int replicationMetadataVersionId = getMultiClusterConfigs().getCommonConfig().getReplicationMetadataVersionId();
    Pair<Boolean, Version> result = getVeniceHelixAdmin().addVersionAndTopicOnly(clusterName, storeName, pushJobId,
        versionNumber, numberOfPartitions, replicationFactor, sendStartOfPush, sorted, pushType, compressionDictionary,
        null, sourceGridFabric, rewindTimeInSecondsOverride, replicationMetadataVersionId, emergencySourceRegion, versionSwapDeferred);
    Version newVersion = result.getSecond();
    if (result.getFirst()) {
      if (newVersion.isActiveActiveReplicationEnabled()) {
        updateReplicationMetadataSchemaForAllValueSchema(clusterName, storeName);
      }
      // Send admin message if the version is newly created.
      acquireAdminMessageLock(clusterName, storeName);
      try {
        sendAddVersionAdminMessage(clusterName, storeName, pushJobId, newVersion, numberOfPartitions, pushType);
      } finally {
        releaseAdminMessageLock(clusterName);
      }
      getSystemStoreLifeCycleHelper().maybeCreateSystemStoreWildcardAcl(storeName);
    }
    return newVersion;
  }

  protected void sendAddVersionAdminMessage(String clusterName, String storeName, String pushJobId, Version version,
      int numberOfPartitions, Version.PushType pushType) {
    AdminOperation message = new AdminOperation();
    message.operationType = AdminMessageType.ADD_VERSION.getValue();
    message.payloadUnion = getAddVersionMessage(clusterName, storeName, pushJobId, version, numberOfPartitions, pushType);
    sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);
  }

  private AddVersion getAddVersionMessage(String clusterName, String storeName, String pushJobId, Version version,
      int numberOfPartitions, Version.PushType pushType) {
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
    addVersion.timestampMetadataVersionId = version.getReplicationMetadataVersionId();
    addVersion.versionSwapDeferred = version.isVersionSwapDeferred();
    return addVersion;
  }

  @Override
  public String getRealTimeTopic(String clusterName, String storeName){
    return getVeniceHelixAdmin().getRealTimeTopic(clusterName, storeName);
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
    ExecutionStatus status = getOffLinePushStatus(clusterName, incrementalPushTopic, Optional.empty()).getExecutionStatus();

    return getIncrementalPushVersion(incrementalPushVersion, status);
  }

  //This method is only for internal / test use case
  Version getIncrementalPushVersion(Version incrementalPushVersion, ExecutionStatus status) {
    String storeName = incrementalPushVersion.getStoreName();

    String incrementalPushTopic;
    if (incrementalPushVersion.getIncrementalPushPolicy().equals(IncrementalPushPolicy.INCREMENTAL_PUSH_SAME_AS_REAL_TIME)) {
      incrementalPushTopic = Version.composeRealTimeTopic(storeName);
    } else {
      incrementalPushTopic = incrementalPushVersion.kafkaTopicName();
    }

    if (!status.isTerminal()) {
      throw new VeniceException("Cannot start incremental push since batch push is on going." + " store: " + storeName);
    }

    if(status == ExecutionStatus.ERROR || getVeniceHelixAdmin().isTopicTruncated(incrementalPushTopic)) {
      throw new VeniceException("Cannot start incremental push since previous batch push has failed. Please run another bash job."
          + " store: " + storeName);
    }

    return incrementalPushVersion;
  }

  @Override
  public int getCurrentVersion(String clusterName, String storeName) {
    throw new VeniceUnsupportedOperationException("getCurrentVersion", "Please use getCurrentVersionsForMultiColos in Parent controller.");
  }

  /**
   * Query the current version for the given store. In parent colo, Venice do not update the current version because
   * there is not offline push monitor. So parent controller will query each prod controller and return the map.
   */
  @Override
  public Map<String, Integer> getCurrentVersionsForMultiColos(String clusterName, String storeName) {
    Map<String, ControllerClient> controllerClients = getVeniceHelixAdmin().getControllerClientMap(clusterName);
    return getCurrentVersionForMultiColos(clusterName, storeName, controllerClients);
  }

  @Override
  public RepushInfo getRepushInfo(String clusterName, String storeName, Optional<String> fabricName) {
    Map<String, ControllerClient> controllerClients = getVeniceHelixAdmin().getControllerClientMap(clusterName);

    if (fabricName.isPresent()) {
      StoreResponse response = controllerClients.get(fabricName.get()).getStore(storeName);
      if (response.isError()) {
        throw new VeniceException("Could not query store from colo: " + fabricName.get() + " for cluster: " + clusterName + ". " + response.getError());
      }
      return RepushInfo.createRepushInfo(response.getStore().getVersion(response.getStore().getCurrentVersion()).get(),
          response.getStore().getKafkaBrokerUrl());
    }
    // fabricName not present, get the largest version info among the child colos.
    Map<String, Integer> currentVersionsMap = getCurrentVersionForMultiColos(clusterName, storeName, controllerClients);
    int largestVersion = Integer.MIN_VALUE;
    String colo = null;
    for (Map.Entry<String, Integer> mapEntry : currentVersionsMap.entrySet()) {
      if (mapEntry.getValue() > largestVersion) {
        largestVersion = mapEntry.getValue();
        colo = mapEntry.getKey();
      }
    }
    StoreResponse response = controllerClients.get(colo).getStore(storeName);
    if (response.isError()) {
      throw new VeniceException("Could not query store from largest version colo: " + fabricName.get() + " for cluster: " + clusterName + ". " + response.getError());
    }
    return RepushInfo.createRepushInfo(response.getStore().getVersion((response.getStore().getCurrentVersion())).get(),
        response.getStore().getKafkaBrokerUrl());
  }

  @Override
  public Map<String, String> getFutureVersionsForMultiColos(String clusterName, String storeName) {
    Map<String, ControllerClient> controllerClients = getVeniceHelixAdmin().getControllerClientMap(clusterName);
    Map<String, String> result = new HashMap<>();
    for (Map.Entry<String, ControllerClient> entry: controllerClients.entrySet()) {
      String region = entry.getKey();
      ControllerClient controllerClient = entry.getValue();
      MultiStoreStatusResponse response = controllerClient.getFutureVersions(clusterName, storeName);
      if (response.isError()) {
        logger.error(
            "Could not query store from region: " + region + " for cluster: " + clusterName + ". " + response.getError());
        result.put(region, String.valueOf(AdminConsumptionTask.IGNORED_CURRENT_VERSION));
      } else {
        result.put(region,response.getStoreStatusMap().get(storeName));
      }
    }
    return result;
  }

  @Override
  public int getFutureVersion(String clusterName, String storeName) {
    return Store.NON_EXISTING_VERSION;
  }

  protected Map<String, Integer> getCurrentVersionForMultiColos(String clusterName, String storeName,
      Map<String, ControllerClient> controllerClients) {
    Map<String, Integer> result = new HashMap<>();
    for (Map.Entry<String, ControllerClient> entry: controllerClients.entrySet()) {
      String region = entry.getKey();
      ControllerClient controllerClient = entry.getValue();
      StoreResponse response = controllerClient.getStore(storeName);
      if (response.isError()) {
        logger.error(
            "Could not query store from region: " + region + " for cluster: " + clusterName + ". " + response.getError());
        result.put(region, AdminConsumptionTask.IGNORED_CURRENT_VERSION);
      } else {
        result.put(region,response.getStore().getCurrentVersion());
      }
    }
    return result;
  }

  @Override
  public Version peekNextVersion(String clusterName, String storeName) {
    throw new VeniceUnsupportedOperationException("peekNextVersion");
  }

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
      releaseAdminMessageLock(clusterName);
    }
  }

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
      releaseAdminMessageLock(clusterName);
    }
  }

  @Override
  public List<Version> versionsForStore(String clusterName, String storeName) {
    return getVeniceHelixAdmin().versionsForStore(clusterName, storeName);
  }

  @Override
  public List<Store> getAllStores(String clusterName) {
    return getVeniceHelixAdmin().getAllStores(clusterName);
  }

  @Override
  public Map<String, String> getAllStoreStatuses(String clusterName) {
    throw new VeniceUnsupportedOperationException("getAllStoreStatuses");
  }

  @Override
  public Store getStore(String clusterName, String storeName) {
    return getVeniceHelixAdmin().getStore(clusterName, storeName);
  }

  @Override
  public boolean hasStore(String clusterName, String storeName) {
    return getVeniceHelixAdmin().hasStore(clusterName, storeName);
  }

  @Override
  public void setStoreCurrentVersion(String clusterName,
                                String storeName,
                                int versionNumber) {
    throw new VeniceUnsupportedOperationException("setStoreCurrentVersion", "Please use set-version only on child controllers, "
        + "setting version on parent is not supported, since the version list could be different fabric by fabric");
  }

  @Override
  public void setStoreLargestUsedVersion(String clusterName, String storeName, int versionNumber) {
    throw new VeniceUnsupportedOperationException("setStoreLargestUsedVersion", "This is only supported in the Child Controller.");
  }


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
      releaseAdminMessageLock(clusterName);
    }
  }

  @Override
  public void setStorePartitionCount(String clusterName, String storeName, int partitionCount) {
    acquireAdminMessageLock(clusterName, storeName);
    try {
      getVeniceHelixAdmin().checkPreConditionForUpdateStoreMetadata(clusterName, storeName);

      int maxPartitionNum = getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName).getConfig().getMaxNumberOfPartition();
      if (partitionCount > maxPartitionNum) {
        throw new ConfigurationException("Partition count: "
            + partitionCount + " should be less than max: " + maxPartitionNum);
      }
      if (partitionCount < 0) {
        throw new ConfigurationException("Partition count: "
            + partitionCount + " should NOT be negative");
      }

      SetStorePartitionCount setStorePartition = (SetStorePartitionCount) AdminMessageType.SET_STORE_PARTITION.getNewInstance();
      setStorePartition.clusterName = clusterName;
      setStorePartition.storeName = storeName;
      setStorePartition.partitionNum = partitionCount;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.SET_STORE_PARTITION.getValue();
      message.payloadUnion = setStorePartition;

      sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);
    } finally {
      releaseAdminMessageLock(clusterName);
    }
  }

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
      releaseAdminMessageLock(clusterName);
    }
  }

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
      releaseAdminMessageLock(clusterName);
    }
  }

  @Override
  public void setStoreReadWriteability(String clusterName, String storeName, boolean isAccessible) {
    setStoreReadability(clusterName, storeName, isAccessible);
    setStoreWriteability(clusterName, storeName, isAccessible);
  }

  /**
   * Expectation for this API: Also send out an admin message to admin channel.
   */
  @Override
  public void setLeaderFollowerModelEnabled(String clusterName, String storeName, boolean leaderFollowerModelEnabled) {
    //place holder
    //will add it in the following RB
  }

  /**
   * Only change the configs in parent; do not send it to admin channel.
   */
  @Override
  public void enableLeaderFollowerModelLocally(String clusterName, String storeName,
      boolean leaderFollowerModelEnabled) {
    getVeniceHelixAdmin().setLeaderFollowerModelEnabled(clusterName, storeName, leaderFollowerModelEnabled);
  }

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
      Optional<Boolean> accessControlled = params.getAccessControlled();
      Optional<CompressionStrategy> compressionStrategy = params.getCompressionStrategy();
      Optional<Boolean> clientDecompressionEnabled = params.getClientDecompressionEnabled();
      Optional<Boolean> chunkingEnabled = params.getChunkingEnabled();
      Optional<Integer> batchGetLimit = params.getBatchGetLimit();
      Optional<Integer> numVersionsToPreserve = params.getNumVersionsToPreserve();
      Optional<Boolean> incrementalPushEnabled = params.getIncrementalPushEnabled();
      Optional<Boolean> storeMigration = params.getStoreMigration();
      Optional<Boolean> writeComputationEnabled = params.getWriteComputationEnabled();
      Optional<Boolean> readComputationEnabled = params.getReadComputationEnabled();
      Optional<Integer> bootstrapToOnlineTimeoutInHours = params.getBootstrapToOnlineTimeoutInHours();
      Optional<Boolean> leaderFollowerModelEnabled = params.getLeaderFollowerModelEnabled();
      Optional<BackupStrategy> backupStrategy = params.getBackupStrategy();
      Optional<Boolean> autoSchemaRegisterPushJobEnabled = params.getAutoSchemaRegisterPushJobEnabled();
      Optional<Boolean> hybridStoreDiskQuotaEnabled = params.getHybridStoreDiskQuotaEnabled();
      Optional<Boolean> regularVersionETLEnabled = params.getRegularVersionETLEnabled();
      Optional<Boolean> futureVersionETLEnabled = params.getFutureVersionETLEnabled();
      Optional<String> etledUserProxyAccount = params.getETLedProxyUserAccount();
      Optional<Boolean> nativeReplicationEnabled = params.getNativeReplicationEnabled();
      Optional<String> pushStreamSourceAddress = params.getPushStreamSourceAddress();
      Optional<IncrementalPushPolicy> incrementalPushPolicy = params.getIncrementalPushPolicy();
      Optional<Long> backupVersionRetentionMs = params.getBackupVersionRetentionMs();
      Optional<Integer> replicationFactor = params.getReplicationFactor();
      Optional<Boolean> migrationDuplicateStore = params.getMigrationDuplicateStore();
      Optional<String> nativeReplicationSourceFabric = params.getNativeReplicationSourceFabric();
      Optional<Boolean> activeActiveReplicationEnabled = params.getActiveActiveReplicationEnabled();
      Optional<String> regionsFilter = params.getRegionsFilter();
      Optional<Boolean> applyTargetStoreFilterForIncPush = params.applyTargetVersionFilterForIncPush();
      Optional<Boolean> isMetaSystemStoreEnabled = params.isMetaSystemStoreEnabled();

      /**
       * Check whether parent controllers will only propagate the update configs to child controller, or all unchanged
       * configs should be replicated to children too.
       */
      Optional<Boolean> replicateAll = params.getReplicateAllConfigs();
      boolean replicateAllConfigs = replicateAll.isPresent() && replicateAll.get();
      List<CharSequence> updatedConfigsList = new LinkedList<>();

      Store currStore = getVeniceHelixAdmin().getStore(clusterName, storeName);
      if (null == currStore) {
        throw new VeniceException("The store '" + storeName + "' in cluster '" + clusterName + "' does not exist, and thus cannot be updated.");
      }
      UpdateStore setStore = (UpdateStore) AdminMessageType.UPDATE_STORE.getNewInstance();
      setStore.clusterName = clusterName;
      setStore.storeName = storeName;
      setStore.owner = owner.map(addToUpdatedConfigList(updatedConfigsList, OWNER)).orElseGet(currStore::getOwner);

      // Invalid config update on hybrid will not be populated to admin channel so subsequent updates on the store won't be blocked by retry mechanism.
      if (currStore.isHybrid()) {
        // Update-store message copied to the other cluster during store migration also has partitionCount.
        // Allow updating store if the partitionCount is equal to the existing value.
        if (partitionCount.isPresent() && partitionCount.get() != currStore.getPartitionCount()){
          throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, "Cannot change partition count for hybrid stores", ExceptionType.BAD_REQUEST);
        }
        if (partitionerClass.isPresent() || partitionerParams.isPresent()) {
          throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, "Cannot change partitioner class and parameters for hybrid stores", ExceptionType.BAD_REQUEST);
        }
      }

      if (partitionCount.isPresent()) {
        setStore.partitionNum = partitionCount.get();
        int maxPartitionNum = getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName).getConfig().getMaxNumberOfPartition();
        if (setStore.partitionNum > maxPartitionNum) {
          throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, "Partition count: "
              + partitionCount + " should be less than max: " + maxPartitionNum, ExceptionType.INVALID_CONFIG);
        }
        if (setStore.partitionNum < 0) {
          throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, "Partition count: "
              + partitionCount + " should NOT be negative", ExceptionType.INVALID_CONFIG);
        }
        updatedConfigsList.add(PARTITION_COUNT);
      } else {
        setStore.partitionNum = currStore.getPartitionCount();
      }

      /**
       * TODO: We should build an UpdateStoreHelper that takes current store config and update command as input, and
       *       return whether the update command is valid.
       */
      validateNativeReplicationEnableConfigs(nativeReplicationEnabled, leaderFollowerModelEnabled, currStore, clusterName);
      validateActiveActiveReplicationEnableConfigs(activeActiveReplicationEnabled, nativeReplicationEnabled, currStore);

      setStore.nativeReplicationEnabled = nativeReplicationEnabled
          .map(addToUpdatedConfigList(updatedConfigsList, NATIVE_REPLICATION_ENABLED))
          .orElseGet(currStore::isNativeReplicationEnabled);
      setStore.pushStreamSourceAddress = pushStreamSourceAddress
          .map(addToUpdatedConfigList(updatedConfigsList, PUSH_STREAM_SOURCE_ADDRESS))
          .orElseGet(currStore::getPushStreamSourceAddress);
      setStore.activeActiveReplicationEnabled = activeActiveReplicationEnabled
          .map(addToUpdatedConfigList(updatedConfigsList, ACTIVE_ACTIVE_REPLICATION_ENABLED))
          .orElseGet(currStore::isActiveActiveReplicationEnabled);

      if (partitionerClass.isPresent() || partitionerParams.isPresent() || amplificationFactor.isPresent()) {
        // Only update fields that are set, other fields will be read from the original store's partitioner config.
        PartitionerConfig updatedPartitionerConfig = VeniceHelixAdmin.mergeNewSettingsIntoOldPartitionerConfig(currStore,
            partitionerClass, partitionerParams, amplificationFactor);
        // Update updatedConfigsList.
        partitionerClass.ifPresent(p -> updatedConfigsList.add(PARTITIONER_CLASS));
        partitionerParams.ifPresent(p -> updatedConfigsList.add(PARTITIONER_PARAMS));
        amplificationFactor.ifPresent(p -> updatedConfigsList.add(AMPLIFICATION_FACTOR));
        // Create PartitionConfigRecord for admin channel transmission.
        PartitionerConfigRecord partitionerConfigRecord = new PartitionerConfigRecord();
        partitionerConfigRecord.partitionerClass = updatedPartitionerConfig.getPartitionerClass();
        partitionerConfigRecord.partitionerParams = CollectionUtils.getCharSequenceMapFromStringMap(updatedPartitionerConfig.getPartitionerParams());
        partitionerConfigRecord.amplificationFactor = updatedPartitionerConfig.getAmplificationFactor();
        // Before setting partitioner config, verify the updated partitionerConfig can be built
        try {
          PartitionUtils.getVenicePartitioner(
              partitionerConfigRecord.partitionerClass.toString(),
              partitionerConfigRecord.amplificationFactor,
              new VeniceProperties(partitionerConfigRecord.partitionerParams),
              getKeySchema(clusterName, storeName).getSchema()
          );
        } catch (Exception e) {
          throw new VeniceException("Partitioner Configs invalid, please verify that partitioner configs like classpath and parameters are correct!", e);
        }
        setStore.partitionerConfig = partitionerConfigRecord;
      }

      setStore.enableReads = readability
          .map(addToUpdatedConfigList(updatedConfigsList, ENABLE_READS))
          .orElseGet(currStore::isEnableReads);
      setStore.enableWrites = writeability
          .map(addToUpdatedConfigList(updatedConfigsList, ENABLE_WRITES))
          .orElseGet(currStore::isEnableWrites);

      setStore.readQuotaInCU = readQuotaInCU
          .map(addToUpdatedConfigList(updatedConfigsList, READ_QUOTA_IN_CU))
          .orElseGet(currStore::getReadQuotaInCU);

      //We need to to be careful when handling currentVersion.
      //Since it is not synced between parent and local controller,
      //It is very likely to override local values unintentionally.
      setStore.currentVersion = currentVersion
          .map(addToUpdatedConfigList(updatedConfigsList, VERSION))
          .orElse(AdminConsumptionTask.IGNORED_CURRENT_VERSION);

      hybridRewindSeconds.map(addToUpdatedConfigList(updatedConfigsList, REWIND_TIME_IN_SECONDS));
      hybridOffsetLagThreshold.map(addToUpdatedConfigList(updatedConfigsList, OFFSET_LAG_TO_GO_ONLINE));
      hybridTimeLagThreshold.map(addToUpdatedConfigList(updatedConfigsList, TIME_LAG_TO_GO_ONLINE));
      hybridDataReplicationPolicy.map(addToUpdatedConfigList(updatedConfigsList, DATA_REPLICATION_POLICY));
      hybridBufferReplayPolicy.map(addToUpdatedConfigList(updatedConfigsList, BUFFER_REPLAY_POLICY));
      HybridStoreConfig hybridStoreConfig = VeniceHelixAdmin.mergeNewSettingsIntoOldHybridStoreConfig(
          currStore, hybridRewindSeconds, hybridOffsetLagThreshold, hybridTimeLagThreshold, hybridDataReplicationPolicy,
          hybridBufferReplayPolicy);
      if (null == hybridStoreConfig) {
        setStore.hybridStoreConfig = null;
      } else {
        HybridStoreConfigRecord hybridStoreConfigRecord = new HybridStoreConfigRecord();
        hybridStoreConfigRecord.offsetLagThresholdToGoOnline = hybridStoreConfig.getOffsetLagThresholdToGoOnline();
        hybridStoreConfigRecord.rewindTimeInSeconds = hybridStoreConfig.getRewindTimeInSeconds();
        hybridStoreConfigRecord.producerTimestampLagThresholdToGoOnlineInSeconds = hybridStoreConfig.getProducerTimestampLagThresholdToGoOnlineInSeconds();
        hybridStoreConfigRecord.dataReplicationPolicy = hybridStoreConfig.getDataReplicationPolicy().getValue();
        hybridStoreConfigRecord.bufferReplayPolicy = hybridStoreConfig.getBufferReplayPolicy().getValue();
        setStore.hybridStoreConfig = hybridStoreConfigRecord;
      }

      getVeniceHelixAdmin().checkWhetherStoreWillHaveConflictConfigForIncrementalAndHybrid(currStore, incrementalPushEnabled, incrementalPushPolicy, Optional.ofNullable(hybridStoreConfig));
      getVeniceHelixAdmin().checkWhetherStoreWillHaveConflictConfigForCompressionAndHybrid(currStore, compressionStrategy, Optional.ofNullable(hybridStoreConfig));

      /**
       * Set storage quota according to store properties. For hybrid stores, rocksDB has the overhead ratio as we
       * do append-only and compaction will happen later.
       * We expose actual disk usage to users, instead of multiplying/dividing the overhead ratio by situations.
       */
      setStore.storageQuotaInByte = storageQuotaInByte
          .map(addToUpdatedConfigList(updatedConfigsList, STORAGE_QUOTA_IN_BYTE))
          .orElseGet(currStore::getStorageQuotaInByte);

      setStore.accessControlled = accessControlled
          .map(addToUpdatedConfigList(updatedConfigsList, ACCESS_CONTROLLED))
          .orElseGet(currStore::isAccessControlled);
      setStore.compressionStrategy = compressionStrategy
          .map(addToUpdatedConfigList(updatedConfigsList, COMPRESSION_STRATEGY))
          .map(CompressionStrategy::getValue)
          .orElse(currStore.getCompressionStrategy().getValue());
      setStore.clientDecompressionEnabled =  clientDecompressionEnabled
          .map(addToUpdatedConfigList(updatedConfigsList, CLIENT_DECOMPRESSION_ENABLED))
          .orElseGet(currStore::getClientDecompressionEnabled);
      setStore.chunkingEnabled = chunkingEnabled
          .map(addToUpdatedConfigList(updatedConfigsList, CHUNKING_ENABLED))
          .orElseGet(currStore::isChunkingEnabled);
      setStore.batchGetLimit = batchGetLimit
          .map(addToUpdatedConfigList(updatedConfigsList, BATCH_GET_LIMIT))
          .orElseGet(currStore::getBatchGetLimit);
      setStore.numVersionsToPreserve = numVersionsToPreserve
          .map(addToUpdatedConfigList(updatedConfigsList, NUM_VERSIONS_TO_PRESERVE))
          .orElseGet(currStore::getNumVersionsToPreserve);
      setStore.incrementalPushEnabled = incrementalPushEnabled
          .map(addToUpdatedConfigList(updatedConfigsList, INCREMENTAL_PUSH_ENABLED))
          .orElseGet(currStore::isIncrementalPushEnabled);
      setStore.isMigrating = storeMigration
          .map(addToUpdatedConfigList(updatedConfigsList, STORE_MIGRATION))
          .orElseGet(currStore::isMigrating);
      setStore.writeComputationEnabled = writeComputationEnabled
          .map(addToUpdatedConfigList(updatedConfigsList, WRITE_COMPUTATION_ENABLED))
          .orElseGet(currStore::isWriteComputationEnabled);
      setStore.readComputationEnabled = readComputationEnabled
          .map(addToUpdatedConfigList(updatedConfigsList, READ_COMPUTATION_ENABLED))
          .orElseGet(currStore::isReadComputationEnabled);
      setStore.bootstrapToOnlineTimeoutInHours = bootstrapToOnlineTimeoutInHours
          .map(addToUpdatedConfigList(updatedConfigsList, BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS))
          .orElseGet(currStore::getBootstrapToOnlineTimeoutInHours);
      setStore.leaderFollowerModelEnabled = leaderFollowerModelEnabled
          .map(addToUpdatedConfigList(updatedConfigsList, LEADER_FOLLOWER_MODEL_ENABLED))
          .orElseGet(currStore::isLeaderFollowerModelEnabled);
      setStore.backupStrategy = (backupStrategy
          .map(addToUpdatedConfigList(updatedConfigsList, BACKUP_STRATEGY))
          .orElse(currStore.getBackupStrategy())).ordinal();

      setStore.schemaAutoRegisterFromPushJobEnabled = autoSchemaRegisterPushJobEnabled
          .map(addToUpdatedConfigList(updatedConfigsList, AUTO_SCHEMA_REGISTER_FOR_PUSHJOB_ENABLED))
          .orElse(currStore.isSchemaAutoRegisterFromPushJobEnabled());

      setStore.hybridStoreDiskQuotaEnabled = hybridStoreDiskQuotaEnabled
          .map(addToUpdatedConfigList(updatedConfigsList, HYBRID_STORE_DISK_QUOTA_ENABLED))
          .orElse(currStore.isHybridStoreDiskQuotaEnabled());

      regularVersionETLEnabled.map(addToUpdatedConfigList(updatedConfigsList, REGULAR_VERSION_ETL_ENABLED));
      futureVersionETLEnabled.map(addToUpdatedConfigList(updatedConfigsList, FUTURE_VERSION_ETL_ENABLED));
      etledUserProxyAccount.map(addToUpdatedConfigList(updatedConfigsList, ETLED_PROXY_USER_ACCOUNT));
      setStore.ETLStoreConfig = mergeNewSettingIntoOldETLStoreConfig(currStore, regularVersionETLEnabled, futureVersionETLEnabled, etledUserProxyAccount);

      setStore.largestUsedVersionNumber = largestUsedVersionNumber
          .map(addToUpdatedConfigList(updatedConfigsList, LARGEST_USED_VERSION_NUMBER))
          .orElseGet(currStore::getLargestUsedVersionNumber);

      setStore.incrementalPushPolicy = incrementalPushPolicy
          .map(addToUpdatedConfigList(updatedConfigsList, INCREMENTAL_PUSH_POLICY))
          .map(IncrementalPushPolicy::getValue)
          .orElse(currStore.getIncrementalPushPolicy().getValue());
      setStore.backupVersionRetentionMs = backupVersionRetentionMs
          .map(addToUpdatedConfigList(updatedConfigsList, BACKUP_VERSION_RETENTION_MS))
          .orElseGet(currStore::getBackupVersionRetentionMs);
      setStore.replicationFactor = replicationFactor
          .map(addToUpdatedConfigList(updatedConfigsList, REPLICATION_FACTOR))
          .orElseGet(currStore::getReplicationFactor);
      setStore.migrationDuplicateStore = migrationDuplicateStore
          .map(addToUpdatedConfigList(updatedConfigsList, MIGRATION_DUPLICATE_STORE))
          .orElseGet(currStore::isMigrationDuplicateStore);
      setStore.nativeReplicationSourceFabric = nativeReplicationSourceFabric
          .map(addToUpdatedConfigList(updatedConfigsList, NATIVE_REPLICATION_SOURCE_FABRIC))
          .orElseGet((currStore::getNativeReplicationSourceFabric));

      setStore.applyTargetVersionFilterForIncPush = applyTargetStoreFilterForIncPush
          .map(addToUpdatedConfigList(updatedConfigsList, APPLY_TARGET_VERSION_FILTER_FOR_INC_PUSH))
          .orElseGet(currStore::isApplyTargetVersionFilterForIncPush);

      setStore.metaSystemStoreEnabled = isMetaSystemStoreEnabled
          .map(addToUpdatedConfigList(updatedConfigsList, META_SYSTEM_STORE_ENABLED))
          .orElseGet(currStore::isStoreMetadataSystemStoreEnabled);


      /**
       * By default, parent controllers will not try to replicate the unchanged store configs to child controllers;
       * an updatedConfigsList will be used to represent which configs are updated by users.
       */
      setStore.replicateAllConfigs = replicateAllConfigs;
      if (!replicateAllConfigs) {
        if (updatedConfigsList.size() == 0) {
          String errMsg = "UpdateStore command failed for store " + storeName + ". The command didn't change any specific"
              + " store config and didn't specify \"--replicate-all-configs\" flag.";
          logger.error(errMsg);
          throw new VeniceException(errMsg);
        }
        setStore.updatedConfigsList = updatedConfigsList;
      } else {
        setStore.updatedConfigsList = Collections.emptyList();
      }

      /**
       * Fabrics filter is not a store config, so we don't need to add it into {@link UpdateStore#updatedConfigsList}
       */
      setStore.regionsFilter = regionsFilter.orElse(null);

      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.UPDATE_STORE.getValue();
      message.payloadUnion = setStore;
      sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);

      final boolean readComputeJustEnabled = readComputationEnabled.orElse(false) && !currStore.isReadComputationEnabled();
      final boolean writeComputeJustEnabled = writeComputationEnabled.orElse(false) && !currStore.isWriteComputationEnabled();
      if (readComputeJustEnabled || writeComputeJustEnabled) {
        addSupersetSchemaForStore(clusterName, storeName, currStore.isActiveActiveReplicationEnabled());
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
      releaseAdminMessageLock(clusterName);
    }
  }

  private void addSupersetSchemaForStore(String clusterName, String storeName, boolean activeActiveReplicationEnabled) {
    // Generate a superset schema and add it.
    SchemaEntry supersetSchemaEntry = AvroSchemaUtils.generateSupersetSchemaFromAllValueSchemas(getValueSchemas(clusterName, storeName));
    final Schema supersetSchema = supersetSchemaEntry.getSchema();
    final int supersetSchemaID = supersetSchemaEntry.getId();
    addValueSchemaEntry(clusterName, storeName, supersetSchema.toString(), supersetSchemaID, true);

    if (activeActiveReplicationEnabled) {
      updateReplicationMetadataSchema(clusterName, storeName, supersetSchema, supersetSchemaID);
    }
  }

  @Override
  public void updateClusterConfig(String clusterName, UpdateClusterConfigQueryParams params) {
    getVeniceHelixAdmin().updateClusterConfig(clusterName, params);
  }

  private void validateNativeReplicationEnableConfigs(
      Optional<Boolean> nativeReplicationEnabledOptional,
      Optional<Boolean> leaderFollowerModelEnabled,
      Store store,
      String clusterName
  ) {
    final boolean nativeReplicationEnabled = nativeReplicationEnabledOptional.orElse(false);
    if (!nativeReplicationEnabled) {
      return;
    }

    final boolean isLeaderFollowerModelEnabled = (!leaderFollowerModelEnabled.isPresent() && store.isLeaderFollowerModelEnabled())
        || (leaderFollowerModelEnabled.isPresent() && leaderFollowerModelEnabled.get());
    final boolean isLfModelDependencyCheckDisabled =
        getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName).getConfig().isLfModelDependencyCheckDisabled();
    if(!isLeaderFollowerModelEnabled && !isLfModelDependencyCheckDisabled) {
      throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, "Native Replication cannot be enabled for store " + store.getName() + " since it's not on L/F state model", ExceptionType.INVALID_CONFIG);
    }
  }

  private void validateActiveActiveReplicationEnableConfigs(
      Optional<Boolean> activeActiveReplicationEnabledOptional,
      Optional<Boolean> nativeReplicationEnabledOptional,
      Store store
  ) {
    final boolean activeActiveReplicationEnabled = activeActiveReplicationEnabledOptional.orElse(false);
    if (!activeActiveReplicationEnabled) {
      return;
    }

    final boolean nativeReplicationEnabled =
        nativeReplicationEnabledOptional.isPresent() ? nativeReplicationEnabledOptional.get() : store.isNativeReplicationEnabled();

    if (!nativeReplicationEnabled) {
      throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, "Active/Active Replication cannot be enabled for store "
          + store.getName() + " since Native Replication is not enabled on it.", ExceptionType.INVALID_CONFIG);
    }
  }

  @Override
  public double getStorageEngineOverheadRatio(String clusterName) {
    return getVeniceHelixAdmin().getStorageEngineOverheadRatio(clusterName);
  }

  @Override
  public SchemaEntry getKeySchema(String clusterName, String storeName) {
    return getVeniceHelixAdmin().getKeySchema(clusterName, storeName);
  }

  @Override
  public Collection<SchemaEntry> getValueSchemas(String clusterName, String storeName) {
    return getVeniceHelixAdmin().getValueSchemas(clusterName, storeName);
  }

  @Override
  public Collection<DerivedSchemaEntry> getDerivedSchemas(String clusterName, String storeName) {
    return getVeniceHelixAdmin().getDerivedSchemas(clusterName, storeName);
  }

  @Override
  public int getValueSchemaId(String clusterName, String storeName, String valueSchemaStr) {
    return getVeniceHelixAdmin().getValueSchemaId(clusterName, storeName, valueSchemaStr);
  }

  @Override
  public Pair<Integer, Integer> getDerivedSchemaId(String clusterName, String storeName, String schemaStr) {
    return getVeniceHelixAdmin().getDerivedSchemaId(clusterName, storeName, schemaStr);
  }

  @Override
  public SchemaEntry getValueSchema(String clusterName, String storeName, int id) {
    return getVeniceHelixAdmin().getValueSchema(clusterName, storeName, id);
  }

  @Override
  public SchemaEntry addValueSchema(
      String clusterName,
      String storeName,
      String newValueSchemaStr,
      DirectionalSchemaCompatibilityType expectedCompatibilityType
  ) {
    acquireAdminMessageLock(clusterName, storeName);
    try {
      final int newValueSchemaId = getVeniceHelixAdmin().checkPreConditionForAddValueSchemaAndGetNewSchemaId(
          clusterName, storeName, newValueSchemaStr, expectedCompatibilityType);

      // if we find this is a exactly duplicate schema, return the existing schema id
      // else add the schema with possible doc field change
      if (newValueSchemaId == SchemaData.DUPLICATE_VALUE_SCHEMA_CODE) {
        return new SchemaEntry(getVeniceHelixAdmin().getValueSchemaId(clusterName, storeName, newValueSchemaStr), newValueSchemaStr);
      }

      final Store store = getVeniceHelixAdmin().getStore(clusterName, storeName);
      Schema existingValueSchema = getVeniceHelixAdmin().getLatestValueSchema(clusterName, store);

      final boolean doUpdateSupersetSchemaID;
      if (existingValueSchema != null && (store.isReadComputationEnabled() || store.isWriteComputationEnabled())) {
        Schema newValueSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(newValueSchemaStr);
        Schema newSuperSetSchema = AvroSupersetSchemaUtils.generateSuperSetSchema(existingValueSchema, newValueSchema);
        String newSuperSetSchemaStr = newSuperSetSchema.toString();

        if (AvroSchemaUtils.compareSchemaIgnoreFieldOrder(newSuperSetSchema, newValueSchema)) {
          doUpdateSupersetSchemaID = true;

        } else if (AvroSchemaUtils.compareSchemaIgnoreFieldOrder(newSuperSetSchema, existingValueSchema)) {
          doUpdateSupersetSchemaID = false;

        } else {
          // Register superset schema only if it does not match with existing or new schema.

          // validate compatibility of the new superset schema
          getVeniceHelixAdmin().checkPreConditionForAddValueSchemaAndGetNewSchemaId(
              clusterName, storeName, newSuperSetSchemaStr, expectedCompatibilityType);
          // Check if the superset schema already exists or not. If exists use the same ID, else bump the value ID by one.
          int supersetSchemaId = getVeniceHelixAdmin().getValueSchemaIdIgnoreFieldOrder(clusterName, storeName, newSuperSetSchemaStr);
          if (supersetSchemaId == SchemaData.INVALID_VALUE_SCHEMA_ID) {
            supersetSchemaId = newValueSchemaId + 1;
          }
          return addValueAndSupersetSchemaEntries(
              clusterName,
              storeName,
              new SchemaEntry(newValueSchemaId, newValueSchema),
              new SchemaEntry(supersetSchemaId, newSuperSetSchema)
          );
        }
      } else {
        doUpdateSupersetSchemaID = false;
      }

      SchemaEntry schemaEntry = addValueSchemaEntry(clusterName, storeName, newValueSchemaStr, newValueSchemaId, doUpdateSupersetSchemaID);

      /**
       * if active-active replication is enabled for the store then generate and register the new Replication metadata schema
       * for this newly added value schema.
       */
      if (store.isActiveActiveReplicationEnabled()) {
        Schema latestValueSchema = getVeniceHelixAdmin().getLatestValueSchema(clusterName, store);
        final int valueSchemaId = getValueSchemaId(clusterName, storeName, latestValueSchema.toString());
        updateReplicationMetadataSchema(clusterName, storeName, latestValueSchema, valueSchemaId);
      }

      return schemaEntry;
    } finally {
      releaseAdminMessageLock(clusterName);
    }
  }

  private SchemaEntry addValueAndSupersetSchemaEntries(
      String clusterName,
      String storeName,
      SchemaEntry newValueSchemaEntry,
      SchemaEntry newSupersetSchemaEntry
  ) {
    validateNewSupersetAndValueSchemaEntries(storeName, clusterName, newValueSchemaEntry, newSupersetSchemaEntry);
    logger.info("Adding value schema {} and superset schema {} to store: {} in cluster: {}",
        newValueSchemaEntry, newSupersetSchemaEntry, storeName, clusterName);

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
    updateReplicationMetadataSchema(clusterName, storeName, newValueSchemaEntry.getSchema(), newValueSchemaEntry.getId());
    updateReplicationMetadataSchema(clusterName, storeName, newSupersetSchemaEntry.getSchema(), newSupersetSchemaEntry.getId());
    return newValueSchemaEntry;
  }

  private void validateNewSupersetAndValueSchemaEntries(
      String storeName,
      String clusterName,
      SchemaEntry newValueSchemaEntry,
      SchemaEntry newSupersetSchemaEntry
  ) {
    if (newValueSchemaEntry.getId() == newSupersetSchemaEntry.getId()) {
      throw new IllegalArgumentException(
          String.format("Superset schema ID and value schema ID are expected to be different for store %s in cluster %s. "
              + "Got ID: %d", storeName, clusterName, newValueSchemaEntry.getId()));
    }
    if (AvroSchemaUtils.compareSchemaIgnoreFieldOrder(newValueSchemaEntry.getSchema(), newSupersetSchemaEntry.getSchema())) {
      throw new IllegalArgumentException(
          String.format("Superset and value schemas are expected to be different for store %s in cluster %s. Got schema: %s",
              storeName, clusterName, newValueSchemaEntry.getSchema()));
    }
  }

  private SchemaEntry addValueSchemaEntry(
      String clusterName,
      String storeName,
      String valueSchemaStr,
      final int newValueSchemaId,
      final boolean doUpdateSupersetSchemaID
  ) {
    logger.info("Adding value schema: " + valueSchemaStr + " to store: " + storeName + " in cluster: " + clusterName);

    ValueSchemaCreation valueSchemaCreation =
        (ValueSchemaCreation) AdminMessageType.VALUE_SCHEMA_CREATION.getNewInstance();
    valueSchemaCreation.clusterName = clusterName;
    valueSchemaCreation.storeName = storeName;
    SchemaMeta schemaMeta = new SchemaMeta();
    schemaMeta.definition = valueSchemaStr;
    schemaMeta.schemaType = SchemaType.AVRO_1_4.getValue();
    valueSchemaCreation.schema = schemaMeta;
    valueSchemaCreation.schemaId = newValueSchemaId;
    valueSchemaCreation.doUpdateSupersetSchemaID = doUpdateSupersetSchemaID;

    AdminOperation message = new AdminOperation();
    message.operationType = AdminMessageType.VALUE_SCHEMA_CREATION.getValue();
    message.payloadUnion = valueSchemaCreation;
    sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);

    //defensive code checking
    int actualValueSchemaId = getValueSchemaId(clusterName, storeName, valueSchemaStr);
    if (actualValueSchemaId != newValueSchemaId) {
      throw new VeniceException(
          "Something bad happens, the expected new value schema id is: " + newValueSchemaId + ", but got: "
              + actualValueSchemaId);
    }

    return new SchemaEntry(actualValueSchemaId, valueSchemaStr);
  }

  @Override
  public SchemaEntry addSupersetSchema(String clusterName, String storeName, String valueSchemaStr, int valueSchemaId,
      String supersetSchemaStr, int supersetSchemaId) {
    throw new VeniceUnsupportedOperationException("addValueSchema");
  }

  @Override
  public SchemaEntry addValueSchema(String clusterName, String storeName, String valueSchemaStr, int schemaId, boolean doUpdateSupersetSchemaID) {
    throw new VeniceUnsupportedOperationException("addValueSchema");
  }

  @Override
  public DerivedSchemaEntry addDerivedSchema(String clusterName, String storeName, int valueSchemaId, String derivedSchemaStr) {
    acquireAdminMessageLock(clusterName, storeName);
    try {
      int newDerivedSchemaId = veniceHelixAdmin
          .checkPreConditionForAddDerivedSchemaAndGetNewSchemaId(clusterName, storeName, valueSchemaId, derivedSchemaStr);

      //if we find this is a duplicate schema, return the existing schema id
      if (newDerivedSchemaId == SchemaData.DUPLICATE_VALUE_SCHEMA_CODE) {
        return new DerivedSchemaEntry(valueSchemaId,
            getVeniceHelixAdmin().getDerivedSchemaId(clusterName, storeName, derivedSchemaStr).getSecond(), derivedSchemaStr);
      }

      logger.info("Adding derived schema: " + derivedSchemaStr + " to store: " + storeName + ", version: " +
          valueSchemaId + " in cluster: " + clusterName);

      DerivedSchemaCreation derivedSchemaCreation = (DerivedSchemaCreation) AdminMessageType.DERIVED_SCHEMA_CREATION.getNewInstance();
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

      //defensive code checking
      Pair<Integer, Integer> actualValueSchemaIdPair = getDerivedSchemaId(clusterName, storeName, derivedSchemaStr);
      if (actualValueSchemaIdPair.getFirst() != valueSchemaId || actualValueSchemaIdPair.getSecond() != newDerivedSchemaId) {
        throw new VeniceException(String.format("Something bad happened, the expected new value schema id pair is:"
                + "%d_%d, but got: %d_%d", valueSchemaId, newDerivedSchemaId, actualValueSchemaIdPair.getFirst(),
            actualValueSchemaIdPair.getSecond()));
      }

      return new DerivedSchemaEntry(valueSchemaId, newDerivedSchemaId, derivedSchemaStr);
    } finally {
      releaseAdminMessageLock(clusterName);
    }
  }

  @Override
  public DerivedSchemaEntry addDerivedSchema(String clusterName, String storeName, int valueSchemaId, int derivedSchemaId, String derivedSchemaStr) {
    throw new VeniceUnsupportedOperationException("addDerivedSchema");
  }

  @Override
  public DerivedSchemaEntry removeDerivedSchema(String clusterName, String storeName, int valueSchemaId, int derivedSchemaId) {
    throw new VeniceUnsupportedOperationException("removeDerivedSchema");
  }

  @Override
  public Collection<ReplicationMetadataSchemaEntry> getReplicationMetadataSchemas(String clusterName, String storeName) {
    return getVeniceHelixAdmin().getReplicationMetadataSchemas(clusterName, storeName);
  }

  @Override
  public Optional<Schema> getReplicationMetadataSchema(String clusterName, String storeName, int valueSchemaID, int rmdVersionID) {
    return getVeniceHelixAdmin().getReplicationMetadataSchema(clusterName, storeName, valueSchemaID, rmdVersionID);
  }

  @Override
  public ReplicationMetadataSchemaEntry addReplicationMetadataSchema(
      String clusterName,
      String storeName,
      int valueSchemaId,
      int replicationMetadataVersionId,
      String replicationMetadataSchemaStr
  ) {
    acquireAdminMessageLock(clusterName, storeName);
    try {
      ReplicationMetadataSchemaEntry replicationMetadataSchemaEntry =
          new ReplicationMetadataSchemaEntry(valueSchemaId, replicationMetadataVersionId, replicationMetadataSchemaStr);
      final boolean replicationMetadataSchemaAlreadyPresent = getVeniceHelixAdmin().checkIfMetadataSchemaAlreadyPresent(
          clusterName,
          storeName,
          valueSchemaId,
          replicationMetadataSchemaEntry
      );
      if (replicationMetadataSchemaAlreadyPresent) {
        logger.info("Replication metadata schema already exists for store:" + storeName + " in cluster:" + clusterName + " metadataSchema:" + replicationMetadataSchemaStr
            + " replicationMetadataVersionId:" + replicationMetadataVersionId + " valueSchemaId:" + valueSchemaId);
        return replicationMetadataSchemaEntry;
      }

      logger.info("Adding Replication metadata schema: for store:" + storeName + " in cluster:" + clusterName + " metadataSchema:" + replicationMetadataSchemaStr
          + " replicationMetadataVersionId:" + replicationMetadataVersionId + " valueSchemaId:" + valueSchemaId);

      MetadataSchemaCreation replicationMetadataSchemaCreation = (MetadataSchemaCreation) AdminMessageType.REPLICATION_METADATA_SCHEMA_CREATION
          .getNewInstance();
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

      // Be defensive and check that RMD schema has been added indeed.
      final Schema expectedRmdSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(replicationMetadataSchemaStr);
      validateRmdSchemaIsAddedAsExpected(clusterName, storeName, valueSchemaId, replicationMetadataVersionId, expectedRmdSchema);
      return new ReplicationMetadataSchemaEntry(valueSchemaId, replicationMetadataVersionId, replicationMetadataSchemaStr);

    } finally {
      releaseAdminMessageLock(clusterName);
    }
  }

  private void validateRmdSchemaIsAddedAsExpected(
      String clusterName,
      String storeName,
      int valueSchemaID,
      int rmdVersionID,
      Schema expectedRmdSchema
  ) {
    final Schema addedRmdSchema = getReplicationMetadataSchema(clusterName, storeName, valueSchemaID, rmdVersionID).orElse(null);
    if (addedRmdSchema == null) {
      throw new VeniceException(String.format("No replication metadata schema found for store %s in cluster %s with value "
          + "schema ID %s and RMD protocol version ID %d", storeName, clusterName, valueSchemaID, rmdVersionID));
    }
    if (!AvroSchemaUtils.compareSchemaIgnoreFieldOrder(addedRmdSchema, expectedRmdSchema)) {
      throw new VeniceException(String.format("For store %s in cluster %s with value schema ID %d and RMD protocol"
              + " version ID %d. Expected RMD schema %s. But got RMD schema: %s",
          storeName, clusterName, valueSchemaID, rmdVersionID, expectedRmdSchema.toString(true), addedRmdSchema.toString(true))
      );
    }
  }

  @Override
  public void validateAndMaybeRetrySystemStoreAutoCreation(String clusterName, String storeName, VeniceSystemStoreType veniceSystemStoreType) {
    throw new VeniceUnsupportedOperationException("validateAndMaybeRetrySystemStoreAutoCreation");
  }

  private void updateReplicationMetadataSchemaForAllValueSchema(String clusterName, String storeName) {
    final Collection<SchemaEntry> valueSchemas = getValueSchemas(clusterName, storeName);
    for (SchemaEntry valueSchemaEntry : valueSchemas) {
      updateReplicationMetadataSchema(clusterName, storeName, valueSchemaEntry.getSchema(), valueSchemaEntry.getId());
    }
  }

  private void updateReplicationMetadataSchema(String clusterName, String storeName, Schema valueSchema, int valueSchemaId) {
    final int rmdVersionId = getMultiClusterConfigs().getCommonConfig().getReplicationMetadataVersionId();
    final boolean valueSchemaAlreadyHasRmdSchema = getVeniceHelixAdmin()
        .checkIfValueSchemaAlreadyHasRmdSchema(clusterName, storeName, valueSchemaId, rmdVersionId);
    if (valueSchemaAlreadyHasRmdSchema) {
      logger.info("Store {} in cluster {} already has a replication metadata schema for its value schema with "
          + "ID {} and replication metadata version ID {}. So skip updating this value schema's RMD schema.",
          storeName, clusterName, valueSchemaId, rmdVersionId);
      return;
    }
    String replicationMetadataSchemaStr = ReplicationMetadataSchemaGenerator.generateMetadataSchema(valueSchema, rmdVersionId).toString();
    addReplicationMetadataSchema(clusterName, storeName, valueSchemaId, rmdVersionId, replicationMetadataSchemaStr);
  }

  @Override
  public List<String> getStorageNodes(String clusterName) {
    throw new VeniceUnsupportedOperationException("getStorageNodes");
  }

  @Override
  public Map<String, String> getStorageNodesStatus(String clusterName) {
    throw new VeniceUnsupportedOperationException("getStorageNodesStatus");
  }

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

  @Override
  public OfflinePushStatusInfo getOffLinePushStatus(String clusterName, String kafkaTopic, Optional<String> incrementalPushVersion) {
    Map<String, ControllerClient> controllerClients = getVeniceHelixAdmin().getControllerClientMap(clusterName);
    return getOffLineJobStatus(clusterName, kafkaTopic, controllerClients, incrementalPushVersion);
  }

  protected OfflinePushStatusInfo getOffLineJobStatus(String clusterName, String kafkaTopic,
    Map<String, ControllerClient> controllerClients) {
    return getOffLineJobStatus(clusterName, kafkaTopic, controllerClients, Optional.empty());
  }

  protected OfflinePushStatusInfo getOffLineJobStatus(String clusterName, String kafkaTopic,
      Map<String, ControllerClient> controllerClients, Optional<String> incrementalPushVersion) {
    Set<String> childClusters = controllerClients.keySet();
    ExecutionStatus currentReturnStatus = ExecutionStatus.NEW;
    Optional<String> currentReturnStatusDetails = Optional.empty();
    List<ExecutionStatus> statuses = new ArrayList<>();
    Map<String, String> extraInfo = new HashMap<>();
    Map<String, String> extraDetails = new HashMap<>();
    int failCount = 0;
    for (Map.Entry<String, ControllerClient> entry: controllerClients.entrySet()) {
      String region = entry.getKey();
      ControllerClient controllerClient = entry.getValue();
      String leaderControllerUrl = "Unspecified leader controller url";
      try {
        leaderControllerUrl = controllerClient.getLeaderControllerUrl();
      } catch (VeniceException getMasterException) {
        logger.warn("Couldn't query " + region + " for job status of " + kafkaTopic, getMasterException);
        statuses.add(ExecutionStatus.UNKNOWN);
        extraInfo.put(region, ExecutionStatus.UNKNOWN.toString());
        extraDetails.put(region, "Failed to get leader controller url " + getMasterException.getMessage());
        continue;
      }
      JobStatusQueryResponse response = controllerClient.queryJobStatus(kafkaTopic, incrementalPushVersion);
      if (response.isError()) {
        failCount += 1;
        logger.warn("Couldn't query " + region + " for job " + kafkaTopic + " status: " + response.getError());
        statuses.add(ExecutionStatus.UNKNOWN);
        extraInfo.put(region, ExecutionStatus.UNKNOWN.toString());
        extraDetails.put(region, leaderControllerUrl + " " + response.getError());
      } else {
        ExecutionStatus status = ExecutionStatus.valueOf(response.getStatus());

        statuses.add(status);
        extraInfo.put(region, response.getStatus());
        Optional<String> statusDetails = response.getOptionalStatusDetails();
        if (statusDetails.isPresent()) {
          extraDetails.put(region, leaderControllerUrl + " " + statusDetails.get());
        }
      }
    }
    // Sort the per-datacenter status in this order, and return the first one in the list
    // Edge case example: if one cluster is stuck in NOT_CREATED, then
    //   as another cluster goes from PROGRESS to COMPLETED
    //   the aggregate status will go from PROGRESS back down to NOT_CREATED.
    statuses.sort(Comparator.comparingInt(VeniceHelixAdmin.STATUS_PRIORITIES::indexOf));
    if (statuses.size() > 0) {
      currentReturnStatus = statuses.get(0);
    }

    int successCount = childClusters.size() - failCount;
    if (! (successCount >= (childClusters.size() / 2) + 1)) { // Strict majority must be reachable, otherwise keep polling
      currentReturnStatus = ExecutionStatus.PROGRESS;
    }

    if (currentReturnStatus.isTerminal()) {
      // If there is a temporary datacenter connection failure, we want H2V to report failure while allowing the push
      // to succeed in remaining datacenters.  If we want to allow the push to succeed in asyc in the remaining datacenter
      // then put the topic delete into an else block under `if (failcount > 0)`
      if (failCount > 0) {
        currentReturnStatus = ExecutionStatus.ERROR;
        currentReturnStatusDetails = Optional.of(failCount + "/" + childClusters.size() + " DCs unreachable. ");
      }

      // TODO: Set parent controller's version status based on currentReturnStatus
      // COMPLETED -> ONLINE
      // ERROR -> ERROR
      //TODO: remove this if statement since it was only for debugging purpose
      if (maxErroredTopicNumToKeep > 0 && currentReturnStatus.equals(ExecutionStatus.ERROR)) {
        currentReturnStatusDetails = Optional.of(currentReturnStatusDetails.orElse("") + "Parent Kafka topic won't be truncated");
        logger.info("The errored kafka topic: " + kafkaTopic + " won't be truncated since it will be used to investigate"
            + "some Kafka related issue");
      } else {
        /**
         * truncate the topic if either
         * 1. the store is not incremental push enabled and the push completed (no ERROR)
         * 2. this is a failed batch push
         * 3. the store is incremental push enabled and same incPushToRT and batch push finished
         */
        Store store = getVeniceHelixAdmin().getStore(clusterName, Version.parseStoreFromKafkaTopicName(kafkaTopic));
        boolean failedBatchPush = !incrementalPushVersion.isPresent() && currentReturnStatus == ExecutionStatus.ERROR;
        boolean incPushEnabledBatchpushSuccess = !incrementalPushVersion.isPresent() && store.isIncrementalPushEnabled() &&
            store.getIncrementalPushPolicy() == INCREMENTAL_PUSH_SAME_AS_REAL_TIME;
        boolean nonIncPushBatchSucess =  !store.isIncrementalPushEnabled() && currentReturnStatus != ExecutionStatus.ERROR;

        if ((failedBatchPush || nonIncPushBatchSucess || incPushEnabledBatchpushSuccess) &&
            !getMultiClusterConfigs().getCommonConfig().disableParentTopicTruncationUponCompletion()) {
            logger.info("Truncating kafka topic: " + kafkaTopic + " with job status: " + currentReturnStatus);
            truncateKafkaTopic(kafkaTopic);
            Optional<Version> version = store.getVersion(Version.parseVersionFromKafkaTopicName(kafkaTopic));
            if (version.isPresent() && version.get().getPushType().isStreamReprocessing()) {
              truncateKafkaTopic(Version.composeStreamReprocessingTopic(store.getName(), version.get().getNumber()));
            }
            currentReturnStatusDetails = Optional.of(currentReturnStatusDetails.orElse("") + "Parent Kafka topic truncated");
          }
        }
    }

    return new OfflinePushStatusInfo(currentReturnStatus, extraInfo, currentReturnStatusDetails, extraDetails);
  }

  /**
   * Queries child clusters for job progress.  Prepends the cluster name to the task ID and provides an aggregate
   * Map of progress for all tasks.
   * @param clusterName
   * @param kafkaTopic
   * @return
   */
  @Override
  public Map<String, Long> getOfflinePushProgress(String clusterName, String kafkaTopic){
    Map<String, ControllerClient> controllerClients = getVeniceHelixAdmin().getControllerClientMap(clusterName);
    return getOfflineJobProgress(clusterName, kafkaTopic, controllerClients);
  }

  @Override
  public Set<String> getOngoingIncrementalPushVersions(String clusterName, String kafkaTopic) {
    getVeniceHelixAdmin().checkControllerLeadershipFor(clusterName);
    Set<String> result = new HashSet<>();
    for (ControllerClient controllerClient : getVeniceHelixAdmin().getControllerClientMap(clusterName).values()) {
      IncrementalPushVersionsResponse response = controllerClient.getOngoingIncrementalPushVersions(kafkaTopic);
      // return any ongoing incremental push version we find in any of the child data centers.
      result.addAll(response.getIncrementalPushVersions());
    }
    return result;
  }

  protected static Map<String, Long> getOfflineJobProgress(String clusterName, String kafkaTopic, Map<String, ControllerClient> controllerClients){
    Map<String, Long> aggregateProgress = new HashMap<>();
    for (Map.Entry<String, ControllerClient> clientEntry : controllerClients.entrySet()){
      String childCluster = clientEntry.getKey();
      ControllerClient client = clientEntry.getValue();
      JobStatusQueryResponse statusResponse = client.queryJobStatus(kafkaTopic);
      if (statusResponse.isError()){
        logger.warn("Failed to query " + childCluster + " for job progress on topic " + kafkaTopic + ".  " + statusResponse.getError());
      } else {
        Map<String, Long> clusterProgress = statusResponse.getPerTaskProgress();
        for (Map.Entry<String, Long> entry: clusterProgress.entrySet()){
          aggregateProgress.put(childCluster + "_" + entry.getKey(), entry.getValue());
        }
      }
    }
    return aggregateProgress;
  }

  @Override
  public String getKafkaBootstrapServers(boolean isSSL) {
    return getVeniceHelixAdmin().getKafkaBootstrapServers(isSSL);
  }

  @Override
  public Pair<String, String> getNativeReplicationKafkaBootstrapServerAndZkAddress(String sourceFabric) {
    return getVeniceHelixAdmin().getNativeReplicationKafkaBootstrapServerAndZkAddress(sourceFabric);
  }

  @Override
  public String getNativeReplicationSourceFabric(String clusterName, Store store, Optional<String> sourceGridFabric, Optional<String> emergencySourceRegion) {
    return getVeniceHelixAdmin().getNativeReplicationSourceFabric(clusterName, store, sourceGridFabric, emergencySourceRegion);
  }

  @Override
  public boolean isSSLEnabledForPush(String clusterName, String storeName) {
    return getVeniceHelixAdmin().isSSLEnabledForPush(clusterName, storeName);
  }

  @Override
  public boolean isSslToKafka() {
    return getVeniceHelixAdmin().isSslToKafka();
  }

  @Override
  public TopicManager getTopicManager() {
    return getVeniceHelixAdmin().getTopicManager();
  }

  @Override
  public TopicManager getTopicManager(Pair<String, String> kafkaBootstrapServersAndZkAddress) {
    return getVeniceHelixAdmin().getTopicManager(kafkaBootstrapServersAndZkAddress);
  }

  @Override
  public boolean isLeaderControllerFor(String clusterName) {
    return getVeniceHelixAdmin().isLeaderControllerFor(clusterName);
  }

  @Override
  public int calculateNumberOfPartitions(String clusterName, String storeName, long storeSize) {
    return getVeniceHelixAdmin().calculateNumberOfPartitions(clusterName, storeName, storeSize);
  }

  @Override
  public int getReplicationFactor(String clusterName, String storeName) {
    return getVeniceHelixAdmin().getReplicationFactor(clusterName, storeName);
  }

  @Override
  public int getDatacenterCount(String clusterName){
    return getMultiClusterConfigs().getControllerConfig(clusterName).getChildDataCenterControllerUrlMap().size();
  }

  @Override
  public List<Replica> getReplicas(String clusterName, String kafkaTopic) {
    throw new VeniceException("getReplicas is not supported!");
  }

  @Override
  public List<Replica> getReplicasOfStorageNode(String clusterName, String instanceId) {
    throw new VeniceException("getReplicasOfStorageNode is not supported!");
  }

  @Override
  public NodeRemovableResult isInstanceRemovable(String clusterName, String instanceId, List<String> lockedNodes,
      boolean isFromInstanceView) {
    throw new VeniceException("isInstanceRemovable is not supported!");
  }

  @Override
  public Pair<NodeReplicasReadinessState, List<Replica>> nodeReplicaReadiness(String cluster, String helixNodeId) {
    throw new VeniceUnsupportedOperationException("nodeReplicaReadiness is not supported");
  }

  @Override
  public void initiateDataRecovery(String clusterName, String storeName, int version, String sourceFabric,
      String destinationFabric, boolean copyAllVersionConfigs, Optional<Version> ignored) {
    Map<String, ControllerClient> childControllerClientMap = veniceHelixAdmin.getControllerClientMap(clusterName);
    DataRecoveryManager.validateSourceAndDestinationFabrics(childControllerClientMap, sourceFabric, destinationFabric);
    StoreInfo storeInfo = childControllerClientMap.get(sourceFabric).getStore(storeName).getStore();
    Optional<Version> sourceVersion = storeInfo.getVersion(version);
    if (!sourceVersion.isPresent()) {
      throw new VeniceException("Version: " + version + " does not exist in the given source fabric: " + sourceFabric);
    }
    ControllerResponse destinationFabricResponse = childControllerClientMap.get(destinationFabric)
        .dataRecovery(sourceFabric, destinationFabric, storeName, version, true, copyAllVersionConfigs,
            sourceVersion);
    if (destinationFabricResponse.isError()) {
      throw new VeniceException("Failed to initiate data recovery in destination fabric, error: "
          + destinationFabricResponse.getError());
    }
  }

  @Override
  public void prepareDataRecovery(String clusterName, String storeName, int version, String sourceFabric,
      String destinationFabric, Optional<Integer> ignored) {
    Map<String, ControllerClient> childControllerClientMap = veniceHelixAdmin.getControllerClientMap(clusterName);
    DataRecoveryManager.validateSourceAndDestinationFabrics(childControllerClientMap, sourceFabric, destinationFabric);
    StoreInfo sourceStoreInfo = childControllerClientMap.get(sourceFabric).getStore(storeName).getStore();
    int amplificationFactor = sourceStoreInfo.getPartitionerConfig().getAmplificationFactor();
    ControllerResponse destinationFabricResponse = childControllerClientMap.get(destinationFabric)
        .prepareDataRecovery(sourceFabric, destinationFabric, storeName, version, Optional.of(amplificationFactor));
    if (destinationFabricResponse.isError()) {
      throw new VeniceException("Failed to prepare for data recovery in destination fabric, error: "
          + destinationFabricResponse.getError());
    }
  }

  @Override
  public Pair<Boolean, String> isStoreVersionReadyForDataRecovery(String clusterName, String storeName, int version,
      String sourceFabric, String destinationFabric, Optional<Integer> ignored) {
    Map<String, ControllerClient> childControllerClientMap = veniceHelixAdmin.getControllerClientMap(clusterName);
    try {
      DataRecoveryManager.validateSourceAndDestinationFabrics(childControllerClientMap, sourceFabric, destinationFabric);
      StoreInfo sourceStoreInfo = childControllerClientMap.get(sourceFabric).getStore(storeName).getStore();
      int amplificationFactor = sourceStoreInfo.getPartitionerConfig().getAmplificationFactor();
      ReadyForDataRecoveryResponse destinationFabricResponse = childControllerClientMap
          .get(destinationFabric).isStoreVersionReadyForDataRecovery(sourceFabric, destinationFabric, storeName,
              version, Optional.of(amplificationFactor));
      return new Pair<>(destinationFabricResponse.isReady(), destinationFabricResponse.getReason());
    } catch (Exception e) {
      return new Pair<>(false, e.getMessage());
    }
  }

  @Override
  public Instance getLeaderController(String clusterName) {
    return getVeniceHelixAdmin().getLeaderController(clusterName);
  }

  @Override
  public void addInstanceToAllowlist(String clusterName, String helixNodeId) {
    throw new VeniceException("addInstanceToAllowlist is not supported!");
  }

  @Override
  public void removeInstanceFromAllowList(String clusterName, String helixNodeId) {
    throw new VeniceException("removeInstanceFromAllowList is not supported!");
  }

  @Override
  public Set<String> getAllowlist(String clusterName) {
    throw new VeniceException("getAllowlist is not supported!");
  }

  @Override
  public void killOfflinePush(String clusterName, String kafkaTopic, boolean isForcedKill) {
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    if (getStore(clusterName, storeName) == null) {
      throw new VeniceNoStoreException(storeName, clusterName);
    }
    acquireAdminMessageLock(clusterName, storeName);
    try {
      getVeniceHelixAdmin().checkPreConditionForKillOfflinePush(clusterName, kafkaTopic);
      logger.info("Killing offline push job for topic: " + kafkaTopic + " in cluster: " + clusterName);
      /**
       * When parent controller wants to keep some errored topics, this function won't remove topic,
       * but relying on the next push to clean up this topic if it hasn't been removed by {@link #getOffLineJobStatus}.
       *
       * The reason is that every errored push will call this function.
       */
      if (0 == maxErroredTopicNumToKeep) {
        // Truncate Kafka topic
        logger.info("Truncating topic when kill offline push job, topic: " + kafkaTopic);
        truncateKafkaTopic(kafkaTopic);
        String correspondingStreamReprocessingTopic = Version.composeStreamReprocessingTopicFromVersionTopic(kafkaTopic);
        if (getTopicManager().containsTopic(correspondingStreamReprocessingTopic)) {
          truncateKafkaTopic(correspondingStreamReprocessingTopic);
        }
      }

      // TODO: Set parent controller's version status (to ERROR, most likely?)

      KillOfflinePushJob killJob = (KillOfflinePushJob) AdminMessageType.KILL_OFFLINE_PUSH_JOB.getNewInstance();
      killJob.clusterName = clusterName;
      killJob.kafkaTopic = kafkaTopic;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.KILL_OFFLINE_PUSH_JOB.getValue();
      message.payloadUnion = killJob;

      sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);
    } finally {
      releaseAdminMessageLock(clusterName);
    }
  }

  @Override
  public StorageNodeStatus getStorageNodesStatus(String clusterName, String instanceId) {
    throw new VeniceUnsupportedOperationException("getStorageNodesStatus");
  }

  @Override
  public boolean isStorageNodeNewerOrEqualTo(String clusterName, String instanceId,
                                             StorageNodeStatus oldServerStatus) {
    throw new VeniceUnsupportedOperationException("isStorageNodeNewerOrEqualTo");
  }

  public void setAdminConsumerService(String clusterName, AdminConsumerService service){
    getVeniceHelixAdmin().setAdminConsumerService(clusterName, service);
  }

  @Override
  public void skipAdminMessage(String clusterName, long offset, boolean skipDIV){
    getVeniceHelixAdmin().skipAdminMessage(clusterName, offset, skipDIV);
  }

  @Override
  public Long getLastSucceedExecutionId(String clustername) {
    return getVeniceHelixAdmin().getLastSucceedExecutionId(clustername);
  }

  protected Time getTimer() {
    return timer;
  }

  // Visible for testing
  protected void setTimer(Time timer) {
    this.timer = timer;
  }

  @Override
  public Optional<AdminCommandExecutionTracker> getAdminCommandExecutionTracker(String clusterName) {
    if(adminCommandExecutionTrackers.containsKey(clusterName)){
      return Optional.of(adminCommandExecutionTrackers.get(clusterName));
    }else{
      return Optional.empty();
    }
  }

  @Override
  public Map<String, Long> getAdminTopicMetadata(String clusterName, Optional<String> storeName) {
    throw new VeniceUnsupportedOperationException("getAdminTopicMetadata");
  }

  @Override
  public void updateAdminTopicMetadata(String clusterName, long executionId, Optional<String> storeName,
      Optional<Long> offset, Optional<Long> upstreamOffset) {
    throw new VeniceUnsupportedOperationException("updateAdminTopicMetadata");
  }

  @Override
  public RoutersClusterConfig getRoutersClusterConfig(String clusterName) {
    throw new VeniceUnsupportedOperationException("getRoutersClusterConfig");
  }

  @Override
  public void updateRoutersClusterConfig(String clusterName, Optional<Boolean> isThrottlingEnable,
      Optional<Boolean> isQuotaRebalancedEnable, Optional<Boolean> isMaxCapaictyProtectionEnabled,
      Optional<Integer> expectedRouterCount) {
    throw new VeniceUnsupportedOperationException("updateRoutersClusterConfig");
  }

  @Override
  public Map<String, String> getAllStorePushStrategyForMigration() {
    return pushStrategyZKAccessor.getAllPushStrategies();
  }

  @Override
  public void setStorePushStrategyForMigration(String voldemortStoreName, String strategy) {
    pushStrategyZKAccessor.setPushStrategy(voldemortStoreName, strategy);
  }

  @Override
  public Pair<String, String> discoverCluster(String storeName) {
    return getVeniceHelixAdmin().discoverCluster(storeName);
  }

  @Override
  public Map<String, String> findAllBootstrappingVersions(String clusterName) {
    throw new VeniceUnsupportedOperationException("findAllBootstrappingVersions");
  }

  public VeniceWriterFactory getVeniceWriterFactory() {
    return getVeniceHelixAdmin().getVeniceWriterFactory();
  }

  @Override
  public ControllerKafkaClientFactory getVeniceConsumerFactory() {
    return getVeniceHelixAdmin().getVeniceConsumerFactory();
  }

  @Override
  public synchronized void stop(String clusterName) {
    getVeniceHelixAdmin().stop(clusterName);
    // Close the admin producer for this cluster
    VeniceWriter<byte[], byte[], byte[]> veniceWriter = veniceWriterMap.get(clusterName);
    if (null != veniceWriter) {
      veniceWriter.close();
    }
    asyncSetupEnabledMap.put(clusterName, false);
  }

  @Override
  public void stopVeniceController() {
    getVeniceHelixAdmin().stopVeniceController();
  }

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
  }

  @Override
  public boolean isLeaderControllerOfControllerCluster() {
    return getVeniceHelixAdmin().isLeaderControllerOfControllerCluster();
  }

  @Override
  public boolean isTopicTruncated(String kafkaTopicName) {
    return getVeniceHelixAdmin().isTopicTruncated(kafkaTopicName);
  }

  @Override
  public boolean isTopicTruncatedBasedOnRetention(long retention) {
    return getVeniceHelixAdmin().isTopicTruncatedBasedOnRetention(retention);
  }

  @Override
  public int getMinNumberOfUnusedKafkaTopicsToPreserve() {
    return getVeniceHelixAdmin().getMinNumberOfUnusedKafkaTopicsToPreserve();
  }

  public boolean truncateKafkaTopic(String kafkaTopicName) {
    return getVeniceHelixAdmin().truncateKafkaTopic(kafkaTopicName);
  }

  @Override
  public boolean isResourceStillAlive(String resourceName) {
    throw new VeniceException("VeniceParentHelixAdmin#isResourceStillAlive is not supported!");
  }

  public ParentHelixOfflinePushAccessor getOfflinePushAccessor() {
    return offlinePushAccessor;
  }

  /* Used by test only*/
  protected void setOfflinePushAccessor(ParentHelixOfflinePushAccessor offlinePushAccessor) {
    this.offlinePushAccessor = offlinePushAccessor;
  }

  @Override
  public void updateClusterDiscovery(String storeName, String oldCluster, String newCluster, String initiatingCluster) {
    getVeniceHelixAdmin().updateClusterDiscovery(storeName, oldCluster, newCluster, initiatingCluster);
  }

  @Override
  public void sendPushJobDetails(PushJobStatusRecordKey key, PushJobDetails value) {
    getVeniceHelixAdmin().sendPushJobDetails(key, value);
  }

  @Override
  public PushJobDetails getPushJobDetails(PushJobStatusRecordKey key) {
    return getVeniceHelixAdmin().getPushJobDetails(key);
  }

  @Override
  public BatchJobHeartbeatValue getBatchJobHeartbeatValue(BatchJobHeartbeatKey batchJobHeartbeatKey) {
    return getVeniceHelixAdmin().getBatchJobHeartbeatValue(batchJobHeartbeatKey);
  }

  @Override
  public void writeEndOfPush(String clusterName, String storeName, int versionNumber, boolean alsoWriteStartOfPush) {
    getVeniceHelixAdmin().writeEndOfPush(clusterName, storeName, versionNumber, alsoWriteStartOfPush);
  }

  @Override
  public boolean whetherEnableBatchPushFromAdmin() {
    /**
     * Batch push to Parent Cluster is always enabled.
     */
    return true;
  }

  @Override
  public boolean isStoreMigrationAllowed(String clusterName) {
    return getVeniceHelixAdmin().isStoreMigrationAllowed(clusterName);
  }

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

  @Override
  public void completeMigration(String srcClusterName, String destClusterName, String storeName) {
    getVeniceHelixAdmin().updateClusterDiscovery(storeName, srcClusterName, destClusterName, srcClusterName);
  }

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

  /**
   * Check if etled proxy account is set before enabling any ETL and return a {@link ETLStoreConfigRecord}
   */
  private ETLStoreConfigRecord mergeNewSettingIntoOldETLStoreConfig(Store store,
                                                                    Optional<Boolean> regularVersionETLEnabled,
                                                                    Optional<Boolean> futureVersionETLEnabled,
                                                                    Optional<String> etledUserProxyAccount) {
    ETLStoreConfig etlStoreConfig = store.getEtlStoreConfig();
    /**
     * If etl enabled is true (either current version or future version), then account name must be specified in the command
     * and it's not empty, or the store metadata already contains a non-empty account name.
     */
    if (regularVersionETLEnabled.orElse(false) || futureVersionETLEnabled.orElse(false)) {
      if ((!etledUserProxyAccount.isPresent() || etledUserProxyAccount.get().isEmpty()) &&
          (etlStoreConfig.getEtledUserProxyAccount() == null || etlStoreConfig.getEtledUserProxyAccount().isEmpty())) {
        throw new VeniceException("Cannot enable ETL for this store because etled user proxy account is not set");
      }
    }
    ETLStoreConfigRecord etlStoreConfigRecord = new ETLStoreConfigRecord();
    etlStoreConfigRecord.etledUserProxyAccount = etledUserProxyAccount.orElse(etlStoreConfig.getEtledUserProxyAccount());
    etlStoreConfigRecord.regularVersionETLEnabled = regularVersionETLEnabled.orElse(etlStoreConfig.isRegularVersionETLEnabled());
    etlStoreConfigRecord.futureVersionETLEnabled = futureVersionETLEnabled.orElse(etlStoreConfig.isFutureVersionETLEnabled());
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
  private void provisionAclsForStore(String storeName, Optional<String> accessPermissions,
      List<VeniceSystemStoreType> enabledVeniceSystemStores) {
    //provision the ACL's needed to read/write venice store and kafka topic
    if (authorizerService.isPresent() && accessPermissions.isPresent()) {
      Resource resource = new Resource(storeName);
      Iterator<JsonNode> readPermissions = null;
      Iterator<JsonNode> writePermissions = null;
      ObjectMapper mapper = new ObjectMapper();
      try {
        JsonNode root = mapper.readTree(accessPermissions.get());
        JsonNode perms = root.path("AccessPermissions");
        if (perms.has("Read")) {
          readPermissions = perms.path("Read").getElements();
        }
        if (perms.has("Write")) {
          writePermissions = perms.path("Write").getElements();
        }
      } catch (Exception e) {
        logger.error("ACLProvisioning: invalid accessPermission schema for store:" + storeName, e);
        throw new VeniceException(e);
      }

      try {
        AclBinding aclBinding = new AclBinding(resource);
        if (readPermissions != null) {
          while (readPermissions.hasNext()) {
            String readPerm = readPermissions.next().getTextValue();
            Principal principal = new Principal(readPerm);
            AceEntry readAceEntry = new AceEntry(principal, Method.Read, Permission.ALLOW);
            aclBinding.addAceEntry(readAceEntry);
          }
        }
        if (writePermissions != null) {
          while (writePermissions.hasNext()) {
            String writePerm = writePermissions.next().getTextValue();
            Principal principal = new Principal(writePerm);
            AceEntry writeAceEntry = new AceEntry(principal, Method.Write, Permission.ALLOW);
            aclBinding.addAceEntry(writeAceEntry);
          }
        }
        authorizerService.get().setAcls(aclBinding);
        // Provision the ACL's needed to read/write corresponding venice system stores if any are specified.
        for (VeniceSystemStoreType veniceSystemStoreType : enabledVeniceSystemStores) {
          AclBinding systemStoreAclBinding = veniceSystemStoreType.generateSystemStoreAclBinding(aclBinding);
          authorizerService.get().setAcls(systemStoreAclBinding);
        }
      } catch (Exception e) {
        logger.error("ACLProvisioning: failure in setting ACL's for store:" + storeName, e);
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
        logger.error("ACLProvisioning: null ACL returned for store:" + storeName);
        return result;
      }

      //return empty string in case there is no ACL's present currently.
      if (aclBinding.countAceEntries() == 0) {
        return "";
      }

      JsonNodeFactory factory = JsonNodeFactory.instance;
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode root = factory.objectNode();
      ObjectNode perms = factory.objectNode();
      ArrayNode readP = factory.arrayNode();
      ArrayNode writeP = factory.arrayNode();
      for (AceEntry aceEntry : aclBinding.getAceEntries()) {
        if (aceEntry.getPermission() != Permission.ALLOW) {
          continue;
        }
        if (aceEntry.getMethod() == Method.Read) {
          readP.add(aceEntry.getPrincipal().getName());
        } else if (aceEntry.getMethod() == Method.Write) {
          writeP.add(aceEntry.getPrincipal().getName());
        }
      }
      perms.put("Read", readP);
      perms.put("Write", writeP);
      root.put("AccessPermissions", perms);
      result = mapper.writeValueAsString(root);
      return result;
    } catch (Exception e) {
      logger.error("ACLProvisioning: failure in getting ACL's for store:" + storeName, e);
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
        for (VeniceSystemStoreType veniceSystemStoreType : enabledVeniceSystemStores) {
          Resource systemStoreResource = new Resource(veniceSystemStoreType.getSystemStoreName(storeName));
          authorizerService.get().clearAcls(systemStoreResource);
          authorizerService.get().clearResource(systemStoreResource);
        }
      } catch (Exception e) {
        logger.error("ACLProvisioning: failure in deleting ACL's for store ", e);
        throw new VeniceException(e);
      }
    }
  }

  @Override
  public void updateAclForStore(String clusterName, String storeName, String accessPermissions) {
    HelixVeniceClusterResources resources = getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
      logger.info("ACLProvisioning: UpdateAcl:" + storeName + " in cluster: " + clusterName);
      if (!authorizerService.isPresent()) {
        throw new VeniceUnsupportedOperationException("updateAclForStore is not supported yet!");
      }
      Store store = getVeniceHelixAdmin().checkPreConditionForAclOp(clusterName, storeName);
      provisionAclsForStore(storeName, Optional.of(accessPermissions), VeniceSystemStoreType.getEnabledSystemStoreTypes(store));
    }
  }

  public void updateSystemStoreAclForStore(String clusterName, String regularStoreName, AclBinding systemStoreAclBinding) {
    HelixVeniceClusterResources resources = getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(regularStoreName)) {
      if (!authorizerService.isPresent()) {
        throw new VeniceUnsupportedOperationException("updateAclForStore is not supported yet!");
      }
      getVeniceHelixAdmin().checkPreConditionForAclOp(clusterName, regularStoreName);
      authorizerService.get().setAcls(systemStoreAclBinding);
    }
  }

  @Override
  public String getAclForStore(String clusterName, String storeName) {
    HelixVeniceClusterResources resources = getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreReadLock(storeName)) {
      logger.info("ACLProvisioning: GetAcl:" + storeName + " in cluster: " + clusterName);
      if (!authorizerService.isPresent()) {
        throw new VeniceUnsupportedOperationException("getAclForStore is not supported yet!");
      }
      getVeniceHelixAdmin().checkPreConditionForAclOp(clusterName, storeName);
      String accessPerms = fetchAclsForStore(storeName);
      return accessPerms;
    }
  }

  @Override
  public void deleteAclForStore(String clusterName, String storeName) {
    HelixVeniceClusterResources resources = getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName);
    try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
      logger.info("ACLProvisioning: DeleteAcl:" + storeName + " in cluster: " + clusterName);
      if (!authorizerService.isPresent()) {
        throw new VeniceUnsupportedOperationException("deleteAclForStore is not supported yet!");
      }
      Store store = getVeniceHelixAdmin().checkPreConditionForAclOp(clusterName, storeName);
      if (!store.isMigrating()) {
        cleanUpAclsForStore(storeName, VeniceSystemStoreType.getEnabledSystemStoreTypes(store));
      } else {
        logger.info("Store " + storeName + " is migrating! Skipping acl deletion!");
      }
    }
  }

  @Override
  public void configureNativeReplication(String clusterName, VeniceUserStoreType storeType, Optional<String> storeName,
      boolean enableNativeReplicationForCluster, Optional<String> newSourceRegion, Optional<String> regionsFilter) {
    acquireAdminMessageLock(clusterName, null);

    try {
      ConfigureNativeReplicationForCluster migrateClusterToNativeReplication
          = (ConfigureNativeReplicationForCluster) AdminMessageType.CONFIGURE_NATIVE_REPLICATION_FOR_CLUSTER.getNewInstance();
      migrateClusterToNativeReplication.clusterName = clusterName;
      migrateClusterToNativeReplication.storeType = storeType.toString();
      migrateClusterToNativeReplication.enabled = enableNativeReplicationForCluster;
      migrateClusterToNativeReplication.nativeReplicationSourceRegion = newSourceRegion.orElse(null);
      migrateClusterToNativeReplication.regionsFilter = regionsFilter.orElse(null);

      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.CONFIGURE_NATIVE_REPLICATION_FOR_CLUSTER.getValue();
      message.payloadUnion = migrateClusterToNativeReplication;
      sendAdminMessageAndWaitForConsumed(clusterName, null, message);
    } finally {
      releaseAdminMessageLock(clusterName);
    }
  }

  @Override
  public void configureActiveActiveReplication(String clusterName, VeniceUserStoreType storeType, Optional<String> storeName,
      boolean enableNativeReplicationForCluster, Optional<String> regionsFilter) {
    acquireAdminMessageLock(clusterName, null);

    try {
      ConfigureActiveActiveReplicationForCluster migrateClusterToActiveActiveReplication
          = (ConfigureActiveActiveReplicationForCluster) AdminMessageType.CONFIGURE_ACTIVE_ACTIVE_REPLICATION_FOR_CLUSTER.getNewInstance();
      migrateClusterToActiveActiveReplication.clusterName = clusterName;
      migrateClusterToActiveActiveReplication.storeType = storeType.toString();
      migrateClusterToActiveActiveReplication.enabled = enableNativeReplicationForCluster;
      migrateClusterToActiveActiveReplication.regionsFilter = regionsFilter.orElse(null);

      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.CONFIGURE_ACTIVE_ACTIVE_REPLICATION_FOR_CLUSTER.getValue();
      message.payloadUnion = migrateClusterToActiveActiveReplication;
      sendAdminMessageAndWaitForConsumed(clusterName, null, message);
    } finally {
      releaseAdminMessageLock(clusterName);
    }
  }

  @Override
  public void configureIncrementalPushForCluster(String clusterName, Optional<String> storeName,
      IncrementalPushPolicy incrementalPushPolicyToApply, Optional<IncrementalPushPolicy> incrementalPushPolicyToFilter,
      Optional<String> regionsFilter) {
    acquireAdminMessageLock(clusterName, null);

    try {
      ConfigureIncrementalPushForCluster incrementalPushBatchUpdateMessage
          = (ConfigureIncrementalPushForCluster) AdminMessageType.CONFIGURE_INCREMENTAL_PUSH_FOR_CLUSTER.getNewInstance();
      incrementalPushBatchUpdateMessage.clusterName = clusterName;
      incrementalPushBatchUpdateMessage.incrementalPushPolicyToApply = incrementalPushPolicyToApply.getValue();
      incrementalPushBatchUpdateMessage.incrementalPushPolicyToFilter =
          incrementalPushPolicyToFilter.isPresent() ? incrementalPushPolicyToFilter.get().getValue() : -1;
      incrementalPushBatchUpdateMessage.regionsFilter = regionsFilter.orElse(null);

      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.CONFIGURE_INCREMENTAL_PUSH_FOR_CLUSTER.getValue();
      message.payloadUnion = incrementalPushBatchUpdateMessage;
      sendAdminMessageAndWaitForConsumed(clusterName, null, message);
    } finally {
      releaseAdminMessageLock(clusterName);
    }
  }

  /**
   * This function will iterate over all of Helix Parent Admin's child controllers,
   * in order to ask about stale stores.
   */

  @Override
  public Map<String, StoreDataAudit> getClusterStaleStores(String clusterName, Optional<String> regionsFilter) {
    Map<String, StoreDataAudit> dataMap = new HashMap<>();
    Map<String, StoreDataAudit> retMap = new HashMap<>();
    try {
      Map<String, ControllerClient> childControllers = getVeniceHelixAdmin().getControllerClientMap(clusterName);

      //iterate through child controllers
      for (Map.Entry<String, ControllerClient> controller : childControllers.entrySet()) {
        MultiStoreInfoResponse response = controller.getValue().getClusterStores(clusterName); // get all stores from child
        response.getStoreInfoList().forEach((storeInfo) -> {
          dataMap.putIfAbsent(storeInfo.getName(), new StoreDataAudit());
          dataMap.get(storeInfo.getName()).setStoreName(storeInfo.getName());
          dataMap.get(storeInfo.getName()).insert(controller.getKey(), storeInfo); // StoreDataAudit.insert manages version, and healthy/stale region delineation
        });
      }
      //filter out
      for (Map.Entry<String, StoreDataAudit> store : dataMap.entrySet()) {
        StoreDataAudit audit = store.getValue();
        Optional<String> currentPushJobTopic = getTopicForCurrentPushJob(
            clusterName,
            store.getValue().getStoreName(),
            false,
            false
        );
        if (audit.getStaleRegions().size() > 0 && !currentPushJobTopic.isPresent())
          retMap.put(store.getKey(), audit);
      }
    } catch (Exception e) {
      throw new VeniceException("Something went wrong trying to fetch stale stores.", e);
    }
    return retMap;
  }

  @Override
  public int getStoreLargestUsedVersion(String clusterName, String storeName) {
    Map<String, ControllerClient> childControllers = getVeniceHelixAdmin().getControllerClientMap(clusterName);
    int aggregatedLargestUsedVersionNumber;
    if (hasStore(clusterName, storeName)) {
      aggregatedLargestUsedVersionNumber = getStore(clusterName, storeName).getLargestUsedVersionNumber();
    } else {
      aggregatedLargestUsedVersionNumber = getVeniceHelixAdmin().getStoreGraveyard().getLargestUsedVersionNumber(storeName);
    }
    for (Map.Entry<String, ControllerClient> controller : childControllers.entrySet()) {
      VersionResponse response = controller.getValue().getStoreLargestUsedVersion(clusterName, storeName);
      if (response.getVersion() > aggregatedLargestUsedVersionNumber) {
        aggregatedLargestUsedVersionNumber = response.getVersion();
      }
    }
    return aggregatedLargestUsedVersionNumber;
  }

  public List<StoreInfo> getClusterStores(String clusterName) { throw new UnsupportedOperationException("This function has no implementation."); }

  @Override
  public RegionPushDetails getRegionPushDetails(String clusterName, String storeName) { throw new UnsupportedOperationException("This function has no implementation."); }

  /**
   * This function will look for a single store, given a name and cluster name, and return information about the current
   * push jobs for that store across all regions.
   */

  @Override
  public Map<String, RegionPushDetails> listStorePushInfo(String clusterName, String storeName) {
    Map<String, RegionPushDetails> retMap = new HashMap<>();

    try {
      Map<String, ControllerClient> controllerClientMap = getVeniceHelixAdmin().getControllerClientMap(clusterName);
      for (Map.Entry<String, ControllerClient> entry : controllerClientMap.entrySet()) {
        RegionPushDetailsResponse details = entry.getValue().getRegionPushDetails(storeName, clusterName);
        if (details != null && details.getRegionPushDetails() != null) {
          retMap.put(entry.getKey(), details.getRegionPushDetails());
          retMap.get(entry.getKey()).setRegionName(entry.getKey());
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
      throw new VeniceException("Encountered the following error during re-creation check, please try to recreate"
          + " your store later: " + e.getMessage());
    }
  }

  @Override
  public boolean isParent() {
    return getVeniceHelixAdmin().isParent();
  }

  @Override
  public Map<String, String> getChildDataCenterControllerUrlMap(String clusterName) {
    return getVeniceHelixAdmin().getChildDataCenterControllerUrlMap(clusterName);
  }

  @Override
  public HelixReadOnlyStoreConfigRepository getStoreConfigRepo() {
    return getVeniceHelixAdmin().getStoreConfigRepo();
  }

  @Override
  public HelixReadOnlyZKSharedSystemStoreRepository getReadOnlyZKSharedSystemStoreRepository() {
    return getVeniceHelixAdmin().getReadOnlyZKSharedSystemStoreRepository();
  }

  @Override
  public HelixReadOnlyZKSharedSchemaRepository getReadOnlyZKSharedSchemaRepository() {
    return getVeniceHelixAdmin().getReadOnlyZKSharedSchemaRepository();
  }

  @Override
  public MetaStoreWriter getMetaStoreWriter() {
    return getVeniceHelixAdmin().getMetaStoreWriter();
  }

  @Override
  public Optional<PushStatusStoreRecordDeleter> getPushStatusStoreRecordDeleter() {
    return getVeniceHelixAdmin().getPushStatusStoreRecordDeleter();
  }

  @Override
  public Optional<String> getEmergencySourceRegion() {
    return getMultiClusterConfigs().getEmergencySourceRegion().equals("") ? Optional.empty() : Optional.of(getMultiClusterConfigs().getEmergencySourceRegion());
  }

  public List<String> getClustersLeaderOf() {
    return getVeniceHelixAdmin().getClustersLeaderOf();
  }

  // Function that can be overridden in tests
  protected VeniceHelixAdmin getVeniceHelixAdmin() {
    return veniceHelixAdmin;
  }

  private <T> Function<T, T> addToUpdatedConfigList(List<CharSequence> updatedConfigList, String config) {
    return (configValue) -> {
      updatedConfigList.add(config);
      return configValue;
    };
  }

  @Override
  public long getBackupVersionDefaultRetentionMs() {
    return getVeniceHelixAdmin().getBackupVersionDefaultRetentionMs();
  }

  @Override
  public void wipeCluster(String clusterName, String fabric, Optional<String> storeName, Optional<Integer> versionNum) {
    String childControllerUrl = multiClusterConfigs.getControllerConfig(clusterName).getChildControllerUrl(fabric);
    if (StringUtils.isEmpty(childControllerUrl)) {
      throw new VeniceException("child.cluster.url." + fabric + " is missing in parent controller.");
    }
    ControllerClient childControllerClient = ControllerClient.constructClusterControllerClient(clusterName,
        childControllerUrl, sslFactory);
    ControllerResponse response = childControllerClient.wipeCluster(fabric, storeName, versionNum);
    if (response.isError()) {
      throw new VeniceException("Could not wipe cluster " + clusterName + " in colo: " + fabric + ". " + response.getError());
    }
  }

  @Override
  public StoreComparisonInfo compareStore(String clusterName, String storeName, String fabricA, String fabricB)
      throws IOException {
    String childControllerUrlA = multiClusterConfigs.getControllerConfig(clusterName).getChildControllerUrl(fabricA);
    if (StringUtils.isEmpty(childControllerUrlA)) {
      throw new VeniceException("child.cluster.url." + fabricA + " is missing in parent controller.");
    }
    String childControllerUrlB = multiClusterConfigs.getControllerConfig(clusterName).getChildControllerUrl(fabricB);
    if (StringUtils.isEmpty(childControllerUrlB)) {
      throw new VeniceException("child.cluster.url." + fabricB + " is missing in parent controller.");
    }
    ControllerClient controllerClientA = ControllerClient.constructClusterControllerClient(clusterName,
        childControllerUrlA, sslFactory);
    ControllerClient controllerClientB = ControllerClient.constructClusterControllerClient(clusterName,
        childControllerUrlB, sslFactory);

    StoreComparisonInfo result = new StoreComparisonInfo();
    compareStoreProperties(storeName, fabricA, fabricB, controllerClientA, controllerClientB, result);
    compareStoreSchemas(storeName, fabricA, fabricB, controllerClientA, controllerClientB, result);
    compareStoreVersions(storeName, fabricA, fabricB, controllerClientA, controllerClientB, result);

    return result;
  }

  private static void compareStoreProperties(String storeName, String fabricA, String fabricB,
      ControllerClient controllerClientA, ControllerClient controllerClientB, StoreComparisonInfo result) {
    StoreInfo storeA = checkControllerResponse(controllerClientA.getStore(storeName), fabricA).getStore();
    StoreInfo storeB = checkControllerResponse(controllerClientB.getStore(storeName), fabricB).getStore();
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> storePropertiesA = mapper.convertValue(storeA, Map.class);
    Map<String, Object> storePropertiesB = mapper.convertValue(storeB, Map.class);

    for (Map.Entry<String, Object> entry : storePropertiesA.entrySet()) {
      // Filter out non-store-level configs
      if (entry.getKey().equals("coloToCurrentVersions") || entry.getKey().equals("versions") || entry.getKey().equals("kafkaBrokerUrl")) {
        continue;
      }
      if (!Objects.equals(entry.getValue(), storePropertiesB.get(entry.getKey()))) {
        result.addPropertyDiff(fabricA, fabricB, entry.getKey(), entry.getValue().toString(),
            storePropertiesB.get(entry.getKey()).toString());
      }
    }
  }

  private static void compareStoreSchemas(String storeName, String fabricA, String fabricB,
      ControllerClient controllerClientA, ControllerClient controllerClientB, StoreComparisonInfo result) {
    String keySchemaA = checkControllerResponse(controllerClientA.getKeySchema(storeName), fabricA).getSchemaStr();
    String keySchemaB = checkControllerResponse(controllerClientB.getKeySchema(storeName), fabricB).getSchemaStr();

    if (!Objects.equals(keySchemaA, keySchemaB)) {
      result.addSchemaDiff(fabricA, fabricB, "key-schema", keySchemaA, keySchemaB);
    }
    populateSchemaDiff(fabricA, fabricB,
        checkControllerResponse(controllerClientA.getAllValueAndDerivedSchema(storeName), fabricA).getSchemas(),
        checkControllerResponse(controllerClientB.getAllValueAndDerivedSchema(storeName), fabricB).getSchemas(),
        "derived-schema", result);
    populateSchemaDiff(fabricA, fabricB,
        checkControllerResponse(controllerClientA.getAllReplicationMetadataSchemas(storeName), fabricA).getSchemas(),
        checkControllerResponse(controllerClientB.getAllReplicationMetadataSchemas(storeName), fabricB).getSchemas(),
        "timestamp-metadata-schema", result);
  }

  private static void compareStoreVersions(String storeName, String fabricA, String fabricB,
      ControllerClient controllerClientA, ControllerClient controllerClientB, StoreComparisonInfo result) {
    StoreInfo storeA = checkControllerResponse(controllerClientA.getStore(storeName), fabricA).getStore();
    StoreInfo storeB = checkControllerResponse(controllerClientB.getStore(storeName), fabricB).getStore();
    List<Version> versionsB = storeB.getVersions();

    for (Version version : storeA.getVersions()) {
      int versionNum = version.getNumber();
      Optional<Version> versionB = storeB.getVersion(versionNum);
      if (versionB.isPresent()) {
        if (!version.getStatus().equals(versionB.get().getStatus())) {
          result.addVersionStateDiff(fabricA, fabricB, versionNum, version.getStatus(), versionB.get().getStatus());
        }
        versionsB.remove(versionB.get());
      } else {
        result.addVersionStateDiff(fabricA, fabricB, versionNum, version.getStatus(),
            VersionStatus.NOT_CREATED);
      }
    }
    for (Version version : versionsB) {
      result.addVersionStateDiff(fabricA, fabricB, version.getNumber(),
          VersionStatus.NOT_CREATED, version.getStatus());
    }
  }

  private static <T extends ControllerResponse> T checkControllerResponse(T controllerResponse, String fabric) {
    if (controllerResponse.isError()) {
      throw new VeniceException("ControllerResponse from fabric " + fabric + " has error " + controllerResponse);
    }
    return controllerResponse;
  }

  private static void populateSchemaDiff(String fabricA, String fabricB, MultiSchemaResponse.Schema[] schemasA,
      MultiSchemaResponse.Schema[] schemasB, String derivedSchemaName, StoreComparisonInfo storeComparisonInfo) {
    Map<String, String> schemaMapB = new HashMap<>();
    for (MultiSchemaResponse.Schema schema : schemasB) {
      String key = schema.getDerivedSchemaId() == -1 ?
          "value-schema-" + schema.getId() : derivedSchemaName + "-" + schema.getId() + "-" + schema.getDerivedSchemaId();
      schemaMapB.put(key, schema.getSchemaStr());
    }
    for (MultiSchemaResponse.Schema schema : schemasA) {
      String key = schema.getDerivedSchemaId() == -1 ?
          "value-schema-" + schema.getId() : derivedSchemaName + "-" + schema.getId() + "-" + schema.getDerivedSchemaId();
      if (schemaMapB.containsKey(key)) {
        if (!schema.getSchemaStr().equals(schemaMapB.get(key))) {
          storeComparisonInfo.addSchemaDiff(fabricA, fabricB, key, schema.getSchemaStr(), schemaMapB.get(key));
        }
        schemaMapB.remove(key);
      } else {
        storeComparisonInfo.addSchemaDiff(fabricA, fabricB, key, schema.getSchemaStr(), "N/A");
      }
    }
    for (Map.Entry<String, String> entry : schemaMapB.entrySet()) {
      storeComparisonInfo.addSchemaDiff(fabricA, fabricB, entry.getKey(), "N/A", entry.getValue());
    }
  }

  @Override
  public StoreInfo copyOverStoreSchemasAndConfigs(String clusterName, String srcFabric, String destFabric, String storeName) {
    String srcFabricChildControllerUrl = multiClusterConfigs.getControllerConfig(clusterName).getChildControllerUrl(srcFabric);
    if (StringUtils.isEmpty(srcFabricChildControllerUrl)) {
      throw new VeniceException("child.cluster.url." + srcFabric + " is missing in parent controller.");
    }
    String destFabricChildControllerUrl = multiClusterConfigs.getControllerConfig(clusterName).getChildControllerUrl(destFabric);
    if (StringUtils.isEmpty(destFabricChildControllerUrl)) {
      throw new VeniceException("child.cluster.url." + destFabric + " is missing in parent controller.");
    }

    try {
      ControllerClient srcFabricChildControllerClient =
          ControllerClient.constructClusterControllerClient(clusterName, srcFabricChildControllerUrl, sslFactory);
      ControllerClient destFabricChildControllerClient =
          ControllerClient.constructClusterControllerClient(clusterName, destFabricChildControllerUrl, sslFactory);

      long storeExecutionId;
      StoreInfo storeInfo;
      String keySchema;
      MultiSchemaResponse.Schema[] valueAndDerivedSchemas;
      // Acquire a lock to guarantee parent controller cannot send new admin messages for the store during metadata dump
      acquireAdminMessageLock(clusterName, storeName);
      try {
        // Src fabric local controller dumps out the store's execution id, configs and schemas
        storeExecutionId = srcFabricChildControllerClient.getAdminTopicMetadata(Optional.of(storeName)).getExecutionId();
        storeInfo = srcFabricChildControllerClient.getStore(storeName).getStore();
        keySchema = srcFabricChildControllerClient.getKeySchema(storeName).getSchemaStr();
        valueAndDerivedSchemas = srcFabricChildControllerClient.getAllValueAndDerivedSchema(storeName).getSchemas();
      } catch (Exception e) {
        throw new VeniceException("Error when getting store " + storeName + " metadata from source fabric " + srcFabric
            + " Exception: " + e.getMessage());
      } finally {
        releaseAdminMessageLock(clusterName);
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
      destFabricChildControllerClient.createNewStore(storeInfo.getName(), storeInfo.getOwner(), keySchema,
          valueAndDerivedSchemas[0].getSchemaStr());
      for (int i = 1; i < valueAndDerivedSchemas.length; i++) {
        MultiSchemaResponse.Schema schema = valueAndDerivedSchemas[i];
        if (schema.getDerivedSchemaId() == -1) {
          destFabricChildControllerClient.addValueSchema(storeInfo.getName(), schema.getSchemaStr(), schema.getId());
        } else {
          destFabricChildControllerClient.addDerivedSchema(storeInfo.getName(), schema.getId(), schema.getSchemaStr(),
              schema.getDerivedSchemaId());
        }
      }
      UpdateStoreQueryParams params = new UpdateStoreQueryParams(storeInfo, false);
      ControllerResponse response = destFabricChildControllerClient.updateStore(storeInfo.getName(), params);
      if (response.isError()) {
        throw new VeniceException("Failed to update store " + response.getError());
      }

      response = destFabricChildControllerClient.updateAdminTopicMetadata(storeExecutionId, Optional.of(storeName),
          Optional.empty(), Optional.empty());
      if (response.isError()) {
        throw new VeniceException("Failed to update store's execution id " + response.getError());
      }

      return storeInfo;
    } catch (Exception e) {
      throw new VeniceException("Error copying src fabric's metadata to dest fabric.", e.getCause());
    }
  }

  // Allow overriding in tests
  protected LingeringStoreVersionChecker getLingeringStoreVersionChecker() {
    return lingeringStoreVersionChecker;
  }

  protected VeniceControllerMultiClusterConfig getMultiClusterConfigs() {
    return multiClusterConfigs;
  }

  protected UserSystemStoreLifeCycleHelper getSystemStoreLifeCycleHelper() {
    return systemStoreLifeCycleHelper;
  }
}
