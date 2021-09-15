package com.linkedin.venice.controller;

import com.linkedin.avroutil1.compatibility.AvroIncompatibleSchemaException;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.VeniceStateModel;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.exception.HelixClusterMaintenanceModeException;
import com.linkedin.venice.controller.helix.SharedHelixReadOnlyZKSharedSchemaRepository;
import com.linkedin.venice.controller.helix.SharedHelixReadOnlyZKSharedSystemStoreRepository;
import com.linkedin.venice.controller.init.ClusterLeaderInitializationManager;
import com.linkedin.venice.controller.init.ClusterLeaderInitializationRoutine;
import com.linkedin.venice.controller.init.LatestVersionPromoteToCurrentTimestampCorrectionRoutine;
import com.linkedin.venice.controller.init.SystemSchemaInitializationRoutine;
import com.linkedin.venice.controller.kafka.StoreStatusDecider;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumerService;
import com.linkedin.venice.controller.kafka.consumer.VeniceControllerConsumerFactory;
import com.linkedin.venice.controller.stats.VeniceHelixAdminStats;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.exceptions.InvalidVeniceSchemaException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.exceptions.VeniceNoClusterException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.exceptions.VeniceRetriableException;
import com.linkedin.venice.exceptions.VeniceStoreAlreadyExistsException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSystemStoreRepository;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.helix.HelixStoreGraveyard;
import com.linkedin.venice.helix.Replica;
import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.helix.ZkWhitelistAccessor;
import com.linkedin.venice.kafka.TopicDoesNotExistException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.TopicManagerRepository;
import com.linkedin.venice.kafka.VeniceOperationAgainstKafkaTimedOut;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.ETLStoreConfig;
import com.linkedin.venice.meta.ETLStoreConfigImpl;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.IncrementalPushPolicy;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.InstanceStatus;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadWriteSchemaRepository;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RoutersClusterConfig;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.StoreGraveyard;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.VeniceUserStoreType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.participant.protocol.KillPushJob;
import com.linkedin.venice.participant.protocol.ParticipantMessageKey;
import com.linkedin.venice.participant.protocol.ParticipantMessageValue;
import com.linkedin.venice.participant.protocol.enums.ParticipantMessageType;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.KillOfflinePushMessage;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.pushmonitor.PartitionStatus;
import com.linkedin.venice.pushmonitor.PushMonitor;
import com.linkedin.venice.pushmonitor.PushMonitorDelegator;
import com.linkedin.venice.pushmonitor.PushStatusDecider;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.pushstatushelper.PushStatusStoreRecordDeleter;
import com.linkedin.venice.replication.LeaderStorageNodeReplicator;
import com.linkedin.venice.replication.TopicReplicator;
import com.linkedin.venice.schema.DerivedSchemaEntry;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.ReplicationMetadataSchemaEntry;
import com.linkedin.venice.schema.ReplicationMetadataVersionId;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.stats.AbstractVeniceAggStats;
import com.linkedin.venice.stats.ZkClientStatusStats;
import com.linkedin.venice.status.StatusMessageChannel;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatKey;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatValue;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.EncodingUtils;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.RandomAvroObjectGenerator;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import io.tehuti.metrics.MetricsRepository;
import java.nio.ByteBuffer;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.strategy.AutoRebalanceStrategy;
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;

import static com.linkedin.venice.meta.HybridStoreConfigImpl.*;
import static com.linkedin.venice.meta.Version.*;
import static com.linkedin.venice.meta.VersionStatus.*;
import static com.linkedin.venice.utils.AvroSchemaUtils.*;


/**
 * Helix Admin based on 0.8.4.215 APIs.
 *
 * <p>
 * After using controller as service mode. There are two levels of cluster and controllers. Each venice controller will
 * hold a level1 helix controller which will keep connecting to Helix, there is a cluster only used for all of these
 * level1 controllers(controller's cluster). The second level is our venice clusters. Like prod cluster, dev cluster
 * etc. Each of cluster will be Helix resource in the controller's cluster. Helix will choose one of level1 controller
 * becoming the master of our venice cluster. In our distributed controllers state transition handler, a level2 controller
 * will be initialized to manage this venice cluster only. If this level1 controller is chosen as the master controller
 * of multiple Venice clusters, multiple level2 controller will be created based on cluster specific config.
 * <p>
 * Admin is shared by multiple cluster's controllers running in one physical Venice controller instance.
 */
public class VeniceHelixAdmin implements Admin, StoreCleaner {

    public static final List<ExecutionStatus> STATUS_PRIORITIES = Arrays.asList(
        ExecutionStatus.PROGRESS,
        ExecutionStatus.STARTED,
        ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED,
        ExecutionStatus.UNKNOWN,
        ExecutionStatus.NEW,
        ExecutionStatus.NOT_CREATED,
        ExecutionStatus.END_OF_PUSH_RECEIVED,
        ExecutionStatus.ERROR,
        ExecutionStatus.WARNING,
        ExecutionStatus.COMPLETED,
        ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED,
        ExecutionStatus.ARCHIVED);

    private static final Logger logger = Logger.getLogger(VeniceHelixAdmin.class);
    private static final int RECORD_COUNT = 10;
    private static final String REGION_FILTER_LIST_SEPARATOR = ",\\s*";

    private final VeniceControllerMultiClusterConfig multiClusterConfigs;
    private final String controllerClusterName;
    private final int controllerClusterReplica;
    private final String controllerName;
    private final String kafkaBootstrapServers;
    private final String kafkaSSLBootstrapServers;
    private final Map<String, AdminConsumerService> adminConsumerServices = new ConcurrentHashMap<>();

    public static final int CONTROLLER_CLUSTER_NUMBER_OF_PARTITION = 1;
    public static final long CONTROLLER_CLUSTER_RESOURCE_EV_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(5);
    public static final long CONTROLLER_CLUSTER_RESOURCE_EV_CHECK_DELAY_MS = 500;
    public static final long HELIX_RESOURCE_ASSIGNMENT_RETRY_INTERVAL_MS = 500;
    public static final long HELIX_RESOURCE_ASSIGNMENT_LOG_INTERVAL_MS = TimeUnit.MINUTES.toMillis(1);

    private static final int INTERNAL_STORE_GET_RRT_TOPIC_ATTEMPTS = 3;
    private static final long INTERNAL_STORE_RTT_RETRY_BACKOFF_MS = TimeUnit.SECONDS.toMillis(5);
    private static final int PARTICIPANT_MESSAGE_STORE_SCHEMA_ID = 1;

    // TODO remove this field and all invocations once we are fully on HaaS. Use the helixAdminClient instead.
    private final HelixAdmin admin;
    /**
     * Client/wrapper used for performing Helix operations in Venice.
     */
    private final HelixAdminClient helixAdminClient;
    private TopicManagerRepository topicManagerRepository;
    private final ZkClient zkClient;
    private final HelixAdapterSerializer adapterSerializer;
    private final ZkWhitelistAccessor whitelistAccessor;
    private final ExecutionIdAccessor executionIdAccessor;
    private final Optional<TopicReplicator> onlineOfflineTopicReplicator;
    private final Optional<TopicReplicator> leaderFollowerTopicReplicator;
    private final long deprecatedJobTopicRetentionMs;
    private final long deprecatedJobTopicMaxRetentionMs;
    private final HelixReadOnlyStoreConfigRepository storeConfigRepo;
    private final VeniceWriterFactory veniceWriterFactory;
    private final VeniceControllerConsumerFactory veniceConsumerFactory;
    private final int minNumberOfUnusedKafkaTopicsToPreserve;
    private final int minNumberOfStoreVersionsToPreserve;
    private final StoreGraveyard storeGraveyard;
    private final Map<String, String> participantMessageStoreRTTMap;
    private final Map<String, VeniceWriter> participantMessageWriterMap;
    private final VeniceHelixAdminStats veniceHelixAdminStats;
    private final boolean isControllerClusterHAAS;
    private final String coloMasterClusterName;
    private final Optional<SSLFactory> sslFactory;
    private final String pushJobStatusStoreClusterName;
    private final MetadataStoreWriter metadataStoreWriter;
    private final Optional<PushStatusStoreReader> pushStatusStoreReader;
    private final Optional<PushStatusStoreRecordDeleter> pushStatusStoreDeleter;
    private final SharedHelixReadOnlyZKSharedSystemStoreRepository zkSharedSystemStoreRepository;
    private final SharedHelixReadOnlyZKSharedSchemaRepository zkSharedSchemaRepository;
    private final MetaStoreWriter metaStoreWriter;
    private final D2Client d2Client;
    private AvroSpecificStoreClient<PushJobStatusRecordKey, PushJobDetails> pushJobDetailsStoreClient;

    /**
     * Level-1 controller, it always being connected to Helix. And will create sub-controller for specific cluster when
     * getting notification from Helix.
     */
    private SafeHelixManager manager;
    /**
     * Builder used to build the data path to access Helix internal data of level-1 cluster.
     */
    private PropertyKey.Builder level1KeyBuilder;

    private String pushJobDetailsRTTopic;

    // Those variables will be initialized lazily.
    private int pushJobDetailsSchemaId = -1;

    private static final String PUSH_JOB_DETAILS_WRITER = "PUSH_JOB_DETAILS_WRITER";
    private static final String SYSTEM_STORE_PUSH_JOB_ID = "system_store_push_job";
    private final Map<String, VeniceWriter> jobTrackingVeniceWriterMap = new VeniceConcurrentHashMap<>();

    // This map stores the time when topics were created. It only contains topics whose information has not yet been persisted to Zk.
    private final Map<String, Long> topicToCreationTime = new VeniceConcurrentHashMap<>();

    /**
     * Controller Client Map per cluster per colo
     */
    private final Map<String, Map<String, ControllerClient>> clusterControllerClientPerColoMap = new VeniceConcurrentHashMap<>();

    private VeniceDistClusterControllerStateModelFactory controllerStateModelFactory;

    private long backupVersionDefaultRetentionMs;

    public VeniceHelixAdmin(
        VeniceControllerMultiClusterConfig multiClusterConfigs,
        MetricsRepository metricsRepository,
        D2Client d2Client
    ) {
        this(multiClusterConfigs, metricsRepository, false, d2Client, Optional.empty(), Optional.empty());
    }

    //TODO Use different configs for different clusters when creating helix admin.
    public VeniceHelixAdmin(
        VeniceControllerMultiClusterConfig multiClusterConfigs,
        MetricsRepository metricsRepository,
        boolean sslEnabled,
        D2Client d2Client,
        Optional<SSLConfig> sslConfig,
        Optional<DynamicAccessController> accessController
    ) {
        this.multiClusterConfigs = multiClusterConfigs;
        VeniceControllerConfig commonConfig = multiClusterConfigs.getCommonConfig();
        this.controllerName = Utils.getHelixNodeIdentifier(multiClusterConfigs.getAdminPort());
        this.controllerClusterName = multiClusterConfigs.getControllerClusterName();
        this.controllerClusterReplica = multiClusterConfigs.getControllerClusterReplica();
        this.kafkaBootstrapServers = multiClusterConfigs.getKafkaBootstrapServers();
        this.kafkaSSLBootstrapServers = multiClusterConfigs.getSslKafkaBootstrapServers();
        this.deprecatedJobTopicRetentionMs = multiClusterConfigs.getDeprecatedJobTopicRetentionMs();
        this.deprecatedJobTopicMaxRetentionMs = multiClusterConfigs.getDeprecatedJobTopicMaxRetentionMs();
        this.backupVersionDefaultRetentionMs = multiClusterConfigs.getBackupVersionDefaultRetentionMs();

        this.minNumberOfUnusedKafkaTopicsToPreserve = multiClusterConfigs.getMinNumberOfUnusedKafkaTopicsToPreserve();
        this.minNumberOfStoreVersionsToPreserve = multiClusterConfigs.getMinNumberOfStoreVersionsToPreserve();
        this.d2Client = Utils.notNull(d2Client, "D2 client cannot be null.");

        if (sslEnabled) {
            try {
                String sslFactoryClassName = multiClusterConfigs.getSslFactoryClassName();
                Properties sslProperties = sslConfig.get().getSslProperties();
                sslFactory = Optional.of(SslUtils.getSSLFactory(sslProperties, sslFactoryClassName));
            } catch (Exception e) {
                logger.error("Failed to create SSL engine", e);
                throw new VeniceException(e);
            }
        } else {
            sslFactory = Optional.empty();
        }

        // TODO: Consider re-using the same zkClient for the ZKHelixAdmin and TopicManager.
        ZkClient zkClientForHelixAdmin = ZkClientFactory.newZkClient(multiClusterConfigs.getZkAddress());
        zkClientForHelixAdmin.subscribeStateChanges(new ZkClientStatusStats(metricsRepository, "controller-zk-client-for-helix-admin"));
        /**
         * N.B.: The following setup steps are necessary when using the {@link ZKHelixAdmin} constructor which takes
         * in an external {@link ZkClient}.
         *
         * {@link ZkClient#setZkSerializer(ZkSerializer)} is necessary, otherwise Helix will throw:
         *
         * org.apache.helix.zookeeper.zkclient.exception.ZkMarshallingError: java.io.NotSerializableException: org.apache.helix.ZNRecord
         */
        zkClientForHelixAdmin.setZkSerializer(new ZNRecordSerializer());
        if (!zkClientForHelixAdmin.waitUntilConnected(ZkClient.DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)) {
            throw new VeniceException("Failed to connect to ZK within " + ZkClient.DEFAULT_CONNECTION_TIMEOUT + " ms!");
        }
        this.admin = new ZKHelixAdmin(zkClientForHelixAdmin);
        helixAdminClient = new ZkHelixAdminClient(multiClusterConfigs, metricsRepository);
        //There is no way to get the internal zkClient from HelixManager or HelixAdmin. So create a new one here.
        this.zkClient = ZkClientFactory.newZkClient(multiClusterConfigs.getZkAddress());
        this.zkClient.subscribeStateChanges(new ZkClientStatusStats(metricsRepository, "controller-zk-client"));
        this.adapterSerializer = new HelixAdapterSerializer();
        veniceConsumerFactory = new VeniceControllerConsumerFactory(commonConfig);

        this.topicManagerRepository = new TopicManagerRepository(getKafkaBootstrapServers(isSslToKafka()),
                                                                 multiClusterConfigs.getKafkaZkAddress(),
                                                                 multiClusterConfigs.getTopicManagerKafkaOperationTimeOutMs(),
                                                                 multiClusterConfigs.getTopicDeletionStatusPollIntervalMs(),
                                                                 multiClusterConfigs.getKafkaMinLogCompactionLagInMs(),
                                                                 veniceConsumerFactory,
                                                                 metricsRepository);
        this.whitelistAccessor = new ZkWhitelistAccessor(zkClient, adapterSerializer);
        this.executionIdAccessor = new ZkExecutionIdAccessor(zkClient, adapterSerializer);
        this.storeConfigRepo = new HelixReadOnlyStoreConfigRepository(zkClient, adapterSerializer,
            commonConfig.getRefreshAttemptsForZkReconnect(), commonConfig.getRefreshIntervalForZkReconnectInMs());
        storeConfigRepo.refresh();
        this.storeGraveyard = new HelixStoreGraveyard(zkClient, adapterSerializer, multiClusterConfigs.getClusters());
        veniceWriterFactory = new VeniceWriterFactory(commonConfig.getProps().toProperties());
        this.onlineOfflineTopicReplicator =
            TopicReplicator.getTopicReplicator(topicManagerRepository.getTopicManager(), commonConfig.getProps(), veniceWriterFactory);
        this.leaderFollowerTopicReplicator = TopicReplicator.getTopicReplicator(
            LeaderStorageNodeReplicator.class.getName(), topicManagerRepository.getTopicManager(), commonConfig.getProps(), veniceWriterFactory);
        this.participantMessageStoreRTTMap = new VeniceConcurrentHashMap<>();
        this.participantMessageWriterMap = new VeniceConcurrentHashMap<>();
        this.veniceHelixAdminStats = new VeniceHelixAdminStats(metricsRepository, "venice_helix_admin");
        isControllerClusterHAAS = commonConfig.isControllerClusterLeaderHAAS();
        coloMasterClusterName = commonConfig.getClusterName();
        pushJobStatusStoreClusterName = commonConfig.getPushJobStatusStoreClusterName();
        metadataStoreWriter = new MetadataStoreWriter(topicManagerRepository.getTopicManager(), veniceWriterFactory, this);
        if (commonConfig.isDaVinciPushStatusStoreEnabled()) {
            pushStatusStoreReader = Optional.of(
                new PushStatusStoreReader(d2Client, commonConfig.getPushStatusStoreHeartbeatExpirationTimeInSeconds())
            );
            pushStatusStoreDeleter = Optional.of(new PushStatusStoreRecordDeleter(getVeniceWriterFactory()));
        } else {
            pushStatusStoreReader = Optional.empty();
            pushStatusStoreDeleter = Optional.empty();
        }

        zkSharedSystemStoreRepository = new SharedHelixReadOnlyZKSharedSystemStoreRepository(
            zkClient, adapterSerializer, commonConfig.getSystemSchemaClusterName());
        zkSharedSchemaRepository = new SharedHelixReadOnlyZKSharedSchemaRepository(
            zkSharedSystemStoreRepository, zkClient, adapterSerializer, commonConfig.getSystemSchemaClusterName(),
            commonConfig.getRefreshAttemptsForZkReconnect(), commonConfig.getRefreshIntervalForZkReconnectInMs());
        metaStoreWriter = new MetaStoreWriter(topicManagerRepository.getTopicManager(), veniceWriterFactory, zkSharedSchemaRepository);

        List<ClusterLeaderInitializationRoutine> initRoutines = new ArrayList<>();
        initRoutines.add(new SystemSchemaInitializationRoutine(
            AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE, multiClusterConfigs, this));
        initRoutines.add(new SystemSchemaInitializationRoutine(
            AvroProtocolDefinition.PARTITION_STATE, multiClusterConfigs, this));
        initRoutines.add(new SystemSchemaInitializationRoutine(
            AvroProtocolDefinition.STORE_VERSION_STATE, multiClusterConfigs, this));
        initRoutines.add(new SystemSchemaInitializationRoutine(
                AvroProtocolDefinition.BATCH_JOB_HEARTBEAT,
                multiClusterConfigs,
                this,
                Optional.of(BatchJobHeartbeatKey.SCHEMA$),
                Optional.of(new UpdateStoreQueryParams()
                        .setHybridStoreDiskQuotaEnabled(false)
                        .setHybridTimeLagThreshold(TimeUnit.HOURS.toSeconds(1))
                        .setHybridRewindSeconds(TimeUnit.HOURS.toSeconds(1))
                        .setHybridOffsetLagThreshold(1000L)
                        .setPartitionCount(16)
                        .setLeaderFollowerModel(true)),
                false)
        );
        if (multiClusterConfigs.isZkSharedMetadataSystemSchemaStoreAutoCreationEnabled()) {
            // Add routine to create zk shared metadata system store
            UpdateStoreQueryParams metadataSystemStoreUpdate = new UpdateStoreQueryParams().setHybridRewindSeconds(TimeUnit.DAYS.toSeconds(1)) // 1 day rewind
                .setHybridOffsetLagThreshold(1).setHybridTimeLagThreshold(TimeUnit.MINUTES.toSeconds(1)) // 1 mins
                .setLeaderFollowerModel(true).setWriteComputationEnabled(true)
                .setPartitionCount(1);
            initRoutines.add(new SystemSchemaInitializationRoutine(AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE,
                multiClusterConfigs, this, Optional.of(AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE_KEY.getCurrentProtocolVersionSchema()),
                Optional.of(metadataSystemStoreUpdate), true));
        }
        if (multiClusterConfigs.isZkSharedDaVinciPushStatusSystemSchemaStoreAutoCreationEnabled()) {
            // Add routine to create zk shared da vinci push status system store
            UpdateStoreQueryParams daVinciPushStatusSystemStoreUpdate = new UpdateStoreQueryParams().setHybridRewindSeconds(TimeUnit.DAYS.toSeconds(1)) // 1 day rewind
                .setHybridOffsetLagThreshold(1).setHybridTimeLagThreshold(TimeUnit.MINUTES.toSeconds(1)) // 1 mins
                .setLeaderFollowerModel(true).setWriteComputationEnabled(true)
                .setPartitionCount(1);
            initRoutines.add(new SystemSchemaInitializationRoutine(AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE,
                multiClusterConfigs, this, Optional.of(AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE_KEY.getCurrentProtocolVersionSchema()),
                Optional.of(daVinciPushStatusSystemStoreUpdate), true));
        }

        // TODO: Remove this latest version promote to current timestamp update logic in the future.
        initRoutines.add(new LatestVersionPromoteToCurrentTimestampCorrectionRoutine(this));
        ClusterLeaderInitializationRoutine controllerInitialization = new ClusterLeaderInitializationManager(initRoutines);

        // Create the controller cluster if required.
        if (isControllerClusterHAAS) {
            if (!helixAdminClient.isVeniceControllerClusterCreated(controllerClusterName)) {
                helixAdminClient.createVeniceControllerCluster(controllerClusterName);
            } else if (!helixAdminClient.isClusterInGrandCluster(controllerClusterName)) {
                // This is possible when transitioning to HaaS for the first time or if the grand cluster is reset.
                helixAdminClient.addClusterToGrandCluster(controllerClusterName);
            }
        } else {
            createControllerClusterIfRequired();
        }
        controllerStateModelFactory = new VeniceDistClusterControllerStateModelFactory(
            zkClient, adapterSerializer, this, multiClusterConfigs, metricsRepository, controllerInitialization,
            onlineOfflineTopicReplicator, leaderFollowerTopicReplicator, accessController, metadataStoreWriter,
            helixAdminClient);
        // Initialized the helix manger for the level1 controller. If the controller cluster leader is going to be in
        // HaaS then level1 controllers should be only in participant mode.
        initLevel1Controller(isControllerClusterHAAS);
    }

    private void initLevel1Controller(boolean isParticipantOnly) {
        InstanceType instanceType = isParticipantOnly ? InstanceType.PARTICIPANT : InstanceType.CONTROLLER_PARTICIPANT;
        manager = new SafeHelixManager(HelixManagerFactory
            .getZKHelixManager(controllerClusterName, controllerName, instanceType,
                multiClusterConfigs.getControllerClusterZkAddress()));
        StateMachineEngine stateMachine = manager.getStateMachineEngine();
        stateMachine.registerStateModelFactory(LeaderStandbySMD.name, controllerStateModelFactory);
        try {
            manager.connect();
        } catch (Exception ex) {
            String errorMessage =
                " Error starting Helix controller cluster " + controllerClusterName + " controller " + controllerName;
            logger.error(errorMessage, ex);
            throw new VeniceException(errorMessage, ex);
        }
        level1KeyBuilder = new PropertyKey.Builder(manager.getClusterName());
    }

    public ZkClient getZkClient() {
        return zkClient;
    }

    public ExecutionIdAccessor getExecutionIdAccessor() {
        return executionIdAccessor;
    }

    public HelixAdapterSerializer getAdapterSerializer() {
        return adapterSerializer;
    }

    // For testing purpose.
    void setTopicManagerRepository(TopicManagerRepository topicManagerRepository) {
        this.topicManagerRepository = topicManagerRepository;
    }

    @Override
    public synchronized void initVeniceControllerClusterResource(String clusterName) {
        //Simply validate cluster name here.
        clusterName = clusterName.trim();
        if (clusterName.startsWith("/") || clusterName.endsWith("/") || clusterName.indexOf(' ') >= 0) {
            throw new IllegalArgumentException("Invalid cluster name:" + clusterName);
        }
        if (multiClusterConfigs.getControllerConfig(clusterName).isVeniceClusterLeaderHAAS()) {
            setupStorageClusterAsNeeded(clusterName);
        } else {
            createClusterIfRequired(clusterName);
        }
        // The resource and partition may be disabled for this controller before, we need to enable again at first. Then the state transition will be triggered.
        List<String> partitionNames = Collections.singletonList(VeniceControllerStateModel.getPartitionNameFromVeniceClusterName(clusterName));
        helixAdminClient.enablePartition(true, controllerClusterName, controllerName, clusterName, partitionNames);
        if (multiClusterConfigs.getControllerConfig(clusterName).isParticipantMessageStoreEnabled()) {
            participantMessageStoreRTTMap.put(clusterName,
                Version.composeRealTimeTopic(VeniceSystemStoreUtils.getParticipantStoreNameForCluster(clusterName)));
        }
        waitUntilClusterResourceIsVisibleInEV(clusterName);
    }

    private void waitUntilClusterResourceIsVisibleInEV(String clusterName) {
        long startTime = System.currentTimeMillis();
        PropertyKey.Builder keyBuilder = new PropertyKey.Builder(controllerClusterName);
        while (System.currentTimeMillis() - startTime < CONTROLLER_CLUSTER_RESOURCE_EV_TIMEOUT_MS) {
            ExternalView externalView =
                manager.getHelixDataAccessor().getProperty(keyBuilder.externalView(clusterName));
            String partitionName = HelixUtils.getPartitionName(clusterName, 0);
            if (externalView != null && externalView.getStateMap(partitionName) != null
                && !externalView.getStateMap(partitionName).isEmpty()) {
                logger.info("External view is available for cluster resource: " + clusterName);
                return;
            }
            try {
                // Wait
                Thread.sleep(CONTROLLER_CLUSTER_RESOURCE_EV_CHECK_DELAY_MS);
            } catch (InterruptedException e) {
                throw new VeniceException("Failed to verify the external view of cluster resource: " + clusterName, e);
            }
        }
        throw new VeniceException("Timed out when waiting for the external view of cluster resource: " + clusterName);
    }

    private boolean isResourceStillAlive(String clusterName, String resourceName) {
        String externalViewPath = "/" + clusterName + "/EXTERNALVIEW/" + resourceName;
        return zkClient.exists(externalViewPath);
    }

    List<String> getAllLiveHelixResources(String clusterName) {
        return zkClient.getChildren("/" + clusterName + "/EXTERNALVIEW");
    }

    @Override
    public boolean isResourceStillAlive(String resourceName) {
        if (!Version.isVersionTopicOrStreamReprocessingTopic(resourceName)) {
            throw new VeniceException("Resource name: " + resourceName + " is invalid");
        }
        String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
        // Find out the cluster first
        Optional<StoreConfig> storeConfig = storeConfigRepo.getStoreConfig(storeName);
        if (!storeConfig.isPresent()) {
            if (VeniceSystemStoreUtils.getSystemStoreType(storeName) == VeniceSystemStoreType.METADATA_STORE) {
                // Use the corresponding Venice store name to get cluster information
                storeConfig = storeConfigRepo.getStoreConfig(
                    VeniceSystemStoreUtils.getStoreNameFromSystemStoreName(storeName));
            }
            if (!storeConfig.isPresent()) {
                logger.info(
                    "StoreConfig doesn't exist for store: " + storeName + ", will treat resource:" + resourceName + " as deprecated");
                return false;
            }
        }
        String clusterName = storeConfig.get().getCluster();
        return isResourceStillAlive(clusterName, resourceName);
    }

    @Override
    public boolean isClusterValid(String clusterName) {
        return admin.getClusters().contains(clusterName);
    }

    /**
     * This function is used to determine whether the requested cluster is in maintenance mode or not.
     * Right now, this class is using {@link #zkClient} to access the zookeeper node of controller
     * maintenance since this info couldn't be retrieved through {@link HelixAdmin}.
     * TODO: Once Helix provides the right API, we should switch to the formal way since we shouldn't
     * touch the implementation details of Helix.
     *
     * @param clusterName
     * @return
     */
    private boolean isClusterInMaintenanceMode(String clusterName) {
        // Construct the path to maintenance mode ZNode
        String maintenanceZNodePath = "/" + clusterName + "/CONTROLLER/MAINTENANCE";
        return zkClient.exists(maintenanceZNodePath);
    }

    protected HelixAdmin getHelixAdmin() {
        return this.admin;
    }

    @Override
    public void createStore(String clusterName, String storeName, String owner, String keySchema,
                            String valueSchema, boolean isSystemStore, Optional<String> accessPermissions) {
        HelixVeniceClusterResources clusterResources = getHelixVeniceClusterResources(clusterName);
        logger.info(String.format("Start creating store %s in cluster %s with owner %s", storeName, clusterName, owner));
        try (AutoCloseableLock ignore = clusterResources.getClusterLockManager().createStoreWriteLock(storeName)) {
            checkPreConditionForCreateStore(clusterName, storeName, keySchema, valueSchema, isSystemStore, true);
            VeniceControllerClusterConfig config = getHelixVeniceClusterResources(clusterName).getConfig();
            Store newStore = new ZKStore(storeName, owner, System.currentTimeMillis(), config.getPersistenceType(),
                config.getRoutingStrategy(), config.getReadStrategy(), config.getOfflinePushStrategy(),
                config.getReplicationFactor());

            if (config.isLeaderFollowerEnabledForBatchOnlyStores()) {
                // Enable L/F for the new store (no matter which type it is) if the config is set to true.
                newStore.setLeaderFollowerModelEnabled(true);
            }
            if (newStore.isLeaderFollowerModelEnabled()) {
                newStore.setNativeReplicationEnabled(config.isNativeReplicationEnabledAsDefaultForBatchOnly());
                newStore.setActiveActiveReplicationEnabled(config.isActiveActiveReplicationEnabledAsDefaultForBatchOnly());
            } else {
                newStore.setNativeReplicationEnabled(false);
                newStore.setActiveActiveReplicationEnabled(false);
            }

            /**
             * Initialize default NR source fabric base on default config for different store types.
             */
            if (newStore.isIncrementalPushEnabled()) {
                newStore.setNativeReplicationSourceFabric(config.getNativeReplicationSourceFabricAsDefaultForIncremental());
            } else if (newStore.isHybrid()) {
                newStore.setNativeReplicationSourceFabric(config.getNativeReplicationSourceFabricAsDefaultForHybrid());
            } else {
                newStore.setNativeReplicationSourceFabric(config.getNativeReplicationSourceFabricAsDefaultForBatchOnly());
            }

            configureNewStore(newStore, config);
            ReadWriteStoreRepository storeRepo = clusterResources.getStoreMetadataRepository();
            Store existingStore = storeRepo.getStore(storeName);
            if (existingStore != null) {
                // We already check the pre-condition before, so if we could find a store with the same name,
                // it means the store is a legacy store which is left by a failed deletion. So we should delete it.
                deleteStore(clusterName, storeName, existingStore.getLargestUsedVersionNumber(), true);
            }
            // Now there is not store exists in the store repository, we will try to retrieve the info from the graveyard.
            // Get the largestUsedVersionNumber from graveyard to avoid resource conflict.
            final int largestUsedVersionNumber = storeGraveyard.getLargestUsedVersionNumber(storeName);
            newStore.setLargestUsedVersionNumber(largestUsedVersionNumber);
            storeRepo.addStore(newStore);
            // Create global config for that store.
            ZkStoreConfigAccessor storeConfigAccessor = getHelixVeniceClusterResources(clusterName).getStoreConfigAccessor();
            if (!storeConfigAccessor.containsConfig(storeName)) {
                storeConfigAccessor.createConfig(storeName, clusterName);
            }

            // Add schemas
            ReadWriteSchemaRepository schemaRepo = clusterResources.getSchemaRepository();
            schemaRepo.initKeySchema(storeName, keySchema);
            schemaRepo.addValueSchema(storeName, valueSchema, HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID);
            // Write store schemas to metadata store.
            logger.info(String.format("Completed creating Store %s in cluster %s with owner %s and largestUsedVersionNumber %d",
                    storeName, clusterName, owner, newStore.getLargestUsedVersionNumber()));
        }
    }

    private void configureNewStore(Store newStore, VeniceControllerClusterConfig config) {
        if (config.isLeaderFollowerEnabledForBatchOnlyStores()) {
            // Enable L/F for the new store (no matter which type it is) if the config is set to true.
            newStore.setLeaderFollowerModelEnabled(true);
        }
        if (newStore.isLeaderFollowerModelEnabled()) {
            newStore.setNativeReplicationEnabled(config.isNativeReplicationEnabledAsDefaultForBatchOnly());
        } else {
            newStore.setNativeReplicationEnabled(false);
        }

        /**
         * Initialize default NR source fabric base on default config for different store types.
         */
        if (newStore.isIncrementalPushEnabled()) {
            newStore.setNativeReplicationSourceFabric(config.getNativeReplicationSourceFabricAsDefaultForIncremental());
        } else if (newStore.isHybrid()) {
            newStore.setNativeReplicationSourceFabric(config.getNativeReplicationSourceFabricAsDefaultForHybrid());
        } else {
            newStore.setNativeReplicationSourceFabric(config.getNativeReplicationSourceFabricAsDefaultForBatchOnly());
        }
    }

    @Override
    /**
     * This method will delete store data, metadata, version and rt topics
     * One exception is for stores with isMigrating flag set. In that case, the corresponding kafka topics and storeConfig
     * will not be deleted so that they are still available for the cloned store.
     */
    public void deleteStore(String clusterName, String storeName, int largestUsedVersionNumber,
        boolean waitOnRTTopicDeletion) {
        checkControllerMastership(clusterName);
        logger.info(String.format("Start deleting store: %s in cluster %s", storeName, clusterName));
        HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
        try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
            ReadWriteStoreRepository storeRepository = resources.getStoreMetadataRepository();
            ZkStoreConfigAccessor storeConfigAccessor = getHelixVeniceClusterResources(clusterName).getStoreConfigAccessor();
            StoreConfig storeConfig = storeConfigAccessor.getStoreConfig(storeName);
            Store store = storeRepository.getStore(storeName);
            checkPreConditionForDeletion(clusterName, storeName, store);
            if (largestUsedVersionNumber == Store.IGNORE_VERSION) {
                // ignore and use the local largest used version number.
                logger.info("Give largest used version number is: " + largestUsedVersionNumber
                    + " will skip overwriting the local store.");
            } else if (largestUsedVersionNumber < store.getLargestUsedVersionNumber()) {
                throw new VeniceException("Given largest used version number: " + largestUsedVersionNumber
                    + " is smaller than the largest used version number: " + store.getLargestUsedVersionNumber() +
                    " found in repository. Cluster: " + clusterName + ", store: " + storeName);
            } else {
                store.setLargestUsedVersionNumber(largestUsedVersionNumber);
            }

            String currentlyDiscoveredClusterName = storeConfig.getCluster(); // This is for store migration
            // TODO: and ismigration flag is false, only then delete storeConfig
            if (!currentlyDiscoveredClusterName.equals(clusterName)) {
                // This is most likely the deletion after a store migration operation.
                // In this case the storeConfig should not be deleted,
                // because it is still being used to discover the cloned store
                logger.warn("storeConfig for this store " + storeName + " in cluster " + clusterName
                    + " will not be deleted because it is currently pointing to another cluster: "
                    + currentlyDiscoveredClusterName);
            } else if (store.isMigrating()) {
                // Cluster discovery is correct but store migration flag has not been reset.
                // This is most likely a direct deletion command from admin-tool sent to the wrong cluster.
                // i.e. instead of using the proper --end-migration command, a --delete-store command was issued AND sent to the wrong cluster
                String errMsg = "Abort storeConfig deletion for store " + storeName + " in cluster " + clusterName
                    + " because this is either the cloned store after a successful migration"
                    + " or the original store after a failed migration.";
                logger.warn(errMsg);
            } else {
                // Update deletion flag in Store to start deleting. In case of any failures during the deletion, the store
                // could be deleted later after controller is recovered.
                // A worse case is deletion succeeded in parent controller but failed in production controller. After skip
                // the admin message offset, a store was left in some prod colos. While user re-create the store, we will
                // delete this legacy store if isDeleting is true for this store.
                storeConfig.setDeleting(true);
                storeConfigAccessor.updateConfig(storeConfig, store.isStoreMetaSystemStoreEnabled());
            }
            storeRepository.updateStore(store);

            if (store.isStoreMetadataSystemStoreEnabled()) {
                // Attempt to dematerialize all possible versions, no-op if a version doesn't actually exist.
                for (Version version : storeRepository.getStore(VeniceSystemStoreType.METADATA_STORE.getPrefix()).getVersions()) {
                    dematerializeMetadataStoreVersion(clusterName, storeName, version.getNumber(), !store.isMigrating());
                }
            }
            // Delete All versions and push statues
            deleteAllVersionsInStore(clusterName, storeName);
            resources.getPushMonitor().cleanupStoreStatus(storeName);
            // Clean up topics
            if (!store.isMigrating()) {
                // for RT topic block on deletion so that next create store does not see the lingering RT topic which could have different partition count
                String rtTopic = Version.composeRealTimeTopic(storeName);
                truncateKafkaTopic(rtTopic);
                if (waitOnRTTopicDeletion && getTopicManager().containsTopic(rtTopic)) {
                    throw new VeniceRetriableException("Waiting for RT topic deletion for store: " + storeName);
                }
                String metadataSystemStoreRTTopic =
                    Version.composeRealTimeTopic(VeniceSystemStoreUtils.getMetadataStoreName(storeName));
                if (getTopicManager().containsTopic(metadataSystemStoreRTTopic)) {
                    truncateKafkaTopic(metadataSystemStoreRTTopic);
                    // Don't need to block on deletion for metadata system store RT topic because it's handled in materializeMetadataStoreVersion.
                }
            }
            truncateOldTopics(clusterName, store, true);

            // Cleanup meta system store if necessary
            if (store.isStoreMetaSystemStoreEnabled()) {
                String metaSystemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
                logger.info("Start deleting meta system store: " + metaSystemStoreName);
                // Delete All versions and push statues for meta system store
                deleteAllVersionsInStore(clusterName, metaSystemStoreName);
                resources.getPushMonitor().cleanupStoreStatus(metaSystemStoreName);
                if (!store.isMigrating()) {
                    // Clean up venice writer before truncating RT topic
                    metaStoreWriter.removeMetaStoreWriter(metaSystemStoreName);
                    // Delete RT topic
                    truncateKafkaTopic(Version.composeRealTimeTopic(metaSystemStoreName));
                } else {
                    logger.info("The rt topic for " + metaSystemStoreName + " will be kept since the store is migrating");
                }
                Store metaSystemStore = storeRepository.getStore(metaSystemStoreName);
                if (metaSystemStore != null) {
                    truncateOldTopics(clusterName, metaSystemStore, true);
                }
                logger.info("Finished deleting meta system store: " + metaSystemStoreName);
            }
            if (store.isDaVinciPushStatusStoreEnabled()) {
                String daVinciPushStatusSystemStoreName = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName);
                logger.info("Start deleting Da Vinci push status system store: " + daVinciPushStatusSystemStoreName);
                // Delete All versions and push statues for da vinci push status system store
                deleteAllVersionsInStore(clusterName, daVinciPushStatusSystemStoreName);
                resources.getPushMonitor().cleanupStoreStatus(daVinciPushStatusSystemStoreName);
                if (!store.isMigrating()) {
                    truncateKafkaTopic(Version.composeRealTimeTopic(daVinciPushStatusSystemStoreName));
                } else {
                    logger.info("The rt topic for " + daVinciPushStatusSystemStoreName + " will be kept since the store is migrating");
                }
                Store daVinciPushStatusSystemStore = storeRepository.getStore(daVinciPushStatusSystemStoreName);
                if (daVinciPushStatusSystemStore != null) {
                    truncateOldTopics(clusterName, daVinciPushStatusSystemStore, true);
                }
                logger.info("Finished deleting da vinci push status system store: " + daVinciPushStatusSystemStore);
            }
            // Move the store to graveyard. It will only re-create the znode for store's metadata excluding key and
            // value schemas.
            logger.info("Putting store: " + storeName + " into graveyard");
            storeGraveyard.putStoreIntoGraveyard(clusterName, storeRepository.getStore(storeName));
            // Helix will remove all data under this store's znode including key and value schemas.
            resources.getStoreMetadataRepository().deleteStore(storeName);

            // Delete the config for this store after deleting the store.
            if (storeConfig.isDeleting()) {
                storeConfigAccessor.deleteConfig(storeName);
            }
            logger.info("Store " + storeName + " in cluster " + clusterName + " has been deleted.");
        }
    }

    private Integer fetchSystemStoreSchemaId(String clusterName, String storeName, String valueSchemaStr) {
        if (isMasterController(clusterName)) {
            // Can be fetched from local repository
            return getValueSchemaId(clusterName, storeName, valueSchemaStr);
        }
        ControllerClient controllerClient = new ControllerClient(clusterName,
            getLeaderController(clusterName).getUrl(false), sslFactory);
        SchemaResponse response = controllerClient.getValueSchemaID(storeName, valueSchemaStr);
        if (response.isError()) {
            throw new VeniceException("Failed to fetch schema id for store: " + storeName + ", error: "
                + response.getError());
        }
        return response.getId();
    }

    @Override
    public void sendPushJobDetails(PushJobStatusRecordKey key, PushJobDetails value) {
        if (pushJobStatusStoreClusterName.isEmpty()) {
            throw new VeniceException(("Unable to send the push job details because "
                + ConfigKeys.PUSH_JOB_STATUS_STORE_CLUSTER_NAME) + " is not configured");
        }
        String pushJobDetailsStoreName = VeniceSystemStoreUtils.getPushJobDetailsStoreName();
        if (pushJobDetailsRTTopic == null) {
            // Verify the RT topic exists and give some time in case it's getting created.
            String expectedRTTopic = Version.composeRealTimeTopic(pushJobDetailsStoreName);
            for (int attempt = 0; attempt < INTERNAL_STORE_GET_RRT_TOPIC_ATTEMPTS; attempt ++) {
                if (attempt > 0)
                    Utils.sleep(INTERNAL_STORE_RTT_RETRY_BACKOFF_MS);
                if (getTopicManager().containsTopicAndAllPartitionsAreOnline(expectedRTTopic)) {
                    pushJobDetailsRTTopic = expectedRTTopic;
                    logger.info("Topic " + expectedRTTopic
                        + " exists and is configured to receive push job details events");
                    break;
                }
            }
            if (pushJobDetailsRTTopic == null) {
                throw new VeniceException("Expected RT topic " + expectedRTTopic + " to receive push job details events"
                    + " not found. The topic either hasn't been created yet or it's mis-configured");
            }
        }

        VeniceWriter pushJobDetailsWriter = jobTrackingVeniceWriterMap.computeIfAbsent(PUSH_JOB_DETAILS_WRITER, k -> {
            pushJobDetailsSchemaId = fetchSystemStoreSchemaId(pushJobStatusStoreClusterName,
                VeniceSystemStoreUtils.getPushJobDetailsStoreName(), value.getSchema().toString());
            return getVeniceWriterFactory().createVeniceWriter(pushJobDetailsRTTopic,
                new VeniceAvroKafkaSerializer(key.getSchema().toString()),
                new VeniceAvroKafkaSerializer(value.getSchema().toString()));
        });

        pushJobDetailsWriter.put(key, value, pushJobDetailsSchemaId, null);
    }

    @Override
    public PushJobDetails getPushJobDetails(PushJobStatusRecordKey key) {
        Utils.notNull(key);
        String storeName = VeniceSystemStoreUtils.getPushJobDetailsStoreName();
        String d2Service = discoverCluster(storeName).getSecond();
        return readValue(key, storeName, d2Service, PushJobDetails.class);
    }

    @Override
    public BatchJobHeartbeatValue getBatchJobHeartbeatValue(BatchJobHeartbeatKey batchJobHeartbeatKey) {
        Utils.notNull(batchJobHeartbeatKey);
        String storeName = VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE.getPrefix();
        String d2Service = discoverCluster(storeName).getSecond();
        return readValue(batchJobHeartbeatKey, storeName, d2Service, BatchJobHeartbeatValue.class);
    }

    private <K, V extends SpecificRecord> V readValue(K key, String storeName, String d2Service,  Class<V> specificValueClass) {
        // TODO: we may need to use the ICProvider interface to avoid missing IC warning logs when making these client calls
        try (AvroSpecificStoreClient<K, V> client = ClientFactory.getAndStartSpecificAvroClient(
                ClientConfig.defaultSpecificClientConfig(
                    storeName,
                    specificValueClass
                ).setD2ServiceName(d2Service).setD2Client(this.d2Client))
        ) {
            return client.get(key).get();
        } catch (Exception e) {
            throw new VeniceException(e);
        }
    }

    @Override
    public void writeEndOfPush(String clusterName, String storeName, int versionNumber, boolean alsoWriteStartOfPush) {
        //validate store and version exist
        Store store = getStore(clusterName, storeName);

        if (null == store) {
            throw new VeniceNoStoreException(storeName);
        }

        if (store.getCurrentVersion() == versionNumber){
            if (!VeniceSystemStoreUtils.isSystemStore(storeName)) {
                // This check should only apply to non Zk shared stores.
                throw new VeniceHttpException(HttpStatus.SC_CONFLICT, "Cannot end push for version " + versionNumber + " that is currently being served");
            }
        }

        Optional<Version> version = store.getVersion(versionNumber);
        if (!version.isPresent()){
            throw new VeniceHttpException(HttpStatus.SC_NOT_FOUND, "Version " + versionNumber + " was not found for Store " + storeName
                + ".  Cannot end push for version that does not exist");
        }

        String topicToReceiveEndOfPush = version.get().getPushType().isStreamReprocessing() ?
            Version.composeStreamReprocessingTopic(storeName, versionNumber) :
            Version.composeKafkaTopic(storeName, versionNumber);
        //write EOP message
        getVeniceWriterFactory().useVeniceWriter(
            () -> (multiClusterConfigs.isParent() && version.get().isNativeReplicationEnabled())
                ? getVeniceWriterFactory().createVeniceWriter(topicToReceiveEndOfPush, version.get().getPushStreamSourceAddress())
                : getVeniceWriterFactory().createVeniceWriter(topicToReceiveEndOfPush),
            veniceWriter -> {
                if (alsoWriteStartOfPush) {
                    veniceWriter.broadcastStartOfPush(new HashMap<>());
                }
                veniceWriter.broadcastEndOfPush(new HashMap<>());
            }
            );
    }

    @Override
    public boolean whetherEnableBatchPushFromAdmin() {
        return multiClusterConfigs.isEnableBatchPushFromAdminInChildController();
    }

    @Override
    public void migrateStore(String srcClusterName, String destClusterName, String storeName) {
        if (srcClusterName.equals(destClusterName)) {
            throw new VeniceException("Source cluster and destination cluster cannot be the same!");
        }

        // Update store and storeConfig to support single datacenter store migration
        if (!multiClusterConfigs.isParent()) {
            this.updateStore(srcClusterName, storeName, new UpdateStoreQueryParams().setStoreMigration(true));
            this.setStoreConfigForMigration(storeName, srcClusterName, destClusterName);
        }

        String destControllerUrl = this.getLeaderController(destClusterName).getUrl(false);
        ControllerClient destControllerClient = new ControllerClient(destClusterName, destControllerUrl, sslFactory);

        // Get original store properties
        Store srcStore = this.getStore(srcClusterName, storeName);
        String keySchema = this.getKeySchema(srcClusterName, storeName).getSchema().toString();
        List<SchemaEntry> valueSchemaEntries = this.getValueSchemas(srcClusterName, storeName)
            .stream()
            .sorted(Comparator.comparingInt(SchemaEntry::getId))
            .collect(Collectors.toList());

        int schemaNum = valueSchemaEntries.size();
        // Create a new store in destination cluster
        destControllerClient.createNewStore(
            storeName,
            srcStore.getOwner(),
            keySchema,
            valueSchemaEntries.get(0).getSchema().toString());
        // Add other value schemas
        for (int i = 1; i < schemaNum; i++) {
            destControllerClient.addValueSchema(storeName, valueSchemaEntries.get(i).getSchema().toString());
        }

        // Copy remaining properties that will make the cloned store almost identical to the original
        UpdateStoreQueryParams params = new UpdateStoreQueryParams(srcStore)
            .setStoreMigration(true)
            .setMigrationDuplicateStore(true) // Mark as duplicate store, to which L/F SN refers to avoid multi leaders
            .setLargestUsedVersionNumber(0); // Decrease the largestUsedVersionNumber to trigger bootstrap in dest cluster
        destControllerClient.updateStore(storeName, params);

        Consumer<String> versionMigrationConsumer = migratingStoreName -> {
            Store migratingStore = this.getStore(srcClusterName, migratingStoreName);
            List<Version> versionsToMigrate = getVersionsToMigrate(srcClusterName, migratingStoreName, migratingStore);
            logger.info("Adding versions: "
                + versionsToMigrate.stream().map(Version::getNumber).map(String::valueOf).collect(Collectors.joining(","))
                + " to the dest cluster " + destClusterName);
            for (Version version : versionsToMigrate) {
                try {
                    /**
                     * Topic manager partitions might be cleaned up in parent controller.
                     * Get partition count from existing version instead of topic manager.
                     */
                    int partitionCount = version.getPartitionCount();
                    /**
                     * Remote Kafka is set to null for store migration because version topics at source fabric might be deleted
                     * already; migrated stores should bootstrap by consuming the version topics in its local fabric.
                     */
                    long rewindTimeInSecondsOverride = -1;
                    if (version.getHybridStoreConfig() != null) {
                        rewindTimeInSecondsOverride = version.getHybridStoreConfig().getRewindTimeInSeconds();
                    }
                    int replicationMetadataVersionId = version.getTimestampMetadataVersionId();
                    destControllerClient.addVersionAndStartIngestion(migratingStoreName, version.getPushJobId(), version.getNumber(),
                        partitionCount, version.getPushType(), null, rewindTimeInSecondsOverride,
                        replicationMetadataVersionId);
                } catch (Exception e) {
                    throw new VeniceException("An exception was thrown when attempting to add version and start ingestion for store "
                        + migratingStoreName + " and version " + version.getNumber(), e);
                }
            }
        };

        if (srcStore.isStoreMetaSystemStoreEnabled()) {
            // Migrate all the meta system store versions
            versionMigrationConsumer.accept(VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName));
        }
        if (srcStore.isDaVinciPushStatusStoreEnabled()) {
            // enable da vinci push status system store in dest cluster
            versionMigrationConsumer.accept(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName));
        }
        // Migrate all the versions belonging to the regular Venice store
        versionMigrationConsumer.accept(storeName);
    }

    private List<Version> getVersionsToMigrate(String srcClusterName, String storeName, Store srcStore) {
        // For single datacenter store migration, sort, filter and return the version list
        if (!multiClusterConfigs.isParent()) {
            return srcStore.getVersions()
                .stream()
                .sorted(Comparator.comparingInt(Version::getNumber))
                .filter(version -> Arrays.asList(STARTED, PUSHED, ONLINE).contains(version.getStatus()))
                .collect(Collectors.toList());
        }

        // For multi data center store migration, all store versions in parent controller might be error versions.
        // Therefore, we have to gather live versions from child fabrics.
        Map<Integer, Version> versionNumberToVersionMap = new HashMap<>();
        Map<String, ControllerClient> controllerClients = this.getControllerClientMap(srcClusterName);
        Set<String> prodColos = controllerClients.keySet();
        for (String colo : prodColos) {
            StoreResponse storeResponse = controllerClients.get(colo).getStore(storeName);
            if (storeResponse.isError()) {
                throw new VeniceException("Could not query store from child: " + colo + " for cluster: " + srcClusterName + ". "
                    + storeResponse.getError());
            } else {
                StoreInfo storeInfo = storeResponse.getStore();
                storeInfo.getVersions()
                    .stream()
                    .filter(version -> Arrays.asList(STARTED, PUSHED, ONLINE).contains(version.getStatus()))
                    .forEach(version -> versionNumberToVersionMap.putIfAbsent(version.getNumber(), version));
            }
        }
        List<Version> versionsToMigrate = versionNumberToVersionMap.values()
            .stream()
            .sorted(Comparator.comparingInt(Version::getNumber))
            .collect(Collectors.toList());
        int largestChildVersionNumber = versionsToMigrate.size() > 0 ?
            versionsToMigrate.get(versionsToMigrate.size() - 1).getNumber() : 0;

        // Then append new versions from the source store in parent controller.
        // Because a new version might not appear in child controllers yet.
        srcStore.getVersions()
            .stream()
            .sorted(Comparator.comparingInt(Version::getNumber))
            .filter(version -> version.getNumber() > largestChildVersionNumber)
            .forEach(versionsToMigrate::add);

        return versionsToMigrate;
    }

    Map<String, ControllerClient> getControllerClientMap(String clusterName) {
        if (!multiClusterConfigs.isParent()) {
            throw new VeniceUnsupportedOperationException("getControllerClientMap");
        }

        return clusterControllerClientPerColoMap.computeIfAbsent(clusterName, cn -> {
            Map<String, ControllerClient> controllerClients = new HashMap<>();
            VeniceControllerConfig veniceControllerConfig = multiClusterConfigs.getControllerConfig(clusterName);
            veniceControllerConfig.getChildDataCenterControllerUrlMap().entrySet().
                forEach(entry -> controllerClients.put(entry.getKey(), new ControllerClient(clusterName, entry.getValue(), sslFactory)));
            veniceControllerConfig.getChildDataCenterControllerD2Map().entrySet().
                forEach(entry -> controllerClients.put(entry.getKey(),
                    new D2ControllerClient(veniceControllerConfig.getD2ServiceName(), clusterName, entry.getValue(), sslFactory)));
            return controllerClients;
        });
    }

    @Override
    public void completeMigration(String srcClusterName, String destClusterName, String storeName) {
        this.updateClusterDiscovery(storeName, srcClusterName, destClusterName, srcClusterName);
    }

    @Override
    public void abortMigration(String srcClusterName, String destClusterName, String storeName) {
        if (srcClusterName.equals(destClusterName)) {
            throw new VeniceException("Source cluster and destination cluster cannot be the same!");
        }

        // Reset migration flag
        // Assume this is the source controller
        // As a result, this store will be removed from the migration watchlist in VeniceHelixAdmin
        this.updateStore(srcClusterName, storeName, new UpdateStoreQueryParams().setStoreMigration(false));

        // Reset storeConfig
        // Change destination to the source cluster name so that they are the same.
        // This indicates an aborted migration
        // As a result, this store will be removed from the migration watchlist in VeniceParentHelixAdmin
        this.setStoreConfigForMigration(storeName, srcClusterName, srcClusterName);

        // Force update cluster discovery so that it will always point to the source cluster
        // Whichever cluster it currently belongs to does not matter
        String clusterDiscovered = this.discoverCluster(storeName).getFirst();
        this.updateClusterDiscovery(storeName, clusterDiscovered, srcClusterName, srcClusterName);
    }

    @Override
    public void updateClusterDiscovery(String storeName, String oldCluster, String newCluster, String initiatingCluster) {
        HelixVeniceClusterResources resources = getHelixVeniceClusterResources(initiatingCluster);
        try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
            ZkStoreConfigAccessor storeConfigAccessor = resources.getStoreConfigAccessor();
            StoreConfig storeConfig = storeConfigAccessor.getStoreConfig(storeName);
            if (storeConfig == null) {
                throw new VeniceException("Store config is empty!");
            } else if (!storeConfig.getCluster().equals(oldCluster)) {
                throw new VeniceException("Store " + storeName + " is expected to be in " + oldCluster + " cluster, but is actually in " + storeConfig.getCluster());
            }
            Store store = getStore(oldCluster, storeName);
            storeConfig.setCluster(newCluster);
            storeConfigAccessor.updateConfig(storeConfig, store.isStoreMetaSystemStoreEnabled());
            logger.info("Store " + storeName + " now belongs to cluster " + newCluster + " instead of " + oldCluster);
        }
    }

    /**
     * Check whether Controller should block the incoming store creation.
     * Inside this function, there is a logic to check whether there are any lingering resources since the requested
     * store could be just deleted recently.
     * This check should be skipped in Child Controller, but only enabled in Parent Controller because of the following
     * reasons:
     * 1. Parent Controller has the strict order that the system store must be created before the host Venice store.
     * 2. Child Controller doesn't have this strict order since the admin messages of Child Controller could be executed
     * in parallel since they are different store names. So when such kind of race condition happens, it will cause a
     * dead loop:
     *   a. The version creation of system store will create a RT topic in Parent Cluster.
     *   b. The RT topic will be mirrored by KMM to the Child Cluster.
     *   c. The version creation admin message of system store will be blocked in Child Controller since the host Venice
     *   store doesn't exist.
     *   d. The store creation admin message of the host Venice store will be blocked in Child Controller because of
     *   lingering resource check (RT topic of its system store already exists, which is created by KMM).
     *
     * In the future, once Venice gets rid of KMM, the topic won't be automatically created by KMM, and this race condition
     * will be addressed.
     * So far, Child Controller will skip lingering resource check when handling store creation admin message.
     */
    protected void checkPreConditionForCreateStore(String clusterName, String storeName, String keySchema,
                                                   String valueSchema, boolean allowSystemStore, boolean skipLingeringResourceCheck) {
        if (!Store.isValidStoreName(storeName)) {
            throw new VeniceException("Invalid store name " + storeName + ". Only letters, numbers, underscore or dash");
        }
        AvroSchemaUtils.validateAvroSchemaStr(keySchema);
        AvroSchemaUtils.validateAvroSchemaStr(valueSchema);
        checkControllerMastership(clusterName);
        checkStoreNameConflict(storeName, allowSystemStore);
        // Before creating store, check the global stores configs at first.
        // TODO As some store had already been created before we introduced global store config
        // TODO so we need a way to sync up the data. For example, while we loading all stores from ZK for a cluster,
        // TODO put them into global store configs.
        boolean isLagecyStore = false;
        /**
         * In the following situation, we will skip the lingering resource check for the new store request:
         * 1. The store is migrating, and the cluster name is equal to the migrating destination cluster.
         * 2. The legacy store.
         */
        ZkStoreConfigAccessor storeConfigAccessor = getHelixVeniceClusterResources(clusterName).getStoreConfigAccessor();
        if (storeConfigAccessor.containsConfig(storeName)) {
            StoreConfig storeConfig = storeConfigAccessor.getStoreConfig(storeName);
            // Controller was trying to delete the old store but failed.
            // Delete again before re-creating.
            // We lock the resource during deletion, so it's impossible to access the store which is being
            // deleted from here. So the only possible case is the deletion failed before.
            if (!storeConfig.isDeleting()) {
                // It is ok to create the same store in destination cluster during store migration.
                if (!clusterName.equals(storeConfig.getMigrationDestCluster())) {
                    throw new VeniceStoreAlreadyExistsException(storeName);
                } else {
                    skipLingeringResourceCheck = true;
                }
            } else {
                isLagecyStore = true;
                skipLingeringResourceCheck = true;
            }
        }

        ReadWriteStoreRepository repository = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
        Store store = repository.getStore(storeName);
        // If the store exists in store repository and it's still active(not being deleted), we don't allow to re-create it.
        if (store != null && !isLagecyStore) {
            throwStoreAlreadyExists(clusterName, storeName);
        }

        if (!skipLingeringResourceCheck) {
            checkResourceCleanupBeforeStoreCreation(clusterName, storeName, !multiClusterConfigs.isParent());
        }

        // Check whether the schema is valid or not
        new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, keySchema);
        new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, valueSchema);
    }

    private void checkStoreNameConflict(String storeName, boolean allowSystemStore) {
        if (storeName.equals(AbstractVeniceAggStats.STORE_NAME_FOR_TOTAL_STAT)) {
            throw new VeniceException("Store name: " + storeName + " clashes with the internal usage, please change it");
        }

        if (!allowSystemStore && VeniceSystemStoreUtils.isSystemStore(storeName)) {
            throw new VeniceException("Store name: " + storeName
                + " clashes with the Venice system store usage, please change it");
        }
    }

    protected Store checkPreConditionForDeletion(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        ReadWriteStoreRepository repository = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
        Store store = repository.getStore(storeName);
        checkPreConditionForDeletion(clusterName, storeName, store);
        return store;
    }

    private void checkPreConditionForDeletion(String clusterName, String storeName, Store store) {
        if (store == null) {
            throwStoreDoesNotExist(clusterName, storeName);
        }
        if (store.isEnableReads() || store.isEnableWrites()) {
            String errorMsg = "Unable to delete the entire store or versions for store: " + storeName
                + ". Store has not been disabled. Both read and write need to be disabled before deleting.";
            logger.error(errorMsg);
            throw new VeniceException(errorMsg);
        }
    }

    protected Store checkPreConditionForSingleVersionDeletion(String clusterName, String storeName, int versionNum) {
        checkControllerMastership(clusterName);
        ReadWriteStoreRepository repository = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
        Store store = repository.getStore(storeName);
        checkPreConditionForSingleVersionDeletion(clusterName, storeName, store, versionNum);
        return store;
    }

    private void checkPreConditionForSingleVersionDeletion(String clusterName, String storeName, Store store, int versionNum) {
        if (store == null) {
            throwStoreDoesNotExist(clusterName, storeName);
        }
        // The cannot delete current version restriction is only applied to non-system stores.
        if (!store.isSystemStore() && store.getCurrentVersion() == versionNum) {
            String errorMsg =
                "Unable to delete the version: " + versionNum + ". The current version could be deleted from store: "
                    + storeName;
            logger.error(errorMsg);
            throw new VeniceUnsupportedOperationException("delete single version", errorMsg);
        }
    }

    protected final static int VERSION_ID_UNSET = -1;

    /**
     * This is a wrapper for VeniceHelixAdmin#addVersion but performs additional operations needed for add version invoked
     * from the admin channel. Therefore, this method is mainly invoked from the admin task upon processing an add
     * version message.
     */
    @Override
    public void addVersionAndStartIngestion(
        String clusterName, String storeName, String pushJobId, int versionNumber, int numberOfPartitions,
        Version.PushType pushType, String remoteKafkaBootstrapServers, long rewindTimeInSecondsOverride, int replicationMetadataVersionId) {
        Store store = getStore(clusterName, storeName);
        if (null == store) {
            throw new VeniceNoStoreException(storeName, clusterName);
        }
        if (versionNumber <= store.getLargestUsedVersionNumber()) {
            logger.info("Ignoring the add version message since version " + versionNumber
                + " is less than the largestUsedVersionNumber of " + store.getLargestUsedVersionNumber()
                + " for store " + storeName + " in cluster " + clusterName);
        } else {
            addVersion(clusterName, storeName, pushJobId, versionNumber, numberOfPartitions,
                getReplicationFactor(clusterName, storeName), true, false, false,
                true, pushType, null, remoteKafkaBootstrapServers, Optional.empty(),
                rewindTimeInSecondsOverride, replicationMetadataVersionId, Optional.empty());
        }
    }

    /**
     * This method is invoked in parent controllers to replicate new version signals for migrating store.
     */
    public void replicateAddVersionAndStartIngestion(
        String clusterName, String storeName, String pushJobId, int versionNumber, int numberOfPartitions,
        Version.PushType pushType, String remoteKafkaBootstrapServers, long rewindTimeInSecondsOverride,
        int timestampMetadataVersionId) {
        checkControllerMastership(clusterName);
        try {
            StoreConfig storeConfig = storeConfigRepo.getStoreConfigOrThrow(storeName);
            String destinationCluster = storeConfig.getMigrationDestCluster();
            String sourceCluster = storeConfig.getMigrationSrcCluster();
            if (storeConfig.getCluster().equals(destinationCluster)) {
                // Migration has completed in this colo but the overall migration is still in progress.
                if (clusterName.equals(destinationCluster)) {
                    // Mirror new pushes back to the source cluster in case we abort migration after completion.
                    ControllerClient sourceClusterControllerClient =
                        new ControllerClient(sourceCluster, getLeaderController(sourceCluster).getUrl(false), sslFactory);
                    VersionResponse response = sourceClusterControllerClient.addVersionAndStartIngestion(storeName,
                        pushJobId, versionNumber, numberOfPartitions, pushType, remoteKafkaBootstrapServers,
                        rewindTimeInSecondsOverride, timestampMetadataVersionId);
                    if (response.isError()) {
                        // Throw exceptions here to utilize admin channel's retry property to overcome transient errors.
                        throw new VeniceException("Replicate add version endpoint call back to source cluster: "
                            + sourceCluster + " failed for store: " + storeName + " with version: " + versionNumber
                            + ". Error: " + response.getError());
                    }
                }
            } else if (clusterName.equals(sourceCluster)) {
                // Migration is still in progress and we need to mirror new version signal from source to dest.
                ControllerClient destClusterControllerClient =
                    new ControllerClient(destinationCluster, getLeaderController(destinationCluster).getUrl(false), sslFactory);
                VersionResponse response = destClusterControllerClient.addVersionAndStartIngestion(storeName,
                    pushJobId, versionNumber, numberOfPartitions, pushType, remoteKafkaBootstrapServers,
                    rewindTimeInSecondsOverride, timestampMetadataVersionId);
                if (response.isError()) {
                    throw new VeniceException("Replicate add version endpoint call to destination cluster: "
                        + destinationCluster + " failed for store: " + storeName + " with version: " + versionNumber
                        + ". Error: " + response.getError());
                }
            }
        } catch (Exception e) {
            logger.warn("Exception thrown when replicating add version for store " + storeName + " and version "
                + versionNumber + " as part of store migration", e);
        }
    }

    /**
     * A wrapper to invoke VeniceHelixAdmin#addVersion to only increment the version and create the topic(s) needed
     * without starting ingestion.
     */
    public Pair<Boolean, Version> addVersionAndTopicOnly(String clusterName, String storeName, String pushJobId, int numberOfPartitions,
        int replicationFactor, boolean sendStartOfPush, boolean sorted, Version.PushType pushType,
        String compressionDictionary, String remoteKafkaBootstrapServers, Optional<String> sourceGridFabric,
        long rewindTimeInSecondsOverride, int replicationMetadataVersionId, Optional<String> emergencySourceRegion) {
        return addVersion(clusterName, storeName, pushJobId, VERSION_ID_UNSET, numberOfPartitions, replicationFactor,
            false, sendStartOfPush, sorted, false, pushType,
            compressionDictionary, remoteKafkaBootstrapServers, sourceGridFabric, rewindTimeInSecondsOverride,
            replicationMetadataVersionId, emergencySourceRegion);
    }

    /**
     * Only add version to the store without creating the topic or start ingestion. Used to sync version metadata in the
     * parent fabric during store migration.
     */
    public Version addVersionOnly(String clusterName, String storeName, String pushJobId, int versionNumber,
        int numberOfPartitions, Version.PushType pushType, String remoteKafkaBootstrapServers,
        long rewindTimeInSecondsOverride, int replicationMetadataVersionId) {
        checkControllerMastership(clusterName);
        HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
        ReadWriteStoreRepository repository = resources.getStoreMetadataRepository();
        Store store = repository.getStore(storeName);
        if (null == store) {
            throw new VeniceNoStoreException(storeName, clusterName);
        }
        Version version = new VersionImpl(storeName, versionNumber, pushJobId, numberOfPartitions);
        if (versionNumber < store.getLargestUsedVersionNumber() || store.containsVersion(versionNumber)) {
            logger.info("Ignoring the add version message since version " + versionNumber
                + " is less than the largestUsedVersionNumber of " + store.getLargestUsedVersionNumber()
                + " for store " + storeName + " in cluster " + clusterName);
        } else {
            try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
                version.setPushType(pushType);
                store.addVersion(version);
                // Apply cluster-level native replication configs
                VeniceControllerClusterConfig clusterConfig = resources.getConfig();
                if (version.isLeaderFollowerModelEnabled()) {
                    boolean nativeReplicationEnabled = version.isNativeReplicationEnabled();
                    if (store.isHybrid()) {
                        nativeReplicationEnabled |= clusterConfig.isNativeReplicationEnabledForHybrid();
                    } else {
                        if (store.isIncrementalPushEnabled()) {
                            nativeReplicationEnabled |= clusterConfig.isNativeReplicationEnabledForIncremental();
                        } else {
                            nativeReplicationEnabled |= clusterConfig.isNativeReplicationEnabledForBatchOnly();
                        }
                    }
                    version.setNativeReplicationEnabled(nativeReplicationEnabled);
                }
                if (version.isNativeReplicationEnabled()) {
                    if (remoteKafkaBootstrapServers != null) {
                        version.setPushStreamSourceAddress(remoteKafkaBootstrapServers);
                    } else {
                        version.setPushStreamSourceAddress(getKafkaBootstrapServers(isSslToKafka()));
                    }
                }
                /**
                 * Version-level rewind time override.
                 */
                handleRewindTimeOverride(store, version, rewindTimeInSecondsOverride);

                version.setTimestampMetadataVersionId(replicationMetadataVersionId);

                repository.updateStore(store);
                logger.info("Add version: " + version.getNumber() + " for store: " + storeName);
            }
        }
        return version;
    }

    private void handleRewindTimeOverride(Store store, Version version, long rewindTimeInSecondsOverride) {
        if (store.isHybrid() && rewindTimeInSecondsOverride >= 0
            && rewindTimeInSecondsOverride != version.getHybridStoreConfig().getRewindTimeInSeconds()) {
            HybridStoreConfig hybridStoreConfig = version.getHybridStoreConfig();
            logger.info("Overriding rewind time in seconds: " + rewindTimeInSecondsOverride + " for store: "+
                store.getName() + " and version: " + version.getNumber() + " the original rewind time config: "
                + hybridStoreConfig.getRewindTimeInSeconds());
            hybridStoreConfig.setRewindTimeInSeconds(rewindTimeInSecondsOverride);
            version.setHybridStoreConfig(hybridStoreConfig);
        }
    }

    /**
     * Note, versionNumber may be VERSION_ID_UNSET, which must be accounted for.
     * Add version is a multi step process that can be broken down to three main steps:
     * 1. topic creation or verification, 2. version creation or addition, 3. start ingestion. The execution for some of
     * these steps are different depending on how it's invoked.
     *
     * @return Boolean - whether the version is newly created. Version - existing or new version, or null if the version
     *         is skipped during store migration.
     */
    private Pair<Boolean, Version> addVersion(String clusterName, String storeName, String pushJobId, int versionNumber,
        int numberOfPartitions, int replicationFactor, boolean startIngestion, boolean sendStartOfPush,
        boolean sorted, boolean useFastKafkaOperationTimeout, Version.PushType pushType, String compressionDictionary,
        String remoteKafkaBootstrapServers, Optional<String> sourceGridFabric, long rewindTimeInSecondsOverride,
        int replicationMetadataVersionId, Optional<String> emergencySourceRegion) {
        if (isClusterInMaintenanceMode(clusterName)) {
            throw new HelixClusterMaintenanceModeException(clusterName);
        }

        checkControllerMastership(clusterName);
        HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
        ReadWriteStoreRepository repository = resources.getStoreMetadataRepository();
        Version version = null;
        OfflinePushStrategy strategy;
        boolean isLeaderFollowerStateModel = false;
        VeniceControllerClusterConfig clusterConfig = resources.getConfig();
        BackupStrategy backupStrategy;

        try {
            try (AutoCloseableLock ignore = resources.getClusterLockManager().createClusterReadLock()) {
                try (AutoCloseableLock ignored = resources.getClusterLockManager().createStoreWriteLockOnly(storeName)) {
                    Optional<Version> versionWithPushId = getVersionWithPushId(clusterName, storeName, pushJobId);
                    if (versionWithPushId.isPresent()) {
                        return new Pair<>(false, versionWithPushId.get());
                    }

                    /**
                     * For meta system store, Controller will produce a snapshot to RT topic before creating a new version, so that
                     * it will guarantee the rewind will recover the state for both store properties and replica statuses.
                     */
                    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
                    if (null != systemStoreType && systemStoreType.equals(VeniceSystemStoreType.META_STORE)) {
                        produceSnapshotToMetaStoreRT(clusterName, systemStoreType.extractRegularStoreName(storeName));
                    }
                    if (null != systemStoreType && systemStoreType.equals(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE)) {
                        setUpDaVinciPushStatusStore(clusterName, systemStoreType.extractRegularStoreName(storeName));
                    }

                    Store store = repository.getStore(storeName);
                    if (store == null) {
                        throwStoreDoesNotExist(clusterName, storeName);
                    }
                    // Dest child controllers skip the version whose kafka topic is truncated
                    if (store.isMigrating() && skipMigratingVersion(clusterName, storeName, versionNumber)) {
                        if (versionNumber > store.getLargestUsedVersionNumber()) {
                            store.setLargestUsedVersionNumber(versionNumber);
                            repository.updateStore(store);
                        }
                        logger.warn("Skip adding version: " + versionNumber + " for store: " + storeName
                            + " in cluster: " + clusterName + " because the version topic is truncated");
                        return new Pair<>(false, null);
                    }
                    backupStrategy = store.getBackupStrategy();
                    int amplificationFactor = store.getPartitionerConfig().getAmplificationFactor();
                    if (versionNumber == VERSION_ID_UNSET) {
                        // No version supplied, generate a new version. This could happen either in the parent
                        // controller or local Samza jobs.
                        version = new VersionImpl(storeName, store.peekNextVersion().getNumber(), pushJobId, numberOfPartitions);
                    } else {
                        if (store.containsVersion(versionNumber)) {
                            throwVersionAlreadyExists(storeName, versionNumber);
                        }
                        version = new VersionImpl(storeName, versionNumber, pushJobId, numberOfPartitions);
                    }

                    topicToCreationTime.computeIfAbsent(version.kafkaTopicName(), topic -> System.currentTimeMillis());

                    // Topic created by Venice Controller is always without Kafka compaction.
                    getTopicManager().createTopic(
                        version.kafkaTopicName(),
                        numberOfPartitions * amplificationFactor,
                        clusterConfig.getKafkaReplicationFactor(),
                        true,
                        false,
                        clusterConfig.getMinIsr(),
                        useFastKafkaOperationTimeout
                    );
                    if (pushType.isStreamReprocessing()) {
                        getTopicManager().createTopic(
                            Version.composeStreamReprocessingTopic(storeName, version.getNumber()),
                            numberOfPartitions * amplificationFactor,
                            clusterConfig.getKafkaReplicationFactor(),
                            true,
                            false,
                            clusterConfig.getMinIsr(),
                            useFastKafkaOperationTimeout
                        );
                    }

                    ByteBuffer compressionDictionaryBuffer = null;
                    if (compressionDictionary != null) {
                        compressionDictionaryBuffer = ByteBuffer.wrap(EncodingUtils.base64DecodeFromString(compressionDictionary));
                    }

                    Pair<String, String> sourceKafkaBootstrapServersAndZk = null;

                    store = repository.getStore(storeName);
                    strategy = store.getOffLinePushStrategy();
                    if (!store.containsVersion(version.getNumber())) {
                        version.setPushType(pushType);
                        store.addVersion(version);
                    }
                    //We set the version level LF config to true if this LF dependency check is disabled.
                    //This should be applicable for parent controllers only.
                    if (clusterConfig.isLfModelDependencyCheckDisabled()) {
                        version.setLeaderFollowerModelEnabled(true);
                    }

                    if (version.isLeaderFollowerModelEnabled()) {
                        isLeaderFollowerStateModel = true;
                    }
                    // Apply cluster-level native replication configs
                    if (version.isLeaderFollowerModelEnabled()) {
                        boolean nativeReplicationEnabled = version.isNativeReplicationEnabled();
                        if (store.isHybrid()) {
                            nativeReplicationEnabled |= clusterConfig.isNativeReplicationEnabledForHybrid();
                        } else {
                            if (store.isIncrementalPushEnabled()) {
                                nativeReplicationEnabled |= clusterConfig.isNativeReplicationEnabledForIncremental();
                            } else {
                                nativeReplicationEnabled |= clusterConfig.isNativeReplicationEnabledForBatchOnly();
                            }
                        }
                        version.setNativeReplicationEnabled(nativeReplicationEnabled);
                    }

                    // Check whether native replication is enabled
                    if (version.isNativeReplicationEnabled()) {
                        if (remoteKafkaBootstrapServers != null) {
                            /**
                             * AddVersion is invoked by {@link com.linkedin.venice.controller.kafka.consumer.AdminExecutionTask}
                             * which is processing an AddVersion message that contains remote Kafka bootstrap servers url.
                             */
                            version.setPushStreamSourceAddress(remoteKafkaBootstrapServers);
                        } else {
                            /**
                             * AddVersion is invoked by directly querying controllers; there are 3 different configs that
                             * can determine where the source fabric is:
                             * 1. Cluster level config
                             * 2. Push job config which can identify where the push starts from, which has a higher
                             *    priority than cluster level config
                             * 3. Store level config
                             * 4. Emergency source fabric config in parent controller config, which has the highest priority;
                             *    By default, it's not used unless specified. It has the highest priority because it can be used to fail
                             *    over a push.
                             */
                            String sourceFabric = getNativeReplicationSourceFabric(clusterName, store, sourceGridFabric, emergencySourceRegion);
                            sourceKafkaBootstrapServersAndZk = getNativeReplicationKafkaBootstrapServerAndZkAddress(sourceFabric);
                            String sourceKafkaBootstrapServers = sourceKafkaBootstrapServersAndZk.getFirst();
                            if (sourceKafkaBootstrapServers == null) {
                                sourceKafkaBootstrapServers = getKafkaBootstrapServers(isSslToKafka());
                            }
                            version.setPushStreamSourceAddress(sourceKafkaBootstrapServers);
                            version.setNativeReplicationSourceFabric(sourceFabric);
                        }
                        if (isParent() && ((store.isHybrid()
                            && store.getHybridStoreConfig().getDataReplicationPolicy() == DataReplicationPolicy.AGGREGATE) ||
                            (store.isIncrementalPushEnabled() && store.getIncrementalPushPolicy().equals(IncrementalPushPolicy.INCREMENTAL_PUSH_SAME_AS_REAL_TIME)))) {
                            // Create rt topic in parent colo if the store is aggregate mode hybrid store
                            String realTimeTopic = Version.composeRealTimeTopic(storeName);
                            if (!getTopicManager().containsTopic(realTimeTopic)) {
                                getTopicManager().createTopic(
                                    realTimeTopic,
                                    numberOfPartitions,
                                    clusterConfig.getKafkaReplicationFactor(),
                                    TopicManager.getExpectedRetentionTimeInMs(store, store.getHybridStoreConfig()),
                                    false, // Note: do not enable RT compaction! Might make jobs in Online/Offline model stuck
                                    clusterConfig.getMinIsr(),
                                    false);
                            } else {
                                // If real-time topic already exists, check whether its retention time is correct.
                                Properties topicProperties = getTopicManager().getCachedTopicConfig(realTimeTopic);
                                long topicRetentionTimeInMs = getTopicManager().getTopicRetention(topicProperties);
                                long expectedRetentionTimeMs = TopicManager.getExpectedRetentionTimeInMs(store, store.getHybridStoreConfig());
                                if (topicRetentionTimeInMs != expectedRetentionTimeMs) {
                                    getTopicManager().updateTopicRetention(realTimeTopic, expectedRetentionTimeMs, topicProperties);
                                }
                            }
                        }
                    }
                    /**
                     * Version-level rewind time override.
                     */
                    handleRewindTimeOverride(store, version, rewindTimeInSecondsOverride);
                    store.setPersistenceType(PersistenceType.ROCKS_DB);

                    version.setTimestampMetadataVersionId(replicationMetadataVersionId);

                    repository.updateStore(store);
                    logger.info("Add version: " + version.getNumber() + " for store: " + storeName);

                    /**
                     *  When native replication is enabled and it's in parent controller, directly create the topic in
                     *  the specified source fabric if the source fabric is not the local fabric; the above topic creation
                     *  is still required since child controllers need to create a topic locally, and parent controller uses
                     *  local VT to determine whether there is any ongoing offline push.
                     */
                    if (multiClusterConfigs.isParent() && version.isNativeReplicationEnabled()
                        && !version.getPushStreamSourceAddress().equals(getKafkaBootstrapServers(isSslToKafka()))) {
                        if (sourceKafkaBootstrapServersAndZk == null || sourceKafkaBootstrapServersAndZk.getFirst() == null
                            || sourceKafkaBootstrapServersAndZk.getSecond() == null) {
                            throw new VeniceException("Parent controller should know the source Kafka bootstrap server url "
                                + "and source Kafka ZK address for store: " + storeName + " and version: " + version.getNumber()
                                + " in cluster: " + clusterName);
                        }
                        getTopicManager(sourceKafkaBootstrapServersAndZk).createTopic(
                            version.kafkaTopicName(),
                            numberOfPartitions * amplificationFactor,
                            clusterConfig.getKafkaReplicationFactor(),
                            true,
                            false,
                            clusterConfig.getMinIsr(),
                            useFastKafkaOperationTimeout
                        );
                        if (pushType.isStreamReprocessing()) {
                            getTopicManager(sourceKafkaBootstrapServersAndZk).createTopic(
                                Version.composeStreamReprocessingTopic(storeName, version.getNumber()),
                                numberOfPartitions,
                                clusterConfig.getKafkaReplicationFactor(),
                                true,
                                false,
                                clusterConfig.getMinIsr(),
                                useFastKafkaOperationTimeout
                            );
                        }
                    }

                    if (sendStartOfPush) {
                        final Version finalVersion = version;
                        VeniceWriter veniceWriter = null;
                        try {
                            if (multiClusterConfigs.isParent() && finalVersion.isNativeReplicationEnabled()) {
                                /**
                                 * Produce directly into one of the child fabric
                                 */
                                veniceWriter = getVeniceWriterFactory().createVeniceWriter(finalVersion.kafkaTopicName(),
                                    finalVersion.getPushStreamSourceAddress());
                            } else {
                                veniceWriter = getVeniceWriterFactory().createVeniceWriter(finalVersion.kafkaTopicName());
                            }
                            veniceWriter.broadcastStartOfPush(
                                sorted,
                                finalVersion.isChunkingEnabled(),
                                finalVersion.getCompressionStrategy(),
                                Optional.ofNullable(compressionDictionaryBuffer),
                                Collections.emptyMap()
                            );
                            if (pushType.isStreamReprocessing()) {
                                // Send TS message to version topic to inform leader to switch to the stream reprocessing topic
                                veniceWriter.broadcastTopicSwitch(
                                    Collections.singletonList(getKafkaBootstrapServers(isSslToKafka())),
                                    Version.composeStreamReprocessingTopic(finalVersion.getStoreName(), finalVersion.getNumber()),
                                    -1L,  // -1 indicates rewinding from the beginning of the source topic
                                    new HashMap<>());
                            }
                        } finally {
                            if (veniceWriter != null) {
                                veniceWriter.close();
                            }
                        }
                    }

                    if (startIngestion) {
                        // We need to prepare to monitor before creating helix resource.
                        startMonitorOfflinePush(
                            clusterName, version.kafkaTopicName(), numberOfPartitions, replicationFactor, strategy);
                        helixAdminClient.createVeniceStorageClusterResources(clusterName, version.kafkaTopicName(),
                            numberOfPartitions, replicationFactor, isLeaderFollowerStateModel);
                    }
                }

                if (startIngestion) {
                    // Store write lock is released before polling status
                    waitUntilNodesAreAssignedForResource(clusterName, version.kafkaTopicName(), strategy,
                        clusterConfig.getOffLineJobWaitTimeInMilliseconds(), replicationFactor);

                    // Early delete backup version on start of a push, controlled by store config earlyDeleteBackupEnabled
                    if (backupStrategy == BackupStrategy.DELETE_ON_NEW_PUSH_START
                        && multiClusterConfigs.getControllerConfig(clusterName).isEarlyDeleteBackUpEnabled()) {
                        try {
                            retireOldStoreVersions(clusterName, storeName, true);
                        } catch (Throwable t) {
                            String errorMessage =
                                "Failed to delete previous backup version while pushing " + versionNumber + " to store "
                                    + storeName + " in cluster " + clusterName;
                            logger.error(errorMessage, t);
                        }
                    }
                }
            }
            return new Pair<>(true, version);

        } catch (Throwable e) {
            if (useFastKafkaOperationTimeout && e instanceof VeniceOperationAgainstKafkaTimedOut) {
                // Expected and retriable exception skip error handling within VeniceHelixAdmin and let the caller to
                // handle the exception.
                throw e;
            }
            int failedVersionNumber = version == null ? versionNumber : version.getNumber();
            String errorMessage = "Failed to add version " + failedVersionNumber + " to store " + storeName + " in cluster " + clusterName;
            logger.error(errorMessage, e);
            try {
                if (version != null) {
                    failedVersionNumber = version.getNumber();
                    String statusDetails = "Version creation failure, caught:\n" + ExceptionUtils.stackTraceToString(e);
                    handleVersionCreationFailure(clusterName, storeName, failedVersionNumber, statusDetails);
                }
            } catch (Throwable e1) {
                String handlingErrorMsg = "Exception occurred while handling version " + versionNumber
                    + " creation failure for store " + storeName + " in cluster " + clusterName;
                logger.error(handlingErrorMsg, e1);
                errorMessage += "\n" + handlingErrorMsg + ". Error message: " + e1.getMessage();
            }
            throw new VeniceException(errorMessage + ". Detailed stack trace: " + ExceptionUtils.stackTraceToString(e), e);
        } finally {
            // If topic creation succeeds and version info is persisted to Zk, we should remove the creation time for a topic
            // If topic creation fails or if version persistence fails, we should remove the creation time for a topic
            if (version != null) {
                topicToCreationTime.remove(version.kafkaTopicName());
            }
        }
    }

    /**
     * During store migration, skip a version if:
     * This is the child controller of the destination cluster
     * And the kafka topic of related store and version is truncated
     *
     * @param clusterName the name of the cluster
     * @param storeName the name of the store
     * @param versionNumber version number
     * @return whether to skip adding the version
     */
    private boolean skipMigratingVersion(String clusterName, String storeName, int versionNumber) {
        if (multiClusterConfigs.isParent()) {
            return false;
        }
        StoreConfig storeConfig = storeConfigRepo.getStoreConfigOrThrow(storeName);
        String destCluster = storeConfig.getMigrationDestCluster();
        if (clusterName.equals(destCluster)) {
            String versionTopic = Version.composeKafkaTopic(storeName, versionNumber);
            // If the topic doesn't exist, we don't know whether it's not created or already deleted, so we don't skip
            return getTopicManager().containsTopic(versionTopic) && isTopicTruncated(versionTopic);
        }
        return false;
    }

    protected void handleVersionCreationFailure(String clusterName, String storeName, int versionNumber, String statusDetails){
        // Mark offline push job as Error and clean up resources because add version failed.
        PushMonitor offlinePushMonitor = getHelixVeniceClusterResources(clusterName).getPushMonitor();
        offlinePushMonitor.markOfflinePushAsError(Version.composeKafkaTopic(storeName, versionNumber), statusDetails);
        deleteOneStoreVersion(clusterName, storeName, versionNumber);
    }

    /**
     * Note: this currently use the pushID to guarantee idempotence, unexpected behavior may result if multiple
     * batch jobs push to the same store at the same time.
     */
    @Override
    public Version incrementVersionIdempotent(String clusterName, String storeName, String pushJobId,
        int numberOfPartitions, int replicationFactor, Version.PushType pushType, boolean sendStartOfPush, boolean sorted,
        String compressionDictionary, Optional<String> sourceGridFabric, Optional<X509Certificate> requesterCert,
        long rewindTimeInSecondsOverride, Optional<String> emergencySourceRegion) {
        checkControllerMastership(clusterName);

        return pushType.isIncremental() ? getIncrementalPushVersion(clusterName, storeName)
            : addVersion(clusterName, storeName, pushJobId, VERSION_ID_UNSET, numberOfPartitions, replicationFactor,
                true, sendStartOfPush, sorted, false, pushType,
                compressionDictionary, null, sourceGridFabric, rewindTimeInSecondsOverride,
                REPLICATION_METADATA_VERSION_ID_UNSET, emergencySourceRegion).getSecond();
    }

    /**
     * The intended semantic is to use this method to find the version that something is currently pushing to.  It looks
     * at all versions greater than the current version and identifies the version with a status of STARTED.  If there
     * is no STARTED version, it creates a new one for the push to use.  This means we cannot use this method to support
     * multiple concurrent pushes.
     *
     * @param store
     * @return the started version if there is only one, throws an exception if there is an error version with
     * a greater number than the current version.  Otherwise returns Optional.empty()
     */
    protected static Optional<Version> getStartedVersion(Store store){
        List<Version> startedVersions = new ArrayList<>();
        for (Version version : store.getVersions()) {
            if (version.getNumber() <= store.getCurrentVersion()) {
                continue;
            }
            switch (version.getStatus()) {
                case ONLINE:
                case PUSHED:
                    break; // These we can ignore
                case STARTED:
                    startedVersions.add(version);
                    break;
                case ERROR:
                case NOT_CREATED:
                default:
                    throw new VeniceException("Version " + version.getNumber() + " for store " + store.getName()
                        + " has status " + version.getStatus().toString() + ".  Cannot create a new version until this store is cleaned up.");
            }
        }
        if (startedVersions.size() == 1){
            return Optional.of(startedVersions.get(0));
        } else if (startedVersions.size() > 1) {
            String startedVersionsString = startedVersions.stream().map(Version::getNumber).map(n -> Integer.toString(n)).collect(Collectors.joining(","));
            throw new VeniceException("Store " + store.getName() + " has versions " + startedVersionsString + " that are all STARTED.  "
                + "Cannot create a new version while there are multiple STARTED versions");
        }
        return Optional.empty();
    }

    private Optional<Version> getVersionWithPushId(String clusterName, String storeName, String pushId) {
        Store store = getStore(clusterName, storeName);
        if (store == null) {
            throwStoreDoesNotExist(clusterName, storeName);
        }
        for (Version version : store.getVersions()) {
            if (version.getPushJobId().equals(pushId)) {
                logger.info("Version request for pushId " + pushId + " and store " + store.getName()
                    + ".  pushId already exists, so returning existing version " + version.getNumber());
                return Optional.of(version); // Early exit
            }
        }
        return Optional.empty();
    }

    @Override
    public String getRealTimeTopic(String clusterName, String storeName){
        checkControllerMastership(clusterName);
        TopicManager topicManager = getTopicManager();
        String realTimeTopic = Version.composeRealTimeTopic(storeName);
        if (topicManager.containsTopic(realTimeTopic)) {
            return realTimeTopic;
        } else {
            HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
            try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
                // The topic might be created by another thread already. Check before creating.
                if (topicManager.containsTopic(realTimeTopic)) {
                    return realTimeTopic;
                }
                ReadWriteStoreRepository repository = resources.getStoreMetadataRepository();
                Store store = repository.getStore(storeName);
                if (store == null) {
                    throwStoreDoesNotExist(clusterName, storeName);
                }
                if (!store.isHybrid() && !store.isWriteComputationEnabled()){
                    logAndThrow("Store " + storeName + " is not hybrid, refusing to return a realtime topic");
                }
                Optional<Version> version = store.getVersion(store.getLargestUsedVersionNumber());
                int partitionCount = version.isPresent() ? version.get().getPartitionCount() : 0;
                // during transition to version based partition count, some old stores may have partition count on the store config only.
                if (partitionCount == 0) {
                    partitionCount = store.getPartitionCount();
                }
                if (0 == partitionCount){
                    //TODO:  partitioning is currently decided on first version push, and we need to match that versioning
                    // we should evaluate alternatives such as allowing the RT topic request to initialize the number of
                    // partitions, or setting the number of partitions at store creation time instead of at first version
                    if (!version.isPresent()) {
                        throw new VeniceException("Store: " + storeName + " is not initialized with a version yet");
                    } else {
                        throw new VeniceException("Store: " + storeName + " has partition count set to 0");
                    }
                }
                VeniceControllerClusterConfig clusterConfig = getHelixVeniceClusterResources(clusterName).getConfig();

                getTopicManager().createTopic(
                    realTimeTopic,
                    partitionCount,
                    clusterConfig.getKafkaReplicationFactor(),
                    store.getRetentionTime(),
                    false, // Note: do not enable RT compaction! Might make jobs in Online/Offline model stuck
                    clusterConfig.getMinIsr(),
                    false
                );
                //TODO: if there is an online version from a batch push before this store was hybrid then we won't start
                // replicating to it.  A new version must be created.
                logger.warn("Creating real time topic per topic request for store " + storeName + ".  "
                  + "Buffer replay wont start for any existing versions");
            }
            return realTimeTopic;
        }
    }

    @Override
    public Version getIncrementalPushVersion(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
        try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreReadLock(storeName)) {
            Store store = resources.getStoreMetadataRepository().getStore(storeName);
            if (store == null) {
                throwStoreDoesNotExist(clusterName, storeName);
            }

            if (!store.isIncrementalPushEnabled()) {
                throw new VeniceException("Incremental push is not enabled for store: " + storeName);
            }

            List<Version> versions = store.getVersions();
            if (versions.isEmpty()) {
                throw new VeniceException("Store: " + storeName + " is not initialized with a version yet");
            }

            /**
             * Don't use {@link Store#getCurrentVersion()} here since it is always 0 in parent controller
             */
            Version version = versions.get(versions.size() - 1);
            if (version.getStatus() == ERROR) {
                throw new VeniceException("cannot have incremental push because current version is in error status. "
                    + "Version: " + version.getNumber() + " Store:" + storeName);
            }

            String kafkaTopic;
            if (version.getIncrementalPushPolicy().equals(IncrementalPushPolicy.INCREMENTAL_PUSH_SAME_AS_REAL_TIME)) {
              kafkaTopic = Version.composeRealTimeTopic(storeName);
            } else {
              kafkaTopic = Version.composeKafkaTopic(storeName, version.getNumber());
            }

            if (!getTopicManager().containsTopicAndAllPartitionsAreOnline(kafkaTopic) || isTopicTruncated(kafkaTopic)) {
              veniceHelixAdminStats.recordUnexpectedTopicAbsenceCount();
              throw new VeniceException("Incremental push cannot be started for store: " + storeName + " in cluster: "
                  + clusterName + " because the topic: " + kafkaTopic + " is either absent or being truncated");
            }
            return version;
        }
    }

    @Override
    public int getCurrentVersion(String clusterName, String storeName) {
        Store store = getStoreForReadOnly(clusterName, storeName);
        if (store.isEnableReads()) {
            return store.getCurrentVersion();
        } else {
            return Store.NON_EXISTING_VERSION;
        }
    }

    @Override
    public int getFutureVersion(String clusterName, String storeName) {
        // Find all ongoing offline pushes at first.
        PushMonitor monitor = getHelixVeniceClusterResources(clusterName).getPushMonitor();
        Optional<String> offlinePush = monitor.getTopicsOfOngoingOfflinePushes().stream()
                .filter(topic -> Version.parseStoreFromKafkaTopicName(topic).equals(storeName)).findFirst();
        if(offlinePush.isPresent()) {
            return Version.parseVersionFromKafkaTopicName(offlinePush.get());
        }
        return Store.NON_EXISTING_VERSION;
    }

    @Override
    public Map<String, Integer> getCurrentVersionsForMultiColos(String clusterName, String storeName) {
        return null;
    }

    @Override
    public Map<String, String> getFutureVersionsForMultiColos(String clusterName, String storeName) {
        return Collections.EMPTY_MAP;
    }

    @Override
    public Version peekNextVersion(String clusterName, String storeName) {
        Store store = getStoreForReadOnly(clusterName, storeName);
        Version version = store.peekNextVersion(); /* Does not modify the store */
        logger.info("Next version would be: " + version.getNumber() + " for store: " + storeName);
        return version;
    }

    @Override
    public List<Version> deleteAllVersionsInStore(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
        try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
            ReadWriteStoreRepository repository = resources.getStoreMetadataRepository();
            Store store = repository.getStore(storeName);
            checkPreConditionForDeletion(clusterName, storeName, store);
            logger.info("Deleting all versions in store: " + store + " in cluster: " + clusterName);
            // Set current version to NON_VERSION_AVAILABLE. Otherwise after this store is enabled again, as all of
            // version were deleted, router will get a current version which does not exist actually.
            store.setEnableWrites(true);
            store.setCurrentVersion(Store.NON_EXISTING_VERSION);
            store.setEnableWrites(false);
            repository.updateStore(store);
            List<Version> deletingVersionSnapshot = new ArrayList<>(store.getVersions());

            // Do not lock the entire deleting block, because during the deleting, controller would acquire repository lock
            // to query store when received the status update from storage node.
            for (Version version : deletingVersionSnapshot) {
                deleteOneStoreVersion(clusterName, version.getStoreName(), version.getNumber());
            }
            logger.info("Deleted all versions in store: " + storeName + " in cluster: " + clusterName);
            return deletingVersionSnapshot;
        }
    }

    @Override
    public void deleteOldVersionInStore(String clusterName, String storeName, int versionNum) {
        checkControllerMastership(clusterName);
        HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
        try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
            if (VeniceSystemStoreType.getSystemStoreType(storeName) == VeniceSystemStoreType.METADATA_STORE) {
                logger.info("Deleting version: " + versionNum + " in store: " + storeName + " in cluster: " + clusterName);
                deleteMetadataStoreVersion(clusterName, storeName, versionNum);
            } else {
                ReadWriteStoreRepository repository = resources.getStoreMetadataRepository();
                Store store = repository.getStore(storeName);
                // Here we do not require the store be disabled. So it might impact reads
                // The thing is a new version is just online, now we delete the old version. So some of routers
                // might still use the old one as the current version, so when they send the request to that version,
                // they will get error response.
                // TODO the way to solve this could be: Introduce a timestamp to represent when the version is online.
                // TOOD And only allow to delete the old version that the newer version has been online for a while.
                checkPreConditionForSingleVersionDeletion(clusterName, storeName, store, versionNum);
                if (!store.containsVersion(versionNum)) {
                    logger.warn("Ignore the deletion request. Could not find version: " + versionNum + " in store: " + storeName
                        + " in cluster: " + clusterName);
                    return;
                }
                logger.info("Deleting version: " + versionNum + " in store: " + storeName + " in cluster: " + clusterName);
                deleteOneStoreVersion(clusterName, storeName, versionNum);
            }
            logger.info("Deleted version: " + versionNum + " in store: " + storeName + " in cluster: " + clusterName);
        }
    }

    /**
     * Delete version from cluster, removing all related resources
     */
    @Override
    public void deleteOneStoreVersion(String clusterName, String storeName, int versionNumber) {
        HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
        try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
            Store store = resources.getStoreMetadataRepository().getStore(storeName);
            Store storeToCheckOngoingMigration =
                VeniceSystemStoreUtils.getSystemStoreType(storeName) == VeniceSystemStoreType.METADATA_STORE ?
                    resources.getStoreMetadataRepository()
                        .getStore(VeniceSystemStoreUtils.getStoreNameFromSystemStoreName(storeName)) : store;
            Optional<Version> versionToBeDeleted = store.getVersion(versionNumber);
            if (!versionToBeDeleted.isPresent()) {
                logger.info("Version: " + versionNumber + " doesn't exist in store: " + storeName + ", will skip `deleteOneStoreVersion`");
                return;
            }
            String resourceName = Version.composeKafkaTopic(storeName, versionNumber);
            logger.info("Deleting helix resource:" + resourceName + " in cluster:" + clusterName);
            deleteHelixResource(clusterName, resourceName);
            logger.info("Killing offline push for:" + resourceName + " in cluster:" + clusterName);
            killOfflinePush(clusterName, resourceName, true);

            if (!versionToBeDeleted.get().isLeaderFollowerModelEnabled() && onlineOfflineTopicReplicator.isPresent()) {
                // Do not delete topic replicator during store migration
                // In such case, the topic replicator will be deleted after store migration, triggered by a new push job
                if (!storeToCheckOngoingMigration.isMigrating()) {
                    String realTimeTopic = Version.composeRealTimeTopic(storeName);
                    onlineOfflineTopicReplicator.get().terminateReplication(realTimeTopic, resourceName);
                }
            }

            stopMonitorOfflinePush(clusterName, resourceName, true);
            Optional<Version> deletedVersion = deleteVersionFromStoreRepository(clusterName, storeName, versionNumber);
            if (deletedVersion.isPresent()) {
                // Do not delete topic during store migration
                // In such case, the topic will be deleted after store migration, triggered by a new push job
                if (!storeToCheckOngoingMigration.isMigrating()) {
                    // Not using deletedVersion.get().kafkaTopicName() because it's incorrect for Zk shared stores.
                    truncateKafkaTopic(Version.composeKafkaTopic(storeName, deletedVersion.get().getNumber()));
                    if (deletedVersion.get().getPushType().isStreamReprocessing()) {
                        truncateKafkaTopic(Version.composeStreamReprocessingTopic(storeName, versionNumber));
                    }
                }
                if (store.isDaVinciPushStatusStoreEnabled() && pushStatusStoreDeleter.isPresent()) {
                    pushStatusStoreDeleter.get().deletePushStatus(storeName, deletedVersion.get().getNumber(), Optional.empty(), deletedVersion.get().getPartitionCount());
                }
            }
        }
    }

    // TODO to be removed once legacy system store resources are cleaned up.
    private void deleteMetadataStoreVersion(String clusterName, String storeName, int versionNumber) {
        if (VeniceSystemStoreUtils.getSharedZkNameForMetadataStore(clusterName).equals(storeName)
            || VeniceSystemStoreType.METADATA_STORE.getPrefix().equals(storeName)) {
            // Versions of the Zk shared store object itself will never be materialized.
            return;
        }
        HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
        try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
            String resourceName = Version.composeKafkaTopic(storeName, versionNumber);
            logger.info("Deleting helix resource:" + resourceName + " in cluster:" + clusterName);
            deleteHelixResource(clusterName, resourceName);
            logger.info("Killing offline push for:" + resourceName + " in cluster:" + clusterName);
            killOfflinePush(clusterName, resourceName, true);
            String realTimeTopic = Version.composeRealTimeTopic(storeName);
            onlineOfflineTopicReplicator.get().terminateReplication(realTimeTopic, resourceName);
            truncateKafkaTopic(Version.composeKafkaTopic(storeName, versionNumber));
        }
    }

    @Override
    public void retireOldStoreVersions(String clusterName, String storeName, boolean deleteBackupOnStartPush) {
        HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
        try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
            Store store = resources.getStoreMetadataRepository().getStore(storeName);

            //  if deleteBackupOnStartPush is true decrement minNumberOfStoreVersionsToPreserve by one
            // as newly started push is considered as another version. the code in retrieveVersionsToDelete
            // will not return any store if we pass minNumberOfStoreVersionsToPreserve during push
            int numVersionToPreserve = minNumberOfStoreVersionsToPreserve - (deleteBackupOnStartPush ? 1 : 0);
            List<Version> versionsToDelete = store.retrieveVersionsToDelete(numVersionToPreserve);
            if (versionsToDelete.isEmpty()) {
                return;
            }

            if (store.getBackupStrategy() == BackupStrategy.DELETE_ON_NEW_PUSH_START) {
                logger.info("Deleting backup versions as the new push started for upcoming version for store : " + storeName);
            } else {
                logger.info("Retiring old versions after successful push for store : " + storeName);

            }

            for (Version version : versionsToDelete) {
                try {
                    deleteOneStoreVersion(clusterName, storeName, version.getNumber());
                } catch (VeniceException e) {
                    logger.warn("Could not delete store " + storeName + " version number " + version.getNumber()
                        + " in cluster " + clusterName, e);
                    continue;
                }
                logger.info("Retired store: " + store.getName() + " version:" + version.getNumber());
            }

            logger.info("Retired " + versionsToDelete.size() + " versions for store: " + storeName);

            truncateOldTopics(clusterName, store, false);
        }
    }

    /**
     * In this function, Controller will setup proper compaction strategy when the push job is full completed, and here are the
     * reasons to set it up after the job completes:
     * 1. For batch push jobs to batch-only store, there is no impact. There could still be duplicate entries because of
     * speculative executions in map-reduce job, but we are not planning to clean them up now.
     * 2. For batch push jobs to hybrid/incremental stores, if the compaction is enabled at the beginning of the job,
     * Kafka compaction could kick in during push job, and storage node could detect DIV error, such as missing messages,
     * checksum mismatch, because speculative execution could produce duplicate entries, and we don't want to fail the push in this scenario
     * and we still want to perform the strong DIV validation in batch push, so we could only enable compaction after the batch push completes.
     * 3. For GF jobs to hybrid store, it is similar as #2, and it contains duplicate entries because there is no de-dedup
     * happening anywhere.
     *
     * With this way, when load rebalance happens for hybrid/incremental stores, DIV error could be detected during ingestion
     * at any phase since compaction might be enabled long-time ago. So in storage node, we need to add one more safeguard
     * before throwing the DIV exception to check whether the topic is compaction-enabled or not.
     * Since Venice is not going to change the compaction policy between non-compact and compact back and forth, checking
     * whether topic is compaction-enabled or not when encountering DIV error should be good enough.
     */
    @Override
    public void topicCleanupWhenPushComplete(String clusterName, String storeName, int versionNumber) {
        HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
        VeniceControllerClusterConfig clusterConfig = resources.getConfig();
        ReadWriteStoreRepository storeRepository = resources.getStoreMetadataRepository();
        Store store = storeRepository.getStore(storeName);
        if ((store.isHybrid() && clusterConfig.isKafkaLogCompactionForHybridStoresEnabled())
            || (store.isIncrementalPushEnabled() && clusterConfig.isKafkaLogCompactionForIncrementalPushStoresEnabled())) {
            getTopicManager().updateTopicCompactionPolicy(Version.composeKafkaTopic(storeName, versionNumber), true);
        }
    }

    /**
     * Delete the version specified from the store and return the deleted version. Zk shared store will only return the
     * version to be deleted (if it exists) and not actually removing it from the {@link Store} object.
     */
    protected Optional<Version> deleteVersionFromStoreRepository(String clusterName, String storeName, int versionNumber) {
        HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
        logger.info("Deleting version " + versionNumber + " in Store: " + storeName + " in cluster: " + clusterName);
        Optional<Version> deletedVersion;
        try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
            ReadWriteStoreRepository storeRepository = resources.getStoreMetadataRepository();
            Store store = storeRepository.getStore(storeName);
            if (store == null) {
                throw new VeniceNoStoreException(storeName);
            }
            VeniceSystemStoreType systemStore = VeniceSystemStoreUtils.getSystemStoreType(storeName);

            if (systemStore != null && systemStore.isStoreZkShared()) {
                deletedVersion = store.getVersion(versionNumber);
            } else {
              /**
               * If the system store type is {@link VeniceSystemStoreType.META_STORE}, we will use the same logic
               * as the regular Venice store here since it also contains multiple versions maintained on system store basis.
               */
                Version version = store.deleteVersion(versionNumber);
                if (version == null) {
                    deletedVersion = Optional.empty();
                } else {
                    deletedVersion = Optional.of(version);
                    storeRepository.updateStore(store);
                }
            }
            if (!deletedVersion.isPresent()) {
                logger.warn("Can not find version: " + versionNumber + " in store: " + storeName + ".  It has probably already been deleted");
            }
        }
        logger.info("Deleted version " + versionNumber + " in Store: " + storeName + " in cluster: " + clusterName);
        return deletedVersion;
    }

    @Override
    public boolean isTopicTruncated(String kafkaTopicName) {
        return getTopicManager().isTopicTruncated(kafkaTopicName, deprecatedJobTopicMaxRetentionMs);
    }

    @Override
    public boolean isTopicTruncatedBasedOnRetention(long retention) {
        return getTopicManager().isRetentionBelowTruncatedThreshold(retention, deprecatedJobTopicMaxRetentionMs);
    }

    @Override
    public int getMinNumberOfUnusedKafkaTopicsToPreserve() {
        return multiClusterConfigs.getCommonConfig().getMinNumberOfUnusedKafkaTopicsToPreserve();
    }

    /**
     * We don't actually truncate any Kafka topic here; we just update the retention time.
     * @param kafkaTopicName
     * @return
     */
    @Override
    public boolean truncateKafkaTopic(String kafkaTopicName) {
        /**
         * Topic truncation doesn't care about whether the topic actually exists in Kafka Broker or not since
         * the truncation will only update the topic metadata in Kafka Zookeeper.
         * This logic is used to avoid the scenario that the topic exists in Kafka Zookeeper, but not fully ready in
         * Kafka Broker yet.
         */
        if (multiClusterConfigs.isParent()) {
            return truncateKafkaTopicInParent(kafkaTopicName);
        } else {
            return truncateKafkaTopic(getTopicManager(), kafkaTopicName);
        }
    }

    private boolean truncateKafkaTopic(String kafkaTopicName, Map<String, Properties> topicConfigs) {
        if (multiClusterConfigs.isParent()) {
            /**
             * topicConfigs is ignored on purpose, since we couldn't guarantee configs are in sync in
             * different Kafka clusters.
             */
            return truncateKafkaTopicInParent(kafkaTopicName);
        } else {
            return truncateKafkaTopic(getTopicManager(), kafkaTopicName, topicConfigs);
        }
    }

    /**
     * Iterate through all Kafka clusters in parent fabric to truncate the same topic name
     */
    private boolean truncateKafkaTopicInParent(String kafkaTopicName) {
        boolean allTopicsAreDeleted = true;
        Set<String> parentFabrics = multiClusterConfigs.getParentFabrics();
        for (String parentFabric : parentFabrics) {
            Pair<String, String> kafkaUrlAndZk = getNativeReplicationKafkaBootstrapServerAndZkAddress(parentFabric);
            allTopicsAreDeleted &= truncateKafkaTopic(getTopicManager(kafkaUrlAndZk), kafkaTopicName);
        }
        return allTopicsAreDeleted;
    }

    private boolean truncateKafkaTopic(TopicManager topicManager, String kafkaTopicName) {
        try {
            if (topicManager.updateTopicRetention(kafkaTopicName, deprecatedJobTopicRetentionMs)) {
                return true;
            }
        } catch (TopicDoesNotExistException e) {
            logger.warn(String.format("Unable to update the retention for topic %s in Kafka cluster %s since the topic doesn't " +
                    "exist in Kafka anymore, will skip the truncation", kafkaTopicName, topicManager.getKafkaBootstrapServers()));
        }
        return false;
    }

    private boolean truncateKafkaTopic(TopicManager topicManager, String kafkaTopicName, Map<String, Properties> topicConfigs) {
        if (topicConfigs.containsKey(kafkaTopicName)) {
            if (topicManager.updateTopicRetention(kafkaTopicName, deprecatedJobTopicRetentionMs, topicConfigs.get(kafkaTopicName))) {
                return true;
            }
        } else {
            logger.info("Topic: " + kafkaTopicName + " doesn't exist or not found in the configs, will skip the truncation");
        }
        // return false to indicate the retention config has already been updated.
        return false;
    }

    /**
     * Truncate old unused Kafka topics. These could arise from resource leaking of topics during certain
     * scenarios.
     *
     * If it is for store deletion, this function will truncate all the topics;
     * Otherwise, it will only truncate topics without corresponding active version.
     *
     * N.B.: visibility is package-private to ease testing...
     *
     * @param store for which to clean up old topics
     * @param forStoreDeletion
     *
     */
    void truncateOldTopics(String clusterName, Store store, boolean forStoreDeletion) {
        if (store.isMigrating())  {
            logger.info("This store " + store.getName() + " is being migrated. Skip topic deletion.");
            return;
        }

        Set<Integer> currentlyKnownVersionNumbers = store.getVersions().stream()
            .map(version -> version.getNumber())
            .collect(Collectors.toSet());

        Set<String> allTopics = getTopicManager().listTopics();
        List<String> allTopicsRelatedToThisStore = allTopics.stream()
            /** Exclude RT buffer topics, admin topics and all other special topics */
            .filter(t -> Version.isVersionTopicOrStreamReprocessingTopic(t))
            /** Keep only those topics pertaining to the store in question */
            .filter(t -> Version.parseStoreFromKafkaTopicName(t).equals(store.getName()))
            .collect(Collectors.toList());

        if (allTopicsRelatedToThisStore.isEmpty()) {
            logger.info("Searched for old topics belonging to store '" + store.getName() + "', and did not find any.");
            return;
        }
        List<String> oldTopicsToTruncate = allTopicsRelatedToThisStore;
        if (!forStoreDeletion) {
            /**
             * For store version deprecation, controller will truncate all the topics without corresponding versions and
             * the version belonging to is smaller than the largest used version of current store.
             *
             * The reason to check whether the to-be-deleted version is smaller than the largest used version of current store or not:
             * 1. Topic could be created either by Kafka MM or addVersion function call (triggered by
             * {@link com.linkedin.venice.controller.kafka.consumer.AdminConsumptionTask};
             * 2. If the topic is created by Kafka MM and the actual version creation gets delayed for some reason, the following
             * scenario could happen (assuming the current version is n):
             *   a. Topics: store_v(n-2), store_v(n-1), store_v(n), store_v(n+1) could exist at the same time because of the actual
             *     version creation gets delayed;
             *   b. The largest used version of current store is (n).
             * In this scenario, Controller should only deprecate store_v(n-2), instead of both store_v(n-2) and store_v(n+1) [because
             * of no corresponding store version], since the later one is still valid.
             */
            oldTopicsToTruncate = allTopicsRelatedToThisStore.stream().
                filter((topic) -> {
                    int versionForCurrentTopic = Version.parseVersionFromKafkaTopicName(topic);
                    return !currentlyKnownVersionNumbers.contains(versionForCurrentTopic) &&
                        versionForCurrentTopic <= store.getLargestUsedVersionNumber();
                })
                .collect(Collectors.toList());
        }

        if (oldTopicsToTruncate.isEmpty()) {
            logger.info("Searched for old topics belonging to store '" + store.getName() + "', and did not find any.");
        } else {
            logger.info("Detected the following old topics to truncate: " + String.join(", ", oldTopicsToTruncate));
            int numberOfNewTopicsMarkedForDelete = 0;
            Map<String, Properties> topicConfigs = getTopicManager().getAllTopicConfig();
            for (String t : oldTopicsToTruncate) {
                if (truncateKafkaTopic(t, topicConfigs)) {
                    ++numberOfNewTopicsMarkedForDelete;
                }
                deleteHelixResource(clusterName, t);
            }
            logger.info(String.format("Deleted %d old HelixResources for store '%s'.", numberOfNewTopicsMarkedForDelete, store.getName()));
            logger.info(String.format(
                "Finished truncating old topics for store '%s'. Retention time for %d topics out of %d have been updated.",
                store.getName(), numberOfNewTopicsMarkedForDelete, oldTopicsToTruncate.size()));
        }
    }

    /***
     * If you need to do mutations on the store, then you must hold onto the lock until you've persisted your mutations.
     * Only use this method if you're doing read-only operations on the store.
     * @param clusterName
     * @param storeName
     * @return
     */
    private Store getStoreForReadOnly(String clusterName, String storeName){
        checkControllerMastership(clusterName);
        HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
        try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreReadLock(storeName)) {
            Store store = resources.getStoreMetadataRepository().getStore(storeName);
            if(store == null){
                throw new VeniceNoStoreException(storeName);
            }
            return store; /* is a clone */
        }
    }

    @Override
    public List<Version> versionsForStore(String clusterName, String storeName){
        checkControllerMastership(clusterName);
        HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
        List<Version> versions;
        try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreReadLock(storeName)) {
            Store store = resources.getStoreMetadataRepository().getStore(storeName);
            if(store == null){
                throw new VeniceNoStoreException(storeName);
            }
            versions = store.getVersions();
        }
        return versions;
    }

    @Override
    public List<Store> getAllStores(String clusterName){
        checkControllerMastership(clusterName);
        HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
        return resources.getStoreMetadataRepository().getAllStores();
    }

    @Override
    public Map<String, String> getAllStoreStatuses(String clusterName) {
        checkControllerMastership(clusterName);
        HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
        List<Store> storeList = resources.getStoreMetadataRepository().getAllStores();
        RoutingDataRepository routingDataRepository =
            getHelixVeniceClusterResources(clusterName).getRoutingDataRepository();
        ResourceAssignment resourceAssignment = routingDataRepository.getResourceAssignment();
        return StoreStatusDecider.getStoreStatues(storeList, resourceAssignment,
            getHelixVeniceClusterResources(clusterName).getPushMonitor());
    }

    @Override
    public boolean hasStore(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
        try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreReadLock(storeName)) {
            return resources.getStoreMetadataRepository().hasStore(storeName);
        }
    }

    @Override
    public Store getStore(String clusterName, String storeName){
        checkControllerMastership(clusterName);
        ReadWriteStoreRepository repository = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
        return repository.getStore(storeName);
    }

    public Pair<Store, Version> waitVersion(String clusterName, String storeName, int versionNumber, Duration timeout) {
        checkControllerMastership(clusterName);
        ReadWriteStoreRepository repository = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
        return repository.waitVersion(storeName, versionNumber, timeout);
    }

    @Override
    public void setStoreCurrentVersion(String clusterName, String storeName, int versionNumber){
        storeMetadataUpdate(clusterName, storeName, store -> {
            if (store.getCurrentVersion() != Store.NON_EXISTING_VERSION) {
                if (!store.containsVersion(versionNumber)) {
                    throw new VeniceException("Version:" + versionNumber + " does not exist for store:" + storeName);
                }

                if (!store.isEnableWrites()) {
                    throw new VeniceException("Unable to update store:" + storeName + " current version since store writeability is false");
                }
            }
            store.setCurrentVersion(versionNumber);

            return store;
        });
    }

    @Override
    public void setStoreLargestUsedVersion(String clusterName, String storeName, int versionNumber) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setLargestUsedVersionNumber(versionNumber);
            return store;
        });
    }

    @Override
    public void setStoreOwner(String clusterName, String storeName, String owner) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setOwner(owner);
            return store;
        });
    }

    /**
     * Since partition check/calculation only happens when adding new store version, {@link #setStorePartitionCount(String, String, int)}
     * would only change the number of partition for the following pushes. Current version would not be changed.
     */
    @Override
    public void setStorePartitionCount(String clusterName, String storeName, int partitionCount) {
        VeniceControllerClusterConfig clusterConfig = getHelixVeniceClusterResources(clusterName).getConfig();
        storeMetadataUpdate(clusterName, storeName, store -> {
            if (store.getPartitionCount() != partitionCount && store.isHybrid()) {
                throw new ConfigurationException("Cannot change partition count for a hybrid store");
            }

            int maxPartitionNum = clusterConfig.getMaxNumberOfPartition();
            if (partitionCount > maxPartitionNum) {
                throw new ConfigurationException("Partition count: "
                    + partitionCount + " should be less than max: " + maxPartitionNum);
            }
            if (partitionCount < 0) {
                throw new ConfigurationException("Partition count: "
                    + partitionCount + " should NOT be negative");
            }

            // Do not update the partitionCount on the store.version as version config is immutable. The version.getPartitionCount()
            // is read only in getRealTimeTopic and createInternalStore creation, so modifying currentVersion should not have any effect.
            if (partitionCount != 0) {
                store.setPartitionCount(partitionCount);
            } else {
                store.setPartitionCount(clusterConfig.getNumberOfPartition());
            }

            return store;
        });
    }

    public void setStorePartitionerConfig(String clusterName, String storeName, PartitionerConfig partitionerConfig) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            // Only amplification factor is allowed to be changed if the store is a hybrid store.
            if (store.isHybrid() && !(Objects.equals(store.getPartitionerConfig(), partitionerConfig) || isAmplificationFactorUpdateOnly(store.getPartitionerConfig(), partitionerConfig))) {
                throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, "Partitioner config change from "
                    + store.getPartitionerConfig() + " to " + partitionerConfig + " in hybrid store is not supported except amplification factor.");
            } else {
                store.setPartitionerConfig(partitionerConfig);
                return store;
            }
        });
    }

    @Override
    public void setStoreWriteability(String clusterName, String storeName, boolean desiredWriteability) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setEnableWrites(desiredWriteability);

            return store;
        });
    }

    @Override
    public void setStoreReadability(String clusterName, String storeName, boolean desiredReadability) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setEnableReads(desiredReadability);

            return store;
        });
    }

    @Override
    public void setStoreReadWriteability(String clusterName, String storeName, boolean isAccessible) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setEnableReads(isAccessible);
            store.setEnableWrites(isAccessible);

            return store;
        });
    }

    /**
     * We will not expose this interface to Spark server. Updating quota can only be done by #updateStore
     * TODO: remove all store attribute setters.
     */
    private void setStoreStorageQuota(String clusterName, String storeName, long storageQuotaInByte) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            if (storageQuotaInByte < 0 && storageQuotaInByte != Store.UNLIMITED_STORAGE_QUOTA) {
                throw new VeniceException("storage quota can not be less than 0");
            }
            store.setStorageQuotaInByte(storageQuotaInByte);

            return store;
        });
    }

    private void setStoreReadQuota(String clusterName, String storeName, long readQuotaInCU) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            if (readQuotaInCU < 0) {
                throw new VeniceException("read quota can not be less than 0");
            }
            store.setReadQuotaInCU(readQuotaInCU);

            return store;
        });
    }

    public void setAccessControl(String clusterName, String storeName, boolean accessControlled) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setAccessControlled(accessControlled);

            return store;
        });
    }

    public void setStoreCompressionStrategy(String clusterName, String storeName,
                                                          CompressionStrategy compressionStrategy) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setCompressionStrategy(compressionStrategy);

            return store;
        });
    }

    public void setClientDecompressionEnabled(String clusterName, String storeName, boolean clientDecompressionEnabled) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setClientDecompressionEnabled(clientDecompressionEnabled);
            return store;
        });
    }

    public void setChunkingEnabled(String clusterName, String storeName,
        boolean chunkingEnabled) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setChunkingEnabled(chunkingEnabled);

            return store;
        });
    }

    public void setIncrementalPushEnabled(String clusterName, String storeName, boolean incrementalPushEnabled) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            IncrementalPushPolicy incrementalPushPolicy = store.getIncrementalPushPolicy();
            if (incrementalPushEnabled && store.isHybrid() && incrementalPushPolicy.equals(IncrementalPushPolicy.PUSH_TO_VERSION_TOPIC)) {
                throw new VeniceException("hybrid store doesn't support incremental push with policy " + incrementalPushPolicy);
            }
            VeniceControllerClusterConfig config = getHelixVeniceClusterResources(clusterName).getConfig();
            if (incrementalPushEnabled) {
                // Enabling incremental push
                if (config.isLeaderFollowerEnabledForIncrementalPushStores()) {
                    store.setLeaderFollowerModelEnabled(true);
                }
                store.setActiveActiveReplicationEnabled(config.isActiveActiveReplicationEnabledAsDefaultForIncremental());
                store.setNativeReplicationEnabled(config.isNativeReplicationEnabledAsDefaultForIncremental());
                store.setNativeReplicationSourceFabric(config.getNativeReplicationSourceFabricAsDefaultForIncremental());
            } else {
                // Disabling incremental push
                if (store.isHybrid()) {
                    store.setActiveActiveReplicationEnabled(config.isActiveActiveReplicationEnabledAsDefaultForHybrid());
                    store.setNativeReplicationEnabled(config.isNativeReplicationEnabledAsDefaultForHybrid());
                    store.setNativeReplicationSourceFabric(config.getNativeReplicationSourceFabricAsDefaultForHybrid());
                } else {
                    store.setActiveActiveReplicationEnabled(config.isActiveActiveReplicationEnabledAsDefaultForBatchOnly());
                    store.setNativeReplicationEnabled(config.isNativeReplicationEnabledAsDefaultForBatchOnly());
                    store.setNativeReplicationSourceFabric(config.getNativeReplicationSourceFabricAsDefaultForBatchOnly());
                }
            }
            store.setIncrementalPushEnabled(incrementalPushEnabled);

            return  store;
        });
    }

    public void setReplicationFactor(String clusterName, String storeName, int replicaFactor) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setReplicationFactor(replicaFactor);

            return store;
        });
    }

    public void setBatchGetLimit(String clusterName, String storeName,
        int batchGetLimit) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setBatchGetLimit(batchGetLimit);

            return store;
        });
    }

    public void setNumVersionsToPreserve(String clusterName, String storeName,
        int numVersionsToPreserve) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setNumVersionsToPreserve(numVersionsToPreserve);

            return store;
        });
    }

    public void setStoreMigration(String clusterName, String storeName, boolean migrating) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setMigrating(migrating);
            return store;
        });
    }

    public void setMigrationDuplicateStore(String clusterName, String storeName,
        boolean migrationDuplicateStore) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setMigrationDuplicateStore(migrationDuplicateStore);
            return store;
        });
    }

    public void setWriteComputationEnabled(String clusterName, String storeName,
        boolean writeComputationEnabled) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setWriteComputationEnabled(writeComputationEnabled);
            return store;
        });
    }

    public void setReadComputationEnabled(String clusterName, String storeName,
        boolean computationEnabled) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setReadComputationEnabled(computationEnabled);
            return store;
        });
    }

    public void setBootstrapToOnlineTimeoutInHours(String clusterName, String storeName,
        int bootstrapToOnlineTimeoutInHours) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setBootstrapToOnlineTimeoutInHours(bootstrapToOnlineTimeoutInHours);
            return store;
        });
    }

    public void setLeaderFollowerModelEnabled(String clusterName, String storeName,
        boolean leaderFollowerModelEnabled) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setLeaderFollowerModelEnabled(leaderFollowerModelEnabled);
            return store;
        });
    }

    public void enableLeaderFollowerModelLocally(String clusterName, String storeName,
            boolean leaderFollowerModelEnabled) {
        setLeaderFollowerModelEnabled(clusterName, storeName, leaderFollowerModelEnabled);
    }

    public void setNativeReplicationEnabled(String clusterName, String storeName,
        boolean nativeReplicationEnabled) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setNativeReplicationEnabled(nativeReplicationEnabled);
            return store;
        });
    }

    public void setPushStreamSourceAddress(String clusterName, String storeName, String pushStreamSourceAddress) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setPushStreamSourceAddress(pushStreamSourceAddress);
            return store;
        });
    }

    public void setBackupStrategy(String clusterName, String storeName,
        BackupStrategy backupStrategy) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setBackupStrategy(backupStrategy);
            return store;
        });
    }

    public void setAutoSchemaRegisterPushJobEnabled(String clusterName, String storeName,
        boolean autoSchemaRegisterPushJobEnabled) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setSchemaAutoRegisterFromPushJobEnabled(autoSchemaRegisterPushJobEnabled);
            return store;
        });
    }

    public void setHybridStoreDiskQuotaEnabled(String clusterName, String storeName,
        boolean hybridStoreDiskQuotaEnabled) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setHybridStoreDiskQuotaEnabled(hybridStoreDiskQuotaEnabled);
            return store;
        });
    }

    public void setIncrementalPushPolicy(String clusterName, String storeName,
        IncrementalPushPolicy incrementalPushPolicy) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setIncrementalPushPolicy(incrementalPushPolicy);
            return store;
        });
    }

    public void setBackupVersionRetentionMs(String clusterName, String storeName,
        long backupVersionRetentionMs) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setBackupVersionRetentionMs(backupVersionRetentionMs);
            return store;
        });
    }

    public void setNativeReplicationSourceFabric(String clusterName, String storeName, String nativeReplicationSourceFabric) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setNativeReplicationSourceFabric(nativeReplicationSourceFabric);
            return store;
        });
    }

    public void setActiveActiveReplicationEnabled(String clusterName, String storeName, boolean activeActiveReplicationEnabled) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setActiveActiveReplicationEnabled(activeActiveReplicationEnabled);
            return store;
        });
    }

    public void setApplyTargetVersionFilterForIncPush(String clusterName, String storeName, boolean applyTargetVersionFilterForIncPush) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setApplyTargetVersionFilterForIncPush(applyTargetVersionFilterForIncPush);
            return store;
        });
    }

    /**
     * This function will check whether the store update will cause the case that a store can not have the specified
     * hybrid store and incremental push configs.
     *
     * @param store The store object whose configs are being updated
     * @param newIncrementalPushEnabled The new incremental push enabled config that will be set for the store
     * @param newIncrementalPushPolicy The new incremental push policy that will be set for the store
     * @param newHybridStoreConfig The new hybrid store config that will be set for the store
     */
    protected void checkWhetherStoreWillHaveConflictConfigForIncrementalAndHybrid(Store store,
        Optional<Boolean> newIncrementalPushEnabled,
        Optional<IncrementalPushPolicy> newIncrementalPushPolicy,
        Optional<HybridStoreConfig> newHybridStoreConfig) {
        String storeName = store.getName();

        boolean finalIncrementalPushEnabled = newIncrementalPushEnabled.orElse(store.isIncrementalPushEnabled());
        IncrementalPushPolicy finalIncrementalPushPolicy = newIncrementalPushPolicy.orElse(store.getIncrementalPushPolicy());
        HybridStoreConfig finalHybridStoreConfig = newHybridStoreConfig.orElse(store.getHybridStoreConfig());
        boolean finalHybridEnabled = isHybrid(finalHybridStoreConfig);

        if (finalIncrementalPushEnabled && finalHybridEnabled && !finalIncrementalPushPolicy.isCompatibleWithHybridStores()) {
            throw new VeniceException("Hybrid and incremental push cannot be enabled simultaneously for store: " + storeName
                + " since it has incremental push policy: " + finalIncrementalPushPolicy.name());
        } else if (finalIncrementalPushEnabled && !finalHybridEnabled && !finalIncrementalPushPolicy.isCompatibleWithNonHybridStores()) {
            throw new VeniceException("Incremental push with incremental push policy " + finalIncrementalPushPolicy.name()
                + " cannot be enabled for store: " + storeName + " unless it also enables hybrid store.");
        }
    }

    /**
     * This function will check whether the store update will cause the case that a store can not have the specified
     * hybrid store and compression strategy configs.
     *
     * @param store The store object whose configs are being updated
     * @param newCompressionStrategy The new incremental push enabled config that will be set for the store
     * @param newHybridStoreConfig The new hybrid store config that will be set for the store
     */
    protected void checkWhetherStoreWillHaveConflictConfigForCompressionAndHybrid(Store store,
        Optional<CompressionStrategy> newCompressionStrategy,
        Optional<HybridStoreConfig> newHybridStoreConfig) {
        String storeName = store.getName();

        final CompressionStrategy finalCompressionStrategy = newCompressionStrategy.orElse(store.getCompressionStrategy());
        final HybridStoreConfig currentHybridStoreConfig = store.getVersion(store.getCurrentVersion())
                .filter(Version::isUseVersionLevelHybridConfig)
                .map(Version::getHybridStoreConfig)
                .orElse(store.getHybridStoreConfig());

        final HybridStoreConfig finalHybridStoreConfig = newHybridStoreConfig.orElse(currentHybridStoreConfig);
        boolean finalHybridEnabled = isHybrid(finalHybridStoreConfig);

        // Hybrid stores can only have NoOpCompression
        if (finalHybridEnabled && !CompressionStrategy.NO_OP.equals(finalCompressionStrategy)) {
            throw new VeniceException("Hybrid and compression cannot be enabled simultaneously for store: " + storeName
                + " since it has compression strategy: " + finalCompressionStrategy.name());
        }
    }

    /**
     * TODO: some logics are in parent controller {@link VeniceParentHelixAdmin} #updateStore and
     *       some are in the child controller here. Need to unify them in the future.
     */
    @Override
    public void updateStore(String clusterName, String storeName, UpdateStoreQueryParams params) {
        checkControllerMastership(clusterName);
        HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
        try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
            internalUpdateStore(clusterName, storeName, params);
        }
    }

    private void internalUpdateStore(String clusterName, String storeName, UpdateStoreQueryParams params) {
        /**
         * Check whether the command affects this fabric.
         */
        if (params.getRegionsFilter().isPresent()) {
            Set<String> regionsFilter = parseRegionsFilterList(params.getRegionsFilter().get());
            if (!regionsFilter.contains(multiClusterConfigs.getRegionName())) {
                logger.info("UpdateStore command will be skipped for store " + storeName + " in cluster " + clusterName
                    + ", because the fabrics filter is " + regionsFilter.toString() + " which doesn't include the "
                    + "current fabric: " + multiClusterConfigs.getRegionName());
                return;
            }
        }

        Store originalStore = getStore(clusterName, storeName);
        if (null == originalStore) {
            throw new VeniceException("The store '" + storeName + "' in cluster '" + clusterName + "' does not exist, and thus cannot be updated.");
        }
        if (originalStore.isHybrid()) {
            // If this is a hybrid store, always try to disable compaction if RT topic exists.
            try {
                getTopicManager().updateTopicCompactionPolicy(Version.composeRealTimeTopic(storeName), false);
            } catch (TopicDoesNotExistException e) {
                logger.error(String.format("Could not find realtime topic for hybrid store %s", storeName));
            }
        }

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
        Optional<Boolean> applyTargetVersionFilterForIncPush = params.applyTargetVersionFilterForIncPush();

        final Optional<HybridStoreConfig> newHybridStoreConfig;
        if (hybridRewindSeconds.isPresent() || hybridOffsetLagThreshold.isPresent()
            || hybridTimeLagThreshold.isPresent() || hybridDataReplicationPolicy.isPresent()
            || hybridBufferReplayPolicy.isPresent()) {
            HybridStoreConfig hybridConfig = mergeNewSettingsIntoOldHybridStoreConfig(
                originalStore, hybridRewindSeconds, hybridOffsetLagThreshold, hybridTimeLagThreshold, hybridDataReplicationPolicy,
                hybridBufferReplayPolicy);
            newHybridStoreConfig = Optional.ofNullable(hybridConfig);
        } else {
            newHybridStoreConfig = Optional.empty();
        }

        checkWhetherStoreWillHaveConflictConfigForIncrementalAndHybrid(originalStore, incrementalPushEnabled, incrementalPushPolicy, newHybridStoreConfig);
        checkWhetherStoreWillHaveConflictConfigForCompressionAndHybrid(originalStore, compressionStrategy, newHybridStoreConfig);

        try {
            if (owner.isPresent()) {
                setStoreOwner(clusterName, storeName, owner.get());
            }

            if (readability.isPresent()) {
                setStoreReadability(clusterName, storeName, readability.get());
            }

            if (writeability.isPresent()) {
                setStoreWriteability(clusterName, storeName, writeability.get());
            }

            if (partitionCount.isPresent()) {
                setStorePartitionCount(clusterName, storeName, partitionCount.get());
            }

            String amplificationFactorNotSupportedErrorMessage = "amplificationFactor is not supported in Online/Offline state model";
            if (amplificationFactor.isPresent() && amplificationFactor.get() != 1) {
                if (leaderFollowerModelEnabled.isPresent()) {
                    if (!leaderFollowerModelEnabled.get()) {
                        throw new VeniceException(amplificationFactorNotSupportedErrorMessage);
                    }
                } else {
                    if (!originalStore.isLeaderFollowerModelEnabled()) {
                        throw new VeniceException(amplificationFactorNotSupportedErrorMessage);
                    }
                }
            }
            /**
             * If either of these three fields is not present, we should use default value to construct correct PartitionerConfig.
             */
            if (partitionerClass.isPresent() || partitionerParams.isPresent() || amplificationFactor.isPresent()) {
                PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
                partitionerClass.ifPresent(partitionerConfig::setPartitionerClass);
                partitionerParams.ifPresent(partitionerConfig::setPartitionerParams);
                amplificationFactor.ifPresent(partitionerConfig::setAmplificationFactor);

                setStorePartitionerConfig(clusterName, storeName, partitionerConfig);
            }

            if (storageQuotaInByte.isPresent()) {
                setStoreStorageQuota(clusterName, storeName, storageQuotaInByte.get());
            }

            if (readQuotaInCU.isPresent()) {
                setStoreReadQuota(clusterName, storeName, readQuotaInCU.get());
            }

            if (currentVersion.isPresent()) {
                setStoreCurrentVersion(clusterName, storeName, currentVersion.get());
            }

            if (largestUsedVersionNumber.isPresent()) {
                setStoreLargestUsedVersion(clusterName, storeName, largestUsedVersionNumber.get());
            }

            if (bootstrapToOnlineTimeoutInHours.isPresent()) {
                setBootstrapToOnlineTimeoutInHours(clusterName, storeName, bootstrapToOnlineTimeoutInHours.get());
            }

            VeniceControllerClusterConfig clusterConfig = getHelixVeniceClusterResources(clusterName).getConfig();
            if (newHybridStoreConfig.isPresent()) {
                // To fix the final variable problem in the lambda expression
                final HybridStoreConfig finalHybridConfig = newHybridStoreConfig.get();
                storeMetadataUpdate(clusterName, storeName, store -> {
                    if (!isHybrid(finalHybridConfig)) {
                        /**
                         * If all of the hybrid config values are negative, it indicates that the store is being set back to batch-only store.
                         */
                        store.setHybridStoreConfig(null);
                        String realTimeTopic = Version.composeRealTimeTopic(storeName);
                        truncateKafkaTopic(realTimeTopic);
                        // Also remove the Brooklin replication streams
                        if (onlineOfflineTopicReplicator.isPresent()) {
                            store.getVersions().stream().forEach(version -> {
                                if (!version.isLeaderFollowerModelEnabled()) {
                                    onlineOfflineTopicReplicator.get().terminateReplication(realTimeTopic, version.kafkaTopicName());
                                }
                            });
                        }
                        if (store.isLeaderFollowerModelEnabled() || clusterConfig.isLfModelDependencyCheckDisabled()) {
                            // Disabling hybrid configs for a L/F store
                            if (!store.isIncrementalPushEnabled()) {
                                // Enable/disable native replication for batch-only stores if the cluster level config for new batch stores is on
                                store.setNativeReplicationEnabled(clusterConfig.isNativeReplicationEnabledAsDefaultForBatchOnly());
                                store.setNativeReplicationSourceFabric(clusterConfig.getNativeReplicationSourceFabricAsDefaultForBatchOnly());
                                store.setActiveActiveReplicationEnabled(clusterConfig.isActiveActiveReplicationEnabledAsDefaultForBatchOnly());
                            } else {
                                store.setNativeReplicationEnabled(clusterConfig.isNativeReplicationEnabledAsDefaultForIncremental());
                                store.setNativeReplicationSourceFabric(clusterConfig.getNativeReplicationSourceFabricAsDefaultForIncremental());
                                store.setActiveActiveReplicationEnabled(clusterConfig.isActiveActiveReplicationEnabledAsDefaultForIncremental());
                            }
                        }
                    } else {
                        if (!store.isHybrid() && (clusterConfig.isLeaderFollowerEnabledForHybridStores() || clusterConfig.isLfModelDependencyCheckDisabled())) {
                            // This is a new hybrid store. Enable L/F if the config is set to true.
                            store.setLeaderFollowerModelEnabled(true);
                            if (!store.isIncrementalPushEnabled()) {
                                // Enable/disable native replication for hybrid stores if the cluster level config for new hybrid stores is on
                                store.setNativeReplicationEnabled(clusterConfig.isNativeReplicationEnabledAsDefaultForHybrid());
                                store.setNativeReplicationSourceFabric(clusterConfig.getNativeReplicationSourceFabricAsDefaultForHybrid());
                                // Enable/disable active-active replication for hybrid stores if the cluster level config for new hybrid stores is on
                                store.setActiveActiveReplicationEnabled(clusterConfig.isActiveActiveReplicationEnabledAsDefaultForHybrid());
                            } else {
                                // The native replication cluster level config for incremental push will cover all incremental push policy
                                store.setNativeReplicationEnabled(clusterConfig.isNativeReplicationEnabledAsDefaultForIncremental());
                                store.setNativeReplicationSourceFabric(clusterConfig.getNativeReplicationSourceFabricAsDefaultForIncremental());
                                // The active-active replication cluster level config for incremental push will cover all incremental push policy
                                store.setActiveActiveReplicationEnabled(clusterConfig.isActiveActiveReplicationEnabledAsDefaultForIncremental());
                            }
                        }
                        store.setHybridStoreConfig(finalHybridConfig);
                        if (getTopicManager().containsTopicAndAllPartitionsAreOnline(Version.composeRealTimeTopic(storeName))) {
                            // RT already exists, ensure the retention is correct
                            getTopicManager().updateTopicRetention(Version.composeRealTimeTopic(storeName),
                                TopicManager.getExpectedRetentionTimeInMs(store, finalHybridConfig));
                        }
                    }
                    return store;
                });
            }

            if (accessControlled.isPresent()) {
                setAccessControl(clusterName, storeName, accessControlled.get());
            }

            if (compressionStrategy.isPresent()) {
                setStoreCompressionStrategy(clusterName, storeName, compressionStrategy.get());
            }

            if (clientDecompressionEnabled.isPresent()) {
                setClientDecompressionEnabled(clusterName, storeName, clientDecompressionEnabled.get());
            }

            if (chunkingEnabled.isPresent()) {
                setChunkingEnabled(clusterName, storeName, chunkingEnabled.get());
            }

            if (batchGetLimit.isPresent()) {
                setBatchGetLimit(clusterName, storeName, batchGetLimit.get());
            }

            if (numVersionsToPreserve.isPresent()) {
                setNumVersionsToPreserve(clusterName, storeName, numVersionsToPreserve.get());
            }

            if (incrementalPushPolicy.isPresent()) {
                setIncrementalPushPolicy(clusterName, storeName, incrementalPushPolicy.get());

                if (incrementalPushPolicy.get() == IncrementalPushPolicy.INCREMENTAL_PUSH_SAME_AS_REAL_TIME) {
                    // Set ataReplicationPolicy.NONE on batch-only store (without hybrid store config)
                    storeMetadataUpdate(clusterName, storeName, store -> {
                        HybridStoreConfig hybridStoreConfig = store.getHybridStoreConfig();
                        if (hybridStoreConfig == null) {
                            store.setHybridStoreConfig(new HybridStoreConfigImpl(
                                    DEFAULT_REWIND_TIME_IN_SECONDS,
                                    DEFAULT_HYBRID_OFFSET_LAG_THRESHOLD,
                                    DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
                                    DataReplicationPolicy.NONE,
                                    null
                            ));
                        } else if (hybridStoreConfig.getDataReplicationPolicy() == null) {
                            store.setHybridStoreConfig(new HybridStoreConfigImpl(
                                    hybridStoreConfig.getRewindTimeInSeconds(),
                                    hybridStoreConfig.getOffsetLagThresholdToGoOnline(),
                                    hybridStoreConfig.getProducerTimestampLagThresholdToGoOnlineInSeconds(),
                                    DataReplicationPolicy.NONE,
                                    hybridStoreConfig.getBufferReplayPolicy()
                            ));
                        }
                        return store;
                    });
                }
            }

            if (incrementalPushEnabled.isPresent()) {
                setIncrementalPushEnabled(clusterName, storeName, incrementalPushEnabled.get());
            }

            if (replicationFactor.isPresent()) {
                setReplicationFactor(clusterName, storeName, replicationFactor.get());
            }

            if (storeMigration.isPresent()) {
                setStoreMigration(clusterName, storeName, storeMigration.get());
            }

            if (migrationDuplicateStore.isPresent()) {
                setMigrationDuplicateStore(clusterName, storeName, migrationDuplicateStore.get());
            }

            if (writeComputationEnabled.isPresent()) {
                setWriteComputationEnabled(clusterName, storeName, writeComputationEnabled.get());
            }

            if (readComputationEnabled.isPresent()) {
                setReadComputationEnabled(clusterName, storeName, readComputationEnabled.get());
            }

            if (leaderFollowerModelEnabled.isPresent() && !leaderFollowerModelEnabled.get()) {
                if (amplificationFactor.isPresent()) {
                    if (amplificationFactor.get() != 1) {
                        throw new VeniceException(amplificationFactorNotSupportedErrorMessage);
                    }
                } else {
                    if (originalStore.getPartitionerConfig() != null
                        && originalStore.getPartitionerConfig().getAmplificationFactor() != 1) {
                        throw new VeniceException(amplificationFactorNotSupportedErrorMessage);
                    }
                }
            }

            if (leaderFollowerModelEnabled.isPresent()) {
                setLeaderFollowerModelEnabled(clusterName, storeName, leaderFollowerModelEnabled.get());
            }

            if (nativeReplicationEnabled.isPresent()) {
                /**
                 * Leader/follower mode can be enabled/disabled within the same command.
                 */
                boolean isLeaderFollowerModelEnabled = getStore(clusterName, storeName).isLeaderFollowerModelEnabled();
                if (!isLeaderFollowerModelEnabled && !clusterConfig.isLfModelDependencyCheckDisabled() && nativeReplicationEnabled.get()) {
                    throw new VeniceException("Native Replication cannot be enabled on store " + storeName + " which does not have leader/follower mode enabled!");
                }
                setNativeReplicationEnabled(clusterName, storeName, nativeReplicationEnabled.get());
            }

            if (activeActiveReplicationEnabled.isPresent()) {
                boolean isLeaderFollowerModelEnabled = getStore(clusterName, storeName).isLeaderFollowerModelEnabled();
                if (!isLeaderFollowerModelEnabled && activeActiveReplicationEnabled.get()) {
                    throw new VeniceException("Active active replication cannot be enabled on store " + storeName + " which does not have L/F mode enabled.");
                }
                setActiveActiveReplicationEnabled(clusterName, storeName, activeActiveReplicationEnabled.get());
            }

            if (pushStreamSourceAddress.isPresent()) {
                setPushStreamSourceAddress(clusterName, storeName, pushStreamSourceAddress.get());
            }

            if (backupStrategy.isPresent()) {
                setBackupStrategy(clusterName, storeName, backupStrategy.get());
            }

            autoSchemaRegisterPushJobEnabled.ifPresent(value ->
                setAutoSchemaRegisterPushJobEnabled(clusterName, storeName, value));
            hybridStoreDiskQuotaEnabled.ifPresent(value ->
                setHybridStoreDiskQuotaEnabled(clusterName, storeName, value));
            if (regularVersionETLEnabled.isPresent() || futureVersionETLEnabled.isPresent() || etledUserProxyAccount.isPresent()) {
                ETLStoreConfig etlStoreConfig = new ETLStoreConfigImpl(
                    etledUserProxyAccount.orElse(originalStore.getEtlStoreConfig().getEtledUserProxyAccount()),
                    regularVersionETLEnabled.orElse(originalStore.getEtlStoreConfig().isRegularVersionETLEnabled()),
                    futureVersionETLEnabled.orElse(originalStore.getEtlStoreConfig().isFutureVersionETLEnabled()));
                storeMetadataUpdate(clusterName, storeName, store -> {
                    store.setEtlStoreConfig(etlStoreConfig);
                    return store;
                });
            }
            if (backupVersionRetentionMs.isPresent()) {
                setBackupVersionRetentionMs(clusterName, storeName, backupVersionRetentionMs.get());
            }

            if (nativeReplicationSourceFabric.isPresent()) {
                setNativeReplicationSourceFabric(clusterName, storeName, nativeReplicationSourceFabric.get());
            }

            if (applyTargetVersionFilterForIncPush.isPresent()) {
                setApplyTargetVersionFilterForIncPush(clusterName, storeName, applyTargetVersionFilterForIncPush.get());
            }
            logger.info("Finished updating store: " + storeName + " in cluster: " + clusterName);
        } catch (VeniceException e) {
            logger.error("Caught exception during update to store '" + storeName + "' in cluster: '" + clusterName
                + "'. Will attempt to rollback changes.", e);
            //rollback to original store
            storeMetadataUpdate(clusterName, storeName, store -> originalStore);
            if (originalStore.isHybrid() && newHybridStoreConfig.isPresent()
                && getTopicManager().containsTopicAndAllPartitionsAreOnline(Version.composeRealTimeTopic(storeName))) {
                // Ensure the topic retention is rolled back too
                getTopicManager().updateTopicRetention(Version.composeRealTimeTopic(storeName),
                    TopicManager.getExpectedRetentionTimeInMs(originalStore, originalStore.getHybridStoreConfig()));
            }
            logger.error("Successfully rolled back changes to store '" + storeName + "' in cluster: '" + clusterName
                + "'. Will now throw the original exception (" + e.getClass().getSimpleName() + ").");
            throw e;
        }
    }

    /**
     * This method is invoked in parent controllers for store migration.
     */
    public void replicateUpdateStore(String clusterName, String storeName, UpdateStoreQueryParams params) {
        try {
            StoreConfig storeConfig = storeConfigRepo.getStoreConfigOrThrow(storeName);
            String destinationCluster = storeConfig.getMigrationDestCluster();
            String sourceCluster = storeConfig.getMigrationSrcCluster();
            if (storeConfig.getCluster().equals(destinationCluster)) {
                // Migration has completed in this colo but the overall migration is still in progress.
                if (clusterName.equals(destinationCluster)) {
                    // Mirror new updates back to the source cluster in case we abort migration after completion.
                    ControllerClient sourceClusterControllerClient =
                        new ControllerClient(sourceCluster, getLeaderController(sourceCluster).getUrl(false), sslFactory);
                    ControllerResponse response = sourceClusterControllerClient.updateStore(storeName, params);
                    if (response.isError()) {
                        logger.warn("Replicate new update endpoint call to source cluster: " + sourceCluster
                            + " failed for store " + storeName + " Error: " + response.getError());
                    }
                }
            } else if (clusterName.equals(sourceCluster)) {
                ControllerClient destClusterControllerClient =
                    new ControllerClient(destinationCluster, getLeaderController(destinationCluster).getUrl(false), sslFactory);
                ControllerResponse response = destClusterControllerClient.updateStore(storeName, params);
                if (response.isError()) {
                    logger.warn("Replicate update store endpoint call to destination cluster: " + destinationCluster
                        + " failed for store " + storeName + " Error: " + response.getError());
                }
            }
        } catch (Exception e) {
            logger.warn("Exception thrown when replicating new update for store " + storeName
                + " as part of store migration", e);
        }
    }

    /**
     * Used by both the {@link VeniceHelixAdmin} and the {@link VeniceParentHelixAdmin}
     *
     * @param oldStore Existing Store that is the source for updates. This object will not be modified by this method.
     * @param hybridRewindSeconds Optional is present if the returned object should include a new rewind time
     * @param hybridOffsetLagThreshold Optional is present if the returned object should include a new offset lag threshold
     * @return null if oldStore has no hybrid configs and optionals are not present,
     *   otherwise a fully specified {@link HybridStoreConfig}
     */
    protected static HybridStoreConfig mergeNewSettingsIntoOldHybridStoreConfig(Store oldStore,
            Optional<Long> hybridRewindSeconds, Optional<Long> hybridOffsetLagThreshold,
            Optional<Long> hybridTimeLagThreshold, Optional<DataReplicationPolicy> hybridDataReplicationPolicy,
            Optional<BufferReplayPolicy> bufferReplayPolicy) {
        if (!hybridRewindSeconds.isPresent() && !hybridOffsetLagThreshold.isPresent() && !oldStore.isHybrid()){
            return null; //For the nullable union in the avro record
        }
        HybridStoreConfig mergedHybridStoreConfig;
        if (oldStore.isHybrid()){ // for an existing hybrid store, just replace any specified values
            HybridStoreConfig oldHybridConfig = oldStore.getHybridStoreConfig().clone();
            mergedHybridStoreConfig = new HybridStoreConfigImpl(
                hybridRewindSeconds.isPresent()
                    ? hybridRewindSeconds.get()
                    : oldHybridConfig.getRewindTimeInSeconds(),
                hybridOffsetLagThreshold.isPresent()
                    ? hybridOffsetLagThreshold.get()
                    : oldHybridConfig.getOffsetLagThresholdToGoOnline(),
                hybridTimeLagThreshold.isPresent()
                    ? hybridTimeLagThreshold.get()
                    : oldHybridConfig.getProducerTimestampLagThresholdToGoOnlineInSeconds(),
                hybridDataReplicationPolicy.isPresent()
                    ? hybridDataReplicationPolicy.get()
                    : oldHybridConfig.getDataReplicationPolicy(),
                bufferReplayPolicy.isPresent()
                    ? bufferReplayPolicy.get()
                    : oldHybridConfig.getBufferReplayPolicy()
            );
        } else {
            // switching a non-hybrid store to hybrid; must specify:
            // 1. rewind time
            // 2. either offset lag threshold or time lag threshold, or both
            if (!(hybridRewindSeconds.isPresent() && (hybridOffsetLagThreshold.isPresent() || hybridTimeLagThreshold.isPresent()))) {
                throw new VeniceException(oldStore.getName() + " was not a hybrid store.  In order to make it a hybrid store both "
                    + " rewind time in seconds and offset or time lag threshold must be specified");
            }
            mergedHybridStoreConfig = new HybridStoreConfigImpl(
                hybridRewindSeconds.get(),
                // If not specified, offset/time lag threshold will be -1 and will not be used to determine whether
                // a partition is ready to serve
                hybridOffsetLagThreshold.orElse(DEFAULT_HYBRID_OFFSET_LAG_THRESHOLD),
                hybridTimeLagThreshold.orElse(DEFAULT_HYBRID_TIME_LAG_THRESHOLD),
                hybridDataReplicationPolicy.orElse(DataReplicationPolicy.NON_AGGREGATE),
                bufferReplayPolicy.orElse(BufferReplayPolicy.REWIND_FROM_EOP)
            );
        }
        if (mergedHybridStoreConfig.getRewindTimeInSeconds() > 0 && mergedHybridStoreConfig.getOffsetLagThresholdToGoOnline() < 0
            && mergedHybridStoreConfig.getProducerTimestampLagThresholdToGoOnlineInSeconds() < 0) {
            throw new VeniceException("Both offset lag threshold and time lag threshold are negative when setting hybrid"
                + " configs for store " + oldStore.getName());
        }
        return mergedHybridStoreConfig;
    }

    /**
     * A helper function to split a region list with {@link VeniceHelixAdmin#REGION_FILTER_LIST_SEPARATOR}
     */
    protected static Set<String> parseRegionsFilterList(String regionsFilterList) {
        Set<String> fabrics = new HashSet<>();
        String[] tokens = regionsFilterList.trim().toLowerCase().split(REGION_FILTER_LIST_SEPARATOR);
        Collections.addAll(fabrics, tokens);
        return fabrics;
    }

    public void storeMetadataUpdate(String clusterName, String storeName, StoreMetadataOperation operation) {
        checkPreConditionForUpdateStore(clusterName, storeName);
        HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
        try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(storeName)) {
            ReadWriteStoreRepository repository = resources.getStoreMetadataRepository();
            Store store = repository.getStore(storeName);
            Store updatedStore = operation.update(store);
            repository.updateStore(updatedStore);
        } catch (Exception e) {
            logger.error("Failed to execute StoreMetadataOperation.", e);
            throw e;
        }
    }

    protected void checkPreConditionForUpdateStore(String clusterName, String storeName){
        checkControllerMastership(clusterName);
        ReadWriteStoreRepository repository = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
        if (repository.getStore(storeName) == null) {
            throwStoreDoesNotExist(clusterName, storeName);
        }
    }

    @Override
    public double getStorageEngineOverheadRatio(String clusterName) {
        return multiClusterConfigs.getControllerConfig(clusterName).getStorageEngineOverheadRatio();
    }

  private void waitUntilNodesAreAssignedForResource(
      String clusterName,
      String topic,
      OfflinePushStrategy strategy,
      long maxWaitTimeMs,
      int replicationFactor) {

    HelixVeniceClusterResources clusterResources = getHelixVeniceClusterResources(clusterName);
    PushMonitor pushMonitor = clusterResources.getPushMonitor();
    RoutingDataRepository routingDataRepository = clusterResources.getRoutingDataRepository();
    PushStatusDecider statusDecider = PushStatusDecider.getDecider(strategy);

    Optional<String> notReadyReason = Optional.of("unknown");
    long startTime = System.currentTimeMillis();
    long logTime = 0;
    for (long elapsedTime = 0; elapsedTime <= maxWaitTimeMs; elapsedTime = System.currentTimeMillis() - startTime) {
      if (pushMonitor.getOfflinePushOrThrow(topic).getCurrentStatus().equals(ExecutionStatus.ERROR)) {
        throw new VeniceException("Push " + topic + " has already failed.");
      }

      ResourceAssignment resourceAssignment = routingDataRepository.getResourceAssignment();
      notReadyReason = statusDecider.hasEnoughNodesToStartPush(topic, replicationFactor, resourceAssignment, notReadyReason);

      if (!notReadyReason.isPresent()) {
        logger.info("After waiting for " + elapsedTime + "ms, resource allocation is completed for " + topic + ".");
        pushMonitor.refreshAndUpdatePushStatus(topic, ExecutionStatus.STARTED, Optional.of("Helix assignment complete"));
        pushMonitor.recordPushPreparationDuration(topic, TimeUnit.MILLISECONDS.toSeconds(elapsedTime));
        return;
      }
      if ((elapsedTime - logTime) > HELIX_RESOURCE_ASSIGNMENT_LOG_INTERVAL_MS) {
          logger.info("After waiting for " + elapsedTime + "ms, resource assignment for: " + topic
              + " is still not complete, strategy=" + strategy.toString() + ", replicationFactor="
              + replicationFactor + ", reason=" + notReadyReason.get());
          logTime = elapsedTime;
      }
      Utils.sleep(HELIX_RESOURCE_ASSIGNMENT_RETRY_INTERVAL_MS);
    }

    // Time out, after waiting maxWaitTimeMs, there are not enough nodes assigned.
    pushMonitor.recordPushPreparationDuration(topic, TimeUnit.MILLISECONDS.toSeconds(maxWaitTimeMs));
    throw new VeniceException("After waiting for " + maxWaitTimeMs + "ms, resource assignment for: " + topic
        + " timed out, strategy=" + strategy.toString() + ", replicationFactor=" + replicationFactor
        + ", reason=" + notReadyReason.get());
  }

    protected void deleteHelixResource(String clusterName, String kafkaTopic) {
        checkControllerMastership(clusterName);
        helixAdminClient.dropResource(clusterName, kafkaTopic);
        logger.info("Successfully dropped the resource " + kafkaTopic + " for cluster " + clusterName);
    }

    @Override
    public SchemaEntry getKeySchema(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        ReadWriteSchemaRepository schemaRepo = getHelixVeniceClusterResources(clusterName).getSchemaRepository();
        return schemaRepo.getKeySchema(storeName);
    }

    @Override
    public Collection<SchemaEntry> getValueSchemas(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        ReadWriteSchemaRepository schemaRepo = getHelixVeniceClusterResources(clusterName).getSchemaRepository();
        return schemaRepo.getValueSchemas(storeName);
    }

    @Override
    public Collection<DerivedSchemaEntry> getDerivedSchemas(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        ReadWriteSchemaRepository schemaRepo = getHelixVeniceClusterResources(clusterName).getSchemaRepository();
        return schemaRepo.getDerivedSchemas(storeName);
    }

    @Override
    public int getValueSchemaId(String clusterName, String storeName, String valueSchemaStr) {
        checkControllerMastership(clusterName);
        ReadWriteSchemaRepository schemaRepo = getHelixVeniceClusterResources(clusterName).getSchemaRepository();
        int schemaId = schemaRepo.getValueSchemaId(storeName, valueSchemaStr);
        // validate the schema as VPJ uses this method to fetch the value schema. Fail loudly if the schema user trying
        // to push is bad.
        if (schemaId != SchemaData.INVALID_VALUE_SCHEMA_ID) {
            AvroSchemaUtils.validateAvroSchemaStr(valueSchemaStr);
        }
        return schemaId;
    }

    @Override
    public Pair<Integer, Integer> getDerivedSchemaId(String clusterName, String storeName, String schemaStr) {
        checkControllerMastership(clusterName);
        ReadWriteSchemaRepository schemaRepo = getHelixVeniceClusterResources(clusterName).getSchemaRepository();
        Pair<Integer, Integer> schamaID = schemaRepo.getDerivedSchemaId(storeName, schemaStr);
        // validate the schema as VPJ uses this method to fetch the value schema. Fail loudly if the schema user trying
        // to push is bad.
        if (schamaID.getFirst() != SchemaData.INVALID_VALUE_SCHEMA_ID) {
            AvroSchemaUtils.validateAvroSchemaStr(schemaStr);
        }
        return schamaID;
    }

    @Override
    public SchemaEntry getValueSchema(String clusterName, String storeName, int id) {
        checkControllerMastership(clusterName);
        ReadWriteSchemaRepository schemaRepo = getHelixVeniceClusterResources(clusterName).getSchemaRepository();
        return schemaRepo.getValueSchema(storeName, id);
    }

    private void validateSchema(String schemaStr, String clusterName, String storeName) {
        VeniceControllerClusterConfig config = getHelixVeniceClusterResources(clusterName).getConfig();
        if (!config.isControllerSchemaValidationEnabled()) {
            return;
        }

        ReadWriteSchemaRepository schemaRepository = getHelixVeniceClusterResources(clusterName).getSchemaRepository();
        Collection<SchemaEntry> schemaEntries = schemaRepository.getValueSchemas(storeName);
        AvroSerializer serializer;
        Schema existingSchema = null;

        Schema newSchema = Schema.parse(schemaStr);
        RandomAvroObjectGenerator generator = new RandomAvroObjectGenerator(newSchema, new Random());
        for (int i = 0; i < RECORD_COUNT; i++) {
            // check if new records written with new schema can be read using existing older schema
            Object record = generator.generate();
            serializer = new AvroSerializer(newSchema);
            byte[] bytes = serializer.serialize(record);
            for (SchemaEntry schemaEntry : schemaEntries) {
                try {
                    existingSchema = schemaEntry.getSchema();
                    if (!isValidAvroSchema(existingSchema)) {
                        logger.warn("Skip validating ill-formed schema " + existingSchema + " for store " + storeName);
                        continue;
                    }
                    RecordDeserializer<Object> deserializer = SerializerDeserializerFactory.getAvroGenericDeserializer(newSchema, existingSchema);
                    deserializer.deserialize(bytes);
                } catch (Exception e) {
                    if (e instanceof AvroIncompatibleSchemaException) {
                        logger.warn("Found incompatible avro schema with bad union branch for store " + storeName, e);
                        continue;
                    }
                    throw new InvalidVeniceSchemaException("Error while trying to add new schema: " + schemaStr + "  for store " + storeName + " as it is incompatible with existing schema: " + existingSchema, e);
                }
            }
        }

        // check if records written with older schema can be read using the new schema
        for (int i = 0; i < RECORD_COUNT; i++) {
            for (SchemaEntry schemaEntry : schemaEntries) {
                try {
                    generator = new RandomAvroObjectGenerator(schemaEntry.getSchema(), new Random());
                    Object record = generator.generate();
                    serializer = new AvroSerializer(schemaEntry.getSchema());
                    byte[] bytes = serializer.serialize(record);
                    existingSchema = schemaEntry.getSchema();
                    if (!isValidAvroSchema(existingSchema)) {
                        logger.warn("Skip validating ill-formed schema " + existingSchema + " for store " + storeName);
                        continue;
                    }
                    RecordDeserializer<Object> deserializer = SerializerDeserializerFactory.getAvroGenericDeserializer(existingSchema, newSchema);
                    deserializer.deserialize(bytes);
                } catch (Exception e) {
                    if (e instanceof AvroIncompatibleSchemaException) {
                        logger.warn("Found incompatible avro schema with bad union branch for store " + storeName, e);
                        continue;
                    }
                    throw new InvalidVeniceSchemaException("Error while trying to add new schema: " + schemaStr + "  for store " + storeName + " as it is incompatible with existing schema: " + existingSchema, e);
                }
            }
        }
    }

    @Override
    public SchemaEntry addValueSchema(String clusterName, String storeName, String valueSchemaStr,
        DirectionalSchemaCompatibilityType expectedCompatibilityType) {
        checkControllerMastership(clusterName);
        ReadWriteSchemaRepository schemaRepository = getHelixVeniceClusterResources(clusterName).getSchemaRepository();

        SchemaEntry schemaEntry = schemaRepository.addValueSchema(storeName, valueSchemaStr, expectedCompatibilityType);
        // Write store schemas to metadata store.
        Store store = getStore(clusterName, storeName);

        return new SchemaEntry(schemaRepository.getValueSchemaId(storeName, valueSchemaStr), valueSchemaStr);
    }

    @Override
    public SchemaEntry addValueSchema(String clusterName, String storeName, String valueSchemaStr, int schemaId) {
        return addValueSchema(clusterName, storeName, valueSchemaStr, schemaId,
            SchemaEntry.DEFAULT_SCHEMA_CREATION_COMPATIBILITY_TYPE);
    }

    public SchemaEntry addValueSchema(String clusterName, String storeName, String valueSchemaStr, int schemaId,
        DirectionalSchemaCompatibilityType compatibilityType) {
        checkControllerMastership(clusterName);
        ReadWriteSchemaRepository schemaRepository = getHelixVeniceClusterResources(clusterName).getSchemaRepository();
        int newValueSchemaId = schemaRepository.preCheckValueSchemaAndGetNextAvailableId(storeName, valueSchemaStr,
            compatibilityType);
        if (newValueSchemaId != SchemaData.DUPLICATE_VALUE_SCHEMA_CODE && newValueSchemaId != schemaId) {
            throw new VeniceException("Inconsistent value schema id between the caller and the local schema repository."
                + " Expected new schema id of " + schemaId + " but the next available id from the local repository is "
                + newValueSchemaId + " for store " + storeName + " in cluster " + clusterName + " Schema: " + valueSchemaStr);
        }

        SchemaEntry schemaEntry = schemaRepository.addValueSchema(storeName, valueSchemaStr, newValueSchemaId);
        // Write store schemas to metadata store.
        Store store = getStore(clusterName, storeName);
        return schemaEntry;
    }

    @Override
    public DerivedSchemaEntry addDerivedSchema(String clusterName, String storeName, int valueSchemaId, String derivedSchemaStr) {
        checkControllerMastership(clusterName);
        ReadWriteSchemaRepository schemaRepository = getHelixVeniceClusterResources(clusterName).getSchemaRepository();
        schemaRepository.addDerivedSchema(storeName, derivedSchemaStr, valueSchemaId);

        return new DerivedSchemaEntry(valueSchemaId,
            schemaRepository.getDerivedSchemaId(storeName, derivedSchemaStr).getSecond(), derivedSchemaStr );
    }

    @Override
    public DerivedSchemaEntry addDerivedSchema(String clusterName, String storeName, int valueSchemaId,
        int derivedSchemaId, String derivedSchemaStr) {
        checkControllerMastership(clusterName);
        return getHelixVeniceClusterResources(clusterName).getSchemaRepository()
            .addDerivedSchema(storeName, derivedSchemaStr, valueSchemaId, derivedSchemaId);
    }

    @Override
    public DerivedSchemaEntry removeDerivedSchema(String clusterName, String storeName, int valueSchemaId, int derivedSchemaId) {
        checkControllerMastership(clusterName);
        return getHelixVeniceClusterResources(clusterName).getSchemaRepository()
            .removeDerivedSchema(storeName, valueSchemaId, derivedSchemaId);
    }

    @Override
    public SchemaEntry addSupersetSchema(String clusterName, String storeName, String valueSchema,
        int valueSchemaId, String supersetSchema, int supersetSchemaId) {
        checkControllerMastership(clusterName);
        ReadWriteSchemaRepository schemaRepository = getHelixVeniceClusterResources(clusterName).getSchemaRepository();
        ReadWriteStoreRepository storeRepository = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();

        // If the new superset schema does not exist in the schema repo, add it
        SchemaEntry existingSchema = schemaRepository.getValueSchema(storeName, supersetSchemaId);
        if (existingSchema != null) {
            if (!AvroSchemaUtils.compareSchemaIgnoreFieldOrder(existingSchema.getSchema(), Schema.parse(supersetSchema))) {
                throw new VeniceException("Existing schema with id " + existingSchema.getId() + " does not match with new schema " + supersetSchema);
            }
        } else {
            logger.info("Adding superset schema: " + supersetSchema + " for store: " + storeName);
            schemaRepository.addValueSchema(storeName, supersetSchema, supersetSchemaId);
        }

        // Update the store config
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setLatestSuperSetValueSchemaId(supersetSchemaId);
            return store;
        });
        // add the value schema
        return schemaRepository.addValueSchema(storeName, valueSchema, valueSchemaId);
    }

    public int getValueSchemaIdIgnoreFieldOrder(String clusterName, String storeName, String valueSchemaStr) {
        checkControllerMastership(clusterName);
        SchemaEntry valueSchemaEntry = new SchemaEntry(SchemaData.UNKNOWN_SCHEMA_ID, valueSchemaStr);
        ReadWriteSchemaRepository schemaRepository = getHelixVeniceClusterResources(clusterName).getSchemaRepository();

        return schemaRepository.getValueSchemaIdIgnoreFieldOrder(storeName, valueSchemaEntry);
    }

    protected int checkPreConditionForAddValueSchemaAndGetNewSchemaId(String clusterName, String storeName,
        String valueSchemaStr, DirectionalSchemaCompatibilityType expectedCompatibilityType) {
        AvroSchemaUtils.validateAvroSchemaStr(valueSchemaStr);
        validateSchema(valueSchemaStr, clusterName, storeName);
        checkControllerMastership(clusterName);
        return getHelixVeniceClusterResources(clusterName).getSchemaRepository()
            .preCheckValueSchemaAndGetNextAvailableId(storeName, valueSchemaStr, expectedCompatibilityType);
    }

    protected int checkPreConditionForAddDerivedSchemaAndGetNewSchemaId(String clusterName, String storeName,
        int valueSchemaId, String derivedSchemaStr) {
        checkControllerMastership(clusterName);
        return getHelixVeniceClusterResources(clusterName).getSchemaRepository()
            .preCheckDerivedSchemaAndGetNextAvailableId(storeName, valueSchemaId, derivedSchemaStr);
    }


    @Override
    public Collection<ReplicationMetadataSchemaEntry> getReplicationMetadataSchemas(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        ReadWriteSchemaRepository schemaRepo = getHelixVeniceClusterResources(clusterName).getSchemaRepository();
        return schemaRepo.getReplicationMetadataSchemas(storeName);
    }

    public ReplicationMetadataVersionId getReplicationMetadataVersionId(String clusterName, String storeName, String replicationMetadataSchemaStr) {
        checkControllerMastership(clusterName);
        ReadWriteSchemaRepository schemaRepo = getHelixVeniceClusterResources(clusterName).getSchemaRepository();
        return schemaRepo.getReplicationMetadataVersionId(storeName, replicationMetadataSchemaStr);
    }

    protected boolean checkIfMetadataSchemaAlreadyPresent(String clusterName, String storeName, int valueSchemaId,
        ReplicationMetadataSchemaEntry replicationMetadataSchemaEntry) {
        checkControllerMastership(clusterName);
        try {
            Collection<ReplicationMetadataSchemaEntry> schemaEntries =
                getHelixVeniceClusterResources(clusterName).getSchemaRepository().getReplicationMetadataSchemas(storeName);
            for (ReplicationMetadataSchemaEntry schemaEntry : schemaEntries) {
                if (schemaEntry.equals(replicationMetadataSchemaEntry)) {
                    return true;
                }
            }
        } catch (Exception e) {
            logger.warn("Exception in checkIfMetadataSchemaAlreadyPresent ", e);
        }
        return false;
    }


    @Override
    public ReplicationMetadataSchemaEntry addReplicationMetadataSchema(String clusterName, String storeName, int valueSchemaId,
        int replicationMetadataVersionId, String replicationMetadataSchemaStr) {
        checkControllerMastership(clusterName);

        ReplicationMetadataSchemaEntry replicationMetadataSchemaEntry =
            new ReplicationMetadataSchemaEntry(valueSchemaId, replicationMetadataVersionId,
                replicationMetadataSchemaStr);
        if (checkIfMetadataSchemaAlreadyPresent(clusterName, storeName, valueSchemaId, replicationMetadataSchemaEntry)) {
            logger.info("Timestamp metadata schema Already present: for store:" + storeName + " in cluster:" + clusterName + " metadataSchema:" + replicationMetadataSchemaStr
                + " timestampMetadataVersionId:" + replicationMetadataVersionId + " valueSchemaId:" + valueSchemaId);
            return replicationMetadataSchemaEntry;
        }

        return getHelixVeniceClusterResources(clusterName).getSchemaRepository()
            .addMetadataSchema(storeName, valueSchemaId, replicationMetadataSchemaStr, replicationMetadataVersionId);
    }

    @Override
    public List<String> getStorageNodes(String clusterName){
        checkControllerMastership(clusterName);
        return helixAdminClient.getInstancesInCluster(clusterName);
    }

    @Override
    public Map<String, String> getStorageNodesStatus(String clusterName) {
        checkControllerMastership(clusterName);
        List<String> instances = helixAdminClient.getInstancesInCluster(clusterName);
        RoutingDataRepository routingDataRepository = getHelixVeniceClusterResources(clusterName).getRoutingDataRepository();
        Set<String> liveInstances = routingDataRepository.getLiveInstancesMap().keySet();
        Map<String, String> instancesStatusesMap = new HashMap<>();
        for (String instance : instances) {
            if (liveInstances.contains(instance)) {
                instancesStatusesMap.put(instance, InstanceStatus.CONNECTED.toString());
            } else {
                instancesStatusesMap.put(instance, InstanceStatus.DISCONNECTED.toString());
            }
        }
        return instancesStatusesMap;
    }

    public Schema getLatestValueSchema(String clusterName, Store store) {
        ReadWriteSchemaRepository schemaRepository = getHelixVeniceClusterResources(clusterName).getSchemaRepository();
        // If already a superset schema exists, try to generate the new superset from that and the input value schema
        SchemaEntry existingSchema = schemaRepository.getLatestValueSchema(store.getName());

        return existingSchema == null ? null : existingSchema.getSchema();
    }

    @Override
    public void removeStorageNode(String clusterName, String instanceId) {
        checkControllerMastership(clusterName);
        logger.info("Removing storage node: " + instanceId + " from cluster: " + clusterName);
        RoutingDataRepository routingDataRepository = getHelixVeniceClusterResources(clusterName).getRoutingDataRepository();
        if (routingDataRepository.getLiveInstancesMap().containsKey(instanceId)) {
            // The given storage node is still connected to cluster.
            throw new VeniceException("Storage node: " + instanceId + " is still connected to cluster: " + clusterName
                + ", could not be removed from that cluster.");
        }

        // Remove storage node from both whitelist and helix instances list.
        removeInstanceFromWhiteList(clusterName, instanceId);
        helixAdminClient.dropStorageInstance(clusterName, instanceId);
        logger.info("Removed storage node: " + instanceId + " from cluster: " + clusterName);
    }

    @Override
    public synchronized void stop(String clusterName) {
        // Instead of disconnecting the sub-controller for the given cluster, we should disable it for this controller,
        // then the LEADER->STANDBY and STANDBY->OFFLINE will be triggered, our handler will handle the resource collection.
        List<String> partitionNames = new ArrayList<>();
        partitionNames.add(VeniceControllerStateModel.getPartitionNameFromVeniceClusterName(clusterName));
        helixAdminClient.enablePartition(false, controllerClusterName, controllerName, clusterName, partitionNames);
    }

    @Override
    public void stopVeniceController() {
        try {
            manager.disconnect();
            topicManagerRepository.close();
            zkClient.close();
            admin.close();
            helixAdminClient.close();
        } catch (Exception e) {
            throw new VeniceException("Can not stop controller correctly.", e);
        }
    }

    @Override
    public OfflinePushStatusInfo getOffLinePushStatus(String clusterName, String kafkaTopic) {
        return getOffLinePushStatus(clusterName, kafkaTopic, Optional.empty());
    }

    @Override
    public OfflinePushStatusInfo getOffLinePushStatus(String clusterName, String kafkaTopic, Optional<String> incrementalPushVersion) {
        checkControllerMastership(clusterName);
        PushMonitor monitor = getHelixVeniceClusterResources(clusterName).getPushMonitor();
        Pair<ExecutionStatus, Optional<String>> statusAndDetails =
            monitor.getPushStatusAndDetails(kafkaTopic, incrementalPushVersion);
        ExecutionStatus executionStatus = statusAndDetails.getFirst();
        Optional<String> details = statusAndDetails.getSecond();
        if (executionStatus.equals(ExecutionStatus.NOT_CREATED)) {
            StringBuilder moreDetailsBuilder = new StringBuilder(details.isPresent() ? details.get() + " and " : "");
            // Check whether cluster is in maintenance mode or not
            if (isClusterInMaintenanceMode(clusterName)) {
                moreDetailsBuilder.append("Cluster: ")
                    .append(clusterName)
                    .append(" is in maintenance mode");
            } else {
                moreDetailsBuilder.append("Version creation for topic: ")
                    .append(kafkaTopic)
                    .append(" got delayed");
            }
            details = Optional.of(moreDetailsBuilder.toString());
        }

        // Retrive Da Vinci push status
        String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
        Store store = getStore(clusterName, storeName);
        int versionNumber = Version.parseVersionFromVersionTopicName(kafkaTopic);
        // Da Vinci can only subscribe to an existing version, so skip 1st push
        if (store.isDaVinciPushStatusStoreEnabled() && (versionNumber > 1 || incrementalPushVersion.isPresent())) {
            Version version = store.getVersion(versionNumber)
                .orElseThrow(() -> new VeniceException("Version " + versionNumber + " of " + storeName + " does not exist."));
            Pair<ExecutionStatus, Optional<String>> daVinciStatusAndDetails = getDaVinciPushStatusAndDetails(version, incrementalPushVersion);
            ExecutionStatus daVinciStatus = daVinciStatusAndDetails.getFirst();
            Optional<String> daVinciDetails = daVinciStatusAndDetails.getSecond();
            executionStatus = getOverallPushStatus(executionStatus, daVinciStatus);
            if (details.isPresent() || daVinciDetails.isPresent()) {
                String overallDetails = "";
                if (details.isPresent()) {
                    overallDetails += details.get();
                }
                if (daVinciDetails.isPresent()) {
                    overallDetails += (overallDetails.isEmpty() ? "" : " ") + daVinciDetails.get();
                }
                details = Optional.of(overallDetails);
            }
        }
        return new OfflinePushStatusInfo(executionStatus, details);
    }

    private ExecutionStatus getOverallPushStatus(ExecutionStatus veniceStatus, ExecutionStatus daVinciStatus) {
        List<ExecutionStatus> statuses = Arrays.asList(veniceStatus, daVinciStatus);
        Collections.sort(statuses, Comparator.comparingInt(STATUS_PRIORITIES::indexOf));
        long failCount = statuses.stream().filter(s -> s == ExecutionStatus.ERROR).count();
        ExecutionStatus currentStatus = statuses.get(0);
        if (failCount == 1) {
            currentStatus = ExecutionStatus.PROGRESS;
        }
        if (currentStatus.isTerminal() && failCount != 0) {
            currentStatus = ExecutionStatus.ERROR;
        }
        return currentStatus;
    }

    /**
     * getDaVinciPushStatusAndDetails checks all partitions and compute a final status.
     * Inside each partition, getDaVinciPushStatusAndDetails will compute status based on all active replicas/Da Vinci instances.
     * A replica/Da Vinci instance sent heartbeat to controllers recently is considered active.
     */
    private Pair<ExecutionStatus, Optional<String>> getDaVinciPushStatusAndDetails(Version version, Optional<String> incrementalPushVersion) {
        if (!pushStatusStoreReader.isPresent()) {
            throw new VeniceException("D2Client must be provided to read from push status store.");
        }
        int partitionCount = version.getPartitionCount();
        logger.info("Getting Da Vinci push status for store " + version.getStoreName());
        boolean allMiddleStatusReceived = true;
        ExecutionStatus completeStatus = incrementalPushVersion.isPresent() ?
            ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED : ExecutionStatus.COMPLETED;
        ExecutionStatus middleStatus = incrementalPushVersion.isPresent() ?
            ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED : ExecutionStatus.END_OF_PUSH_RECEIVED;
        Optional<String> erroredInstance = Optional.empty();
        String storeName = version.getStoreName();
        int completedPartitions = 0;
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            Map<CharSequence, Integer> instances = pushStatusStoreReader.get().getPartitionStatus(storeName, version.getNumber(), partitionId, incrementalPushVersion);
            boolean allInstancesCompleted = true;
            for (Map.Entry<CharSequence, Integer> entry : instances.entrySet()) {
                ExecutionStatus status = ExecutionStatus.fromInt(entry.getValue());
                if (status == middleStatus) {
                    if (allInstancesCompleted && pushStatusStoreReader.get().isInstanceAlive(storeName, entry.getKey().toString())) {
                        allInstancesCompleted = false;
                    }
                } else if (status != completeStatus) {
                    if (allInstancesCompleted || allMiddleStatusReceived) {
                        if (pushStatusStoreReader.get().isInstanceAlive(storeName, entry.getKey().toString())) {
                            allInstancesCompleted = false;
                            allMiddleStatusReceived = false;
                            if (status == ExecutionStatus.ERROR) {
                                erroredInstance = Optional.of(entry.getKey().toString());
                                break;
                            }
                        }
                    }
                }
            }
            if (allInstancesCompleted) {
                completedPartitions++;
            }
        }
        Optional<String> statusDetail;
        String details = "";
        if (completedPartitions > 0) {
            details += completedPartitions + "/" + partitionCount + " partitions completed in Da Vinci.";
        }
        if (erroredInstance.isPresent()) {
            details += "Found a failed instance in Da Vinci, it is " + erroredInstance;
        }
        if (details.length() != 0) {
            statusDetail = Optional.of(details);
        } else {
            statusDetail = Optional.empty();
        }
        if (completedPartitions == partitionCount) {
            return new Pair<>(completeStatus, statusDetail);
        } else if (allMiddleStatusReceived) {
            return new Pair<>(middleStatus, statusDetail);
        } else if (erroredInstance.isPresent()) {
            return new Pair<>(ExecutionStatus.ERROR, statusDetail);
        } else {
            return new Pair<>(ExecutionStatus.STARTED, statusDetail);
        }
    }

    @Override
    public Map<String, Long> getOfflinePushProgress(String clusterName, String kafkaTopic){
        checkControllerMastership(clusterName);
        PushMonitor monitor = getHelixVeniceClusterResources(clusterName).getPushMonitor();
        return monitor.getOfflinePushProgress(kafkaTopic);
    }

    // TODO remove this method once we are fully on HaaS
    // Create the controller cluster for venice cluster assignment if required.
    private void createControllerClusterIfRequired() {
        if(admin.getClusters().contains(controllerClusterName)) {
            logger.info("Cluster  " + controllerClusterName + " already exists. ");
            return;
        }

        boolean isClusterCreated = admin.addCluster(controllerClusterName, false);
        if(isClusterCreated == false) {
            /**
             * N.B.: {@link HelixAdmin#addCluster(String, boolean)} has a somewhat quirky implementation:
             *
             * When it returns true, it does not necessarily mean the cluster is fully created, because it
             * short-circuits the rest of its work if it sees the top-level znode is present.
             *
             * When it returns false, it means the cluster is either not created at all or is only partially
             * created.
             *
             * Therefore, when calling this function twice in a row, it is possible that the first invocation
             * may do a portion of the setup work, and then fail on a subsequent step (thus returning false);
             * and that the second invocation would short-circuit when seeing that the initial portion of the
             * work is done (thus returning true). In this case, however, the cluster is actually not created.
             *
             * Because the function swallows any errors (though still logs them, thankfully) and only returns
             * a boolean, it is impossible to catch the specific exception that prevented the first invocation
             * from working, hence why our own logs instruct the operator to look for previous Helix logs for
             * the details...
             *
             * In the main code, I (FGV) believe we don't retry the #addCluster() call, so we should not fall
             * in the scenario where this returns true and we mistakenly think the cluster exists. This does
             * happen in the integration test suite, however, which retries service creation several times
             * if it gets any exception.
             *
             * In any case, if we call this function twice and progress past this code block, another Helix
             * function below, {@link HelixAdmin#setConfig(HelixConfigScope, Map)}, fails with the following
             * symptoms:
             *
             * org.apache.helix.HelixException: fail to set config. cluster: venice-controllers is NOT setup.
             *
             * Thus, if you see this, it is actually because {@link HelixAdmin#addCluster(String, boolean)}
             * returned true even though the Helix cluster is only partially setup.
             */
            throw new VeniceException("admin.addCluster() for '" + controllerClusterName + "' returned false. "
                + "Look for previous errors logged by Helix for more details...");
        }
        HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).
            forCluster(controllerClusterName).build();
        Map<String, String> helixClusterProperties = new HashMap<String, String>();
        helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
        // Topology and fault zone type fields are used by CRUSH alg. Helix would apply the constrains on CRUSH alg to choose proper instance to hold the replica.
        helixClusterProperties.put(ClusterConfig.ClusterConfigProperty.TOPOLOGY_AWARE_ENABLED.name(), String.valueOf(false));
        /**
         * This {@link HelixAdmin#setConfig(HelixConfigScope, Map)} function will throw a HelixException if
         * the previous {@link HelixAdmin#addCluster(String, boolean)} call failed silently (details above).
         */
        admin.setConfig(configScope, helixClusterProperties);
        admin.addStateModelDef(controllerClusterName, LeaderStandbySMD.name, LeaderStandbySMD.build());
    }

    private void setupStorageClusterAsNeeded(String clusterName) {
        if (helixAdminClient.isVeniceStorageClusterCreated(clusterName)) {
            if (!helixAdminClient.isClusterInGrandCluster(clusterName)) {
                helixAdminClient.addClusterToGrandCluster(clusterName);
            }
            return;
        }
        Map<String, String> helixClusterProperties = new HashMap<>();
        helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
        long delayRebalanceTimeMs = multiClusterConfigs.getControllerConfig(clusterName).getDelayToRebalanceMS();
        if (delayRebalanceTimeMs > 0) {
            helixClusterProperties.put(ClusterConfig.ClusterConfigProperty.DELAY_REBALANCE_ENABLED.name(), String.valueOf(true));
            helixClusterProperties.put(ClusterConfig.ClusterConfigProperty.DELAY_REBALANCE_TIME.name(),
                String.valueOf(delayRebalanceTimeMs));
        }
        helixClusterProperties.put(ClusterConfig.ClusterConfigProperty.PERSIST_BEST_POSSIBLE_ASSIGNMENT.name(), String.valueOf(true));
        // Topology and fault zone type fields are used by CRUSH alg. Helix would apply the constrains on CRUSH alg to choose proper instance to hold the replica.
        helixClusterProperties.put(ClusterConfig.ClusterConfigProperty.TOPOLOGY.name(), "/" + HelixUtils.TOPOLOGY_CONSTRAINT);
        helixClusterProperties.put(ClusterConfig.ClusterConfigProperty.FAULT_ZONE_TYPE.name(), HelixUtils.TOPOLOGY_CONSTRAINT);
        helixAdminClient.createVeniceStorageCluster(clusterName, controllerClusterName, helixClusterProperties);
    }

    // TODO remove this method once we are fully on HaaS
    private void createClusterIfRequired(String clusterName) {
        if(admin.getClusters().contains(clusterName)) {
            logger.info("Cluster  " + clusterName + " already exists. ");
            return;
        }

        boolean isClusterCreated = admin.addCluster(clusterName, false);
        if (!isClusterCreated) {
            logger.info("Cluster  " + clusterName + " Creation returned false. ");
            return;
        }

        VeniceControllerClusterConfig config = multiClusterConfigs.getControllerConfig(clusterName);
        HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).
                forCluster(clusterName).build();
        Map<String, String> helixClusterProperties = new HashMap<String, String>();
        helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
        long delayedTime = config.getDelayToRebalanceMS();
        if (delayedTime > 0) {
            helixClusterProperties.put(ClusterConfig.ClusterConfigProperty.DELAY_REBALANCE_TIME.name(),
                String.valueOf(delayedTime));
        }
        // Topology and fault zone type fields are used by CRUSH alg. Helix would apply the constrains on CRUSH alg to choose proper instance to hold the replica.
        helixClusterProperties.put(ClusterConfig.ClusterConfigProperty.TOPOLOGY.name(), "/" + HelixUtils.TOPOLOGY_CONSTRAINT);
        helixClusterProperties.put(ClusterConfig.ClusterConfigProperty.FAULT_ZONE_TYPE.name(), HelixUtils.TOPOLOGY_CONSTRAINT);
        admin.setConfig(configScope, helixClusterProperties);
        logger.info(
            "Cluster  " + clusterName + "  Completed, auto join to true. Delayed rebalance time:" + delayedTime);

        admin.addStateModelDef(clusterName, VeniceStateModel.PARTITION_ONLINE_OFFLINE_STATE_MODEL,
            VeniceStateModel.getDefinition());
        admin.addStateModelDef(clusterName, LeaderStandbySMD.name,
            LeaderStandbySMD.build());

        admin
            .addResource(controllerClusterName, clusterName, CONTROLLER_CLUSTER_NUMBER_OF_PARTITION, LeaderStandbySMD.name,
                IdealState.RebalanceMode.FULL_AUTO.toString(), AutoRebalanceStrategy.class.getName());
        IdealState idealState = admin.getResourceIdealState(controllerClusterName, clusterName);
        // Use crush alg to allocate controller as well.
        idealState.setMinActiveReplicas(controllerClusterReplica);
        idealState.setRebalancerClassName(DelayedAutoRebalancer.class.getName());
        idealState.setRebalanceStrategy(CrushRebalanceStrategy.class.getName());
        admin.setResourceIdealState(controllerClusterName, clusterName, idealState);
        admin.rebalance(controllerClusterName, clusterName, controllerClusterReplica);
    }

    private void throwStoreAlreadyExists(String clusterName, String storeName) {
        String errorMessage = "Store:" + storeName + " already exists. Can not add it to cluster:" + clusterName;
        logger.error(errorMessage);
        throw new VeniceStoreAlreadyExistsException(storeName, clusterName);
    }

    private void throwStoreDoesNotExist(String clusterName, String storeName) {
        String errorMessage = "Store:" + storeName + " does not exist in cluster:" + clusterName;
        logger.error(errorMessage);
        throw new VeniceNoStoreException(storeName, clusterName);
    }

    private void throwVersionAlreadyExists(String storeName, int version) {
        String errorMessage =
            "Version" + version + " already exists in Store:" + storeName + ". Can not add it to store.";
        logAndThrow(errorMessage);
    }

    private void throwPartitionCountMismatch(String clusterName, String storeName, int version, int partitionCount,
        int storePartitionCount) {
        String errorMessage = "Partition mismatch for store " + storeName + " version " + version + " in cluster "
            + clusterName + " Found partition count of " + partitionCount + " but partition count in store config is "
            + storePartitionCount;
        logAndThrow(errorMessage);
    }

    private void throwClusterNotInitialized(String clusterName) {
        throw new VeniceNoClusterException(clusterName);
    }

    private void logAndThrow(String msg){
        logger.info(msg);
        throw new VeniceException(msg);
    }

    @Override
    public String getKafkaBootstrapServers(boolean isSSL) {
        if(isSSL) {
            return kafkaSSLBootstrapServers;
        } else {
            return kafkaBootstrapServers;
        }
    }

    @Override
    public Pair<String, String> getNativeReplicationKafkaBootstrapServerAndZkAddress(String sourceFabric) {
        return Pair.create(multiClusterConfigs.getChildDataCenterKafkaUrlMap().get(sourceFabric),
                           multiClusterConfigs.getChildDataCenterKafkaZkMap().get(sourceFabric));
    }

    @Override
    public String getNativeReplicationSourceFabric(String clusterName, Store store, Optional<String> sourceGridFabric, Optional<String> emergencySourceRegion) {
        /**
         * Source fabric selection priority:
         * 1. Parent controller emergency source fabric config.
         * 2. H2V plugin source grid fabric config.
         * 3. Store level source fabric config.
         * 4. Cluster level source fabric config.
         */
        String sourceFabric = emergencySourceRegion.orElse(null);

        if (sourceGridFabric.isPresent() && (sourceFabric == null || sourceFabric.isEmpty())) {
            sourceFabric = sourceGridFabric.get();
        }

        if (sourceFabric == null || sourceFabric.isEmpty()) {
            sourceFabric = store.getNativeReplicationSourceFabric();
        }

        if (sourceFabric == null || sourceFabric.isEmpty()) {
            sourceFabric = multiClusterConfigs.getControllerConfig(clusterName).getNativeReplicationSourceFabric();
        }

        return sourceFabric;
    }

    @Override
    public boolean isSSLEnabledForPush(String clusterName, String storeName) {
        if (isSslToKafka()) {
            Store store = getStore(clusterName, storeName);
            if (store == null) {
                throw new VeniceNoStoreException(storeName);
            }
            if (store.isHybrid()) {
                if (multiClusterConfigs.getCommonConfig().isEnableNearlinePushSSLWhitelist()
                    && (!multiClusterConfigs.getCommonConfig().getPushSSLWhitelist().contains(storeName))) {
                    // whitelist is enabled but the given store is not in that list, so ssl is not enabled for this store.
                    return false;
                }
            } else {
                if (multiClusterConfigs.getCommonConfig().isEnableOfflinePushSSLWhitelist()
                    && (!multiClusterConfigs.getCommonConfig().getPushSSLWhitelist().contains(storeName))) {
                    // whitelist is enabled but the given store is not in that list, so ssl is not enabled for this store.
                    return false;
                }
            }
            // whitelist is not enabled, or whitelist is enabled and the given store is in that list, so ssl is enabled for this store for push.
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean isSslToKafka() {
        return this.multiClusterConfigs.isSslToKafka();
    }

    public TopicManagerRepository getTopicManagerRepository() {
        return this.topicManagerRepository;
    }

    @Override
    public TopicManager getTopicManager() {
        return this.topicManagerRepository.getTopicManager();
    }

    @Override
    public TopicManager getTopicManager(Pair<String, String> kafkaBootstrapServersAndZkAddress) {
        return this.topicManagerRepository.getTopicManager(kafkaBootstrapServersAndZkAddress);
    }

    @Override
    public boolean isMasterController(String clusterName) {
        VeniceControllerStateModel model = controllerStateModelFactory.getModel(clusterName);
        if (model == null ) {
            return false;
        }
        return model.isLeader();
    }

  /**
   * Calculate number of partition for given store by give size.
   */
    @Override
    public int calculateNumberOfPartitions(String clusterName, String storeName, long storeSize) {
        checkControllerMastership(clusterName);
        VeniceControllerClusterConfig config = getHelixVeniceClusterResources(clusterName).getConfig();
        return PartitionUtils.calculatePartitionCount(storeName, storeSize,
            getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository(),
            getHelixVeniceClusterResources(clusterName).getRoutingDataRepository(), config.getPartitionSize(),
            config.getNumberOfPartition(), config.getMaxNumberOfPartition());
  }

    @Override
    public int getReplicationFactor(String clusterName, String storeName) {
        int replicationFactor = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository().getStore(storeName).getReplicationFactor();
        if (replicationFactor <= 0) {
            throw new VeniceException("Unexpected replication factor: " + replicationFactor + " for store: "
                + storeName + " in cluster: " + clusterName);
        }
        return replicationFactor;
    }

    @Override
    public List<Replica> getReplicas(String clusterName, String kafkaTopic) {
        checkControllerMastership(clusterName);
        List<Replica> replicas = new ArrayList<>();
        PartitionAssignment partitionAssignment = getHelixVeniceClusterResources(clusterName).getRoutingDataRepository()
            .getPartitionAssignments(kafkaTopic);

        partitionAssignment.getAllPartitions().forEach(partition -> {
            int partitionId = partition.getId();
            partition.getAllInstances().forEach((helixState, instanceList) -> {
                instanceList.forEach(instance -> {
                    Replica replica = new Replica(instance, partitionId, kafkaTopic);
                    replica.setStatus(helixState);
                    replicas.add(replica);
                });
            });
        });

        return replicas;
    }

    @Override
    public List<Replica> getReplicasOfStorageNode(String cluster, String instanceId) {
        checkControllerMastership(cluster);
        return InstanceStatusDecider.getReplicasForInstance(getHelixVeniceClusterResources(cluster), instanceId);
    }

    @Override
    public NodeRemovableResult isInstanceRemovable(String clusterName, String helixNodeId, boolean isFromInstanceView) {
        checkControllerMastership(clusterName);
        return InstanceStatusDecider
            .isRemovable(getHelixVeniceClusterResources(clusterName), clusterName, helixNodeId, isFromInstanceView);
    }

    @Override
    public Instance getLeaderController(String clusterName) {
        if (multiClusterConfigs.getControllerConfig(clusterName).isVeniceClusterLeaderHAAS()) {
            return getVeniceControllerLeader(clusterName);
        } else {
            if (!multiClusterConfigs.getClusters().contains(clusterName)) {
                throw new VeniceNoClusterException(clusterName);
            }

            final int maxAttempts = 10;
            PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);

            for (int attempt = 1; attempt <= maxAttempts; ++attempt) {
                LiveInstance instance = manager.getHelixDataAccessor().getProperty(keyBuilder.controllerLeader());
                if (instance != null) {
                    String id = instance.getId();
                    return new Instance(id, Utils.parseHostFromHelixNodeIdentifier(id), Utils.parsePortFromHelixNodeIdentifier(id), multiClusterConfigs.getAdminSecurePort());
                }

                if (attempt < maxAttempts) {
                    logger.warn(
                        "Master controller does not exist, cluster=" + clusterName + ", attempt=" + attempt + "/" + maxAttempts);
                    Utils.sleep(5 * Time.MS_PER_SECOND);
                }
            }

            String message = "Master controller does not exist, cluster=" + clusterName;
            logger.error(message);
            throw new VeniceException(message);
        }
    }

    /**
     * Get the Venice controller leader for a given Venice cluster when running Helix as a Service. We need to look at
     * the external view of the controller cluster to find the Venice logic leader for a Venice cluster. In HaaS the
     * controller leader property will be a HaaS controller and is not the Venice controller that we want.
     * TODO replace the implementation of Admin#getMasterController with this method once we are fully on HaaS
     * @param clusterName of the Venice cluster
     * @return
     */
    private Instance getVeniceControllerLeader(String clusterName) {
        if (!multiClusterConfigs.getClusters().contains(clusterName)) {
            throw new VeniceNoClusterException(clusterName);
        }
        final int maxAttempts = 10;
        PropertyKey.Builder keyBuilder = new PropertyKey.Builder(controllerClusterName);
        String partitionName = HelixUtils.getPartitionName(clusterName, 0);
        for (int attempt = 1; attempt <= maxAttempts; ++attempt) {
            ExternalView externalView = manager.getHelixDataAccessor().getProperty(keyBuilder.externalView(clusterName));
            if (externalView == null || externalView.getStateMap(partitionName) == null) {
                // Assignment is incomplete, try again later
                continue;
            }
            Map<String, String> veniceClusterStateMap = externalView.getStateMap(partitionName);
            for (Map.Entry<String, String> instanceNameAndState : veniceClusterStateMap.entrySet()) {
                if (instanceNameAndState.getValue().equals(HelixState.LEADER_STATE)) {
                    // Found the Venice controller leader
                    String id = instanceNameAndState.getKey();
                    return new Instance(id, Utils.parseHostFromHelixNodeIdentifier(id),
                        Utils.parsePortFromHelixNodeIdentifier(id), multiClusterConfigs.getAdminSecurePort());
                }
            }
            if (attempt < maxAttempts) {
                logger.warn("Venice controller leader does not exist for cluster: " + clusterName + ", attempt="
                    + attempt + "/" + maxAttempts);
                Utils.sleep(5 * Time.MS_PER_SECOND);
            }
        }
        String message = "Unable to find Venice controller leader for cluster: " + clusterName + " after "
            + maxAttempts + " attempts";
        logger.error(message);
        throw new VeniceException(message);
    }

    @Override
    public void addInstanceToWhitelist(String clusterName, String helixNodeId) {
        checkControllerMastership(clusterName);
        whitelistAccessor.addInstanceToWhiteList(clusterName, helixNodeId);
    }

    @Override
    public void removeInstanceFromWhiteList(String clusterName, String helixNodeId) {
        checkControllerMastership(clusterName);
        whitelistAccessor.removeInstanceFromWhiteList(clusterName, helixNodeId);
    }

    @Override
    public Set<String> getWhitelist(String clusterName) {
        checkControllerMastership(clusterName);
        return whitelistAccessor.getWhiteList(clusterName);
    }

    protected void checkPreConditionForKillOfflinePush(String clusterName, String kafkaTopic) {
        checkControllerMastership(clusterName);
        if (!Version.isVersionTopicOrStreamReprocessingTopic(kafkaTopic)) {
            throw new VeniceException("Topic: " + kafkaTopic + " is not a valid Venice version topic.");
        }
    }

    private Optional<Version> getStoreVersion(String clusterName, String topic) {
        String storeName = Version.parseStoreFromKafkaTopicName(topic);
        int versionId = Version.parseVersionFromKafkaTopicName(topic);
        Store store = getStore(clusterName, storeName);
        if (null == store) {
            throw new VeniceNoStoreException(storeName, clusterName);
        }
        return store.getVersion(versionId);
    }

    @Override
    public void killOfflinePush(String clusterName, String kafkaTopic, boolean isForcedKill) {
        if (!isResourceStillAlive(kafkaTopic)) {
            /**
             * To avoid sending kill job messages if the resource is already removed, and this
             * could happen in the following scenario:
             * 1. The first try of resource deletion fails in the middle, which could be caused by
             * sending killing job message timeout;
             * 2. The second try of deleting the same resource will try to send the killing job message
             * again even the resource has already been deleted (kill job messages are eventually processed
             * by every participant);
             * So the killing job message is not necessary since the resource doesn't exist, and we should
             * not send kill job message since it is expensive and easy to time-out.
             */
            logger.info("Resource: " + kafkaTopic + " doesn't exist, kill job will be skipped");
            return;
        }
        checkPreConditionForKillOfflinePush(clusterName, kafkaTopic);
        if (!isForcedKill) {
            /**
             * Check whether the specified store is already terminated or not.
             * If yes, the kill job message will be skipped.
             */
            Optional<Version> version;
            try {
                version = getStoreVersion(clusterName, kafkaTopic);
            } catch (VeniceNoStoreException e) {
                logger.warn("Kill job will be skipped since the corresponding store for topic: " + kafkaTopic + " doesn't exist in cluster: " + clusterName);
                return;
            }
            if (version.isPresent() && VersionStatus.isBootstrapCompleted(version.get().getStatus())) {
                /**
                 * This is trying to avoid kill job entry in participant store if the version is already online.
                 * This won't solve all the edge cases since the following race condition could still happen, but it is fine.
                 * The race condition could be:
                 * 1. The version status is still not 'ONLINE' yet from Controller's view, then kill job message will be sent to storage node;
                 * 2. When storage node receives the kill job message, the version from storage node's perspective could be already
                 *    'online';
                 *
                 * The reason not to fix this race condition:
                 * 1. It should happen rarely;
                 * 2. This will change the lifecycle of store version greatly, which might introduce other potential issues;
                 *
                 * The target use case is for new fabric build out.
                 * In Blueshift project, we are planning to leverage participant store to short-cuit
                 * the bootstrap if the corresponding version has been killed in the source fabric.
                 * So in the new fabric, we need the kill-job message to be as accurate as possible to
                 * avoid the discrepancy.
                 */
                logger.info("Resource: " + kafkaTopic + " has finished bootstrapping, so kill job will be skipped");
                return;
            }
        }

        StatusMessageChannel messageChannel = getHelixVeniceClusterResources(clusterName).getMessageChannel();
        // As we should already have retry outside of this function call, so we do not need to retry again inside.
        int retryCount = 1;
        // Broadcast kill message to all of storage nodes assigned to given resource. Helix will help us to only send
        // message to the live instances.
        // The alternative way here is that get the storage nodes in BOOTSTRAP state of given resource, then send the
        // kill message node by node. Considering the simplicity, broadcast is a better.
        // In prospective of performance, each time helix sending a message needs to read the whole LIVE_INSTANCE and
        // EXTERNAL_VIEW from ZK, so sending message nodes by nodes would generate lots of useless read requests. Of course
        // broadcast would generate useless write requests to ZK(N-M useless messages, N=number of nodes assigned to resource,
        // M=number of nodes have completed the ingestion or have not started). But considering the number of nodes in
        // our cluster is not too big, so it's not a big deal here.
        if (multiClusterConfigs.getControllerConfig(clusterName).isAdminHelixMessagingChannelEnabled()) {
          messageChannel.sendToStorageNodes(clusterName, new KillOfflinePushMessage(kafkaTopic), kafkaTopic, retryCount);
        }
        if (multiClusterConfigs.getControllerConfig(clusterName).isParticipantMessageStoreEnabled() &&
            participantMessageStoreRTTMap.containsKey(clusterName)) {
            sendKillMessageToParticipantStore(clusterName, kafkaTopic);
        }
    }

    private void sendKillMessageToParticipantStore(String clusterName, String kafkaTopic) {
        VeniceWriter writer = getParticipantStoreWriter(clusterName);
        ParticipantMessageType killPushJobType = ParticipantMessageType.KILL_PUSH_JOB;
        ParticipantMessageKey key = new ParticipantMessageKey();
        key.resourceName = kafkaTopic;
        key.messageType = killPushJobType.getValue();
        KillPushJob message = new KillPushJob();
        message.timestamp = System.currentTimeMillis();
        ParticipantMessageValue value = new ParticipantMessageValue();
        value.messageType = killPushJobType.getValue();
        value.messageUnion = message;
        writer.put(key, value, PARTICIPANT_MESSAGE_STORE_SCHEMA_ID);
    }

    private VeniceWriter getParticipantStoreWriter(String clusterName) {
        return participantMessageWriterMap.computeIfAbsent(clusterName, k -> {
                int attempts = 0;
                boolean verified = false;
                String topic = participantMessageStoreRTTMap.get(clusterName);
                while (attempts < INTERNAL_STORE_GET_RRT_TOPIC_ATTEMPTS) {
                    if (getTopicManager().containsTopicAndAllPartitionsAreOnline(topic)) {
                        verified = true;
                        logger.info("Participant message store RTT topic set to " + topic + " for cluster "
                            + clusterName);
                        break;
                    }
                    attempts++;
                    Utils.sleep(INTERNAL_STORE_RTT_RETRY_BACKOFF_MS);
                }
                if (!verified) {
                    throw new VeniceException("Can't find the expected topic " + topic
                        + " for participant message store "
                        + VeniceSystemStoreUtils.getParticipantStoreNameForCluster(clusterName));
                }
                return getVeniceWriterFactory().createVeniceWriter(topic,
                    new VeniceAvroKafkaSerializer(ParticipantMessageKey.SCHEMA$.toString()),
                    new VeniceAvroKafkaSerializer(ParticipantMessageValue.SCHEMA$.toString()));
        });
    }

    @Override
    public StorageNodeStatus getStorageNodesStatus(String clusterName, String instanceId) {
        checkControllerMastership(clusterName);
        List<Replica> replicas = getReplicasOfStorageNode(clusterName, instanceId);
        StorageNodeStatus status = new StorageNodeStatus();
        for (Replica replica : replicas) {
            status.addStatusForReplica(HelixUtils.getPartitionName(replica.getResource(), replica.getPartitionId()),
                replica.getStatus());
        }
        return status;
    }

    // TODO we don't use this function to check the storage node status. The isRemovable looks enough to ensure the
    // TODO upgrading would not affect the push and online reading request. Leave this function here to see do we need it in the future.
    @Override
    public boolean isStorageNodeNewerOrEqualTo(String clusterName, String instanceId,
        StorageNodeStatus oldStatus) {
        checkControllerMastership(clusterName);
        StorageNodeStatus currentStatus = getStorageNodesStatus(clusterName, instanceId);
        return currentStatus.isNewerOrEqual(oldStatus);
    }

    @Override
    public void setDelayedRebalanceTime(String clusterName, long delayedTime) {
        boolean enable = delayedTime > 0;
        PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
        ClusterConfig clusterConfig = manager.getHelixDataAccessor().getProperty(keyBuilder.clusterConfig());
        clusterConfig.getRecord()
            .setLongField(ClusterConfig.ClusterConfigProperty.DELAY_REBALANCE_TIME.name(), delayedTime);
        manager.getHelixDataAccessor().setProperty(keyBuilder.clusterConfig(), clusterConfig);
        //TODO use the helix new config API below once it's ready. Right now helix has a bug that controller would not get the update from the new config.
        /* ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
        HelixConfigScope clusterScope =
            new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(clusterName).build();
        configAccessor.set(clusterScope, ClusterConfig.ClusterConfigProperty.DELAY_REBALANCE_DISABLED.name(),
            String.valueOf(disable));*/
        logger.info(enable ? "Enable"
            : "Disable" + " delayed rebalance for cluster:" + clusterName + (enable ? " with delayed time:"
                + delayedTime : ""));
    }

    @Override
    public long getDelayedRebalanceTime(String clusterName) {
        PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
        ClusterConfig clusterConfig = manager.getHelixDataAccessor().getProperty(keyBuilder.clusterConfig());
        return clusterConfig.getRecord()
            .getLongField(ClusterConfig.ClusterConfigProperty.DELAY_REBALANCE_TIME.name(), 0l);
    }

    public void setAdminConsumerService(String clusterName, AdminConsumerService service){
        adminConsumerServices.put(clusterName, service);
    }

    @Override
    public void skipAdminMessage(String clusterName, long offset, boolean skipDIV){
        if (adminConsumerServices.containsKey(clusterName)){
            adminConsumerServices.get(clusterName).setOffsetToSkip(clusterName, offset, skipDIV);
        } else {
            throw new VeniceException("Cannot skip execution, must first setAdminConsumerService for cluster " + clusterName);
        }

    }

    @Override
    public Long getLastSucceedExecutionId(String clusterName) {
        if (adminConsumerServices.containsKey(clusterName)) {
            return adminConsumerServices.get(clusterName).getLastSucceededExecutionIdInCluster(clusterName);
        } else {
            throw new VeniceException(
                "Cannot get the last succeed execution Id, must first setAdminConsumerService for cluster "
                    + clusterName);
        }
    }

    /**
     * Get last succeeded execution id for a given store; if storeName is null, return the last succeeded execution id
     * for the cluster
     * @param clusterName
     * @param storeName
     * @return the last succeeded execution id or null if the cluster/store is invalid or the admin consumer service
     *         for the given cluster is not up and running yet.
     */
    public Long getLastSucceededExecutionId(String clusterName, String storeName) {
        if (storeName == null) {
            return getLastSucceedExecutionId(clusterName);
        } else {
            return adminConsumerServices.containsKey(clusterName)
                ? adminConsumerServices.get(clusterName).getLastSucceededExecutionId(storeName) : null;
        }
    }

    public Exception getLastExceptionForStore(String clusterName, String storeName) {
        return adminConsumerServices.containsKey(clusterName)
            ? adminConsumerServices.get(clusterName).getLastExceptionForStore(storeName) : null;
    }

    @Override
    public Optional<AdminCommandExecutionTracker> getAdminCommandExecutionTracker(String clusterName) {
        return Optional.empty();
    }

    @Override
    public RoutersClusterConfig getRoutersClusterConfig(String clusterName) {
        checkControllerMastership(clusterName);
        ZkRoutersClusterManager routersClusterManager = getHelixVeniceClusterResources(clusterName).getRoutersClusterManager();
        return routersClusterManager.getRoutersClusterConfig();
    }

    @Override
    public void updateRoutersClusterConfig(String clusterName, Optional<Boolean> isThrottlingEnable,
        Optional<Boolean> isQuotaRebalancedEnable, Optional<Boolean> isMaxCapaictyProtectionEnabled,
        Optional<Integer> expectedRouterCount) {
        ZkRoutersClusterManager routersClusterManager = getHelixVeniceClusterResources(clusterName).getRoutersClusterManager();

        checkControllerMastership(clusterName);
        if (isThrottlingEnable.isPresent()) {
            routersClusterManager.enableThrottling(isThrottlingEnable.get());
        }
        if (isMaxCapaictyProtectionEnabled.isPresent()) {
            routersClusterManager.enableMaxCapacityProtection(isMaxCapaictyProtectionEnabled.get());
        }
        if (isQuotaRebalancedEnable.isPresent() && expectedRouterCount.isPresent()) {
            routersClusterManager.enableQuotaRebalance(isQuotaRebalancedEnable.get(), expectedRouterCount.get());
        }
    }

    @Override
    public Map<String, String> getAllStorePushStrategyForMigration() {
        throw new VeniceUnsupportedOperationException("getAllStorePushStrategyForMigration");
    }

    @Override
    public void setStorePushStrategyForMigration(String voldemortStoreName, String strategy) {
        throw new VeniceUnsupportedOperationException("setStorePushStrategyForMigration");
    }

    @Override
    public Pair<String, String> discoverCluster(String storeName) {
        StoreConfig config = storeConfigRepo.getStoreConfigOrThrow(storeName);
        if (config == null || Utils.isNullOrEmpty(config.getCluster())) {
            throw new VeniceNoStoreException("Could not find the given store: " + storeName
            + ". Make sure the store is created and the provided store name is correct");
        }
        String clusterName = config.getCluster();
        String d2Service = multiClusterConfigs.getClusterToD2Map().get(clusterName);
        if (d2Service == null) {
            throw new VeniceException("Could not find d2 service by given cluster: " + clusterName);
        }
        return new Pair<>(clusterName, d2Service);
    }

    @Override
    public Map<String, String> findAllBootstrappingVersions(String clusterName) {
        checkControllerMastership(clusterName);
        Map<String, String> result = new HashMap<>();
        // Find all ongoing offline pushes at first.
        PushMonitor monitor = getHelixVeniceClusterResources(clusterName).getPushMonitor();
        monitor.getTopicsOfOngoingOfflinePushes().forEach(topic -> result.put(topic, VersionStatus.STARTED.toString()));
        // Find the versions which had been ONLINE, but some of replicas are still bootstrapping due to:
        // 1. As we use N-1 strategy, so there might be some slow replicas caused by kafka or other issues.
        // 2. Storage node was added/removed/disconnected, so replicas need to bootstrap again on the same or orther node.
        RoutingDataRepository routingDataRepository = getHelixVeniceClusterResources(clusterName).getRoutingDataRepository();
        ReadWriteStoreRepository storeRepository = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
        ResourceAssignment resourceAssignment = routingDataRepository.getResourceAssignment();
        for (String topic : resourceAssignment.getAssignedResources()) {
            if (result.containsKey(topic)) {
                continue;
            }
            PartitionAssignment partitionAssignment = resourceAssignment.getPartitionAssignment(topic);
            for (Partition p : partitionAssignment.getAllPartitions()) {
                if (p.getBootstrapInstances().size() > 0) {
                    String storeName = Version.parseStoreFromKafkaTopicName(topic);
                    VersionStatus status;
                    Store store = storeRepository.getStore(storeName);
                    if (store != null) {
                        status = store.getVersionStatus(Version.parseVersionFromKafkaTopicName(topic));
                    } else {
                        status = NOT_CREATED;
                    }
                    result.put(topic, status.toString());
                    // Found at least one bootstrap replica, skip to next topic.
                    break;
                }
            }
        }
        return result;
    }

    public VeniceWriterFactory getVeniceWriterFactory() {
        return veniceWriterFactory;
    }

    @Override
    public VeniceControllerConsumerFactory getVeniceConsumerFactory() {
        return veniceConsumerFactory;
    }

    protected void startMonitorOfflinePush(String clusterName, String kafkaTopic, int numberOfPartition,
        int replicationFactor, OfflinePushStrategy strategy) {
        PushMonitorDelegator offlinePushMonitor = getHelixVeniceClusterResources(clusterName).getPushMonitor();
        offlinePushMonitor.startMonitorOfflinePush(
            kafkaTopic,
            numberOfPartition,
            replicationFactor,
            strategy);
    }

    protected void stopMonitorOfflinePush(String clusterName, String topic, boolean deletePushStatus) {
        PushMonitor offlinePushMonitor = getHelixVeniceClusterResources(clusterName).getPushMonitor();
        offlinePushMonitor.stopMonitorOfflinePush(topic, deletePushStatus);
    }

    protected Store checkPreConditionForUpdateStoreMetadata(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        ReadWriteStoreRepository repository = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
        Store store = repository.getStore(storeName);
        if (null == store) {
            throw new VeniceNoStoreException(storeName);
        }
        return store;
    }

    @Override
    public void close() {
        manager.disconnect();
        Utils.closeQuietlyWithErrorLogged(zkSharedSystemStoreRepository);
        Utils.closeQuietlyWithErrorLogged(zkSharedSchemaRepository);
        zkClient.close();
        jobTrackingVeniceWriterMap.forEach( (k, v) -> Utils.closeQuietlyWithErrorLogged(v));
        jobTrackingVeniceWriterMap.clear();
        participantMessageWriterMap.forEach( (k, v) -> Utils.closeQuietlyWithErrorLogged(v));
        participantMessageWriterMap.clear();
        metadataStoreWriter.close();
        Utils.closeQuietlyWithErrorLogged(topicManagerRepository);
        pushStatusStoreReader.ifPresent(PushStatusStoreReader::close);
        pushStatusStoreDeleter.ifPresent(PushStatusStoreRecordDeleter::close);
        clusterControllerClientPerColoMap.forEach((clusterName, controllerClientMap) -> controllerClientMap.values().forEach(c -> Utils.closeQuietlyWithErrorLogged(c)));
        D2ClientUtils.shutdownClient(d2Client);
    }

    /**
     * Check whether this controller is master or not. If not, throw the VeniceException to skip the request to
     * this controller.
     *
     * @param clusterName
     */
    protected void checkControllerMastership(String clusterName) {
        if (!isMasterController(clusterName)) {
            throw new VeniceException("This controller:" + controllerName + " is not the master of '" + clusterName
                + "'. Can not handle the admin request.");
        }
    }

    protected HelixVeniceClusterResources getHelixVeniceClusterResources(String cluster) {
        Optional<HelixVeniceClusterResources> resources = controllerStateModelFactory.getModel(cluster).getResources();
        if (!resources.isPresent()) {
            throwClusterNotInitialized(cluster);
        }
        return resources.get();
    }

    public void addConfig(VeniceControllerConfig config) {
        multiClusterConfigs.addClusterConfig(config);
    }

    public void addMetric(String clusterName, MetricsRepository metricsRepository){

    }

    public ZkWhitelistAccessor getWhitelistAccessor() {
        return whitelistAccessor;
    }

    public StoreGraveyard getStoreGraveyard() {
        return storeGraveyard;
    }

    public String getControllerName(){
        return controllerName;
    }

    @Override
    public HelixReadOnlyStoreConfigRepository getStoreConfigRepo() { return storeConfigRepo; }

    public interface StoreMetadataOperation {
        /**
         * define the operation that update a store. Return the store after metadata being updated so that it could
         * be updated by metadataRepository
         */
        Store update(Store store);
    }

    /**
     * This function is used to detect whether current node is the master controller of controller cluster.
     *
     * Be careful to use this function since it will talk to Zookeeper directly every time.
     *
     * @return
     */
    @Override
    public boolean isMasterControllerOfControllerCluster() {
        if (isControllerClusterHAAS) {
          return isMasterController(coloMasterClusterName);
        }
        LiveInstance leader = manager.getHelixDataAccessor().getProperty(level1KeyBuilder.controllerLeader());
        if (null == leader || null == leader.getId()) {
            logger.warn("Cannot determine the result of isMasterControllerOfControllerCluster(). "
                + "leader: " + leader
                + (null == leader ? "" : ", leader.getId(): " + leader.getId()));
            // Will result in a NPE... TODO: Investigate proper fix.
        }
        return leader.getId().equals(this.controllerName);
    }

    private Version setZkSharedStoreVersion(String clusterName, String zkSharedStoreName, String pushJobId, int versionNum,
        int numberOfPartitions, Version.PushType pushType, String remoteKafkaBootstrapServers) {
        Version newVersion = new VersionImpl(zkSharedStoreName, versionNum, pushJobId, numberOfPartitions);
        newVersion.setPushType(pushType);
        checkControllerMastership(clusterName);
        HelixVeniceClusterResources resources = getHelixVeniceClusterResources(clusterName);
        try {
            try (AutoCloseableLock ignore = resources.getClusterLockManager().createStoreWriteLock(zkSharedStoreName)) {
                ReadWriteStoreRepository repository = resources.getStoreMetadataRepository();
                Store zkSharedStore = repository.getStore(zkSharedStoreName);
                zkSharedStore.addVersion(newVersion);
                zkSharedStore.setCurrentVersion(newVersion.getNumber());
                zkSharedStore.updateVersionStatus(versionNum, ONLINE);
                if (newVersion.isNativeReplicationEnabled()) {
                    if (remoteKafkaBootstrapServers != null) {
                        newVersion.setPushStreamSourceAddress(remoteKafkaBootstrapServers);
                    } else {
                        newVersion.setPushStreamSourceAddress(getKafkaBootstrapServers(isSslToKafka()));
                    }
                }
                repository.updateStore(zkSharedStore);
                logger.info("New version: " + newVersion.getNumber() + " created for Zk shared store: "
                    + zkSharedStoreName);
            }
        } catch (Exception e) {
            throw new VeniceException("Failed to set new version: " + newVersion.getNumber() + " for Zk shared store: "
                + zkSharedStoreName, e);
        }
        return newVersion;
    }

    @Override
    public void dematerializeMetadataStoreVersion(String clusterName, String storeName, int versionNumber, boolean deleteRT) {
        String metadataStoreName = VeniceSystemStoreUtils.getMetadataStoreName(storeName);
        deleteOldVersionInStore(clusterName, metadataStoreName, versionNumber);
        if (deleteRT) {
            // Clean up venice writer before truncating RT topic
            metadataStoreWriter.removeMetadataStoreWriter(storeName);
            truncateKafkaTopic(Version.composeRealTimeTopic(metadataStoreName));
        }
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setStoreMetadataSystemStoreEnabled(false);
            return store;
        });
    }

    public void setStoreConfigForMigration(String storeName, String srcClusterName, String destClusterName) {
        ZkStoreConfigAccessor storeConfigAccessor = getHelixVeniceClusterResources(srcClusterName).getStoreConfigAccessor();
        StoreConfig storeConfig = storeConfigAccessor.getStoreConfig(storeName);
        storeConfig.setMigrationSrcCluster(srcClusterName);
        storeConfig.setMigrationDestCluster(destClusterName);
        Store store = getStore(srcClusterName, storeName);
        storeConfigAccessor.updateConfig(storeConfig, store.isStoreMetaSystemStoreEnabled());
    }

    @Override
    public void  updateAclForStore(String clusterName, String storeName, String accessPermissions) {
        throw new VeniceUnsupportedOperationException("updateAclForStore is not supported in child controller!");
    }

    @Override
    public String getAclForStore(String clusterName, String storeName) {
        throw new VeniceUnsupportedOperationException("getAclForStore is not supported!");
    }

    @Override
    public void deleteAclForStore(String clusterName, String storeName) {
        throw new VeniceUnsupportedOperationException("deleteAclForStore is not supported!");
    }

    @Override
    public void configureNativeReplication(String clusterName, VeniceUserStoreType storeType, Optional<String> storeName,
        boolean enableNativeReplicationForCluster, Optional<String> newSourceFabric, Optional<String> regionsFilter) {
        /**
         * Check whether the command affects this fabric.
         */
        if (regionsFilter.isPresent()) {
            Set<String> fabrics = parseRegionsFilterList(regionsFilter.get());
            if (!fabrics.contains(multiClusterConfigs.getRegionName())) {
                logger.info("EnableNativeReplicationForCluster command will be skipped for cluster " + clusterName
                    + ", because the fabrics filter is " + fabrics.toString() + " which doesn't include the "
                    + "current fabric: " + multiClusterConfigs.getRegionName());
                return;
            }
        }

        VeniceControllerClusterConfig clusterConfig = getHelixVeniceClusterResources(clusterName).getConfig();
        if (storeName.isPresent()) {
            /**
             * Legacy stores venice_system_store_davinci_push_status_store_<cluster_name> still exist.
             * But {@link com.linkedin.venice.helix.HelixReadOnlyStoreRepositoryAdapter#getStore(String)} cannot find
             * them by store names. Skip davinci push status stores until legacy znodes are cleaned up.
             */
            VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName.get());
            if (systemStoreType != null && systemStoreType.equals(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE)) {
                logger.info("Will not enable native replication for davinci push status store " + storeName.get());
                return;
            }

            /**
             * The function is invoked by {@link com.linkedin.venice.controller.kafka.consumer.AdminExecutionTask} if the
             * storeName is present.
             */
            Store originalStore = getStore(clusterName, storeName.get());
            if (null == originalStore) {
                throw new VeniceException("The store '" + storeName.get() + "' in cluster '" + clusterName + "' does not exist, and thus cannot be updated.");
            }
            boolean shouldUpdateNativeReplication = false;
            switch (storeType) {
                case BATCH_ONLY:
                    shouldUpdateNativeReplication = !originalStore.isHybrid() && !originalStore.isIncrementalPushEnabled() && !originalStore.isSystemStore();
                    break;
                case HYBRID_ONLY:
                    shouldUpdateNativeReplication = originalStore.isHybrid() && !originalStore.isIncrementalPushEnabled() && !originalStore.isSystemStore();
                    break;
                case INCREMENTAL_PUSH:
                    shouldUpdateNativeReplication = originalStore.isIncrementalPushEnabled() && !originalStore.isSystemStore();
                    break;
                case HYBRID_OR_INCREMENTAL:
                    shouldUpdateNativeReplication = (originalStore.isHybrid() || originalStore.isIncrementalPushEnabled()) && !originalStore.isSystemStore();
                    break;
                case SYSTEM:
                    shouldUpdateNativeReplication = originalStore.isSystemStore();
                    break;
                case ALL:
                    shouldUpdateNativeReplication = true;
                    break;
                default:
                    throw new VeniceException("Unsupported store type." + storeType);
            }
            /**
             * If the command is trying to enable native replication, the store must have Leader/Follower state model enabled.
             */
            if (enableNativeReplicationForCluster) {
                shouldUpdateNativeReplication &= (originalStore.isLeaderFollowerModelEnabled() || clusterConfig.isLfModelDependencyCheckDisabled());
            }
            if (shouldUpdateNativeReplication) {
                logger.info("Will enable native replication for store " + storeName.get());
                setNativeReplicationEnabled(clusterName, storeName.get(), enableNativeReplicationForCluster);
                newSourceFabric.ifPresent(f -> setNativeReplicationSourceFabric(clusterName, storeName.get(), f));
            } else {
                logger.info("Will not enable native replication for store " + storeName.get());
            }
        } else {
            /**
             * The batch update command hits child controller directly; all stores in the cluster will be updated
             */
            List<Store> storesToBeConfigured;
            switch (storeType) {
                case BATCH_ONLY:
                    storesToBeConfigured = getAllStores(clusterName).stream()
                        .filter(s -> (!s.isHybrid() && !s.isIncrementalPushEnabled() && !s.isSystemStore()))
                        .collect(Collectors.toList());
                    break;
                case HYBRID_ONLY:
                    storesToBeConfigured = getAllStores(clusterName).stream()
                        .filter(s -> (s.isHybrid() && !s.isIncrementalPushEnabled() && !s.isSystemStore()))
                        .collect(Collectors.toList());
                    break;
                case INCREMENTAL_PUSH:
                    storesToBeConfigured = getAllStores(clusterName).stream()
                        .filter(s -> (s.isIncrementalPushEnabled() && !s.isSystemStore()))
                        .collect(Collectors.toList());
                    break;
                case HYBRID_OR_INCREMENTAL:
                    storesToBeConfigured = getAllStores(clusterName).stream()
                        .filter(s -> ((s.isHybrid() || s.isIncrementalPushEnabled()) && !s.isSystemStore()))
                        .collect(Collectors.toList());
                    break;
                case SYSTEM:
                    storesToBeConfigured = getAllStores(clusterName).stream()
                        .filter(Store::isSystemStore)
                        .collect(Collectors.toList());
                    break;
                case ALL:
                    storesToBeConfigured = getAllStores(clusterName);
                    break;
                default:
                    throw new VeniceException("Unsupported store type." + storeType);
            }

            storesToBeConfigured.forEach(store -> {
                if (enableNativeReplicationForCluster && !(store.isLeaderFollowerModelEnabled() || clusterConfig.isLfModelDependencyCheckDisabled())) {
                    logger.info("Will not enable native replication for store " + store.getName()
                        + " since it doesn't have Leader/Follower state model enabled.");
                } else {
                    logger.info("Will enable native replication for store " + store.getName());
                    setNativeReplicationEnabled(clusterName, store.getName(), enableNativeReplicationForCluster);
                    newSourceFabric.ifPresent(f -> setNativeReplicationSourceFabric(clusterName, storeName.get(), f));
                }
            });
        }
    }

    @Override
    public void configureActiveActiveReplication(String clusterName, VeniceUserStoreType storeType, Optional<String> storeName,
        boolean enableActiveActiveReplicationForCluster, Optional<String> regionsFilter) {
        /**
         * Check whether the command affects this fabric.
         */
        if (regionsFilter.isPresent()) {
            Set<String> fabrics = parseRegionsFilterList(regionsFilter.get());
            if (!fabrics.contains(multiClusterConfigs.getRegionName())) {
                logger.info("EnableActiveActiveReplicationForCluster command will be skipped for cluster " + clusterName
                    + ", because the fabrics filter is " + fabrics.toString() + " which doesn't include the "
                    + "current fabric: " + multiClusterConfigs.getRegionName());
                return;
            }

        }

        VeniceControllerClusterConfig clusterConfig = getHelixVeniceClusterResources(clusterName).getConfig();
        if (storeName.isPresent()) {
            /**
             * Legacy stores venice_system_store_davinci_push_status_store_<cluster_name> still exist.
             * But {@link com.linkedin.venice.helix.HelixReadOnlyStoreRepositoryAdapter#getStore(String)} cannot find
             * them by store names. Skip davinci push status stores until legacy znodes are cleaned up.
             */
            VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName.get());
            if (systemStoreType != null && systemStoreType.equals(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE)) {
                logger.info("Will not enable active active replication for davinci push status store " + storeName.get());
                return;
            }

            /**
             * The function is invoked by {@link com.linkedin.venice.controller.kafka.consumer.AdminExecutionTask} if the
             * storeName is present.
             */
            Store originalStore = getStore(clusterName, storeName.get());
            if (null == originalStore) {
                throw new VeniceException("The store '" + storeName.get() + "' in cluster '" + clusterName + "' does not exist, and thus cannot be updated.");
            }
            boolean shouldUpdateActiveActiveReplication = false;
            switch (storeType) {
                case BATCH_ONLY:
                    shouldUpdateActiveActiveReplication = !originalStore.isHybrid() && !originalStore.isIncrementalPushEnabled() && !originalStore.isSystemStore();
                    break;
                case HYBRID_ONLY:
                    shouldUpdateActiveActiveReplication = originalStore.isHybrid() && !originalStore.isIncrementalPushEnabled() && !originalStore.isSystemStore();
                    break;
                case INCREMENTAL_PUSH:
                    shouldUpdateActiveActiveReplication = originalStore.isIncrementalPushEnabled() && !originalStore.isSystemStore();
                    break;
                case HYBRID_OR_INCREMENTAL:
                    shouldUpdateActiveActiveReplication = (originalStore.isHybrid() || originalStore.isIncrementalPushEnabled()) && !originalStore.isSystemStore();
                    break;
                case SYSTEM:
                    shouldUpdateActiveActiveReplication = originalStore.isSystemStore();
                    break;
                case ALL:
                    shouldUpdateActiveActiveReplication = true;
                    break;
                default:
                    break;
            }
            /**
             * If the command is trying to enable active active replication, the store must have Leader/Follower state model enabled.
             */
            if (enableActiveActiveReplicationForCluster) {
                shouldUpdateActiveActiveReplication &= (originalStore.isLeaderFollowerModelEnabled() || clusterConfig.isLfModelDependencyCheckDisabled());
                // Filter out aggregate mode store explicitly.
                if (originalStore.isHybrid() && originalStore.getHybridStoreConfig().getDataReplicationPolicy().equals(DataReplicationPolicy.AGGREGATE)) {
                    shouldUpdateActiveActiveReplication = false;
                }
            }
            if (shouldUpdateActiveActiveReplication) {
                logger.info("Will enable active active replication for store " + storeName.get());
                setActiveActiveReplicationEnabled(clusterName, storeName.get(), enableActiveActiveReplicationForCluster);
            } else {
                logger.info("Will not enable active active replication for store " + storeName.get());
            }
        } else {
            /**
             * The batch update command hits child controller directly; all stores in the cluster will be updated
             */
            List<Store> storesToBeConfigured;
            switch (storeType) {
                case BATCH_ONLY:
                    storesToBeConfigured = getAllStores(clusterName).stream()
                        .filter(s -> (!s.isHybrid() && !s.isIncrementalPushEnabled()))
                        .collect(Collectors.toList());
                    break;
                case HYBRID_ONLY:
                    storesToBeConfigured = getAllStores(clusterName).stream()
                        .filter(s -> (s.isHybrid() && !s.isIncrementalPushEnabled()))
                        .collect(Collectors.toList());
                    break;
                case INCREMENTAL_PUSH:
                    storesToBeConfigured = getAllStores(clusterName).stream()
                        .filter(Store::isIncrementalPushEnabled)
                        .collect(Collectors.toList());
                    break;
                case HYBRID_OR_INCREMENTAL:
                    storesToBeConfigured = getAllStores(clusterName).stream()
                        .filter(store -> (store.isHybrid() || store.isIncrementalPushEnabled()))
                        .collect(Collectors.toList());
                    break;
                case SYSTEM:
                    storesToBeConfigured = getAllStores(clusterName).stream()
                        .filter(Store::isSystemStore)
                        .collect(Collectors.toList());
                    break;
                case ALL:
                    storesToBeConfigured = getAllStores(clusterName);
                    break;
                default:
                    throw new VeniceException("Unsupported store type." + storeType);
            }

            // Filter out aggregate mode store explicitly.
            storesToBeConfigured = storesToBeConfigured.stream()
                .filter(store -> !(store.isHybrid() && store.getHybridStoreConfig().getDataReplicationPolicy().equals(DataReplicationPolicy.AGGREGATE)))
                .collect(Collectors.toList());
            storesToBeConfigured.forEach(store -> {
                if (enableActiveActiveReplicationForCluster && !(store.isLeaderFollowerModelEnabled() || clusterConfig.isLfModelDependencyCheckDisabled())) {
                    logger.info("Will not enable active active replication for store " + store.getName()
                        + " since it doesn't have Leader/Follower state model enabled.");
                } else {
                    logger.info("Will enable active active replication for store " + store.getName());
                    setActiveActiveReplicationEnabled(clusterName, store.getName(), enableActiveActiveReplicationForCluster);
                }
            });
        }
    }

    @Override
    public void checkResourceCleanupBeforeStoreCreation(String clusterName, String storeName) {
        checkResourceCleanupBeforeStoreCreation(clusterName, storeName, true);
    }

    protected ZkStoreConfigAccessor getStoreConfigAccessor(String clusterName) {
        return getHelixVeniceClusterResources(clusterName).getStoreConfigAccessor();
    }

    protected ReadWriteStoreRepository getMetadataRepository(String clusterName) {
        return getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
    }

    protected void checkResourceCleanupBeforeStoreCreation(String clusterName, String storeName, boolean checkHelixResource) {
        checkControllerMastership(clusterName);
        ZkStoreConfigAccessor storeConfigAccess = getStoreConfigAccessor(clusterName);
        StoreConfig storeConfig = storeConfigAccess.getStoreConfig(storeName);
        if (storeConfig != null) {
            throw new VeniceException("Store: " + storeName + " still exists in cluster: " + storeConfig.getCluster());
        }
        ReadWriteStoreRepository repository = getMetadataRepository(clusterName);
        Store store = repository.getStore(storeName);
        if (store != null) {
            throw new VeniceException("Store: " + storeName + " still exists in cluster: " + clusterName);
        }

        Set<String> allRelevantStores = new HashSet<>();
        Arrays.stream(VeniceSystemStoreType.values()).forEach(s -> allRelevantStores.add(s.getSystemStoreName(storeName)));
        allRelevantStores.add(storeName);

        // Check all the topics belonging to this store.
        Set<String> topics = getTopicManager().listTopics();
        /**
         * So far, there is still a policy for Venice Store to keep the latest couple of topics to avoid KMM crash, so
         * this function will skip the Version Topic check for now.
         * Once Venice gets rid of KMM, we could consider to remove all the deprecated version topics.
         * So for topic check, we will ensure the RT topic for the Venice store will be deleted, and all other system
         * store topics.
         */
        topics.forEach(topic -> {
            String storeNameForTopic = null;
            if (Version.isRealTimeTopic(topic)) {
                storeNameForTopic = Version.parseStoreFromRealTimeTopic(topic);
            } else if (Version.isVersionTopicOrStreamReprocessingTopic(topic)) {
                storeNameForTopic = Version.parseStoreFromKafkaTopicName(topic);
                if (storeNameForTopic.equals(storeName)) {
                    /** Skip Version Topic Check */
                    storeNameForTopic = null;
                }
            }
            if (storeNameForTopic != null && allRelevantStores.contains(storeNameForTopic)) {
                throw new VeniceException("Topic: " + ": " + topic + " still exists for store: " + storeName +
                    ", please make sure all the resources are removed before store re-creation");
            }
        });
        // Check all the helix resources.
        if (checkHelixResource) {
            List<String> helixAliveResources = getAllLiveHelixResources(clusterName);
            helixAliveResources.forEach(resource -> {
                if (Version.isVersionTopic(resource)) {
                    String storeNameForResource = Version.parseStoreFromKafkaTopicName(resource);
                    if (allRelevantStores.contains(storeNameForResource)) {
                        throw new VeniceException("Helix Resource: " + ": " + resource + " still exists for store: " + storeName +
                            ", please make sure all the resources are removed before store re-creation");
                    }
                }
            });
        }
    }

    protected Store checkPreConditionForAclOp(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        ReadWriteStoreRepository repository = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
        Store store = repository.getStore(storeName);
        if (store == null) {
            throwStoreDoesNotExist(clusterName, storeName);
        }
        return store;
    }

    private boolean isHybrid(HybridStoreConfig hybridStoreConfig) {
        /** A store is not hybrid in the following two scenarios:
         * If hybridStoreConfig is null, it means store is not hybrid.
         * If all of the hybrid config values are negative, it indicates that the store is being set back to batch-only store.
         */
        return hybridStoreConfig != null
            && (hybridStoreConfig.getRewindTimeInSeconds() >= 0
                || hybridStoreConfig.getOffsetLagThresholdToGoOnline() >= 0
                || hybridStoreConfig.getProducerTimestampLagThresholdToGoOnlineInSeconds() >= 0);
    }

    @Override
    public boolean isParent() {
        return multiClusterConfigs.isParent();
    }

    @Override
    public Map<String, String> getChildDataCenterControllerUrlMap(String clusterName) {
        /**
         * According to {@link VeniceControllerConfig#VeniceControllerConfig(VeniceProperties)}, the map is empty
         * if this is a child controller.
         */
        return multiClusterConfigs.getControllerConfig(clusterName).getChildDataCenterControllerUrlMap();
    }

    @Override
    public HelixReadOnlyZKSharedSystemStoreRepository getReadOnlyZKSharedSystemStoreRepository() {
        return zkSharedSystemStoreRepository;
    }

    @Override
    public HelixReadOnlyZKSharedSchemaRepository getReadOnlyZKSharedSchemaRepository() {
        return zkSharedSchemaRepository;
    }

    @Override
    public MetaStoreWriter getMetaStoreWriter() {
        return metaStoreWriter;
    }

    @Override
    public Optional<PushStatusStoreRecordDeleter> getPushStatusStoreRecordDeleter() {
        return pushStatusStoreDeleter;
    }

    @Override
    public Optional<String> getEmergencySourceRegion() {
        return multiClusterConfigs.getEmergencySourceRegion().equals("") ? Optional.empty() : Optional.of(multiClusterConfigs.getEmergencySourceRegion());
    }

    @Override
    public boolean isActiveActiveReplicationEnabledInAllRegion(String clusterName, String storeName) {
        throw new VeniceUnsupportedOperationException("isActiveActiveReplicationEnabledInAllRegion is not supported in child controller!");
    }

    public List<String> getClustersLeaderOf() {
        List<String> clusters = new ArrayList<>();
        for (VeniceControllerStateModel model : controllerStateModelFactory.getAllModels()) {
            if (model.getCurrentState().equals(LeaderStandbySMD.States.LEADER.toString())) {
                clusters.add(model.getClusterName());
            }
        }
        return clusters;
    }

    /**
     * Return the topic creation time if it has not been persisted to Zk yet.
     * @param topic The topic whose creation time is needed.
     * @return the topic creation time if it has not yet persisted to Zk yet. 0 if topic information has persisted to Zk or if the topic doesn't exist.
     */
    Long getInMemoryTopicCreationTime(String topic) {
        return topicToCreationTime.get(topic);
    }

    private void setUpDaVinciPushStatusStore(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        ReadWriteStoreRepository repository = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
        Store store = repository.getStore(storeName);
        if (store == null) {
            throwStoreDoesNotExist(clusterName, storeName);
        }
        String daVinciPushStatusStoreName = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName);
        getRealTimeTopic(clusterName, daVinciPushStatusStoreName);
        if (!store.isDaVinciPushStatusStoreEnabled()) {
            storeMetadataUpdate(clusterName, storeName, (s) -> {
                s.setDaVinciPushStatusStoreEnabled(true);
                return s;
            });
        }
    }

    public void produceSnapshotToMetaStoreRT(String clusterName, String regularStoreName) {
        checkControllerMastership(clusterName);
        ReadWriteStoreRepository repository = getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository();
        Store store = repository.getStore(regularStoreName);
        if (store == null) {
            throwStoreDoesNotExist(clusterName, regularStoreName);
        }
        String metaStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(regularStoreName);
        // Make sure RT topic is before producing the snapshot
        getRealTimeTopic(clusterName, metaStoreName);

        // Update the store flag to enable meta system store.
        if (!store.isStoreMetaSystemStoreEnabled()) {
            storeMetadataUpdate(clusterName, regularStoreName, (s) -> {
                s.setStoreMetaSystemStoreEnabled(true);
                return s;
            });
        }
        Optional<MetaStoreWriter> metaStoreWriter = getHelixVeniceClusterResources(clusterName).getMetaStoreWriter();
        if (!metaStoreWriter.isPresent()) {
            logger.info("MetaStoreWriter retrieved from VeniceHelixResource is absent, so producing snapshot to meta"
                + " store RT topic will be skipped for store: " + regularStoreName);
            return;
        }
        // Get updated store
        store = repository.getStore(regularStoreName);
        // Local store properties
        metaStoreWriter.get().writeStoreProperties(clusterName, store);
        // Store cluster configs
        metaStoreWriter.get().writeStoreClusterConfig(getHelixVeniceClusterResources(clusterName).getStoreConfigAccessor()
            .getStoreConfig(regularStoreName));
        logger.info("Wrote store property snapshot to meta system store for venice store: " + regularStoreName + " in cluster: " + clusterName);
        // Key/value schemas
        Collection<SchemaEntry> keySchemas = new HashSet<>();
        keySchemas.add(getKeySchema(clusterName, regularStoreName));
        metaStoreWriter.get().writeStoreKeySchemas(regularStoreName, keySchemas);
        logger.info("Wrote key schema to meta system store for venice store: " + regularStoreName + " in cluster: " + clusterName);
        Collection<SchemaEntry> valueSchemas = getValueSchemas(clusterName, regularStoreName);
        metaStoreWriter.get().writeStoreValueSchemas(regularStoreName, valueSchemas);
        logger.info("Wrote value schemas to meta system store for venice store: " + regularStoreName + " in cluster: " + clusterName);
        // replica status for all the available versions
        List<Version> versions = store.getVersions();
        if (versions.isEmpty()) {
            return;
        }
        for (Version version: versions) {
            int versionNumber = version.getNumber();
            String topic = Version.composeKafkaTopic(regularStoreName, versionNumber);
            OfflinePushStatus offlinePushStatus = getHelixVeniceClusterResources(clusterName).getPushMonitor().getOfflinePushOrThrow(topic);
            Collection<PartitionStatus> partitionStatuses = offlinePushStatus.getPartitionStatuses();
            for (PartitionStatus ps : partitionStatuses) {
                metaStoreWriter.get().writeStoreReplicaStatuses(clusterName, regularStoreName, versionNumber, ps.getPartitionId(), ps.getReplicaStatuses());
            }
            logger.info("Wrote replica status snapshot for version: " + versionNumber + " to meta system store for venice store: "
                + regularStoreName + " in cluster: " + clusterName);
        }
    }

    private boolean isAmplificationFactorUpdateOnly(PartitionerConfig originalPartitionerConfig, PartitionerConfig newPartitionerConfig) {
        if (newPartitionerConfig == null) {
            throw new VeniceException("New partitioner config is null, in theory it will never happen as we should pre-fill new partitioner config.");
        }
        // We verify if only amp factor is changed by updating original partition config's amp factor and compare it with given partitioner config.
        PartitionerConfig ampFactorUpdatedPartitionerConfig = originalPartitionerConfig == null ? new PartitionerConfigImpl() : originalPartitionerConfig.clone();
        ampFactorUpdatedPartitionerConfig.setAmplificationFactor(newPartitionerConfig.getAmplificationFactor());
        return Objects.equals(ampFactorUpdatedPartitionerConfig, newPartitionerConfig);
    }

    @Override
    public long getBackupVersionDefaultRetentionMs() {
        return backupVersionDefaultRetentionMs;
    }
}
