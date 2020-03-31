package com.linkedin.venice.controller;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.exception.HelixClusterMaintenanceModeException;
import com.linkedin.venice.controller.init.ClusterLeaderInitializationManager;
import com.linkedin.venice.controller.init.ClusterLeaderInitializationRoutine;
import com.linkedin.venice.controller.init.SystemSchemaInitializationRoutine;
import com.linkedin.venice.controller.kafka.StoreStatusDecider;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumerService;
import com.linkedin.venice.controller.kafka.consumer.VeniceControllerConsumerFactory;
import com.linkedin.venice.controller.stats.VeniceHelixAdminStats;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.exceptions.VeniceNoClusterException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.exceptions.VeniceStoreAlreadyExistsException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.helix.HelixReadWriteSchemaRepository;
import com.linkedin.venice.helix.HelixReadWriteStoreRepository;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.helix.HelixStoreGraveyard;
import com.linkedin.venice.helix.Replica;
import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.helix.ZkWhitelistAccessor;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.VeniceOperationAgainstKafkaTimedOut;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.ETLStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.InstanceStatus;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.ReadWriteSchemaRepository;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RoutersClusterConfig;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.StoreGraveyard;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.participant.protocol.KillPushJob;
import com.linkedin.venice.participant.protocol.ParticipantMessageKey;
import com.linkedin.venice.participant.protocol.ParticipantMessageValue;
import com.linkedin.venice.participant.protocol.enums.ParticipantMessageType;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.KillOfflinePushMessage;
import com.linkedin.venice.pushmonitor.PushMonitor;
import com.linkedin.venice.pushmonitor.PushMonitorDelegator;
import com.linkedin.venice.pushmonitor.PushStatusDecider;
import com.linkedin.venice.replication.LeaderStorageNodeReplicator;
import com.linkedin.venice.replication.TopicReplicator;
import com.linkedin.venice.schema.DerivedSchemaEntry;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.stats.AbstractVeniceAggStats;
import com.linkedin.venice.stats.ZkClientStatusStats;
import com.linkedin.venice.status.StatusMessageChannel;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.status.protocol.PushJobStatusRecordValue;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.PartitionCountUtils;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
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
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;

import static com.linkedin.venice.meta.VersionStatus.*;
/**
 * Helix Admin based on 0.6.6.4 APIs.
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
    private static final Logger logger = Logger.getLogger(VeniceHelixAdmin.class);

    private final VeniceControllerMultiClusterConfig multiClusterConfigs;
    private final String controllerClusterName;
    private final int controllerClusterReplica;
    private final String controllerName;
    private final String kafkaBootstrapServers;
    private final String kafkaSSLBootstrapServers;
    private final Map<String, AdminConsumerService> adminConsumerServices = new ConcurrentHashMap<>();

    public static final int CONTROLLER_CLUSTER_NUMBER_OF_PARTITION = 1;
    public static final long CONTROLLER_CLUSTER_RESOURCE_EV_TIMEOUT_MS = 1000*300l; // 5min
    public static final long CONTROLLER_CLUSTER_RESOURCE_EV_CHECK_DELAY_MS = 500l;
    public static final long WAIT_FOR_HELIX_RESOURCE_ASSIGNMENT_FINISH_RETRY_MS = 500;

    private static final int INTERNAL_STORE_GET_RRT_TOPIC_ATTEMPTS = 3;
    private static final long INTERNAL_STORE_RTT_RETRY_BACKOFF_MS = 5000;
    private static final int PARTICIPANT_MESSAGE_STORE_SCHEMA_ID = 1;

    // TODO remove this field and all invocations once we are fully on HaaS. Use the helixAdminClient instead.
    private final HelixAdmin admin;
    /**
     * Client/wrapper used for performing Helix operations in Venice.
     */
    private final HelixAdminClient helixAdminClient;
    private TopicManager topicManager;
    private final ZkClient zkClient;
    private final HelixAdapterSerializer adapterSerializer;
    private final ZkWhitelistAccessor whitelistAccessor;
    private final ExecutionIdAccessor executionIdAccessor;
    private final Optional<TopicReplicator> onlineOfflineTopicReplicator;
    private final Optional<TopicReplicator> leaderFollowerTopicReplicator;
    private final long deprecatedJobTopicRetentionMs;
    private final long deprecatedJobTopicMaxRetentionMs;
    private final ZkStoreConfigAccessor storeConfigAccessor;
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
    private final String pushJobStatusStoreName;

  /**
     * Level-1 controller, it always being connected to Helix. And will create sub-controller for specific cluster when
     * getting notification from Helix.
     */
    private SafeHelixManager manager;
    /**
     * Builder used to build the data path to access Helix internal data of level-1 cluster.
     */
    private PropertyKey.Builder level1KeyBuilder;

    private String pushJobStatusTopicName;
    private String pushJobDetailsRTTopic;

    // Those variables will be initialized lazily.
    private int pushJobStatusValueSchemaId = -1;
    private int pushJobDetailsSchemaId = -1;

    private static final String PUSH_JOB_DETAILS_WRITER = "PUSH_JOB_DETAILS_WRITER";
    private static final String PUSH_JOB_STATUS_WRITER = "PUSH_JOB_STATUS_WRITER";
    private final Map<String, VeniceWriter> jobTrackingVeniceWriterMap = new VeniceConcurrentHashMap<>();

    private VeniceDistClusterControllerStateModelFactory controllerStateModelFactory;

    public VeniceHelixAdmin(VeniceControllerMultiClusterConfig multiClusterConfigs, MetricsRepository metricsRepository) {
        this(multiClusterConfigs, metricsRepository, false, Optional.empty(), Optional.empty());
    }

    //TODO Use different configs for different clusters when creating helix admin.
    public VeniceHelixAdmin(VeniceControllerMultiClusterConfig multiClusterConfigs, MetricsRepository metricsRepository,
            boolean sslEnabled, Optional<SSLConfig> sslConfig, Optional<DynamicAccessController> accessController) {
        this.multiClusterConfigs = multiClusterConfigs;
        VeniceControllerConfig commonConfig = multiClusterConfigs.getCommonConfig();
        this.controllerName = Utils.getHelixNodeIdentifier(multiClusterConfigs.getAdminPort());
        this.controllerClusterName = multiClusterConfigs.getControllerClusterName();
        this.controllerClusterReplica = multiClusterConfigs.getControllerClusterReplica();
        this.kafkaBootstrapServers = multiClusterConfigs.getKafkaBootstrapServers();
        this.kafkaSSLBootstrapServers = multiClusterConfigs.getSslKafkaBootstrapServers();
        this.deprecatedJobTopicRetentionMs = multiClusterConfigs.getDeprecatedJobTopicRetentionMs();
        this.deprecatedJobTopicMaxRetentionMs = multiClusterConfigs.getDeprecatedJobTopicMaxRetentionMs();

        this.minNumberOfUnusedKafkaTopicsToPreserve = multiClusterConfigs.getMinNumberOfUnusedKafkaTopicsToPreserve();
        this.minNumberOfStoreVersionsToPreserve = multiClusterConfigs.getMinNumberOfStoreVersionsToPreserve();

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
         * org.I0Itec.zkclient.exception.ZkMarshallingError: java.io.NotSerializableException: org.apache.helix.ZNRecord
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

        this.topicManager = new TopicManager(multiClusterConfigs.getKafkaZkAddress(),
                                             multiClusterConfigs.getTopicManagerKafkaOperationTimeOutMs(),
                                             multiClusterConfigs.getTopicDeletionStatusPollIntervalMs(),
                                             multiClusterConfigs.getKafkaMinLogCompactionLagInMs(),
                                             veniceConsumerFactory);
        this.whitelistAccessor = new ZkWhitelistAccessor(zkClient, adapterSerializer);
        this.executionIdAccessor = new ZkExecutionIdAccessor(zkClient, adapterSerializer);
        this.storeConfigAccessor = new ZkStoreConfigAccessor(zkClient, adapterSerializer);
        this.storeConfigRepo = new HelixReadOnlyStoreConfigRepository(zkClient, adapterSerializer,
            commonConfig.getRefreshAttemptsForZkReconnect(), commonConfig.getRefreshIntervalForZkReconnectInMs());
        this.storeGraveyard = new HelixStoreGraveyard(zkClient, adapterSerializer, multiClusterConfigs.getClusters());
        veniceWriterFactory = new VeniceWriterFactory(commonConfig.getProps().toProperties());
        this.onlineOfflineTopicReplicator =
            TopicReplicator.getTopicReplicator(topicManager, commonConfig.getProps(), veniceWriterFactory);
        this.leaderFollowerTopicReplicator = TopicReplicator.getTopicReplicator(
            LeaderStorageNodeReplicator.class.getName(), topicManager, commonConfig.getProps(), veniceWriterFactory);
        this.participantMessageStoreRTTMap = new VeniceConcurrentHashMap<>();
        this.participantMessageWriterMap = new VeniceConcurrentHashMap<>();
        this.veniceHelixAdminStats = new VeniceHelixAdminStats(metricsRepository, "venice_helix_admin");
        isControllerClusterHAAS = commonConfig.isControllerClusterLeaderHAAS();
        coloMasterClusterName = commonConfig.getClusterName();
        pushJobStatusStoreClusterName = commonConfig.getPushJobStatusStoreClusterName();
        pushJobStatusStoreName = commonConfig.getPushJobStatusStoreName();

        List<ClusterLeaderInitializationRoutine> initRoutines = new ArrayList<>();
        initRoutines.add(new SystemSchemaInitializationRoutine(
            AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE, multiClusterConfigs, this));
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
            onlineOfflineTopicReplicator, leaderFollowerTopicReplicator, accessController);

        // Initialized the helix manger for the level1 controller. If the controller cluster leader is going to be in
        // HaaS then level1 controllers should be only in participant mode.
        initLevel1Controller(isControllerClusterHAAS);

        // Start store migration monitor background thread
        storeConfigRepo.refresh();
        startStoreMigrationMonitor();
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
    void setTopicManager(TopicManager topicManager) {
        this.topicManager = topicManager;
    }

    @Override
    public synchronized void start(String clusterName) {
        //Simply validate cluster name here.
        clusterName = clusterName.trim();
        if (clusterName.startsWith("/") || clusterName.endsWith("/") || clusterName.indexOf(' ') >= 0) {
            throw new IllegalArgumentException("Invalid cluster name:" + clusterName);
        }
        if (multiClusterConfigs.getConfigForCluster(clusterName).isVeniceClusterLeaderHAAS()) {
            setupStorageClusterAsNeeded(clusterName);
        } else {
            createClusterIfRequired(clusterName);
        }
        // The resource and partition may be disabled for this controller before, we need to enable again at first. Then the state transition will be triggered.
        List<String> partitionNames = new ArrayList<>();
        partitionNames.add(VeniceDistClusterControllerStateModel.getPartitionNameFromVeniceClusterName(clusterName));
        helixAdminClient.enablePartition(true, controllerClusterName, controllerName, clusterName, partitionNames);
        if (multiClusterConfigs.getConfigForCluster(clusterName).isParticipantMessageStoreEnabled()) {
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

    @Override
    public boolean isResourceStillAlive(String resourceName) {
        if (!Version.topicIsValidStoreVersion(resourceName)) {
            throw new VeniceException("Resource name: " + resourceName + " is invalid");
        }
        String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
        // Find out the cluster first
        StoreConfig storeConfig = getStoreConfigAccessor().getStoreConfig(storeName);
        if (null == storeConfig) {
            logger.info("StoreConfig doesn't exist for store: " + storeName + ", will treat resource:" + resourceName + " as deprecated");
            return false;
        }
        String clusterName = storeConfig.getCluster();
        // Compose ZK path for external view of the resource
        String externalViewPath = "/" + clusterName + "/EXTERNALVIEW/" + resourceName;
        return zkClient.exists(externalViewPath);
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
    public synchronized void addStore(String clusterName, String storeName, String owner, String keySchema,
        String valueSchema) {
        VeniceHelixResources resources = getVeniceHelixResource(clusterName);
        logger.info("Start creating store: " + storeName);
        resources.lockForMetadataOperation();
        try{
            checkPreConditionForAddStore(clusterName, storeName, keySchema, valueSchema);
            VeniceControllerClusterConfig config = getVeniceHelixResource(clusterName).getConfig();
            Store newStore = new Store(storeName, owner, System.currentTimeMillis(), config.getPersistenceType(),
                config.getRoutingStrategy(), config.getReadStrategy(), config.getOfflinePushStrategy());
            HelixReadWriteStoreRepository storeRepo = resources.getMetadataRepository();
            storeRepo.lock();
            try {
                Store existingStore = storeRepo.getStore(storeName);
                if (existingStore != null) {
                    // We already check the pre-condition before, so if we could find a store with the same name,
                    // it means the store is a legacy store which is left by a failed deletion. So we should delete it.
                    deleteStore(clusterName, storeName, existingStore.getLargestUsedVersionNumber());
                }
                // Now there is not store exists in the store repository, we will try to retrieve the info from the graveyard.
                // Get the largestUsedVersionNumber from graveyard to avoid resource conflict.
                int largestUsedVersionNumber = storeGraveyard.getLargestUsedVersionNumber(storeName);
                newStore.setLargestUsedVersionNumber(largestUsedVersionNumber);
                storeRepo.addStore(newStore);
                // Create global config for that store.
                storeConfigAccessor.createConfig(storeName, clusterName);
                logger.info("Store: " + storeName +
                    " has been created with largestUsedVersionNumber: " + newStore.getLargestUsedVersionNumber());
            } finally {
                storeRepo.unLock();
            }
            // Add schema
            HelixReadWriteSchemaRepository schemaRepo = resources.getSchemaRepository();
            schemaRepo.initKeySchema(storeName, keySchema);
            schemaRepo.addValueSchema(storeName, valueSchema, HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID);
            logger.info("Completed creating Store: " + storeName);
        }finally {
            resources.unlockForMetadataOperation();
        }
    }

    @Override
    /**
     * This method will delete store data, metadata, version and rt topics
     * One exception is for stores with isMigrating flag set. In that case, the corresponding kafka topics and storeConfig
     * will not be deleted so that they are still avaiable for the cloned store.
     */
    public synchronized void deleteStore(String clusterName, String storeName, int largestUsedVersionNumber) {
        checkControllerMastership(clusterName);
        logger.info("Start deleting store: " + storeName);
        VeniceHelixResources resources = getVeniceHelixResource(clusterName);
        resources.lockForMetadataOperation();
        try{
            HelixReadWriteStoreRepository storeRepository = resources.getMetadataRepository();
            storeRepository.lock();
            Store store = null;
            StoreConfig storeConfig = storeConfigAccessor.getStoreConfig(storeName);
            try {
                store = storeRepository.getStore(storeName);
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
                    storeConfigAccessor.updateConfig(storeConfig);
                }
                storeRepository.updateStore(store);
            } finally {
                storeRepository.unLock();
            }
            // Delete All versions and push statues
            deleteAllVersionsInStore(clusterName, storeName);
            resources.getPushMonitor().cleanupStoreStatus(storeName);
            // Clean up topics
            if (!store.isMigrating()) {
                truncateKafkaTopic(Version.composeRealTimeTopic(storeName));
            }
            if (store != null) {
                truncateOldTopics(clusterName, store, true);
            } else {
                // Defensive coding: This should never happen, unless someone adds a catch block to the above try/finally clause...
                logger.error("Unexpected null store instance...!");
            }
            // Move the store to graveyard. It will only re-create the znode for store's metadata excluding key and
            // value schemas.
            logger.info("Putting store: " + storeName + " into graveyard");
            storeGraveyard.putStoreIntoGraveyard(clusterName, storeRepository.getStore(storeName));
            // Helix will remove all data under this store's znode including key and value schemas.
            resources.getMetadataRepository().deleteStore(storeName);

            // Delete the config for this store after deleting the store.
            if (storeConfig.isDeleting()) {
                storeConfigAccessor.deleteConfig(storeName);
            }
            logger.info("Store " + storeName + " in cluster " + clusterName + " has been deleted.");
        } finally {
            resources.unlockForMetadataOperation();
        }
    }

    public synchronized void cloneStore(String srcClusterName, String destClusterName, StoreInfo srcStore,
        String keySchema, MultiSchemaResponse.Schema[] valueSchemas) {
        if (srcClusterName.equals(destClusterName)) {
            throw new VeniceException("Source cluster and destination cluster cannot be the same!");
        }

        String storeName = srcStore.getName();
        checkPreConditionForCloneStore(destClusterName, storeName);
        logger.info("Start cloning store: " + storeName);

        VeniceHelixResources resources = getVeniceHelixResource(destClusterName);
        HelixReadWriteSchemaRepository schemaRepo = resources.getSchemaRepository();
        HelixReadWriteStoreRepository storeRepo = resources.getMetadataRepository();

        // Create new store
        VeniceControllerClusterConfig config = getVeniceHelixResource(destClusterName).getConfig();
        Store clonedStore = new Store(storeName, srcStore.getOwner(), System.currentTimeMillis(), config.getPersistenceType(),
            config.getRoutingStrategy(), config.getReadStrategy(), config.getOfflinePushStrategy());

        storeRepo.lock();
        try {
            Store existingStore = storeRepo.getStore(storeName);
            if (existingStore != null) {
                throwStoreAlreadyExists(destClusterName, storeName);
            }
            storeRepo.addStore(clonedStore);
            logger.info("Cloned store " + storeName + " has been created with largestUsedVersionNumber "
                + clonedStore.getLargestUsedVersionNumber());

            //check store config
            if (!storeConfigAccessor.containsConfig(storeName)) {
                logger.warn("Expecting but cannot find storeConfig for this store " + storeName);
                storeConfigAccessor.createConfig(storeName, destClusterName);
            }
        } finally {
            storeRepo.unLock();
        }

        // Copy schemas
        synchronized (schemaRepo) {
            // Add key schema
            new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, keySchema); // Check whether key schema is valid. It should, because this is from an existing store.
            schemaRepo.initKeySchema(storeName, keySchema);

            // Add value schemas
            for (MultiSchemaResponse.Schema valueSchema : valueSchemas) {
                new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, valueSchema.getSchemaStr()); // Check whether value schema is valid. It should, because this is from an existing store.
                schemaRepo.addValueSchema(storeName, valueSchema.getSchemaStr(), valueSchema.getId());
            }
        }

        // Copy remaining properties that will make the cloned store almost identical to the original
        UpdateStoreQueryParams params = new UpdateStoreQueryParams(srcStore);
        this.updateStore(destClusterName, storeName, params);

        logger.info("Completed cloning Store: " + storeName);
    }

    private Integer fetchSystemStoreSchemaId(String clusterName, String storeName, String valueSchemaStr) {
        if (isMasterController(clusterName)) {
            // Can be fetched from local repository
            return getValueSchemaId(clusterName, storeName, valueSchemaStr);
        }
        ControllerClient controllerClient = new ControllerClient(clusterName,
            getMasterController(clusterName).getUrl(false), sslFactory);
        SchemaResponse response = controllerClient.getValueSchemaID(storeName, valueSchemaStr);
        if (response.isError()) {
            throw new VeniceException("Failed to fetch schema id for store: " + storeName + ", error: "
                + response.getError());
        }
        return response.getId();
    }

    // TODO duplicate code with VeniceHelixAdmin::sendPushJobStatusMessage but we intend to keep this method and remove
    // the other one in the near future.
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
                if (topicManager.containsTopic(expectedRTTopic)) {
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


    public void sendPushJobStatusMessage(PushJobStatusRecordKey key, PushJobStatusRecordValue value) {
        if (pushJobStatusStoreClusterName.isEmpty() || pushJobStatusStoreName.isEmpty()) {
            // Push job status upload store is not configured.
            throw new VeniceException("Unable to upload the push job status because corresponding store is not configured");
        }
        if (pushJobStatusTopicName == null) {
            int getTopicAttempts = 0;
            String expectedTopicName = Version.composeRealTimeTopic(pushJobStatusStoreName);
            // Retry with backoff to allow the store and topic to be created when the config is changed.
            while (getTopicAttempts < INTERNAL_STORE_GET_RRT_TOPIC_ATTEMPTS) {
                if (getTopicAttempts != 0)
                    Utils.sleep(INTERNAL_STORE_RTT_RETRY_BACKOFF_MS);
                if (topicManager.containsTopic(expectedTopicName)) {
                    pushJobStatusTopicName = expectedTopicName;
                    logger.info("Push job status topic name set to " + expectedTopicName);
                    break;
                }
                getTopicAttempts++;
            }
            if (pushJobStatusTopicName == null) {
                throw new VeniceException("Can't find the expected topic " + expectedTopicName
                    + " for push job status. Either the topic hasn't been created yet or it's misconfigured.");
            }
        }

        VeniceWriter pushJobStatusWriter = jobTrackingVeniceWriterMap.computeIfAbsent(PUSH_JOB_STATUS_WRITER, k -> {
            pushJobStatusValueSchemaId = fetchSystemStoreSchemaId(pushJobStatusStoreClusterName,
                pushJobStatusStoreName, value.getSchema().toString());
            return getVeniceWriterFactory().createVeniceWriter(pushJobStatusTopicName,
                new VeniceAvroKafkaSerializer(key.getSchema().toString()),
                new VeniceAvroKafkaSerializer(value.getSchema().toString()));
        });

        pushJobStatusWriter.put(key, value, pushJobStatusValueSchemaId, null);
        logger.info("Successfully sent push job status for store " + value.storeName.toString() + " in cluster ");
    }

    public void writeEndOfPush(String clusterName, String storeName, int versionNumber, boolean alsoWriteStartOfPush) {
        //validate store and version exist
        Store store = getStore(clusterName, storeName);

        if (null == store) {
            throw new VeniceNoStoreException(storeName);
        }

        if (store.getCurrentVersion() == versionNumber){
            throw new VeniceHttpException(HttpStatus.SC_CONFLICT, "Cannot end push for version " + versionNumber + " that is currently being served");
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
            () -> getVeniceWriterFactory().createVeniceWriter(topicToReceiveEndOfPush),
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

        // Get original store properties
        String srcControllerUrl = this.getMasterController(srcClusterName).getUrl(false);
        ControllerClient srcControllerClient = new ControllerClient(srcClusterName, srcControllerUrl, sslFactory);
        // Update migration src and dest cluster in storeConfig
        setStoreConfigForMigration(storeName, srcClusterName, destClusterName);

        UpdateStoreQueryParams params = new UpdateStoreQueryParams().setStoreMigration(true);
        StoreInfo srcStore = srcControllerClient.getStore(storeName).getStore();
        String srcKeySchema = srcControllerClient.getKeySchema(storeName).getSchemaStr();
        MultiSchemaResponse.Schema[] srcValueSchemasResponse = srcControllerClient.getAllValueSchema(storeName).getSchemas();
        this.cloneStore(srcClusterName, destClusterName, srcStore, srcKeySchema, srcValueSchemasResponse);
        // Decrease the largestUsedVersionNumber to trigger bootstrap in destination cluster
        params.setLargestUsedVersionNumber(0);
        this.updateStore(destClusterName, storeName, params);   // update cloned store in destination cluster
        List<Version> sourceOnlineVersions = srcStore
            .getVersions()
            .stream()
            .sorted(Comparator.comparingInt(Version::getNumber))
            .filter(version -> Arrays.asList(STARTED, PUSHED, ONLINE).contains(version.getStatus()))
            .collect(Collectors.toList());
        for (Version version : sourceOnlineVersions) {
            try {
                int partitionCount = topicManager.getPartitions(version.kafkaTopicName()).size();
                addVersionAndStartIngestion(destClusterName, storeName, version.getPushJobId(), version.getNumber(),
                    partitionCount, version.getPushType());
            } catch (Exception e) {
                logger.warn("An exception was thrown when attempting to add version and start ingestion for store "
                + storeName + " and version " + version.getNumber(), e);
            }
        }
         // Set store migration flag for the original store. Possible race condition where we miss a new push while we
         // are calling addVersionAndStartIngestion on existing ONLINE versions. However, if we update the store's
         // migrating status first then we might run into another race condition where for example v3 (the new push) can
         // be added prior to v1 and v2 (existing versions). Even worse, the source cluster controller could be calling
         // the destination cluster controller's addVersionAndStartIngestion prior to the store is fully cloned.
        UpdateStoreQueryParams srcStoreParams = new UpdateStoreQueryParams().setStoreMigration(true);
        srcControllerClient.updateStore(storeName, srcStoreParams);
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
        this.updateClusterDiscovery(storeName, clusterDiscovered, srcClusterName);
    }

    @Override
    public synchronized void updateClusterDiscovery(String storeName, String oldCluster, String newCluster) {
        StoreConfig storeConfig = this.storeConfigAccessor.getStoreConfig(storeName);
        if (storeConfig == null) {
            throw new VeniceException("Store config is empty!");
        } else if (!storeConfig.getCluster().equals(oldCluster)) {
            throw new VeniceException("Store " + storeName + " is expected to be in " + oldCluster + " cluster, but is actually in " + storeConfig.getCluster());
        }

        storeConfig.setCluster(newCluster);
        this.storeConfigAccessor.updateConfig(storeConfig);
        logger.info("Store " + storeName + " now belongs to cluster " + newCluster + " instead of " + oldCluster);
    }

    protected void checkPreConditionForCloneStore(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        checkStoreNameConflict(storeName);

        if (storeConfigAccessor.containsConfig(storeName)) {
            if (storeConfigAccessor.getStoreConfig(storeName).getCluster().equals(clusterName)) {
                // store exists in dest cluster
                throwStoreAlreadyExists(clusterName, storeName);
            }
        }
    }

    protected void checkPreConditionForAddStore(String clusterName, String storeName, String keySchema, String valueSchema) {
        if (!Store.isValidStoreName(storeName)) {
            throw new VeniceException("Invalid store name " + storeName + ". Only letters, numbers, underscore or dash");
        }
        checkControllerMastership(clusterName);
        checkStoreNameConflict(storeName);

        // Before creating store, check the global stores configs at first.
        // TODO As some store had already been created before we introduced global store config
        // TODO so we need a way to sync up the data. For example, while we loading all stores from ZK for a cluster,
        // TODO put them into global store configs.
        boolean isLagecyStore = false;
        if (storeConfigAccessor.containsConfig(storeName)) {
            // Controller was trying to delete the old store but failed.
            // Delete again before re-creating.
            // We lock the resource during deletion, so it's impossible to access the store which is being
            // deleted from here. So the only possible case is the deletion failed before.
            if (!storeConfigAccessor.getStoreConfig(storeName).isDeleting()) {
                throw new VeniceStoreAlreadyExistsException(storeName);
            } else {
                isLagecyStore = true;
            }
        }

        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        Store store = repository.getStore(storeName);
        // If the store exists in store repository and it's still active(not being deleted), we don't allow to re-create it.
        if (store != null && !isLagecyStore) {
            throwStoreAlreadyExists(clusterName, storeName);
        }

        // Check whether the schema is valid or not
        new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, keySchema);
        new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, valueSchema);
    }

    private void checkStoreNameConflict(String storeName) {
        if (storeName.equals(AbstractVeniceAggStats.STORE_NAME_FOR_TOTAL_STAT)) {
            throw new VeniceException("Store name: " + storeName + " clashes with the internal usage, please change it");
        }
    }

    protected Store checkPreConditionForDeletion(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
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
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        Store store = repository.getStore(storeName);
        checkPreConditionForSingleVersionDeletion(clusterName, storeName, store, versionNum);
        return store;
    }

    private void checkPreConditionForSingleVersionDeletion(String clusterName, String storeName, Store store, int versionNum) {
        if (store == null) {
            throwStoreDoesNotExist(clusterName, storeName);
        }
        if (store.getCurrentVersion() == versionNum) {
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
     * from the admin channel or child controller's endpoint. Therefore, this method is mainly invoked from the admin
     * task upon processing an add version message and for store migration.
     */
    @Override
    public void addVersionAndStartIngestion(
        String clusterName, String storeName, String pushJobId, int versionNumber, int numberOfPartitions,
        Version.PushType pushType) {
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
                getReplicationFactor(clusterName, storeName), true, false, false, true, pushType);
            if (store.isMigrating()) {
                try {
                    StoreConfig storeConfig = storeConfigRepo.getStoreConfig(storeName).get();
                    String destinationCluster = storeConfig.getMigrationDestCluster();
                    String sourceCluster = storeConfig.getMigrationSrcCluster();
                    if (storeConfig.getCluster().equals(destinationCluster)) {
                        // Migration has completed in this colo but the overall migration is still in progress.
                        if (clusterName.equals(destinationCluster)) {
                            // Mirror new pushes back to the source cluster in case we abort migration after completion.
                            ControllerClient sourceClusterControllerClient =
                                new ControllerClient(sourceCluster, getMasterController(sourceCluster).getUrl(false), sslFactory);
                            VersionResponse response = sourceClusterControllerClient.addVersionAndStartIngestion(storeName,
                                pushJobId, versionNumber, numberOfPartitions, pushType);
                            if (response.isError()) {
                                logger.warn("Replicate add version endpoint call to source cluster: " + sourceCluster
                                    + " failed for store " + storeName + " and version " + versionNumber + " Error: "
                                    + response.getError());
                            }
                        }
                    } else if (clusterName.equals(sourceCluster)) {
                        ControllerClient destClusterControllerClient =
                            new ControllerClient(destinationCluster, getMasterController(destinationCluster).getUrl(false), sslFactory);
                        VersionResponse response = destClusterControllerClient.addVersionAndStartIngestion(storeName,
                            pushJobId, versionNumber, numberOfPartitions, pushType);
                        if (response.isError()) {
                            logger.warn("Replicate add version endpoint call to destination cluster: " + destinationCluster
                                + " failed for store " + storeName + " and version " + versionNumber + " Error: "
                                + response.getError());
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Exception thrown when replicating add version for store " + storeName + " and version "
                    + versionNumber + " as part of store migration", e);
                }
            }
        }
    }

    /**
     * A wrapper to invoke VeniceHelixAdmin#addVersion to only increment the version and create the topic(s) needed
     * without starting ingestion.
     */
    public Version addVersionOnly(String clusterName, String storeName, String pushJobId, int numberOfPartitions,
        int replicationFactor, boolean sendStartOfPush, boolean sorted, Version.PushType pushType) {
        Store store = getStore(clusterName, storeName);
        if (store == null) {
            throwStoreDoesNotExist(clusterName, storeName);
        }
        Optional<Version> existingVersionToUse = getVersionWithPushId(store, pushJobId);
        return existingVersionToUse.orElseGet(
            () -> addVersion(clusterName, storeName, pushJobId, VERSION_ID_UNSET, numberOfPartitions, replicationFactor,
                false, sendStartOfPush, sorted, false, pushType));
    }

    /**
     * Note, versionNumber may be VERSION_ID_UNSET, which must be accounted for.
     * Add version is a multi step process that can be broken down to three main steps:
     * 1. topic creation or verification, 2. version creation or addition, 3. start ingestion. The execution for some of
     * these steps are different depending on how it's invoked.
     */
    private Version addVersion(String clusterName, String storeName, String pushJobId, int versionNumber,
        int numberOfPartitions, int replicationFactor, boolean startIngestion, boolean sendStartOfPush,
        boolean sorted, boolean useFastKafkaOperationTimeout, Version.PushType pushType) {
        if (isClusterInMaintenanceMode(clusterName)) {
            throw new HelixClusterMaintenanceModeException(clusterName);
        }

        checkControllerMastership(clusterName);
        VeniceHelixResources resources = getVeniceHelixResource(clusterName);
        HelixReadWriteStoreRepository repository = resources.getMetadataRepository();
        Version version = null;
        OfflinePushStrategy strategy;
        boolean isLeaderFollowerStateModel = false;
        VeniceControllerClusterConfig clusterConfig = resources.getConfig();
        BackupStrategy backupStrategy;

        try {
            resources.lockForMetadataOperation();
            try {
                repository.lock();
                try {
                    Store store = repository.getStore(storeName);
                    if (store == null) {
                        throwStoreDoesNotExist(clusterName, storeName);
                    }
                    backupStrategy = store.getBackupStrategy();
                    if (versionNumber == VERSION_ID_UNSET) {
                        // No version supplied, generate a new version. This could happen either in the parent
                        // controller or local Samza jobs.
                        version = new Version(storeName, store.peekNextVersion().getNumber(), pushJobId, numberOfPartitions);
                    } else {
                        if (store.containsVersion(versionNumber)) {
                            throwVersionAlreadyExists(storeName, versionNumber);
                        }
                        version = new Version(storeName, versionNumber, pushJobId, numberOfPartitions);
                    }
                } finally {
                    repository.unLock();
                }
                // Topic created by Venice Controller is always without Kafka compaction.
                topicManager.createTopic(
                    version.kafkaTopicName(),
                    numberOfPartitions,
                    clusterConfig.getKafkaReplicationFactor(),
                    true,
                    false,
                    clusterConfig.getMinIsr(),
                    useFastKafkaOperationTimeout
                );
                if (pushType.isStreamReprocessing()) {
                    topicManager.createTopic(
                        Version.composeStreamReprocessingTopic(storeName, version.getNumber()),
                        numberOfPartitions,
                        clusterConfig.getKafkaReplicationFactor(),
                        true,
                        false,
                        clusterConfig.getMinIsr(),
                        useFastKafkaOperationTimeout
                    );
                }

                repository.lock();
                try {
                    Store store = repository.getStore(storeName);
                    strategy = store.getOffLinePushStrategy();
                    if (!store.containsVersion(version.getNumber())) {
                        version.setPushType(pushType);
                        store.addVersion(version);
                    }
                    if (version.isLeaderFollowerModelEnabled()) {
                        isLeaderFollowerStateModel = true;
                    }
                    // Disable buffer replay for hybrid according to cluster config
                    if (store.isHybrid() && clusterConfig.isSkipBufferRelayForHybrid()) {
                      store.setBufferReplayForHybridForVersion(version.getNumber(), false);
                      logger.info("Disabled buffer replay for store: " + storeName + " and version: " +
                          version.getNumber() + " in cluster: " + clusterName);
                    }
                    repository.updateStore(store);
                    logger.info("Add version: " + version.getNumber() + " for store: " + storeName);
                } finally {
                    repository.unLock();
                }

                if (sendStartOfPush) {
                    final Version finalVersion = version;
                    try (VeniceWriter veniceWriter = getVeniceWriterFactory().createVeniceWriter(
                        finalVersion.kafkaTopicName())) {
                        veniceWriter.broadcastStartOfPush(
                            sorted,
                            finalVersion.isChunkingEnabled(),
                            finalVersion.getCompressionStrategy(),
                            new HashMap<>());
                        if (pushType.isStreamReprocessing()) {
                            // Send TS message to version topic to inform leader to switch to the stream reprocessing topic
                            veniceWriter.broadcastTopicSwitch(
                                Arrays.asList(getKafkaBootstrapServers(isSslToKafka())),
                                Version.composeStreamReprocessingTopic(finalVersion.getStoreName(), finalVersion.getNumber()),
                                0L,
                                new HashMap<>());
                        }
                    }
                }

                if (startIngestion) {
                    // We need to prepare to monitor before creating helix resource.
                    startMonitorOfflinePush(
                        clusterName, version.kafkaTopicName(), numberOfPartitions, replicationFactor, strategy);
                    helixAdminClient.createVeniceStorageClusterResources(clusterName, version.kafkaTopicName(),
                        numberOfPartitions, replicationFactor, isLeaderFollowerStateModel);
                    waitUntilNodesAreAssignedForResource(clusterName, version.kafkaTopicName(), strategy,
                        clusterConfig.getOffLineJobWaitTimeInMilliseconds(), replicationFactor);

                    // Early delete backup version on start of a push, controlled by store config earlyDeleteBackupEnabled
                    if (backupStrategy == BackupStrategy.DELETE_ON_NEW_PUSH_START
                        && multiClusterConfigs.getConfigForCluster(clusterName).isEarlyDeleteBackUpEnabled()) {
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
            } finally {
                resources.unlockForMetadataOperation();
            }
            return version;

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
        }
    }

    protected void handleVersionCreationFailure(String clusterName, String storeName, int versionNumber, String statusDetails){
        // Mark offline push job as Error and clean up resources because add version failed.
        PushMonitor offlinePushMonitor = getVeniceHelixResource(clusterName).getPushMonitor();
        offlinePushMonitor.markOfflinePushAsError(Version.composeKafkaTopic(storeName, versionNumber), statusDetails);
        deleteOneStoreVersion(clusterName, storeName, versionNumber);
    }

    /**
     * Note: this currently use the pushID to guarantee idempotence, unexpected behavior may result if multiple
     * batch jobs push to the same store at the same time.
     */
    @Override
    public synchronized Version incrementVersionIdempotent(String clusterName, String storeName, String pushJobId,
        int numberOfPartitions, int replicationFactor, Version.PushType pushType, boolean sendStartOfPush, boolean sorted) {
        checkControllerMastership(clusterName);
        Store store = getStore(clusterName, storeName);
        if (store != null) {
            Optional<Version> existingVersionWithSamePushId = getVersionWithPushId(store, pushJobId);
            if (existingVersionWithSamePushId.isPresent()) {
                return existingVersionWithSamePushId.get();
            }
        } else {
            throwStoreDoesNotExist(clusterName, storeName);
        }

        return pushType.isIncremental() ? getIncrementalPushVersion(clusterName, storeName)
            : addVersion(clusterName, storeName, pushJobId, VERSION_ID_UNSET, numberOfPartitions, replicationFactor,
                true, sendStartOfPush, sorted, false, pushType);
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

    /**
     *
     * @param store
     * @param pushId
     * @return
     */
    protected static Optional<Version> getVersionWithPushId(Store store, String pushId){
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
    public synchronized String getRealTimeTopic(String clusterName, String storeName){
        checkControllerMastership(clusterName);
        Set<String> currentTopics = topicManager.listTopics();
        String realTimeTopic = Version.composeRealTimeTopic(storeName);
        if (currentTopics.contains(realTimeTopic)){
            return realTimeTopic;
        } else {
            HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
            repository.lock();
            try {
                Store store = repository.getStore(storeName);
                if (store == null) {
                    throwStoreDoesNotExist(clusterName, storeName);
                }
                if (!store.isHybrid()){
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
                VeniceControllerClusterConfig clusterConfig = getVeniceHelixResource(clusterName).getConfig();

                checkControllerMastership(clusterName);
                topicManager.createTopic(
                    realTimeTopic,
                    partitionCount,
                    clusterConfig.getKafkaReplicationFactor(),
                    store.getHybridStoreConfig().getRetentionTimeInMs(),
                    false, // RT topics are never compacted
                    clusterConfig.getMinIsr(),
                    false
                );
                //TODO: if there is an online version from a batch push before this store was hybrid then we won't start
                // replicating to it.  A new version must be created.
                logger.warn("Creating real time topic per topic request for store " + storeName + ".  "
                  + "Buffer replay wont start for any existing versions");
            } finally {
                repository.unLock();
            }
            return realTimeTopic;
        }
    }

    @Override
    public synchronized Version getIncrementalPushVersion(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        repository.lock();
        try {
            Store store = repository.getStore(storeName);
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
            String versionTopic = Version.composeKafkaTopic(storeName, version.getNumber());
            if (!topicManager.containsTopic(versionTopic) || isTopicTruncated(versionTopic)) {
                veniceHelixAdminStats.recordUnexpectedTopicAbsenceCount();
                throw new VeniceException("Incremental push cannot be started for store: " + storeName + " in cluster: "
                    + clusterName + " because the version topic: " + versionTopic
                    + " is either absent or being truncated");
            }
            return version;
        } finally {
            repository.unLock();
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
    public Map<String, Integer> getCurrentVersionsForMultiColos(String clusterName, String storeName) {
        return null;
    }

    @Override
    public Version peekNextVersion(String clusterName, String storeName) {
        Store store = getStoreForReadOnly(clusterName, storeName);
        Version version = store.peekNextVersion(); /* Does not modify the store */
        logger.info("Next version would be: " + version.getNumber() + " for store: " + storeName);
        return version;
    }

    @Override
    public synchronized List<Version> deleteAllVersionsInStore(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        VeniceHelixResources resources = getVeniceHelixResource(clusterName);
        resources.lockForMetadataOperation();
        try {

            HelixReadWriteStoreRepository repository = resources.getMetadataRepository();
            List<Version> deletingVersionSnapshot = new ArrayList<>();
            repository.lock();
            try {
                Store store = repository.getStore(storeName);
                checkPreConditionForDeletion(clusterName, storeName, store);
                logger.info("Deleting all versions in store: " + store + " in cluster: " + clusterName);
                // Set current version to NON_VERSION_AVAILABLE. Otherwise after this store is enabled again, as all of
                // version were deleted, router will get a current version which does not exist actually.
                store.setEnableWrites(true);
                store.setCurrentVersion(Store.NON_EXISTING_VERSION);
                store.setEnableWrites(false);
                repository.updateStore(store);
                deletingVersionSnapshot = new ArrayList<>(store.getVersions());
            } finally {
                repository.unLock();
            }
            // Do not lock the entire deleting block, because during the deleting, controller would acquire repository lock
            // to query store when received the status update from storage node.
            for (Version version : deletingVersionSnapshot) {
                deleteOneStoreVersion(clusterName, version.getStoreName(), version.getNumber());
            }
            logger.info("Deleted all versions in store: " + storeName + " in cluster: " + clusterName);
            return deletingVersionSnapshot;
        }finally {
            resources.unlockForMetadataOperation();
        }
    }

    @Override
    public void deleteOldVersionInStore(String clusterName, String storeName, int versionNum) {
        checkControllerMastership(clusterName);
        VeniceHelixResources resources = getVeniceHelixResource(clusterName);
        resources.lockForMetadataOperation();
        try {
            HelixReadWriteStoreRepository repository = resources.getMetadataRepository();
            Store store = repository.getStore(storeName);
            // Here we do not require the store be disabled. So it might impact reads
            // The thing is a new version is just online, now we delete the old version. So some of routers
            // might still use the old one as the current version, so when they send the request to that version,
            // they will get error response.
            // TODO the way to solve this could be: Introduce a timestamp to represent when the version is online.
            // TOOD And only allow to delete the old version that the newer version has been online for a while.
            checkPreConditionForSingleVersionDeletion(clusterName, storeName, store, versionNum);
            if (!store.containsVersion(versionNum)) {
                logger.warn(
                    "Ignore the deletion request. Could not find version: " + versionNum + " in store: " + storeName + " in cluster: " + clusterName);
                return;
            }
            logger.info("Deleting version: " + versionNum + " in store: " + storeName + " in cluster: " + clusterName);
            deleteOneStoreVersion(clusterName, storeName, versionNum);
            logger.info("Deleted version: " + versionNum + " in store: " + storeName + " in cluster: " + clusterName);
        }finally {
            resources.unlockForMetadataOperation();
        }
    }

    /**
     * Delete version from cluster, removing all related resources
     */
    @Override
    public void deleteOneStoreVersion(String clusterName, String storeName, int versionNumber) {
        VeniceHelixResources resources = getVeniceHelixResource(clusterName);
        resources.lockForMetadataOperation();
        try {
            String resourceName = Version.composeKafkaTopic(storeName, versionNumber);
            logger.info("Deleting helix resource:" + resourceName + " in cluster:" + clusterName);
            deleteHelixResource(clusterName, resourceName);
            logger.info("Killing offline push for:" + resourceName + " in cluster:" + clusterName);
            killOfflinePush(clusterName, resourceName);

            Store store = getVeniceHelixResource(clusterName).getMetadataRepository().getStore(storeName);
            if (!store.isLeaderFollowerModelEnabled() && onlineOfflineTopicReplicator.isPresent()) {
                // Do not delete topic replicator during store migration
                // In such case, the topic replicator will be deleted after store migration, triggered by a new push job
                if (!store.isMigrating()) {
                    String realTimeTopic = Version.composeRealTimeTopic(storeName);
                    onlineOfflineTopicReplicator.get().terminateReplication(realTimeTopic, resourceName);
                }
            }

            stopMonitorOfflinePush(clusterName, resourceName);
            Optional<Version> deletedVersion = deleteVersionFromStoreRepository(clusterName, storeName, versionNumber);
            if (deletedVersion.isPresent()) {
                // Do not delete topic during store migration
                // In such case, the topic will be deleted after store migration, triggered by a new push job
                if (!store.isMigrating()) {
                    truncateKafkaTopic(deletedVersion.get().kafkaTopicName());
                    if (deletedVersion.get().getPushType().isStreamReprocessing()) {
                        truncateKafkaTopic(Version.composeStreamReprocessingTopic(storeName, versionNumber));
                    }
                }
            }
        } finally {
            resources.unlockForMetadataOperation();
        }
    }

    @Override
    public void retireOldStoreVersions(String clusterName, String storeName, boolean deleteBackupOnStartPush) {
        VeniceHelixResources resources = getVeniceHelixResource(clusterName);
        resources.lockForMetadataOperation();
        try {
            HelixReadWriteStoreRepository storeRepository = resources.getMetadataRepository();
            Store store = storeRepository.getStore(storeName);

            //  if deleteBackupOnStartPush is true decrement minNumberOfStoreVersionsToPreserve by one
            // as newly started push is considered as another version. the code in retrieveVersionsToDelete
            // will not return any store if we pass minNumberOfStoreVersionsToPreserve during push
            int numVersionToPreserve = minNumberOfStoreVersionsToPreserve - (deleteBackupOnStartPush ? 1 : 0);
            List<Version> versionsToDelete = store.retrieveVersionsToDelete(numVersionToPreserve);
            if (versionsToDelete.size() == 0) {
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
        } finally {
            resources.unlockForMetadataOperation();
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
        VeniceHelixResources resources = getVeniceHelixResource(clusterName);
        VeniceControllerClusterConfig clusterConfig = resources.getConfig();
        HelixReadWriteStoreRepository storeRepository = resources.getMetadataRepository();
        Store store = storeRepository.getStore(storeName);
        if ((store.isHybrid() && clusterConfig.isKafkaLogCompactionForHybridStoresEnabled())
            || (store.isIncrementalPushEnabled() && clusterConfig.isKafkaLogCompactionForIncrementalPushStoresEnabled())) {
            topicManager.updateTopicCompactionPolicy(Version.composeKafkaTopic(storeName, versionNumber), true);
        }
    }

    /**
     * Delete the version specified from the store and return the deleted version.
     */
    protected Optional<Version> deleteVersionFromStoreRepository(String clusterName, String storeName, int versionNumber) {
        HelixReadWriteStoreRepository storeRepository = getVeniceHelixResource(clusterName).getMetadataRepository();
        logger.info("Deleting version " + versionNumber + " in Store: " + storeName + " in cluster: " + clusterName);
        Version deletedVersion = null;
        storeRepository.lock();
        try {
            Store store = storeRepository.getStore(storeName);
            if (store == null) {
                throw new VeniceNoStoreException(storeName);
            }
            deletedVersion = store.deleteVersion(versionNumber);
            if (deletedVersion == null) {
                logger.warn("Can not find version: " + versionNumber + " in store: " + storeName + ".  It has probably already been deleted");
            }
            storeRepository.updateStore(store);
        } finally {
            storeRepository.unLock();
        }
        logger.info("Deleted version " + versionNumber + " in Store: " + storeName + " in cluster: " + clusterName);
        if (null == deletedVersion) {
            return Optional.empty();
        } else {
            return Optional.of(deletedVersion);
        }
    }

    @Override
    public boolean isTopicTruncated(String kafkaTopicName) {
        return getTopicManager().isTopicTruncated(kafkaTopicName, deprecatedJobTopicMaxRetentionMs);
    }

    @Override
    public boolean isTopicTruncatedBasedOnRetention(long retention) {
        return getTopicManager().isRetentionBelowTruncatedThreshold(retention, deprecatedJobTopicMaxRetentionMs);
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
        if (getTopicManager().containsTopicInKafkaZK(kafkaTopicName)) {
            if (getTopicManager().updateTopicRetention(kafkaTopicName, deprecatedJobTopicRetentionMs)) {
                // Retention time is updated to "deprecatedJobTopicRetentionMs"; log this topic config changes
                logger.info("Updated topic: " + kafkaTopicName + " with retention.ms: " + deprecatedJobTopicRetentionMs);
                return true;
            } // otherwise, the retention time config for this topic has already been updated before
        } else {
            logger.info("Topic: " + kafkaTopicName + " doesn't exist in Kafka Zookeeper, will skip the truncation");
        }
        // return false to indicate the retention config has already been updated before
        return false;
    }

    private boolean truncateKafkaTopic(String kafkaTopicName, scala.collection.Map<String, Properties> topicConfigs) {
        if (topicConfigs.contains(kafkaTopicName)) {
            if (getTopicManager().updateTopicRetention(kafkaTopicName, deprecatedJobTopicRetentionMs,
                topicConfigs.get(kafkaTopicName).get())) {
                // retention is updated.
                logger.info("Updated topic: " + kafkaTopicName + " with retention.ms: " + deprecatedJobTopicRetentionMs);
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

        Set<String> allTopics = topicManager.listTopics();
        List<String> allTopicsRelatedToThisStore = allTopics.stream()
            /** Exclude RT buffer topics, admin topics and all other special topics */
            .filter(t -> Version.topicIsValidStoreVersion(t))
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
            scala.collection.Map<String, Properties> topicConfigs = getTopicManager().getAllTopicConfig();
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
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        repository.lock();
        try {
            Store store = repository.getStore(storeName);
            if(store == null){
                throw new VeniceNoStoreException(storeName);
            }
            return store; /* is a clone */
        } finally {
            repository.unLock();
        }
    }

    @Override
    public List<Version> versionsForStore(String clusterName, String storeName){
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        List<Version> versions;
        repository.lock();
        try {
            Store store = repository.getStore(storeName);
            if(store == null){
                throw new VeniceNoStoreException(storeName);
            }
            versions = store.getVersions();
        } finally {
            repository.unLock();
        }
        return versions;
    }

    @Override
    public List<Store> getAllStores(String clusterName){
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        repository.lock();
        try {
            return repository.getAllStores();
        } finally {
            repository.unLock();
        }
    }

    @Override
    public Map<String, String> getAllStoreStatuses(String clusterName) {
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        repository.lock();
        try {
            List<Store> storeList = repository.getAllStores();
            RoutingDataRepository routingDataRepository =
                getVeniceHelixResource(clusterName).getRoutingDataRepository();
            ResourceAssignment resourceAssignment = routingDataRepository.getResourceAssignment();
            return StoreStatusDecider.getStoreStatues(storeList, resourceAssignment,
                getVeniceHelixResource(clusterName).getPushMonitor(),
                getVeniceHelixResource(clusterName).getConfig());
        } finally {
            repository.unLock();
        }
    }

    @Override
    public boolean hasStore(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        repository.lock();
        try {
            return repository.hasStore(storeName);
        } finally {
            repository.unLock();
        }
    }

    @Override
    public Store getStore(String clusterName, String storeName){
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        return repository.getStore(storeName);
    }

    @Override
    public synchronized void setStoreCurrentVersion(String clusterName, String storeName, int versionNumber){
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
    public synchronized void setStoreLargestUsedVersion(String clusterName, String storeName, int versionNumber) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setLargestUsedVersionNumber(versionNumber);
            return store;
        });
    }

    @Override
    public synchronized void setStoreOwner(String clusterName, String storeName, String owner) {
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
    public synchronized void setStorePartitionCount(String clusterName, String storeName, int partitionCount) {
        VeniceControllerClusterConfig clusterConfig = getVeniceHelixResource(clusterName).getConfig();
        storeMetadataUpdate(clusterName, storeName, store -> {
            if (store.getPartitionCount() != partitionCount && store.isHybrid()) {
                throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, "Cannot change partition count for a hybrid store");
            } else {
                int desiredPartitionCount = partitionCount;

                if (desiredPartitionCount > clusterConfig.getMaxNumberOfPartition()) {
                    desiredPartitionCount = clusterConfig.getMaxNumberOfPartition();
                } else if (desiredPartitionCount < clusterConfig.getNumberOfPartition()) {
                    desiredPartitionCount = clusterConfig.getNumberOfPartition();
                }
                // Do not update the partitionCount on the store.version as version config is immutable. The version.getPartitionCount()
                // is read only in getRealTimeTopic and createInternalStore creation, so modifying currentVersion should not have any effect.
                store.setPartitionCount(desiredPartitionCount);
                return store;
            }
        });
    }

    public synchronized void setStorePartitionerConfig(String clusterName, String storeName, PartitionerConfig partitionerConfig) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            // Cannot change the partitioner config if store is a hybrid store.
            if (!store.getPartitionerConfig().equals(partitionerConfig) && store.isHybrid()) {
                throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, "Partitioner config change in hybrid store is not supported");
            } else {
                store.setPartitionerConfig(partitionerConfig);
                return store;
            }
        });
    }

    @Override
    public synchronized void setStoreWriteability(String clusterName, String storeName, boolean desiredWriteability) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setEnableWrites(desiredWriteability);

            return store;
        });
    }

    @Override
    public synchronized void setStoreReadability(String clusterName, String storeName, boolean desiredReadability) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setEnableReads(desiredReadability);

            return store;
        });
    }

    @Override
    public synchronized void setStoreReadWriteability(String clusterName, String storeName, boolean isAccessible) {
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
    private synchronized void setStoreStorageQuota(String clusterName, String storeName, long storageQuotaInByte) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            if (storageQuotaInByte < 0 && storageQuotaInByte != Store.UNLIMITED_STORAGE_QUOTA) {
                throw new VeniceException("storage quota can not be less than 0");
            }
            store.setStorageQuotaInByte(storageQuotaInByte);

            return store;
        });
    }

    private synchronized void setStoreReadQuota(String clusterName, String storeName, long readQuotaInCU) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            if (readQuotaInCU < 0) {
                throw new VeniceException("read quota can not be less than 0");
            }
            store.setReadQuotaInCU(readQuotaInCU);

            return store;
        });
    }

    public synchronized void setAccessControl(String clusterName, String storeName, boolean accessControlled) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setAccessControlled(accessControlled);

            return store;
        });
    }

    public synchronized void setStoreCompressionStrategy(String clusterName, String storeName,
                                                          CompressionStrategy compressionStrategy) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setCompressionStrategy(compressionStrategy);

            return store;
        });
    }

    public synchronized void setClientDecompressionEnabled(String clusterName, String storeName, boolean clientDecompressionEnabled) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setClientDecompressionEnabled(clientDecompressionEnabled);
            return store;
        });
    }

    public synchronized void setChunkingEnabled(String clusterName, String storeName,
        boolean chunkingEnabled) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setChunkingEnabled(chunkingEnabled);

            return store;
        });
    }

    public synchronized void setIncrementalPushEnabled(String clusterName, String storeName,
        boolean incrementalPushEnabled) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            if (incrementalPushEnabled && store.isHybrid()) {
                throw new VeniceException("hybrid store doesn't support incremental push");
            }
            store.setIncrementalPushEnabled(incrementalPushEnabled);

            return  store;
        });
    }

    public synchronized  void setSingleGetRouterCacheEnabled(String clusterName, String storeName,
        boolean singleGetRouterCacheEnabled) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setSingleGetRouterCacheEnabled(singleGetRouterCacheEnabled);

            return store;
        });
    }

    public synchronized void setBatchGetRouterCacheEnabled(String clusterName, String storeName,
        boolean batchGetRouterCacheEnabled) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setBatchGetRouterCacheEnabled(batchGetRouterCacheEnabled);
            return store;
        });
    }

    public synchronized  void setBatchGetLimit(String clusterName, String storeName,
        int batchGetLimit) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setBatchGetLimit(batchGetLimit);

            return store;
        });
    }

    public synchronized  void setNumVersionsToPreserve(String clusterName, String storeName,
        int numVersionsToPreserve) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setNumVersionsToPreserve(numVersionsToPreserve);

            return store;
        });
    }

    public synchronized void setStoreMigration(String clusterName, String storeName, boolean migrating) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setMigrating(migrating);
            return store;
        });
    }

    public synchronized void setWriteComputationEnabled(String clusterName, String storeName,
        boolean writeComputationEnabled) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setWriteComputationEnabled(writeComputationEnabled);
            return store;
        });
    }

    public synchronized void setReadComputationEnabled(String clusterName, String storeName,
        boolean computationEnabled) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setReadComputationEnabled(computationEnabled);
            return store;
        });
    }

    public synchronized void setBootstrapToOnlineTimeoutInHours(String clusterName, String storeName,
        int bootstrapToOnlineTimeoutInHours) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setBootstrapToOnlineTimeoutInHours(bootstrapToOnlineTimeoutInHours);
            return store;
        });
    }

    public synchronized void setLeaderFollowerModelEnabled(String clusterName, String storeName,
        boolean leaderFollowerModelEnabled) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            store.setLeaderFollowerModelEnabled(leaderFollowerModelEnabled);
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

    public void setSuperSetSchemaAutoGenerationForReadComputeEnabled(String clusterName, String storeName,
        boolean superSetSchemaAutoGenerationForReadComputeEnabled) {
        storeMetadataUpdate(clusterName, storeName, store -> {
            if (store.isReadComputationEnabled()) {
                store.setSuperSetSchemaAutoGenerationForReadComputeEnabled(superSetSchemaAutoGenerationForReadComputeEnabled);
            }
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

    /**
     * This function will check whether the store update will cause the case that a hybrid or incremental push store will have router-cache enabled
     * or a compressed store will have router-cache enabled.
     *
     * For now, router cache shouldn't be enabled for a hybrid store.
     * TODO: need to remove this check when the proper fix (cross-colo race condition) is implemented.
     *
     * Right now, this function doesn't check whether the hybrid/incremental push config and router-cache flag will be updated at the same time:
     * 1. Current store originally is a hybrid/incremental push store;
     * 2. The update operation will turn this store to be a batch-only store, and enable router-cache at the same time;
     * The reason not to check the above scenario is that the hybrid config/router-cache update is not atomic, so admin should
     * update the store to be batch-only store first and turn on the router-cache feature after.
     *
     * BTW, it seems no way to update a hybrid store to be a batch-only store.
     *
     * @param store
     * @param newIncrementalPushConfig
     * @param newHybridStoreConfig
     * @param newSingleGetRouterCacheEnabled
     * @param newBatchGetRouterCacheEnabled
     */
    protected void checkWhetherStoreWillHaveConflictConfigForCaching(Store store,
        Optional<Boolean> newIncrementalPushConfig,
        Optional<HybridStoreConfig> newHybridStoreConfig,
        Optional<Boolean> newSingleGetRouterCacheEnabled,
        Optional<Boolean> newBatchGetRouterCacheEnabled) {
        String storeName = store.getName();
        if ((store.isHybrid() || store.isIncrementalPushEnabled()) && (newSingleGetRouterCacheEnabled.orElse(false) || newBatchGetRouterCacheEnabled.orElse(false))) {
            throw new VeniceException("Router cache couldn't be enabled for store: " + storeName + " since it is a hybrid/incremental push store");
        }
        if ((store.isSingleGetRouterCacheEnabled() || store.isBatchGetRouterCacheEnabled()) && (newHybridStoreConfig.isPresent() || newIncrementalPushConfig.orElse(false))) {
            throw new VeniceException("Hybrid/incremental push couldn't be enabled for store: " + storeName + " since it enables router-cache");
        }
    }

    /**
     * TODO: some logics are in parent controller {@link VeniceParentHelixAdmin} #updateStore and
     *       some are in the child controller here. Need to unify them in the future.
     *       If updateStore is triggered only in child controller, hybridStoreDbOverheadBypass would be ignored.
     */
    @Override
    public synchronized void updateStore(
        String clusterName,
        String storeName,
        Optional<String> owner,
        Optional<Boolean> readability,
        Optional<Boolean> writeability,
        Optional<Integer> partitionCount,
        Optional<String> partitionerClass,
        Optional<Map<String, String>> partitionerParams,
        Optional<Integer> amplificationFactor,
        Optional<Long> storageQuotaInByte,
        Optional<Boolean> hybridStoreDbOverheadBypass,
        Optional<Long> readQuotaInCU,
        Optional<Integer> currentVersion,
        Optional<Integer> largestUsedVersionNumber,
        Optional<Long> hybridRewindSeconds,
        Optional<Long> hybridOffsetLagThreshold,
        Optional<Boolean> accessControlled,
        Optional<CompressionStrategy> compressionStrategy,
        Optional<Boolean> clientDecompressionEnabled,
        Optional<Boolean> chunkingEnabled,
        Optional<Boolean> singleGetRouterCacheEnabled,
        Optional<Boolean> batchGetRouterCacheEnabled,
        Optional<Integer> batchGetLimit,
        Optional<Integer> numVersionsToPreserve,
        Optional<Boolean> incrementalPushEnabled,
        Optional<Boolean> storeMigration,
        Optional<Boolean> writeComputationEnabled,
        Optional<Boolean> readComputationEnabled,
        Optional<Integer> bootstrapToOnlineTimeoutInHours,
        Optional<Boolean> leaderFollowerModelEnabled,
        Optional<BackupStrategy> backupStrategy,
        Optional<Boolean> autoSchemaRegisterPushJobEnabled,
        Optional<Boolean> superSetSchemaAutoGenerationForReadComputeEnabled,
        Optional<Boolean> hybridStoreDiskQuotaEnabled,
        Optional<Boolean> regularVersionETLEnabled,
        Optional<Boolean> futureVersionETLEnabled,
        Optional<String> etledUserProxyAccount) {
        Store originalStoreToBeCloned = getStore(clusterName, storeName);
        if (null == originalStoreToBeCloned) {
            throw new VeniceException("The store '" + storeName + "' in cluster '" + clusterName + "' does not exist, and thus cannot be updated.");
        }
        Store originalStore = originalStoreToBeCloned.cloneStore();

        if (originalStore.isMigrating()) {
            if (!(storeMigration.isPresent() || readability.isPresent() || writeability.isPresent())) {
                String errMsg = "This update operation is not allowed during store migration!";
                logger.warn(errMsg + " Store name: " + storeName);
                throw new VeniceException(errMsg);
            }
        }

        Optional<HybridStoreConfig> hybridStoreConfig = Optional.empty();
        if (hybridRewindSeconds.isPresent() || hybridOffsetLagThreshold.isPresent()) {
            HybridStoreConfig hybridConfig = mergeNewSettingsIntoOldHybridStoreConfig(
                originalStore, hybridRewindSeconds, hybridOffsetLagThreshold);
            if (null != hybridConfig) {
                hybridStoreConfig = Optional.of(hybridConfig);
            }
        }

        checkWhetherStoreWillHaveConflictConfigForCaching(originalStore, incrementalPushEnabled,hybridStoreConfig, singleGetRouterCacheEnabled, batchGetRouterCacheEnabled);

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

            /**
             * If either of these three fields is not present, we should use default value to construct correct PartitionerConfig.
             */
            if (partitionerClass.isPresent() || partitionerParams.isPresent() || amplificationFactor.isPresent()) {
                PartitionerConfig partitionerConfig = new PartitionerConfig();
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

            if (hybridStoreConfig.isPresent()) {
                // To fix the final variable problem in the lambda expression
                final HybridStoreConfig finalHybridConfig = hybridStoreConfig.get();
                storeMetadataUpdate(clusterName, storeName, store -> {
                    if (store.isIncrementalPushEnabled()) {
                        throw new VeniceException("incremental push store could not support hybrid");
                    }
                    if (finalHybridConfig.getRewindTimeInSeconds() < 0 || finalHybridConfig.getOffsetLagThresholdToGoOnline() < 0) {
                        /**
                         * If one of the config values is negative, it indicates that the store is being set back to batch-only store.
                         */
                        store.setHybridStoreConfig(null);
                        String realTimeTopic = Version.composeRealTimeTopic(storeName);
                        truncateKafkaTopic(realTimeTopic);
                        // Also remove the Brooklin replication streams
                        if (!store.isLeaderFollowerModelEnabled() && onlineOfflineTopicReplicator.isPresent()) {
                            store.getVersions().stream().forEach(version ->
                                onlineOfflineTopicReplicator.get().terminateReplication(realTimeTopic, version.kafkaTopicName()));
                        }
                    } else {
                        store.setHybridStoreConfig(finalHybridConfig);
                        if (topicManager.containsTopic(Version.composeRealTimeTopic(storeName))) {
                            // RT already exists, ensure the retention is correct
                            topicManager.updateTopicRetention(Version.composeRealTimeTopic(storeName),
                                finalHybridConfig.getRetentionTimeInMs());
                        }
                    }
                    return store;
                });
            }

            if (singleGetRouterCacheEnabled.isPresent()) {
                setSingleGetRouterCacheEnabled(clusterName, storeName, singleGetRouterCacheEnabled.get());
            }

            if (batchGetRouterCacheEnabled.isPresent()) {
              setBatchGetRouterCacheEnabled(clusterName, storeName, batchGetRouterCacheEnabled.get());
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

            if (incrementalPushEnabled.isPresent()) {
                setIncrementalPushEnabled(clusterName, storeName, incrementalPushEnabled.get());
            }

            if (storeMigration.isPresent()) {
                setStoreMigration(clusterName, storeName, storeMigration.get());
            }

            if (writeComputationEnabled.isPresent()) {
                setWriteComputationEnabled(clusterName, storeName, writeComputationEnabled.get());
            }

            if (readComputationEnabled.isPresent()) {
                setReadComputationEnabled(clusterName, storeName, readComputationEnabled.get());
            }

            if (bootstrapToOnlineTimeoutInHours.isPresent()) {
                setBootstrapToOnlineTimeoutInHours(clusterName, storeName, bootstrapToOnlineTimeoutInHours.get());
            }

            if (leaderFollowerModelEnabled.isPresent()) {
                setLeaderFollowerModelEnabled(clusterName, storeName, leaderFollowerModelEnabled.get());
            }

            if (backupStrategy.isPresent()) {
                setBackupStrategy(clusterName, storeName, backupStrategy.get());
            }

            autoSchemaRegisterPushJobEnabled.ifPresent(value ->
                setAutoSchemaRegisterPushJobEnabled(clusterName, storeName, value));
            superSetSchemaAutoGenerationForReadComputeEnabled.ifPresent(value ->
                setSuperSetSchemaAutoGenerationForReadComputeEnabled(clusterName, storeName, value));
            hybridStoreDiskQuotaEnabled.ifPresent(value ->
                setHybridStoreDiskQuotaEnabled(clusterName, storeName, value));

            if (hybridStoreDbOverheadBypass.isPresent()) {
                logger.warn("If updateStore is triggered only in child controller, "
                    + "hybridStoreDbOverheadBypass would be ignored.");
            }

            if (regularVersionETLEnabled.isPresent() || futureVersionETLEnabled.isPresent() || etledUserProxyAccount.isPresent()) {
                ETLStoreConfig etlStoreConfig = new ETLStoreConfig(etledUserProxyAccount.get(), regularVersionETLEnabled.get(), futureVersionETLEnabled.get());
                storeMetadataUpdate(clusterName, storeName, store -> {
                    store.setEtlStoreConfig(etlStoreConfig);
                    return store;
                });
            }

            logger.info("Finished updating store: " + storeName + " in cluster: " + clusterName);
        } catch (VeniceException e) {
            logger.error("Caught exception during update to store '" + storeName + "' in cluster: '" + clusterName
                + "'. Will attempt to rollback changes.", e);
            //rollback to original store
            storeMetadataUpdate(clusterName, storeName, store -> originalStore);
            if (originalStore.isHybrid() && hybridStoreConfig.isPresent()
                && topicManager.containsTopic(Version.composeRealTimeTopic(storeName))) {
                // Ensure the topic retention is rolled back too
                topicManager.updateTopicRetention(Version.composeRealTimeTopic(storeName),
                    originalStore.getHybridStoreConfig().getRetentionTimeInMs());
            }
            logger.error("Successfully rolled back changes to store '" + storeName + "' in cluster: '" + clusterName
                + "'. Will now throw the original exception (" + e.getClass().getSimpleName() + ").");
            throw e;
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
            Optional<Long> hybridRewindSeconds, Optional<Long> hybridOffsetLagThreshold){
        if (!hybridRewindSeconds.isPresent() && !hybridOffsetLagThreshold.isPresent() && !oldStore.isHybrid()){
            return null; //For the nullable union in the avro record
        }
        HybridStoreConfig hybridConfig;
        if (oldStore.isHybrid()){ // for an existing hybrid store, just replace any specified values
            HybridStoreConfig oldHybridConfig = oldStore.getHybridStoreConfig().clone();
            hybridConfig = new HybridStoreConfig(
                hybridRewindSeconds.isPresent()
                    ? hybridRewindSeconds.get()
                    : oldHybridConfig.getRewindTimeInSeconds(),
                hybridOffsetLagThreshold.isPresent()
                    ? hybridOffsetLagThreshold.get()
                    : oldHybridConfig.getOffsetLagThresholdToGoOnline()
            );
        } else { // switching a non-hybrid store to hybrid; must specify every value
            if (!(hybridRewindSeconds.isPresent() && hybridOffsetLagThreshold.isPresent())) {
                throw new VeniceException(oldStore.getName() + " was not a hybrid store.  In order to make it a hybrid store both "
                    + " rewind time in seconds and offset lag threshold must be specified");
            }
            hybridConfig = new HybridStoreConfig(
                hybridRewindSeconds.get(),
                hybridOffsetLagThreshold.get()
            );
        }
        return hybridConfig;
    }

    public void storeMetadataUpdate(String clusterName, String storeName, StoreMetadataOperation operation) {
        checkPreConditionForUpdateStore(clusterName, storeName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        repository.lock();
        try {
            Store store = repository.getStore(storeName);
            repository.updateStore(operation.update(store));
        } catch (Exception e) {
            logger.error("Failed to execute StoreMetadataOperation.", e);
            throw e;
        } finally {
            repository.unLock();
        }
    }

    protected void checkPreConditionForUpdateStore(String clusterName, String storeName){
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        if (repository.getStore(storeName) == null) {
            throwStoreDoesNotExist(clusterName, storeName);
        }
    }

    @Override
    public double getStorageEngineOverheadRatio(String clusterName) {
        return multiClusterConfigs.getConfigForCluster(clusterName).getStorageEngineOverheadRatio();
    }

  private void waitUntilNodesAreAssignedForResource(
      String clusterName,
      String topic,
      OfflinePushStrategy strategy,
      long maxWaitTimeMs,
      int replicationFactor) {

    VeniceHelixResources clusterResources = getVeniceHelixResource(clusterName);
    PushMonitor pushMonitor = clusterResources.getPushMonitor();
    RoutingDataRepository routingDataRepository = clusterResources.getRoutingDataRepository();
    PushStatusDecider statusDecider = PushStatusDecider.getDecider(strategy);

    Optional<String> notReadyReason = Optional.of("unknown");
    long startTime = System.currentTimeMillis();
    for (long elapsedTime = 0; elapsedTime <= maxWaitTimeMs; elapsedTime = System.currentTimeMillis() - startTime) {
      if (pushMonitor.getOfflinePush(topic).getCurrentStatus().equals(ExecutionStatus.ERROR)) {
        throw new VeniceException("Push " + topic + " has already failed.");
      }

      ResourceAssignment resourceAssignment = routingDataRepository.getResourceAssignment();
      notReadyReason = statusDecider.hasEnoughNodesToStartPush(topic, replicationFactor, resourceAssignment);

      if (!notReadyReason.isPresent()) {
        logger.info("After waiting for " + elapsedTime + "ms, resource allocation is completed for " + topic + ".");
        pushMonitor.refreshAndUpdatePushStatus(topic, ExecutionStatus.STARTED, Optional.of("Helix assignment complete"));
        pushMonitor.recordPushPreparationDuration(topic, TimeUnit.MILLISECONDS.toSeconds(elapsedTime));
        return;
      }

      logger.info("After waiting for " + elapsedTime + "ms, resource " + topic + " does not have enough nodes" +
          ", strategy=" + strategy.toString() + ", replicationFactor=" + replicationFactor + ", reason=" + notReadyReason.get());
      pushMonitor.refreshAndUpdatePushStatus(topic, ExecutionStatus.STARTED, notReadyReason);
      Utils.sleep(WAIT_FOR_HELIX_RESOURCE_ASSIGNMENT_FINISH_RETRY_MS);
    }

    // Time out, after waiting maxWaitTimeMs, there are not enough nodes assigned.
    pushMonitor.recordPushPreparationDuration(topic, TimeUnit.MILLISECONDS.toSeconds(maxWaitTimeMs));
    throw new VeniceException("After waiting for " + maxWaitTimeMs + "ms, resource " + topic + " does not have enough nodes" +
        ", strategy=" + strategy.toString() + ", replicationFactor=" + replicationFactor + ", reason=" + notReadyReason.get());
  }

    protected void deleteHelixResource(String clusterName, String kafkaTopic) {
        checkControllerMastership(clusterName);
        helixAdminClient.dropResource(clusterName, kafkaTopic);
        logger.info("Successfully dropped the resource " + kafkaTopic + " for cluster " + clusterName);
    }

    @Override
    public SchemaEntry getKeySchema(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        HelixReadWriteSchemaRepository schemaRepo = getVeniceHelixResource(clusterName).getSchemaRepository();
        return schemaRepo.getKeySchema(storeName);
    }

    @Override
    public Collection<SchemaEntry> getValueSchemas(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        HelixReadWriteSchemaRepository schemaRepo = getVeniceHelixResource(clusterName).getSchemaRepository();
        return schemaRepo.getValueSchemas(storeName);
    }

    @Override
    public Collection<DerivedSchemaEntry> getDerivedSchemas(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        HelixReadWriteSchemaRepository schemaRepo = getVeniceHelixResource(clusterName).getSchemaRepository();
        return schemaRepo.getDerivedSchemas(storeName);
    }

    @Override
    public int getValueSchemaId(String clusterName, String storeName, String valueSchemaStr) {
        checkControllerMastership(clusterName);
        HelixReadWriteSchemaRepository schemaRepo = getVeniceHelixResource(clusterName).getSchemaRepository();
        return schemaRepo.getValueSchemaId(storeName, valueSchemaStr);
    }

    @Override
    public Pair<Integer, Integer> getDerivedSchemaId(String clusterName, String storeName, String schemaStr) {
        checkControllerMastership(clusterName);
        HelixReadWriteSchemaRepository schemaRepo = getVeniceHelixResource(clusterName).getSchemaRepository();
        return schemaRepo.getDerivedSchemaId(storeName, schemaStr);
    }

    @Override
    public SchemaEntry getValueSchema(String clusterName, String storeName, int id) {
        checkControllerMastership(clusterName);
        HelixReadWriteSchemaRepository schemaRepo = getVeniceHelixResource(clusterName).getSchemaRepository();
        return schemaRepo.getValueSchema(storeName, id);
    }

    @Override
    public SchemaEntry addValueSchema(String clusterName, String storeName, String valueSchemaStr,
        DirectionalSchemaCompatibilityType expectedCompatibilityType) {
        checkControllerMastership(clusterName);
        ReadWriteSchemaRepository schemaRepository = getVeniceHelixResource(clusterName).getSchemaRepository();
        SchemaEntry schemaEntry = schemaRepository.addValueSchema(storeName, valueSchemaStr, expectedCompatibilityType);

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
        HelixReadWriteSchemaRepository schemaRepository = getVeniceHelixResource(clusterName).getSchemaRepository();
        int newValueSchemaId = schemaRepository.preCheckValueSchemaAndGetNextAvailableId(storeName, valueSchemaStr,
            compatibilityType);
        if (newValueSchemaId != SchemaData.DUPLICATE_VALUE_SCHEMA_CODE && newValueSchemaId != schemaId) {
            throw new VeniceException("Inconsistent value schema id between the caller and the local schema repository."
                + " Expected new schema id of " + schemaId + " but the next available id from the local repository is "
                + newValueSchemaId + " for store " + storeName + " in cluster " + clusterName + " Schema: " + valueSchemaStr);
        }
        return schemaRepository.addValueSchema(storeName, valueSchemaStr, newValueSchemaId);
    }

    @Override
    public DerivedSchemaEntry addDerivedSchema(String clusterName, String storeName, int valueSchemaId, String derivedSchemaStr) {
        checkControllerMastership(clusterName);
        ReadWriteSchemaRepository schemaRepository = getVeniceHelixResource(clusterName).getSchemaRepository();
        schemaRepository.addDerivedSchema(storeName, derivedSchemaStr, valueSchemaId);

        return new DerivedSchemaEntry(valueSchemaId,
            schemaRepository.getDerivedSchemaId(storeName, derivedSchemaStr).getSecond(), derivedSchemaStr );
    }

    @Override
    public DerivedSchemaEntry addDerivedSchema(String clusterName, String storeName, int valueSchemaId,
        int derivedSchemaId, String derivedSchemaStr) {
        checkControllerMastership(clusterName);
        return getVeniceHelixResource(clusterName).getSchemaRepository()
            .addDerivedSchema(storeName, derivedSchemaStr, valueSchemaId, derivedSchemaId);
    }

    @Override
    public DerivedSchemaEntry removeDerivedSchema(String clusterName, String storeName, int valueSchemaId, int derivedSchemaId) {
        checkControllerMastership(clusterName);
        return getVeniceHelixResource(clusterName).getSchemaRepository()
            .removeDerivedSchema(storeName, valueSchemaId, derivedSchemaId);
    }

    @Override
    public SchemaEntry addSupersetSchema(String clusterName, String storeName, String valueSchema,
        int valueSchemaId, String supersetSchema, int supersetSchemaId) {
        checkControllerMastership(clusterName);
        Store store = getStore(clusterName, storeName);
        ReadWriteSchemaRepository schemaRepository = getVeniceHelixResource(clusterName).getSchemaRepository();
        HelixReadWriteStoreRepository storeRepository = getVeniceHelixResource(clusterName).getMetadataRepository();

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
        store.setLatestSuperSetValueSchemaId(supersetSchemaId);
        storeRepository.updateStore(store);

        // add the value schema
        return schemaRepository.addValueSchema(storeName, valueSchema, valueSchemaId);
    }

    public int getValueSchemaIdIgnoreFieldOrder(String clusterName, String storeName, String valueSchemaStr) {
        checkControllerMastership(clusterName);
        SchemaEntry valueSchemaEntry = new SchemaEntry(SchemaData.UNKNOWN_SCHEMA_ID, valueSchemaStr);
        HelixReadWriteSchemaRepository schemaRepository = getVeniceHelixResource(clusterName).getSchemaRepository();

        return schemaRepository.getValueSchemaIdIgnoreFieldOrder(storeName, valueSchemaEntry);
    }

    protected int checkPreConditionForAddValueSchemaAndGetNewSchemaId(String clusterName, String storeName,
        String valueSchemaStr, DirectionalSchemaCompatibilityType expectedCompatibilityType) {
        checkControllerMastership(clusterName);
        return getVeniceHelixResource(clusterName).getSchemaRepository()
            .preCheckValueSchemaAndGetNextAvailableId(storeName, valueSchemaStr, expectedCompatibilityType);
    }

    protected int checkPreConditionForAddDerivedSchemaAndGetNewSchemaId(String clusterName, String storeName,
        int valueSchemaId, String derivedSchemaStr) {
        checkControllerMastership(clusterName);
        return getVeniceHelixResource(clusterName).getSchemaRepository()
            .preCheckDerivedSchemaAndGetNextAvailableId(storeName, valueSchemaId, derivedSchemaStr);
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
        RoutingDataRepository routingDataRepository = getVeniceHelixResource(clusterName).getRoutingDataRepository();
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
        ReadWriteSchemaRepository schemaRepository = getVeniceHelixResource(clusterName).getSchemaRepository();
        // If already a superset schema exists, try to generate the new superset from that and the input value schema
        SchemaEntry existingSchema = schemaRepository.getLatestValueSchema(store.getName());

        return existingSchema == null ? null : existingSchema.getSchema();
    }

    @Override
    public void removeStorageNode(String clusterName, String instanceId) {
        checkControllerMastership(clusterName);
        logger.info("Removing storage node: " + instanceId + " from cluster: " + clusterName);
        RoutingDataRepository routingDataRepository = getVeniceHelixResource(clusterName).getRoutingDataRepository();
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
        partitionNames.add(VeniceDistClusterControllerStateModel.getPartitionNameFromVeniceClusterName(clusterName));
        helixAdminClient.enablePartition(false, controllerClusterName, controllerName, clusterName, partitionNames);
    }

    @Override
    public void stopVeniceController() {
        try {
            manager.disconnect();
            topicManager.close();
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
        PushMonitor monitor = getVeniceHelixResource(clusterName).getPushMonitor();
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
        return new OfflinePushStatusInfo(executionStatus, details);
    }

    @Override
    public Map<String, Long> getOfflinePushProgress(String clusterName, String kafkaTopic){
        checkControllerMastership(clusterName);
        PushMonitor monitor = getVeniceHelixResource(clusterName).getPushMonitor();
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
        long delayRebalanceTimeMs = multiClusterConfigs.getConfigForCluster(clusterName).getDelayToRebalanceMS();
        if (delayRebalanceTimeMs > 0)
            helixClusterProperties.put(ClusterConfig.ClusterConfigProperty.DELAY_REBALANCE_TIME.name(),
                String.valueOf(delayRebalanceTimeMs));
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
        if(isClusterCreated == false) {
            logger.info("Cluster  " + clusterName + " Creation returned false. ");
            return;
        }

        VeniceControllerClusterConfig config = multiClusterConfigs.getConfigForCluster(clusterName);
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

    @Override
    public TopicManager getTopicManager() {
        return this.topicManager;
    }

    @Override
    public boolean isMasterController(String clusterName) {
            VeniceDistClusterControllerStateModel model = controllerStateModelFactory.getModel(clusterName);
        if (model == null ) {
            return false;
        }
        return model.getCurrentState().equals(LeaderStandbySMD.States.LEADER.toString());
    }

  /**
   * Calculate number of partition for given store by give size.
   */
    @Override
    public int calculateNumberOfPartitions(String clusterName, String storeName, long storeSize) {
        checkControllerMastership(clusterName);
        VeniceControllerClusterConfig config = getVeniceHelixResource(clusterName).getConfig();
        return PartitionCountUtils.calculatePartitionCount(clusterName, storeName, storeSize,
            getVeniceHelixResource(clusterName).getMetadataRepository(),
            getVeniceHelixResource(clusterName).getRoutingDataRepository(), config.getPartitionSize(),
            config.getNumberOfPartition(), config.getMaxNumberOfPartition());
  }

    @Override
    public int getReplicationFactor(String clusterName, String storeName) {
        //TODO if there is special config for the given store, use that value.
        return getVeniceHelixResource(clusterName).getConfig().getReplicaFactor();
    }

    @Override
    public List<Replica> getReplicas(String clusterName, String kafkaTopic) {
        checkControllerMastership(clusterName);
        List<Replica> replicas = new ArrayList<>();
        PartitionAssignment partitionAssignment = getVeniceHelixResource(clusterName).getRoutingDataRepository()
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
        return InstanceStatusDecider.getReplicasForInstance(getVeniceHelixResource(cluster), instanceId);
    }

    @Override
    public NodeRemovableResult isInstanceRemovable(String clusterName, String helixNodeId, boolean isFromInstanceView) {
        checkControllerMastership(clusterName);
        int minActiveReplicas = getVeniceHelixResource(clusterName).getConfig().getMinActiveReplica();
        return isInstanceRemovable(clusterName, helixNodeId, minActiveReplicas, isFromInstanceView);
    }

    @Override
    public NodeRemovableResult isInstanceRemovable(String clusterName, String helixNodeId, int minActiveReplicas, boolean isInstanceView) {
        checkControllerMastership(clusterName);
        return InstanceStatusDecider
            .isRemovable(getVeniceHelixResource(clusterName), clusterName, helixNodeId, minActiveReplicas, isInstanceView);
    }

    @Override
    public Instance getMasterController(String clusterName) {
        if (multiClusterConfigs.getConfigForCluster(clusterName).isVeniceClusterLeaderHAAS()) {
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
        if (!Version.topicIsValidStoreVersion(kafkaTopic)) {
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
    public void killOfflinePush(String clusterName, String kafkaTopic) {
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
        /**
         * Check whether the specified store is already terminated or not.
         * If yes, the kill job message will be skipped.
         */
        Optional<Version> version;
        try {
            version = getStoreVersion(clusterName, kafkaTopic);
        } catch (VeniceNoStoreException e) {
            logger.warn("Kill job will be skipped since the corresponding store for topic: " + kafkaTopic
                + " doesn't exist in cluster: " + clusterName);
            return;
        }
        if (version.isPresent() && VersionStatus.isBootstrapTerminated(version.get().getStatus())) {
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

        StatusMessageChannel messageChannel = getVeniceHelixResource(clusterName).getMessageChannel();
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
        if (multiClusterConfigs.getConfigForCluster(clusterName).isAdminHelixMessagingChannelEnabled()) {
          messageChannel.sendToStorageNodes(clusterName, new KillOfflinePushMessage(kafkaTopic), kafkaTopic, retryCount);
        }
        if (multiClusterConfigs.getConfigForCluster(clusterName).isParticipantMessageStoreEnabled() &&
            participantMessageStoreRTTMap.containsKey(clusterName)) {
            VeniceWriter writer =
                participantMessageWriterMap.computeIfAbsent(clusterName, k -> {
                    int attempts = 0;
                    boolean verified = false;
                    String topic = participantMessageStoreRTTMap.get(clusterName);
                    while (attempts < INTERNAL_STORE_GET_RRT_TOPIC_ATTEMPTS) {
                        if (topicManager.containsTopic(topic)) {
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
            return adminConsumerServices.get(clusterName).getLastSucceedExecutionId(clusterName);
        } else {
            throw new VeniceException(
                "Cannot get the last succeed execution Id, must first setAdminConsumerService for cluster "
                    + clusterName);
        }
    }

    /**
     * Get last succeeded execution id for a given store.
     * @param clusterName
     * @param storeName
     * @return the last succeeded execution id or null if the cluster/store is invalid or the admin consumer service
     *         for the given cluster is not up and running yet.
     */
    public Long getLastSucceededExecutionId(String clusterName, String storeName) {
        return adminConsumerServices.containsKey(clusterName)
            ? adminConsumerServices.get(clusterName).getLastSucceededExecutionId(storeName) : null;
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
        ZkRoutersClusterManager routersClusterManager = getVeniceHelixResource(clusterName).getRoutersClusterManager();
        return routersClusterManager.getRoutersClusterConfig();
    }

    @Override
    public void updateRoutersClusterConfig(String clusterName, Optional<Boolean> isThrottlingEnable,
        Optional<Boolean> isQuotaRebalancedEnable, Optional<Boolean> isMaxCapaictyProtectionEnabled,
        Optional<Integer> expectedRouterCount) {
        ZkRoutersClusterManager routersClusterManager = getVeniceHelixResource(clusterName).getRoutersClusterManager();

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
    public List<String> getClusterOfStoreInMasterController(String storeName) {
        List<String> matchingClusters = new LinkedList<>();

        for (VeniceDistClusterControllerStateModel model : controllerStateModelFactory.getAllModels()) {
            Optional<VeniceHelixResources> resources = model.getResources();
            if (resources.isPresent()) {
                if (resources.get().getMetadataRepository().hasStore(storeName)) {
                    matchingClusters.add(model.getClusterName());
                }
            }
        }

        // Most of the time there should be only one matching cluster
        // During store migration there might be two matching clusters
        if (matchingClusters.size() > 2) {
            logger.warn("More than 2 matching clusters found for store " + storeName + "! Check these clusters: "
                + matchingClusters);
        }

        return matchingClusters;
    }

    @Override
    public Pair<String, String> discoverCluster(String storeName) {
        StoreConfig config = storeConfigAccessor.getStoreConfig(storeName);
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
        PushMonitor monitor = getVeniceHelixResource(clusterName).getPushMonitor();
        monitor.getTopicsOfOngoingOfflinePushes().forEach(topic -> result.put(topic, VersionStatus.STARTED.toString()));
        // Find the versions which had been ONLINE, but some of replicas are still bootstrapping due to:
        // 1. As we use N-1 strategy, so there might be some slow replicas caused by kafka or other issues.
        // 2. Storage node was added/removed/disconnected, so replicas need to bootstrap again on the same or orther node.
        RoutingDataRepository routingDataRepository = getVeniceHelixResource(clusterName).getRoutingDataRepository();
        ReadWriteStoreRepository storeRepository = getVeniceHelixResource(clusterName).getMetadataRepository();
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
        PushMonitorDelegator offlinePushMonitor = getVeniceHelixResource(clusterName).getPushMonitor();
        offlinePushMonitor.startMonitorOfflinePush(
            kafkaTopic,
            numberOfPartition,
            replicationFactor,
            strategy);
    }

    protected void stopMonitorOfflinePush(String clusterName, String topic) {
        PushMonitor offlinePushMonitor = getVeniceHelixResource(clusterName).getPushMonitor();
        offlinePushMonitor.stopMonitorOfflinePush(topic);
    }

    protected Store checkPreConditionForUpdateStoreMetadata(String clusterName, String storeName) {
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        Store store = repository.getStore(storeName);
        if (null == store) {
            throw new VeniceNoStoreException(storeName);
        }
        return store;
    }

    @Override
    public void close() {
        manager.disconnect();
        zkClient.close();
        jobTrackingVeniceWriterMap.forEach( (k, v) -> IOUtils.closeQuietly(v));
        jobTrackingVeniceWriterMap.clear();
        participantMessageWriterMap.forEach( (k, v) -> IOUtils.closeQuietly(v));
        participantMessageWriterMap.clear();
        IOUtils.closeQuietly(topicManager);
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

    protected VeniceHelixResources getVeniceHelixResource(String cluster) {
        Optional<VeniceHelixResources> resources = controllerStateModelFactory.getModel(cluster).getResources();
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

    public ZkStoreConfigAccessor getStoreConfigAccessor() {
        return storeConfigAccessor;
    }

    HelixReadOnlyStoreConfigRepository getStoreConfigRepo() { return storeConfigRepo; }

    private interface StoreMetadataOperation {
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

    public void setStoreConfigForMigration(String storeName, String srcClusterName, String destClusterName) {
        StoreConfig storeConfig = storeConfigAccessor.getStoreConfig(storeName);
        storeConfig.setMigrationSrcCluster(srcClusterName);
        storeConfig.setMigrationDestCluster(destClusterName);
        storeConfigAccessor.updateConfig(storeConfig);
    }

    /**
     * This thread will run in the background and update cluster discovery information when necessary
     */
    private void startStoreMigrationMonitor() {
        Thread thread = new Thread(() -> {
            Map<String, ControllerClient> srcControllerClients = new HashMap<>();

            while (true) {
                try {
                    Utils.sleep(10000);

                    // Get a list of clusters that this controller is responsible for
                    List<String> activeClusters = this.multiClusterConfigs.getClusters()
                        .stream()
                        .filter(cluster -> this.isMasterController(cluster))
                        .collect(Collectors.toList());

                    for (String clusterName : activeClusters) {
                        // For each cluster, get a list of stores that are migrating
                        HelixReadWriteStoreRepository storeRepo = this.getVeniceHelixResource(clusterName).getMetadataRepository();
                        List<Store> migratingStores = storeRepo.getAllStores()
                            .stream()
                            .filter(s -> s.isMigrating())
                            .filter(s -> this.storeConfigRepo.getStoreConfig(s.getName()).get().getMigrationSrcCluster() != null)
                            .filter(s -> this.storeConfigRepo.getStoreConfig(s.getName()).get().getMigrationDestCluster() != null)
                            .collect(Collectors.toList());

                        // For each migrating stores, check if store migration is complete.
                        // If so, update cluster discovery according to storeConfig
                        for (Store store : migratingStores) {
                            String storeName = store.getName();
                            StoreConfig storeConfig = this.storeConfigRepo.getStoreConfig(storeName).get();
                            String srcClusterName = storeConfig.getMigrationSrcCluster();
                            String destClusterName = storeConfig.getMigrationDestCluster();
                            String clusterDiscovered = storeConfig.getCluster();

                            // Both src and dest controller can do the job. Just pick one.
                            if (!clusterName.equals(destClusterName)) {
                                // Source controller will ignore
                                continue;
                            }

                            if (clusterDiscovered.equals(destClusterName)) {
                                // Migration complete already
                                continue;
                            }

                            ControllerClient srcControllerClient =
                                srcControllerClients.computeIfAbsent(srcClusterName,
                                    src_cluster_name -> new ControllerClient(src_cluster_name,
                                        this.getMasterController(src_cluster_name).getUrl(false), sslFactory));

                            List<Version> srcSortedOnlineVersions = srcControllerClient.getStore(storeName)
                                .getStore()
                                .getVersions()
                                .stream()
                                .sorted(Comparator.comparingInt(Version::getNumber).reversed()) // descending order
                                .filter(version -> version.getStatus().equals(VersionStatus.ONLINE))
                                .collect(Collectors.toList());

                            if (srcSortedOnlineVersions.size() == 0) {
                                logger.warn("Original store " + storeName + " in cluster " + srcClusterName + " does not have any online versions!");
                                // In this case updating cluster discovery information won't make it worse
                                this.updateClusterDiscovery(storeName, srcClusterName, destClusterName);
                                continue;
                            }
                            int srcLatestOnlineVersionNum = srcSortedOnlineVersions.get(0).getNumber();

                            Optional<Version> destLatestOnlineVersion = this.getStore(destClusterName, storeName)
                                .getVersions()
                                .stream()
                                .filter(version -> version.getStatus().equals(VersionStatus.ONLINE)
                                    && version.getNumber() >= srcLatestOnlineVersionNum)
                                .findAny();

                            if (destLatestOnlineVersion.isPresent()) {
                                logger.info(storeName + " cloned store in " + destClusterName
                                    + " is ready. Will update cluster discovery.");
                                // Switch read traffic from new clients; existing clients still need redeploy
                                this.updateClusterDiscovery(storeName, srcClusterName, destClusterName);
                                continue;
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.error("Caught exception in store migration monitor", e);
                }
            }
        });

        thread.start();
    }
}
