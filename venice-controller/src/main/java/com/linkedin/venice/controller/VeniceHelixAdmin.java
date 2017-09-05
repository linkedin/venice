package com.linkedin.venice.controller;

import com.linkedin.venice.controller.kafka.StoreStatusDecider;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumerService;
import com.linkedin.venice.exceptions.SchemaIncompatibilityException;
import com.linkedin.venice.exceptions.VeniceNoClusterException;
import com.linkedin.venice.exceptions.VeniceStoreAlreadyExistsException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.HelixReadWriteSchemaRepository;
import com.linkedin.venice.helix.HelixStoreGraveyard;
import com.linkedin.venice.helix.Replica;
import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.helix.ZkWhitelistAccessor;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.InstanceStatus;
import com.linkedin.venice.meta.RoutersClusterConfig;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.StoreGraveyard;
import com.linkedin.venice.pushmonitor.KillOfflinePushMessage;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.HelixReadWriteStoreRepository;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.OfflinePushMonitor;
import com.linkedin.venice.replication.TopicReplicator;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.status.StatusMessageChannel;
import com.linkedin.venice.utils.*;
import io.tehuti.metrics.MetricsRepository;

import java.util.*;

import java.util.concurrent.ConcurrentHashMap;

import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.strategy.AutoRebalanceStrategy;
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.log4j.Logger;


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
    private final VeniceControllerMultiClusterConfig multiClusterConfigs;
    private final String controllerClusterName;
    private final int controllerClusterReplica;
    private final String controllerName;
    private final String kafkaBootstrapServers;
    private final Map<String, AdminConsumerService> adminConsumerServices = new HashMap<>();
    // Track last exception when necessary
    private Map<String, Exception> lastExceptionMap = new ConcurrentHashMap<String, Exception>();

    public static final int CONTROLLER_CLUSTER_NUMBER_OF_PARTITION = 1;
    public static final long CONTROLLER_JOIN_CLUSTER_TIMEOUT_MS = 1000*300l; // 5min
    public static final long CONTROLLER_JOIN_CLUSTER_RETRY_DURATION_MS = 500l;
    public final int NUM_VERSIONS_TO_PRESERVE = 2;
    private static final Logger logger = Logger.getLogger(VeniceHelixAdmin.class);
    private final HelixAdmin admin;
    private TopicManager topicManager;
    private final ZkClient zkClient;
    private final HelixAdapterSerializer adapterSerializer;
    private final ZkWhitelistAccessor whitelistAccessor;
    private final ExecutionIdAccessor executionIdAccessor;
    private final Optional<TopicReplicator> topicReplicator;
    private final long failedJobTopicRetentionMs;
    private final ZkStoreConfigAccessor storeConfigAccessor;


  /**
     * Parent controller, it always being connected to Helix. And will create sub-controller for specific cluster when
     * getting notification from Helix.
     */
    private HelixManager manager;

    private VeniceDistClusterControllerStateModelFactory controllerStateModelFactory;
    //TODO Use different configs for different clusters when creating helix admin.
    public VeniceHelixAdmin(VeniceControllerMultiClusterConfig multiClusterConfigs, Map<String,MetricsRepository> metricsRepositories) {
        this.multiClusterConfigs = multiClusterConfigs;
        this.controllerName = Utils.getHelixNodeIdentifier(multiClusterConfigs.getAdminPort());
        this.controllerClusterName = multiClusterConfigs.getControllerClusterName();
        this.controllerClusterReplica = multiClusterConfigs.getControllerClusterReplica();
        this.kafkaBootstrapServers =  multiClusterConfigs.getKafkaBootstrapServers();
        this.failedJobTopicRetentionMs = multiClusterConfigs.getFailedJobTopicRetentionMs();

        // TODO: Re-use the internal zkClient for the ZKHelixAdmin and TopicManager.
        this.admin = new ZKHelixAdmin(multiClusterConfigs.getZkAddress());
        //There is no way to get the internal zkClient from HelixManager or HelixAdmin. So create a new one here.
        this.zkClient = new ZkClient(multiClusterConfigs.getZkAddress(), ZkClient.DEFAULT_SESSION_TIMEOUT,
            ZkClient.DEFAULT_CONNECTION_TIMEOUT);
        this.adapterSerializer = new HelixAdapterSerializer();
        this.topicManager = new TopicManager(multiClusterConfigs.getKafkaZkAddress());
        this.whitelistAccessor = new ZkWhitelistAccessor(zkClient, adapterSerializer);
        this.executionIdAccessor = new ZkExecutionIdAccessor(zkClient, adapterSerializer);
        this.storeConfigAccessor =
            new ZkStoreConfigAccessor(zkClient, adapterSerializer);
        // TODO should pass a config instance instead of properties to get topic replicator.
        this.topicReplicator =
            TopicReplicator.getTopicReplicator(topicManager, multiClusterConfigs.getCommonConfig().getProps());

        // Create the parent controller and related cluster if required.
        createControllerClusterIfRequired();
        controllerStateModelFactory =
            new VeniceDistClusterControllerStateModelFactory(zkClient, adapterSerializer, this);
        // Preset the configs for all known clusters.
        for (String cluster : multiClusterConfigs.getClusters()) {
            controllerStateModelFactory.addClusterConfig(cluster, multiClusterConfigs.getConfigForCluster(cluster), metricsRepositories.get(cluster));
        }

        // Initialized the helix manger for the level1 controller.
        initLevel1Controller();
    }

    private void initLevel1Controller() {
        manager = HelixManagerFactory
            .getZKHelixManager(controllerClusterName, controllerName, InstanceType.CONTROLLER_PARTICIPANT,
                multiClusterConfigs.getControllerClusterZkAddresss());
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

    @Override
    public synchronized void start(String clusterName) {
        //Simply validate cluster name here.
        clusterName = clusterName.trim();
        if (clusterName.startsWith("/") || clusterName.endsWith("/") || clusterName.indexOf(' ') >= 0) {
            throw new IllegalArgumentException("Invalid cluster name:" + clusterName);
        }
        createClusterIfRequired(clusterName);
        // The resource and partition may be disabled for this controller before, we need to enable again at first. Then the state transition will be triggered.
        List<String> partitionNames = new ArrayList<>();
        partitionNames.add(VeniceDistClusterControllerStateModel.getPartitionNameFromVeniceClusterName(clusterName));
        admin.enablePartition(true, controllerClusterName, controllerName, clusterName, partitionNames);
        waitUnitControllerJoinsCluster(clusterName);
    }

    private void waitUnitControllerJoinsCluster(String clusterName) {
        PropertyKey.Builder keyBuilder = new PropertyKey.Builder(controllerClusterName);
        long startTime = System.currentTimeMillis();
        try {
            while (!controllerStateModelFactory.hasJoinedCluster(clusterName)) {
                // Waiting time out
                if (System.currentTimeMillis() - startTime >= CONTROLLER_JOIN_CLUSTER_TIMEOUT_MS) {
                    throw new InterruptedException("Time out when waiting controller join cluster:" + clusterName);
                }
                // Check whether enough controller has been assigned.
                ExternalView externalView =
                    manager.getHelixDataAccessor().getProperty(keyBuilder.externalView(clusterName));
                String partitionName = HelixUtils.getPartitionName(clusterName, 0);
                if (externalView != null && externalView.getStateMap(partitionName) != null) {
                    int assignedControllerCount = externalView.getStateMap(partitionName).size();
                    if (assignedControllerCount >= controllerClusterReplica) {
                        logger.info(
                            "Do not need to wait, this controller can not join because there is enough controller for cluster:"
                                + clusterName);
                        return;
                    }
                }
                // Wait
                Thread.sleep(CONTROLLER_JOIN_CLUSTER_RETRY_DURATION_MS);
            }
            logger.info("Controller joined the cluster:" + clusterName);
        } catch (InterruptedException e) {
            String errorMsg = "Controller can not join the cluster:" + clusterName;
            logger.error(errorMsg, e);
            throw new VeniceException(errorMsg, e);
        }
    }

    @Override
    public boolean isClusterValid(String clusterName) {
        return admin.getClusters().contains(clusterName);
    }

    @Override
    public synchronized void addStore(String clusterName, String storeName, String owner, String keySchema,
        String valueSchema) {
        VeniceHelixResources resources = getVeniceHelixResource(clusterName);
        logger.info("Start creating store: " + storeName);
        synchronized (resources) { // Sloppy solution to race condition between add store and LEADER -> STANDBY controller state change
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
                StoreGraveyard storeGraveyard = resources.getStoreGraveyard();
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
        }
    }

    @Override
    public synchronized void deleteStore(String clusterName, String storeName, int largestUsedVersionNumber) {
        checkControllerMastership(clusterName);
        logger.info("Start deleting store: " + storeName);
        VeniceHelixResources resources = getVeniceHelixResource(clusterName);
        synchronized (resources) {
            HelixReadWriteStoreRepository storeRepository = resources.getMetadataRepository();
            storeRepository.lock();
            try {
                Store store = storeRepository.getStore(storeName);
                checkPreConditionForDeletion(clusterName, storeName, store);
                if (largestUsedVersionNumber == Store.IGNORE_VERSION) {
                    // ignore and use the local largest used version number.
                    logger.info("Give largest used version number is: " + largestUsedVersionNumber
                        + " will skip overwriting the local store.");
                } else if (largestUsedVersionNumber < store.getLargestUsedVersionNumber()) {
                    throw new VeniceException("Given largest used version number: " + largestUsedVersionNumber
                        + " is smaller than the largest used version number: " + store.getLargestUsedVersionNumber() +
                        " found in repository.");
                } else {
                    store.setLargestUsedVersionNumber(largestUsedVersionNumber);
                }
                // Update deletion flag in Store to start deleting. In case of any failures during the deletion, the store
                // could be deleted later after controller is recovered.
                // A worse case is deletion succeeded in parent controller but failed in production controller. After skip
                // the admin message offset, a store was left in some prod colos. While user re-create the store, we will
                // delete this legacy store if isDeleting is true for this store.
                StoreConfig storeConfig = storeConfigAccessor.getConfig(storeName);
                storeConfig.setDeleting(true);
                storeConfigAccessor.updateConfig(storeConfig);
                storeRepository.updateStore(store);
            } finally {
                storeRepository.unLock();
            }
            // Delete All versions
            deleteAllVersionsInStore(clusterName, storeName);
            topicManager.ensureTopicIsDeletedAndBlock(Version.composeRealTimeTopic(storeName));
            // Move the store to graveyard. It will only re-create the znode for store's metadata excluding key and
            // value schemas.
            logger.info("Putting store:" + storeName + " into graveyard");
            HelixStoreGraveyard storeGraveyard = resources.getStoreGraveyard();
            storeGraveyard.putStoreIntoGraveyard(storeRepository.getStore(storeName));
            // Helix will remove all data under this store's znode including key and value schemas.
            resources.getMetadataRepository().deleteStore(storeName);
            // Delete the config for this store after deleting the store.
            storeConfigAccessor.deleteConfig(storeName);
            logger.info("Store:" + storeName + "has been deleted.");
        }
    }

    protected void checkPreConditionForAddStore(String clusterName, String storeName, String keySchema, String valueSchema) {
        checkControllerMastership(clusterName);
        // Before creating store, check the global stores configs at first.
        // TODO As some store had already been created before we introduced global store config
        // TODO so we need a way to sync up the data. For example, while we loading all stores from ZK for a cluster,
        // TODO put them into global store configs.
        boolean isLagecyStore = false;
        if(storeConfigAccessor.containsConfig(storeName)){
            // Controller was trying to delete the old store but failed.
            // Delete again before re-creating.
            // We lock the resource during deletion, so it's impossible to access the store which is being
            // deleted from here. So the only possible case is the deletion failed before.
            if(!storeConfigAccessor.getConfig(storeName).isDeleting()) {
                throw new VeniceStoreAlreadyExistsException(storeName);
            } else {
                isLagecyStore = true;
            }
        }

        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        Store  store= repository.getStore(storeName);
        // If the store exists in store repository and it's still active(not being deleted), we don't allow to re-create it.
        if (store != null && !isLagecyStore) {
            throwStoreAlreadyExists(clusterName, storeName);
        }
        // Check whether the schema is valid or not
        new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, keySchema);
        new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, valueSchema);
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
            String errorMsg = "Unable to delete the entire store or versions for store:" + storeName
                + ". Store has not been disabled. Both read and write need to be disabled before deleting.";
            logger.error(errorMsg);
            throw new VeniceException(errorMsg);
        }
    }

    protected final static int VERSION_ID_UNSET = -1;

    @Override
    public synchronized Version addVersion(String clusterName, String storeName, int versionNumber, int numberOfPartition, int replicationFactor) {
        return addVersion(clusterName, storeName, Version.guidBasedDummyPushId(), versionNumber, numberOfPartition, replicationFactor, true);
    }

    /**
     * Note, versionNumber may be VERSION_ID_UNSET, which must be accounted for
     */
    protected synchronized Version addVersion(String clusterName, String storeName, String pushJobId, int versionNumber, int numberOfPartition, int replicationFactor, boolean whetherStartOfflinePush) {
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();

        Version version = null;
        OfflinePushStrategy strategy = null;
        try {
            repository.lock();
            int newTopicPartitionCount = 0;
            try {
                Store store = repository.getStore(storeName);
                if (store == null) {
                    throwStoreDoesNotExist(clusterName, storeName);
                }
                strategy = store.getOffLinePushStrategy();
                if (versionNumber == VERSION_ID_UNSET) {
                    // No Version supplied, generate new version.
                    version = store.increaseVersion(pushJobId);
                } else {
                    if (store.containsVersion(versionNumber)) {
                        throwVersionAlreadyExists(storeName, versionNumber);
                    }
                    version = new Version(storeName, versionNumber, pushJobId);
                    store.addVersion(version);
                }
                // Update default partition count if it have not been assigned.
                if (store.getPartitionCount() == 0) {
                    store.setPartitionCount(numberOfPartition); //TODO, persist numberOfPartitions at the version level
                }
                newTopicPartitionCount = store.getPartitionCount();
                repository.updateStore(store);
                logger.info("Add version:" + version.getNumber() + " for store:" + storeName);
            } finally {
                repository.unLock();
            }

            VeniceControllerClusterConfig clusterConfig = getVeniceHelixResource(clusterName).getConfig();
            createKafkaTopic(clusterName, version.kafkaTopicName(), newTopicPartitionCount, clusterConfig.getKafkaReplicaFactor(), true);
            if (whetherStartOfflinePush) {
                // TODO: there could be some problem here since topic creation is an async op, which means the new topic
                // may not exist, When storage node is trying to consume the new created topic.

                // We need to prepare to monitor before creating helix resource.
                startMonitorOfflinePush(clusterName, version.kafkaTopicName(), numberOfPartition, replicationFactor,
                    strategy);
                createHelixResources(clusterName, version.kafkaTopicName(), numberOfPartition, replicationFactor);
            }
            return version;
        } catch (Throwable e) {
            // Clean up resources because add version failed.
            int failedVersionNumber = versionNumber;
            if (version != null) {
                failedVersionNumber = version.getNumber();
                deleteOneStoreVersion(clusterName, storeName, failedVersionNumber);
            }
            throw new VeniceException(
                "Failed to create helix resource:" + Version.composeKafkaTopic(storeName, failedVersionNumber), e);
        }
    }

    @Override
    public synchronized Version incrementVersion(String clusterName, String storeName, int numberOfPartition,
        int replicationFactor) {
        return addVersion(clusterName, storeName, VERSION_ID_UNSET, numberOfPartition, replicationFactor);
    }

    /**
     * Note: this doesn't currently use the pushID to guarantee idempotence, unexpected behavior may result if multiple
     * batch jobs push to the same store at the same time.
     */
    @Override
    public synchronized Version incrementVersionIdempotent(String clusterName, String storeName, String pushJobId, int numberOfPartitions, int replicationFactor, boolean offlinePush) {
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        repository.lock();
        try {
            Store store = repository.getStore(storeName);
            if(store == null) {
                throwStoreDoesNotExist(clusterName, storeName);
            }
            // This uses the pushId to make sure any requests with the same push ID get the same version.  As of May 2017
            // Samza cannot provide a unique ID, so we're using different logic.  We should keep pushing Samza to provide
            // that ID and then restore this behavior.
            //Optional<Version> existingVersionToUse = getVersionWithPushId(store, pushJobId);
            Optional<Version> existingVersionToUse = getStartedVersion(store);
            if (existingVersionToUse.isPresent()){
                return existingVersionToUse.get();
            }
        } finally {
            repository.unLock();
        }
        return addVersion(clusterName, storeName, pushJobId, VERSION_ID_UNSET, numberOfPartitions, replicationFactor, offlinePush);
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
                int partitionCount = store.getPartitionCount();
                if (partitionCount == 0){
                    //TODO:  partitioning is currently decided on first version push, and we need to match that versioning
                    // we should evaluate alternatives such as allowing the RT topic request to initialize the number of
                    // partitions, or setting the number of partitions at store creation time instead of at first version
                    throw new VeniceException("Store: " + storeName + " is not initialized with a version yet");
                }
                VeniceControllerClusterConfig clusterConfig = getVeniceHelixResource(clusterName).getConfig();
                createKafkaTopic(clusterName, realTimeTopic, partitionCount, clusterConfig.getKafkaReplicaFactor(), false);
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
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
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
    }

    /**
     * Delete version from cluster, removing all related resources
     */
    @Override
    public void deleteOneStoreVersion(String clusterName, String storeName, int versionNumber) {
        String resourceName = Version.composeKafkaTopic(storeName, versionNumber);
        logger.info("Deleting helix resource:" + resourceName + " in cluster:" + clusterName);
        deleteHelixResource(clusterName, resourceName);
        logger.info("Killing offline push for:" + resourceName + " in cluster:" + clusterName);
        killOfflinePush(clusterName, resourceName);
        if (topicReplicator.isPresent()) {
            String realTimeTopic = Version.composeRealTimeTopic(storeName);
            topicReplicator.get().terminateReplication(realTimeTopic, resourceName);
        }
        Optional<Version> deletedVersion = deleteVersionFromStoreRepository(clusterName, storeName, versionNumber);
        if (deletedVersion.isPresent()) {
            deleteKafkaTopicForVersion(clusterName, deletedVersion.get());
        }
        stopMonitorOfflinePush(clusterName, resourceName);

    }

    @Override
    public void retireOldStoreVersions(String clusterName, String storeName) {
        logger.info("Retiring old versions for store: " + storeName);
        HelixReadWriteStoreRepository storeRepository = getVeniceHelixResource(clusterName).getMetadataRepository();
        Store store = storeRepository.getStore(storeName);
        List<Version> versionsToDelete = store.retrieveVersionsToDelete(NUM_VERSIONS_TO_PRESERVE);
        for (Version version : versionsToDelete) {
            deleteOneStoreVersion(clusterName, storeName, version.getNumber());
            logger.info("Retired store:" + store.getName() + " version:" + version.getNumber());
        }
        logger.info("Retired " + versionsToDelete.size() + " versions for store: " + storeName);
    }

    /***
     * Delete the version specified from the store and return the deleted version.
     */
    protected Optional<Version> deleteVersionFromStoreRepository(String clusterName, String storeName, int versionNumber) {
        HelixReadWriteStoreRepository storeRepository = getVeniceHelixResource(clusterName).getMetadataRepository();
        logger.info("Deleting version " + versionNumber + " in Store:" + storeName + " in cluster:" + clusterName);
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

    protected void deleteKafkaTopicForVersion(String clusterName, Version version) {
        String kafkaTopicName = version.kafkaTopicName();
        boolean isCompleted = false;
        if (VersionStatus.isBootstrapTerminated(version.getStatus())) {
            // Only delete kafka topic after consumption is completed. In that case, there is no more data will
            // be produced from Kafka MM. Otherwise, deleting would cause Kafka MM crashed.
            isCompleted = true;
        }

        if (isCompleted || getVeniceHelixResource(clusterName).getConfig()
            .isEnableTopicDeletionForUncompletedJob()) {
            logger.info("Deleting topic: " + kafkaTopicName);
            getTopicManager().ensureTopicIsDeletedAsync(kafkaTopicName);
            logger.info("Deleted topic: " + kafkaTopicName);
        } else {
            // TODO an async task is needed to collect topics later to prevent resource leaking.
            logger.warn("Could not delete Kafka topic: " + kafkaTopicName
                + "Because the offline push for this version has not been completed, and topic deletion for uncompleted push is disabled. Version status:"
                + version.getStatus());
            logger.info("Will update topic: " + kafkaTopicName + " with retention.ms: " + failedJobTopicRetentionMs);
            getTopicManager().updateTopicRetention(kafkaTopicName, failedJobTopicRetentionMs);

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
            return repository.listStores();
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
            List<Store> storeList = repository.listStores();
            RoutingDataRepository routingDataRepository =
                getVeniceHelixResource(clusterName).getRoutingDataRepository();
            ResourceAssignment resourceAssignment = routingDataRepository.getResourceAssignment();
            return StoreStatusDecider.getStoreStatues(storeList, resourceAssignment,
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
            int desiredPartitionCount = partitionCount;

            if (desiredPartitionCount > clusterConfig.getMaxNumberOfPartition()) {
                desiredPartitionCount = clusterConfig.getMaxNumberOfPartition();
            } else if (desiredPartitionCount < clusterConfig.getNumberOfPartition()) {
                desiredPartitionCount = clusterConfig.getNumberOfPartition();
            }

            store.setPartitionCount(desiredPartitionCount);

            return store;
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

    @Override
    public synchronized void updateStore(
        String clusterName,
        String storeName,
        Optional<String> owner,
        Optional<Boolean> readability,
        Optional<Boolean> writeability,
        Optional<Integer> partitionCount,
        Optional<Long> storageQuotaInByte,
        Optional<Long> readQuotaInCU,
        Optional<Integer> currentVersion,
        Optional<Long> hybridRewindSeconds,
        Optional<Long> hybridOffsetLagThreshold) {
        Store originalStore = getStore(clusterName, storeName).cloneStore();

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

            if (storageQuotaInByte.isPresent()) {
                setStoreStorageQuota(clusterName, storeName, storageQuotaInByte.get());
            }

            if (readQuotaInCU.isPresent()) {
                setStoreReadQuota(clusterName, storeName, readQuotaInCU.get());
            }

            if (currentVersion.isPresent()) {
                setStoreCurrentVersion(clusterName, storeName, currentVersion.get());
            }

            if (hybridRewindSeconds.isPresent() || hybridOffsetLagThreshold.isPresent()) {
                HybridStoreConfig hybridConfig = mergeNewSettingsIntoOldHybridStoreConfig(
                    originalStore, hybridRewindSeconds, hybridOffsetLagThreshold);
                if (null != hybridConfig) {
                    storeMetadataUpdate(clusterName, storeName, store -> {
                        store.setHybridStoreConfig(hybridConfig);
                        return store;
                    });
                }
            }

        } catch (VeniceException e) {
            //rollback to original store
            storeMetadataUpdate(clusterName, storeName, store -> originalStore);
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

    // TODO: Though controller can control, multiple Venice-clusters, kafka topic name needs to be unique
    // among them. If there the same store name is present in two different venice clusters, the code
    // will fail and might exhibit other issues.
    private void createKafkaTopic(String clusterName, String kafkaTopic, int numberOfPartition, int kafkaReplicaFactor, boolean eternal) {
        checkControllerMastership(clusterName);
        topicManager.createTopic(kafkaTopic, numberOfPartition, kafkaReplicaFactor, eternal);
    }

    private void createHelixResources(String clusterName, String kafkaTopic , int numberOfPartition , int replicationFactor)
        throws InterruptedException {
        if (!admin.getResourcesInCluster(clusterName).contains(kafkaTopic)) {
            admin.addResource(clusterName, kafkaTopic, numberOfPartition,
                VeniceStateModel.PARTITION_ONLINE_OFFLINE_STATE_MODEL, IdealState.RebalanceMode.FULL_AUTO.toString(),
                AutoRebalanceStrategy.class.getName());
            VeniceControllerClusterConfig config = getVeniceHelixResource(clusterName).getConfig();
            IdealState idealState = admin.getResourceIdealState(clusterName, kafkaTopic);
            // We don't set the delayed time per resoruce, we will use the cluster level helix config to decide
            // the delayed reblance time
            idealState.setRebalancerClassName(DelayedAutoRebalancer.class.getName());
            idealState.setMinActiveReplicas(config.getMinActiveReplica());
            // Use crush alg to allocate resources
            idealState.setRebalanceStrategy(CrushRebalanceStrategy.class.getName());
            admin.setResourceIdealState(clusterName, kafkaTopic, idealState);
            logger.info("Enabled delayed re-balance for resource:" + kafkaTopic);
            admin.rebalance(clusterName, kafkaTopic, replicationFactor);
            logger.info("Added " + kafkaTopic + " as a resource to cluster: " + clusterName);

            // TODO Wait until there are enough nodes assigned to the given resource.
            // This waiting is not mandatory in our new offline push monitor. But in order to keep the logic as similar
            // as before, we still waiting here. This logic could be removed later.
            OfflinePushMonitor monitor = getVeniceHelixResource(clusterName).getOfflinePushMonitor();
            monitor.waitUntilNodesAreAssignedForResource(kafkaTopic, config.getOffLineJobWaitTimeInMilliseconds());
        } else {
            throwResourceAlreadyExists(kafkaTopic);
        }
    }

    protected void deleteHelixResource(String clusterName, String kafkaTopic) {
        checkControllerMastership(clusterName);
        admin.dropResource(clusterName, kafkaTopic);
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
    public int getValueSchemaId(String clusterName, String storeName, String valueSchemaStr) {
        checkControllerMastership(clusterName);
        HelixReadWriteSchemaRepository schemaRepo = getVeniceHelixResource(clusterName).getSchemaRepository();
        return schemaRepo.getValueSchemaId(storeName, valueSchemaStr);
    }

    @Override
    public SchemaEntry getValueSchema(String clusterName, String storeName, int id) {
        checkControllerMastership(clusterName);
        HelixReadWriteSchemaRepository schemaRepo = getVeniceHelixResource(clusterName).getSchemaRepository();
        return schemaRepo.getValueSchema(storeName, id);
    }

    @Override
    public SchemaEntry addValueSchema(String clusterName, String storeName, String valueSchemaStr) {
        checkPreConditionForAddValueSchemaAndGetNewSchemaId(clusterName, storeName, valueSchemaStr);
        HelixReadWriteSchemaRepository schemaRepo = getVeniceHelixResource(clusterName).getSchemaRepository();
        return schemaRepo.addValueSchema(storeName, valueSchemaStr);
    }

    @Override
    public SchemaEntry addValueSchema(String clusterName, String storeName, String valueSchemaStr, int schemaId) {
        checkPreConditionForAddValueSchemaAndGetNewSchemaId(clusterName, storeName, valueSchemaStr);
        HelixReadWriteSchemaRepository schemaRepo = getVeniceHelixResource(clusterName).getSchemaRepository();
        return schemaRepo.addValueSchema(storeName, valueSchemaStr, schemaId);
    }

  /**
   * This function will check whether the provided schema is good to add to the provided store.
   * If yes, it will return the value schema id to be used.
   * @param clusterName
   * @param storeName
   * @param valueSchemaStr
   * @return
   */
    protected int checkPreConditionForAddValueSchemaAndGetNewSchemaId(String clusterName, String storeName, String valueSchemaStr) {
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        if (!repository.hasStore(storeName)) {
            throw new VeniceNoStoreException(storeName);
        }
        // Check compatibility
        SchemaEntry newValueSchemaWithInvalidId = new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, valueSchemaStr);
        Collection<SchemaEntry> valueSchemas = getValueSchemas(clusterName, storeName);
        int maxValueSchemaId = SchemaData.INVALID_VALUE_SCHEMA_ID;
        for (SchemaEntry entry : valueSchemas) {
            // Idempotent
            if (entry.equals(newValueSchemaWithInvalidId)) {
                return entry.getId();
            }
            if (!entry.isCompatible(newValueSchemaWithInvalidId)) {
                throw new SchemaIncompatibilityException(entry, newValueSchemaWithInvalidId);
            }
            if (entry.getId() > maxValueSchemaId) {
                maxValueSchemaId = entry.getId();
            }
        }
        if (SchemaData.INVALID_VALUE_SCHEMA_ID == maxValueSchemaId) {
            return HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;
        } else {
            return maxValueSchemaId + 1;
        }
    }

    @Override
    public List<String> getStorageNodes(String clusterName){
        checkControllerMastership(clusterName);
        return admin.getInstancesInCluster(clusterName);
    }

    @Override
    public Map<String, String> getStorageNodesStatus(String clusterName) {
        checkControllerMastership(clusterName);
        List<String> instances = admin.getInstancesInCluster(clusterName);
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
        InstanceConfig instanceConfig = admin.getInstanceConfig(clusterName, instanceId);
        admin.dropInstance(clusterName, instanceConfig);
        logger.info("Removed storage node: " + instanceId + " from cluster: " + clusterName);
    }

    @Override
    public synchronized void stop(String clusterName) {
        // Instead of disconnecting the sub-controller for the given cluster, we should disable it for this controller,
        // then the LEADER->STANDBY and STANDBY->OFFLINE will be triggered, our handler will handle the resource collection.
        List<String> partitionNames = new ArrayList<>();
        partitionNames.add(VeniceDistClusterControllerStateModel.getPartitionNameFromVeniceClusterName(clusterName));
        admin.enablePartition(false, controllerClusterName, controllerName, clusterName, partitionNames);
    }

    @Override
    public void stopVeniceController() {
        try {
            manager.disconnect();
            topicManager.close();
            zkClient.close();
            admin.close();
        } catch (Exception e) {
            throw new VeniceException("Can not stop controller correctly.", e);
        }
    }

    @Override
    public OfflinePushStatusInfo getOffLinePushStatus(String clusterName, String kafkaTopic) {
        checkControllerMastership(clusterName);
        OfflinePushMonitor monitor = getVeniceHelixResource(clusterName).getOfflinePushMonitor();
        return new OfflinePushStatusInfo(monitor.getOfflinePushStatus(kafkaTopic));
    }

    @Override
    public Map<String, Long> getOfflinePushProgress(String clusterName, String kafkaTopic){
        checkControllerMastership(clusterName);
        OfflinePushMonitor monitor = getVeniceHelixResource(clusterName).getOfflinePushMonitor();
        return monitor.getOfflinePushProgress(kafkaTopic);
    }

    // Create the cluster for all of parent controllers if required.
    private void createControllerClusterIfRequired(){
        if(admin.getClusters().contains(controllerClusterName)) {
            logger.info("Cluster  " + controllerClusterName + " already exists. ");
            return;
        }

        boolean isClusterCreated = admin.addCluster(controllerClusterName, false);
        if(isClusterCreated == false) {
            logger.info("Cluster  " + controllerClusterName + " Creation returned false. ");
            return;
        }
        HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).
            forCluster(controllerClusterName).build();
        Map<String, String> helixClusterProperties = new HashMap<String, String>();
        helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
        admin.setConfig(configScope, helixClusterProperties);
        admin.addStateModelDef(controllerClusterName, LeaderStandbySMD.name, LeaderStandbySMD.build());
    }

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

        VeniceControllerClusterConfig config = controllerStateModelFactory.getClusterConfig(clusterName);
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

        admin
            .addResource(controllerClusterName, clusterName, CONTROLLER_CLUSTER_NUMBER_OF_PARTITION, LeaderStandbySMD.name,
                IdealState.RebalanceMode.FULL_AUTO.toString());
        admin.rebalance(controllerClusterName, clusterName, controllerClusterReplica);
    }

    private void throwStoreAlreadyExists(String clusterName, String storeName) {
        String errorMessage = "Store:" + storeName + " already exists. Can not add it to cluster:" + clusterName;
        logger.info(errorMessage);
        throw new VeniceStoreAlreadyExistsException(storeName);
    }

    private void throwStoreDoesNotExist(String clusterName, String storeName) {
        String errorMessage = "Store:" + storeName + " does not exist in cluster:" + clusterName;
        logger.info(errorMessage);
        throw new VeniceNoStoreException(storeName);
    }

    private void throwResourceAlreadyExists(String resourceName) {
        String errorMessage = "Resource:" + resourceName + " already exists, Can not add it to Helix.";
        logAndThrow(errorMessage);
    }

    private void throwVersionAlreadyExists(String storeName, int version) {
        String errorMessage =
            "Version" + version + " already exists in Store:" + storeName + ". Can not add it to store.";
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
    public String getKafkaBootstrapServers() {
        return this.kafkaBootstrapServers;
    }

    @Override
    public TopicManager getTopicManager() {
        return this.topicManager;
    }

    @Override
    public synchronized boolean isMasterController(String clusterName) {
        VeniceDistClusterControllerStateModel model = controllerStateModelFactory.getModel(clusterName);
        if (model == null ) {
            throwClusterNotInitialized(clusterName);
        }
        return model.getCurrentState().equals(LeaderStandbySMD.States.LEADER.toString());
    }

  /**
   * Calculate number of partition for given store by give size.
   *
   * @param clusterName
   * @param storeName
   * @param storeSize
   * @return
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
    public List<Replica> getBootstrapReplicas(String clusterName, String kafkaTopic) {
        checkControllerMastership(clusterName);
        List<Replica> replicas = new ArrayList<>();
        PartitionAssignment partitionAssignment = getVeniceHelixResource(clusterName).getRoutingDataRepository().getPartitionAssignments(kafkaTopic);
        for(Partition partition:partitionAssignment.getAllPartitions()){
            addInstancesToReplicaList(replicas, partition.getBootstrapInstances(), kafkaTopic, partition.getId(), HelixState.BOOTSTRAP_STATE);
        }
        return replicas;
    }

    @Override
    public List<Replica> getErrorReplicas(String clusterName, String kafkaTopic) {
        checkControllerMastership(clusterName);
        List<Replica> replicas = new ArrayList<>();
        PartitionAssignment partitionAssignment = getVeniceHelixResource(clusterName).getRoutingDataRepository().getPartitionAssignments(kafkaTopic);
        for(Partition partition:partitionAssignment.getAllPartitions()){
            addInstancesToReplicaList(replicas, partition.getErrorInstances(), kafkaTopic, partition.getId(), HelixState.ERROR_STATE);
        }
        return replicas;
    }

    @Override
    public List<Replica> getReplicas(String clusterName, String kafkaTopic) {
        checkControllerMastership(clusterName);
        List<Replica> replicas = new ArrayList<>();
        PartitionAssignment partitionAssignment = getVeniceHelixResource(clusterName).getRoutingDataRepository().getPartitionAssignments(kafkaTopic);
        for(Partition partition:partitionAssignment.getAllPartitions()){
            addInstancesToReplicaList(replicas, partition.getErrorInstances(), kafkaTopic, partition.getId(), HelixState.ERROR_STATE);
            addInstancesToReplicaList(replicas, partition.getBootstrapInstances(), kafkaTopic, partition.getId(), HelixState.BOOTSTRAP_STATE);
            addInstancesToReplicaList(replicas, partition.getReadyToServeInstances(), kafkaTopic, partition.getId(), HelixState.ONLINE_STATE);
        }
        return replicas;
    }

    private void addInstancesToReplicaList(List<Replica> replicaList, List<Instance> instancesToAdd, String resource, int partitionId, String stateOfAddedReplicas){
        for (Instance instance : instancesToAdd){
            Replica replica = new Replica(instance, partitionId, resource);
            replica.setStatus(stateOfAddedReplicas);
            replicaList.add(replica);
        }
    }

    @Override
    public List<Replica> getReplicasOfStorageNode(String cluster, String instanceId) {
        checkControllerMastership(cluster);
        return InstanceStatusDecider.getReplicasForInstance(getVeniceHelixResource(cluster), instanceId);
    }

    @Override
    public boolean isInstanceRemovable(String clusterName, String helixNodeId) {
        checkControllerMastership(clusterName);
        int minActiveReplicas = getVeniceHelixResource(clusterName).getConfig().getMinActiveReplica();
        return isInstanceRemovable(clusterName, helixNodeId, minActiveReplicas);
    }

    @Override
    public boolean isInstanceRemovable(String clusterName, String helixNodeId, int minActiveReplicas) {
        checkControllerMastership(clusterName);
        return InstanceStatusDecider
            .isRemovable(getVeniceHelixResource(clusterName), clusterName, helixNodeId, minActiveReplicas);
    }

    @Override
    public Instance getMasterController(String clusterName) {
        PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
        LiveInstance instance = manager.getHelixDataAccessor().getProperty(keyBuilder.controllerLeader());
        if (instance == null) {
            throw new VeniceException("Can not find a master controller in the cluster:" + clusterName);
        } else {
            String instanceId = instance.getId();
            return new Instance(instanceId, Utils.parseHostFromHelixNodeIdentifier(instanceId),
                Utils.parsePortFromHelixNodeIdentifier(instanceId));
        }
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
    }

    @Override
    public void killOfflinePush(String clusterName, String kafkaTopic) {
        checkPreConditionForKillOfflinePush(clusterName, kafkaTopic);
        StatusMessageChannel messageChannel = getVeniceHelixResource(clusterName).getMessageChannel();
        int retryCount = 3;
        // Broadcast kill message to all of storage nodes assigned to given resource. Helix will help us to only send
        // message to the live instances.
        // The alternative way here is that get the storage nodes in BOOTSTRAP state of given resource, then send the
        // kill message node by node. Considering the simplicity, broadcast is a better.
        // In prospective of performance, each time helix sending a message needs to read the whole LIVE_INSTANCE and
        // EXTERNAL_VIEW from ZK, so sending message nodes by nodes would generate lots of useless read requests. Of course
        // broadcast would generate useless write requests to ZK(N-M useless messages, N=number of nodes assigned to resource,
        // M=number of nodes have completed the ingestion or have not started). But considering the number of nodes in
        // our cluster is not too big, so it's not a big deal here.
        messageChannel.sendToStorageNodes(new KillOfflinePushMessage(kafkaTopic), kafkaTopic, retryCount);
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
    public void skipAdminMessage(String clusterName, long offset){
        if (adminConsumerServices.containsKey(clusterName)){
            adminConsumerServices.get(clusterName).setOffsetToSkip(clusterName, offset);
        } else {
            throw new VeniceException("Cannot skip offset, must first setAdminConsumerService for cluster " + clusterName);
        }

    }

    @Override
    public long getLastSucceedExecutionId(String clusterName) {
        if (adminConsumerServices.containsKey(clusterName)) {
            return adminConsumerServices.get(clusterName).getLastSucceedExecutionId(clusterName);
        } else {
            throw new VeniceException(
                "Cannot get the last succeed execution Id, must first setAdminConsumerService for cluster "
                    + clusterName);
        }
    }

    @Override
    public synchronized void setLastException(String clusterName, Exception e) {
        lastExceptionMap.put(clusterName, e);
    }

    @Override
    public synchronized Exception getLastException(String clusterName) {
        return lastExceptionMap.get(clusterName);
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
    public Optional<String> getClusterOfStoreInMasterController(String storeName) {
        for (VeniceDistClusterControllerStateModel model : controllerStateModelFactory.getAllModels()) {
            Optional<VeniceHelixResources> resources = model.getResources();
            if (resources.isPresent()) {
                if (resources.get().getMetadataRepository().hasStore(storeName)) {
                    return Optional.of(model.getClusterName());
                }
            }
        }
        return Optional.empty();
    }

    protected void startMonitorOfflinePush(String clusterName, String kafkaTopic, int numberOfPartition,
        int replicationFactor, OfflinePushStrategy strategy) {
        OfflinePushMonitor offlinePushMonitor = getVeniceHelixResource(clusterName).getOfflinePushMonitor();
        offlinePushMonitor.setTopicReplicator(topicReplicator);
        offlinePushMonitor.startMonitorOfflinePush(
            kafkaTopic,
            numberOfPartition,
            replicationFactor,
            strategy);
    }

    protected void stopMonitorOfflinePush(String clusterName, String topic) {
        OfflinePushMonitor offlinePushMonitor = getVeniceHelixResource(clusterName).getOfflinePushMonitor();
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
        IOUtils.closeQuietly(topicManager);
    }

    /**
     * Check whether this controller is master or not. If not, throw the VeniceException to skip the request to
     * this controller.
     *
     * @param clusterName
     */
    private void checkControllerMastership(String clusterName) {
        if (!isMasterController(clusterName)) {
            throw new VeniceException("This controller:" + controllerName + " is not the master of '" + clusterName
                + "'. Can not handle the admin request.");
        }
    }

    protected VeniceHelixResources getVeniceHelixResource(String cluster){
        Optional<VeniceHelixResources> resources = controllerStateModelFactory.getModel(cluster).getResources();
        if (!resources.isPresent()) {
            throwClusterNotInitialized(cluster);
        }
        return resources.get();
    }

    public void addConfig(String clusterName,VeniceControllerConfig config, MetricsRepository metricsRepository){
        controllerStateModelFactory.addClusterConfig(clusterName, config, metricsRepository);
    }

    public void addMetric(String clusterName, MetricsRepository metricsRepository){

    }

    public ZkWhitelistAccessor getWhitelistAccessor() {
        return whitelistAccessor;
    }

    public String getControllerName(){
        return controllerName;
    }

    public ZkStoreConfigAccessor getStoreConfigAccessor() {
        return storeConfigAccessor;
    }

    private interface StoreMetadataOperation {
        /**
         * define the operation that update a store. Return the store after metadata being updated so that it could
         * be updated by metadataRepository
         */
        Store update(Store store);
    }
}
