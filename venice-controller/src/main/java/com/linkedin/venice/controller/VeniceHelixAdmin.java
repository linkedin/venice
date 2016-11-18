package com.linkedin.venice.controller;

import com.linkedin.venice.controller.kafka.consumer.AdminConsumerService;
import com.linkedin.venice.exceptions.SchemaIncompatibilityException;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.HelixReadWriteSchemaRepository;
import com.linkedin.venice.helix.Replica;
import com.linkedin.venice.helix.ZkWhitelistAccessor;
import com.linkedin.venice.job.KillJobMessage;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.HelixReadWriteStoreRepository;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.status.StatusMessageChannel;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.PartitionCountUtils;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
 * hold a parent helix controller, which will always connecting to ZK. And there is a cluster only used for all of these
 * parent controllers. The second level is our venice clusters. Like prod cluster, dev cluster etc. Each of cluster will
 * be Helix resource in the controller's cluster. So that Helix will choose one of parent controller becoming the leader
 * of our venice cluster. In the state transition handler, we will create sub-controller for this venice cluster only.
 */
public class VeniceHelixAdmin implements Admin {
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
    private static final Logger logger = Logger.getLogger(VeniceHelixAdmin.class);
    private final HelixAdmin admin;
    private TopicManager topicManager;
    private final ZkClient zkClient;
    private ZkWhitelistAccessor whitelistAccessor;

    /**
     * Parent controller, it always being connected to Helix. And will create sub-controller for specific cluster when
     * getting notification from Helix.
     */
    private HelixManager manager;

    private VeniceDistClusterControllerStateModelFactory controllerStateModelFactory;
    //TODO Use different configs for different clusters when creating helix admin.
    public VeniceHelixAdmin(VeniceControllerConfig config) {
        this.controllerName = Utils.getHelixNodeIdentifier(config.getAdminPort());
        this.controllerClusterName = config.getControllerClusterName();
        this.controllerClusterReplica = config.getControllerClusterReplica();
        this.kafkaBootstrapServers =  config.getKafkaBootstrapServers();

        // TODO: Re-use the internal zkClient for the ZKHelixAdmin and TopicManager.
        this.admin = new ZKHelixAdmin(config.getZkAddress());
        //There is no way to get the internal zkClient from HelixManager or HelixAdmin. So create a new one here.
        this.zkClient = new ZkClient(config.getZkAddress(), ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT);
        this.topicManager = new TopicManager(config.getKafkaZkAddress());
        this.whitelistAccessor = new ZkWhitelistAccessor(zkClient);

        // Create the parent controller and related cluster if required.
        createControllerClusterIfRequired();
        controllerStateModelFactory =
            new VeniceDistClusterControllerStateModelFactory(zkClient);
        controllerStateModelFactory.addClusterConfig(config.getClusterName(), config);
        // TODO should the Manager initialization be part of start method instead of the constructor.
        manager = HelixManagerFactory
            .getZKHelixManager(controllerClusterName, controllerName, InstanceType.CONTROLLER_PARTICIPANT, config.getControllerClusterZkAddresss());
        StateMachineEngine stateMachine = manager.getStateMachineEngine();
        stateMachine.registerStateModelFactory(LeaderStandbySMD.name, controllerStateModelFactory);
        try {
            manager.connect();
        } catch (Exception ex) {
            String errorMessage = " Error starting Helix controller cluster "
                + controllerClusterName + " controller " + controllerName;
            logger.error(errorMessage, ex);
            throw new VeniceException(errorMessage, ex);
        }
    }

    private void setJobManagerAdmin(VeniceJobManager jobManager) {
        // TODO : Alternative ways of doing admin operation need to be explored. In the current code Admin has
        // relationship to all the resources and stores, but resources and stores are not aware of the admin.
        // This creates one centralized point, where all the admin operations need to flow through.
        // IF Admin object is shared, but stores/resources implement their own operation, it might be easier
        // to maintain and understand the code.

        jobManager.setAdmin(this);
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
    public synchronized void addStore(String clusterName, String storeName, String owner, String keySchema, String valueSchema) {
        VeniceHelixResources resources = getVeniceHelixResource(clusterName);
        synchronized (resources) { // Sloppy solution to race condition between add store and LEADER -> STANDBY controller state change
            checkPreConditionForAddStore(clusterName, storeName, owner, keySchema, valueSchema);
            VeniceControllerClusterConfig config = getVeniceHelixResource(clusterName).getConfig();
            Store store = new Store(storeName, owner, System.currentTimeMillis(), config.getPersistenceType(), config.getRoutingStrategy(), config.getReadStrategy(), config.getOfflinePushStrategy());
            HelixReadWriteStoreRepository storeRepo = resources.getMetadataRepository();
            storeRepo.addStore(store);
            // Add schema
            HelixReadWriteSchemaRepository schemaRepo = resources.getSchemaRepository();
            schemaRepo.initKeySchema(storeName, keySchema);
            schemaRepo.addValueSchema(storeName, valueSchema, HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID);
        }
    }

    protected void checkPreConditionForAddStore(String clusterName, String storeName, String owner, String keySchema, String valueSchema) {
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        if (repository.getStore(storeName) != null) {
            throwStoreAlreadyExists(clusterName, storeName);
        }
        // Check whether the schema is valid or not
        new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, keySchema);
        new SchemaEntry(SchemaData.INVALID_VALUE_SCHEMA_ID, valueSchema);
    }

    protected final static int VERSION_ID_UNSET = -1;

    @Override
    public synchronized Version addVersion(String clusterName, String storeName,int versionNumber, int numberOfPartition, int replicationFactor) {
        return addVersion(clusterName, storeName, versionNumber, numberOfPartition, replicationFactor, true);
    }

    protected synchronized Version addVersion(String clusterName, String storeName,int versionNumber, int numberOfPartition, int replicationFactor, boolean whetherStartOfflinePush) {
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();

        Version version = null;
        OfflinePushStrategy strategy = null;
        repository.lock();
        try {
            Store store = repository.getStore(storeName);
            if(store == null) {
                throwStoreDoesNotExist(clusterName, storeName);
            }

            strategy = store.getOffLinePushStrategy();
            if(versionNumber == VERSION_ID_UNSET) {
                // No Version supplied, generate new version.
                version = store.increaseVersion();
            } else {
                if (store.containsVersion(versionNumber)) {
                    throwVersionAlreadyExists(storeName, versionNumber);
                }
                //TODO add constraints that added version should be smaller than reserved one.
                version = new Version(storeName, versionNumber);
                store.addVersion(version);
            }
            // Update default partition count if it have not been assigned.
            if(store.getPartitionCount() == 0){
                store.setPartitionCount(numberOfPartition);
            }
            repository.updateStore(store);
            logger.info("Add version:"+version.getNumber()+" for store:" + storeName);
        } finally {
            repository.unLock();
        }

        VeniceControllerClusterConfig clusterConfig = controllerStateModelFactory.getModel(clusterName).getResources().getConfig();
        createKafkaTopic(clusterName, version.kafkaTopicName(), numberOfPartition, clusterConfig.getKafkaReplicaFactor());
        if (whetherStartOfflinePush) {
            createHelixResources(clusterName, version.kafkaTopicName(), numberOfPartition, replicationFactor);
            //Start offline push job for this new version.
            startOfflinePush(clusterName, version.kafkaTopicName(), numberOfPartition, replicationFactor, strategy);
        }
        return version;
    }

    @Override
    public synchronized Version incrementVersion(String clusterName, String storeName, int numberOfPartition,
        int replicationFactor) {
        return addVersion(clusterName, storeName, VERSION_ID_UNSET, numberOfPartition, replicationFactor);
    }

    @Override
    public int getCurrentVersion(String clusterName, String storeName){
        Store store = getStoreForReadOnly(clusterName, storeName);
        int version = store.getCurrentVersion(); /* Does not modify the store */
        return version;
    }

    @Override
    public Version peekNextVersion(String clusterName, String storeName) {
        Store store = getStoreForReadOnly(clusterName, storeName);
        Version version = store.peekNextVersion(); /* Does not modify the store */
        logger.info("Next version would be: " + version.getNumber() + " for store: " + storeName);
        return version;
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
        repository.lock();
        try {
            return repository.getStore(storeName);
        } finally {
            repository.unLock();
        }
    }

    @Override
    public synchronized void setCurrentVersion(String clusterName, String storeName, int versionNumber){
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        repository.lock();
        try {
            Store store = repository.getStore(storeName);
            if(store.containsVersion(versionNumber)) {
                store.setCurrentVersion(versionNumber);
            } else {
                String errorMsg = "Version:" + versionNumber + " does not exist for store:" + storeName;
                logger.error(errorMsg);
                throw new VeniceException(errorMsg);
            }
            repository.updateStore(store);
            logger.info("Set version:" + versionNumber +" for store:" + storeName);
        } finally {
            repository.unLock();
        }
    }

    // TODO: Though controller can control, multiple Venice-clusters, kafka topic name needs to be unique
    // among them. If there the same store name is present in two different venice clusters, the code
    // will fail and might exhibit other issues.
    private void createKafkaTopic(String clusterName, String kafkaTopic, int numberOfPartition, int kafkaReplicaFactor) {
        checkControllerMastership(clusterName);
        topicManager.createTopic(kafkaTopic, numberOfPartition, kafkaReplicaFactor);
    }

    private void createHelixResources(String clusterName, String kafkaTopic , int numberOfPartition , int replicationFactor) {
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
        } else {
            throwResourceAlreadyExists(kafkaTopic);
        }
    }

    @Override
    public void startOfflinePush(String clusterName, String kafkaTopic, int numberOfPartition, int replicationFactor, OfflinePushStrategy strategy) {
        checkControllerMastership(clusterName);
        VeniceJobManager jobManager = controllerStateModelFactory.getModel(clusterName).getResources().getJobManager();
        setJobManagerAdmin(jobManager);
        jobManager.startOfflineJob(kafkaTopic, numberOfPartition, replicationFactor, strategy);
    }

    @Override
    public void deleteHelixResource(String clusterName, String kafkaTopic) {
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
    public ExecutionStatus getOffLineJobStatus(String clusterName, String kafkaTopic) {
        checkControllerMastership(clusterName);
        VeniceJobManager jobManager = getVeniceHelixResource(clusterName).getJobManager();
        return jobManager.getOfflineJobStatus(kafkaTopic);
    }

    @Override
    public Map<String, Long> getOfflineJobProgress(String clusterName, String kafkaTopic){
        checkControllerMastership(clusterName);
        VeniceJobManager jobManager = getVeniceHelixResource(clusterName).getJobManager();
        return jobManager.getOfflineJobProgress(kafkaTopic);
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
        throw new VeniceException(errorMessage);
    }

    private void throwStoreDoesNotExist(String clusterName, String storeName) {
        String errorMessage = "Store:" + storeName + " does not exist in cluster:" + clusterName;
        logger.info(errorMessage);
        throw new VeniceException(errorMessage);
    }

    private void throwResourceAlreadyExists(String resourceName) {
        String errorMessage = "Resource:" + resourceName + " already exists, Can not add it to Helix.";
        logger.info(errorMessage);
        throw new VeniceException(errorMessage);
    }

    private void throwVersionAlreadyExists(String storeName, int version) {
        String errorMessage =
            "Version" + version + " already exists in Store:" + storeName + ". Can not add it to store.";
        logger.info(errorMessage);
        throw new VeniceException(errorMessage);
    }

    private void throwClusterNotInitialized(String clusterName) {
        String errorMessage = "Cluster " + clusterName + " is not initialized.";
        logger.info(errorMessage);
        throw new VeniceException(errorMessage);
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
    public void pauseStore(String clusterName, String storeName) {
        setPausedForStore(clusterName, storeName, true);
    }

    @Override
    public void resumeStore(String clusterName, String storeName) {
        setPausedForStore(clusterName, storeName, false);
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

    protected void checkPreConditionForKillOfflineJob(String clusterName, String kafkaTopic) {
        checkControllerMastership(clusterName);
    }

    @Override
    public void killOfflineJob(String clusterName, String kafkaTopic) {
        checkPreConditionForKillOfflineJob(clusterName, kafkaTopic);
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
        messageChannel.sendToStorageNodes(new KillJobMessage(kafkaTopic), kafkaTopic, retryCount);
    }

    @Override
    public StorageNodeStatus getStorageNodeStatus(String clusterName, String instanceId) {
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
        StorageNodeStatus currentStatus = getStorageNodeStatus(clusterName, instanceId);
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
    public synchronized void setLastException(String clusterName, Exception e) {
        lastExceptionMap.put(clusterName, e);
    }

    @Override
    public synchronized Exception getLastException(String clusterName) {
        return lastExceptionMap.get(clusterName);
    }

    private void setPausedForStore(String clusterName, String storeName, boolean paused) {
        checkControllerMastership(clusterName);
        HelixReadWriteStoreRepository repository = getVeniceHelixResource(clusterName).getMetadataRepository();
        repository.lock();
        try {
            Store store = checkPreConditionForPauseStoreAndGetStore(clusterName, storeName, paused);
            store.setPaused(paused);
            repository.updateStore(store);
        } finally {
            repository.unLock();
        }
    }

    protected Store checkPreConditionForPauseStoreAndGetStore(String clusterName, String storeName, boolean paused) {
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
        VeniceHelixResources resources = controllerStateModelFactory.getModel(cluster).getResources();
        if(resources == null){
            throwClusterNotInitialized(cluster);
        }
        return resources;
    }

    public void addConfig(String clusterName,VeniceControllerConfig config){
        controllerStateModelFactory.addClusterConfig(clusterName, config);
    }

    public ZkWhitelistAccessor getWhitelistAccessor() {
        return whitelistAccessor;
    }

    public String getControllerName(){
        return controllerName;
    }
}
