package com.linkedin.venice.controller;

import com.linkedin.venice.config.VeniceStorePartitionInformation;
import com.linkedin.venice.controller.kafka.TopicCreator;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.log4j.Logger;


/**
 * Helix Admin based on 0.6.5 APIs.
 */
public class VeniceHelixAdmin implements Admin {
    private final String controllerName;
    private final String zkConnString;
    private final Map<String, HelixManager> helixManagers = new HashMap<>();
    private static final Logger logger = Logger.getLogger(VeniceHelixAdmin.class.getName());
    private HelixAdmin admin;
    private TopicCreator topicCreator;

    public VeniceHelixAdmin(String controllerName, String zkConnString) {
        /* Controller name can be generated from the hostname and
        VMID https://docs.oracle.com/javase/7/docs/api/java/rmi/dgc/VMID.html
        but taking this parameter from the user for now
         */
        this.controllerName = controllerName;
        this.zkConnString = zkConnString;
        this.topicCreator = new TopicCreator(zkConnString);

    }

    @Override
    public void start(String cluster) {
        if(helixManagers.containsKey(cluster)) {
             throw new IllegalArgumentException("Cluster " + cluster + " already has a helix controller ");
        }
        HelixManager helixManager = HelixControllerMain
                .startHelixController(zkConnString, cluster, controllerName, HelixControllerMain.STANDALONE);
        helixManagers.put(cluster , helixManager);
    }

    @Override
    public void stop(String clusterName) {
        HelixManager helixManager = helixManagers.get(clusterName);
        if(helixManager == null) {
            logger.info("Cluster " + clusterName + " does not exist, skipping stop");
            return;
        }
        helixManager.disconnect();
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

        HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).
                forCluster(clusterName).build();
        Map<String, String> helixClusterProperties = new HashMap<String, String>();
        helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
        admin.setConfig(configScope, helixClusterProperties);
        logger.info("Cluster  " + clusterName + "  Completed, auto join to true. ");

        admin.addStateModelDef(clusterName, VeniceStateModel.PARTITION_ONLINE_OFFLINE_STATE_MODEL, VeniceStateModel.getDefinition());
    }

    @Override
    public void addStore(String clusterName, VeniceStorePartitionInformation storePartitionInformation) {
        if(admin == null ) {
            admin = new ZKHelixAdmin(zkConnString);
        }

        createClusterIfRequired(clusterName);
        topicCreator.createTopic(storePartitionInformation);

        String resourceName = storePartitionInformation.getStoreName();
        if(!admin.getResourcesInCluster(clusterName).contains(resourceName)) {
            admin.addResource(clusterName, resourceName, storePartitionInformation.getPartitionsCount(),
                    VeniceStateModel.PARTITION_ONLINE_OFFLINE_STATE_MODEL,
                    IdealState.RebalanceMode.FULL_AUTO.toString());
            admin.rebalance(clusterName, resourceName, storePartitionInformation.getReplicationFactor());
            logger.info("Added " + storePartitionInformation.getStoreName() + " as a resource to cluster: " + clusterName);
        } else {
            logger.info("Already exists " + storePartitionInformation.getStoreName()
                    + "as a resource in cluster: " + clusterName);
        }

    }
}
