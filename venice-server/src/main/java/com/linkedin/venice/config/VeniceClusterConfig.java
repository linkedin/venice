package com.linkedin.venice.config;

import com.google.common.collect.ImmutableMap;
import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.kafka.consumer.offsets.OffsetManager;
import com.linkedin.venice.partition.ModuloPartitionNodeAssignmentScheme;
import com.linkedin.venice.server.VeniceConfigService;
import com.linkedin.venice.utils.Props;

import java.io.File;
import java.util.Map;


/**
 * class that maintains config very specific to a Venice cluster
 */
public class VeniceClusterConfig {

    public static final Map<String, String> partitionNodeAssignmentSchemeClassMap =
            ImmutableMap.of("modulo", ModuloPartitionNodeAssignmentScheme.class.getName());

    private String clusterName;
    private int storageNodeCount;
    protected String dataBasePath;
    private String partitionNodeAssignmentSchemeName;
    private boolean enableKafkaConsumersOffsetManagement;
    private String offsetManagerType = null;
    private String offsetDatabasePath = null;
    private long offsetManagerFlushIntervalMs;

    private boolean enableConsumptionAcksForAzkabanJobs;

    private String kafkaConsumptionAcksBrokerUrl;

    public VeniceClusterConfig(Props clusterProperties)
            throws ConfigurationException {
        checkProperties(clusterProperties);
    }

    protected void checkProperties(Props clusterProps)
            throws ConfigurationException {
        clusterName = clusterProps.getString(VeniceConfigService.CLUSTER_NAME);
        storageNodeCount = clusterProps.getInt(VeniceConfigService.STORAGE_NODE_COUNT, 1);     // Default 1
        partitionNodeAssignmentSchemeName = clusterProps
                .getString(VeniceConfigService.PARTITION_NODE_ASSIGNMENT_SCHEME, "modulo"); // Default "modulo" scheme
        if (!partitionNodeAssignmentSchemeClassMap.containsKey(partitionNodeAssignmentSchemeName)) {
            throw new ConfigurationException(
                    "unknown partition node assignment scheme: " + partitionNodeAssignmentSchemeName);
        }
        enableKafkaConsumersOffsetManagement =
                clusterProps.getBoolean(VeniceConfigService.ENABLE_KAFKA_CONSUMER_OFFSET_MANAGEMENT, false);
        if (enableKafkaConsumersOffsetManagement) {
            offsetManagerType = clusterProps.getString(VeniceConfigService.OFFSET_MANAGER_TYPE, "bdb"); // Default "bdb"
            offsetDatabasePath = clusterProps.getString(VeniceConfigService.OFFSET_DATA_BASE_PATH,
                    System.getProperty("java.io.tmpdir") + File.separator + OffsetManager.OFFSETS_STORE_NAME);
            offsetManagerFlushIntervalMs = clusterProps.getLong(VeniceConfigService.OFFSET_MANAGER_FLUSH_INTERVAL_MS, 10000); // 10 sec default
        }
        enableConsumptionAcksForAzkabanJobs = clusterProps.getBoolean(VeniceConfigService.ENABLE_CONSUMPTION_ACKS_FOR_AZKABAN_JOBS, false);
        if(enableConsumptionAcksForAzkabanJobs){
            kafkaConsumptionAcksBrokerUrl = clusterProps.getString(VeniceConfigService.KAFKA_CONSUMPTION_ACKS_BROKER_URL);
            if(kafkaConsumptionAcksBrokerUrl.isEmpty()){
                throw new ConfigurationException("The kafka broker url cannot be empty when consumption acknowledgement is enabled!");
            }
        }
    }

    public String getClusterName() {
        return clusterName;
    }

    public int getStorageNodeCount() {
        return storageNodeCount;
    }

    public String getPartitionNodeAssignmentSchemeClassName() {
        return partitionNodeAssignmentSchemeClassMap.get(partitionNodeAssignmentSchemeName);
    }

    public boolean isEnableKafkaConsumersOffsetManagement() {
        return enableKafkaConsumersOffsetManagement;
    }

    public String getOffsetManagerType() {
        return offsetManagerType;
    }

    public String getOffsetDatabasePath() {
        return offsetDatabasePath;
    }

    public long getOffsetManagerFlushIntervalMs() {
        return offsetManagerFlushIntervalMs;
    }

    public boolean isEnableConsumptionAcksForAzkabanJobs() {
        return enableConsumptionAcksForAzkabanJobs;
    }

    public void setEnableConsumptionAcksForAzkabanJobs(boolean enableConsumptionAcksForAzkabanJobs) {
        this.enableConsumptionAcksForAzkabanJobs = enableConsumptionAcksForAzkabanJobs;
    }

    public String getKafkaConsumptionAcksBrokerUrl() {
        return kafkaConsumptionAcksBrokerUrl;
    }

    public void setKafkaConsumptionAcksBrokerUrl(String kafkaConsumptionAcksBrokerUrl) {
        this.kafkaConsumptionAcksBrokerUrl = kafkaConsumptionAcksBrokerUrl;
    }
}
