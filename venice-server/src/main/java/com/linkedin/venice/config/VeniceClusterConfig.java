package com.linkedin.venice.config;

import com.google.common.collect.ImmutableMap;
import com.linkedin.venice.partition.ModuloPartitionNodeAssignmentScheme;
import com.linkedin.venice.server.VeniceConfigService;
import com.linkedin.venice.utils.Props;
import java.util.Map;


/**
 * class that maintains config very specific to a Venice cluster
 */
public class VeniceClusterConfig {

  public static final Map<String, String> partitionNodeAssignmentSchemeClassMap =
      ImmutableMap.of("modulo", ModuloPartitionNodeAssignmentScheme.class.getName());

  private String clusterName;
  private int storageNodeCount;
  private String partitionNodeAssignmentSchemeName;

  public VeniceClusterConfig(Props clusterProperties)
      throws Exception {
    checkProperties(clusterProperties);
  }

  protected void checkProperties(Props clusterProps)
      throws Exception {
    clusterName = clusterProps.getString(VeniceConfigService.CLUSTER_NAME);
    storageNodeCount = clusterProps.getInt(VeniceConfigService.STORAGE_NODE_COUNT, 1);     // Default 1
    partitionNodeAssignmentSchemeName = clusterProps
        .getString(VeniceConfigService.PARTITION_NODE_ASSIGNMENT_SCHEME, "modulo"); // Default "modulo" scheme
    if (!partitionNodeAssignmentSchemeClassMap.containsKey(partitionNodeAssignmentSchemeName)) {
      // TODO throw appropriate exception later
      throw new Exception("unknown partition node assignment scheme: " + partitionNodeAssignmentSchemeName);
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
}
