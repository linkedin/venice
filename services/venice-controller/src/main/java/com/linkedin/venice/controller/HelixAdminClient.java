package com.linkedin.venice.controller;

import java.util.List;
import java.util.Map;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.RESTConfig;


/**
 * Interface and wrapper for Helix related admin operations needed by Venice when running Helix as a service.
 */
public interface HelixAdminClient {
  /**
   * Check if the Venice controller cluster is created and configured.
   * @return true or false.
   */
  boolean isVeniceControllerClusterCreated();

  /**
   * Check if the given Venice storage cluster is created and configured.
   * @param clusterName of the Venice cluster.
   * @return true or false.
   */
  boolean isVeniceStorageClusterCreated(String clusterName);

  /**
   * Create and configure the Venice controller cluster.
   */
  void createVeniceControllerCluster();

  /**
   * Create and configure the Venice storage cluster.
   * @param clusterName of the Venice storage cluster.
   * @param clusterConfig {@link ClusterConfig} for the new cluster.
   * @param restConfig {@link RESTConfig} for the new cluster.
   */
  void createVeniceStorageCluster(String clusterName, ClusterConfig clusterConfig, RESTConfig restConfig);

  /**
   * Check if the given Venice storage cluster's cluster resource is in the Venice controller cluster.
   * @param clusterName of the Venice storage cluster.
   * @return true or false.
   */
  boolean isVeniceStorageClusterInControllerCluster(String clusterName);

  /**
   * Add the given Venice storage cluster's cluster resource to the controller cluster.
   * @param clusterName of the Venice storage cluster.
   */
  void addVeniceStorageClusterToControllerCluster(String clusterName);

  /**
   * Check if the grand cluster managed by HaaS controllers is aware of the given cluster.
   * @param clusterName of the cluster.
   * @return true or false.
   */
  boolean isClusterInGrandCluster(String clusterName);

  /**
   * Add the specified cluster as a resource to the grand cluster to be managed by HaaS controllers.
   * @param clusterName of the cluster to be added as a resource to the grand cluster.
   */
  void addClusterToGrandCluster(String clusterName);

  /**
   * Update some Helix cluster properties for the given cluster.
   * @param clusterName of the cluster to be updated.
   * @param clusterConfig {@link ClusterConfig} for the new cluster.
   */
  void updateClusterConfigs(String clusterName, ClusterConfig clusterConfig);

  /**
   * Update some Helix cluster properties for the given cluster.
   * @param clusterName of the cluster to be updated.
   * @param restConfig {@link RESTConfig} for the new cluster.
   */
  void updateRESTConfigs(String clusterName, RESTConfig restConfig);

  /**
   * Disable or enable a list of partitions on an instance.
   */
  void enablePartition(
      boolean enabled,
      String clusterName,
      String instanceName,
      String resourceName,
      List<String> partitionNames);

  /**
   * Get a list of instances under a cluster.
   * @return a list of instance names.
   */
  List<String> getInstancesInCluster(String clusterName);

  /**
   * Create resources for a given storage node cluster.
   */
  void createVeniceStorageClusterResources(
      String clusterName,
      String kafkaTopic,
      int numberOfPartition,
      int replicationFactor);

  /**
   * Check if a resource exists in a cluster by checking its ideal state.
   */
  boolean containsResource(String clusterName, String resourceName);

  /**
   * Drop a resource from a cluster.
   */
  void dropResource(String clusterName, String resourceName);

  /**
   * Drop a storage node instance from the given cluster.
   */
  void dropStorageInstance(String clusterName, String instanceName);

  /**
   * Returns a list of disabled partitions in an instance.
   */
  Map<String, List<String>> getDisabledPartitionsMap(String clusterName, String instanceName);

  /**
   * Reset a list of partitions in error state for an instance.
   * <p>
   * The partitions are assumed to be in error state and reset will bring them from error
   * to initial state. An error to initial state transition is required for reset.
   */
  void resetPartition(String clusterName, String instanceName, String resourceName, List<String> partitionNames);

  /**
   * Release resources.
   */
  void close();

  /**
   * Manually enable maintenance mode. To be called by the REST client that accepts KV mappings as
   * the payload.
   */
  void manuallyEnableMaintenanceMode(
      String clusterName,
      boolean enabled,
      String reason,
      Map<String, String> customFields);

  /**
   * Set the instanceOperation of and instance with {@link InstanceConstants.InstanceOperation}.
   *
   * @param clusterName       The cluster name
   * @param instanceName      The instance name
   * @param instanceOperation The instance operation type
   * @param reason            The reason for the operation
   */
  void setInstanceOperation(
      String clusterName,
      String instanceName,
      InstanceConstants.InstanceOperation instanceOperation,
      String reason);

  IdealState getResourceIdealState(String clusterName, String resourceName);

  void updateIdealState(String clusterName, String resourceName, IdealState idealState);
}
