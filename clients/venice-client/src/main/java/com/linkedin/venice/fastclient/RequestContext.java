package com.linkedin.venice.fastclient;

import com.linkedin.venice.fastclient.meta.InstanceHealthMonitor;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * This class is used to include all the intermediate fields required for the communication between the different tiers.
 */
public abstract class RequestContext {
  int currentVersion = -1;
  boolean noAvailableReplica = false;

  double decompressionTime = -1;
  double responseDeserializationTime = -1;
  double requestSerializationTime = -1;
  double requestSubmissionToResponseHandlingTime = -1;

  long requestSentTimestampNS = -1;

  // Keeping track for successful keys for the request.
  AtomicInteger successRequestKeyCount = new AtomicInteger(0);

  InstanceHealthMonitor instanceHealthMonitor = null;

  // Tracking per-route request completion
  Map<String, CompletableFuture<Integer>> routeRequestMap = new VeniceConcurrentHashMap<>();

  String serverClusterName;

  Set<Integer> partitionsWithNoAvailableReplica = new ConcurrentSkipListSet<>();

  CompletableFuture resultFuture;

  long requestId = -1;
  /**
   * The Helix Group id selected for the current request, and it is important to set the initial value to be -1 since
   * the first valid group id is 0.
   *
   * Also, this field is also used to avoid the group id assigned to the original request when handling the retry request.
   * Check {@link com.linkedin.venice.fastclient.meta.AbstractStoreMetadata#routeRequest} for more details.
   */
  int helixGroupId = -1;

  boolean retryRequest = false;
  boolean requestRejectedByLoadController = false;

  double requestRejectionRatio = 0;

  public RequestContext() {
  }

  public abstract RequestType getRequestType();

  public void setServerClusterName(String serverClusterName) {
    this.serverClusterName = serverClusterName;
  }

  public String getServerClusterName() {
    return serverClusterName;
  }

  public void setInstanceHealthMonitor(InstanceHealthMonitor instanceHealthMonitor) {
    this.instanceHealthMonitor = instanceHealthMonitor;
  }

  public InstanceHealthMonitor getInstanceHealthMonitor() {
    return instanceHealthMonitor;
  }

  public boolean isRetryRequest() {
    return retryRequest;
  }

  public void setRetryRequest(boolean retryRequest) {
    this.retryRequest = retryRequest;
  }

  public CompletableFuture getResultFuture() {
    return resultFuture;
  }

  public void setResultFuture(CompletableFuture resultFuture) {
    this.resultFuture = resultFuture;
  }

  public void setRequestId(long requestId) {
    this.requestId = requestId;
  }

  public long getRequestId() {
    return requestId;
  }

  public int getHelixGroupId() {
    return helixGroupId;
  }

  public void setHelixGroupId(int helixGroupId) {
    this.helixGroupId = helixGroupId;
  }

  public void addNonAvailableReplicaPartition(int partitionId) {
    noAvailableReplica = true;
    partitionsWithNoAvailableReplica.add(partitionId);
  }

  public Set<Integer> getNonAvailableReplicaPartitions() {
    return partitionsWithNoAvailableReplica;
  }

  public int getCurrentVersion() {
    return currentVersion;
  }

  public void setCurrentVersion(int currentVersion) {
    this.currentVersion = currentVersion;
  }

  public Map<String, CompletableFuture<Integer>> getRouteRequestMap() {
    return routeRequestMap;
  }

  public boolean hasNonAvailablePartition() {
    return !partitionsWithNoAvailableReplica.isEmpty();
  }

  public void setRequestSerializationTime(double latency) {
    this.requestSerializationTime = latency;
  }
}
