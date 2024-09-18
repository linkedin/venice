package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.fastclient.transport.TransportClientResponseForRoute;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.Validate;


/**
 * Keep track of the progress of a multi-key requests - batch get and compute. This includes tracking all the scatter
 * requests and utilities to gather responses.
 * @param <K> Key type
 * @param <V> Value type
 */
public abstract class MultiKeyRequestContext<K, V> extends RequestContext {
  /**
   * Tracks routes and corresponding request contexts
   */
  private final Map<String, RouteRequestContext<K>> routeRequests;

  /**
   * For synchronized access to timestamps tracking request and response
   */
  private final AtomicLong firstRequestSentTS;
  private final AtomicLong firstResponseReceivedTS;
  private final AtomicReference<Throwable> partialResponseException;

  private Map<Integer, Set<String>> routesForPartition;

  String requestUri;
  String resourceName;

  final int numKeysInRequest;
  AtomicInteger numKeysCompleted;
  RetryContext retryContext;
  final boolean isPartialSuccessAllowed;
  private boolean completed;

  private int fanoutSize;

  MultiKeyRequestContext(int numKeysInRequest, boolean isPartialSuccessAllowed) {
    this.routeRequests = new VeniceConcurrentHashMap<>();
    this.firstRequestSentTS = new AtomicLong(-1);
    this.firstResponseReceivedTS = new AtomicLong(-1);
    this.partialResponseException = new AtomicReference<>();
    this.routesForPartition = new HashMap<>();
    this.numKeysInRequest = numKeysInRequest;
    this.numKeysCompleted = new AtomicInteger();
    this.retryContext = null;
    this.isPartialSuccessAllowed = isPartialSuccessAllowed;
    this.completed = false;
  }

  void addKey(String route, K key, byte[] serializedKey, int partitionId) {
    Validate.notNull(route);
    routeRequests.computeIfAbsent(route, r -> new RouteRequestContext<>()).addKeyInfo(key, serializedKey, partitionId);
    routesForPartition.computeIfAbsent(partitionId, (k) -> new HashSet<>()).add(route);
  }

  Set<String> getRoutes() {
    return routeRequests.keySet();
  }

  List<KeyInfo<K>> keysForRoutes(String route) {
    Validate.notNull(route);
    return routeRequests.get(route).keysRequested;
  }

  void markComplete(TransportClientResponseForRoute response) {
    validateResponseRoute(response);
  }

  void markCompleteExceptionally(TransportClientResponseForRoute response, Throwable exception) {
    validateResponseRoute(response);
    Validate.notNull(exception);
    partialResponseException.compareAndSet(null, exception);
  }

  void complete() {
    if (completed) {
      return;
    }
    completed = true;
    // Roll up route stats into overall stats
    long decompressionTimeNS = 0;
    long responseDeserializationTimeNS = 0;
    long recordDeserializationTimeNS = 0;
    long requestSerializationTimeNS = 0;
    for (RouteRequestContext<K> rrc: routeRequests.values()) {
      decompressionTimeNS += rrc.decompressionTime.get();
      responseDeserializationTimeNS += rrc.responseDeserializationTime.get();
      recordDeserializationTimeNS += rrc.recordDeserializationTime.get();
      requestSerializationTimeNS += rrc.requestSerializationTime.get();
    }
    decompressionTime = LatencyUtils.convertNSToMS(decompressionTimeNS);
    responseDeserializationTime =
        LatencyUtils.convertNSToMS(responseDeserializationTimeNS + recordDeserializationTimeNS);
    requestSerializationTime = LatencyUtils.convertNSToMS(requestSerializationTimeNS);

    if (firstRequestSentTS.get() != -1 && firstResponseReceivedTS.get() != -1) {
      requestSubmissionToResponseHandlingTime = firstResponseReceivedTS.get() - firstRequestSentTS.get();
    }
  }

  void recordDecompressionTime(String routeId, long latencyInNS) {
    Validate.notNull(routeId);
    routeRequests.get(routeId).decompressionTime.addAndGet(latencyInNS);
  }

  void recordRequestDeserializationTime(String routeId, long latencyInNS) {
    Validate.notNull(routeId);
    routeRequests.get(routeId).responseDeserializationTime.addAndGet(latencyInNS);
  }

  void recordRecordDeserializationTime(String routeId, long latencyInNS) {
    Validate.notNull(routeId);
    routeRequests.get(routeId).recordDeserializationTime.addAndGet(latencyInNS);
  }

  public void recordRequestSerializationTime(String routeId, long latencyInNS) {
    Validate.notNull(routeId);
    routeRequests.get(routeId).requestSerializationTime.addAndGet(latencyInNS);
  }

  void recordRequestSentTimeStamp(String routeId) {
    Validate.notNull(routeId);
    long requestSentTS = System.nanoTime();
    if (firstRequestSentTS.compareAndSet(-1, requestSentTS)) {
      requestSentTimestampNS = firstRequestSentTS.get();
    }
  }

  void recordRequestSubmissionToResponseHandlingTime(String routeId) {
    Validate.notNull(routeId);
    firstResponseReceivedTS.compareAndSet(-1, System.nanoTime());
  }

  Optional<Throwable> getPartialResponseException() {
    return Optional.ofNullable(partialResponseException.get());
  }

  void setPartialResponseExceptionIfNull(Throwable exception) {
    this.partialResponseException.compareAndSet(null, exception);
  }

  void setPartialResponseException(Throwable exception) {
    this.partialResponseException.set(exception);
  }

  /* Utility validation methods */

  private void validateResponseRoute(TransportClientResponseForRoute response) {
    if (response == null) {
      throw new VeniceClientException(new IllegalArgumentException("Response object cannot be null"));
    } else if (response.getRouteId() == null) {
      throw new VeniceClientException(new IllegalArgumentException("Response route cannot be null"));
    } else if (!routeRequests.containsKey(response.getRouteId())) {
      throw new VeniceClientException(
          new IllegalStateException(String.format("Unexpected route %s", response.getRouteId())));
    }
  }

  public Map<Integer, Set<String>> getRoutesForPartitionMapping() {
    return routesForPartition;
  }

  public void setRoutesForPartitionMapping(Map<Integer, Set<String>> routesForPartition) {
    this.routesForPartition = routesForPartition;
  }

  public void setFanoutSize(int fanoutSize) {
    this.fanoutSize = fanoutSize;
  }

  public int getFanoutSize() {
    return fanoutSize;
  }

  public void setResourceName(String resourceName) {
    this.resourceName = resourceName;
  }

  public String getResourceName() {
    return resourceName;
  }

  public abstract String computeRequestUri();

  public abstract QueryAction getQueryAction();

  /**
   * Utility class to keep track of a single request to a route
   * Each context tracks the keys that were requested along with metadata used to
   * collate responses
   * @param <K>
   */
  private static class RouteRequestContext<K> {
    List<KeyInfo<K>> keysRequested = new ArrayList<>();

    AtomicLong decompressionTime = new AtomicLong();
    AtomicLong responseDeserializationTime = new AtomicLong();
    AtomicLong recordDeserializationTime = new AtomicLong();
    AtomicLong requestSerializationTime = new AtomicLong();

    void addKeyInfo(K key, byte[] serializedKey, int partitionId) {
      keysRequested.add(new KeyInfo<>(key, serializedKey, partitionId));
    }
  }

  /**
   * represents a key requested on a route and tracks info related to it but specific to the route
   * @param <K>
   */
  public static class KeyInfo<K> {
    private final K key;
    private final byte[] serializedKey;
    private final int partitionId;

    public KeyInfo(K key, byte[] serializedKey, int partitionId) {
      this.key = key;
      this.serializedKey = serializedKey;
      this.partitionId = partitionId;
    }

    public K getKey() {
      return key;
    }

    public byte[] getSerializedKey() {
      return serializedKey;
    }

    public int getPartitionId() {
      return partitionId;
    }
  }

  static class RetryContext<K, V> {
    MultiKeyRequestContext<K, V> retryRequestContext;

    RetryContext() {
      retryRequestContext = null;
    }
  }

  private boolean isCompletedWithoutErrors() {
    return completed && partialResponseException.get() == null;
  }

  boolean isCompletedSuccessfullyWithPartialResponse() {
    return completed && (isPartialSuccessAllowed && partialResponseException.get() != null);
  }

  boolean isCompletedAcceptably() {
    if (!completed) {
      return false;
    }

    if (isCompletedWithoutErrors()) {
      return true;
    }

    if (isPartialSuccessAllowed) {
      return isCompletedSuccessfullyWithPartialResponse();
    }

    return false;
  }
}
