package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.fastclient.transport.TransportClientResponseForRoute;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;


/**
 * Keep track of the progress of a batch get request . This includes tracking
 * all the scatter requests and utilities to gather responses.
 * @param <K> Key type
 * @param <V> Value type
 */
public class BatchGetRequestContext<K, V> extends RequestContext {
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

  // True if long tail retry was triggered
  boolean longTailRetryTriggered;
  // Number of keys triggered in the retry request
  int numberOfKeysSentInRetryRequest;
  // Number of keys that were successfully resolved in retry request
  AtomicInteger numberOfKeysCompletedInOriginalRequest;
  AtomicInteger numberOfKeysCompletedInRetryRequest;

  BatchGetRequestContext() {
    routeRequests = new VeniceConcurrentHashMap<>();
    firstRequestSentTS = new AtomicLong(-1);
    firstResponseReceivedTS = new AtomicLong(-1);
    partialResponseException = new AtomicReference<>();
    routesForPartition = new HashMap<>();
    longTailRetryTriggered = false;
    numberOfKeysSentInRetryRequest = 0;
    numberOfKeysCompletedInOriginalRequest = new AtomicInteger();
    numberOfKeysCompletedInRetryRequest = new AtomicInteger();
  }

  void addKey(String route, K key, int partitionId) {
    Validate.notNull(route);
    routeRequests.computeIfAbsent(route, r -> new RouteRequestContext<>()).addKeyInfo(key, partitionId);
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
    routeRequests.get(response.getRouteId()).setComplete(response);
  }

  void markCompleteExceptionally(TransportClientResponseForRoute response, Throwable exception) {
    validateResponseRoute(response);
    Validate.notNull(exception);
    routeRequests.get(response.getRouteId()).setCompleteExceptionally(exception);
    partialResponseException.compareAndSet(null, exception);
  }

  void complete() {

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
    decompressionTime = LatencyUtils.convertLatencyFromNSToMS(decompressionTimeNS);
    responseDeserializationTime =
        LatencyUtils.convertLatencyFromNSToMS(responseDeserializationTimeNS + recordDeserializationTimeNS);
    requestSerializationTime = LatencyUtils.convertLatencyFromNSToMS(requestSerializationTimeNS);

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

  void recordRequestSerializationTime(String routeId, long latencyInNS) {
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

  List<CompletableFuture<TransportClientResponseForRoute>> getAllRouteFutures() {
    return routeRequests.values().stream().map(rrc -> rrc.routeRequestCompletionFuture).collect(Collectors.toList());
  }

  Optional<Throwable> getPartialResponseException() {
    return Optional.ofNullable(partialResponseException.get());
  }

  void setPartialResponseException(Throwable exception) {
    this.partialResponseException.compareAndSet(null, exception);
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

  /**
   * Utility class to keep track of a single request to a route
   * Each context tracks the keys that were requested along with metadata used to
   * collate responses
   * @param <K>
   */
  private static class RouteRequestContext<K> {
    List<KeyInfo<K>> keysRequested = new ArrayList<>();
    CompletableFuture<TransportClientResponseForRoute> routeRequestCompletionFuture = new CompletableFuture<>();

    AtomicLong decompressionTime = new AtomicLong();
    AtomicLong responseDeserializationTime = new AtomicLong();
    AtomicLong recordDeserializationTime = new AtomicLong();
    AtomicLong requestSerializationTime = new AtomicLong();

    void addKeyInfo(K key, int partitionId) {
      keysRequested.add(new KeyInfo<>(key, partitionId));
    }

    void setComplete(TransportClientResponseForRoute response) {
      routeRequestCompletionFuture.complete(response);
    }

    void setCompleteExceptionally(Throwable exception) {
      routeRequestCompletionFuture.completeExceptionally(exception);
    }
  }

  /**
   * represents a key requested on a route and tracks info related to it but specific to the route
   * @param <K>
   */
  public static class KeyInfo<K> {
    private final K key;
    private final int partitionId;

    public KeyInfo(K key, int partitionId) {
      this.key = key;
      this.partitionId = partitionId;
    }

    public K getKey() {
      return key;
    }

    public int getPartitionId() {
      return partitionId;
    }
  }
}
