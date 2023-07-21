package com.linkedin.venice.fastclient;

import com.linkedin.restli.common.HttpStatus;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AppTimeOutTrackingCompletableFuture;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.streaming.VeniceResponseMap;
import com.linkedin.venice.fastclient.meta.InstanceHealthMonitor;
import com.linkedin.venice.fastclient.stats.ClusterStats;
import com.linkedin.venice.fastclient.stats.FastClientStats;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Time;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is in charge of all the metric emissions per request.
 */
public class StatsAvroGenericStoreClient<K, V> extends DelegatingAvroStoreClient<K, V> {
  private static final Logger LOGGER = LogManager.getLogger(StatsAvroGenericStoreClient.class);
  private static final int TIMEOUT_IN_SECOND = 5;

  private final FastClientStats clientStatsForSingleGet;
  private final FastClientStats clientStatsForBatchGet;
  private final ClusterStats clusterStats;

  private final int maxAllowedKeyCntInBatchGetReq;
  private final boolean useStreamingBatchGetAsDefault;

  public StatsAvroGenericStoreClient(InternalAvroStoreClient<K, V> delegate, ClientConfig clientConfig) {
    super(delegate);
    this.clientStatsForSingleGet = clientConfig.getStats(RequestType.SINGLE_GET);
    this.clientStatsForBatchGet = clientConfig.getStats(RequestType.MULTI_GET);
    this.clusterStats = clientConfig.getClusterStats();
    this.maxAllowedKeyCntInBatchGetReq = clientConfig.getMaxAllowedKeyCntInBatchGetReq();
    this.useStreamingBatchGetAsDefault = clientConfig.useStreamingBatchGetAsDefault();
  }

  @Override
  protected CompletableFuture<V> get(GetRequestContext requestContext, K key) throws VeniceClientException {
    long startTimeInNS = System.nanoTime();
    CompletableFuture<V> innerFuture = super.get(requestContext, key);
    return recordMetrics(requestContext, 1, innerFuture, startTimeInNS, clientStatsForSingleGet);
  }

  protected CompletableFuture<Map<K, V>> batchGet(BatchGetRequestContext<K, V> requestContext, Set<K> keys)
      throws VeniceClientException {
    return this.useStreamingBatchGetAsDefault
        ? batchGetUsingStreamingBatchGet(requestContext, keys)
        : batchGetUsingSingleGet(keys);
  }

  protected CompletableFuture<Map<K, V>> batchGetUsingStreamingBatchGet(
      BatchGetRequestContext<K, V> requestContext,
      Set<K> keys) throws VeniceClientException {
    long startTimeInNS = System.nanoTime();

    CompletableFuture<Map<K, V>> innerFuture = super.batchGet(requestContext, keys);
    return recordMetrics(requestContext, keys.size(), innerFuture, startTimeInNS, clientStatsForBatchGet);
  }

  /**
   *  Leverage single-get implementation here:
   *  1. Looping through all keys and call get() for each of the keys
   *  2. Collect the replies and pass it to the caller
   *
   *  Transient change to support {@link ClientConfig#useStreamingBatchGetAsDefault}
   */
  protected CompletableFuture<Map<K, V>> batchGetUsingSingleGet(Set<K> keys) throws VeniceClientException {
    if (keys.isEmpty()) {
      return CompletableFuture.completedFuture(Collections.emptyMap());
    }
    int keyCnt = keys.size();
    if (keyCnt > maxAllowedKeyCntInBatchGetReq) {
      throw new VeniceClientException(
          "Currently, the max allowed key count in a batch-get request: " + maxAllowedKeyCntInBatchGetReq
              + ", but received: " + keyCnt);
    }
    CompletableFuture<Map<K, V>> resultFuture = new CompletableFuture<>();
    Map<K, CompletableFuture<V>> valueFutures = new HashMap<>();
    keys.forEach(k -> valueFutures.put(k, (get(k))));
    CompletableFuture.allOf(valueFutures.values().toArray(new CompletableFuture[keyCnt]))
        .whenComplete(((aVoid, throwable) -> {
          if (throwable != null) {
            resultFuture.completeExceptionally(throwable);
          }
          Map<K, V> resultMap = new HashMap<>();
          valueFutures.forEach((k, f) -> {
            try {
              resultMap.put(k, f.get());
            } catch (Exception e) {
              resultFuture.completeExceptionally(
                  new VeniceClientException("Failed to complete future for key: " + k.toString(), e));
            }
          });
          resultFuture.complete(resultMap);
        }));

    return resultFuture;
  }

  @Override
  protected void streamingBatchGet(
      BatchGetRequestContext<K, V> requestContext,
      Set<K> keys,
      StreamingCallback<K, V> callback) {
    long startTimeInNS = System.nanoTime();
    CompletableFuture<Void> statFuture = new CompletableFuture<>();
    super.streamingBatchGet(
        requestContext,
        keys,
        new StatTrackingStreamingCallBack<>(callback, statFuture, requestContext));
    recordMetrics(requestContext, keys.size(), statFuture, startTimeInNS, clientStatsForBatchGet);
  }

  @Override
  protected CompletableFuture<VeniceResponseMap<K, V>> streamingBatchGet(
      BatchGetRequestContext<K, V> requestContext,
      Set<K> keys) {
    long startTimeInNS = System.nanoTime();
    CompletableFuture<VeniceResponseMap<K, V>> innerFuture = super.streamingBatchGet(requestContext, keys);
    return recordMetrics(requestContext, keys.size(), innerFuture, startTimeInNS, clientStatsForBatchGet);
  }

  private <R> CompletableFuture<R> recordMetrics(
      RequestContext requestContext,
      int numberOfKeys,
      CompletableFuture<R> innerFuture,
      long startTimeInNS,
      FastClientStats clientStats) {
    CompletableFuture<R> statFuture =
        recordRequestMetrics(requestContext, numberOfKeys, innerFuture, startTimeInNS, clientStats);
    // Record per replica metric
    recordPerRouteMetrics(requestContext, clientStats);

    return AppTimeOutTrackingCompletableFuture.track(statFuture, clientStats);
  }

  private <R> CompletableFuture<R> recordRequestMetrics(
      RequestContext requestContext,
      int numberOfKeys,
      CompletableFuture<R> innerFuture,
      long startTimeInNS,
      FastClientStats clientStats) {

    return innerFuture.handle((value, throwable) -> {
      double latency = LatencyUtils.getLatencyInMS(startTimeInNS);
      clientStats.recordRequestKeyCount(numberOfKeys);

      if (throwable != null) {
        /**
         * If the request (both original and retry) failed due to an exception,
         * just increment the unhealthy counters as the other counters might not
         * be useful, especially when the counters are currently common for
         * healthy/unhealthy requests. No new unhealthy specific error
         * counters for now apart from the below 2.
         */
        clientStats.recordUnhealthyRequest();
        clientStats.recordUnhealthyLatency(latency);
        if (throwable instanceof VeniceClientException) {
          throw (VeniceClientException) throwable;
        } else {
          throw new VeniceClientException(throwable);
        }
      }

      if (latency > TIMEOUT_IN_SECOND * Time.MS_PER_SECOND) {
        clientStats.recordUnhealthyRequest();
        clientStats.recordUnhealthyLatency(latency);
      } else {
        clientStats.recordHealthyRequest();
        clientStats.recordHealthyLatency(latency);
      }

      if (requestContext.noAvailableReplica) {
        clientStats.recordNoAvailableReplicaRequest();
      }

      // Record additional metrics
      if (requestContext.requestSerializationTime > 0) {
        clientStats.recordRequestSerializationTime(requestContext.requestSerializationTime);
      }
      if (requestContext.requestSubmissionToResponseHandlingTime > 0) {
        clientStats
            .recordRequestSubmissionToResponseHandlingTime(requestContext.requestSubmissionToResponseHandlingTime);
      }
      if (requestContext.decompressionTime > 0) {
        clientStats.recordResponseDecompressionTime(requestContext.decompressionTime);
      }
      if (requestContext.responseDeserializationTime > 0) {
        clientStats.recordResponseDeserializationTime(requestContext.responseDeserializationTime);
      }
      clientStats.recordSuccessRequestKeyCount(requestContext.successRequestKeyCount.get());

      /**
       * Record some single-get specific metrics, and these metrics should be applied to other types of requests once
       * the corresponding features are ready.
        */
      if (requestContext instanceof GetRequestContext) {
        GetRequestContext getRequestContext = (GetRequestContext) requestContext;

        if (getRequestContext.longTailRetryRequestTriggered) {
          clientStats.recordLongTailRetryRequest();
        }
        if (getRequestContext.errorRetryRequestTriggered) {
          clientStats.recordErrorRetryRequest();
        }
        if (getRequestContext.retryWin) {
          clientStats.recordRetryRequestWin();
        }
      } else if (requestContext instanceof BatchGetRequestContext) {
        BatchGetRequestContext<K, V> batchGetRequestContext = (BatchGetRequestContext<K, V>) requestContext;
        if (batchGetRequestContext.longTailRetryTriggered) {
          clientStats.recordLongTailRetryRequest();
          clientStats.recordRetryRequestKeyCount(batchGetRequestContext.numberOfKeysSentInRetryRequest);
          clientStats
              .recordRetryRequestSuccessKeyCount(batchGetRequestContext.numberOfKeysCompletedInRetryRequest.get());
        }
      }

      return value;
    });
  }

  private void recordPerRouteMetrics(RequestContext requestContext, FastClientStats clientStats) {
    final long requestSentTimestampNS = requestContext.requestSentTimestampNS;
    if (requestSentTimestampNS > 0) {
      Map<String, CompletableFuture<HttpStatus>> replicaRequestFuture = requestContext.routeRequestMap;
      final InstanceHealthMonitor monitor = requestContext.instanceHealthMonitor;
      if (monitor != null) {
        clusterStats.recordBlockedInstanceCount(monitor.getBlockedInstanceCount());
        clusterStats.recordUnhealthyInstanceCount(monitor.getUnhealthyInstanceCount());
      }
      replicaRequestFuture.forEach((instance, future) -> {
        future.whenComplete((status, throwable) -> {
          if (monitor != null) {
            clusterStats.recordPendingRequestCount(instance, monitor.getPendingRequestCounter(instance));
          }

          if (throwable != null) {
            LOGGER.error("Received unexpected exception from replica request future: ", throwable);
            return;
          }
          clientStats.recordRequest(instance);
          clientStats.recordResponseWaitingTime(instance, LatencyUtils.getLatencyInMS(requestSentTimestampNS));
          switch (status) {
            case S_200_OK:
            case S_404_NOT_FOUND:
              clientStats.recordHealthyRequest(instance);
              break;
            case S_429_TOO_MANY_REQUESTS:
              clientStats.recordQuotaExceededRequest(instance);
              break;
            case S_500_INTERNAL_SERVER_ERROR:
              clientStats.recordInternalServerErrorRequest(instance);
              break;
            case S_410_GONE:
              /* Check {@link InstanceHealthMonitor#sendRequestToInstance} to understand this special http status. */
              clientStats.recordLeakedRequest(instance);
              break;
            case S_503_SERVICE_UNAVAILABLE:
              clientStats.recordServiceUnavailableRequest(instance);
              break;
            default:
              clientStats.recordOtherErrorRequest(instance);
          }
        });
      });
    }
  }

  private static class StatTrackingStreamingCallBack<K, V> implements StreamingCallback<K, V> {
    private final StreamingCallback<K, V> inner;
    // This future is completed with a number of keys whose values were successfully received.
    private final CompletableFuture<Void> statFuture;
    private final RequestContext requestContext;

    StatTrackingStreamingCallBack(
        StreamingCallback<K, V> callback,
        CompletableFuture<Void> statFuture,
        RequestContext requestContext) {
      this.inner = callback;
      this.statFuture = statFuture;
      this.requestContext = requestContext;
    }

    @Override
    public void onRecordReceived(K key, V value) {
      if (value != null) {
        requestContext.successRequestKeyCount.incrementAndGet();
      }
      inner.onRecordReceived(key, value);
    }

    @Override
    public void onCompletion(Optional<Exception> exception) {
      if (exception.isPresent()) {
        statFuture.completeExceptionally(exception.get());
      } else {
        statFuture.complete(null);
      }
      inner.onCompletion(exception);
    }
  }
}
