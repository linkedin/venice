package com.linkedin.venice.fastclient;

import com.linkedin.restli.common.HttpStatus;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AppTimeOutTrackingCompletableFuture;
import com.linkedin.venice.client.store.ComputeGenericRecord;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.fastclient.meta.InstanceHealthMonitor;
import com.linkedin.venice.fastclient.stats.ClusterStats;
import com.linkedin.venice.fastclient.stats.FastClientStats;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Time;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is in charge of all the metric emissions per request.
 */
public class StatsAvroGenericStoreClient<K, V> extends DelegatingAvroStoreClient<K, V> {
  private static final Logger LOGGER = LogManager.getLogger(StatsAvroGenericStoreClient.class);
  private static final int TIMEOUT_IN_SECOND = 5;

  private final FastClientStats clientStatsForSingleGet;
  private final FastClientStats clientStatsForStreamingBatchGet;
  private final FastClientStats clientStatsForStreamingCompute;
  private final ClusterStats clusterStats;

  public StatsAvroGenericStoreClient(InternalAvroStoreClient<K, V> delegate, ClientConfig clientConfig) {
    super(delegate, clientConfig);
    this.clientStatsForSingleGet = clientConfig.getStats(RequestType.SINGLE_GET);
    this.clientStatsForStreamingBatchGet = clientConfig.getStats(RequestType.MULTI_GET_STREAMING);
    this.clientStatsForStreamingCompute = clientConfig.getStats(RequestType.COMPUTE_STREAMING);
    this.clusterStats = clientConfig.getClusterStats();
  }

  @Override
  protected CompletableFuture<V> get(GetRequestContext requestContext, K key) throws VeniceClientException {
    long startTimeInNS = System.nanoTime();
    CompletableFuture<V> innerFuture = super.get(requestContext, key);
    return recordMetrics(requestContext, 1, innerFuture, startTimeInNS, clientStatsForSingleGet);
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
    recordMetrics(requestContext, keys.size(), statFuture, startTimeInNS, clientStatsForStreamingBatchGet);
  }

  @Override
  public void compute(
      ComputeRequestContext<K, V> requestContext,
      ComputeRequestWrapper computeRequestWrapper,
      Set<K> keys,
      Schema resultSchema,
      StreamingCallback<K, ComputeGenericRecord> callback,
      long preRequestTimeInNS) throws VeniceClientException {
    long startTimeInNS = System.nanoTime();
    CompletableFuture<Void> statFuture = new CompletableFuture<>();
    super.compute(
        requestContext,
        computeRequestWrapper,
        keys,
        resultSchema,
        new StatTrackingStreamingCallBack<>(callback, statFuture, requestContext),
        preRequestTimeInNS);
    recordMetrics(requestContext, keys.size(), statFuture, startTimeInNS, clientStatsForStreamingCompute);
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

  /**
   * Metrics are incremented after one of the below cases
   * 1. request is complete or
   * 2. exception is thrown or
   * 3. routingLeakedRequestCleanupThresholdMS is elapsed: In case of streamingBatchGet.get(timeout) returning
   *            partial response and this timeout happens after than and before the full response is returned,
   *            it will still raise a silent exception leading to the request being considered an unhealthy request.
   */
  private <R> CompletableFuture<R> recordRequestMetrics(
      RequestContext requestContext,
      int numberOfKeys,
      CompletableFuture<R> innerFuture,
      long startTimeInNS,
      FastClientStats clientStats) {
    return innerFuture.handle((value, throwable) -> {
      double latency = LatencyUtils.getLatencyInMS(startTimeInNS);
      clientStats.recordRequestKeyCount(numberOfKeys);

      boolean exceptionReceived = false;
      if (throwable != null) {
        /**
         * If the request (both original and retry) failed due to an exception, its
         * considered an unhealthy request: so only incrementing a subset of metrics.
         */
        exceptionReceived = true;
      }

      if (exceptionReceived || (latency > TIMEOUT_IN_SECOND * Time.MS_PER_SECOND)) {
        clientStats.recordUnhealthyRequest();
        clientStats.recordUnhealthyLatency(latency);
      } else {
        clientStats.recordHealthyRequest();
        clientStats.recordHealthyLatency(latency);
      }

      if (requestContext.noAvailableReplica) {
        clientStats.recordNoAvailableReplicaRequest();
      }

      if (!exceptionReceived) {
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
      }

      if (requestContext instanceof GetRequestContext) {
        GetRequestContext getRequestContext = (GetRequestContext) requestContext;

        if (getRequestContext.longTailRetryRequestTriggered) {
          clientStats.recordLongTailRetryRequest();
          clientStats.recordRetryRequestKeyCount(1);
        }
        if (getRequestContext.errorRetryRequestTriggered) {
          clientStats.recordErrorRetryRequest();
          clientStats.recordRetryRequestKeyCount(1);
        }
        if (!exceptionReceived) {
          if (getRequestContext.retryWin) {
            clientStats.recordRetryRequestWin();
            clientStats.recordRetryRequestSuccessKeyCount(1);
          }
        }
      } else if (requestContext instanceof MultiKeyRequestContext) {
        // MultiKeyRequestContext is the superclass for ComputeRequestContext and BatchGetRequestContext
        MultiKeyRequestContext<K, V> multiKeyRequestContext = (MultiKeyRequestContext<K, V>) requestContext;
        if (multiKeyRequestContext.longTailRetryTriggered) {
          clientStats.recordLongTailRetryRequest();
          clientStats.recordRetryRequestKeyCount(multiKeyRequestContext.numberOfKeysSentInRetryRequest);
          if (!exceptionReceived) {
            clientStats
                .recordRetryRequestSuccessKeyCount(multiKeyRequestContext.numberOfKeysCompletedInRetryRequest.get());
            if (multiKeyRequestContext.numberOfKeysCompletedInRetryRequest.get() > 0) {
              clientStats.recordRetryRequestWin();
            }
          }
        }
      }

      if (exceptionReceived) {
        // throw an exception after incrementing some error related metrics
        if (throwable instanceof VeniceClientException) {
          throw (VeniceClientException) throwable;
        } else {
          throw new VeniceClientException(throwable);
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
              /* Check {@link InstanceHealthMonitor#trackHealthBasedOnRequestToInstance} to understand this special http status. */
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
    private final MultiKeyRequestContext requestContext;

    StatTrackingStreamingCallBack(
        StreamingCallback<K, V> callback,
        CompletableFuture<Void> statFuture,
        MultiKeyRequestContext requestContext) {
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
