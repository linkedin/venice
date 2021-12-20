package com.linkedin.venice.fastclient;

import com.linkedin.restli.common.HttpStatus;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AppTimeOutTrackingCompletableFuture;
import com.linkedin.venice.fastclient.meta.InstanceHealthMonitor;
import com.linkedin.venice.fastclient.stats.ClientStats;
import com.linkedin.venice.fastclient.stats.ClusterStats;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.utils.LatencyUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.linkedin.venice.client.store.StatTrackingStoreClient.*;


/**
 * This class is in charge of all the metric emissions per request.
 */
public class StatsAvroGenericStoreClient<K, V> extends DelegatingAvroStoreClient<K, V> {
  private static final Logger LOGGER = LogManager.getLogger(StatsAvroGenericStoreClient.class);

  private final ClientStats clientStatsForSingleGet;
  private final ClusterStats clusterStats;

  private final int maxAllowedKeyCntInBatchGetReq;

  public StatsAvroGenericStoreClient(InternalAvroStoreClient<K, V> delegate, ClientConfig clientConfig) {
    super(delegate);
    this.clientStatsForSingleGet = clientConfig.getStats(RequestType.SINGLE_GET);
    this.clusterStats = clientConfig.getClusterStats();
    this.maxAllowedKeyCntInBatchGetReq = clientConfig.getMaxAllowedKeyCntInBatchGetReq();
  }

  @Override
  protected CompletableFuture<V> get(GetRequestContext requestContext, K key) throws VeniceClientException {
    long startTimeInNS = System.nanoTime();
    CompletableFuture<V> innerFuture = super.get(requestContext, key);
    clientStatsForSingleGet.recordRequestKeyCount(1);
    CompletableFuture<V> statFuture = innerFuture.handle(
        (BiFunction<? super V, Throwable, ? extends V>) getStatCallback(clientStatsForSingleGet, startTimeInNS))
        .handle( (value, throwable) -> {
          // Record additional metrics
          if (requestContext.noAvailableReplica) {
            clientStatsForSingleGet.recordNoAvailableReplicaRequest();
          }

          if (throwable != null) {
            if (throwable instanceof VeniceClientException) {
              throw (VeniceClientException)throwable;
            } else {
              throw new VeniceClientException(throwable);
            }
          }
          // Record additional metrics
          if (requestContext.requestSerializationTime > 0) {
            clientStatsForSingleGet.recordRequestSerializationTime(requestContext.requestSerializationTime);
          }
          if (requestContext.requestSubmissionToResponseHandlingTime > 0) {
            clientStatsForSingleGet.recordRequestSubmissionToResponseHandlingTime(requestContext.requestSubmissionToResponseHandlingTime);
          }
          if (requestContext.decompressionTime > 0) {
            clientStatsForSingleGet.recordResponseDecompressionTime(requestContext.decompressionTime);
          }
          if (requestContext.responseDeserializationTime > 0) {
            clientStatsForSingleGet.recordResponseDeserializationTime(requestContext.responseDeserializationTime);
          }

          return value;
        });
    // Record per replica metric
    final long requestSentTimestampNS = requestContext.requestSentTimestampNS;
    if (requestSentTimestampNS > 0) {
      Map<String, CompletableFuture<HttpStatus>> replicaRequestFuture = requestContext.replicaRequestMap;
      final InstanceHealthMonitor monitor = requestContext.instanceHealthMonitor;
      if (monitor != null) {
        clusterStats.recordBlockedInstanceCount(monitor.getBlockedInstanceCount());
        clusterStats.recordUnhealthyInstanceCount(monitor.getUnhealthyInstanceCount());
      }
      replicaRequestFuture.forEach((instance, future) -> {
        future.whenComplete( (status, throwable) -> {
          if (monitor != null) {
            clusterStats.recordPendingRequestCount(instance, monitor.getPendingRequestCounter(instance));
          }

          if (throwable != null) {
            LOGGER.error("Received unexpected exception from replica request future: " + throwable);
            return;
          }
          clientStatsForSingleGet.recordRequest(instance);
          clientStatsForSingleGet.recordResponseWaitingTime(instance, LatencyUtils.getLatencyInMS(requestSentTimestampNS));
          switch (status) {
            case S_200_OK:
            case S_404_NOT_FOUND:
              clientStatsForSingleGet.recordHealthyRequest(instance);
              break;
            case S_429_TOO_MANY_REQUESTS:
              clientStatsForSingleGet.recordQuotaExceededRequest(instance);
              break;
            case S_500_INTERNAL_SERVER_ERROR:
              clientStatsForSingleGet.recordInternalServerErrorRequest(instance);
              break;
            case S_410_GONE:
              /**
               * Check {@link InstanceHealthMonitor#sendRequestToInstance} to understand this special http status.
               */
              clientStatsForSingleGet.recordLeakedRequest(instance);
              break;
            case S_503_SERVICE_UNAVAILABLE:
              clientStatsForSingleGet.recordServiceUnavailableRequest(instance);
              break;
            default:
              clientStatsForSingleGet.recordOtherErrorRequest(instance);
          }
        });
      });
    }

    return AppTimeOutTrackingCompletableFuture.track(statFuture, clientStatsForSingleGet);
  }

  @Override
  public CompletableFuture<Map<K, V>> batchGet(Set<K> keys) throws VeniceClientException {
    if (keys.isEmpty()) {
      return CompletableFuture.completedFuture(Collections.emptyMap());
    }
    int keyCnt = keys.size();
    if (keyCnt > maxAllowedKeyCntInBatchGetReq) {
      throw new VeniceClientException("Currently, the max allowed keyc count in a batch-get request: "
          + maxAllowedKeyCntInBatchGetReq + ", but received: " + keyCnt);
    }
    CompletableFuture<Map<K, V>> resultFuture = new CompletableFuture<>();
    /**
     * Leverage single-get implementation here.
     */
    Map<K, CompletableFuture<V>> valueFutures = new HashMap<>();
    keys.forEach(k -> valueFutures.put(k, (get(k))));
    CompletableFuture.allOf(valueFutures.values().toArray(new CompletableFuture[keyCnt])).whenComplete( ((aVoid, throwable) -> {
      if (throwable != null) {
        resultFuture.completeExceptionally(throwable);
      }
      Map<K, V> resultMap = new HashMap<>();
      valueFutures.forEach((k, f) -> {
        try {
          resultMap.put(k, f.get());
        } catch (Exception e) {
          resultFuture.completeExceptionally(new VeniceClientException("Failed to complete future for key: " + k.toString(), e));
        }
      });
      resultFuture.complete(resultMap);
    }));

    return resultFuture;
  }
}
