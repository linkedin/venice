package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AppTimeOutTrackingCompletableFuture;
import com.linkedin.venice.fastclient.stats.ClientStats;
import com.linkedin.venice.read.RequestType;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import static com.linkedin.venice.client.store.StatTrackingStoreClient.*;


/**
 * This class is in charge of all the metric emissions per request.
 */
public class StatsAvroGenericStoreClient<K, V> extends DelegatingAvroStoreClient<K, V> {
  private final ClientStats clientStatsForSingleGet;

  public StatsAvroGenericStoreClient(InternalAvroStoreClient<K, V> delegate, ClientConfig clientConfig) {
    super(delegate);
    this.clientStatsForSingleGet = clientConfig.getStats(RequestType.SINGLE_GET);
  }

  @Override
  protected CompletableFuture<V> get(GetRequestContext requestContext, K key) throws VeniceClientException {
    long startTimeInNS = System.nanoTime();
    CompletableFuture<V> innerFuture = super.get(requestContext, key);
    clientStatsForSingleGet.recordRequestKeyCount(1);
    CompletableFuture<V> statFuture = innerFuture.handle(
        (BiFunction<? super V, Throwable, ? extends V>) getStatCallback(clientStatsForSingleGet, startTimeInNS))
        .handle( (value, throwable) -> {
          if (throwable != null) {
            if (throwable instanceof VeniceClientException) {
              throw (VeniceClientException)throwable;
            } else {
              throw new VeniceClientException(throwable);
            }
          }
          // Record additional metrics
          if (requestContext.noAvailableReplica) {
            clientStatsForSingleGet.recordNoAvailableReplicaRequest();
          }
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

    return AppTimeOutTrackingCompletableFuture.track(statFuture, clientStatsForSingleGet);
  }

}
