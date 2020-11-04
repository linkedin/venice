package com.linkedin.venice.fastclient;

import com.google.common.util.concurrent.AtomicDouble;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.fastclient.stats.ClientStats;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.utils.LatencyUtils;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;


/**
 * The following class has capability to send dual read requests via Fast-client and Thin-client.
 * 1. If both of them succeed, return the faster one.
 * 2. If one of them fails, return the succeeded one.
 * 3. If both of them fail, throw exception.
 */
public class DualReadAvroGenericStoreClient<K, V> extends DelegatingAvroStoreClient<K, V> {
  private final AvroGenericStoreClient<K, V> thinClient;
  private final ClientStats clientStatsForSingleGet;

  public DualReadAvroGenericStoreClient(InternalAvroStoreClient<K, V> delegate, ClientConfig config) {
    this(delegate, config, config.getGenericThinClient());
    if (config.getGenericThinClient() == null) {
      throw new VeniceClientException("GenericThinClient in ClientConfig shouldn't be null when constructing a generic dual-read store client");
    }
  }

  protected DualReadAvroGenericStoreClient(InternalAvroStoreClient<K, V> delegate, ClientConfig config, AvroGenericStoreClient<K, V> thinClient) {
    super(delegate);
    this.thinClient = thinClient;
    this.clientStatsForSingleGet = config.getStats(RequestType.SINGLE_GET);
  }

  private CompletableFuture<V> sendRequest(Supplier<CompletableFuture<V>> supplier, long startTimeNS,
      AtomicBoolean error, AtomicDouble latency, CompletableFuture<V> valueFuture) {
    CompletableFuture<V> requestFuture;
    try {
      requestFuture = supplier.get();
    } catch (Exception e) {
      // This used to catch exception thrown when trying to send out the request.
      requestFuture = new CompletableFuture<>();
      requestFuture.completeExceptionally(e);
    }

    CompletableFuture<V> latencyFuture = requestFuture.handle((response, throwable) -> {
      /**
       * We need to record the latency metric before trying to complete {@link valueFuture} since the the pre-registered
       * callbacks to {@link valueFuture} could be executed in the same thread.
       */
      latency.set(LatencyUtils.getLatencyInMS(startTimeNS));

      if (throwable != null) {
        error.set(true);
        if (throwable instanceof VeniceClientException) {
          throw (VeniceClientException)throwable;
        }
        throw new VeniceClientException(throwable);
      }
      return response;
    });
    requestFuture.whenComplete((response, throwable) -> {
      if (throwable == null) {
        valueFuture.complete(response);
      }
    });

    /**
     * Returning {@link latencyFuture} here will allow the dependencies of latency metrics to complete as soon as possible
     * instead of waiting for {@link valueFuture#complete}, which could take a long time because of the registered callbacks
     * of {@link valueFuture}.
     */
    return latencyFuture;
  }

  @Override
  protected CompletableFuture<V> get(GetRequestContext requestContext, K key) throws VeniceClientException {
    CompletableFuture<V> valueFuture = new CompletableFuture<>();
    long startTimeNS = System.nanoTime();
    AtomicBoolean fastClientError = new AtomicBoolean(false);
    AtomicBoolean thinClientError = new AtomicBoolean(false);
    AtomicDouble fastClientLatency = new AtomicDouble();
    AtomicDouble thinClientLatency = new AtomicDouble();
    CompletableFuture<V> fastClientFuture = sendRequest(() -> super.get(requestContext, key),
        startTimeNS, fastClientError, fastClientLatency, valueFuture);
    CompletableFuture<V> thinClientFuture = sendRequest(() -> thinClient.get(key),
        startTimeNS, thinClientError, thinClientLatency, valueFuture);

    CompletableFuture.allOf(fastClientFuture, thinClientFuture).whenComplete( (response, throwable) -> {
      /**
       * Throw exception only if both fast-client and thin-client return error.
       */
      if (throwable != null) {
        valueFuture.completeExceptionally(throwable);
      }

      if (fastClientError.get() && !thinClientError.get()) {
        // fast client returns error, but thin client returns good.
        clientStatsForSingleGet.recordFastClientErrorThinClientSucceedRequest();
      }
      // record latency delta and comparison only both requests succeed.
      if (!thinClientError.get() && !fastClientError.get()) {
        clientStatsForSingleGet.recordThinClientFastClientLatencyDelta(thinClientLatency.get() - fastClientLatency.get());
        if (fastClientLatency.get() > thinClientLatency.get()) {
          // fast client is slower than thin client
          clientStatsForSingleGet.recordFastClientSlowerRequest();
        }
      }
    });

    return valueFuture;
  }
}
