package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.fastclient.stats.FastClientStats;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.utils.LatencyUtils;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;


/**
 * The following class has capability to send dual read requests via Fast-client and Thin-client.
 * 1. If both of them succeed, return the faster one.
 * 2. If one of them fails, return the succeeded one.
 * 3. If both of them fail, throw exception.
 *
 * Currently, this implementation only supports dual-read for single-get and batch-get requests. Compute requests have
 * not been implemented for two reasons:
 * 1. We prefer to do a gradual ramp of the feature as dual reads can lead to extra load on the storage nodes
 * 2. The complexity of implementing dual reads for compute is higher and needs extra work to convert a
 * {@link com.linkedin.venice.client.store.streaming.StreamingCallback} into a {@link CompletableFuture Future}.
 */
public class DualReadAvroGenericStoreClient<K, V> extends DelegatingAvroStoreClient<K, V> {
  private final AvroGenericStoreClient<K, V> thinClient;
  private final FastClientStats clientStatsForSingleGet;
  private final FastClientStats clientStatsForMultiGet;

  public DualReadAvroGenericStoreClient(InternalAvroStoreClient<K, V> delegate, ClientConfig config) {
    this(delegate, config, config.getGenericThinClient());
    if (config.getGenericThinClient() == null) {
      throw new VeniceClientException(
          "GenericThinClient in ClientConfig shouldn't be null when constructing a generic dual-read store client");
    }
  }

  protected DualReadAvroGenericStoreClient(
      InternalAvroStoreClient<K, V> delegate,
      ClientConfig config,
      AvroGenericStoreClient<K, V> thinClient) {
    super(delegate, config);
    this.thinClient = thinClient;
    this.clientStatsForSingleGet = config.getStats(RequestType.SINGLE_GET);
    this.clientStatsForMultiGet = config.getStats(RequestType.MULTI_GET_STREAMING);
  }

  private static <T> CompletableFuture<T> sendRequest(
      Supplier<CompletableFuture<T>> supplier,
      AtomicBoolean error,
      AtomicReference<Double> latency,
      CompletableFuture<T> valueFuture) {
    CompletableFuture<T> requestFuture;
    long startTimeNS = System.nanoTime();
    try {
      requestFuture = supplier.get();
    } catch (Exception e) {
      // This used to catch exception thrown when trying to send out the request.
      requestFuture = new CompletableFuture<>();
      requestFuture.completeExceptionally(e);
    }

    CompletableFuture<T> latencyFuture = requestFuture.handle((response, throwable) -> {
      /**
       * We need to record the latency metric before trying to complete {@link valueFuture} since the pre-registered
       * callbacks to {@link valueFuture} could be executed in the same thread.
       */
      latency.set(LatencyUtils.getElapsedTimeFromNSToMS(startTimeNS));

      if (throwable != null) {
        error.set(true);
        if (throwable instanceof VeniceClientException) {
          throw (VeniceClientException) throwable;
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

  private static <T> CompletableFuture<T> dualExecute(
      Supplier<CompletableFuture<T>> fastClientFutureSupplier,
      Supplier<CompletableFuture<T>> thinClientFutureSupplier,
      FastClientStats clientStats) {
    CompletableFuture<T> valueFuture = new CompletableFuture<>();
    AtomicBoolean fastClientError = new AtomicBoolean(false);
    AtomicBoolean thinClientError = new AtomicBoolean(false);
    AtomicReference<Double> fastClientLatency = new AtomicReference<>();
    AtomicReference<Double> thinClientLatency = new AtomicReference<>();
    CompletableFuture<T> fastClientFuture =
        sendRequest(fastClientFutureSupplier, fastClientError, fastClientLatency, valueFuture);
    CompletableFuture<T> thinClientFuture =
        sendRequest(thinClientFutureSupplier, thinClientError, thinClientLatency, valueFuture);

    CompletableFuture.allOf(fastClientFuture, thinClientFuture).whenComplete((response, throwable) -> {
      /**
       * Throw exception only if both fast-client and thin-client return error.
       */
      if (throwable != null && fastClientFuture.isCompletedExceptionally()
          && thinClientFuture.isCompletedExceptionally()) {
        valueFuture.completeExceptionally(throwable);
      }

      if (fastClientError.get() && !thinClientError.get()) {
        // fast client returns error, but thin client returns good.
        clientStats.recordFastClientErrorThinClientSucceedRequest();
      }
      // record latency delta and comparison only both requests succeed.
      if (!thinClientError.get() && !fastClientError.get()) {
        clientStats.recordThinClientFastClientLatencyDelta(thinClientLatency.get() - fastClientLatency.get());
        if (fastClientLatency.get() > thinClientLatency.get()) {
          // fast client is slower than thin client
          clientStats.recordFastClientSlowerRequest();
        }
      }
    });

    return valueFuture;
  }

  /**
   * TODO both super.get(key) and super.get(ctx,key) fetches non map for vsonClient for the first request.
   *  Needs to be investigated */
  @Override
  protected CompletableFuture<V> get(GetRequestContext<K> requestContext, K key) throws VeniceClientException {
    /**
     * If a user calls {@link batchGet}, the {@link batchGet} would trigger a dual read on the thin-client and
     * fast-client.
     */
    return dualExecute(() -> super.get(requestContext, key), () -> thinClient.get(key), clientStatsForSingleGet);
  }

  @Override
  public CompletableFuture<Map<K, V>> batchGet(BatchGetRequestContext<K, V> requestContext, Set<K> keys)
      throws VeniceClientException {
    return dualExecute(
        () -> super.batchGet(requestContext, keys),
        () -> thinClient.batchGet(keys),
        clientStatsForMultiGet);
  }
}
