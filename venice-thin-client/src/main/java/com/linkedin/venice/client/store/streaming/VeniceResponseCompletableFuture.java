package com.linkedin.venice.client.store.streaming;

import com.linkedin.venice.client.stats.ClientStats;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;


/**
 * {@link CompletableFuture} implementation to handle partial response.
 * @param <T>
 */
public class VeniceResponseCompletableFuture<T> extends CompletableFuture<T> {
  private final Supplier<VeniceResponseMap> resultSupplier;
  private final int totalKeyCnt;
  private final Optional<ClientStats> stats;

  public VeniceResponseCompletableFuture(
      Supplier<VeniceResponseMap> supplier,
      int totalKeyCnt,
      Optional<ClientStats> stats) {
    this.resultSupplier = supplier;
    this.totalKeyCnt = totalKeyCnt;
    this.stats = stats;
  }

  @Override
  public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException {
    try {
      return super.get(timeout, unit);
    } catch (TimeoutException toE) {
      VeniceResponseMap partialResponse = resultSupplier.get();
      stats.ifPresent(s -> {
        s.recordAppTimedOutRequest();
        if (totalKeyCnt > 0) {
          s.recordAppTimedOutRequestResultRatio(
              ((double) (partialResponse.size() + partialResponse.getNonExistingKeys().size())) / totalKeyCnt);
        }
      });
      // Always return the available result
      return (T) partialResponse;
    }
  }
}
