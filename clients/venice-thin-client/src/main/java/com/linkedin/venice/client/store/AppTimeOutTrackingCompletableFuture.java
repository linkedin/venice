package com.linkedin.venice.client.store;

import com.linkedin.venice.client.stats.ClientStats;
import io.tehuti.utils.Time;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * CompletableFuture implementation, which is able to track the timeout behavior when happening.
 * @param <T>
 */
public class AppTimeOutTrackingCompletableFuture<T> extends CompletableFuture<T> {
  private final ClientStats stats;
  private long startTime;
  private final static long TIMEOUT_COLLECTION_WINDOW = 10 * Time.MS_PER_MINUTE;
  private boolean collectTimeout;

  private AppTimeOutTrackingCompletableFuture(ClientStats stats) {
    this.stats = stats;
    this.startTime = System.currentTimeMillis();
    this.collectTimeout = true;
  }

  @Override
  public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    try {
      if (collectTimeout) {
        if (System.currentTimeMillis() < startTime + TIMEOUT_COLLECTION_WINDOW) {
          stats.recordClientFutureTimeout(unit.toMillis(timeout));
        } else {
          collectTimeout = false;
        }
      }
      return super.get(timeout, unit);
    } catch (TimeoutException e) {
      stats.recordAppTimedOutRequest();
      throw e;
    }
  }

  public static <T> CompletableFuture<T> track(CompletableFuture<T> future, ClientStats stats) {
    AppTimeOutTrackingCompletableFuture<T> trackingFuture = new AppTimeOutTrackingCompletableFuture<>(stats);
    future.whenComplete((T v, Throwable t) -> {
      if (t != null) {
        trackingFuture.completeExceptionally(t);
      } else {
        trackingFuture.complete(v);
      }
    });
    return trackingFuture;
  }
}
