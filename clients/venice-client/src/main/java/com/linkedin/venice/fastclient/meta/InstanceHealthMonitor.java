package com.linkedin.venice.fastclient.meta;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The class is used to measure the healthiness about the cluster the store belongs to.
 * So far, it is per store because of the following reasons:
 * 1. Simplify the logic in this class since we don't need to maintain the per-store status, such as quota related responses.
 * 2. Isolate the healthiness decision among different stores to reduce the impact of false signal.
 * There are concerns with this approach as well, for example, the mis-behaving instances will take a longer time to be discovered
 * in each store.
 *
 * This class is using the pending requests + response status of each Route to decide the healthiness.
 * 1. For the good response, the pending request counter will be reset when receiving the response.
 * 2. For the error response, the pending request counter reset will be delayed, which is a way to downgrade the instance.
 * 3. When the pending request counter exceeds the pre-defined threshold, the instance will be completely blocked.
 *
 */
public class InstanceHealthMonitor implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(InstanceHealthMonitor.class);
  private final ClientConfig clientConfig;

  // Map/set of per store replica instances
  private final Map<String, Integer> pendingRequestCounterMap = new VeniceConcurrentHashMap<>();
  private final Set<String> unhealthyInstanceSet = new ConcurrentSkipListSet<>();

  private final TimeoutProcessor timeoutProcessor;
  private final Consumer<String> counterResetConsumer;

  public InstanceHealthMonitor(ClientConfig clientConfig) {
    this.clientConfig = clientConfig;
    this.timeoutProcessor = new TimeoutProcessor(null, true, 1);

    this.counterResetConsumer = (instance) -> {
      pendingRequestCounterMap.compute(instance, (k, v) -> {
        if (v == null) {
          LOGGER.error(
              "Pending request counter for instance: {} doesn't exist when trying to reset for a completed request",
              instance);
          return 0;
        } else if (v == 0) {
          LOGGER.error(
              "Pending request counter for instance: {} is 0 when trying to reset for a completed request",
              instance);
          return 0;
        }
        return v - 1;
      });
    };
  }

  public TimeoutProcessor getTimeoutProcessor() {
    return this.timeoutProcessor;
  }

  /**
   * This function tracks the health of an Instance based on the request sent to that Instance:
   * by returning an incomplete completable future for {@link AbstractStoreMetadata} which
   * 1. increments {@link InstanceHealthMonitor#pendingRequestCounterMap} for each server instances
   *    per store. This is done in this function which is called before starting a get() request.
   * 2. whenComplete() of this completable future decrements the above counters once the response
   *    for the get() request is received.
   *
   * Using this we can track the number of pending requests for each server instance.
   */
  public CompletableFuture<HttpStatus> trackHealthBasedOnRequestToInstance(String instance) {
    CompletableFuture<HttpStatus> requestFuture = new CompletableFuture<>();
    pendingRequestCounterMap.compute(instance, (k, v) -> {
      if (v == null) {
        return 1;
      }
      return v + 1;
    });

    TimeoutProcessor.TimeoutFuture timeoutFuture = timeoutProcessor.schedule(
        /** Using a special http status to indicate the leaked request */
        () -> requestFuture.complete(HttpStatus.S_410_GONE),
        clientConfig.getRoutingLeakedRequestCleanupThresholdMS(),
        TimeUnit.MILLISECONDS);

    requestFuture.whenComplete((httpStatus, throwable) -> {
      if (throwable != null) {
        /**
         * In theory, throwable should be null all the time since {@link DispatchingAvroGenericStoreClient}
         * will always set a http status in every code path, and this is defensive code.
         */
        LOGGER.error(
            "Received unexpected throwable in replica request future since DispatchingAvroGenericStoreClient"
                + " should always setup a http status");
        return;
      }

      if (!timeoutFuture.isDone()) {
        // TODO check for race conditions
        timeoutFuture.cancel();
      }

      long counterResetDelayMS = 0;
      boolean unhealthyInstance = false;
      switch (httpStatus) {
        case S_200_OK:
        case S_404_NOT_FOUND:
          break;
        case S_429_TOO_MANY_REQUESTS:
          /**
           * Specific to a store.
           * This case will fall under blocked instances as there are too many requests waiting on
           * them, so will be implicitly marked under blocked instances and so not marked unhealthy.
            */
          counterResetDelayMS = clientConfig.getRoutingQuotaExceededRequestCounterResetDelayMS();
          break;
        case S_410_GONE:
        case S_503_SERVICE_UNAVAILABLE:
          counterResetDelayMS = clientConfig.getRoutingUnavailableRequestCounterResetDelayMS();
          unhealthyInstance = true;
          break;
        default:
          // All other error statuses
          counterResetDelayMS = clientConfig.getRoutingErrorRequestCounterResetDelayMS();
          unhealthyInstance = true;
      }
      if (counterResetDelayMS == 0) {
        counterResetConsumer.accept(instance);
      } else {
        /**
         * Even when httpStatus is not 200/404, we want to reset the counter after some delay: to
         * recheck for health once in a while rather than being permanently blocking it
         */
        timeoutProcessor.schedule(
            () -> counterResetConsumer.accept(instance),
            counterResetDelayMS,
            TimeUnit.MILLISECONDS.MILLISECONDS);
      }
      if (unhealthyInstance) {
        if (unhealthyInstanceSet.add(instance)) {
          LOGGER.info("Marked instance: {} as unhealthy because of error response", instance);
        }
      } else {
        if (unhealthyInstanceSet.remove(instance)) {
          LOGGER.info("Marked instance: {} as healthy because of good response", instance);
        }
      }
    });

    return requestFuture;
  }

  /**
   * If an instance is marked unhealthy, this instances will be retried again continuously to know
   * if that instance comes back up and start serving requests. Note that these instances will
   * eventually become blocked when it reaches the threshold for pendingRequestCounter. This
   * provides some break between continuously sending requests to these instances.
   */
  public boolean isInstanceHealthy(String instance) {
    return !unhealthyInstanceSet.contains(instance);
  }

  /**
   * If an instance is blocked, it won't be considered for new requests until the requests are closed either
   * in a proper manner or closed by {@link #trackHealthBasedOnRequestToInstance#timeoutFuture}
   */
  public boolean isInstanceBlocked(String instance) {
    return getPendingRequestCounter(instance) >= clientConfig.getRoutingPendingRequestCounterInstanceBlockThreshold();
  }

  public int getBlockedInstanceCount() {
    int blockedInstanceCount = 0;
    // TODO: need to evaluate whether it is too expensive to emit a metric per request for this.
    for (int count: pendingRequestCounterMap.values()) {
      if (count >= clientConfig.getRoutingPendingRequestCounterInstanceBlockThreshold()) {
        ++blockedInstanceCount;
      }
    }
    return blockedInstanceCount;
  }

  public int getUnhealthyInstanceCount() {
    return unhealthyInstanceSet.size();
  }

  public int getPendingRequestCounter(String instance) {
    Integer pendingRequestCounter = pendingRequestCounterMap.get(instance);
    return pendingRequestCounter == null ? 0 : pendingRequestCounter;
  }

  @Override
  public void close() throws IOException {
    if (timeoutProcessor != null) {
      timeoutProcessor.shutdownNow();
      try {
        timeoutProcessor.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
