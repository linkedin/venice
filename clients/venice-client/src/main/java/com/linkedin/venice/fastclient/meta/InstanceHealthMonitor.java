package com.linkedin.venice.fastclient.meta;

import static org.apache.hc.core5.http.HttpStatus.SC_GONE;
import static org.apache.hc.core5.http.HttpStatus.SC_SERVICE_UNAVAILABLE;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.alpini.base.concurrency.ScheduledExecutorService;
import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.fastclient.transport.R2TransportClient;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.concurrent.ChainedCompletableFuture;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is used to track the health of the instances Fast Client is talking to.
 * Here is the strategy:
 * 1. It is maintaining a pending request map to track the pending request count per instance.
 * 2. If the pending request count exceeds the configured threshold, the instance will be blocked.
 * 3. If the request is timed out based on the configured timeout threshold, this instance will be put
 *    into suspicious instance set.
 * 4. The embedded heart-beat runnable will send heartbeat request to the suspicious/unhealthy instances
 *    periodically, and if the heartbeat request still times out or receives error response, the instance
 *    will be put into the unhealthy instance set. Otherwise, the instance will be removed from the suspicious
 *    and unhealthy instance set.
 * 5. Fast Client won't send user requests to the blocked instances and unhealthy instances.
 */
public class InstanceHealthMonitor implements Closeable {
  private static final RedundantExceptionFilter REDUNDANT_EXCEPTION_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();
  private static final Logger LOGGER = LogManager.getLogger(InstanceHealthMonitor.class);
  private final InstanceHealthMonitorConfig config;

  private final Map<String, Integer> pendingRequestCounterMap = new VeniceConcurrentHashMap<>();
  private final Set<String> unhealthyInstanceSet = new ConcurrentSkipListSet<>();
  private final Set<String> suspiciousInstanceSet = new ConcurrentSkipListSet<>();

  private final TimeoutProcessor timeoutProcessor;

  private final ScheduledExecutorService heartBeatScheduler =
      Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("VeniceFastClient-HeartBeat"));
  private final R2TransportClient heartBeatClient;

  private final Consumer<String> counterResetConsumer;

  private final InstanceLoadController loadController;

  public InstanceHealthMonitor(InstanceHealthMonitorConfig config) {
    this.config = config;
    this.timeoutProcessor = new TimeoutProcessor(null, true, 1);
    this.heartBeatClient = new R2TransportClient(config.getClient());
    this.heartBeatScheduler
        .scheduleAtFixedRate(this::heartBeat, 0, config.getHeartBeatIntervalSeconds(), TimeUnit.SECONDS);

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
    this.loadController = new InstanceLoadController(config);
  }

  private void heartBeat() {
    Set<String> heartBeatInstances = new HashSet<>(suspiciousInstanceSet);
    heartBeatInstances.addAll(unhealthyInstanceSet);
    if (heartBeatInstances.isEmpty()) {
      return;
    }
    for (String instance: heartBeatInstances) {
      String heartBeatUrl = instance + "/" + QueryAction.HEALTH.toString().toLowerCase();
      CompletableFuture<TransportClientResponse> heartBeatFuture = heartBeatClient.get(heartBeatUrl);
      TimeoutProcessor.TimeoutFuture timeoutFuture = null;
      if (heartBeatFuture != null) {
        timeoutFuture = timeoutProcessor.schedule(
            /** Using a special http status to indicate the timed-out request */
            () -> {
              heartBeatFuture
                  .completeExceptionally(new VeniceClientHttpException("Heartbeat request timed out", SC_GONE));
              LOGGER.warn(
                  "Heartbeat request to instance: {} timed out after {} ms",
                  instance,
                  config.getHeartBeatRequestTimeoutMS());
            },
            config.getHeartBeatRequestTimeoutMS(),
            TimeUnit.MILLISECONDS);

        TimeoutProcessor.TimeoutFuture finalTimeoutFuture = timeoutFuture;
        heartBeatFuture.whenComplete((response, throwable) -> {
          if (finalTimeoutFuture != null && !finalTimeoutFuture.isDone()) {
            finalTimeoutFuture.cancel();
          }
          if (throwable != null) {
            if (!unhealthyInstanceSet.contains(instance)) {
              LOGGER.warn(
                  "Heartbeat to instance: {} failed with exception: {}, will add it to unhealthy instance set",
                  instance,
                  throwable.getMessage());
              unhealthyInstanceSet.add(instance);
            }
            suspiciousInstanceSet.remove(instance);
            return;
          }
          LOGGER.info("Heartbeat to instance: {} succeeded, will remove it from unhealthy instance set", instance);
          unhealthyInstanceSet.remove(instance);
          suspiciousInstanceSet.remove(instance);
        });
      }
    }
  }

  public TimeoutProcessor getTimeoutProcessor() {
    return this.timeoutProcessor;
  }

  public ChainedCompletableFuture<Integer, Integer> trackHealthBasedOnRequestToInstance(String instance) {
    return trackHealthBasedOnRequestToInstance(instance, null);
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
  public ChainedCompletableFuture<Integer, Integer> trackHealthBasedOnRequestToInstance(
      String instance,
      CompletableFuture<TransportClientResponse> transportFuture) {
    CompletableFuture<Integer> requestFuture = new CompletableFuture<>();
    pendingRequestCounterMap.compute(instance, (k, v) -> {
      // currently tracking the number of requests as 1 for single get
      // and 1 for each route requests in batchGet scatter.
      if (v == null) {
        return 1;
      }
      return v + 1;
    });

    TimeoutProcessor.TimeoutFuture timeoutFuture = null;
    if (transportFuture != null) {
      timeoutFuture = timeoutProcessor.schedule(
          /** Using a special http status to indicate the timed out request */
          () -> {
            transportFuture.completeExceptionally(new VeniceClientHttpException("Request timed out", SC_GONE));
            String logMessage = String.format(
                "Request to instance: %s timed out after %d ms, will start sending heart-beat to this instance to check whether it is healthy or not",
                instance,
                config.getRoutingRequestDefaultTimeoutMS());
            if (!REDUNDANT_EXCEPTION_FILTER.isRedundantException(logMessage)) {
              LOGGER.warn(logMessage);
            }
            suspiciousInstanceSet.add(instance);
          },
          config.getRoutingRequestDefaultTimeoutMS(),
          TimeUnit.MILLISECONDS);
    }

    TimeoutProcessor.TimeoutFuture finalTimeoutFuture = timeoutFuture;
    CompletableFuture<Integer> resultFuture = requestFuture.whenComplete((httpStatus, throwable) -> {
      if (finalTimeoutFuture != null && !finalTimeoutFuture.isDone()) {
        finalTimeoutFuture.cancel();
      }

      if (throwable != null) {
        httpStatus = (throwable instanceof VeniceClientHttpException)
            ? ((VeniceClientHttpException) throwable).getHttpStatus()
            : SC_SERVICE_UNAVAILABLE;
      }

      if (httpStatus.equals(SC_GONE)) {
        /**
         * For timeout cases, we would like to delay the pending request counter reset to reduce the traffic
         * to this suspicious instance before heartbeat makes a decision about whether this instance is indeed
         * unhealthy or not.
         */
        timeoutProcessor.schedule(
            () -> counterResetConsumer.accept(instance),
            config.getRoutingTimedOutRequestCounterResetDelayMS(),
            TimeUnit.MILLISECONDS);
      } else {
        counterResetConsumer.accept(instance);
      }

      loadController.recordResponse(instance, httpStatus);
    });

    return new ChainedCompletableFuture<>(requestFuture, resultFuture);
  }

  /**
   * If an instance is marked unhealthy, this instances will be retried again continuously to know
   * if that instance comes back up and start serving requests. Note that these instances will
   * eventually become blocked when it reaches the threshold for pendingRequestCounter. This
   * provides some break between continuously sending requests to these instances.
   */
  boolean isInstanceHealthy(String instance) {
    return !unhealthyInstanceSet.contains(instance);
  }

  /**
   * If an instance is blocked, it won't be considered for new requests until the requests are closed either
   * in a proper manner or closed by {@link #trackHealthBasedOnRequestToInstance#timeoutFuture}
   */
  boolean isInstanceBlocked(String instance) {
    return getPendingRequestCounter(instance) >= config.getRoutingPendingRequestCounterInstanceBlockThreshold();
  }

  boolean shouldRejectRequest(String instance) {
    return loadController.shouldRejectRequest(instance);
  }

  public boolean isRequestAllowed(String instance) {
    return isInstanceHealthy(instance) && !isInstanceBlocked(instance) && !shouldRejectRequest(instance);
  }

  public int getBlockedInstanceCount() {
    int blockedInstanceCount = 0;
    // TODO: need to evaluate whether it is too expensive to emit a metric per request for this.
    for (int count: pendingRequestCounterMap.values()) {
      if (count >= config.getRoutingPendingRequestCounterInstanceBlockThreshold()) {
        ++blockedInstanceCount;
      }
    }
    return blockedInstanceCount;
  }

  public int getOverloadedInstanceCount() {
    return loadController.getTotalNumberOfOverLoadedInstances();
  }

  public double getRejectionRatio(String instance) {
    return loadController.getRejectionRatio(instance);
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

    if (heartBeatScheduler != null) {
      heartBeatScheduler.shutdownNow();
      try {
        heartBeatScheduler.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

}
