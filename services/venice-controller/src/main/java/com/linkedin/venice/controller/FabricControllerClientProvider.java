package com.linkedin.venice.controller;

import static com.linkedin.venice.controller.kafka.consumer.AdminConsumptionTask.IGNORED_CURRENT_VERSION;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Owns the controller-client maps used for cross-region / cross-fabric coordination and centralizes their lazy
 * construction and lifecycle. A single instance is created by {@link VeniceHelixAdmin} (child) and shared with
 * {@link VeniceParentHelixAdmin} (parent), which previously each maintained their own copy of this plumbing.
 *
 * Two maps are maintained:
 * <ul>
 *   <li>{@link #clusterControllerClientPerFabricMap}: cluster -&gt; fabric -&gt; client, built from the standard child
 *       data-center allowlist (URL and D2 maps) in the cluster config.</li>
 *   <li>{@link #newFabricControllerClientMap}: cluster -&gt; fabric -&gt; client, built on demand for fabrics that are
 *       not in the standard allowlist (e.g. build-out / data-recovery destinations).</li>
 * </ul>
 *
 * Both maps are {@link VeniceConcurrentHashMap}s populated via {@code computeIfAbsent}; no external locking is needed.
 */
public class FabricControllerClientProvider implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(FabricControllerClientProvider.class);
  private static final int CURRENT_VERSION_QUERY_TIMEOUT_SECONDS = 15;
  private static final int CURRENT_VERSION_QUERIES_PER_REGION = 6;
  private final ThreadPoolExecutor currentVersionQueryExecutor;
  // Only admitted regions are retained; removing zero counts bounds this map by queued/running work.
  private final Map<String, Integer> currentVersionQueriesPerRegion = new VeniceConcurrentHashMap<>();

  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final Optional<SSLFactory> sslFactory;
  private final Map<String, D2Client> d2Clients;

  /** Controller Client Map per cluster per fabric */
  private final Map<String, Map<String, ControllerClient>> clusterControllerClientPerFabricMap =
      new VeniceConcurrentHashMap<>();

  /** New fabric controller client map per cluster per fabric */
  private final Map<String, Map<String, ControllerClient>> newFabricControllerClientMap =
      new VeniceConcurrentHashMap<>();

  public FabricControllerClientProvider(
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      Optional<SSLFactory> sslFactory,
      Map<String, D2Client> d2Clients) {
    this(
        multiClusterConfigs,
        sslFactory,
        d2Clients,
        new ThreadPoolExecutor(
            16,
            16,
            0,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(256),
            new DaemonThreadFactory("CurrentVersionQuery", multiClusterConfigs.getLogContext()),
            new ThreadPoolExecutor.AbortPolicy()));
  }

  FabricControllerClientProvider(
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      Optional<SSLFactory> sslFactory,
      Map<String, D2Client> d2Clients,
      ThreadPoolExecutor currentVersionQueryExecutor) {
    this.multiClusterConfigs = multiClusterConfigs;
    this.sslFactory = sslFactory;
    this.d2Clients = d2Clients;
    this.currentVersionQueryExecutor = currentVersionQueryExecutor;
  }

  /**
   * Returns the lazily-constructed map of controller clients (fabric -&gt; client) for the standard child data-center
   * allowlist of the given cluster.
   */
  public Map<String, ControllerClient> getControllerClientMap(String clusterName) {
    return clusterControllerClientPerFabricMap.computeIfAbsent(clusterName, cn -> {
      Map<String, ControllerClient> controllerClients = new HashMap<>();
      VeniceControllerClusterConfig controllerConfig = multiClusterConfigs.getControllerConfig(clusterName);
      controllerConfig.getChildDataCenterControllerUrlMap()
          .entrySet()
          .forEach(
              entry -> controllerClients.put(
                  entry.getKey(),
                  ControllerClient.constructClusterControllerClient(clusterName, entry.getValue(), sslFactory)));

      // Respect d2Clients from controller constructor, if not provided, create d2 clients by zk d2 service urls
      // (mainly for testing purpose)
      if (d2Clients != null) {
        controllerConfig.getChildDataCenterControllerD2Map()
            .entrySet()
            .forEach(
                entry -> controllerClients.put(
                    entry.getKey(),
                    new D2ControllerClient(
                        controllerConfig.getD2ServiceName(),
                        clusterName,
                        d2Clients.get(entry.getKey()),
                        sslFactory)));
      } else {
        controllerConfig.getChildDataCenterControllerD2Map()
            .entrySet()
            .forEach(
                entry -> controllerClients.put(
                    entry.getKey(),
                    new D2ControllerClient(
                        controllerConfig.getD2ServiceName(),
                        clusterName,
                        entry.getValue(),
                        sslFactory)));
      }

      return controllerClients;
    });
  }

  /**
   * Returns a controller client for a specific fabric of the given cluster. Fabrics in the standard allowlist are
   * served from {@link #getControllerClientMap(String)}; fabrics outside the allowlist (e.g. build-out / data-recovery
   * destinations) are built on demand from child cluster configs and cached in {@link #newFabricControllerClientMap}.
   */
  public ControllerClient getFabricBuildoutControllerClient(String clusterName, String fabric) {
    Map<String, ControllerClient> controllerClients = getControllerClientMap(clusterName);
    if (controllerClients.containsKey(fabric)) {
      return controllerClients.get(fabric);
    }

    // For fabrics not in allowlist, build controller clients using child cluster configs and cache them in another map
    ControllerClient value =
        newFabricControllerClientMap.computeIfAbsent(clusterName, cn -> new VeniceConcurrentHashMap<>())
            .computeIfAbsent(fabric, f -> {
              VeniceControllerClusterConfig controllerConfig = multiClusterConfigs.getControllerConfig(clusterName);
              String d2ZkHost = controllerConfig.getChildControllerD2ZkHost(fabric);
              String d2ServiceName = controllerConfig.getD2ServiceName();
              if (StringUtils.isNotBlank(d2ZkHost) && StringUtils.isNotBlank(d2ServiceName)) {
                if (d2Clients != null) {
                  return new D2ControllerClient(d2ServiceName, clusterName, d2Clients.get(fabric));
                }
                return new D2ControllerClient(d2ServiceName, clusterName, d2ZkHost, sslFactory);
              }
              String url = controllerConfig.getChildControllerUrl(fabric);
              if (StringUtils.isNotBlank(url)) {
                return ControllerClient.constructClusterControllerClient(clusterName, url, sslFactory);
              }
              return null;
            });

    if (value == null) {
      throw new VeniceException(
          "Could not construct child controller client for cluster " + clusterName + " fabric " + fabric
              + ". child.cluster.d2 or child.cluster.url value is missing in parent controller");
    }
    return value;
  }

  /*
  * TODO:
  * we have a wide variety of retry behaviors cataloged in docs/contributing/architecture/controller-cross-region-fanout.md
  * next iterations we need to unify and land on common behavior.
   */

  /**
   * Issues the same query to every region's controller client and collects a per-region result. This captures the
   * best-effort multi-fabric fan-out shape used by the parent controller's version queries: on a per-region error the
   * failure is logged and {@code errorSentinel} is stored for that region (the query is not aborted), while a
   * successful response is mapped to a value via {@code onSuccess}. When {@code maxAttempts > 1} the request is retried
   * via {@link ControllerClient#retryableRequest(ControllerClient, int, Function)}.
   *
   * @param controllerClients region -&gt; controller client (typically {@link #getControllerClientMap(String)})
   * @param clusterName the cluster being queried; used only for log context
   * @param maxAttempts total attempts per region (1 means no retry)
   * @param request the controller RPC to issue against each region's client
   * @param onSuccess maps a successful response to the per-region result value
   * @param errorSentinel the value stored for a region whose query returned an error
   */
  public <R extends ControllerResponse, V> Map<String, V> queryAllRegions(
      Map<String, ControllerClient> controllerClients,
      String clusterName,
      int maxAttempts,
      Function<ControllerClient, R> request,
      Function<R, V> onSuccess,
      V errorSentinel) {
    Map<String, V> result = new HashMap<>();
    for (Map.Entry<String, ControllerClient> entry: controllerClients.entrySet()) {
      String region = entry.getKey();
      ControllerClient controllerClient = entry.getValue();
      R response = maxAttempts > 1
          ? ControllerClient.retryableRequest(controllerClient, maxAttempts, request)
          : request.apply(controllerClient);
      if (response.isError()) {
        LOGGER.error(
            "Could not query store from region: {} for cluster: {}. Error: {}",
            region,
            clusterName,
            response.getError());
        result.put(region, errorSentinel);
      } else {
        result.put(region, onSuccess.apply(response));
      }
    }
    return result;
  }

  /**
   * Queries current versions concurrently with one 15-second budget, including queueing and discovery.
   * Only the caller assembles the result; cancellation cannot allow a worker to modify a returned map.
   */
  public Map<String, Integer> queryCurrentVersions(
      Map<String, ControllerClient> controllerClients,
      String clusterName,
      String storeName) {
    return queryCurrentVersions(
        controllerClients,
        clusterName,
        storeName,
        System.nanoTime() + TimeUnit.SECONDS.toNanos(CURRENT_VERSION_QUERY_TIMEOUT_SECONDS));
  }

  Map<String, Integer> queryCurrentVersions(
      Map<String, ControllerClient> controllerClients,
      String clusterName,
      String storeName,
      long deadlineNs) {
    Map<String, CompletableFuture<StoreResponse>> responses = new HashMap<>();
    Map<CompletableFuture<StoreResponse>, Future<?>> tasks = new HashMap<>();
    try {
      controllerClients.forEach((region, client) -> {
        CompletableFuture<StoreResponse> response = new CompletableFuture<>();
        responses.put(region, response);
        if (Thread.currentThread().isInterrupted() || deadlineNs - System.nanoTime() <= 0) {
          response
              .completeExceptionally(new TimeoutException("Current-version query budget exhausted before submission"));
          return;
        }
        AtomicBoolean admitted = new AtomicBoolean();
        currentVersionQueriesPerRegion.compute(region, (key, count) -> {
          if (count == null || count < CURRENT_VERSION_QUERIES_PER_REGION) {
            admitted.set(true);
            return count == null ? 1 : count + 1;
          }
          return count;
        });
        if (!admitted.get()) {
          response.completeExceptionally(new RejectedExecutionException("Current-version region saturated: " + region));
          return;
        }
        CurrentVersionQueryTask task = new CurrentVersionQueryTask(region, response, () -> {
          try {
            if (deadlineNs - System.nanoTime() <= 0 || Thread.currentThread().isInterrupted()) {
              throw new TimeoutException("Current-version query budget exhausted before request");
            }
            response.complete(client.getStoreWithDeadline(storeName, deadlineNs));
          } catch (Throwable e) {
            response.completeExceptionally(e);
          }
        });
        tasks.put(response, task);
        try {
          currentVersionQueryExecutor.execute(task);
        } catch (RejectedExecutionException e) {
          response.completeExceptionally(e);
          task.cancel(false);
        }
      });

      try {
        CompletableFuture.allOf(responses.values().toArray(new CompletableFuture<?>[0]))
            .get(Math.max(0, deadlineNs - System.nanoTime()), TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (TimeoutException | ExecutionException e) {
        // Collect every completed response below, including successes behind a slow region.
      }
      cancelUnfinishedQueries(tasks);

      Map<String, Integer> result = new HashMap<>();
      responses.forEach((region, response) -> {
        int version = IGNORED_CURRENT_VERSION;
        try {
          StoreResponse storeResponse = response.join();
          if (storeResponse.isError()) {
            LOGGER.error(
                "Could not query current version from region: {} for cluster: {}. Error: {}",
                region,
                clusterName,
                storeResponse.getError());
          } else {
            version = storeResponse.getStore().getCurrentVersion();
          }
        } catch (CompletionException e) {
          Throwable cause = e.getCause();
          if (!(cause instanceof TimeoutException || cause instanceof CancellationException
              || cause instanceof RejectedExecutionException)) {
            if (cause instanceof RuntimeException) {
              throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
              throw (Error) cause;
            }
            throw new VeniceException(cause);
          }
          LOGGER.warn("Could not query current version from region: {} for cluster: {}", region, clusterName, cause);
        } catch (CancellationException e) {
          LOGGER.warn("Could not query current version from region: {} for cluster: {}", region, clusterName, e);
        }
        result.put(region, version);
      });
      return result;
    } finally {
      cancelUnfinishedQueries(tasks);
      currentVersionQueryExecutor.purge();
    }
  }

  private void cancelUnfinishedQueries(Map<CompletableFuture<StoreResponse>, Future<?>> tasks) {
    // CompletableFuture.cancel(true) alone does not interrupt a running supplier.
    tasks.forEach((response, task) -> {
      if (!response.isDone()) {
        response.cancel(true);
        task.cancel(true);
      }
    });
  }

  private void releaseCurrentVersionQuery(String region) {
    currentVersionQueriesPerRegion.compute(region, (key, count) -> count == 1 ? null : count - 1);
  }

  /**
   * FutureTask.done() runs on cancellation, not necessarily on worker exit. Serialize cancellation-before-start
   * with run() claiming ownership; once claimed, only run's finally may release admission.
   */
  private class CurrentVersionQueryTask extends FutureTask<Void> {
    private final String region;
    private final CompletableFuture<StoreResponse> response;
    private boolean started;
    private boolean released;

    CurrentVersionQueryTask(String region, CompletableFuture<StoreResponse> response, Runnable request) {
      super(request, null);
      this.region = region;
      this.response = response;
    }

    @Override
    public void run() {
      synchronized (this) {
        if (isCancelled() || started) {
          return;
        }
        started = true;
      }
      try {
        super.run();
      } finally {
        releaseAdmission();
      }
    }

    @Override
    protected synchronized void done() {
      if (!started) {
        releaseAdmission();
      }
      if (isCancelled()) {
        response.cancel(false);
      }
    }

    private synchronized void releaseAdmission() {
      if (!released) {
        released = true;
        releaseCurrentVersionQuery(region);
      }
    }
  }

  @Override
  public void close() {
    // shutdownNow drains queued tasks without cancelling them. They still own admission and response futures.
    currentVersionQueryExecutor.shutdownNow().forEach(task -> ((Future<?>) task).cancel(false));
    clusterControllerClientPerFabricMap.values()
        .forEach(controllerClientMap -> controllerClientMap.values().forEach(Utils::closeQuietlyWithErrorLogged));
    newFabricControllerClientMap.values()
        .forEach(controllerClientMap -> controllerClientMap.values().forEach(Utils::closeQuietlyWithErrorLogged));
  }
}
