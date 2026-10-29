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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
  private static final int CURRENT_VERSION_QUERY_WORKERS_PER_REGION = 6;
  // Guarded by this, together with closed. Pools are shared across stores and clusters, keyed only by region.
  private final Map<String, ThreadPoolExecutor> currentVersionQueryExecutors = new HashMap<>();
  private boolean closed;

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
    this.multiClusterConfigs = multiClusterConfigs;
    this.sslFactory = sslFactory;
    this.d2Clients = d2Clients;
  }

  synchronized ThreadPoolExecutor getCurrentVersionQueryExecutor(String region) {
    if (closed) {
      throw new RejectedExecutionException("Controller client provider is closed");
    }
    return currentVersionQueryExecutors.computeIfAbsent(
        region,
        key -> new ThreadPoolExecutor(
            CURRENT_VERSION_QUERY_WORKERS_PER_REGION,
            CURRENT_VERSION_QUERY_WORKERS_PER_REGION,
            0,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(256), // Queue bound is per region, not global.
            new DaemonThreadFactory("CurrentVersionQuery-" + key, multiClusterConfigs.getLogContext()),
            new ThreadPoolExecutor.AbortPolicy()));
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
   * Queries current versions for the read-only store response with one 15-second budget, including queueing and
   * discovery. Internal coordination uses the serial {@link #queryAllRegions} policy instead.
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
    Map<String, Future<StoreResponse>> tasks = new HashMap<>();
    Map<String, ThreadPoolExecutor> executors = new HashMap<>();
    Map<String, Integer> result = new HashMap<>();
    try {
      controllerClients.forEach((region, client) -> {
        result.put(region, IGNORED_CURRENT_VERSION);
        if (Thread.currentThread().isInterrupted() || deadlineNs - System.nanoTime() <= 0) {
          LOGGER.warn(
              "Current-version query budget exhausted before submission for region: {} cluster: {}",
              region,
              clusterName);
          return;
        }
        try {
          ThreadPoolExecutor executor = getCurrentVersionQueryExecutor(region);
          executors.put(region, executor);
          tasks.put(region, executor.submit(() -> {
            if (deadlineNs - System.nanoTime() <= 0 || Thread.currentThread().isInterrupted()) {
              throw new TimeoutException("Current-version query budget exhausted before request");
            }
            return client.getStoreWithDeadline(storeName, deadlineNs);
          }));
        } catch (RejectedExecutionException e) {
          LOGGER.warn("Could not submit current-version query for region: {} cluster: {}", region, clusterName, e);
        }
      });

      for (Future<StoreResponse> task: tasks.values()) {
        try {
          task.get(Math.max(0, deadlineNs - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        } catch (TimeoutException e) {
          break;
        } catch (ExecutionException | CancellationException e) {
          // Collect every completed response below, including successes behind a slow region.
        }
      }
      cancelUnfinishedQueries(tasks);

      for (Map.Entry<String, Future<StoreResponse>> entry: tasks.entrySet()) {
        String region = entry.getKey();
        int version = IGNORED_CURRENT_VERSION;
        try {
          StoreResponse storeResponse = entry.getValue().get();
          if (storeResponse.isError()) {
            LOGGER.error(
                "Could not query current version from region: {} for cluster: {}. Error: {}",
                region,
                clusterName,
                storeResponse.getError());
          } else {
            version = storeResponse.getStore().getCurrentVersion();
          }
        } catch (ExecutionException e) {
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
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOGGER.warn("Interrupted querying current version from region: {} for cluster: {}", region, clusterName, e);
        }
        result.put(region, version);
      }
      return result;
    } finally {
      cancelUnfinishedQueries(tasks);
      executors.values().forEach(ThreadPoolExecutor::purge);
    }
  }

  private void cancelUnfinishedQueries(Map<String, Future<StoreResponse>> tasks) {
    tasks.values().forEach(task -> {
      if (!task.isDone()) {
        task.cancel(true);
      }
    });
  }

  @Override
  public void close() {
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      // Pool creation uses the same lock, so no pool can escape shutdown or be created after close.
      currentVersionQueryExecutors.values().forEach(executor -> {
        // shutdownNow interrupts workers but drained queued futures must also be cancelled to unblock callers.
        executor.shutdownNow().forEach(task -> ((Future<?>) task).cancel(false));
      });
      currentVersionQueryExecutors.clear();
    }
    clusterControllerClientPerFabricMap.values()
        .forEach(controllerClientMap -> controllerClientMap.values().forEach(Utils::closeQuietlyWithErrorLogged));
    newFabricControllerClientMap.values()
        .forEach(controllerClientMap -> controllerClientMap.values().forEach(Utils::closeQuietlyWithErrorLogged));
  }
}
