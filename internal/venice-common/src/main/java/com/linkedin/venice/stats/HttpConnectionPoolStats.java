package com.linkedin.venice.stats;

import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.pool.PoolStats;


public class HttpConnectionPoolStats extends AbstractVeniceStats {
  private final List<PoolingNHttpClientConnectionManager> connectionManagerList = new ArrayList<>();
  /**
   * The following lock is used to protect {@link #connectionManagerList}.
   * It is possible that this list will be modified and read at the same time.
   * But for now, we don't create {@link PoolingNHttpClientConnectionManager} dynamically,
   * but I think it is fine to still keep this lock to make it independent from the external logic.
   */
  private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
  private final Map<String, RouteHttpConnectionPoolStats> routeConnectionPoolStatsMap = new VeniceConcurrentHashMap<>();

  /**
   * Measure how much time to take to lease a connection from connection pool.
   *
   * Notes:
   * if this latency is high, it is possible that DNS lookup is slow when setting up a new connection internally,
   * or the connection pool is too busy to lease/release connections.
   */
  private final Sensor connectionLeaseRequestLatency;

  /**
   * This is used to track the pending request count per http client.
   */
  private final Sensor pendingRequestCount;

  public HttpConnectionPoolStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    connectionLeaseRequestLatency = registerSensor("connection_lease_request_latency", new Avg(), new Max());

    pendingRequestCount = registerSensor("pending_request_count", new Avg(), new Max());
    /**
     * Total connections being actively used
     *
     * Check {@link PoolStats#getLeased()} to get more details.
     */
    registerSensor(
        new LambdaStat(
            (c, t) -> getAggStats(connectionManager -> connectionManager.getTotalStats().getLeased()),
            "total_active_connection_count"));
    /**
     * Total idle connections
     *
     * Check {@link PoolStats#getAvailable()} to get more details.
     */
    registerSensor(
        new LambdaStat(
            (c, t) -> getAggStats(connectionManager -> connectionManager.getTotalStats().getAvailable()),
            "total_idle_connection_count"));
    /**
     * Total max connections
     *
     * Check {@link PoolStats#getMax()} to get more details.
     */
    registerSensor(
        new LambdaStat(
            (c, t) -> getAggStats(connectionManager -> connectionManager.getTotalStats().getMax()),
            "total_max_connection_count"));
    /**
     * Total number of connection requests being blocked awaiting a free connection
     *
     * Check {@link PoolStats#getPending()} to get more details.
     */
    registerSensor(
        new LambdaStat(
            (c, t) -> getAggStats(connectionManager -> connectionManager.getTotalStats().getPending()),
            "total_pending_connection_request_count"));
  }

  public void addConnectionPoolManager(PoolingNHttpClientConnectionManager connectionManager) {
    rwLock.writeLock().lock();
    try {
      connectionManagerList.add(connectionManager);
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  public void removeConnectionPoolManager(PoolingNHttpClientConnectionManager connectionManager) {
    rwLock.writeLock().lock();
    try {
      connectionManagerList.remove(connectionManager);
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  /**
   * This function is used to gradually populate per-route stats.
   * @param hostName
   */
  public void addStatsForRoute(String hostName) {
    routeConnectionPoolStatsMap
        .computeIfAbsent(hostName, k -> new RouteHttpConnectionPoolStats(getMetricsRepository(), hostName));
  }

  private Long getAggStats(Function<PoolingNHttpClientConnectionManager, Integer> func) {
    rwLock.readLock().lock();
    try {
      long total = 0;
      for (PoolingNHttpClientConnectionManager connectionManager: connectionManagerList) {
        total += func.apply(connectionManager);
      }
      return total;
    } finally {
      rwLock.readLock().unlock();
    }
  }

  public void recordConnectionLeaseRequestLatency(long latency) {
    connectionLeaseRequestLatency.record(latency);
  }

  public void recordPendingRequestCount(long pendingRequestCount) {
    this.pendingRequestCount.record(pendingRequestCount);
  }

  public long getPendingRequestCount(String hostname) {
    RouteHttpConnectionPoolStats poolStats = routeConnectionPoolStatsMap.get(hostname);
    // Initial few requests will not have per route map populated. return 0
    if (poolStats == null) {
      return 0;
    }
    return poolStats.getPendingRequestCount();
  }

  class RouteHttpConnectionPoolStats extends AbstractVeniceStats {
    private String hostName;

    public RouteHttpConnectionPoolStats(MetricsRepository metricsRepository, String hostName) {
      super(metricsRepository, hostName.replace('.', '_'));
      this.hostName = hostName;

      /**
       * Total connections being actively used
       *
       * Check {@link PoolStats#getLeased()} to get more details.
       */
      registerSensor(
          new AsyncGauge((c, t) -> getRouteAggStats(poolStats -> poolStats.getLeased()), "active_connection_count"));
      /**
       * Total idle connections
       *
       * Check {@link PoolStats#getAvailable()} to get more details.
       */
      registerSensor(
          new AsyncGauge((c, t) -> getRouteAggStats(poolStats -> poolStats.getAvailable()), "idle_connection_count"));

      /**
       * Total max connections
       *
       * Check {@link PoolStats#getMax()} to get more details.
       */
      registerSensor(
          new AsyncGauge((c, t) -> getRouteAggStats(poolStats -> poolStats.getMax()), "max_connection_count"));

      /**
       * Total number of connection requests being blocked awaiting a free connection
       *
       * Check {@link PoolStats#getPending()} to get more details.
       */
      registerSensor(
          new AsyncGauge(
              (c, t) -> getRouteAggStats(poolStats -> poolStats.getPending()),
              "pending_connection_request_count"));
    }

    /**
     * Return number of request being used + awaiting connection.
     * @return
     */
    public Long getPendingRequestCount() {
      return getRouteAggStats(poolStats -> poolStats.getLeased() + poolStats.getPending());
    }

    private Long getRouteAggStats(Function<PoolStats, Integer> func) {
      rwLock.readLock().lock();
      try {
        long total = 0;
        for (PoolingNHttpClientConnectionManager connectionManager: connectionManagerList) {
          Set<HttpRoute> routeSet = connectionManager.getRoutes();
          for (HttpRoute route: routeSet) {
            if (route.getTargetHost().getHostName().equals(hostName)) {
              PoolStats poolStats = connectionManager.getStats(route);
              total += func.apply(poolStats);
              break;
            }
          }
        }
        return total;
      } finally {
        rwLock.readLock().unlock();
      }
    }
  }
}
