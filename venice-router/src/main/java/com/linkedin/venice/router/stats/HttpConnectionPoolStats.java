package com.linkedin.venice.router.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.LambdaStat;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.pool.PoolStats;


public class HttpConnectionPoolStats extends AbstractVeniceStats {
  private List<PoolingNHttpClientConnectionManager> connectionManagerList = new ArrayList<>();
  private ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

  /**
   * Measure how much time to take to lease a connection from connection pool.
   *
   * Notes:
   * if this latency is high, it is possible that DNS lookup is slow when setting up a new connection internally,
   * or the connection pool is too busy to lease/release connections.
   */
  private final Sensor connectionLeaseRequestLatency;

  public HttpConnectionPoolStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    connectionLeaseRequestLatency = registerSensor("connection_lease_request_latency", new Avg(), new Max());
    /**
     * Total connections being actively used
     *
     * Check {@link PoolStats#getLeased()} to get more details.
     */
    registerSensor("total_active_connection_count", new LambdaStat(
        () -> getAggStats(connectionManager -> connectionManager.getTotalStats().getLeased())
    ));
    /**
     * Total idle connections
     *
     * Check {@link PoolStats#getAvailable()} to get more details.
     */
    registerSensor("total_idle_connection_count", new LambdaStat(
        () -> getAggStats(connectionManager -> connectionManager.getTotalStats().getAvailable())
    ));
    /**
     * Total max connections
     *
     * Check {@link PoolStats#getMax()} to get more details.
     */
    registerSensor("total_max_connection_count", new LambdaStat(
        () -> getAggStats(connectionManager -> connectionManager.getTotalStats().getMax())
    ));
    /**
     * Total number of connection requests being blocked awaiting a free connection
     *
     * Check {@link PoolStats#getPending()} to get more details.
     */
    registerSensor("total_pending_connection_request_count", new LambdaStat(
        () -> getAggStats(connectionManager -> connectionManager.getTotalStats().getPending())
    ));
  }

  public void addConnectionPoolManager(PoolingNHttpClientConnectionManager connectionManager) {
    rwLock.writeLock().lock();
    try {
      connectionManagerList.add(connectionManager);
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  private Long getAggStats(Function<PoolingNHttpClientConnectionManager, Integer> func) {
    rwLock.readLock().lock();
    try {
      long total = 0;
      for (PoolingNHttpClientConnectionManager connectionManager : connectionManagerList) {
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
}
