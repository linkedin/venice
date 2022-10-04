package com.linkedin.alpini.netty4.pool;

import com.linkedin.alpini.base.monitoring.CallTracker;
import com.linkedin.alpini.base.monitoring.NullCallTracker;
import com.linkedin.alpini.consts.QOS;
import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;


/**
 * The interface which describes the public methods of a channel pool manager.
 *
 * @author acurtis on 3/30/17.
 */
public interface ChannelPoolManager {
  AttributeKey<Boolean> FAILED_HEALTH_CHECK = AttributeKey.valueOf(ChannelPoolManager.class, "failedHealthCheck");

  /**
   * The number of threads in the eventloopgroup used by this class.
   * @return thread count
   * @deprecated due to confusing name. Use {@link #subpoolCount}
   */
  @Deprecated
  int executorCount();

  /**
   * The number of subpool threads used per pool by this class.
   * Must be less than or equal to the number of threads in the eventloopgroup.
   * Must be greater than zero.
   * @return thread count
   */
  default int subpoolCount() {
    return executorCount();
  }

  /**
   * Returns the total number of channels that are in use.
   * @return channel count
   */
  int activeCount();

  /**
   * Returns the total number of channels that are open.
   * @return channel count
   */
  int openConnections();

  /**
   * Acquire a channel from the pool.
   * @param hostNameAndPort host name and port
   * @param queueName queue name
   * @param qos priority
   * @return future for acquired channel
   */
  @CheckReturnValue
  @Nonnull
  Future<Channel> acquire(@Nonnull String hostNameAndPort, @Nonnull String queueName, @Nonnull QOS qos);

  /**
   * Acquire a channel from the pool.
   * @param hostNameAndPort host name and port
   * @param queueName queue name
   * @param qos priority
   * @return future for acquired channel
   */
  @CheckReturnValue
  @Nonnull
  Future<Channel> acquire(
      @Nonnull EventLoop eventLoop,
      @Nonnull String hostNameAndPort,
      @Nonnull String queueName,
      @Nonnull QOS qos);

  /**
   * Release the channel to the pool.
   * @param channel channel
   * @return future which is completed when done
   */
  @Nonnull
  Future<Void> release(@Nonnull Channel channel);

  /**
   * Open the connection pool to the specified host and port.
   * The default implementation simply acquires a channel from the pool and then immediately releases it.
   * @param hostNameAndPort Host and port string
   * @return future which is completed when done
   */
  @Nonnull
  default Future<Void> open(@Nonnull String hostNameAndPort) {
    Promise<Void> promise = ImmediateEventExecutor.INSTANCE.newPromise();
    acquire(hostNameAndPort, "default", QOS.NORMAL).addListener((Future<Channel> future) -> {
      if (future.isSuccess()) {
        release(future.getNow()).addListener(released -> {
          if (released.isSuccess()) {
            promise.setSuccess(null);
          } else {
            promise.setFailure(released.cause());
          }
        });
      } else {
        promise.setFailure(future.cause());
      }
    });
    return promise;
  }

  /**
   * Close the connection pool to the specified host and port.
   * @param hostNameAndPort Host and port string
   * @return future which is completed when done
   */
  @Nonnull
  Future<Void> close(@Nonnull String hostNameAndPort);

  /**
   * Close all open connections.
   * @return future which is completed when done
   */
  @Nonnull
  Future<Void> closeAll();

  /**
   * Obtain the PoolStats for the specified host and port.
   * @param hostNameAndPort Host and port string
   * @return Optional PoolStats
   */
  @CheckReturnValue
  @Nonnull
  Optional<PoolStats> getPoolStats(@Nonnull String hostNameAndPort);

  /**
   * Obtain the PoolStats for all connection pools.
   * @return Map of PoolStats
   */
  @CheckReturnValue
  @Nonnull
  Map<String, PoolStats> getPoolStats();

  interface Stats {
    /**
     * Name of the stats
     * @return stats name
     */
    @Nonnull
    String name();

    /**
     * The number of active (checked out) channels
     * @return channel count
     */
    int activeCount();

    /**
     * The number of pending requests for channels
     * @return waiting count
     */
    int waitingCount();

    /**
     * Call tracker for the {@link ChannelPoolManager#acquire(String, String, QOS)} method
     * @return call tracker
     */
    @Nonnull
    CallTracker acquireCallTracker();

    /**
     * Call tracker which tracks busy (checked out) channels
     * @return call tracker
     */
    @Nonnull
    CallTracker busyCallTracker();
  }

  interface PoolStats extends Stats {
    SocketAddress remoteAddress();

    /**
     * The number of open channels
     * @return channel count
     */
    int openConnections();

    long createCount();

    long closeCount();

    long closeErrorCount();

    long closeBadCount();

    default int inFlightCount() {
      return 0;
    }

    /**
     * Returns {@literal true} when at least the minimum number of connections exist in the pool.
     */
    boolean isHealthy();

    long totalActiveStreamCounts();

    long currentStreamChannelsReused();

    long totalStreamChannelsReused();

    default long totalStreamCreations() {
      return 0;
    }

    default long totalChannelReusePoolSize() {
      return 0;
    }

    default long getActiveStreamsLimitReachedCount() {
      return 0;
    }

    default long getTotalAcquireRetries() {
      return 0;
    }

    default long getTotalActiveStreamChannels() {
      return 0;
    }

    /**
     * Exposes Pool Stats
     * @return
     */
    default Map<String, ThreadPoolStats> getThreadPoolStats() {
      return Collections.emptyMap();
    }

    default double getAvgResponseTimeOfLatestPings() {
      return 0;
    }

    default CallTracker http2PingCallTracker() {
      return NullCallTracker.INSTANCE;
    }
  }

  /**
   * Represents IoWorker stats
   */
  interface ThreadPoolStats {
    int getMaxConnections();

    int getMaxPendingAcquires();

    int getAcquiredChannelCount();

    int getPendingAcquireCount();

    long getActiveStreamCount();

    long getActiveStreamChannelReUsed();

    long getStreamChannelReUsedCount();

    long getTotalStreamCreations();

    long getChannelReusePoolSize();

    long getActiveStreamsLimitReachedCount();

    long getTotalAcquireRetries();

    long getTotalActiveStreamChannels();

    boolean isClosed();

    int getConnectedChannels();

    int getH2ActiveConnections();
  }

  /**
   * Obtain the QueueStats for the specified host and port.
   * @param queueName Queue name
   * @return Optional QueueStats
   */
  @CheckReturnValue
  @Deprecated
  @Nonnull
  default Optional<QueueStats> getQueueStats(@Nonnull String queueName) {
    return Optional.empty();
  }

  /**
   * Obtain the QueueStats for all named queues.
   * @return Map of QueueStats
   */
  @CheckReturnValue
  @Deprecated
  @Nonnull
  default Map<String, QueueStats> getQueueStats() {
    return Collections.emptyMap();
  }

  interface QueueStats extends Stats {
  }
}
