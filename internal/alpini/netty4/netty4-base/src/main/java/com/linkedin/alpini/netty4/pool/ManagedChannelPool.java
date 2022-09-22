package com.linkedin.alpini.netty4.pool;

import io.netty.channel.group.ChannelGroup;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.util.concurrent.Future;


public interface ManagedChannelPool extends ChannelPoolWithStats {
  /**
   * Returns the {@link ChannelPoolHandler} that will be notified for the different pool actions.
   *
   * @return the {@link ChannelPoolHandler} that will be notified for the different pool actions
   */
  ChannelPoolHandler handler();

  int getConnectedChannels();

  boolean isHealthy();

  Future<Void> closeFuture();

  boolean isClosing();

  default boolean isClosed() {
    return false;
  }

  default long getTotalActiveStreams() {
    return 0;
  }

  default long getCurrentStreamChannelsReused() {
    return 0;
  }

  default long getTotalStreamChannelsReused() {
    return 0;
  }

  default long getTotalStreamCreations() {
    return 0;
  }

  /**
   * Returns the current reuse channel pool size. Normally this should be 0 since we should be reusing as much
   * as possible.
   * @return current reuse channel pool size
   */
  default long getChannelReusePoolSize() {
    return 0;
  }

  default long getActiveStreamsLimitReachedCount() {
    return 0;
  }

  default int getH2ActiveConnections() {
    return -1;
  }

  default ChannelGroup getHttp2ChannelGroup() {
    return null;
  }

  default long getTotalAcquireRetries() {
    return 0;
  }

  default long getTotalActiveStreamChannels() {
    return 0;
  }
}
