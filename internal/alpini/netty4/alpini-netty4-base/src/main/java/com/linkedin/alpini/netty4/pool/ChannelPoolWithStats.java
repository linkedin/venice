package com.linkedin.alpini.netty4.pool;

import io.netty.channel.pool.ChannelPool;


/**
 * A simple interface which extends ChannelPool to expose various stats.
 */
public interface ChannelPoolWithStats extends ChannelPool {
  String name();

  int getMaxConnections();

  int getMaxPendingAcquires();

  int getAcquiredChannelCount();

  int getPendingAcquireCount();

  boolean isClosed();
}
