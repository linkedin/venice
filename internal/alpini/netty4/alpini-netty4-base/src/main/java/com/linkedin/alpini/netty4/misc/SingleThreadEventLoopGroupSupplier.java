package com.linkedin.alpini.netty4.misc;

import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;
import javax.annotation.Nonnull;


/**
 * Created by acurtis on 3/30/17.
 */
public interface SingleThreadEventLoopGroupSupplier extends EventLoopGroup {
  @Nonnull
  Future<EventLoopGroup> singleThreadGroup();

  @Nonnull
  Future<EventLoopGroup> singleThreadGroup(@Nonnull EventLoop eventLoop);
}
