package com.linkedin.alpini.netty4.pool;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.net.InetSocketAddress;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;


/**
 * A simple interface for an asynchronous {@link InetSocketAddress} resolver.
 *
 * @author acurtis on 3/29/17.
 */
public interface ChannelPoolResolver {
  /**
   * Asynchronously resolves the supplied {@link InetSocketAddress} and completes the promise when done.
   * @param address address to resolve.
   * @param promise promise to complete.
   * @return promise
   */
  @CheckReturnValue
  @Nonnull
  Future<InetSocketAddress> resolve(@Nonnull InetSocketAddress address, @Nonnull Promise<InetSocketAddress> promise);
}
