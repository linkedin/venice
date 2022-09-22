package com.linkedin.alpini.netty4.pool;

import com.linkedin.alpini.netty4.handlers.Http2PingSendHandler;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.ChannelPoolHandler;
import java.net.InetSocketAddress;
import java.util.function.Function;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;


/**
 * An interface to construct {@link ManagedChannelPool} instances for the {@link ChannelPoolManager}.
 *
 * @author acurtis on 3/29/17.
 */
public interface ChannelPoolFactory {
  /**
   * Constructs a new ChannelPool which must connect to the specified {@code address}.
   * @param manager The channel pool manager which will manage the pool.
   * @param handler A handler to be notified of events.
   * @param eventLoop The eventloop for the connection pool.
   * @param address Target address/port for the connection pool.
   * @return New instance of {@link ManagedChannelPool}
   */
  @CheckReturnValue
  ManagedChannelPool construct(
      @Nonnull ChannelPoolManager manager,
      @Nonnull ChannelPoolHandler handler,
      @Nonnull EventLoopGroup eventLoop,
      @Nonnull InetSocketAddress address);

  default void setHttp2PingSendHandlerFunction(Function<Channel, Http2PingSendHandler> pingSendHandlerFn) {
  }
}
