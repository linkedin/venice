package com.linkedin.alpini.netty4.pool;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import javax.annotation.Nonnull;


/**
 * A simple asynchronous resolver which uses the default JVM blocking resolver.
 *
 * @author acurtis on 3/30/17.
 */
public class BasicDnsResolver implements ChannelPoolResolver {
  private final ThreadLocal<Executor> _executor = ThreadLocal.withInitial(Executors::newSingleThreadExecutor);

  @Override
  @Nonnull
  public Future<InetSocketAddress> resolve(
      @Nonnull InetSocketAddress address,
      @Nonnull Promise<InetSocketAddress> promise) {
    if (address.isUnresolved()) {
      _executor.get().execute(() -> {
        try {
          if (!promise.isDone()) {
            promise.setSuccess(new InetSocketAddress(address.getHostString(), address.getPort()));
          }
        } catch (Exception e) {
          promise.setFailure(e);
        }
      });
      return promise;
    } else {
      return promise.setSuccess(address);
    }
  }
}
