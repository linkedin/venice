package com.linkedin.alpini.netty4.misc;

import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.util.concurrent.Future;
import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;


/**
 * A specialised {@link EventLoopGroup} which the {@link #next()} method prefers to return
 * an {@link EventLoop} for the current thread, if the current thread is a thread of the wrapped
 * {@linkplain MultithreadEventLoopGroup}.
 *
 * @author acurtis on 3/30/17.
 */
public class LocalThreadEventLoopGroup<E extends MultithreadEventLoopGroup> extends MultithreadEventLoopGroup
    implements SingleThreadEventLoopGroupSupplier {
  private final E _executor;
  private final ThreadLocal<Future<EventLoopGroup>> _singleThreadEventLoopGroupThreadLocal = new ThreadLocal<>();

  public LocalThreadEventLoopGroup(@Nonnull E executor) {
    super(executor.executorCount(), executor, StickyEventExecutorChooserFactory.INSTANCE, executor.iterator());
    _executor = executor;
  }

  private Future<EventLoopGroup> singleThreadEventLoopGroup0(EventLoop executor) {
    Future<EventLoopGroup> eventLoopGroup = _singleThreadEventLoopGroupThreadLocal.get();
    if (eventLoopGroup == null) {
      eventLoopGroup = executor.newSucceededFuture(new SingleThreadEventLoopGroup(executor));
      _singleThreadEventLoopGroupThreadLocal.set(eventLoopGroup);
    }
    return eventLoopGroup;
  }

  /**
   * Return a {@link EventLoopGroup} which represents a single thread.
   * @return future of {@linkplain EventLoopGroup}
   */
  @Nonnull
  public Future<EventLoopGroup> singleThreadGroup() {
    return singleThreadGroup(next());
  }

  /**
   * Return a {@link EventLoopGroup} which represents a single thread.
   * @return future of {@linkplain EventLoopGroup}
   */
  @Nonnull
  public Future<EventLoopGroup> singleThreadGroup(@Nonnull EventLoop executor) {
    assert StreamSupport.stream(_executor.spliterator(), false).anyMatch(e -> e == executor);
    if (executor.inEventLoop()) {
      return singleThreadEventLoopGroup0(executor);
    } else {
      return executor.submit(() -> singleThreadEventLoopGroup0(executor).getNow());
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  protected EventLoop newChild(Executor executor, Object... args) throws Exception {
    return ((Iterator<EventLoop>) args[0]).next();
  }

  @Override
  public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Future<?> terminationFuture() {
    return _executor.terminationFuture();
  }

  @Override
  public boolean isShuttingDown() {
    return _executor.isShuttingDown();
  }

  @Override
  public boolean isShutdown() {
    return _executor.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return _executor.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return _executor.awaitTermination(timeout, unit);
  }
}
