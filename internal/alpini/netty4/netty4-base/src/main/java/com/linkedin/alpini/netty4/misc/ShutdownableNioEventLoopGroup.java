package com.linkedin.alpini.netty4.misc;

import com.linkedin.alpini.base.registry.Shutdownable;
import io.netty.channel.SelectStrategyFactory;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.EventExecutorChooserFactory;
import io.netty.util.concurrent.RejectedExecutionHandler;
import java.nio.channels.spi.SelectorProvider;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class ShutdownableNioEventLoopGroup extends NioEventLoopGroup implements Shutdownable {
  /**
   * Create a new instance using the default number of threads, the default {@link ThreadFactory} and
   * the {@link SelectorProvider} which is returned by {@link SelectorProvider#provider()}.
   */
  public ShutdownableNioEventLoopGroup() {
  }

  /**
   * Create a new instance using the specified number of threads, {@link ThreadFactory} and the
   * {@link SelectorProvider} which is returned by {@link SelectorProvider#provider()}.
   *
   * @param nThreads
   */
  public ShutdownableNioEventLoopGroup(int nThreads) {
    super(nThreads);
  }

  /**
   * Create a new instance using the specified number of threads, the given {@link ThreadFactory} and the
   * {@link SelectorProvider} which is returned by {@link SelectorProvider#provider()}.
   *
   * @param nThreads
   * @param threadFactory
   */
  public ShutdownableNioEventLoopGroup(int nThreads, ThreadFactory threadFactory) {
    super(nThreads, threadFactory);
  }

  public ShutdownableNioEventLoopGroup(int nThreads, Executor executor) {
    super(nThreads, executor);
  }

  /**
   * Create a new instance using the specified number of threads, the given {@link ThreadFactory} and the given
   * {@link SelectorProvider}.
   *
   * @param nThreads
   * @param threadFactory
   * @param selectorProvider
   */
  public ShutdownableNioEventLoopGroup(int nThreads, ThreadFactory threadFactory, SelectorProvider selectorProvider) {
    super(nThreads, threadFactory, selectorProvider);
  }

  public ShutdownableNioEventLoopGroup(
      int nThreads,
      ThreadFactory threadFactory,
      SelectorProvider selectorProvider,
      SelectStrategyFactory selectStrategyFactory) {
    super(nThreads, threadFactory, selectorProvider, selectStrategyFactory);
  }

  public ShutdownableNioEventLoopGroup(int nThreads, Executor executor, SelectorProvider selectorProvider) {
    super(nThreads, executor, selectorProvider);
  }

  public ShutdownableNioEventLoopGroup(
      int nThreads,
      Executor executor,
      SelectorProvider selectorProvider,
      SelectStrategyFactory selectStrategyFactory) {
    super(nThreads, executor, selectorProvider, selectStrategyFactory);
  }

  public ShutdownableNioEventLoopGroup(
      int nThreads,
      Executor executor,
      EventExecutorChooserFactory chooserFactory,
      SelectorProvider selectorProvider,
      SelectStrategyFactory selectStrategyFactory) {
    super(nThreads, executor, chooserFactory, selectorProvider, selectStrategyFactory);
  }

  public ShutdownableNioEventLoopGroup(
      int nThreads,
      Executor executor,
      EventExecutorChooserFactory chooserFactory,
      SelectorProvider selectorProvider,
      SelectStrategyFactory selectStrategyFactory,
      RejectedExecutionHandler rejectedExecutionHandler) {
    super(nThreads, executor, chooserFactory, selectorProvider, selectStrategyFactory, rejectedExecutionHandler);
  }

  /**
   * Waits for shutdown to complete
   *
   * @throws InterruptedException  when the wait is interrupted
   * @throws IllegalStateException when the method is invoked when the shutdown has yet to be started
   */
  @Override
  public void waitForShutdown() throws InterruptedException, IllegalStateException {
    super.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
  }

  /**
   * Waits for shutdown to complete with a timeout
   *
   * @param timeoutInMs number of milliseconds to wait before throwing TimeoutException
   * @throws InterruptedException  when the wait is interrupted
   * @throws IllegalStateException when the method is invoked when the shutdown has yet to be started
   * @throws TimeoutException      when the operation times out
   */
  @Override
  public void waitForShutdown(long timeoutInMs) throws InterruptedException, IllegalStateException, TimeoutException {
    if (!super.awaitTermination(timeoutInMs, TimeUnit.MILLISECONDS)) {
      throw new TimeoutException();
    }
  }
}
