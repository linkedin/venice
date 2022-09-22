package com.linkedin.alpini.netty4.misc;

import com.linkedin.alpini.base.registry.Shutdownable;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * Created by acurtis on 3/30/17.
 */
public class ShutdownableEventLoopGroup<E extends EventLoopGroup> implements EventLoopGroup, Shutdownable {
  private final E _eventLoopGroup;

  public ShutdownableEventLoopGroup(E eventLoop) {
    _eventLoopGroup = Objects.requireNonNull(eventLoop);
  }

  public E eventLoopGroup() {
    return _eventLoopGroup;
  }

  @Override
  public void waitForShutdown() throws InterruptedException, IllegalStateException {
    if (!awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)) {
      throw new IllegalStateException();
    }
  }

  @Override
  public void waitForShutdown(long timeoutInMs) throws InterruptedException, IllegalStateException, TimeoutException {
    if (!awaitTermination(timeoutInMs, TimeUnit.MILLISECONDS)) {
      throw new TimeoutException();
    }
  }

  @Override
  public boolean isShuttingDown() {
    return _eventLoopGroup.isShuttingDown();
  }

  @Override
  public Future<?> shutdownGracefully() {
    return _eventLoopGroup.shutdownGracefully();
  }

  @Override
  public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
    return _eventLoopGroup.shutdownGracefully(quietPeriod, timeout, unit);
  }

  @Override
  public Future<?> terminationFuture() {
    return _eventLoopGroup.terminationFuture();
  }

  @Override
  public void shutdown() {
    _eventLoopGroup.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return _eventLoopGroup.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return _eventLoopGroup.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return _eventLoopGroup.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return _eventLoopGroup.awaitTermination(timeout, unit);
  }

  @Override
  public EventLoop next() {
    return _eventLoopGroup.next();
  }

  @Override
  public Iterator<EventExecutor> iterator() {
    return _eventLoopGroup.iterator();
  }

  @Override
  public Future<?> submit(Runnable task) {
    return _eventLoopGroup.submit(task);
  }

  @Override
  public <T> List<java.util.concurrent.Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    return _eventLoopGroup.invokeAll(tasks);
  }

  @Override
  public <T> List<java.util.concurrent.Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks,
      long timeout,
      TimeUnit unit) throws InterruptedException {
    return _eventLoopGroup.invokeAll(tasks, timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
    return _eventLoopGroup.invokeAny(tasks);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return _eventLoopGroup.invokeAny(tasks, timeout, unit);
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return _eventLoopGroup.submit(task, result);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return _eventLoopGroup.submit(task);
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
    return _eventLoopGroup.schedule(command, delay, unit);
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
    return _eventLoopGroup.schedule(callable, delay, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
    return _eventLoopGroup.scheduleAtFixedRate(command, initialDelay, period, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
    return _eventLoopGroup.scheduleWithFixedDelay(command, initialDelay, delay, unit);
  }

  @Override
  public ChannelFuture register(Channel channel) {
    return _eventLoopGroup.register(channel);
  }

  @Override
  public ChannelFuture register(ChannelPromise promise) {
    return _eventLoopGroup.register(promise);
  }

  @Override
  public ChannelFuture register(Channel channel, ChannelPromise promise) {
    return _eventLoopGroup.register(channel, promise);
  }

  @Override
  public void execute(Runnable command) {
    _eventLoopGroup.execute(command);
  }
}
