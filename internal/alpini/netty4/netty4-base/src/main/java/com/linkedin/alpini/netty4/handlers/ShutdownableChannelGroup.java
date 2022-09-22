package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.Preconditions;
import com.linkedin.alpini.base.registry.ShutdownableResource;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.EventExecutor;
import java.util.Optional;
import java.util.concurrent.TimeoutException;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class ShutdownableChannelGroup extends DefaultChannelGroup implements ShutdownableResource {
  private volatile Optional<ChannelGroupFuture> _shutdown = Optional.empty();

  /**
   * Creates a new group with a generated name and the provided {@link EventExecutor} to notify the
   * {@link ChannelGroupFuture}s.
   *
   * @param executor
   */
  public ShutdownableChannelGroup(EventExecutor executor) {
    super(executor);
  }

  /**
   * Creates a new group with the specified {@code name} and {@link EventExecutor} to notify the
   * {@link ChannelGroupFuture}s.  Please note that different groups can have the same name, which means no
   * duplicate check is done against group names.
   *
   * @param name
   * @param executor
   */
  public ShutdownableChannelGroup(String name, EventExecutor executor) {
    super(name, executor);
  }

  /**
   * Creates a new group with a generated name and the provided {@link EventExecutor} to notify the
   * {@link ChannelGroupFuture}s. {@code stayClosed} defines whether or not, this group can be closed
   * more than once. Adding channels to a closed group will immediately close them, too. This makes it
   * easy, to shutdown server and child channels at once.
   *
   * @param executor
   * @param stayClosed
   */
  public ShutdownableChannelGroup(EventExecutor executor, boolean stayClosed) {
    super(executor, stayClosed);
  }

  /**
   * Creates a new group with the specified {@code name} and {@link EventExecutor} to notify the
   * {@link ChannelGroupFuture}s. {@code stayClosed} defines whether or not, this group can be closed
   * more than once. Adding channels to a closed group will immediately close them, too. This makes it
   * easy, to shutdown server and child channels at once. Please note that different groups can have
   * the same name, which means no duplicate check is done against group names.
   *
   * @param name
   * @param executor
   * @param stayClosed
   */
  public ShutdownableChannelGroup(String name, EventExecutor executor, boolean stayClosed) {
    super(name, executor, stayClosed);
  }

  @Override
  public boolean equals(Object o) {
    return this == o;
  }

  @Override
  public boolean isShutdown() {
    return _shutdown.isPresent();
  }

  @Override
  public boolean isTerminated() {
    return _shutdown.filter(ChannelGroupFuture::isDone).isPresent();
  }

  @Override
  public synchronized void shutdown() {
    if (!_shutdown.isPresent()) {
      _shutdown = Optional.of(close());
    }
  }

  @Override
  public void waitForShutdown() throws InterruptedException, IllegalStateException {
    Preconditions.checkState(_shutdown.isPresent());
    _shutdown.get().await();
  }

  @Override
  public void waitForShutdown(long timeoutInMs) throws InterruptedException, IllegalStateException, TimeoutException {
    Preconditions.checkState(_shutdown.isPresent());
    _shutdown.get().await(timeoutInMs);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + _shutdown.hashCode();
    return result;
  }
}
