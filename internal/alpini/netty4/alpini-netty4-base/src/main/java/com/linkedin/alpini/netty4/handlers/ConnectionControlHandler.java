package com.linkedin.alpini.netty4.handlers;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntSupplier;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;


/**
 * A {@code ChannelHandler} which regulates the number of open connections by disabling
 * the parent socket auto-read config when the number of active connections
 * exceeds the limit value.
 *
 * This avoids the nasty case where a client creates a connection and then expects
 * to be able to use a newly opened connection.
 *
 * When a flood of new connections are created, the actual number of active
 * connections may exceed the limit value; this may be controlled by setting
 * the {@code SO_BACKLOG} value on the server socket but due to thread context
 * switching, some overshoot is inevitable.
 *
 * If the connection limit value is set to a value less than 1, it is possible to
 * get into a state where there are no connections and the server auto-read
 * is {@code false}. If it is desirable to be able to have no connections, a
 * periodic task enabling auto-read on the server channel is advisable.
 *
 * @author acurtis on 4/14/17.
 */
@ChannelHandler.Sharable
public class ConnectionControlHandler extends ConnectionLimitHandler {
  private final LongAdder _activeCount = new LongAdder();
  private final LongAdder _inactiveCount = new LongAdder();
  private final Semaphore _activeSemaphore = new Semaphore(1);
  private final Semaphore _inactiveSemaphore = new Semaphore(1);

  public ConnectionControlHandler(@Nonnegative int connectionLimit) {
    this(() -> connectionLimit);
  }

  public ConnectionControlHandler(@Nonnull IntSupplier connectionLimit) {
    super(connectionLimit);
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    _activeCount.increment();

    Channel parent = ctx.channel().parent();
    if (parent != null && _activeSemaphore.tryAcquire()) {
      parent.eventLoop().submit(() -> {
        if (getConnectedCount() > getConnectionLimit()) {
          parent.config().setAutoRead(false);
        }
      }).addListener(ignored -> _activeSemaphore.release());
    }

    ctx.fireChannelActive();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    _inactiveCount.increment();

    Channel parent = ctx.channel().parent();
    if (parent != null && _inactiveSemaphore.tryAcquire()) {
      parent.eventLoop().submit(() -> {
        if (getConnectedCount() < Math.max(1, getConnectionLimit())) {
          parent.config().setAutoRead(true);
        }
      }).addListener(ignored -> _inactiveSemaphore.release());
    }

    ctx.fireChannelInactive();
  }

  public int getConnectedCount() {
    return (int) (_activeCount.longValue() - _inactiveCount.longValue());
  }
}
