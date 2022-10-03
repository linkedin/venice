package io.netty.bootstrap;

import com.linkedin.alpini.base.misc.Msg;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.GlobalEventExecutor;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The implementation of this method is mostly similar to DefaultChannelFuture except that
 * we store the Channel inside a CompletableFuture because it is not yet known at the time
 * of construction.
 *
 * Created by acurtis on 4/23/18.
 */
public class PendingConnectPromise extends DefaultPromise<Void> implements ChannelPromise {
  private static final Logger LOG = LogManager.getLogger(PendingConnectPromise.class);

  private final CompletableFuture<Channel> _channelFuture;

  public PendingConnectPromise(@Nonnull CompletableFuture<Channel> channelFuture) {
    _channelFuture = channelFuture;
    _channelFuture.handle(this::handleCompletion);
  }

  private Void handleCompletion(Channel channel, Throwable throwable) {
    LOG.debug("handleCompletion({}, {})", channel, Msg.make(throwable, String::valueOf));
    if (throwable != null) {
      super.setFailure(throwable);
    } else {
      super.setSuccess(null);
    }
    return null;
  }

  @Override
  protected EventExecutor executor() {
    return _channelFuture.isDone() && !_channelFuture.isCompletedExceptionally()
        ? channel().eventLoop()
        : globalExecutor();
  }

  private EventExecutor globalExecutor() {
    LOG.warn("not yet connected");
    return GlobalEventExecutor.INSTANCE;
  }

  @Override
  public Channel channel() {
    return _channelFuture.join();
  }

  @Override
  public ChannelPromise setSuccess() {
    throw new IllegalStateException();
  }

  @Override
  public ChannelPromise setSuccess(Void result) {
    throw new IllegalStateException();
  }

  @Override
  public boolean trySuccess() {
    throw new IllegalStateException();
  }

  @Override
  public ChannelPromise setFailure(Throwable cause) {
    if (_channelFuture.completeExceptionally(cause)) {
      return this;
    } else {
      throw new IllegalStateException();
    }
  }

  @Override
  public ChannelPromise addListener(GenericFutureListener<? extends Future<? super Void>> listener) {
    super.addListener(listener);
    return this;
  }

  @Override
  public ChannelPromise addListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
    super.addListeners(listeners);
    return this;
  }

  @Override
  public ChannelPromise removeListener(GenericFutureListener<? extends Future<? super Void>> listener) {
    super.removeListener(listener);
    return this;
  }

  @Override
  public ChannelPromise removeListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
    super.removeListeners(listeners);
    return this;
  }

  @Override
  public ChannelPromise sync() throws InterruptedException {
    super.sync();
    return this;
  }

  @Override
  public ChannelPromise syncUninterruptibly() {
    super.syncUninterruptibly();
    return this;
  }

  @Override
  public ChannelPromise await() throws InterruptedException {
    super.await();
    return this;
  }

  @Override
  public ChannelPromise awaitUninterruptibly() {
    super.awaitUninterruptibly();
    return this;
  }

  @Override
  protected void checkDeadLock() {
    if (_channelFuture.isDone() && channel().isRegistered()) {
      super.checkDeadLock();
    }
  }

  @Override
  public ChannelPromise unvoid() {
    return this;
  }

  @Override
  public boolean isVoid() {
    return false;
  }
}
