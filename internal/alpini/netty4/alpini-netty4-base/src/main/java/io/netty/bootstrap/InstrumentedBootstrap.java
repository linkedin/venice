package io.netty.bootstrap;

import com.linkedin.alpini.base.monitoring.CallCompletion;
import com.linkedin.alpini.base.monitoring.CallTracker;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import java.net.SocketAddress;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nonnull;


/**
 * Because various netty methods and classes have been declared as {@code final},
 * we construct this pile of foo in order to be able to instrument the {@linkplain #connect()}
 * method invocation. This class is in the {@code io.netty.bootstrap} package in
 * order to access some methods of {@link Bootstrap} which have package access.
 *
 * @author acurtis on 3/30/17.
 */
public class InstrumentedBootstrap extends Bootstrap {
  private final Function<SocketAddress, CallTracker> _connectCallTracker;

  public InstrumentedBootstrap(@Nonnull CallTracker connectCallTracker) {
    this(ignored -> connectCallTracker);
  }

  public InstrumentedBootstrap(@Nonnull Function<SocketAddress, CallTracker> connectCallTracker) {
    _connectCallTracker = connectCallTracker;
  }

  protected InstrumentedBootstrap(InstrumentedBootstrap old) {
    _connectCallTracker = old._connectCallTracker;

    Optional.ofNullable(old.channelFactory()).ifPresent(this::channelFactory);
    Optional.ofNullable(old.handler()).ifPresent(this::handler);
    Optional.ofNullable(old.localAddress()).ifPresent(this::localAddress);
    synchronized (old.options0()) {
      options0().putAll(old.options0());
    }
    synchronized (old.attrs0()) {
      attrs0().putAll(old.attrs0());
    }

    resolver(old.resolver());
    remoteAddress(old.remoteAddress());
  }

  ChannelFuture trackConnect(Supplier<ChannelFuture> futureSupplier, CallCompletion completion) {
    ChannelFuture channelFuture = null;
    try {
      channelFuture = futureSupplier.get().addListener(f -> completion.closeCompletion(f.getNow(), f.cause()));
    } finally {
      if (channelFuture == null) {
        completion.closeWithError();
      }
    }
    return channelFuture;
  }

  CallTracker getCallTracker(SocketAddress remoteAddress) {
    return _connectCallTracker.apply(remoteAddress);
  }

  /**
   * Connect a {@link io.netty.channel.Channel} to the remote peer.
   */
  public ChannelFuture connect() {
    CallCompletion callCompletion = getCallTracker(remoteAddress()).startCall();
    return trackConnect(super::connect, callCompletion);
  }

  /**
   * Connect a {@link io.netty.channel.Channel} to the remote peer.
   */
  public ChannelFuture connect(SocketAddress remoteAddress) {
    CallCompletion callCompletion = getCallTracker(remoteAddress).startCall();
    return trackConnect(() -> super.connect(remoteAddress), callCompletion);
  }

  /**
   * Connect a {@link io.netty.channel.Channel} to the remote peer.
   */
  public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
    CallCompletion callCompletion = getCallTracker(remoteAddress).startCall();
    return trackConnect(() -> super.connect(remoteAddress, localAddress), callCompletion);
  }

  @Override
  @SuppressWarnings("CloneDoesntCallSuperClone")
  public Bootstrap clone() {
    return clone(group);
  }

  @Override
  public Bootstrap clone(EventLoopGroup group) {
    return new InstrumentedBootstrap(this).group(group);
  }
}
