package io.netty.bootstrap;

import com.linkedin.alpini.base.misc.ExceptionUtil;
import com.linkedin.alpini.base.misc.Msg;
import com.linkedin.alpini.base.misc.Pair;
import com.linkedin.alpini.base.monitoring.CallCompletion;
import com.linkedin.alpini.base.monitoring.CallTracker;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.resolver.AddressResolver;
import io.netty.resolver.AddressResolverGroup;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import java.net.SocketAddress;
import java.nio.channels.UnresolvedAddressException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The existing Netty Bootstrap has a limitation regarding connecting to remote hosts
 * where the hostname resolves to more than one IP address.
 * It will only try one resolved address and then fail if the connection cannot be established.
 *
 * This is a problem for us: If we have a service which is bound to the IPv4 address and the IPv6
 * address is added to the DNS, attempts to connect to the machine are likely to fail.
 *
 * This bootstrap will attempt to do the right thing by attempting to connect using each of the
 * resolved IP addresses. A sort comparator may be provided to encourage preference in the order
 * of connect attempts. To conform to de-facto standards, one should provide a comparator which
 * will sort IPv6 addresses first.
 *
 * Created by acurtis on 4/20/18.
 */
public class ResolveAllBootstrap extends InstrumentedBootstrap {
  private static final Logger LOG = LogManager.getLogger(ResolveAllBootstrap.class);

  public static final AttributeKey<Comparator<SocketAddress>> SORT_KEY =
      AttributeKey.valueOf(ResolveAllBootstrap.class, "sortKey");

  private final Function<SocketAddress, CallTracker> _resolveCallTracker;

  /**
   * Constructor for the bootstrap.
   * @param connectCallTracker CallTracker for connect operations.
   * @param resolveCallTracker CallTracker for resolveAll operations.
   */
  public ResolveAllBootstrap(@Nonnull CallTracker connectCallTracker, @Nonnull CallTracker resolveCallTracker) {
    this(address -> connectCallTracker, address -> resolveCallTracker);
  }

  /**
   * Constructor for the bootstrap
   * @param connectCallTracker provide CallTracker for connect operations. Function must not return null.
   * @param resolveCallTracker provide CallTracker for resolveAll operations. Function must not return null.
   */
  public ResolveAllBootstrap(
      @Nonnull Function<SocketAddress, CallTracker> connectCallTracker,
      @Nonnull Function<SocketAddress, CallTracker> resolveCallTracker) {
    super(connectCallTracker);
    _resolveCallTracker = resolveCallTracker;
  }

  protected ResolveAllBootstrap(ResolveAllBootstrap old) {
    super(old);
    _resolveCallTracker = old._resolveCallTracker;
  }

  public Bootstrap sortResolvedAddress(@Nonnull Comparator<SocketAddress> comparator) {
    return attr(SORT_KEY, comparator);
  }

  @Override
  public ChannelFuture connect() {
    validate();
    SocketAddress remoteAddress = remoteAddress();
    if (remoteAddress == null) {
      throw new IllegalStateException("remoteAddress not set");
    }
    return doResolveAndConnect(remoteAddress, config().localAddress());
  }

  @Override
  public ChannelFuture connect(SocketAddress remoteAddress) {
    Objects.requireNonNull(remoteAddress, "remoteAddress");
    validate();
    return doResolveAndConnect(remoteAddress, config().localAddress());
  }

  @Override
  public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
    Objects.requireNonNull(remoteAddress, "remoteAddress");
    validate();
    return doResolveAndConnect(remoteAddress, localAddress);
  }

  @Nonnull
  private CompletionStage<Channel> awaitRegistration(ChannelFuture regFuture) {
    CompletableFuture<Channel> reg = new CompletableFuture<>();
    if (regFuture.isDone()) {
      if (!regFuture.isSuccess()) {
        reg.obtrudeException(regFuture.cause());
      } else {
        reg.obtrudeValue(regFuture.channel());
      }
    } else {
      regFuture.addListener((ChannelFutureListener) future -> {
        // Directly obtain the cause and do a null check so we only need one volatile read in case of a
        // failure.
        Throwable cause = future.cause();
        if (cause != null) {
          // Registration on the EventLoop failed so fail the ChannelPromise directly to not cause an
          // IllegalStateException once we try to access the EventLoop of the Channel.
          reg.completeExceptionally(cause);
        } else {
          // Registration was successful, so set the correct executor to use.
          // See https://github.com/netty/netty/issues/2586
          reg.complete(future.channel());
        }
      });
    }
    return reg;
  }

  @Nonnull
  private ChannelFuture doResolveAndConnect(SocketAddress remoteAddress, SocketAddress localAddress) {
    LOG.debug("doResolveAndConnect({}, {})", remoteAddress, localAddress);
    return new PendingConnectPromise(
        CompletableFuture.completedFuture(initAndRegister())
            .thenCompose(this::awaitRegistration)
            .thenCompose(channel -> doResolve(channel, remoteAddress))
            .thenApply(this::doSortAddresses)
            .thenCompose(
                channelAndAddress -> doConnect(
                    channelAndAddress.getFirst(),
                    channelAndAddress.getSecond().iterator(),
                    localAddress)));
  }

  @Nonnull
  private CompletionStage<Channel> doConnect(
      Channel channel,
      Iterator<SocketAddress> remoteAddressIterator,
      SocketAddress localAddress) {
    SocketAddress remoteAddress = remoteAddressIterator.next();

    CompletableFuture<Channel> connect = new CompletableFuture<>();
    final CallCompletion callCompletion = getCallTracker(remoteAddress).startCall();

    (localAddress == null ? channel.connect(remoteAddress) : channel.connect(remoteAddress, localAddress))
        .addListener(future -> {
          callCompletion.closeCompletion(future.getNow(), future.cause());
          if (future.isSuccess()) {
            if (!connect.complete(channel)) {
              channel.close();
            }
          } else {
            Throwable cause = ExceptionUtil.unwrapCompletion(future.cause());
            if (remoteAddressIterator.hasNext() && !connect.isDone()) {
              LOG.info(
                  "Failed to connect to {}, retrying with next address. Cause: {}",
                  remoteAddress,
                  cause.getMessage());
              // channel would already be closed. we need to register a new channel and try again.
              CompletableFuture.completedFuture(initAndRegister())
                  .thenCompose(this::awaitRegistration)
                  .thenCompose(nextChannel -> doConnect(nextChannel, remoteAddressIterator, localAddress))
                  .handle((connectedChannel, throwable) -> {
                    if (throwable != null) {
                      connect.completeExceptionally(throwable);
                    } else if (!connect.complete(connectedChannel)) {
                      connectedChannel.close();
                    }
                    return null;
                  });
            } else {
              if (!connect.completeExceptionally(cause)) {
                LOG.warn("Failed to connect to {}", remoteAddress, cause);
              }
            }
          }
        });
    return connect;
  }

  @SuppressWarnings("unchecked")
  private AddressResolverGroup<SocketAddress> resolverGroup() {
    return (AddressResolverGroup<SocketAddress>) resolver();
  }

  private CompletionStage<Pair<Channel, List<SocketAddress>>> doResolve(
      final Channel channel,
      SocketAddress remoteAddress) {
    final EventLoop eventLoop = channel.eventLoop();
    final AddressResolver<SocketAddress> resolver = resolverGroup().getResolver(eventLoop);

    if (!resolver.isSupported(remoteAddress) || resolver.isResolved(remoteAddress)) {
      return CompletableFuture.completedFuture(Pair.make(channel, Collections.singletonList(remoteAddress)));
    }

    CompletableFuture<Pair<Channel, List<SocketAddress>>> resolve = new CompletableFuture<>();
    final CallCompletion callCompletion = _resolveCallTracker.apply(remoteAddress).startCall();
    resolver.resolveAll(remoteAddress).addListener((Future<List<SocketAddress>> resolveFuture) -> {
      final Throwable resolveFailureCause = resolveFuture.cause();
      callCompletion.closeCompletion(resolveFuture.getNow(), resolveFailureCause);
      LOG.debug("doResolve complete {} {}", resolveFuture.getNow(), Msg.make(resolveFailureCause, String::valueOf));

      if (resolveFailureCause != null || resolveFuture.getNow().isEmpty()) {
        // Failed to resolve immediately
        channel.close();
        resolve.completeExceptionally(
            resolveFailureCause == null ? new UnresolvedAddressException() : resolveFailureCause);
      } else {
        resolve.complete(Pair.make(channel, resolveFuture.getNow()));
      }
    });
    return resolve;
  }

  @Nonnull
  private Pair<Channel, List<SocketAddress>> doSortAddresses(
      @Nonnull Pair<Channel, List<SocketAddress>> channelListPair) {
    if (channelListPair.getSecond().size() < 2 || !channelListPair.getFirst().hasAttr(SORT_KEY)) {
      return channelListPair;
    }
    List<SocketAddress> list = new ArrayList<>(channelListPair.getSecond());
    list.sort(channelListPair.getFirst().attr(SORT_KEY).get());
    return Pair.make(channelListPair.getFirst(), list);
  }

  @Override
  public Bootstrap clone(EventLoopGroup group) {
    return new ResolveAllBootstrap(this).group(group);
  }
}
