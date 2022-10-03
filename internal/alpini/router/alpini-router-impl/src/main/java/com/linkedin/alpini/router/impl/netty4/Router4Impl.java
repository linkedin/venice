package com.linkedin.alpini.router.impl.netty4;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.concurrency.AsyncPromise;
import com.linkedin.alpini.base.misc.ImmutableMapEntry;
import com.linkedin.alpini.base.registry.ResourceRegistry;
import com.linkedin.alpini.base.registry.SyncShutdownable;
import com.linkedin.alpini.netty4.handlers.ConnectionLimitHandler;
import com.linkedin.alpini.netty4.handlers.Http2SettingsFrameLogger;
import com.linkedin.alpini.netty4.handlers.ShutdownableChannelGroup;
import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.alpini.netty4.misc.Futures;
import com.linkedin.alpini.router.ScatterGatherRequestHandler;
import com.linkedin.alpini.router.ScatterGatherRequestHandler4;
import com.linkedin.alpini.router.api.Netty;
import com.linkedin.alpini.router.api.ResourcePath;
import com.linkedin.alpini.router.api.RouterTimeoutProcessor;
import com.linkedin.alpini.router.api.ScatterGatherHelper;
import com.linkedin.alpini.router.impl.Router;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http2.ActiveStreamsCountHandler;
import io.netty.util.Timer;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class Router4Impl<C extends Channel> implements Router, ResourceRegistry.Sync {
  private static final Logger LOG = LogManager.getLogger(Router.class);

  private static final Map<String, String> NETTY3_CHANNEL_OPTIONS = Collections.unmodifiableMap(
      Stream
          .of(
              ImmutableMapEntry.make("TCPNODELAY", ChannelOption.TCP_NODELAY.name()),
              ImmutableMapEntry.make("BACKLOG", ChannelOption.SO_BACKLOG.name()),
              ImmutableMapEntry.make("KEEPALIVE", ChannelOption.SO_KEEPALIVE.name()))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

  static {
    // This code ensures that all the Netty4 ChannelOptions are populated in Netty4's name map.
    LOG.info(
        "Netty4 Options: {}",
        Stream
            .concat(
                Stream.of(ChannelOption.class.getDeclaredFields()),
                Stream.of(EpollChannelOption.class.getDeclaredFields()))
            .filter(f -> Modifier.isStatic(f.getModifiers()))
            .filter(f -> Modifier.isFinal(f.getModifiers()))
            .filter(f -> Modifier.isPublic(f.getModifiers()))
            .filter(f -> ChannelOption.class.isAssignableFrom(f.getType()))
            .map(f -> {
              try {
                return (ChannelOption) f.get(null);
              } catch (IllegalAccessException e) {
                throw new Error(e);
              }
            })
            .map(ChannelOption::name)
            .collect(Collectors.joining(", ")));
  }

  private final ResourceRegistry _resourceRegistry = new ResourceRegistry();
  private final Supplier<ServerBootstrap> _bootstrapInitializer;
  private final ChannelGroup _channelGroup;
  private final ConnectionLimitHandler _connectionLimit;
  private final ActiveStreamsCountHandler _activeStreamsCountHandler;
  private final Http2SettingsFrameLogger _http2SettingsFrameLogger;
  private final Timer _nettyTimer;
  private Supplier<Router4PipelineFactory<C>> _pipelineSupplier;
  private AsyncFuture<SocketAddress> _localAddress;
  private boolean _busyAutoReadDisable;

  public <H, P extends ResourcePath<K>, K, R> Router4Impl(
      String name,
      Class<? extends ServerSocketChannel> channelClass,
      EventLoopGroup bossPool,
      EventLoopGroup workerPool,
      EventExecutor workerExecutor,
      ConnectionLimitHandler connectionLimit,
      ActiveStreamsCountHandler activeStreamsCountHandler,
      Http2SettingsFrameLogger http2SettingsFrameLogger,
      RouterTimeoutProcessor timeoutProcessor,
      Timer nettyTimer,
      Map<String, Object> serverSocketOptions,
      @Nonnull ScatterGatherHelper<H, P, K, R, BasicFullHttpRequest, FullHttpResponse, HttpResponseStatus> scatterGatherHelper) {

    _connectionLimit = Objects.requireNonNull(connectionLimit, "connectionLimit");

    _activeStreamsCountHandler = Objects.requireNonNull(activeStreamsCountHandler, "activeStreamsCountHandler");

    _http2SettingsFrameLogger = Objects.requireNonNull(http2SettingsFrameLogger, "http2SettingsFrameLogger");

    _channelGroup = _resourceRegistry
        .register(new ShutdownableChannelGroup(Objects.requireNonNull(name, "name"), workerExecutor, true));

    ScatterGatherRequestHandler4<H, P, K, R> scatterGatherRequestHandler =
        (ScatterGatherRequestHandler4<H, P, K, R>) ScatterGatherRequestHandler.make(
            Objects.requireNonNull(scatterGatherHelper),
            Objects.requireNonNull(timeoutProcessor, "timeoutProcessor"),
            workerExecutor);

    _nettyTimer = Objects.requireNonNull(nettyTimer, "nettyTimer");

    _pipelineSupplier = () -> constructRouterPipelineFactory(scatterGatherRequestHandler);

    _bootstrapInitializer = () -> {
      ServerBootstrap bootstrap = new ServerBootstrap().channel(Objects.requireNonNull(channelClass, "channelClass"))
          .group(bossPool, workerPool);

      for (Map.Entry<String, Object> entry: Optional.ofNullable(serverSocketOptions)
          .orElse(Collections.emptyMap())
          .entrySet()) {
        setChannelOption(bootstrap, entry.getKey(), entry.getValue());
      }

      return bootstrap;
    };
  }

  static <T> void setChannelOption(ServerBootstrap bootstrap, String optionName, T value) {
    String key;
    BiConsumer<ChannelOption<T>, T> option;
    if (optionName.toUpperCase().startsWith("CHILD.")) {
      key = optionName.substring(6);
      option = bootstrap::childOption;
    } else {
      key = optionName;
      option = bootstrap::option;
    }
    if (ChannelOption.exists(key.toUpperCase())) {
      key = key.toUpperCase();
    } else {
      key = NETTY3_CHANNEL_OPTIONS.getOrDefault(key.toUpperCase(), key);
    }
    if (ChannelOption.exists(key)) {
      ChannelOption<T> channelOption = ChannelOption.valueOf(key);
      try {
        int delim = channelOption.name().lastIndexOf('#');
        Class<?> parameterType = (Class) ((ParameterizedType) (delim > 0
            ? Class.forName(channelOption.name().substring(0, delim)).asSubclass(ChannelOption.class)
            : ChannelOption.class).getDeclaredField(channelOption.name().substring(delim + 1)).getGenericType())
                .getActualTypeArguments()[0];

        if (!Objects.isNull(value) && !parameterType.isAssignableFrom(value.getClass())) {
          throw new IllegalArgumentException(
              "channelOption " + optionName + " has an incompatible type value: " + value.getClass().getSimpleName());
        }
      } catch (NoSuchFieldException | ClassNotFoundException e) {
        LOG.warn("Unable to validate channel option type of {}", optionName, e);
      }
      channelOption.validate(value);
      option.accept(channelOption, value);
      LOG.info("Set channel option {} to {}", optionName, value);
    } else {
      LOG.warn("Unknown channel option: {}", optionName);
    }
  }

  public Supplier<Router4PipelineFactory<C>> getPipelineSupplier() {
    return _pipelineSupplier;
  }

  public void setPipelineSupplier(@Nonnull Supplier<Router4PipelineFactory<C>> pipelineSupplier) {
    _pipelineSupplier = Objects.requireNonNull(pipelineSupplier);
  }

  protected <H, P extends ResourcePath<K>, K, R> Router4PipelineFactory<C> constructRouterPipelineFactory(
      @Nonnull ScatterGatherRequestHandler4<H, P, K, R> scatterGatherRequestHandler) {
    return new Router4PipelineFactory<>(
        getConnectionLimit(),
        getActiveStreamsCountHandler(),
        getHttp2SettingsFrameLogger(),
        /*getWorkerExecutionHandler(),*/ getNettyTimer(),
        this::isShutdown,
        this::isBusyAutoReadDisable,
        scatterGatherRequestHandler);
  }

  @Override
  public Netty nettyVersion() {
    return Netty.NETTY_4_1;
  }

  protected ServerBootstrap bootstrap() {
    return _bootstrapInitializer.get();
  }

  @Override
  public AsyncFuture<SocketAddress> start(SocketAddress address) {
    LOG.info("Binding server channel to {}", address);
    ChannelFuture future = bootstrap().childHandler(_pipelineSupplier.get()).bind(address);
    AsyncPromise<SocketAddress> promise = AsyncFuture.deferred(false);
    future.addListener((ChannelFutureListener) f -> {
      if (f.isSuccess()) {
        _channelGroup.add(f.channel());
        _resourceRegistry.register((SyncShutdownable) () -> {
          LOG.info("Closing server channel: {}", f.channel());
          f.channel().close().syncUninterruptibly();
          LOG.info("Server channel {} is closed", f.channel());
        });
        LOG.info("Server channel bound to {}", f.channel().localAddress());
        promise.setSuccess(f.channel().localAddress());
      } else {
        promise.setFailure(f.cause());
      }
    });
    _localAddress = promise;
    return promise;
  }

  @Override
  public AsyncFuture<SocketAddress> getLocalAddress() {
    return _localAddress;
  }

  @Override
  public AsyncFuture<Void> setAcceptConnection(boolean enabled) {
    AsyncPromise<Void> promise = AsyncFuture.deferred(false);
    Futures
        .allOf(
            _channelGroup.stream()
                .map(channel -> channel.eventLoop().submit(() -> channel.config().setAutoRead(enabled), null))
                .toArray(Future[]::new))
        .addListener(future -> {
          if (future.isSuccess()) {
            promise.setSuccess(null);
          } else {
            promise.setFailure(future.cause());
          }
        });
    return promise;
  }

  @Override
  public int getConnectedCount() {
    return _connectionLimit.getConnectedCount();
  }

  @Override
  public int getActiveStreams() {
    return _activeStreamsCountHandler.getActiveStreamsCount();
  }

  @Override
  public long getRstErrorCount() {
    return _http2SettingsFrameLogger.getErrorCount();
  }

  public boolean isBusyAutoReadDisable() {
    return _busyAutoReadDisable;
  }

  public void setBusyAutoReadDisable(boolean busyAutoReadDisable) {
    _busyAutoReadDisable = busyAutoReadDisable;
  }

  public static void setBusyAutoReadDisable(Router router, boolean busyAutoReadDisable) {
    if (router instanceof Router4Impl) {
      ((Router4Impl) router).setBusyAutoReadDisable(busyAutoReadDisable);
    }
  }

  public static boolean isBusyAutoReadDisable(Router router) {
    return router instanceof Router4Impl && ((Router4Impl) router).isBusyAutoReadDisable();
  }

  /**
   * Returns <tt>true</tt> if this resource has been shut down.
   *
   * @return <tt>true</tt> if this resource has been shut down
   */
  @Override
  public boolean isShutdown() {
    return _resourceRegistry.isShutdown();
  }

  /**
   * Returns <tt>true</tt> if the resource has completed shutting down.
   * Note that <tt>isTerminated</tt> is never <tt>true</tt> unless
   * <tt>shutdown</tt> was called first.
   *
   * @return <tt>true</tt> if the resource has completed shutting down.
   */
  @Override
  public boolean isTerminated() {
    return _resourceRegistry.isTerminated();
  }

  /**
   * Starts the shutdown process.
   * It is recommended to perform the actual shutdown activity in a separate thread
   * from the thread that calls shutdown
   */
  @Override
  public void shutdown() {
    _resourceRegistry.register((SyncShutdownable) () -> setAcceptConnection(false).awaitUninterruptibly());
    _resourceRegistry.shutdown();
  }

  /**
   * Waits for shutdown to complete
   *
   * @throws InterruptedException  when the wait is interrupted
   * @throws IllegalStateException when the method is invoked when the shutdown has yet to be started
   */
  @Override
  public void waitForShutdown() throws InterruptedException, IllegalStateException {
    _resourceRegistry.waitForShutdown();
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
    _resourceRegistry.waitForShutdown();
  }

  public Timer getNettyTimer() {
    return _nettyTimer;
  }

  public ConnectionLimitHandler getConnectionLimit() {
    return _connectionLimit;
  }

  public ActiveStreamsCountHandler getActiveStreamsCountHandler() {
    return _activeStreamsCountHandler;
  }

  public Http2SettingsFrameLogger getHttp2SettingsFrameLogger() {
    return _http2SettingsFrameLogger;
  }
}
