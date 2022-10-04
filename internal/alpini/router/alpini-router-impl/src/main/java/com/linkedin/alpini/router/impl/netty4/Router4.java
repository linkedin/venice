package com.linkedin.alpini.router.impl.netty4;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.alpini.base.misc.CollectionUtil;
import com.linkedin.alpini.base.misc.Preconditions;
import com.linkedin.alpini.base.registry.ResourceRegistry;
import com.linkedin.alpini.base.registry.Shutdownable;
import com.linkedin.alpini.base.registry.ShutdownableExecutors;
import com.linkedin.alpini.base.registry.ShutdownableResource;
import com.linkedin.alpini.netty4.handlers.ConnectionControlHandler;
import com.linkedin.alpini.netty4.handlers.ConnectionLimitHandler;
import com.linkedin.alpini.netty4.handlers.Http2SettingsFrameLogger;
import com.linkedin.alpini.netty4.http2.Http2PipelineInitializer;
import com.linkedin.alpini.netty4.misc.ShutdownableHashedWheelTimer;
import com.linkedin.alpini.netty4.misc.ShutdownableNioEventLoopGroup;
import com.linkedin.alpini.router.ScatterGatherRequestHandler4;
import com.linkedin.alpini.router.api.Netty;
import com.linkedin.alpini.router.api.ResourcePath;
import com.linkedin.alpini.router.api.RouterTimeoutProcessor;
import com.linkedin.alpini.router.api.ScatterGatherHelper;
import com.linkedin.alpini.router.impl.Router;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http2.ActiveStreamsCountHandler;
import io.netty.handler.logging.LogLevel;
import io.netty.util.Timer;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.GlobalEventExecutor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class Router4<C extends Channel> implements Router.Builder, Router.PipelineFactory<ChannelHandler> {
  private static final int TICKS_PER_WHEEL = 4096;

  private final ScatterGatherHelper _scatterGatherHelper;
  private String _name = "Router";
  private ThreadFactory _threadFactory = Executors.defaultThreadFactory();
  private ResourceRegistry _registry;
  private int _bossPoolSize = 1;
  private int _ioWorkerPoolSize = 1;
  private Class<? extends ServerSocketChannel> _serverSocketChannel = NioServerSocketChannel.class;
  private Function<Executor, ? extends EventLoopGroup> _bossPoolBuilder =
      executor -> register(new ShutdownableNioEventLoopGroup(_bossPoolSize, executor));
  private Function<Executor, ? extends EventLoopGroup> _ioWorkerPoolBuilder =
      executor -> register(new ShutdownableNioEventLoopGroup(_ioWorkerPoolSize, executor));
  private Executor _executor;
  private IntSupplier _connectionLimit = () -> Integer.MAX_VALUE;
  private RouterTimeoutProcessor _timeoutProcessor;
  private final Map<String, Object> _serverSocketOptions = new HashMap<>();
  private BooleanSupplier _shutdownFlag;
  private Executor _workerExecutor;
  private int _appWorkerCorePoolSize;
  private long _appWorkerChannelMaxMemorySize;
  private long _appWorkerTotalMaxMemorySize;
  private long _appWorkerKeepAliveSeconds;
  private final List<Function<Router4PipelineFactory<C>, Router4PipelineFactory<C>>> _factoryModifiers =
      new ArrayList<>(Collections.singletonList(Function.identity()));
  private Function<ServerBootstrap, ServerBootstrap> _bootstrapFilter = Function.identity();

  public Router4(@Nonnull ScatterGatherHelper scatterGatherHelper) {
    assert scatterGatherHelper.dispatcherNettyVersion() == Netty.NETTY_4_1;
    _scatterGatherHelper = scatterGatherHelper;
  }

  @Override
  public Netty nettyVersion() {
    return Netty.NETTY_4_1;
  }

  @Override
  public Router.Builder name(@Nonnull String name) {
    _name = Objects.requireNonNull(name, "name");
    return this;
  }

  @Override
  public Router.Builder resourceRegistry(@Nonnull ResourceRegistry resourceRegistry) {
    _registry = Objects.requireNonNull(resourceRegistry, "resourceRegistry");
    return this;
  }

  @Override
  public Router.Builder threadFactory(@Nonnull ThreadFactory threadFactory) {
    _threadFactory = Objects.requireNonNull(threadFactory, "threadFactory");
    return this;
  }

  @Override
  public Router.Builder serverSocketChannel(@Nonnull Class<?> serverSocketChannel) {
    _serverSocketChannel = serverSocketChannel.asSubclass(ServerSocketChannel.class);
    return this;
  }

  @Override
  public Router.Builder bossPoolSize(int bossPoolSize) {
    _bossPoolSize = Preconditions.notLessThan(bossPoolSize, 1, "bossPoolSize");
    return this;
  }

  @Override
  public Router.Builder ioWorkerPoolSize(int ioWorkerPoolSize) {
    _ioWorkerPoolSize = Preconditions.notLessThan(ioWorkerPoolSize, 1, "ioWorkerPoolSize");
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <POOL_TYPE> Router.Builder ioWorkerPoolBuilder(
      @Nonnull Class<POOL_TYPE> poolClass,
      @Nonnull Function<Executor, POOL_TYPE> builder) {
    poolClass.asSubclass(EventLoopGroup.class);
    _ioWorkerPoolBuilder = Objects.requireNonNull((Function) builder, "ioWorkerPoolBuilder");
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <POOL_TYPE> Router.Builder bossPoolBuilder(
      @Nonnull Class<POOL_TYPE> poolClass,
      @Nonnull Function<Executor, POOL_TYPE> builder) {
    poolClass.asSubclass(EventLoopGroup.class);
    _bossPoolBuilder = Objects.requireNonNull((Function) builder, "bossPoolBuilder");
    return this;
  }

  @Override
  public Router.Builder executor(@Nonnull Executor executor) {
    _executor = Objects.requireNonNull(executor, "executor");
    return this;
  }

  @Override
  public Router.Builder workerExecutor(@Nonnull Executor workerExecutor) {
    _workerExecutor = Objects.requireNonNull(workerExecutor, "workerExecutor");
    return this;
  }

  @Override
  public Router.Builder appWorkerCorePoolSize(int corePoolSize) {
    _appWorkerCorePoolSize = Preconditions.notLessThan(corePoolSize, 0, "appWorkerCorePoolSize");
    return this;
  }

  @Override
  public Router.Builder appWorkerChannelMaxMemorySize(long maxChannelMemorySize) {
    _appWorkerChannelMaxMemorySize =
        Preconditions.notLessThan(maxChannelMemorySize, 0, "appWorkerChannelMaxMemorySize");
    return this;
  }

  @Override
  public Router.Builder appWorkerTotalMaxMemorySize(long maxTotalMemorySize) {
    _appWorkerTotalMaxMemorySize = Preconditions.notLessThan(maxTotalMemorySize, 0, "appWorkerTotalMaxMemorySize");
    return this;
  }

  @Override
  public Router.Builder appWorkerKeepAliveSeconds(long appWorkerKeepAliveSeconds) {
    _appWorkerKeepAliveSeconds = Preconditions.notLessThan(appWorkerKeepAliveSeconds, 0, "appWorkerKeepAliveSeconds");
    return this;
  }

  @Override
  public Router.Builder timeoutProcessor(@Nonnull RouterTimeoutProcessor timeoutProcessor) {
    _timeoutProcessor = Objects.requireNonNull(timeoutProcessor, "timeoutProcessor");
    return this;
  }

  @Override
  public Router.Builder connectionLimit(int connectionLimit) {
    Preconditions.notLessThan(connectionLimit, 1, "connectionLimit");
    return connectionLimit(() -> connectionLimit);
  }

  @Override
  public Router.Builder connectionLimit(@Nonnull IntSupplier connectionLimit) {
    _connectionLimit = Objects.requireNonNull(connectionLimit);
    return this;
  }

  @Override
  public Router.Builder serverSocketOptions(Map<String, Object> serverSocketOptions) {
    if (serverSocketOptions != null) {
      _serverSocketOptions.putAll(serverSocketOptions);
    }
    return this;
  }

  @Override
  public Router.Builder serverSocketOptions(@Nonnull String key, Object value) {
    if (value == null) {
      _serverSocketOptions.remove(key);
    } else {
      _serverSocketOptions.put(Objects.requireNonNull(key, "serverSocketOptions"), value);
    }
    return this;
  }

  @Override
  public Router.Builder maxHeaderSize(@Nonnegative int maxHeaderSize) {
    Preconditions.notLessThan(maxHeaderSize, MINIMUM_MAX_HEADER_SIZE, "maxHeaderSize");
    _factoryModifiers.add(factory -> factory.maxHeaderSize(maxHeaderSize));
    return this;
  }

  @Override
  public Router.Builder maxInitialLineLength(@Nonnegative int maxInitialLineLength) {
    Preconditions.notLessThan(maxInitialLineLength, MINIMUM_MAX_INITIAL_LINE_LENGTH, "maxInitialLineLength");
    _factoryModifiers.add(factory -> factory.maxInitialLineLength(maxInitialLineLength));
    return this;
  }

  @Override
  public Router.Builder maxChunkSize(@Nonnegative int maxChunkSize) {
    Preconditions.notLessThan(maxChunkSize, MINIMUM_MAX_CHUNK_SIZE, "maxChunkSize");
    _factoryModifiers.add(factory -> factory.maxChunkSize(maxChunkSize));
    return this;
  }

  @Override
  public Router.Builder maxContentLength(@Nonnegative long maxContentLength) {
    Preconditions.notLessThan(maxContentLength, MINIMUM_MAX_CONTENT_LENGTH, "maxContentLength");
    _factoryModifiers.add(factory -> factory.maxContentLength(maxContentLength));
    return this;
  }

  @Override
  public Router.Builder idleTimeout(@Nonnegative long time, @Nonnull TimeUnit unit) {
    long timeoutMillis = Preconditions
        .notLessThan(Objects.requireNonNull(unit, "unit").toMillis(time), MINIMUM_IDLE_TIMEOUT_MILLIS, "time");
    _factoryModifiers.add(factory -> factory.idleConnectionTimeoutMillis(timeoutMillis));
    return this;
  }

  @Override
  public Router.Builder handshakeTimeout(@Nonnegative long time, @Nonnull TimeUnit unit) {
    long timeoutMillis = Preconditions
        .notLessThan(Objects.requireNonNull(unit, "unit").toMillis(time), MINIMUM_IDLE_TIMEOUT_MILLIS, "time");
    _factoryModifiers.add(factory -> factory.handshakeConnectionTimeoutMillis(timeoutMillis));
    return this;
  }

  @Override
  public Router.Builder enableInboundHttp2(boolean enableHttp2) {
    _factoryModifiers.add(factory -> factory.enableInboundHttp2(enableHttp2));
    return this;
  }

  @Override
  public Router.Builder useCustomMultiplexHandler(boolean useCustomMultiplexHandler) {
    _factoryModifiers.add(factory -> factory.useCustomMultiplexHandler(useCustomMultiplexHandler));
    return this;
  }

  @Override
  public Router.Builder http2MaxConcurrentStreams(int http2MaxConcurrentStreams) {
    _factoryModifiers.add(factory -> factory.http2MaxConcurrentStreams(http2MaxConcurrentStreams));
    return this;
  }

  @Override
  public Router.Builder http2MaxFrameSize(int http2MaxFrameSize) {
    _factoryModifiers.add(factory -> factory.http2MaxFrameSize(http2MaxFrameSize));
    return this;
  }

  @Override
  public Router.Builder http2InitialWindowSize(int http2InitialWindowSize) {
    _factoryModifiers.add(factory -> factory.http2InitialWindowSize(http2InitialWindowSize));
    return this;
  }

  @Override
  public Router.Builder http2HeaderTableSize(int http2HeaderTableSize) {
    _factoryModifiers.add(factory -> factory.http2HeaderTableSize(http2HeaderTableSize));
    return this;
  }

  @Override
  public Router.Builder http2MaxHeaderListSize(int http2MaxHeaderListSize) {
    _factoryModifiers.add(factory -> factory.http2MaxHeaderListSize(http2MaxHeaderListSize));
    return this;
  }

  @Override
  public <T> Router.Builder addBootstrapFilter(Function<T, T> function) {
    // noinspection unchecked
    _bootstrapFilter = boot -> (ServerBootstrap) function.apply((T) boot);
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <CHANNEL_HANDLER> Router.PipelineFactory<CHANNEL_HANDLER> pipelineFactory(
      @Nonnull Class<CHANNEL_HANDLER> handlerClass) {
    handlerClass.asSubclass(ChannelHandler.class);
    return (Router.PipelineFactory) this;
  }

  @SuppressWarnings("unchecked")
  private static <CHANNEL_PIPELINE> Consumer<ChannelPipeline> consumer(
      Class<CHANNEL_PIPELINE> pipelineClass,
      Consumer<CHANNEL_PIPELINE> consumer) {
    pipelineClass.asSubclass(ChannelPipeline.class);
    return Objects.requireNonNull((Consumer) consumer, "consumer");
  }

  private <CHANNEL_PIPELINE> Router4 add(
      Class<CHANNEL_PIPELINE> pipelineClass,
      Consumer<CHANNEL_PIPELINE> consumer,
      Function<Consumer<ChannelPipeline>, Function<Router4PipelineFactory<C>, Router4PipelineFactory<C>>> modifier) {
    _factoryModifiers.add(modifier.apply(consumer(pipelineClass, consumer)));
    return this;
  }

  @Override
  public <CHANNEL_PIPELINE> Router.Builder beforeHttpServerCodec(
      @Nonnull Class<CHANNEL_PIPELINE> pipelineClass,
      @Nonnull Consumer<CHANNEL_PIPELINE> consumer) {
    return add(
        pipelineClass,
        consumer,
        pipelineConsumer -> factory -> factory.addBeforeHttpServerCodec(pipelineConsumer));
  }

  @Override
  public <CHANNEL_PIPELINE> Router.Builder beforeChunkAggregator(
      @Nonnull Class<CHANNEL_PIPELINE> pipelineClass,
      @Nonnull Consumer<CHANNEL_PIPELINE> consumer) {
    return add(
        pipelineClass,
        consumer,
        pipelineConsumer -> factory -> factory.addBeforeChunkAggregator(pipelineConsumer));
  }

  @Override
  public <CHANNEL_PIPELINE> Router.Builder beforeIdleStateHandler(
      @Nonnull Class<CHANNEL_PIPELINE> pipelineClass,
      @Nonnull Consumer<CHANNEL_PIPELINE> consumer) {
    return add(
        pipelineClass,
        consumer,
        pipelineConsumer -> factory -> factory.addBeforeIdleStateHandler(pipelineConsumer));
  }

  @Override
  public <CHANNEL_PIPELINE> Router.Builder beforeHttpRequestHandler(
      @Nonnull Class<CHANNEL_PIPELINE> pipelineClass,
      @Nonnull Consumer<CHANNEL_PIPELINE> consumer) {
    return add(
        pipelineClass,
        consumer,
        pipelineConsumer -> factory -> factory.addBeforeHttpRequestHandler(pipelineConsumer));
  }

  private Router.PipelineFactory<ChannelHandler> addModifier(
      String name,
      Supplier<? extends ChannelHandler> supplier,
      Function<Router4PipelineFactory<C>, Router4PipelineFactory<C>> modifier) {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(supplier, "supplier");
    _factoryModifiers.add(modifier);
    return this;
  }

  @Override
  public Router.PipelineFactory<ChannelHandler> addBeforeHttpServerCodec(
      @Nonnull String name,
      @Nonnull Supplier<? extends ChannelHandler> supplier) {
    return addModifier(name, supplier, factory -> factory.addBeforeHttpServerCodec(name, supplier));
  }

  @Override
  public Router.PipelineFactory<ChannelHandler> addBeforeChunkAggregator(
      @Nonnull String name,
      @Nonnull Supplier<? extends ChannelHandler> supplier) {
    return addModifier(name, supplier, factory -> factory.addBeforeChunkAggregator(name, supplier));
  }

  @Override
  public Router.PipelineFactory<ChannelHandler> addBeforeIdleStateHandler(
      @Nonnull String name,
      @Nonnull Supplier<? extends ChannelHandler> supplier) {
    return addModifier(name, supplier, factory -> factory.addBeforeIdleStateHandler(name, supplier));
  }

  @Override
  public Router.PipelineFactory<ChannelHandler> addBeforeHttpRequestHandler(
      @Nonnull String name,
      @Nonnull Supplier<? extends ChannelHandler> supplier) {
    return addModifier(name, supplier, factory -> factory.addBeforeHttpRequestHandler(name, supplier));
  }

  @Override
  public Router.Builder builder() {
    return this;
  }

  /*private <T extends ExternalResourceReleasable> T register(T resource) {
  _registry.register(ExternalResourceReleaseableAdapter.wrap(resource));
  return resource;
  }*/

  private <T extends Shutdownable> T register(T resource) {
    return _registry.register(resource);
  }

  public <R extends ShutdownableResource, F extends ResourceRegistry.Factory<R>> F factory(@Nonnull Class<F> clazz) {
    return _registry.factory(clazz);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Router build() {
    Preconditions.checkState(_registry != null, "resourceRegistry is not set");

    ThreadFactory threadFactory = Optional.ofNullable(_threadFactory).orElseGet(Executors::defaultThreadFactory);

    Executor executor = Optional.ofNullable(_executor)
        .orElseGet(() -> factory(ShutdownableExecutors.class).newCachedThreadPool(threadFactory));

    /*Executor workerExecutor = Optional.ofNullable(_workerExecutor).orElseGet(() -> register(
        new ShutdownableOrderedMemoryAwareExecutor(
            _appWorkerCorePoolSize, _appWorkerChannelMaxMemorySize, _appWorkerTotalMaxMemorySize,
            _appWorkerKeepAliveSeconds, TimeUnit.SECONDS, threadFactory)));*/

    EventLoopGroup ioWorkerPool = Objects.requireNonNull(_ioWorkerPoolBuilder.apply(executor), "ioWorkerPool");
    EventLoopGroup bossPool = Objects.requireNonNull(_bossPoolBuilder.apply(executor), "bossPool");

    Timer nettyTimer =
        register(new ShutdownableHashedWheelTimer(threadFactory, 937, TimeUnit.MICROSECONDS, TICKS_PER_WHEEL));

    RouterTimeoutProcessor timeoutProcessor =
        Optional.ofNullable(_timeoutProcessor).orElseGet(() -> new TimerTimeoutProcessor(nettyTimer));

    ConnectionLimitHandler connectionLimit = new ConnectionControlHandler(_connectionLimit);

    ActiveStreamsCountHandler activeStreamsCountHandler = new ActiveStreamsCountHandler();

    Http2SettingsFrameLogger http2SettingsFrameLogger = new Http2SettingsFrameLogger(LogLevel.INFO, "server");

    List<Function<Router4PipelineFactory<C>, Router4PipelineFactory<C>>> modifiers = new ArrayList<>(_factoryModifiers);
    Map<String, Object> serverSocketOptions = new HashMap<>(_serverSocketOptions);

    // Handle option for Http2PipelineInitializer
    Object option = serverSocketOptions.remove(Http2PipelineInitializer.class.getName());
    if (option != null) {
      Http2PipelineInitializer.BuilderSupplier builderSupplier;
      if (option instanceof Class) {
        final Class<? extends Http2PipelineInitializer> clazz = (Class<? extends Http2PipelineInitializer>) option;
        builderSupplier = Http2PipelineInitializer.builderOf(clazz);
      } else {
        builderSupplier = (Http2PipelineInitializer.BuilderSupplier) option;
      }
      modifiers.add(factory -> factory.setHttp2PipelineInitializer(builderSupplier));
    }

    EventExecutor workerExecutor = GlobalEventExecutor.INSTANCE; // for now.
    final Function<ServerBootstrap, ServerBootstrap> bootstrapFilter = _bootstrapFilter;

    return _registry.register(
        new Router4Impl<C>(
            _name,
            _serverSocketChannel,
            bossPool,
            ioWorkerPool,
            workerExecutor,
            connectionLimit,
            activeStreamsCountHandler,
            http2SettingsFrameLogger,
            timeoutProcessor,
            nettyTimer,
            serverSocketOptions.isEmpty() ? null : serverSocketOptions,
            _scatterGatherHelper) {
          @Override
          protected <H, P extends ResourcePath<K>, K, R> Router4PipelineFactory<C> constructRouterPipelineFactory(
              @Nonnull ScatterGatherRequestHandler4<H, P, K, R> scatterGatherRequestHandler) {
            return CollectionUtil
                .applyFactoryModifiers(super.constructRouterPipelineFactory(scatterGatherRequestHandler), modifiers);
          }

          @Override
          protected ServerBootstrap bootstrap() {
            return bootstrapFilter.apply(super.bootstrap());
          }
        });
  }
}
