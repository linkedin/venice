package com.linkedin.alpini.router.api;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.concurrency.AsyncFutureListener;
import com.linkedin.alpini.base.concurrency.AsyncPromise;
import com.linkedin.alpini.base.misc.BasicRequest;
import com.linkedin.alpini.base.misc.Headers;
import com.linkedin.alpini.base.misc.Metrics;
import com.linkedin.alpini.base.misc.Pair;
import com.linkedin.alpini.router.monitoring.ScatterGatherStats;
import io.netty.channel.ChannelHandler;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class ScatterGatherHelper<H, P extends ResourcePath<K>, K, R, BASIC_HTTP_REQUEST extends BasicRequest, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> {
  final @Nonnull ExtendedResourcePathParser<P, K, BASIC_HTTP_REQUEST> _pathParser;
  volatile @Nonnull HostFinder<H, R> _hostFinder;
  private final @Nonnull HostHealthMonitor<H> _hostHealthMonitor;
  private final @Nonnull AsyncPartitionFinder<K> _partitionFinder;
  private final @Nonnull RoleFinder<R> _roleFinder;
  private final @Nonnull ScatterGatherMode _broadcastMode;
  private final @Nonnull ScatterGatherMode _scatterMode;
  private final @Nonnull PartitionDispatchHandler<H, P, K, BASIC_HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> _dispatchHandler;
  private final @Nullable ResponseAggregatorFactory<BASIC_HTTP_REQUEST, HTTP_RESPONSE> _responseAggregatorFactory;
  private final @Nonnull Function<Headers, Long> _requestTimeout;
  private final @Nonnull LongTailRetrySupplier<P, K> _longTailRetrySupplier;
  private final @Nonnull Function<BasicRequest, Metrics> _metricsProvider;
  private final @Nonnull BiFunction<Headers, Metrics, Headers> _metricsDecorator;
  private final @Nonnull Function<Headers, Metrics> _responseMetrics;
  private final @Nonnull Function<P, ScatterGatherStats> _scatterGatherStatsProvider;
  private final @Nonnull List<Pair<String, Supplier<?>>> _beforeHttpServerCodec;
  private final @Nonnull List<Pair<String, Supplier<?>>> _beforeChunkAggregator;
  private final @Nonnull List<Pair<String, Supplier<?>>> _beforeIdleStateHandler;
  private final @Nonnull List<Pair<String, Supplier<?>>> _beforeHttpRequestHandler;
  private final @Nonnull IntPredicate _successCodePredicate;
  private final @Nonnull RequestRetriableChecker<P, R, HTTP_RESPONSE_STATUS> _requestRetriableChecker;
  private long _defaultTimeoutMillis;
  private long _dispatchMinimumMillis;
  private long _longTailMinimumMillis;
  private final boolean _enableStackTraceResponseForException;
  private final @Nonnull BooleanSupplier _enableRetryRequestAlwaysUseADifferentHost;
  private final @Nonnull BooleanSupplier _disableRetryOnTimeout;
  private final @Nonnull BooleanSupplier _isReqRedirectionAllowedForQuery;

  protected ScatterGatherHelper(
      @Nonnull ExtendedResourcePathParser<P, K, BASIC_HTTP_REQUEST> pathParser,
      @Nonnull AsyncPartitionFinder<K> partitionFinder,
      @Nonnull HostFinder<H, R> hostFinder,
      @Nonnull HostHealthMonitor<H> hostHealthMonitor,
      @Nonnull RoleFinder<R> roleFinder,
      @Nonnull ScatterGatherMode broadcastMode,
      @Nonnull ScatterGatherMode scatterMode,
      @Nonnull PartitionDispatchHandler<H, P, K, BASIC_HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> dispatchHandler,
      @Nonnull Optional<ResponseAggregatorFactory<BASIC_HTTP_REQUEST, HTTP_RESPONSE>> responseAggregatorFactory,
      @Nonnull Function<Headers, Long> requestTimeout,
      @Nonnull LongTailRetrySupplier<P, K> longTailRetrySupplier,
      @Nonnull Function<BasicRequest, Metrics> metricsProvider,
      @Nonnull BiFunction<Headers, Metrics, Headers> metricsDecorator,
      @Nonnull Function<Headers, Metrics> responseMetrics,
      @Nonnull Function<P, ScatterGatherStats> scatterGatherStatsProvider,
      long defaultTimeoutMillis,
      long dispatchMinimumMillis,
      long longTailMinimumMillis,
      @Nonnull List<Pair<String, Supplier<?>>> beforeHttpServerCodec,
      @Nonnull List<Pair<String, Supplier<?>>> beforeChunkAggregator,
      @Nonnull List<Pair<String, Supplier<?>>> beforeIdleStateHandler,
      @Nonnull List<Pair<String, Supplier<?>>> beforeHttpRequestHandler,
      @Nonnull IntPredicate successCodePredicate,
      @Nonnull RequestRetriableChecker<P, R, HTTP_RESPONSE_STATUS> requestRetriableChecker,
      boolean enableStackTraceResponseForException,
      @Nonnull BooleanSupplier enableRetryRequestAlwaysUseADifferentHost,
      @Nonnull BooleanSupplier disableRetryOnTimeout,
      @Nonnull BooleanSupplier isReqRedirectionAllowedForQuery) {
    _pathParser = Objects.requireNonNull(pathParser, "pathParser");
    _partitionFinder = Objects.requireNonNull(partitionFinder, "partitionFinder");
    _hostFinder = Objects.requireNonNull(hostFinder, "hostFinder").getSnapshot();
    _hostHealthMonitor = Objects.requireNonNull(hostHealthMonitor, "hostHealthMonitor");
    _roleFinder = Objects.requireNonNull(roleFinder, "roleFinder");
    _broadcastMode = Objects.requireNonNull(broadcastMode, "broadcastMode");
    _scatterMode = Objects.requireNonNull(scatterMode, "scatterMode");
    _dispatchHandler = Objects.requireNonNull(dispatchHandler, "dispatchHandler");
    _responseAggregatorFactory = responseAggregatorFactory.orElse(null);
    _requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
    _longTailRetrySupplier = Objects.requireNonNull(longTailRetrySupplier, "longTailRetrySupplier");
    _metricsProvider = Objects.requireNonNull(metricsProvider, "metricsProvider");
    _metricsDecorator = Objects.requireNonNull(metricsDecorator, "metricsDecorator");
    _responseMetrics = Objects.requireNonNull(responseMetrics, "responseMetrics");
    _scatterGatherStatsProvider = Objects.requireNonNull(scatterGatherStatsProvider, "scatterGatherStatsProvider");
    _defaultTimeoutMillis = defaultTimeoutMillis > 0
        ? defaultTimeoutMillis
        : illegalArgumentException("defaultTimeoutMillis must be more than 0");
    _dispatchMinimumMillis = dispatchMinimumMillis > 0
        ? dispatchMinimumMillis
        : illegalArgumentException("dispatchMinimumMillis must be more than 0");
    _longTailMinimumMillis = longTailMinimumMillis > 0
        ? longTailMinimumMillis
        : illegalArgumentException("longTailMinimumMillis must be more than 0");
    _beforeHttpServerCodec = new ArrayList<>(Objects.requireNonNull(beforeHttpServerCodec, "beforeHttpServerCodec"));
    _beforeChunkAggregator = new ArrayList<>(Objects.requireNonNull(beforeChunkAggregator, "beforeChunkAggregator"));
    _beforeIdleStateHandler = new ArrayList<>(Objects.requireNonNull(beforeIdleStateHandler, "beforeIdleStateHandler"));
    _beforeHttpRequestHandler =
        new ArrayList<>(Objects.requireNonNull(beforeHttpRequestHandler, "beforeHttpRequestHandler"));
    _successCodePredicate = Objects.requireNonNull(successCodePredicate, "successCodePredicate");
    _requestRetriableChecker = Objects.requireNonNull(requestRetriableChecker, "requestRetriableChecker");

    _hostFinder.getChangeFuture().addListener(new AsyncFutureListener<HostFinder<H, R>>() {
      @Override
      public void operationComplete(AsyncFuture<HostFinder<H, R>> future) throws Exception {
        if (future.isSuccess()) {
          (_hostFinder = future.getNow().getSnapshot()).getChangeFuture().addListener(this); // SUPPRESS CHECKSTYLE
                                                                                             // InnerAssignment
        }
      }
    });
    _enableStackTraceResponseForException = enableStackTraceResponseForException;
    _enableRetryRequestAlwaysUseADifferentHost =
        Objects.requireNonNull(enableRetryRequestAlwaysUseADifferentHost, "enableRetryRequestAlwaysUseADifferentHost");
    _disableRetryOnTimeout = Objects.requireNonNull(disableRetryOnTimeout, "disableRetryOnTimeout");
    _isReqRedirectionAllowedForQuery =
        Objects.requireNonNull(isReqRedirectionAllowedForQuery, "isReqRedirectionAllowedForQuery");
  }

  private Stream<Pair<String, ?>> streamOf(List<Pair<String, Supplier<?>>> list) {
    return list.stream().map(p -> Pair.make(p.getFirst(), p.getSecond().get()));
  }

  public void forEachBeforeHttpServerCodec(@Nonnull Consumer<Pair<String, ?>> consumer) {
    streamOf(_beforeHttpServerCodec).forEach(consumer);
  }

  public void forEachBeforeChunkAggregator(@Nonnull Consumer<Pair<String, ?>> consumer) {
    streamOf(_beforeChunkAggregator).forEach(consumer);
  }

  public void forEachBeforeIdleStateHandler(@Nonnull Consumer<Pair<String, ?>> consumer) {
    streamOf(_beforeIdleStateHandler).forEach(consumer);
  }

  public void forEachBeforeHttpRequestHandler(@Nonnull Consumer<Pair<String, ?>> consumer) {
    streamOf(_beforeHttpRequestHandler).forEach(consumer);
  }

  public final @Nonnull P parseResourceUri(@Nonnull String uri, @Nonnull BASIC_HTTP_REQUEST request)
      throws RouterException {
    return _pathParser.parseResourceUri(uri, request);
  }

  /**
   * To check if a host is healthy/responsive.
   *
   * @param hostName the host name, including the service port
   * @return the host is healthy or not
   */
  public final boolean isHostHealthy(@Nonnull H hostName, @Nonnull String partitionName) {
    return _hostHealthMonitor.isHostHealthy(hostName, partitionName);
  }

  public final AsyncFuture<LongSupplier> getLongTailRetryMilliseconds(P path, String methodName) {
    return _longTailRetrySupplier.getLongTailRetryMilliseconds(path, methodName);
  }

  public final AsyncFuture<LongSupplier> getLongTailRetryMilliseconds(P path, BasicRequest request) {
    return _longTailRetrySupplier.getLongTailRetryMilliseconds(path, request);
  }

  public final void dispatch(
      @Nonnull Scatter<H, P, K> scatter,
      @Nonnull ScatterGatherRequest<H, K> part,
      @Nonnull P path,
      @Nonnull BASIC_HTTP_REQUEST request,
      @Nonnull AsyncPromise<H> hostSelected,
      @Nonnull AsyncPromise<List<HTTP_RESPONSE>> responseFuture,
      @Nonnull AsyncPromise<HTTP_RESPONSE_STATUS> retryFuture,
      @Nonnull AsyncFuture<Void> timeoutFuture,
      @Nonnull Executor contextExecutor) throws RouterException {
    _dispatchHandler.dispatch(
        scatter,
        part,
        path,
        request,
        hostSelected,
        responseFuture,
        retryFuture,
        timeoutFuture,
        contextExecutor);
  }

  public Netty dispatcherNettyVersion() {
    return Arrays.stream(_dispatchHandler.getClass().getInterfaces())
        .map(cls -> Optional.ofNullable(cls.getAnnotation(NettyVersion.class)))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .map(NettyVersion::value)
        .findFirst()
        .orElseThrow(IllegalStateException::new);
  }

  public @Nonnull CompletionStage<Scatter<H, P, K>> scatter(
      @Nonnull String requestMethod,
      @Nonnull P path,
      @Nonnull Headers headers,
      @Nonnull HostHealthMonitor<H> hostHealthMonitor,
      Metrics metrics,
      String initialHost) throws RouterException {
    requestMethod = Objects.requireNonNull(requestMethod, "request");
    R roles = parseRoles(requestMethod, Objects.requireNonNull(headers, "headers"));
    Scatter<H, P, K> scatter = new Scatter<>(path, _pathParser, roles);
    String resourceName = path.getResourceName();
    HostFinder<H, R> hostFinder = _hostFinder.getSnapshot();
    ScatterGatherMode mode = path.getPartitionKeys().isEmpty() ? _broadcastMode : _scatterMode;
    return mode.scatter(scatter, requestMethod, resourceName, _partitionFinder, hostFinder, hostHealthMonitor, roles);
  }

  public @Nonnull HTTP_RESPONSE aggregateResponse(
      @Nonnull BASIC_HTTP_REQUEST request,
      Metrics metrics,
      @Nonnull List<HTTP_RESPONSE> responses,
      @Nonnull ResponseAggregatorFactory<BASIC_HTTP_REQUEST, HTTP_RESPONSE> defaultAggregator) {
    if (_responseAggregatorFactory == null) {
      return defaultAggregator.buildResponse(request, metrics, responses);
    }
    return _responseAggregatorFactory.buildResponse(request, metrics, responses);
  }

  public CompletionStage<String> findPartitionName(String resourceName, K key) {
    return _partitionFinder.findPartitionName(resourceName, key);
  }

  public long getRequestTimeout(@Nonnull Headers headers) {
    Long timeout = _requestTimeout.apply(headers);
    return timeout != null && timeout > 0 ? timeout : getDefaultTimeoutMillis();
  }

  public long getDefaultTimeoutMillis() {
    return _defaultTimeoutMillis;
  }

  public void setDefaultTimeoutMillis(long timeoutMillis) {
    if (timeoutMillis < 100) {
      throw new IllegalArgumentException("timeoutMillis should be greater than 100 milliseconds");
    }
    _defaultTimeoutMillis = timeoutMillis;
  }

  public long getDispatchMinimumMillis() {
    return _dispatchMinimumMillis;
  }

  public void setDispatchMinimumMillis(long value) {
    if (value < 1) {
      throw new IllegalArgumentException("value");
    }
    _dispatchMinimumMillis = value;
  }

  public long getLongTailMinimumMillis() {
    return _longTailMinimumMillis;
  }

  public void setLongTailMinimumMillis(long value) {
    if (value < 1) {
      throw new IllegalArgumentException("value");
    }
    _longTailMinimumMillis = value;
  }

  public boolean isEnableStackTraceResponseForException() {
    return _enableStackTraceResponseForException;
  }

  public boolean isEnableRetryRequestAlwaysUseADifferentHost() {
    return _enableRetryRequestAlwaysUseADifferentHost.getAsBoolean();
  }

  // when returning true the behavior is not retrying timed out requests
  public boolean disableRetryOnTimeout() {
    return _disableRetryOnTimeout.getAsBoolean();
  }

  public boolean isReqRedirectionAllowedForQuery() {
    return _isReqRedirectionAllowedForQuery.getAsBoolean();
  }

  public void decorateResponse(@Nonnull Headers responseHeaders, @Nonnull Headers requestHeaders, Metrics metrics) {
    if (metrics != null) {
      responseHeaders.set(_metricsDecorator.apply(requestHeaders, metrics));
    }
  }

  public Metrics initializeMetrics(@Nonnull BasicRequest request) {
    return _metricsProvider.apply(request);
  }

  public Metrics responseMetrics(@Nonnull Headers headers) {
    return _responseMetrics.apply(headers);
  }

  public @Nonnull ScatterGatherStats getScatterGatherStatsByPath(P path) {
    return _scatterGatherStatsProvider.apply(path);
  }

  public static Builder<?, ?, ?, ?, ?, ?, ?> builder() {
    return new Builder<>();
  }

  private static final HostHealthMonitor NULL_HOST_HEALTH_MONITOR = (hostName, partitionName) -> true;

  public boolean isSuccessStatus(int code) {
    return _successCodePredicate.test(code);
  }

  private static boolean defaultSuccessPredicate(int code) {
    return code >= 200 && code < 500 && code != 429;
  }

  public boolean isRequestRetriable(@Nonnull P path, @Nonnull R role, @Nonnull HTTP_RESPONSE_STATUS status) {
    return _requestRetriableChecker.isRequestRetriable(path, role, status);
  }

  public R parseRoles(@Nonnull String requestMethod, @Nonnull Headers headers) {
    return _roleFinder.parseRole(requestMethod, headers);
  }

  public static class Builder<H, P extends ResourcePath<K>, K, R, HTTP_REQUEST extends BasicRequest, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> {
    private ExtendedResourcePathParser<P, K, HTTP_REQUEST> _pathParser;
    private HostFinder<H, R> _hostFinder;
    @SuppressWarnings("unchecked")
    private HostHealthMonitor<H> _hostHealthMonitor = NULL_HOST_HEALTH_MONITOR;
    private AsyncPartitionFinder<K> _partitionFinder;
    private RoleFinder<R> _roleFinder;
    private ScatterGatherMode _broadcastMode = ScatterGatherMode.BROADCAST_BY_PARTITION;
    private ScatterGatherMode _scatterMode = ScatterGatherMode.GROUP_BY_PARTITION;
    private PartitionDispatchHandler<H, P, K, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> _dispatchHandler;
    private Optional<ResponseAggregatorFactory<HTTP_REQUEST, HTTP_RESPONSE>> _responseAggregatorFactory =
        Optional.empty();
    private Function<Headers, Long> _requestTimeout = headers -> null;
    private LongTailRetrySupplier<P, K> _longTailRetrySupplier = (resourceName, methodName) -> AsyncFuture.cancelled();
    private Function<BasicRequest, Metrics> _metricsProvider = http -> null;
    private Function<Headers, Metrics> _responseMetrics = headers -> null;
    private BiFunction<Headers, Metrics, Headers> _metricsDecorator = (headers, metrics) -> Headers.EMPTY_HEADERS;
    private Function<P, ScatterGatherStats> _scatterGatherStatsProvider = path -> new ScatterGatherStats();
    private List<Pair<String, Supplier<?>>> _beforeHttpServerCodec = new ArrayList<>();
    private List<Pair<String, Supplier<?>>> _beforeChunkAggregator = new ArrayList<>();
    private List<Pair<String, Supplier<?>>> _beforeIdleStateHandler = new ArrayList<>();
    private List<Pair<String, Supplier<?>>> _beforeHttpRequestHandler = new ArrayList<>();
    private IntPredicate _successCodePredicate = ScatterGatherHelper::defaultSuccessPredicate;
    private RequestRetriableChecker<P, R, HTTP_RESPONSE_STATUS> _requestRetriableChecker = (p, r, hrs) -> true;
    private long _defaultTimeoutMillis = 1000L;
    private long _dispatchMinimumMillis = 4L;
    private long _longTailMinimumMillis = 2L;
    private boolean _enableStackTraceResponseForException = false;
    private BooleanSupplier _enableRetryRequestAlwaysUseADifferentHost = () -> false;
    private BooleanSupplier _disableRetryOnTimeout = () -> false;
    private BooleanSupplier _isReqRedirectionAllowedForQuery = () -> true;

    private Builder() {
    }

    public <PATH extends ResourcePath<KEY>, KEY> Builder<H, PATH, KEY, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> pathParser(
        @Nonnull ResourcePathParser<PATH, KEY> pathParser) {
      return pathParserExtended(ExtendedResourcePathParser.wrap(Objects.requireNonNull(pathParser, "pathParser")));
    }

    public <PATH extends ResourcePath<KEY>, KEY> Builder<H, PATH, KEY, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> pathParserExtended(
        @Nonnull ExtendedResourcePathParser<PATH, KEY, HTTP_REQUEST> pathParser) {
      Builder<H, PATH, KEY, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> builder = cast();
      builder._pathParser = builder._pathParser == null
          ? Objects.requireNonNull(pathParser, "pathParser")
          : illegalStateException("pathParser already set");
      return builder;
    }

    private Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> setRoleFinder(
        @Nonnull RoleFinder<R> roleFinder) {
      _roleFinder = _roleFinder == null ? roleFinder : illegalStateException("roleFinder already set");
      return this;
    }

    public <ROLE> Builder<H, P, K, ROLE, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> roleFinder(
        @Nonnull RoleFinder<ROLE> roleFinder) {
      Builder<H, P, K, ROLE, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> builder = cast();
      return builder.setRoleFinder(Objects.requireNonNull(roleFinder, "roleFinder"));
    }

    Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> setHostFinder(
        @Nonnull HostFinder<H, R> hostFinder) {
      _hostFinder = _hostFinder == null ? hostFinder : illegalStateException("hostFinder already set");
      return this;
    }

    public <HOST> Builder<HOST, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> hostFinder(
        @Nonnull HostFinder<HOST, R> hostFinder) {
      Builder<HOST, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> builder = cast();
      return builder.setHostFinder(hostFinder);
    }

    public Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> partitionFinder(
        @Nonnull AsyncPartitionFinder<K> partitionFinder) {
      checkIsSet(_pathParser, "pathParser not set");
      _partitionFinder = _partitionFinder == null
          ? Objects.requireNonNull(partitionFinder, "partitionFinder")
          : illegalStateException("partitionFinder already set");
      return this;
    }

    public Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> partitionFinder(
        @Nonnull PartitionFinder<K> partitionFinder) {
      return partitionFinder(partitionFinder, Runnable::run);
    }

    public Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> partitionFinder(
        @Nonnull PartitionFinder<K> partitionFinder,
        Executor executor) {
      return partitionFinder(AsyncPartitionFinder.adapt(partitionFinder, Objects.requireNonNull(executor, "executor")));
    }

    public Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> hostHealthMonitor(
        @Nonnull HostHealthMonitor<H> hostHealthMonitor) {
      checkIsSet(_hostFinder, "hostFinder not set");
      _hostHealthMonitor = _hostHealthMonitor == NULL_HOST_HEALTH_MONITOR
          ? Objects.requireNonNull(hostHealthMonitor, "hostHealthMonitor")
          : illegalStateException("hostHealthMonitor already set");
      return this;
    }

    public Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> broadcastMode(
        @Nonnull ScatterGatherMode broadcastMode) {
      _broadcastMode = Objects.requireNonNull(broadcastMode, "broadcastMode").asBroadcast();
      return this;
    }

    public Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> scatterMode(
        @Nonnull ScatterGatherMode scatterMode) {
      _scatterMode = Objects.requireNonNull(scatterMode, "scatterMode").asScatter();
      return this;
    }

    private Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> setDispatchHandler(
        @Nonnull PartitionDispatchHandler<H, P, K, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> dispatchHandler) {
      _dispatchHandler =
          _dispatchHandler == null ? dispatchHandler : illegalStateException("dispatchHandler already set");
      return this;
    }

    public <DISPATCH_REQUEST extends BasicRequest, DISPATCH_RESPONSE, DISPATCH_RESPONSE_STATUS> Builder<H, P, K, R, DISPATCH_REQUEST, DISPATCH_RESPONSE, DISPATCH_RESPONSE_STATUS> dispatchHandler(
        @Nonnull PartitionDispatchHandler<H, P, K, DISPATCH_REQUEST, DISPATCH_RESPONSE, DISPATCH_RESPONSE_STATUS> dispatchHandler) {
      Builder<H, P, K, R, DISPATCH_REQUEST, DISPATCH_RESPONSE, DISPATCH_RESPONSE_STATUS> builder = cast();
      return builder.setDispatchHandler(Objects.requireNonNull(dispatchHandler, "dispatchHandler"));
    }

    public Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> responseAggregatorFactory(
        @Nonnull ResponseAggregatorFactory<HTTP_REQUEST, HTTP_RESPONSE> responseFactory) {
      _responseAggregatorFactory = Optional.of(Objects.requireNonNull(responseFactory, "responseFactory"));
      return this;
    }

    public Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> requestTimeout(
        @Nonnull Function<Headers, Long> requestTimeout) {
      _requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
      return this;
    }

    public Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> longTailRetrySupplier(
        @Nonnull LongTailRetrySupplier<P, K> longTailRetrySupplier) {
      _longTailRetrySupplier = Objects.requireNonNull(longTailRetrySupplier, "longTailRetrySupplier");
      return this;
    }

    public Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> metricsProvider(
        @Nonnull Function<BasicRequest, Metrics> metricsProvider) {
      _metricsProvider = Objects.requireNonNull(metricsProvider, "metricsProvider");
      return this;
    }

    public Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> metricsProvider(
        @Nonnull BiFunction<Headers, Metrics, Headers> metricsDecorator) {
      _metricsDecorator = Objects.requireNonNull(metricsDecorator, "metricsDecorator");
      return this;
    }

    public Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> responseMetrics(
        @Nonnull Function<Headers, Metrics> responseMetrics) {
      _responseMetrics = Objects.requireNonNull(responseMetrics, "metricsDecorator");
      return this;
    }

    public Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> scatterGatherStatsProvider(
        @Nonnull Function<P, ScatterGatherStats> scatterGatherStatsProvider) {
      _scatterGatherStatsProvider = Objects.requireNonNull(scatterGatherStatsProvider, "scatterGatherStatsProvider");
      return this;
    }

    public Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> defaultTimeoutMillis(
        long defaultTimeoutMillis) {
      _defaultTimeoutMillis = defaultTimeoutMillis > 0
          ? defaultTimeoutMillis
          : illegalArgumentException("defaultTimeoutMillis must be more than 0");
      return this;
    }

    public Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> dispatchMinimumMillis(
        long dispatchMinimumMillis) {
      _dispatchMinimumMillis = dispatchMinimumMillis > 0
          ? dispatchMinimumMillis
          : illegalArgumentException("dispatchMinimumMillis must be more than 0");
      return this;
    }

    public Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> longTailMinimumMillis(
        long longTailMinimumMillis) {
      _longTailMinimumMillis = longTailMinimumMillis > 0
          ? longTailMinimumMillis
          : illegalArgumentException("longTailMinimumMillis must be more than 0");
      return this;
    }

    public Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> enableStackTraceResponseForException(
        boolean enableStackTraceResponseForException) {
      _enableStackTraceResponseForException = enableStackTraceResponseForException;
      return this;
    }

    public Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> enableRetryRequestAlwaysUseADifferentHost(
        boolean enableRetryRequestAlwaysUseADifferentHost) {
      _enableRetryRequestAlwaysUseADifferentHost = () -> enableRetryRequestAlwaysUseADifferentHost;
      return this;
    }

    public Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> enableRetryRequestAlwaysUseADifferentHost(
        @Nonnull BooleanSupplier enableRetryRequestAlwaysUseADifferentHost) {
      _enableRetryRequestAlwaysUseADifferentHost = enableRetryRequestAlwaysUseADifferentHost;
      return this;
    }

    public Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> disableRetryOnTimeout(
        @Nonnull BooleanSupplier disableRetryOnTimeout) {
      _disableRetryOnTimeout = disableRetryOnTimeout;
      return this;
    }

    public Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> setIsReqRedirectionAllowedForQuery(
        @Nonnull BooleanSupplier isReqRedirectionAllowedForQuery) {
      _isReqRedirectionAllowedForQuery = isReqRedirectionAllowedForQuery;
      return this;
    }

    public Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> setSuccessCodePredicate(
        @Nonnull IntPredicate successCodePredicate) {
      _successCodePredicate = Objects.requireNonNull(successCodePredicate);
      return this;
    }

    public Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> setRequestRetriableChecker(
        @Nonnull RequestRetriableChecker<P, R, HTTP_RESPONSE_STATUS> requestRetriableChecker) {
      _requestRetriableChecker = Objects.requireNonNull(requestRetriableChecker);
      return this;
    }

    private Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> addHandler(
        List<Pair<String, Supplier<?>>> list,
        String name,
        Supplier<?> supplier) {
      list.add(Pair.make(Objects.requireNonNull(name, "name"), Objects.requireNonNull(supplier, "supplier")));
      return this;
    }

    public Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> addBeforeHttpServerCodec(
        String name,
        Supplier<?> supplier) {
      return addHandler(_beforeHttpServerCodec, name, supplier);
    }

    public Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> addBeforeChunkAggregator(
        String name,
        Supplier<? extends ChannelHandler> supplier) {
      return addHandler(_beforeChunkAggregator, name, supplier);
    }

    public Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> addBeforeIdleStateHandler(
        String name,
        Supplier<? extends ChannelHandler> supplier) {
      return addHandler(_beforeIdleStateHandler, name, supplier);
    }

    public Builder<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> addBeforeHttpRequestHandler(
        String name,
        Supplier<? extends ChannelHandler> supplier) {
      return addHandler(_beforeHttpRequestHandler, name, supplier);
    }

    public ScatterGatherHelper<H, P, K, R, HTTP_REQUEST, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> build() {
      return new ScatterGatherHelper<>(
          _pathParser,
          _partitionFinder,
          _hostFinder,
          _hostHealthMonitor,
          _roleFinder,
          _broadcastMode,
          _scatterMode,
          _dispatchHandler,
          _responseAggregatorFactory,
          _requestTimeout,
          _longTailRetrySupplier,
          _metricsProvider,
          _metricsDecorator,
          _responseMetrics,
          _scatterGatherStatsProvider,
          _defaultTimeoutMillis,
          _dispatchMinimumMillis,
          _longTailMinimumMillis,
          _beforeHttpServerCodec,
          _beforeChunkAggregator,
          _beforeIdleStateHandler,
          _beforeHttpRequestHandler,
          _successCodePredicate,
          _requestRetriableChecker,
          _enableStackTraceResponseForException,
          _enableRetryRequestAlwaysUseADifferentHost,
          _disableRetryOnTimeout,
          _isReqRedirectionAllowedForQuery);
    }

    @SuppressWarnings("unchecked")
    private <HOST, PATH extends ResourcePath<KEY>, KEY, ROLE, B_H_R extends BasicRequest, H_R, H_R_S> Builder<HOST, PATH, KEY, ROLE, B_H_R, H_R, H_R_S> cast() {
      return (Builder) this;
    }
  }

  static <V> V checkIsSet(V value, String name) {
    if (Objects.isNull(value)) {
      throw new IllegalStateException(name);
    }
    return value;
  }

  static <V> V illegalStateException(String name) {
    throw new IllegalStateException(name);
  }

  static <V> V illegalArgumentException(String name) {
    throw new IllegalArgumentException(name);
  }
}
