package com.linkedin.alpini.netty4.pool;

import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.base.monitoring.CallCompletion;
import com.linkedin.alpini.base.monitoring.CallTracker;
import com.linkedin.alpini.base.monitoring.NullCallTracker;
import com.linkedin.alpini.netty4.misc.NettyUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This factory constructs instances of {@link FixedChannelPool} for the {@link ChannelPoolManager}.
 *
 * @author acurtis on 3/29/17.
 */
public class FixedChannelPoolFactory implements ChannelPoolFactory, ChannelHealthChecker {
  private static final AttributeKey<Long> LAST_HEALTHCHECK_TIMESTAMP =
      AttributeKey.valueOf(FixedChannelPoolFactory.class, "lastHealthCheck");

  private final Logger _log = LogManager.getLogger(getClass());

  private final Bootstrap _bootstrap;
  private long _acquireTimeoutMillis;
  private FixedChannelPool.AcquireTimeoutAction _acquireTimeoutAction = FixedChannelPool.AcquireTimeoutAction.FAIL;
  private IntSupplier _minConnections;
  private IntSupplier _maxConnections;
  private IntSupplier _maxPendingAcquires;
  private boolean _releaseHeathCheck;
  private LongSupplier _healthCheckIntervalMillis;
  private final ChannelHealthChecker _channelHealthChecker;
  private final Function<SocketAddress, CallTracker> _healthCheckerTracker;
  private final Consumer<Channel> _channelInitializer;
  protected boolean _usingFastPool;

  protected BooleanSupplier _useQueueSizeForAcquiredChannelCount = Boolean.FALSE::booleanValue;

  private static final AttributeKey<ChannelPoolHandler> CHANNEL_POOL_HANDLER =
      AttributeKey.valueOf(ChannelPoolHandler.class, "channel-pool-handler");

  public FixedChannelPoolFactory(
      @Nonnull Bootstrap bootstrap,
      @Nonnegative long acquireTimeoutMillis,
      @Nonnegative int maxConnections,
      @Nonnegative int maxPendingAcquires,
      boolean releaseHeathCheck,
      @Nonnegative long healthCheckIntervalMillis,
      @Nonnull ChannelHealthChecker channelHealthChecker,
      @Nullable CallTracker healthCheckerTracker) {
    this(
        bootstrap,
        acquireTimeoutMillis,
        maxConnections,
        maxPendingAcquires,
        releaseHeathCheck,
        healthCheckIntervalMillis,
        channelHealthChecker,
        healthCheckerTracker != null ? ignored -> healthCheckerTracker : null);
  }

  public FixedChannelPoolFactory(
      @Nonnull Bootstrap bootstrap,
      @Nonnegative long acquireTimeoutMillis,
      @Nonnegative int maxConnections,
      @Nonnegative int maxPendingAcquires,
      boolean releaseHeathCheck,
      @Nonnegative long healthCheckIntervalMillis,
      @Nonnull ChannelHealthChecker channelHealthChecker,
      @Nullable Function<SocketAddress, CallTracker> healthCheckerTracker) {
    this(
        bootstrap,
        acquireTimeoutMillis,
        0,
        maxConnections,
        maxPendingAcquires,
        releaseHeathCheck,
        healthCheckIntervalMillis,
        channelHealthChecker,
        healthCheckerTracker);
  }

  public FixedChannelPoolFactory(
      @Nonnull Bootstrap bootstrap,
      @Nonnegative long acquireTimeoutMillis,
      @Nonnegative int minConnections,
      @Nonnegative int maxConnections,
      @Nonnegative int maxPendingAcquires,
      boolean releaseHeathCheck,
      @Nonnegative long healthCheckIntervalMillis,
      @Nonnull ChannelHealthChecker channelHealthChecker,
      @Nullable Function<SocketAddress, CallTracker> healthCheckerTracker) {
    this(
        bootstrap,
        acquireTimeoutMillis,
        minConnections,
        maxConnections,
        maxPendingAcquires,
        releaseHeathCheck,
        healthCheckIntervalMillis,
        channelHealthChecker,
        healthCheckerTracker,
        extractChannelInitializer(bootstrap));
  }

  private static Consumer<Channel> extractChannelInitializer(Bootstrap bootstrap) {
    ChannelHandler handler = Objects.requireNonNull(bootstrap.config().handler(), "bootstrap.handler");
    return channel -> channel.pipeline().addLast(NettyUtils.executorGroup(channel), handler);
  }

  @SuppressWarnings("unchecked")
  protected static <T extends ChannelPoolHandler> T getChannelPoolHandler(@Nonnull Channel ch) {
    return (T) ch.attr(CHANNEL_POOL_HANDLER).get();
  }

  private FixedChannelPoolFactory(
      @Nonnull Bootstrap bootstrap,
      @Nonnegative long acquireTimeoutMillis,
      @Nonnegative int minConnections,
      @Nonnegative int maxConnections,
      @Nonnegative int maxPendingAcquires,
      boolean releaseHeathCheck,
      @Nonnegative long healthCheckIntervalMillis,
      @Nonnull ChannelHealthChecker channelHealthChecker,
      @Nullable Function<SocketAddress, CallTracker> healthCheckerTracker,
      @Nonnull Consumer<Channel> channelInitializer) {
    this(
        bootstrap,
        acquireTimeoutMillis,
        Integer.valueOf(minConnections)::intValue,
        Integer.valueOf(maxConnections)::intValue,
        Integer.valueOf(maxPendingAcquires)::intValue,
        releaseHeathCheck,
        Long.valueOf(healthCheckIntervalMillis)::longValue,
        channelHealthChecker,
        healthCheckerTracker,
        channelInitializer);
  }

  public FixedChannelPoolFactory(
      @Nonnull Bootstrap bootstrap,
      @Nonnegative long acquireTimeoutMillis,
      @Nonnull IntSupplier minConnections,
      @Nonnull IntSupplier maxConnections,
      @Nonnull IntSupplier maxPendingAcquires,
      boolean releaseHeathCheck,
      @Nonnull LongSupplier healthCheckIntervalMillis,
      @Nonnull ChannelHealthChecker channelHealthChecker,
      @Nullable Function<SocketAddress, CallTracker> healthCheckerTracker) {
    this(
        bootstrap,
        acquireTimeoutMillis,
        minConnections,
        maxConnections,
        maxPendingAcquires,
        releaseHeathCheck,
        healthCheckIntervalMillis,
        channelHealthChecker,
        healthCheckerTracker,
        extractChannelInitializer(bootstrap));
  }

  private FixedChannelPoolFactory(
      @Nonnull Bootstrap bootstrap,
      @Nonnegative long acquireTimeoutMillis,
      @Nonnull IntSupplier minConnections,
      @Nonnull IntSupplier maxConnections,
      @Nonnull IntSupplier maxPendingAcquires,
      boolean releaseHeathCheck,
      @Nonnull LongSupplier healthCheckIntervalMillis,
      @Nonnull ChannelHealthChecker channelHealthChecker,
      @Nullable Function<SocketAddress, CallTracker> healthCheckerTracker,
      @Nonnull Consumer<Channel> channelInitializer) {

    _bootstrap = Objects.requireNonNull(bootstrap, "bootstrap");
    _acquireTimeoutMillis = acquireTimeoutMillis;
    _minConnections = minConnections;
    _maxConnections = maxConnections;
    _maxPendingAcquires = maxPendingAcquires;
    _releaseHeathCheck = releaseHeathCheck;
    _healthCheckIntervalMillis = healthCheckIntervalMillis;
    _channelHealthChecker = channelHealthChecker;
    _healthCheckerTracker = Optional.ofNullable(healthCheckerTracker).orElse(ignored -> NullCallTracker.INSTANCE);
    _channelInitializer = Objects.requireNonNull(channelInitializer, "channelInitializer");
  }

  public void setUsingFastPool(boolean usingFastPool) {
    _usingFastPool = usingFastPool;
  }

  public boolean isUsingFastPool() {
    return _usingFastPool;
  }

  public void setUseQueueSizeForAcquiredChannelCount(@Nonnull BooleanSupplier useQueueSizeForAcquiredChannelCount) {
    _useQueueSizeForAcquiredChannelCount = useQueueSizeForAcquiredChannelCount;
  }

  public void setMinConnections(@Nonnegative int minConnections) {
    setMinConnections(Integer.valueOf(minConnections)::intValue);
  }

  public void setAcquireTimeoutAction(@Nonnull FixedChannelPool.AcquireTimeoutAction action) {
    _acquireTimeoutAction = Objects.requireNonNull(action);
  }

  public void setMinConnections(@Nonnull IntSupplier minConnections) {
    _minConnections = minConnections;
  }

  public int getMinConnections() {
    return _minConnections.getAsInt();
  }

  public int getMaxConnections() {
    return _maxConnections.getAsInt();
  }

  public int getMaxPendingAcquires() {
    return _maxPendingAcquires.getAsInt();
  }

  public long getAcquireTimeoutMillis() {
    return _acquireTimeoutMillis;
  }

  @Override
  @Nonnull
  public ManagedChannelPool construct(
      @Nonnull ChannelPoolManager manager,
      @Nonnull ChannelPoolHandler handler,
      @Nonnull EventLoopGroup eventLoop,
      @Nonnull InetSocketAddress address) {

    int subpoolCount = manager.subpoolCount();
    IntSupplier minConnections = () -> Math.max(1, (getMinConnections() + subpoolCount - 1) / subpoolCount);
    int maxConnections = Math.max(1, (getMaxConnections() + subpoolCount - 1) / subpoolCount);

    _log.debug(
        "FixedChannelPool - acquireTimeoutMillis={}, minConnections={}, maxConnections={}, maxPendingAcquires={}, releaseHealthCheck={}",
        getAcquireTimeoutMillis(),
        minConnections.getAsInt(),
        maxConnections,
        _maxPendingAcquires,
        _releaseHeathCheck);

    return construct(
        _bootstrap.clone(eventLoop).remoteAddress(address).attr(CHANNEL_POOL_HANDLER, handler),
        new ChannelPoolHandler() {
          @Override
          public void channelReleased(Channel ch) throws Exception {
            handler.channelReleased(ch);
          }

          @Override
          public void channelAcquired(Channel ch) throws Exception {
            handler.channelAcquired(ch);
          }

          @Override
          public void channelCreated(Channel ch) throws Exception {
            _channelInitializer.accept(ch);
            handler.channelCreated(ch);
          }
        },
        this,
        _acquireTimeoutAction,
        minConnections,
        maxConnections,
        _releaseHeathCheck);
  }

  @Nonnull
  protected ManagedChannelPool construct(
      @Nonnull Bootstrap bootstrap,
      @Nonnull ChannelPoolHandler channelPoolHandler,
      @Nonnull ChannelHealthChecker healthChecker,
      @Nonnull FixedChannelPool.AcquireTimeoutAction acquireTimeoutAction,
      @Nonnull IntSupplier minConnections,
      int maxConnections,
      boolean releaseHeathCheck) {
    if (_usingFastPool) {
      return new FixedFastChannelPoolImpl(
          bootstrap,
          channelPoolHandler,
          healthChecker,
          acquireTimeoutAction,
          getAcquireTimeoutMillis(),
          minConnections,
          maxConnections,
          getMaxPendingAcquires(),
          releaseHeathCheck,
          1,
          _useQueueSizeForAcquiredChannelCount);
    }
    return new FixedChannelPoolImpl(
        bootstrap,
        channelPoolHandler,
        healthChecker,
        acquireTimeoutAction,
        getAcquireTimeoutMillis(),
        minConnections,
        maxConnections,
        getMaxPendingAcquires(),
        releaseHeathCheck,
        false,
        _useQueueSizeForAcquiredChannelCount);
  }

  public void setHealthCheckIntervalMillis(@Nonnegative long healthCheckIntervalMillis) {
    setHealthCheckIntervalMillis(Long.valueOf(healthCheckIntervalMillis)::longValue);
  }

  public void setHealthCheckIntervalMillis(@Nonnull LongSupplier healthCheckIntervalMillis) {
    _healthCheckIntervalMillis = healthCheckIntervalMillis;
  }

  public long getHealthCheckIntervalMillis() {
    return _healthCheckIntervalMillis.getAsLong();
  }

  @Override
  public Future<Boolean> isHealthy(Channel channel) {

    if (!channel.isActive()) {
      return channel.eventLoop().newSucceededFuture(Boolean.FALSE);
    }

    if (_channelHealthChecker != null) {
      Attribute<Long> attribute = channel.attr(LAST_HEALTHCHECK_TIMESTAMP);
      Long lastHealthCheckTimestamp = attribute.get();

      if (lastHealthCheckTimestamp == null
          || lastHealthCheckTimestamp + getHealthCheckIntervalMillis() < Time.currentTimeMillis()) {
        CallCompletion completion = Optional.ofNullable(_healthCheckerTracker.apply(channel.remoteAddress()))
            .orElse(NullCallTracker.INSTANCE)
            .startCall();

        // TODO(acurtis) probably should add a healthcheck timeout using the netty hashedwheeltimer

        return _channelHealthChecker.isHealthy(channel).addListener((Future<Boolean> future) -> {
          if (future.isSuccess()) {
            if (Boolean.TRUE == future.getNow()) {
              completion.close();
              attribute.set(Time.currentTimeMillis());
            } else {
              completion.closeWithError();
              channel.attr(ChannelPoolManager.FAILED_HEALTH_CHECK).set(true);
            }
          } else {
            completion.closeWithError(future.cause());
            channel.attr(ChannelPoolManager.FAILED_HEALTH_CHECK).set(true);
          }
        });
      }
    }
    return channel.eventLoop().newSucceededFuture(Boolean.TRUE);
  }
}
