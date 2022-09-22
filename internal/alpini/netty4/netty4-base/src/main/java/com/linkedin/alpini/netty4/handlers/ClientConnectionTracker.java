package com.linkedin.alpini.netty4.handlers;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A connectionTracker that tracks the connection count for each client. Right now, it only spits out a warning message

 */
@ChannelHandler.Sharable
public class ClientConnectionTracker extends ChannelDuplexHandler {
  private final int _connectionCountLimit;
  private final int _connectionWarningResetThreshold;

  private boolean _shouldLogMetricForSiRootCausingTheirOwnClientConnectionSurge;
  // This is supposed to be a static to ensure there be one instance in whole JVM. However, we have to accommodate the
  // TestNG that would run tests in parallel, which would have those tests step on each other's foot.
  private final Map<InetAddress, ConnectionStats> _statsMap = new ConcurrentHashMap<>();
  private static final Logger LOG = LogManager.getLogger(ClientConnectionTracker.class);

  public ClientConnectionTracker(int connectionCountLimit, int connectionWarningResetThreshold) {
    if (connectionWarningResetThreshold > connectionCountLimit) {
      throw new IllegalArgumentException(
          String.format(
              "connectionWarningResetThreshold(%d) must not be higher than connectionCountLimit(%d).",
              connectionWarningResetThreshold,
              connectionCountLimit));
    }
    this._connectionCountLimit = connectionCountLimit;
    this._connectionWarningResetThreshold = connectionWarningResetThreshold;
  }

  public ClientConnectionTracker setShouldLogMetricForSiRootCausingTheirOwnClientConnectionSurge(boolean should) {
    _shouldLogMetricForSiRootCausingTheirOwnClientConnectionSurge = should;
    return this;
  }

  protected static class ConnectionStats {
    private final String _clientHostName;
    private final LongAdder _connectionCount = new LongAdder();
    // We are OK if the reported is a little bit staled here.
    private boolean _reported = false;
    private final LongAdder _activeRequestCount = new LongAdder();

    public ConnectionStats(InetAddress clientAddress) {
      // Use toString to avoid any DNS lookup
      _clientHostName = clientAddress.toString();
    }

    public ConnectionStats increment() {
      _connectionCount.increment();
      return this;
    }

    public int activeRequestCount() {
      return _activeRequestCount.intValue();
    }

    public int increaseActiveRequestAndGet() {
      _activeRequestCount.increment();
      return _activeRequestCount.intValue();
    }

    public int decreaseActiveRequestAndGet() {
      _activeRequestCount.decrement();
      return _activeRequestCount.intValue();
    }

    public int decrementAndGet() {
      _connectionCount.decrement();
      return _connectionCount.intValue();
    }

    public int connectionCount() {
      return _connectionCount.intValue();
    }

    public ConnectionStats reported() {
      this._reported = true;
      return this;
    }

    public ConnectionStats resetReported() {
      this._reported = false;
      return this;
    }

    public boolean isReported() {
      return _reported;
    }

    @Override
    public String toString() {
      int connections = connectionCount();
      int inflightRequest = activeRequestCount();
      return "[host: " + _clientHostName + ", connections: " + connections + ", inflight-requests: " + inflightRequest
          + ", available connections: " + (connections - inflightRequest) + "]";
    }
  }

  Map<InetAddress, ConnectionStats> statsMap() {
    return _statsMap;
  }

  /**
   *  For testing use only
   * @return
   */
  ConnectionStats getStatsByContext(ChannelHandlerContext ctx) {
    // ConcurrentHashMap wont' allow null key
    return getHostInetAddressFromContext(ctx).map(_statsMap::get).orElse(null);
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    checkConnectionLimit(ctx).ifPresent(s -> {
      if (_shouldLogMetricForSiRootCausingTheirOwnClientConnectionSurge) {
        LOG.info("channelActive for client: {}", s);
      }
    });
    super.channelActive(ctx);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (msg instanceof HttpRequest) {
      onRequest(ctx);
    }
    super.channelRead(ctx, msg);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    if (msg instanceof HttpResponse) {
      onWritingLastHttpContent(ctx);
    }
    super.write(ctx, msg, promise);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    onInActive(ctx);
    super.channelInactive(ctx);
  }

  private static Optional<InetAddress> getHostInetAddressFromContext(ChannelHandlerContext ctx) {
    return getHostInetSocketAddressFromContext(ctx).map(InetSocketAddress::getAddress);
  }

  private static Optional<InetSocketAddress> getHostInetSocketAddressFromContext(ChannelHandlerContext ctx) {
    return Optional.ofNullable(ctx.channel())
        .map(Channel::remoteAddress)
        .filter(addr -> addr instanceof InetSocketAddress)
        .map(socketAddress -> ((InetSocketAddress) socketAddress));
  }

  protected Optional<ConnectionStats> checkConnectionLimit(ChannelHandlerContext context) {
    return getHostInetSocketAddressFromContext(context).map(address -> {
      ConnectionStats stats =
          _statsMap.computeIfAbsent(address.getAddress(), k -> new ConnectionStats(address.getAddress())).increment();
      if (stats.connectionCount() > _connectionCountLimit) {
        whenOverLimit(stats, context);
      }
      return stats;
    });
  }

  protected void whenOverLimit(ConnectionStats stats, ChannelHandlerContext context) {
    if (!stats.isReported()) {
      LOG.warn(
          "Connection cap for {} is {}, exceeding the limit {}.",
          stats._clientHostName,
          stats.connectionCount(),
          _connectionCountLimit);
      stats.reported();
    }
  }

  private int onRequest(ChannelHandlerContext ctx) {
    return modifyRequestCount(ctx, ConnectionStats::increaseActiveRequestAndGet);
  }

  private int onWritingLastHttpContent(ChannelHandlerContext ctx) {
    return modifyRequestCount(ctx, ConnectionStats::decreaseActiveRequestAndGet);
  }

  private int modifyRequestCount(ChannelHandlerContext ctx, Function<ConnectionStats, Integer> integerSupplier) {
    return getHostInetAddressFromContext(ctx)
        // The stats should be in the MAP already by channelActive
        .map(_statsMap::get)
        .map(integerSupplier::apply)
        .orElse(0);
  }

  private void onInActive(ChannelHandlerContext ctx) {
    getHostInetAddressFromContext(ctx).ifPresent(key -> _statsMap.computeIfPresent(key, (k, count) -> {
      int countAfterDecrement = count.decrementAndGet();
      // only reset the reported when the count is below the 90% of the limit.
      if (countAfterDecrement < _connectionWarningResetThreshold && count.isReported()) {
        count.resetReported();
      }
      // force COUNT_MAP to remove the entry if the count is less or equal 0.
      if (countAfterDecrement <= 0) {
        count = null;
      }
      return count;
    }));
  }
}
