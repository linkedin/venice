package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.MemoryPressureIndexMonitor;
import com.linkedin.alpini.netty4.misc.MemoryPressureIndexUtils;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A handler that calculates request / response size and passes it to MemoryPressureIndexMonitor
 * @param <R> Referent used by MemoryPressureIndexMonitor
 * @param <K> Key used by MemoryPressureIndexMonitor
 * @param <S> The Stats
 */
@ChannelHandler.Sharable
public class MemoryPressureIndexHandler<R, K, S> extends ChannelDuplexHandler {
  private static final Logger LOG = LogManager.getLogger(MemoryPressureIndexHandler.class);
  private volatile boolean _phantomMode = false;

  private final Function<HttpResponse, Optional<K>> _responseToKeyFunction;
  private final MemoryPressureIndexMonitor<R, K, S> _memoryPressureIndexMonitor;

  public MemoryPressureIndexHandler(
      MemoryPressureIndexMonitor<R, K, S> monitor,
      Function<HttpResponse, Optional<K>> responseToKeyFunction) {
    this._memoryPressureIndexMonitor = Objects.requireNonNull(monitor, "monitor");
    this._responseToKeyFunction = Objects.requireNonNull(responseToKeyFunction, "responseToKeyFunction");
  }

  public MemoryPressureIndexHandler phantomMode(boolean phantomMode) {
    this._phantomMode = phantomMode;
    return this;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (msg instanceof HttpRequest) {
      HttpRequest request = (HttpRequest) msg;
      MemoryPressureIndexUtils.getContentLength(request).ifPresent(contentLength -> {
        LOG.debug(
            "Adding request id={} with content-Length={}",
            _memoryPressureIndexMonitor.getIdSupplier().apply((R) request).orElse(null),
            contentLength);
        _memoryPressureIndexMonitor.addReferentAndByteCount((R) request, contentLength);
      });
    }
    super.channelRead(ctx, msg);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    if (msg instanceof HttpResponse) {
      HttpResponse response = (HttpResponse) msg;
      Optional<K> requestId = _responseToKeyFunction.apply(response);
      requestId
          .map(
              key -> _phantomMode
                  ? _memoryPressureIndexMonitor
                      .removeByteCountAndAddPhantom(key, MemoryPressureIndexUtils.getContentLength(response))
                      .orElse(null)
                  : _memoryPressureIndexMonitor.removeByteCount(key, true, Optional.empty()).orElse(null))
          .ifPresent(
              b -> LOG.debug(
                  "Removing {} bytes{} for request id={}.",
                  b.count(),
                  _phantomMode ? " in delayed mode" : " immediately",
                  requestId.get()));
    }
    super.write(ctx, msg, promise);
  }
}
