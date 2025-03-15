package com.linkedin.alpini.netty4.handlers;

import static io.netty.handler.codec.http.HttpHeaderValues.TEXT_PLAIN;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


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
  private static final Logger LOGGER = LogManager.getLogger(ConnectionControlHandler.class);
  private final LongAdder _activeCount = new LongAdder();
  private final LongAdder _inactiveCount = new LongAdder();
  private final Semaphore _activeSemaphore = new Semaphore(1);
  private final Semaphore _inactiveSemaphore = new Semaphore(1);

  public ConnectionControlHandler(
      @Nonnull IntSupplier connectionLimit,
      @Nonnull Consumer<Integer> connectionCountRecorder) {
    super(connectionLimit, connectionCountRecorder);
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    _activeCount.increment();
    _connectionCountRecorder.accept(getConnectedCount());

    Channel parent = ctx.channel().parent();
    if (parent != null && _activeSemaphore.tryAcquire()) {
      parent.eventLoop().submit(() -> {
        if (getConnectedCount() > getConnectionLimit()) {
          LOGGER.error(
              "Connection limit exceeded! Active connections: {}, Limit: {}",
              getConnectedCount(),
              getConnectionLimit());
          parent.config().setAutoRead(false);
          Channel clientChannel = ctx.channel(); // Get the current client's channel
          if (clientChannel.isActive()) {
            FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.TOO_MANY_REQUESTS,
                Unpooled.copiedBuffer("Connection limit exceeded on Venice routers", StandardCharsets.US_ASCII));
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, TEXT_PLAIN);
            clientChannel.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
          }
        }
      }).addListener(ignored -> _activeSemaphore.release());
    }

    ctx.fireChannelActive();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    _inactiveCount.increment();
    _connectionCountRecorder.accept(getConnectedCount());

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
