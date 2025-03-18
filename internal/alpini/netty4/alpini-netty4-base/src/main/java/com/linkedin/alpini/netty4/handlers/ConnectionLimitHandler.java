package com.linkedin.alpini.netty4.handlers;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
@ChannelHandler.Sharable
public class ConnectionLimitHandler extends ChannelInboundHandlerAdapter {

  /* package */
  static final String REJECT_MESSAGE =
      "HTTP/1.1 429 Too Many Connections\r\n" + "Connection: close\r\n" + "Content-Length: 0\r\n" + "\r\n\r\n";

  private static final Logger LOG = LogManager.getLogger(ConnectionLimitHandler.class);

  private final AtomicInteger _activeCount = new AtomicInteger();
  private IntSupplier _connectionLimit;
  protected final Consumer<Integer> _connectionCountRecorder;
  protected final Consumer<Integer> _rejectedConnectionCountRecorder;

  /**
   * Construct with a preset connection limit.
   * @param limit Maximum number of connections.
   */
  public ConnectionLimitHandler(
      IntSupplier limit,
      Consumer<Integer> connectionCountRecorder,
      Consumer<Integer> rejectedConnectionCountRecorder) {
    _connectionLimit = limit;
    // Build a no-op recorder if the pass-in recorder is null
    _connectionCountRecorder = (connectionCountRecorder == null) ? (ignored) -> {} : connectionCountRecorder;
    _rejectedConnectionCountRecorder =
        (rejectedConnectionCountRecorder == null) ? (ignored) -> {} : rejectedConnectionCountRecorder;
  }

  /**
   * Return the current number of connections.
   * @return number of connections.
   */
  public int getConnectedCount() {
    return _activeCount.get();
  }

  /**
   * Set the maximum number of connections.
   * @param limit  new connection limit.
   */
  public void setConnectionLimit(int limit) {
    _connectionLimit = () -> limit;
  }

  /**
   * Return the current connection limit.
   * @return number of connections.
   */
  public int getConnectionLimit() {
    return _connectionLimit.getAsInt();
  }

  /**
   * Calls {@link ChannelHandlerContext#fireChannelActive()} to forward
   * to the next {@link io.netty.channel.ChannelInboundHandler} in the {@link io.netty.channel.ChannelPipeline}.
   * <p>
   * Sub-classes may override this method to change behavior.
   *
   * @param ctx
   */
  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    int count = _activeCount.incrementAndGet();
    int limit = _connectionLimit.getAsInt();
    _connectionCountRecorder.accept(count);
    if (count > limit) {
      LOG.error("Connection count {} exceeds {}", count, limit);
      _rejectedConnectionCountRecorder.accept(1);

      ctx.writeAndFlush(Unpooled.copiedBuffer(REJECT_MESSAGE, StandardCharsets.US_ASCII))
          .addListener(ChannelFutureListener.CLOSE);

    } else {
      super.channelActive(ctx);
    }
  }

  /**
   * Calls {@link ChannelHandlerContext#fireChannelInactive()} to forward
   * to the next {@link io.netty.channel.ChannelInboundHandler} in the {@link io.netty.channel.ChannelPipeline}.
   * <p>
   * Sub-classes may override this method to change behavior.
   *
   * @param ctx
   */
  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    int count = _activeCount.decrementAndGet();
    _connectionCountRecorder.accept(count);
    super.channelInactive(ctx);
  }
}
