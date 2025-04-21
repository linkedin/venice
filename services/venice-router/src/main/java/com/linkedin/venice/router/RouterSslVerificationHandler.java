package com.linkedin.venice.router;

import com.linkedin.venice.router.stats.SecurityStats;
import com.linkedin.venice.utils.NettyUtils;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


@ChannelHandler.Sharable
public class RouterSslVerificationHandler extends SimpleChannelInboundHandler<HttpRequest> {
  private static final Logger LOGGER = LogManager.getLogger(RouterSslVerificationHandler.class);
  private final SecurityStats stats;
  private final boolean requireSsl;

  public RouterSslVerificationHandler(@Nonnull SecurityStats stats) {
    this(stats, true);
  }

  public RouterSslVerificationHandler(@Nonnull SecurityStats stats, boolean requireSsl) {
    this.stats = stats;
    this.requireSsl = requireSsl;
  }

  /**
   * If the SSL handler is not in the channel pipeline, then return 403
   * otherwise pass the request along the stack.
   *
   * @param ctx
   * @param req
   * @throws IOException
   */
  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpRequest req) throws IOException {
    // Leverage user request to update connection count metric in the current metric time window.
    stats.updateConnectionCountInCurrentMetricTimeWindow();
    SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
    if (sslHandler == null) {
      /**
       * In HTTP/2, the SSLHandler is in parent channel pipeline and the child channels won't have the SSL Handler.
       */
      sslHandler = ctx.channel().parent().pipeline().get(SslHandler.class);
    }
    if (sslHandler == null) {
      stats.recordNonSslRequest();
      if (requireSsl) {
        // Log that we got an unexpected non-ssl request only if SSL is required
        LOGGER.warn(
            "[requireSsl={}] Got an unexpected non-ssl request: {} requested {} {}",
            this.requireSsl,
            ctx.channel().remoteAddress(),
            req.method().name(),
            req.uri());
        NettyUtils.setupResponseAndFlush(HttpResponseStatus.FORBIDDEN, new byte[0], false, ctx);
        ctx.close();
        return;
      }
    }
    ReferenceCountUtil.retain(req);
    ctx.fireChannelRead(req);
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
    if (!(event instanceof SslHandshakeCompletionEvent)) {
      ctx.fireUserEventTriggered(event);
      return;
    }

    if (((SslHandshakeCompletionEvent) event).isSuccess()) {
      stats.recordSslSuccess();
      ctx.fireUserEventTriggered(event);
      return;
    }

    LOGGER.warn("Could not set up connection from: {}. Event:{}", ctx.channel().remoteAddress(), event);
    stats.recordSslError();
    NettyUtils.setupResponseAndFlush(HttpResponseStatus.FORBIDDEN, new byte[0], false, ctx);
    ctx.pipeline().remove(this);
    ctx.close();
  }
}
