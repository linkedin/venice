package com.linkedin.venice.router;

import com.linkedin.venice.utils.NettyUtils;
import com.linkedin.venice.router.stats.SecurityStats;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;
import org.apache.log4j.Logger;

@ChannelHandler.Sharable
public class VerifySslHandler extends SimpleChannelInboundHandler<HttpRequest> {
  private static final Logger logger = Logger.getLogger(VerifySslHandler.class);
  private final SecurityStats stats;
  private final boolean requireSsl;

  public VerifySslHandler(SecurityStats stats) {
    this(stats, true);
  }

  public VerifySslHandler(SecurityStats stats, boolean requireSsl) {
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
    if (ctx.pipeline().get(SslHandler.class) == null) {
      // Log that we got an unexpected non-ssl request
      String remote = ctx.channel().remoteAddress().toString(); //ip and port
      String method = req.method().name();
      String errLine = remote + " requested " + method + " " + req.uri();
      logger.warn("[requireSsl=" + this.requireSsl + "] Got an unexpected non-ssl request: " + errLine);
      stats.recordNonSslRequest();

      if (requireSsl) {
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

    logger.info("Could not set up connection from: " + ctx.channel().remoteAddress());
    logger.warn(event);
    stats.recordSslError();
    NettyUtils.setupResponseAndFlush(HttpResponseStatus.FORBIDDEN, new byte[0], false, ctx);
    ctx.pipeline().remove(this);
    ctx.close();
  }
}
