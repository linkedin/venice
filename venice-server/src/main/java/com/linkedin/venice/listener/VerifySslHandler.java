package com.linkedin.venice.listener;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import org.apache.log4j.Logger;


@ChannelHandler.Sharable
public class VerifySslHandler extends SimpleChannelInboundHandler<HttpRequest> {
  private static final Logger logger = Logger.getLogger(VerifySslHandler.class);

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
    if (ctx.channel().pipeline().toMap().get("ssl-handler") == null) {
      // Log that we got an unexpected non-ssl request
      String remote = ctx.channel().remoteAddress().toString(); //ip and port
      String method = req.method().name();
      String errLine = remote + " requested " + method + " " + req.uri();
      logger.error("Got a non-ssl request on what should be an ssl only port: " + errLine);
      ctx.writeAndFlush(new HttpShortcutResponse("SSL Required", HttpResponseStatus.FORBIDDEN));
    } else {
      ReferenceCountUtil.retain(req);
      ctx.fireChannelRead(req);
      return;
    }
  }
}
