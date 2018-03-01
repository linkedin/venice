package com.linkedin.venice.router;

import com.linkedin.venice.utils.NettyUtils;
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
  private final boolean enforce;

  public VerifySslHandler(){
    enforce = true;
  }

  public VerifySslHandler(boolean enforce){
    this.enforce = enforce;
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
    if (ctx.channel().pipeline().toMap().get("ssl-handler") == null) {

      // Log that we got an unexpected non-ssl request
      String remote = ctx.channel().remoteAddress().toString(); //ip and port
      String method = req.method().name();
      String errLine = remote + " requested " + method + " " + req.uri();
      logger.debug("Got an unexpected non-ssl request: " + errLine);
      if (enforce) {
        NettyUtils.setupResponseAndFlush(HttpResponseStatus.FORBIDDEN, new byte[0], false, ctx);
        return;
      }
    }
    ReferenceCountUtil.retain(req);
    ctx.fireChannelRead(req);
  }
}
