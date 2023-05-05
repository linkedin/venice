package com.linkedin.venice.listener;

import com.linkedin.venice.listener.response.HttpShortcutResponse;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Currently this VerifySslHandler is only used in servers or the isolated ingestion process.
 */
@ChannelHandler.Sharable
public class VerifySslHandler extends SimpleChannelInboundHandler<HttpRequest> {
  private static final Logger LOGGER = LogManager.getLogger(VerifySslHandler.class);

  /**
   * If the SSL handler is not in the channel pipeline, then return 403
   * otherwise pass the request along the stack.
   *
   * @param ctx
   * @param req
   * @throws IOException
   */
  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpRequest req) {
    SslHandler sslHandler = ServerHandlerUtils.extractSslHandler(ctx);
    if (sslHandler != null) {
      ReferenceCountUtil.retain(req);
      ctx.fireChannelRead(req);
    } else {
      // Log that we got an unexpected non-ssl request
      String remote = ctx.channel().remoteAddress().toString(); // ip and port
      String method = req.method().name();
      LOGGER.error(
          "Got a non-ssl request on what should be an ssl only port: {} requested {}, {}",
          remote,
          method,
          req.uri());
      ctx.writeAndFlush(new HttpShortcutResponse("SSL Required", HttpResponseStatus.FORBIDDEN));
      ctx.close();
    }
  }
}
