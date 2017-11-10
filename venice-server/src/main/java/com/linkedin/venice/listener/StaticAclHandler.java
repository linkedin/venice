package com.linkedin.venice.listener;

import com.linkedin.venice.utils.NettyUtils;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLPeerUnverifiedException;
import org.apache.log4j.Logger;


@ChannelHandler.Sharable
public class StaticAclHandler extends SimpleChannelInboundHandler<HttpRequest> {
  private static final Logger logger = Logger.getLogger(StaticAclHandler.class);
  private final String resource;
  private final StaticAccessController accessController;

  public StaticAclHandler(StaticAccessController accessController, String resource) {
    this.accessController = accessController;
    this.resource = resource;
  }

  /**
   * Verify if client has permission to access one particular resource.
   *
   * @param ctx
   * @param req
   * @throws SSLPeerUnverifiedException
   */
  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpRequest req) throws SSLPeerUnverifiedException {
    X509Certificate clientCert = (X509Certificate) ctx.pipeline().get(SslHandler.class).engine().getSession().getPeerCertificates()[0];
    String method = req.method().name();

    if (accessController.hasAccess(clientCert, resource, method)) {
      ReferenceCountUtil.retain(req);
      ctx.fireChannelRead(req);
    } else {
      String client = ctx.channel().remoteAddress().toString(); //ip and port
      String errLine = String.format("%s requested %s %s", client, method, req.uri());
      logger.debug("Unauthorized access rejected: " + errLine);
      NettyUtils.setupResponseAndFlush(HttpResponseStatus.FORBIDDEN, new byte[0], false, ctx);
    }
  }
}
