package com.linkedin.venice.listener;

import com.linkedin.venice.stats.ServerConnectionStats;
import com.linkedin.venice.utils.SslUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLPeerUnverifiedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ServerConnectionStatsHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOGGER = LogManager.getLogger(ServerConnectionStatsHandler.class);
  private final ServerConnectionStats serverConnectionStats;
  private final String routerPrincipalName;
  private VeniceServerNettyStats nettyStats;
  private static final AttributeKey<Boolean> CHANNEL_ACTIVATED = AttributeKey.valueOf("channelActivated");

  public ServerConnectionStatsHandler(ServerConnectionStats serverConnectionStats, String routerPrincipalName) {
    this.serverConnectionStats = serverConnectionStats;
    this.routerPrincipalName = routerPrincipalName;
  }

  public ServerConnectionStatsHandler(
      ServerConnectionStats serverConnectionStats,
      VeniceServerNettyStats nettyStats,
      String routerPrincipalName) {
    this.serverConnectionStats = serverConnectionStats;
    this.routerPrincipalName = routerPrincipalName;
    this.nettyStats = nettyStats;
  }

  @Override
  public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
    SslHandler sslHandler = extractSslHandler(ctx);
    if (sslHandler == null) {
      // No ssl enabled, record all connections as client connections
      long cnt = serverConnectionStats.incrementClientConnectionCount();
      LOGGER.debug("####Channel registered: {} - clientCount: {}", ctx.channel().remoteAddress(), cnt);
      return;
    }
    long cnt;
    String principalName = getPrincipal(sslHandler);
    if (principalName != null && principalName.contains("venice-router")) {
      cnt = serverConnectionStats.incrementRouterConnectionCount();
    } else {
      cnt = serverConnectionStats.incrementClientConnectionCount();
    }
    LOGGER.debug("####Channel registered: {} - clientCount: {}", ctx.channel().remoteAddress(), cnt);
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    Attribute<Boolean> activated = ctx.channel().attr(CHANNEL_ACTIVATED);
    if (activated.get() != null && activated.get()) {
      return;
    }
    activated.set(true);
    int activeConnections = nettyStats.incrementActiveConnections();
    LOGGER.debug("####Channel active: {} - activeCount: {}", ctx.channel().remoteAddress(), activeConnections);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    Attribute<Boolean> activated = ctx.channel().attr(CHANNEL_ACTIVATED);
    if (activated.get() == null || !activated.get()) {
      return;
    }
    activated.set(false);
    int activeConnections = nettyStats.decrementActiveConnections();
    LOGGER.debug("####Channel inactive: {} - activeCount: {}", ctx.channel().remoteAddress(), activeConnections);
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    if (ctx.channel().remoteAddress() == null) {
      return;
    }
    SslHandler sslHandler = extractSslHandler(ctx);
    if (sslHandler == null) {
      // No ssl enabled, record all connections as client connections
      long cnt = serverConnectionStats.decrementClientConnectionCount();
      LOGGER.debug("####Channel unregistered: {} - clientCount: {}", ctx.channel().remoteAddress(), cnt);
      return;
    }
    String principalName = getPrincipal(sslHandler);
    long cnt;
    if (principalName != null && principalName.contains("venice-router")) {
      cnt = serverConnectionStats.decrementRouterConnectionCount();
    } else {
      cnt = serverConnectionStats.decrementClientConnectionCount();
    }
    LOGGER.debug("####Channel unregistered: {} - clientCount: {}", ctx.channel().remoteAddress(), cnt);
  }

  protected SslHandler extractSslHandler(ChannelHandlerContext ctx) {
    return ServerHandlerUtils.extractSslHandler(ctx);
  }

  private String getPrincipal(SslHandler sslHandler) throws SSLPeerUnverifiedException {
    X509Certificate clientCert = SslUtils.getX509Certificate(sslHandler.engine().getSession().getPeerCertificates()[0]);
    return clientCert.getSubjectX500Principal().getName();
  }
}
