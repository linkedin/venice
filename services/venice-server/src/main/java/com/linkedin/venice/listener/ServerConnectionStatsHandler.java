package com.linkedin.venice.listener;

import com.linkedin.venice.stats.ServerConnectionStats;
import com.linkedin.venice.utils.SslUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandler;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLPeerUnverifiedException;


public class ServerConnectionStatsHandler extends ChannelInboundHandlerAdapter {
  private final ServerConnectionStats serverConnectionStats;
  private final String routerPrincipalName;
  private VeniceServerNettyStats nettyStats;

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
      serverConnectionStats.incrementClientConnectionCount();
      return;
    }
    String principalName = getPrincipal(sslHandler);
    if (principalName != null && principalName.contains("venice-router")) {
      serverConnectionStats.incrementRouterConnectionCount();
    } else {
      serverConnectionStats.incrementClientConnectionCount();
    }
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    nettyStats.incrementActiveConnections();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    nettyStats.decrementActiveConnections();
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    SslHandler sslHandler = extractSslHandler(ctx);
    if (sslHandler == null) {
      // No ssl enabled, record all connections as client connections
      serverConnectionStats.decrementClientConnectionCount();
      return;
    }
    String principalName = getPrincipal(sslHandler);
    if (principalName != null && principalName.contains("venice-router")) {
      serverConnectionStats.decrementRouterConnectionCount();
    } else {
      serverConnectionStats.decrementClientConnectionCount();
    }
  }

  protected SslHandler extractSslHandler(ChannelHandlerContext ctx) {
    return ServerHandlerUtils.extractSslHandler(ctx);
  }

  private String getPrincipal(SslHandler sslHandler) throws SSLPeerUnverifiedException {
    X509Certificate clientCert = SslUtils.getX509Certificate(sslHandler.engine().getSession().getPeerCertificates()[0]);
    return clientCert.getSubjectX500Principal().getName();
  }
}
