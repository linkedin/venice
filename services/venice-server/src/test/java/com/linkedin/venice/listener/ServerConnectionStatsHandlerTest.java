package com.linkedin.venice.listener;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.stats.ServerConnectionStats;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslHandler;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ServerConnectionStatsHandlerTest {
  private ChannelHandlerContext context;
  private ChannelPipeline pipeline;
  private Channel channel;

  @BeforeMethod
  public void setUp() {
    context = mock(ChannelHandlerContext.class);
    pipeline = mock(ChannelPipeline.class);
    doReturn(pipeline).when(context).pipeline();
    channel = mock(Channel.class);
    doReturn(channel).when(context).channel();
  }

  @Test
  public void testChannelRegisteredUnregisteredWithNoSslHandler() throws Exception {
    ServerConnectionStats serverConnectionStats = mock(ServerConnectionStats.class);
    ServerConnectionStatsHandler serverConnectionStatsHandler =
        new ServerConnectionStatsHandler(serverConnectionStats, "venice-router");
    serverConnectionStatsHandler.channelRegistered(context);
    verify(serverConnectionStats, times(1)).incrementClientConnectionCount();
    verify(serverConnectionStats, never()).decrementClientConnectionCount();
    serverConnectionStatsHandler.channelUnregistered(context);
    verify(serverConnectionStats, times(1)).incrementClientConnectionCount();
    verify(serverConnectionStats, times(1)).decrementClientConnectionCount();
    verify(serverConnectionStats, never()).incrementRouterConnectionCount();
    verify(serverConnectionStats, never()).decrementRouterConnectionCount();
  }

  @Test
  public void testChannelRegisteredUnregisteredWithSslHandler() throws Exception {
    String veniceRouterPrincipalString = "CN=venice-router";
    SslHandler sslHandler = mock(SslHandler.class);
    doReturn(sslHandler).when(pipeline).get(eq(SslHandler.class));
    X509Certificate cert = mock(X509Certificate.class);
    X500Principal routerPrincipal = new X500Principal(veniceRouterPrincipalString);
    X500Principal clientPrincipal = new X500Principal("CN=test-client");
    when(cert.getSubjectX500Principal()).thenReturn(routerPrincipal, routerPrincipal, clientPrincipal);
    SSLEngine engine = mock(SSLEngine.class);
    SSLSession session = mock(SSLSession.class);
    Certificate[] certificates = { cert };
    doReturn(certificates).when(session).getPeerCertificates();
    doReturn(session).when(engine).getSession();
    doReturn(engine).when(sslHandler).engine();
    ServerConnectionStats serverConnectionStats = mock(ServerConnectionStats.class);
    ServerConnectionStatsHandler serverConnectionStatsHandler =
        new ServerConnectionStatsHandler(serverConnectionStats, veniceRouterPrincipalString);
    serverConnectionStatsHandler.channelRegistered(context);
    serverConnectionStatsHandler.channelUnregistered(context);
    verify(serverConnectionStats, times(1)).incrementRouterConnectionCount();
    verify(serverConnectionStats, times(1)).decrementRouterConnectionCount();
    verify(serverConnectionStats, never()).incrementClientConnectionCount();
    verify(serverConnectionStats, never()).decrementClientConnectionCount();
    serverConnectionStatsHandler.channelRegistered(context);
    serverConnectionStatsHandler.channelUnregistered(context);
    verify(serverConnectionStats, times(1)).incrementRouterConnectionCount();
    verify(serverConnectionStats, times(1)).decrementRouterConnectionCount();
    verify(serverConnectionStats, times(1)).incrementClientConnectionCount();
    verify(serverConnectionStats, times(1)).decrementClientConnectionCount();
  }
}
