package com.linkedin.venice.listener;

import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.ServerConnectionStats;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.util.Attribute;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ServerConnectionStatsHandlerTest {
  private ChannelHandlerContext context;
  private ChannelPipeline pipeline;
  private Channel channel;

  @BeforeMethod(alwaysRun = true)
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
        new ServerConnectionStatsHandler(null, serverConnectionStats, "venice-router");
    Attribute<Boolean> channelActivatedAttr = mock(Attribute.class);
    when(channel.attr(ServerConnectionStatsHandler.CHANNEL_ACTIVATED)).thenReturn(channelActivatedAttr);
    when(channelActivatedAttr.get()).thenReturn(false);
    serverConnectionStatsHandler.channelActive(context);
    verify(serverConnectionStats, never()).incrementClientConnectionCount();
    verify(serverConnectionStats, never()).decrementClientConnectionCount();
    when(channelActivatedAttr.get()).thenReturn(true);
    serverConnectionStatsHandler.channelInactive(context);
    verify(serverConnectionStats, never()).incrementClientConnectionCount();
    verify(serverConnectionStats, never()).decrementClientConnectionCount();
    verify(serverConnectionStats, never()).incrementRouterConnectionCount();
    verify(serverConnectionStats, never()).decrementRouterConnectionCount();
    verify(serverConnectionStats, times(1)).newConnectionRequest();
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
        new ServerConnectionStatsHandler(null, serverConnectionStats, veniceRouterPrincipalString);
    Attribute<Boolean> channelActivatedAttr = mock(Attribute.class);
    when(channel.attr(ServerConnectionStatsHandler.CHANNEL_ACTIVATED)).thenReturn(channelActivatedAttr);
    when(channelActivatedAttr.get()).thenReturn(false);
    serverConnectionStatsHandler.channelActive(context);
    verify(serverConnectionStats, timeout(3000).times(1)).incrementRouterConnectionCount();
    when(channelActivatedAttr.get()).thenReturn(true);
    serverConnectionStatsHandler.channelInactive(context);
    verify(serverConnectionStats, times(1)).decrementRouterConnectionCount();
    verify(serverConnectionStats, never()).incrementClientConnectionCount();
    verify(serverConnectionStats, never()).decrementClientConnectionCount();
    when(channelActivatedAttr.get()).thenReturn(false);
    serverConnectionStatsHandler.channelActive(context);
    verify(serverConnectionStats, timeout(3000).times(1)).incrementClientConnectionCount();

    when(channelActivatedAttr.get()).thenReturn(true);
    serverConnectionStatsHandler.channelInactive(context);
    verify(serverConnectionStats, timeout(3000).times(1)).incrementRouterConnectionCount();
    verify(serverConnectionStats, times(1)).decrementRouterConnectionCount();
    verify(serverConnectionStats, times(1)).decrementClientConnectionCount();
    verify(serverConnectionStats, times(2)).newConnectionRequest();
  }

  @Test
  public void testConnectionSetupLatencyRecordedOnHandshakeSuccess() throws Exception {
    ServerConnectionStats serverConnectionStats = mock(ServerConnectionStats.class);
    ServerConnectionStatsHandler handler =
        new ServerConnectionStatsHandler(null, serverConnectionStats, "venice-router");

    // Simulate initChannel setting the start timestamp
    Attribute<Long> initStartTsAttr = mock(Attribute.class);
    long startTs = System.nanoTime();
    when(channel.attr(ServerConnectionStatsHandler.CHANNEL_INIT_START_TS)).thenReturn(initStartTsAttr);
    when(initStartTsAttr.getAndSet(null)).thenReturn(startTs);

    // Fire a successful handshake event
    handler.userEventTriggered(context, SslHandshakeCompletionEvent.SUCCESS);

    // Verify latency was recorded with a non-negative value
    ArgumentCaptor<Double> latencyCaptor = ArgumentCaptor.forClass(Double.class);
    verify(serverConnectionStats, times(1)).recordNewConnectionSetupLatency(latencyCaptor.capture());
    assertTrue(latencyCaptor.getValue() >= 0, "Latency should be non-negative");
  }

  @Test
  public void testConnectionSetupLatencyNotRecordedOnHandshakeFailure() throws Exception {
    ServerConnectionStats serverConnectionStats = mock(ServerConnectionStats.class);
    ServerConnectionStatsHandler handler =
        new ServerConnectionStatsHandler(null, serverConnectionStats, "venice-router");

    // Fire a failed handshake event
    SslHandshakeCompletionEvent failedEvent =
        new SslHandshakeCompletionEvent(new javax.net.ssl.SSLHandshakeException("test failure"));
    handler.userEventTriggered(context, failedEvent);

    // Verify latency was NOT recorded
    verify(serverConnectionStats, never()).recordNewConnectionSetupLatency(anyDouble());
  }

  @Test
  public void testConnectionSetupLatencyNotRecordedWhenTimestampMissing() throws Exception {
    ServerConnectionStats serverConnectionStats = mock(ServerConnectionStats.class);
    ServerConnectionStatsHandler handler =
        new ServerConnectionStatsHandler(null, serverConnectionStats, "venice-router");

    // No timestamp was set (e.g., non-SSL path)
    Attribute<Long> initStartTsAttr = mock(Attribute.class);
    when(channel.attr(ServerConnectionStatsHandler.CHANNEL_INIT_START_TS)).thenReturn(initStartTsAttr);
    when(initStartTsAttr.getAndSet(null)).thenReturn(null);

    handler.userEventTriggered(context, SslHandshakeCompletionEvent.SUCCESS);

    verify(serverConnectionStats, never()).recordNewConnectionSetupLatency(anyDouble());
  }
}
