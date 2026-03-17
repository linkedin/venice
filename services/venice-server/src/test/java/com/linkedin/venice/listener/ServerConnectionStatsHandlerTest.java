package com.linkedin.venice.listener;

import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.acl.VeniceComponent;
import com.linkedin.venice.stats.ServerConnectionStats;
import com.linkedin.venice.utils.LogContext;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.util.Attribute;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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

  /**
   * Creates a mock {@link Attribute} backed by an {@link AtomicReference} so that {@code get()} and {@code set()}
   * actually store and retrieve values. This avoids Mockito thread-safety issues when the production code's
   * background scanner thread and the test thread both interact with the same mock concurrently.
   */
  @SuppressWarnings("unchecked")
  private <T> Attribute<T> createBackedAttribute(T initialValue) {
    AtomicReference<T> backing = new AtomicReference<>(initialValue);
    Attribute<T> attr = mock(Attribute.class);
    doAnswer(inv -> backing.get()).when(attr).get();
    doAnswer(inv -> {
      backing.set(inv.getArgument(0));
      return null;
    }).when(attr).set(org.mockito.ArgumentMatchers.any());
    return attr;
  }

  /**
   * Waits until the scanner thread sets the channel-activated attribute to {@code true}. This ensures the scanner
   * has fully completed its processing before the test proceeds to call {@code channelInactive}.
   */
  private void waitForActivated(Attribute<Boolean> attr) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (System.nanoTime() < deadline) {
      Boolean val = attr.get();
      if (val != null && val) {
        return;
      }
      Thread.sleep(10);
    }
    throw new AssertionError("Timed out waiting for channel activated attribute to become true");
  }

  @Test
  public void testChannelRegisteredUnregisteredWithNoSslHandler() throws Exception {
    ServerConnectionStats serverConnectionStats = mock(ServerConnectionStats.class);
    ServerConnectionStatsHandler serverConnectionStatsHandler = new ServerConnectionStatsHandler(
        null,
        serverConnectionStats,
        "venice-router",
        LogContext.forTests(VeniceComponent.SERVER.name()));
    Attribute<Boolean> channelActivatedAttr = createBackedAttribute(false);
    when(channel.attr(ServerConnectionStatsHandler.CHANNEL_ACTIVATED)).thenReturn(channelActivatedAttr);
    serverConnectionStatsHandler.channelActive(context);
    verify(serverConnectionStats, never()).incrementClientConnectionCount();
    verify(serverConnectionStats, never()).decrementClientConnectionCount();
    // No SSL handler in the pipeline, so the scanner cannot extract a principal and will never
    // set activated=true. Manually set it to simulate an activated channel for the inactive path.
    channelActivatedAttr.set(true);
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
    ServerConnectionStatsHandler serverConnectionStatsHandler = new ServerConnectionStatsHandler(
        null,
        serverConnectionStats,
        veniceRouterPrincipalString,
        LogContext.forTests(VeniceComponent.SERVER.name()));

    // Use a real-backed attribute so the scanner thread's activated.set(true) and the test thread's
    // channelInactive -> activated.get() share real state without re-stubbing from multiple threads.
    // This eliminates the Mockito thread-safety race that caused the flaky failure.
    Attribute<Boolean> channelActivatedAttr = createBackedAttribute(null);
    when(channel.attr(ServerConnectionStatsHandler.CHANNEL_ACTIVATED)).thenReturn(channelActivatedAttr);
    when(channelActivatedAttr.get()).thenReturn(false);

    // Scanner reads SETUP_LATENCY_MS after identifying the connection source
    Attribute<Double> setupLatencyAttr = mock(Attribute.class);
    when(channel.attr(ServerConnectionStatsHandler.SETUP_LATENCY_MS)).thenReturn(setupLatencyAttr);
    when(setupLatencyAttr.getAndSet(null)).thenReturn(null);

    // First connection: router principal
    serverConnectionStatsHandler.channelActive(context);
    // Wait for the scanner to process the connection and call incrementRouterConnectionCount().
    // The scanner also sets activated=true on the real-backed attribute.
    verify(serverConnectionStats, timeout(3000).times(1)).incrementRouterConnectionCount();
    // Wait until the scanner has fully completed processing (including setting activated=true)
    // before calling channelInactive. The timeout verify above only guarantees the increment
    // happened; the scanner thread may not have executed activated.set(true) yet.
    waitForActivated(channelActivatedAttr);

    serverConnectionStatsHandler.channelInactive(context);
    verify(serverConnectionStats, times(1)).decrementRouterConnectionCount();
    verify(serverConnectionStats, never()).incrementClientConnectionCount();
    verify(serverConnectionStats, never()).decrementClientConnectionCount();

    // Second connection: client principal (3rd call to getSubjectX500Principal returns clientPrincipal)
    serverConnectionStatsHandler.channelActive(context);
    verify(serverConnectionStats, timeout(3000).times(1)).incrementClientConnectionCount();
    waitForActivated(channelActivatedAttr);

    serverConnectionStatsHandler.channelInactive(context);
    verify(serverConnectionStats, times(1)).incrementRouterConnectionCount();
    verify(serverConnectionStats, times(1)).decrementRouterConnectionCount();
    verify(serverConnectionStats, times(1)).decrementClientConnectionCount();
    verify(serverConnectionStats, times(2)).newConnectionRequest();
  }

  private ServerConnectionStatsHandler createLatencyTestHandler() {
    return new ServerConnectionStatsHandler(
        null,
        mock(ServerConnectionStats.class),
        "venice-router",
        LogContext.forTests(VeniceComponent.SERVER.name()));
  }

  private Attribute<Double> mockSetupLatencyAttr() {
    Attribute<Double> attr = mock(Attribute.class);
    when(channel.attr(ServerConnectionStatsHandler.SETUP_LATENCY_MS)).thenReturn(attr);
    return attr;
  }

  @Test
  public void testConnectionSetupLatencyStoredOnHandshakeSuccess() throws Exception {
    ServerConnectionStatsHandler handler = createLatencyTestHandler();

    Attribute<Long> initStartTsAttr = mock(Attribute.class);
    when(channel.attr(ServerConnectionStatsHandler.CHANNEL_INIT_START_TS)).thenReturn(initStartTsAttr);
    when(initStartTsAttr.getAndSet(null)).thenReturn(System.nanoTime());

    Attribute<Double> setupLatencyAttr = mockSetupLatencyAttr();

    handler.userEventTriggered(context, SslHandshakeCompletionEvent.SUCCESS);

    ArgumentCaptor<Double> latencyCaptor = ArgumentCaptor.forClass(Double.class);
    verify(setupLatencyAttr, times(1)).set(latencyCaptor.capture());
    assertTrue(latencyCaptor.getValue() >= 0, "Latency should be non-negative");
  }

  @Test
  public void testConnectionSetupLatencyNotStoredOnHandshakeFailure() throws Exception {
    ServerConnectionStatsHandler handler = createLatencyTestHandler();
    Attribute<Double> setupLatencyAttr = mockSetupLatencyAttr();

    SslHandshakeCompletionEvent failedEvent =
        new SslHandshakeCompletionEvent(new javax.net.ssl.SSLHandshakeException("test failure"));
    handler.userEventTriggered(context, failedEvent);

    verify(setupLatencyAttr, never()).set(anyDouble());
  }

  @Test
  public void testConnectionSetupLatencyNotStoredWhenTimestampMissing() throws Exception {
    ServerConnectionStatsHandler handler = createLatencyTestHandler();

    Attribute<Long> initStartTsAttr = mock(Attribute.class);
    when(channel.attr(ServerConnectionStatsHandler.CHANNEL_INIT_START_TS)).thenReturn(initStartTsAttr);
    when(initStartTsAttr.getAndSet(null)).thenReturn(null);

    Attribute<Double> setupLatencyAttr = mockSetupLatencyAttr();

    handler.userEventTriggered(context, SslHandshakeCompletionEvent.SUCCESS);

    verify(setupLatencyAttr, never()).set(anyDouble());
  }
}
