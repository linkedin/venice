package com.linkedin.venice.listener;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslHandler;
import java.net.SocketAddress;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import org.mockito.ArgumentMatcher;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class StaticAclHandlerTest {
  private StaticAccessController accessController;
  private ChannelHandlerContext ctx;
  private HttpRequest req;
  private StaticAclHandler aclHandler;

  @BeforeMethod
  public void setup() throws Exception {
    ctx = mock(ChannelHandlerContext.class);
    req = mock(HttpRequest.class);

    accessController = mock(StaticAccessController.class);
    aclHandler = spy(new StaticAclHandler(accessController, VeniceComponent.SERVER));

    // Certificate
    ChannelPipeline pipe = mock(ChannelPipeline.class);
    when(ctx.pipeline()).thenReturn(pipe);
    SslHandler sslHandler = mock(SslHandler.class);
    when(pipe.get(SslHandler.class)).thenReturn(sslHandler);
    SSLEngine sslEngine = mock(SSLEngine.class);
    when(sslHandler.engine()).thenReturn(sslEngine);
    SSLSession sslSession = mock(SSLSession.class);
    when(sslEngine.getSession()).thenReturn(sslSession);
    X509Certificate cert = mock(X509Certificate.class);
    when(sslSession.getPeerCertificates()).thenReturn(new Certificate[]{cert});

    // Host
    Channel channel = mock(Channel.class);
    when(ctx.channel()).thenReturn(channel);
    SocketAddress address = mock(SocketAddress.class);
    when(channel.remoteAddress()).thenReturn(address);

    when(req.method()).thenReturn(HttpMethod.GET);
  }

  @Test
  public void testAllow() throws Exception {
    when(accessController.hasAccess(any(), any(), any())).thenReturn(true);
    aclHandler.channelRead0(ctx, req);
    verify(ctx).fireChannelRead(req);
    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.FORBIDDEN)));
  }

  @Test
  public void testDeny() throws Exception {
    when(accessController.hasAccess(any(), any(), any())).thenReturn(false);
    aclHandler.channelRead0(ctx, req);
    verify(ctx, never()).fireChannelRead(req);
    verify(ctx).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.FORBIDDEN)));
  }

  public class ContextMatcher implements ArgumentMatcher<FullHttpResponse> {
    private HttpResponseStatus status;

    public ContextMatcher(HttpResponseStatus status) {
      this.status = status;
    }

    @Override
    public boolean matches(FullHttpResponse argument) {
      return argument.status().equals(status) ;
    }
  }
}