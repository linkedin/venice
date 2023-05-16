package com.linkedin.davinci.ingestion.isolated;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;

import com.linkedin.venice.authorization.IdentityParser;
import com.linkedin.venice.exceptions.VeniceException;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.ssl.SslHandler;
import java.net.SocketAddress;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import org.testng.annotations.Test;


public class IsolatedIngestionServerAclHandlerTest {
  @Test
  public void test() throws Exception {
    IdentityParser parser = mock(IdentityParser.class);
    String allowedPrincipalName = "blah";
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    ChannelPipeline pipeline = mock(ChannelPipeline.class);
    SslHandler sslHandler = mock(SslHandler.class);
    HttpRequest req = mock(HttpRequest.class);
    SSLEngine sslEngine = mock(SSLEngine.class);
    SSLSession sslSession = mock(SSLSession.class);
    Certificate[] peerCertificates = new Certificate[1];
    peerCertificates[0] = mock(X509Certificate.class);
    Channel channel = mock(Channel.class);
    SocketAddress socketAddress = mock(SocketAddress.class);
    IsolatedIngestionServerAclHandler handler = new IsolatedIngestionServerAclHandler(parser, allowedPrincipalName);

    when(ctx.pipeline()).thenReturn(pipeline);
    when(sslHandler.engine()).thenReturn(sslEngine);
    when(sslEngine.getSession()).thenReturn(sslSession);
    when(sslSession.getPeerCertificates()).thenReturn(peerCertificates);
    when(parser.parseIdentityFromCert(any())).thenReturn(allowedPrincipalName);
    when(ctx.channel()).thenReturn(channel);
    when(channel.remoteAddress()).thenReturn(socketAddress);
    when(socketAddress.toString()).thenReturn("socketAddressToString");

    /** Test null {@link SslHandler} */
    verify(ctx, never()).fireChannelRead(any());
    assertThrows(VeniceException.class, () -> handler.channelRead0(ctx, req));

    // Test allowed principal...
    verify(ctx, never()).fireChannelRead(any());
    when(pipeline.get(SslHandler.class)).thenReturn(sslHandler);
    handler.channelRead0(ctx, req);
    verify(ctx, times(1)).fireChannelRead(req);

    // Test wrong principal...
    verify(ctx, times(0)).writeAndFlush(any());
    when(parser.parseIdentityFromCert(any())).thenReturn("bad name");
    handler.channelRead0(ctx, req);
    verify(ctx, times(1)).fireChannelRead(req); // no extra calls to this
    verify(ctx, times(1)).writeAndFlush(any());
  }
}
