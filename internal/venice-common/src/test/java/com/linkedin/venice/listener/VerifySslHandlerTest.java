package com.linkedin.venice.listener;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.ssl.SslHandler;
import java.net.SocketAddress;
import org.testng.annotations.Test;


public class VerifySslHandlerTest {
  @Test
  public void test() {
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    ChannelPipeline pipeline = mock(ChannelPipeline.class);
    SslHandler sslHandler = mock(SslHandler.class);
    HttpRequest req = mock(HttpRequest.class);
    Channel channel = mock(Channel.class);
    Channel parentChannel = mock(Channel.class);
    SocketAddress socketAddress = mock(SocketAddress.class);
    VerifySslHandler handler = new VerifySslHandler();
    HttpMethod httpMethod = mock(HttpMethod.class);

    when(ctx.pipeline()).thenReturn(pipeline);
    when(ctx.channel()).thenReturn(channel);
    when(channel.remoteAddress()).thenReturn(socketAddress);
    when(socketAddress.toString()).thenReturn("socketAddressToString");
    when(req.method()).thenReturn(httpMethod);
    when(httpMethod.name()).thenReturn("get");

    /** Test null {@link SslHandler} */
    verify(ctx, never()).writeAndFlush(any());
    verify(ctx, never()).close();
    handler.channelRead0(ctx, req);
    verify(ctx, never()).fireChannelRead(any());
    verify(ctx, times(1)).writeAndFlush(any());
    verify(ctx, times(1)).close();

    /** Test other null {@link SslHandler} path */
    when(channel.parent()).thenReturn(parentChannel);
    when(parentChannel.pipeline()).thenReturn(pipeline);
    handler.channelRead0(ctx, req);
    verify(ctx, never()).fireChannelRead(any());
    verify(ctx, times(2)).writeAndFlush(any());
    verify(ctx, times(2)).close();

    /** Test {@link SslHandler} present... */
    when(pipeline.get(SslHandler.class)).thenReturn(sslHandler);
    handler.channelRead0(ctx, req);
    verify(ctx, times(1)).fireChannelRead(req);
  }
}
