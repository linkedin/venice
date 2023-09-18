package com.linkedin.venice.listener;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.Attributes;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.ssl.SslHandler;
import java.net.SocketAddress;
import javax.net.ssl.SSLSession;
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

  @Test
  public void testGrpcPath() {
    ServerCall call = mock(ServerCall.class);
    Metadata headers = new Metadata();
    ServerCallHandler next = mock(ServerCallHandler.class);
    Attributes attrSsl = Attributes.newBuilder()
        .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, mock(SocketAddress.class))
        .set(Grpc.TRANSPORT_ATTR_SSL_SESSION, mock(SSLSession.class))
        .build();
    String method = "UNARY";

    when(call.getMethodDescriptor()).thenReturn(
        MethodDescriptor.<String, String>newBuilder()
            .setType(MethodDescriptor.MethodType.valueOf(method))
            .setFullMethodName("UNARY")
            .setRequestMarshaller(mock(MethodDescriptor.Marshaller.class))
            .setResponseMarshaller(mock(MethodDescriptor.Marshaller.class))
            .build());

    when(call.getAttributes()).thenReturn(attrSsl);
    doReturn(attrSsl).when(call).getAttributes();
    VerifySslHandler handler = new VerifySslHandler();

    handler.interceptCall(call, headers, next);
    verify(next, times(1)).startCall(call, headers);

    Attributes attrNonSsl =
        Attributes.newBuilder().set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, mock(SocketAddress.class)).build();
    when(call.getAttributes()).thenReturn(attrNonSsl);
    handler.interceptCall(call, headers, next);
    verify(call, times(1)).close(any(), any());
    verify(next, times(1)).startCall(call, headers);
  }
}
