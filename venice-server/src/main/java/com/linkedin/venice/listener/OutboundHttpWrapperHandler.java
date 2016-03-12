package com.linkedin.venice.listener;

import com.linkedin.venice.exceptions.VeniceException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.nio.charset.StandardCharsets;

import static io.netty.handler.codec.http.HttpVersion.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpHeaders.Names.*;

/***
 * wraps raw bytes into an HTTP response object that HttpServerCodec expects
 */

public class OutboundHttpWrapperHandler extends ChannelOutboundHandlerAdapter {




  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    ByteBuf body;
    String contentType = "application/octet-stream";
    HttpResponseStatus responseStatus = OK;
    if (msg instanceof byte[]){
      body = Unpooled.wrappedBuffer((byte[]) msg);
    } else if (msg instanceof ByteBuf) {
      body = (ByteBuf) msg;
    } else if (msg instanceof HttpError){
      responseStatus = ((HttpError) msg).getStatus();
      body = Unpooled.wrappedBuffer(((HttpError) msg).getMessage().getBytes(StandardCharsets.UTF_8));
      contentType = "text/plain";
    } else { return; }
    FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, responseStatus, body);
    response.headers().set(CONTENT_TYPE, contentType);
    response.headers().set(CONTENT_LENGTH, body.readableBytes());

    ctx.write(response).addListener(ChannelFutureListener.CLOSE);
  }


}
