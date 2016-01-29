package com.linkedin.venice.listener;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;


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
    if (msg instanceof byte[]){
      body = Unpooled.wrappedBuffer((byte[]) msg);
    } else if (msg instanceof ByteBuf) {
      body = (ByteBuf) msg;
    } else { return; }
    FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, body);
    response.headers().set(CONTENT_TYPE, "text/plain");
    response.headers().set(CONTENT_LENGTH, body.readableBytes());

    ctx.write(response).addListener(ChannelFutureListener.CLOSE);
  }


}
