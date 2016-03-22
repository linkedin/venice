package com.linkedin.venice.listener;

import com.linkedin.venice.HttpConstants;
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

import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/***
 * wraps raw bytes into an HTTP response object that HttpServerCodec expects
 */

public class OutboundHttpWrapperHandler extends ChannelOutboundHandlerAdapter {




  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    ByteBuf body;
    String contentType = HttpConstants.APPLICATION_OCTET;
    HttpResponseStatus responseStatus = OK;
    long offset = -1;
    if (msg instanceof StorageResponseObject){
      StorageResponseObject obj = (StorageResponseObject) msg;
      body = Unpooled.wrappedBuffer(obj.getValue());
      offset = obj.getOffset();
    } else if (msg instanceof HttpError){
      responseStatus = ((HttpError) msg).getStatus();
      body = Unpooled.wrappedBuffer(((HttpError) msg).getMessage().getBytes(StandardCharsets.UTF_8));
      contentType = HttpConstants.TEXT_PLAIN;
    } else { return; }
    FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, responseStatus, body);
    response.headers().set(CONTENT_TYPE, contentType);
    response.headers().set(CONTENT_LENGTH, body.readableBytes());
    if (offset > 0) {
      response.headers().set(HttpConstants.VENICE_OFFSET, offset);
    }

    ctx.write(response).addListener(ChannelFutureListener.CLOSE);
  }


}
