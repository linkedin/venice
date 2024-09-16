package com.linkedin.venice.listener;

import static com.linkedin.venice.response.VeniceReadResponseStatus.INTERNAL_SERVER_ERROR;

import com.linkedin.venice.listener.response.HttpShortcutResponse;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;


/***
 * Expects a GetRequestObject which has store name, key, and partition
 * Queries the local store for the associated value
 * writes the value (as a byte[]) back down the stack
 */
@ChannelHandler.Sharable
public class ErrorCatchingHandler extends ChannelInboundHandlerAdapter {
  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    ctx.writeAndFlush(new HttpShortcutResponse(cause.getMessage(), INTERNAL_SERVER_ERROR.getHttpResponseStatus()));
    ctx.close();
  }

  @Override
  public void channelRead(ChannelHandlerContext context, Object message) throws Exception {
  }

}
