package com.linkedin.venice.listener;

import com.linkedin.venice.message.GetRequestObject;
import com.linkedin.venice.server.StoreRepository;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.concurrent.ThreadPoolExecutor;


/***
 * Expects a GetRequestObject which has store name, key, and partition
 * Queries the local store for the associated value
 * writes the value (as a byte[]) back down the stack
 */
@ChannelHandler.Sharable
public class ErrorCatchingHandler extends ChannelInboundHandlerAdapter {

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    ctx.writeAndFlush(new HttpError(cause.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR));
    ctx.close();
  }

  @Override
  public void channelRead(ChannelHandlerContext context, Object message) throws Exception {
  }

}
