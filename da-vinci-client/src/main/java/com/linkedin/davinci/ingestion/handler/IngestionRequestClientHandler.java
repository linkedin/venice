package com.linkedin.davinci.ingestion.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicReference;


public class IngestionRequestClientHandler extends SimpleChannelInboundHandler<FullHttpResponse> {
  private static final Logger logger = Logger.getLogger(IngestionRequestClientHandler.class);
  private final AtomicReference<FullHttpResponse> response = new AtomicReference<>();

  public IngestionRequestClientHandler() {
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    super.channelActive(ctx);
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) {
    FullHttpResponse oldResponse = response.getAndSet(msg.retainedDuplicate());
    if (oldResponse != null) {
      oldResponse.release();
    }
    ctx.close();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    logger.error("Caught exception: " + cause.getMessage());
    ctx.close();
  }

  public FullHttpResponse getResponse() {
    return response.getAndSet(null);
  }
}

