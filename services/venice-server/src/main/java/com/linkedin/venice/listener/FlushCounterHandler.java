package com.linkedin.venice.listener;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


@ChannelHandler.Sharable
public class FlushCounterHandler extends ChannelOutboundHandlerAdapter {
  private static final Logger LOGGER = LogManager.getLogger(FlushCounterHandler.class);
  private final VeniceServerNettyStats nettyStats;

  public FlushCounterHandler(VeniceServerNettyStats nettyStats) {
    this.nettyStats = nettyStats;
  }

  @Override
  public void flush(ChannelHandlerContext ctx) throws Exception {
    try {
      nettyStats.recordNettyFlushCounts();
    } finally {
      super.flush(ctx); // Flush the data to the next handler in the pipeline
    }
  }
}
