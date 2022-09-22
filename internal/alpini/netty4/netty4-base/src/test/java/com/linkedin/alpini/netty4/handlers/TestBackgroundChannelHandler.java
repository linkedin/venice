package com.linkedin.alpini.netty4.handlers;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 5/5/17.
 */
public class TestBackgroundChannelHandler {
  @Test(groups = "unit")
  public void testHandlerSimple() throws InterruptedException {

    EmbeddedChannel channel = new EmbeddedChannel(new BackgroundChannelHandler(new ChannelDuplexHandler() {
      @Override
      public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        super.write(ctx, msg, promise);
        // ctx.writeAndFlush(msg, promise);
      }
    }));

    channel.writeInbound(1, 2, 3, 4, 5);
    Thread.sleep(10);

    Assert.assertEquals(channel.readInbound(), Integer.valueOf(1));
    Assert.assertEquals(channel.readInbound(), Integer.valueOf(2));
    Assert.assertEquals(channel.readInbound(), Integer.valueOf(3));
    Assert.assertEquals(channel.readInbound(), Integer.valueOf(4));
    Assert.assertEquals(channel.readInbound(), Integer.valueOf(5));
    Assert.assertNull(channel.readInbound());

    channel.writeOutbound(6, 7, 8, 9);
    ChannelFuture f = channel.writeAndFlush(10);
    while (!f.isDone()) {
      Thread.sleep(10);
    }

    Assert.assertEquals(channel.readOutbound(), Integer.valueOf(6));
    Assert.assertEquals(channel.readOutbound(), Integer.valueOf(7));
    Assert.assertEquals(channel.readOutbound(), Integer.valueOf(8));
    Assert.assertEquals(channel.readOutbound(), Integer.valueOf(9));
    Assert.assertEquals(channel.readOutbound(), Integer.valueOf(10));
    Assert.assertNull(channel.readOutbound());

  }

}
