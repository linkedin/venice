package io.netty.channel;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import java.nio.channels.ClosedChannelException;
import org.testng.annotations.Test;


/**
 * Forked from Netty's TestAbstractCoalescingBufferQueue
 *
 * @author Abhishek Andhavarapu
 */
@Test(groups = "unit")
public class TestAbstractCoalescingBufferQueue10294 {

  // See https://github.com/netty/netty/issues/10286
  public void testDecrementAllWhenWriteAndRemoveAll() {
    testDecrementAll(true);
  }

  // See https://github.com/netty/netty/issues/10286
  public void testDecrementAllWhenReleaseAndFailAll() {
    testDecrementAll(false);
  }

  private static void testDecrementAll(boolean write) {
    EmbeddedChannel channel = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
      @Override
      public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        ReferenceCountUtil.release(msg);
        promise.setSuccess();
      }
    }, new ChannelHandlerAdapter() {
    });
    final AbstractCoalescingBufferQueue10294 queue = new AbstractCoalescingBufferQueue10294(channel, 128) {
      @Override
      protected ByteBuf compose(ByteBufAllocator alloc, ByteBuf cumulation, ByteBuf next) {
        return composeIntoComposite(alloc, cumulation, next);
      }

      @Override
      protected ByteBuf removeEmptyValue() {
        return Unpooled.EMPTY_BUFFER;
      }
    };

    final byte[] bytes = new byte[128];
    queue.add(Unpooled.wrappedBuffer(bytes), new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) {
        queue.add(Unpooled.wrappedBuffer(bytes));
        assertEquals(bytes.length, queue.readableBytes());
      }
    });

    assertEquals(bytes.length, queue.readableBytes());

    ChannelHandlerContext ctx = channel.pipeline().lastContext();
    if (write) { // SSLHandler
      queue.writeAndRemoveAll(ctx);
    } else { // DefaultHttp2ConnectionEncoder
      queue.releaseAndFailAll(ctx, new ClosedChannelException());
    }
    ByteBuf buffer = queue.remove(channel.alloc(), 128, channel.newPromise());
    assertFalse(buffer.isReadable());
    buffer.release();

    assertTrue(queue.isEmpty());
    assertEquals(0, queue.readableBytes());

    assertFalse(channel.finish());
  }
}
