package com.linkedin.alpini.netty4.handlers;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.embedded.EmbeddedChannel;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = { "unit", "unit-leak", "leak" }, singleThreaded = true)
public final class TestOutboundByteBufAggregator extends AbstractLeakDetect {
  @Test
  public void testBasic() {
    EmbeddedChannel ch = new EmbeddedChannel(new OutboundByteBufAggregator());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    basic(ch);
    ch.finishAndReleaseAll();
  }

  private void basic(EmbeddedChannel ch) {
    ByteBuf test = encodeString("Hello world", StandardCharsets.ISO_8859_1);
    Assert.assertTrue(test.hasMemoryAddress());

    ChannelFuture future = ch.writeOneOutbound(test);

    Assert.assertFalse(future.isDone());
    Assert.assertNull(ch.readOutbound());

    ch.flushOutbound();

    Assert.assertTrue(future.isSuccess());
    ByteBuf outbound = ch.readOutbound();

    Assert.assertSame(outbound, test);

    test.release();
  }

  @Test
  public void testBasicPassthrough() {
    EmbeddedChannel ch = new EmbeddedChannel(new OutboundByteBufAggregator());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    basicPassthrough(ch);
    ch.finishAndReleaseAll();
  }

  private void basicPassthrough(EmbeddedChannel ch) {
    ByteBuf test = Unpooled.wrappedBuffer(new byte[16384]);

    ChannelFuture future = ch.writeOneOutbound(test);

    Assert.assertFalse(future.isDone());
    Assert.assertNull(ch.readOutbound());

    ch.flushOutbound();

    Assert.assertTrue(future.isSuccess());
    ByteBuf outbound = ch.readOutbound();

    Assert.assertSame(outbound, test);

    test.release();
  }

  @Test
  public void testBasicComposite() {
    EmbeddedChannel ch = new EmbeddedChannel(new OutboundByteBufAggregator());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    basicComposite(ch);
    ch.finishAndReleaseAll();
  }

  private void basicComposite(EmbeddedChannel ch) {

    ByteBuf testBuf = encodeString("xxxxxxxxxxHello World! This is a test.", StandardCharsets.ISO_8859_1);
    ByteBuf testBuf2 = encodeString(
        ";UG'IUG;IG;IG;/G;O/UG;OUG;IGUGUGIGGIGIIGIGGGUGUGGUGGUGUGGIGGUOUGOUGGUOGUOOGUOUG! This is a test.42t24t24t24tg24t24t",
        StandardCharsets.ISO_8859_1);

    ChannelFuture future = ch.writeOneOutbound(
        POOLED_ALLOCATOR.compositeBuffer()
            .addComponents(true, testBuf.readRetainedSlice(22), testBuf2.retainedSlice(118 - 38, 16))
            .slice(10, 102 - 74));
    testBuf.release();
    testBuf2.release();

    Assert.assertFalse(future.isDone());
    Assert.assertNull(ch.readOutbound());

    ch.flushOutbound();

    Assert.assertTrue(future.isSuccess());
    ByteBuf outbound = ch.readOutbound();
    Assert.assertNull(ch.readOutbound());

    Assert.assertEquals(outbound.toString(StandardCharsets.ISO_8859_1), "Hello World! This is a test.");
    Assert.assertFalse(outbound instanceof CompositeByteBuf);
    Assert.assertTrue(outbound.hasMemoryAddress());

    outbound.release();
  }

  @Test
  public void testSimpleComposite() {
    EmbeddedChannel ch = new EmbeddedChannel(new OutboundByteBufAggregator());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    simpleComposite(ch);
    ch.finishAndReleaseAll();
  }

  private void simpleComposite(EmbeddedChannel ch) {
    ChannelFuture future = ch.writeOneOutbound(Unpooled.wrappedBuffer(new byte[8192], new byte[8192]));

    Assert.assertFalse(future.isDone());
    Assert.assertNull(ch.readOutbound());

    ch.flushOutbound();

    Assert.assertTrue(future.isSuccess());
    ByteBuf outbound = ch.readOutbound();
    Assert.assertNull(ch.readOutbound());

    Assert.assertEquals(outbound.readableBytes(), 16384);
    Assert.assertFalse(outbound instanceof CompositeByteBuf);

    outbound.release();
  }

  @Test
  public void testOverflowComposite() {
    EmbeddedChannel ch = new EmbeddedChannel(new OutboundByteBufAggregator());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    overflowComposite(ch);
    ch.finishAndReleaseAll();
  }

  private void overflowComposite(EmbeddedChannel ch) {
    ChannelFuture future = ch.writeOneOutbound(Unpooled.wrappedBuffer(new byte[8192], new byte[16384]));

    Assert.assertFalse(future.isDone());
    Assert.assertNull(ch.readOutbound());

    ch.flushOutbound();

    Assert.assertTrue(future.isSuccess());
    ByteBuf outbound = ch.readOutbound();
    Assert.assertNull(ch.readOutbound());

    Assert.assertEquals(outbound.readableBytes(), 24576);
    Assert.assertTrue(outbound instanceof CompositeByteBuf);

    Iterator<ByteBuf> it = ((CompositeByteBuf) outbound).iterator();
    Assert.assertEquals(it.next().readableBytes(), 16384);
    Assert.assertEquals(it.next().readableBytes(), 8192);
    Assert.assertFalse(it.hasNext());

    outbound.release();
  }

  @Test
  public void testOverflowComposite2() {
    EmbeddedChannel ch = new EmbeddedChannel(new OutboundByteBufAggregator());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    overflowComposite2(ch);
    ch.finishAndReleaseAll();
  }

  private void overflowComposite2(EmbeddedChannel ch) {
    ChannelFuture future = ch.writeOneOutbound(Unpooled.wrappedBuffer(new byte[8192], new byte[24576]));

    Assert.assertFalse(future.isDone());
    Assert.assertNull(ch.readOutbound());

    ch.flushOutbound();

    Assert.assertTrue(future.isSuccess());
    ByteBuf outbound = ch.readOutbound();
    Assert.assertNull(ch.readOutbound());

    Assert.assertEquals(outbound.readableBytes(), 32768);
    Assert.assertTrue(outbound instanceof CompositeByteBuf);

    Iterator<ByteBuf> it = ((CompositeByteBuf) outbound).iterator();
    ByteBuf buf = it.next();
    Assert.assertEquals(buf.readableBytes(), 16384);
    Assert.assertFalse(buf.isReadOnly());
    buf = it.next();
    Assert.assertEquals(buf.readableBytes(), 16384);
    Assert.assertTrue(buf.isReadOnly()); // passed-through bytebuf is readonly.

    Assert.assertFalse(it.hasNext());

    outbound.release();
  }

  @Test
  public void testSimple1() {
    EmbeddedChannel ch = new EmbeddedChannel(new OutboundByteBufAggregator());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    simple1(ch);
    ch.finishAndReleaseAll();
  }

  private void simple1(EmbeddedChannel ch) {
    ChannelFuture future1 = ch.writeOneOutbound(Unpooled.wrappedBuffer(new byte[8192]));
    ChannelFuture future2 = ch.writeOneOutbound(Unpooled.wrappedBuffer(new byte[8192]));

    Assert.assertFalse(future1.isDone());
    Assert.assertFalse(future2.isDone());
    Assert.assertNull(ch.readOutbound());

    ch.flushOutbound();

    Assert.assertTrue(future1.isSuccess());
    Assert.assertTrue(future2.isSuccess());
    ByteBuf outbound = ch.readOutbound();
    Assert.assertNull(ch.readOutbound());

    Assert.assertEquals(outbound.readableBytes(), 16384);

    outbound.release();
  }

  @Test
  public void testSimple0() {
    EmbeddedChannel ch = new EmbeddedChannel(new OutboundByteBufAggregator());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    simple0(ch);
    ch.finishAndReleaseAll();
  }

  private void simple0(EmbeddedChannel ch) {
    ChannelFuture future1 = ch.writeOneOutbound(encodeString("Hello world!", StandardCharsets.ISO_8859_1));
    ChannelFuture future2 = ch.writeOneOutbound(encodeString(" This is a test.", StandardCharsets.ISO_8859_1));

    Assert.assertFalse(future1.isDone());
    Assert.assertFalse(future2.isDone());
    Assert.assertNull(ch.readOutbound());

    ch.flushOutbound();

    Assert.assertTrue(future1.isSuccess());
    Assert.assertTrue(future2.isSuccess());
    ByteBuf outbound = ch.readOutbound();

    Assert.assertNotNull(outbound);
    Assert.assertNull(ch.readOutbound());

    Assert.assertEquals(outbound.toString(StandardCharsets.ISO_8859_1), "Hello world! This is a test.");

    outbound.release();
  }

  @Test
  public void testGauntlet() {
    EmbeddedChannel ch = new EmbeddedChannel(new OutboundByteBufAggregator());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    simple0(ch);
    simple1(ch);
    overflowComposite2(ch);
    overflowComposite(ch);
    simpleComposite(ch);
    basicComposite(ch);
    simpleComposite(ch);
    basicPassthrough(ch);
    basic(ch);
    ch.finishAndReleaseAll();
  }

  /**
   * since TestNG tends to sort by method name, this tries to be the last test
   * in the class. We do this because the AfterClass annotated methods may
   * execute after other tests classes have run and doesn't execute immediately
   * after the methods in this test class.
   */
  @Test(alwaysRun = true)
  public final void zz9PluralZAlpha() throws InterruptedException {
    finallyLeakDetect();
  }
}
