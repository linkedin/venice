package com.linkedin.alpini.netty4.handlers;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = { "unit", "unit-leak", "leak" }, singleThreaded = true)
public final class TestHttpByteBufContentChunker extends AbstractLeakDetect {
  @Test
  public void testFullMessagePassthrough() {
    HttpByteBufContentChunker chunker = new HttpByteBufContentChunker(1024, 512);
    EmbeddedChannel ch = new EmbeddedChannel(chunker);
    FullHttpMessage msg = Mockito.mock(FullHttpMessage.class);
    Mockito.when(msg.touch(Mockito.any())).thenReturn(msg);
    Mockito.when(msg.retain()).thenReturn(msg);
    Mockito.when(msg.content()).thenReturn(Unpooled.EMPTY_BUFFER);
    ch.writeOneOutbound(msg);
    ch.flushOutbound();
    Mockito.verify(msg, Mockito.times(2)).touch(Mockito.any());
    Mockito.verify(msg, Mockito.times(2)).release();
    Mockito.verify(msg, Mockito.times(2)).retain();
    Mockito.verify(msg, Mockito.times(2)).content();
    Mockito.verifyNoMoreInteractions(msg);
    Assert.assertSame(ch.readOutbound(), msg);
    Assert.assertEquals(chunker._chunkOn, 0);
    ch.finishAndReleaseAll();
  }

  @Test
  public void testByteBufPassthrough() {
    HttpByteBufContentChunker chunker = new HttpByteBufContentChunker(1024, 512);
    EmbeddedChannel ch = new EmbeddedChannel(chunker);
    ByteBuf msg = Mockito.mock(ByteBuf.class);
    Mockito.when(msg.touch(Mockito.any())).thenReturn(msg);
    Mockito.when(msg.retain()).thenReturn(msg);
    ch.writeOneOutbound(msg);
    ch.flushOutbound();
    Mockito.verify(msg, Mockito.times(2)).touch(Mockito.any());
    Mockito.verify(msg, Mockito.times(2)).release();
    Mockito.verify(msg, Mockito.times(2)).retain();
    Mockito.verify(msg, Mockito.times(2)).readableBytes();
    Mockito.verifyNoMoreInteractions(msg);
    Assert.assertSame(ch.readOutbound(), msg);
    Assert.assertEquals(chunker._chunkOn, 0);
    ch.finishAndReleaseAll();
  }

  @Test
  public void testChunkedByteBufMessage() {
    HttpByteBufContentChunker chunker = new HttpByteBufContentChunker(1024, 512);
    EmbeddedChannel ch = new EmbeddedChannel(chunker);
    HttpResponse msg = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    ByteBuf chunk = ByteBufUtil.writeAscii(
        POOLED_ALLOCATOR,
        IntStream.range(1, 300).mapToObj(String::valueOf).collect(Collectors.joining("-")));
    int size = chunk.readableBytes();
    ch.writeOutbound(msg, chunk, LastHttpContent.EMPTY_LAST_CONTENT);
    ch.flushOutbound();
    Assert.assertSame(ch.readOutbound(), msg);
    HttpContent chunk1 = ch.readOutbound();
    HttpContent chunk2 = ch.readOutbound();
    Assert.assertSame(ch.readOutbound(), LastHttpContent.EMPTY_LAST_CONTENT);
    Assert.assertEquals(chunk.refCnt(), 0);
    Assert.assertEquals(chunk1.content().readableBytes(), 512);
    Assert.assertEquals(chunk1.content().readableBytes() + chunk2.content().readableBytes(), size);
    chunk1.release();
    chunk2.release();
    Assert.assertEquals(chunker._chunkOn, 0);
    ch.finishAndReleaseAll();
  }

  @Test
  public void testChunkedMessage() {
    HttpByteBufContentChunker chunker = new HttpByteBufContentChunker(1024, 512);
    EmbeddedChannel ch = new EmbeddedChannel(chunker);
    HttpResponse msg = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    HttpContent chunk = new DefaultHttpContent(
        ByteBufUtil.writeAscii(
            POOLED_ALLOCATOR,
            IntStream.range(1, 300).mapToObj(String::valueOf).collect(Collectors.joining("-"))));
    int size = chunk.content().readableBytes();
    ch.writeOutbound(msg, chunk, LastHttpContent.EMPTY_LAST_CONTENT);
    ch.flushOutbound();
    Assert.assertSame(ch.readOutbound(), msg);
    HttpContent chunk1 = ch.readOutbound();
    HttpContent chunk2 = ch.readOutbound();
    Assert.assertSame(ch.readOutbound(), LastHttpContent.EMPTY_LAST_CONTENT);
    Assert.assertEquals(chunk.refCnt(), 0);
    Assert.assertEquals(chunk1.content().readableBytes(), 512);
    Assert.assertEquals(chunk1.content().readableBytes() + chunk2.content().readableBytes(), size);
    chunk1.release();
    chunk2.release();
    Assert.assertEquals(chunker._chunkOn, 0);
    ch.finishAndReleaseAll();
  }

  @Test
  public void testChunkedLastMessage() {
    HttpByteBufContentChunker chunker = new HttpByteBufContentChunker(1024, 512);
    EmbeddedChannel ch = new EmbeddedChannel(chunker);
    HttpResponse msg = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    LastHttpContent chunk = new DefaultLastHttpContent(
        ByteBufUtil.writeAscii(
            POOLED_ALLOCATOR,
            IntStream.range(1, 300).mapToObj(String::valueOf).collect(Collectors.joining("-"))));
    chunk.trailingHeaders().add("Foo", "Bar");
    int size = chunk.content().readableBytes();
    ch.writeOutbound(msg, chunk);
    ch.flushOutbound();
    Assert.assertSame(ch.readOutbound(), msg);
    HttpContent chunk1 = ch.readOutbound();
    HttpContent chunk2 = ch.readOutbound();
    LastHttpContent last = ch.readOutbound();
    Assert.assertEquals(last.trailingHeaders().toString(), chunk.trailingHeaders().toString());
    Assert.assertEquals(chunk.refCnt(), 0);
    Assert.assertEquals(chunk1.content().readableBytes(), 512);
    Assert.assertEquals(chunk1.content().readableBytes() + chunk2.content().readableBytes(), size);
    chunk1.release();
    chunk2.release();
    Assert.assertEquals(chunker._chunkOn, 0);
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
