/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.alpini.netty4.misc.BasicFullHttpResponse;
import com.linkedin.alpini.netty4.misc.BasicHttpResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.EncoderException;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpContentEncoder.Result;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.AsciiString;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = { "unit", "unit-leak", "leak" }, singleThreaded = true)
public final class TestBasicHttpContentEncoder extends AbstractLeakDetect {
  private static final class TestEncoder extends BasicHttpContentEncoder {
    @Override
    protected Result beginEncode(HttpMessage headers, String acceptEncoding) {
      return new Result("test", new EmbeddedChannel(new MessageToByteEncoder<ByteBuf>() {
        @Override
        protected void encode(ChannelHandlerContext ctx, ByteBuf in, ByteBuf out) throws Exception {
          ByteBufUtil.writeAscii(out, String.valueOf(in.readableBytes()));
          in.skipBytes(in.readableBytes());
        }
      }));
    }
  }

  @Test
  public void testSplitContent() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new TestEncoder());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    HttpRequest req =
        new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/", Time.currentTimeMillis(), Time.nanoTime());
    ch.writeInbound(req);

    ch.writeOutbound(new BasicHttpResponse(req, HttpResponseStatus.OK));
    ch.writeOutbound(new DefaultHttpContent(copyOf(new byte[3])));
    ch.writeOutbound(new DefaultHttpContent(copyOf(new byte[2])));
    ch.writeOutbound(new DefaultLastHttpContent(copyOf(new byte[1])));

    assertEncodedResponse(ch);

    HttpContent chunk;
    chunk = ch.readOutbound();
    Assert.assertEquals(chunk.content().toString(StandardCharsets.US_ASCII), "3");
    chunk.release();

    chunk = ch.readOutbound();
    Assert.assertEquals(chunk.content().toString(StandardCharsets.US_ASCII), "2");
    chunk.release();

    chunk = ch.readOutbound();
    Assert.assertEquals(chunk.content().toString(StandardCharsets.US_ASCII), "1");
    chunk.release();

    chunk = ch.readOutbound();
    Assert.assertFalse(chunk.content().isReadable());
    Assert.assertTrue(chunk instanceof LastHttpContent);
    chunk.release();

    Assert.assertNull(ch.readOutbound());
  }

  @Test
  public void testChunkedContent() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new TestEncoder());
    HttpRequest req =
        new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/", Time.currentTimeMillis(), Time.nanoTime());
    ch.writeInbound(req);

    HttpResponse res = new BasicHttpResponse(req, HttpResponseStatus.OK);
    res.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
    ch.writeOutbound(res);

    assertEncodedResponse(ch);

    ch.writeOutbound(new DefaultHttpContent(copyOf(new byte[3])));
    ch.writeOutbound(new DefaultHttpContent(copyOf(new byte[2])));
    ch.writeOutbound(new DefaultLastHttpContent(copyOf(new byte[1])));

    HttpContent chunk;
    chunk = ch.readOutbound();
    Assert.assertEquals(chunk.content().toString(StandardCharsets.US_ASCII), "3");
    chunk.release();

    chunk = ch.readOutbound();
    Assert.assertEquals(chunk.content().toString(StandardCharsets.US_ASCII), "2");
    chunk.release();

    chunk = ch.readOutbound();
    Assert.assertEquals(chunk.content().toString(StandardCharsets.US_ASCII), "1");
    Assert.assertTrue(chunk instanceof HttpContent);
    chunk.release();

    chunk = ch.readOutbound();
    Assert.assertFalse(chunk.content().isReadable());
    Assert.assertTrue(chunk instanceof LastHttpContent);
    chunk.release();

    Assert.assertNull(ch.readOutbound());
  }

  @Test
  public void testChunkedContentWithTrailingHeader() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new TestEncoder());
    HttpRequest req =
        new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/", Time.currentTimeMillis(), Time.nanoTime());
    ch.writeInbound(req);

    HttpResponse res = new BasicHttpResponse(req, HttpResponseStatus.OK);
    res.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
    ch.writeOutbound(res);

    assertEncodedResponse(ch);

    ch.writeOutbound(new DefaultHttpContent(copyOf(new byte[3])));
    ch.writeOutbound(new DefaultHttpContent(copyOf(new byte[2])));
    LastHttpContent content = new DefaultLastHttpContent(copyOf(new byte[1]));
    content.trailingHeaders().set(AsciiString.of("X-Test"), AsciiString.of("Netty"));
    ch.writeOutbound(content);

    HttpContent chunk;
    chunk = ch.readOutbound();
    Assert.assertEquals(chunk.content().toString(StandardCharsets.US_ASCII), "3");
    chunk.release();

    chunk = ch.readOutbound();
    Assert.assertEquals(chunk.content().toString(StandardCharsets.US_ASCII), "2");
    chunk.release();

    chunk = ch.readOutbound();
    Assert.assertEquals(chunk.content().toString(StandardCharsets.US_ASCII), "1");
    Assert.assertTrue(chunk instanceof HttpContent);
    chunk.release();

    chunk = ch.readOutbound();
    Assert.assertFalse(chunk.content().isReadable());
    Assert.assertTrue(chunk instanceof LastHttpContent);
    Assert.assertEquals(((LastHttpContent) chunk).trailingHeaders().get(AsciiString.of("X-Test")), "Netty");
    chunk.release();

    Assert.assertNull(ch.readOutbound());
  }

  @Test
  public void testFullContentWithContentLength() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new TestEncoder());
    HttpRequest req =
        new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/", Time.currentTimeMillis(), Time.nanoTime());
    ch.writeInbound(req);

    FullHttpResponse fullRes = new BasicFullHttpResponse(req, HttpResponseStatus.OK, copyOf(new byte[42]));
    fullRes.headers().set(HttpHeaderNames.CONTENT_LENGTH, 42);
    ch.writeOutbound(fullRes);

    HttpResponse res = ch.readOutbound();
    Assert.assertFalse(res instanceof HttpContent);
    Assert.assertNull(res.headers().get(HttpHeaderNames.TRANSFER_ENCODING));
    Assert.assertEquals(res.headers().get(HttpHeaderNames.CONTENT_LENGTH), "2");
    Assert.assertEquals(res.headers().get(HttpHeaderNames.CONTENT_ENCODING), "test");

    HttpContent c = ch.readOutbound();
    Assert.assertEquals(c.content().readableBytes(), 2);
    Assert.assertEquals(c.content().toString(StandardCharsets.US_ASCII), "42");
    c.release();

    LastHttpContent last = ch.readOutbound();
    Assert.assertEquals(last.content().readableBytes(), 0);
    last.release();

    Assert.assertNull(ch.readOutbound());
  }

  @Test
  public void testFullContent() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new TestEncoder());
    HttpRequest req =
        new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/", Time.currentTimeMillis(), Time.nanoTime());
    ch.writeInbound(req);

    FullHttpResponse res = new BasicFullHttpResponse(req, HttpResponseStatus.OK, copyOf(new byte[42]));
    ch.writeOutbound(res);

    assertEncodedResponse(ch);
    HttpContent c = ch.readOutbound();
    Assert.assertEquals(c.content().readableBytes(), 2);
    Assert.assertEquals(c.content().toString(StandardCharsets.US_ASCII), "42");
    c.release();

    LastHttpContent last = ch.readOutbound();
    Assert.assertEquals(last.content().readableBytes(), 0);
    last.release();

    Assert.assertNull(ch.readOutbound());
  }

  /**
   * If the length of the content is unknown, {@link BasicHttpContentEncoder} should not skip encoding the content
   * even if the actual length is turned out to be 0.
   */
  @Test
  public void testEmptySplitContent() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new TestEncoder());
    HttpRequest req =
        new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/", Time.currentTimeMillis(), Time.nanoTime());
    ch.writeInbound(req);

    ch.writeOutbound(new BasicHttpResponse(req, HttpResponseStatus.OK));
    assertEncodedResponse(ch);

    ch.writeOutbound(LastHttpContent.EMPTY_LAST_CONTENT);
    HttpContent chunk = ch.readOutbound();
    Assert.assertEquals(chunk.content().toString(StandardCharsets.US_ASCII), "0");
    Assert.assertTrue(chunk instanceof HttpContent);
    chunk.release();

    chunk = ch.readOutbound();
    Assert.assertFalse(chunk.content().isReadable());
    Assert.assertTrue(chunk instanceof LastHttpContent);
    chunk.release();

    Assert.assertNull(ch.readOutbound());
  }

  /**
   * If the length of the content is 0 for sure, {@link BasicHttpContentEncoder} should skip encoding.
   */
  @Test
  public void testEmptyFullContent() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new TestEncoder());
    HttpRequest req =
        new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/", Time.currentTimeMillis(), Time.nanoTime());
    ch.writeInbound(req);

    FullHttpResponse res = new BasicFullHttpResponse(req, HttpResponseStatus.OK, Unpooled.EMPTY_BUFFER);
    ch.writeOutbound(res);

    Object o = ch.readOutbound();
    Assert.assertTrue(o instanceof FullHttpResponse);

    res = (FullHttpResponse) o;
    Assert.assertNull(res.headers().get(HttpHeaderNames.TRANSFER_ENCODING));

    // Content encoding shouldn't be modified.
    Assert.assertNull(res.headers().get(HttpHeaderNames.CONTENT_ENCODING));
    Assert.assertEquals(res.content().readableBytes(), 0);
    Assert.assertEquals(res.content().toString(StandardCharsets.US_ASCII), "");
    res.release();

    Assert.assertNull(ch.readOutbound());
  }

  @Test
  public void testEmptyFullContentWithTrailer() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new TestEncoder());
    HttpRequest req =
        new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/", Time.currentTimeMillis(), Time.nanoTime());
    ch.writeInbound(req);

    FullHttpResponse res = new BasicFullHttpResponse(req, HttpResponseStatus.OK, Unpooled.EMPTY_BUFFER);
    res.trailingHeaders().set(AsciiString.of("X-Test"), AsciiString.of("Netty"));
    ch.writeOutbound(res);

    Object o = ch.readOutbound();
    Assert.assertTrue(o instanceof FullHttpResponse);

    res = (FullHttpResponse) o;
    Assert.assertNull(res.headers().get(HttpHeaderNames.TRANSFER_ENCODING));

    // Content encoding shouldn't be modified.
    Assert.assertNull(res.headers().get(HttpHeaderNames.CONTENT_ENCODING));
    Assert.assertEquals(res.content().readableBytes(), 0);
    Assert.assertEquals(res.content().toString(StandardCharsets.US_ASCII), "");
    Assert.assertEquals(res.trailingHeaders().get(AsciiString.of("X-Test")), "Netty");
    Assert.assertNull(ch.readOutbound());
  }

  @Test
  public void testEmptyHeadResponse() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new TestEncoder());
    HttpRequest req =
        new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.HEAD, "/", Time.currentTimeMillis(), Time.nanoTime());
    ch.writeInbound(req);

    HttpResponse res = new BasicHttpResponse(req, HttpResponseStatus.OK);
    res.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
    ch.writeOutbound(res);
    ch.writeOutbound(LastHttpContent.EMPTY_LAST_CONTENT);

    assertEmptyResponse(ch);
  }

  @Test
  public void testHttp304Response() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new TestEncoder());
    HttpRequest req =
        new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/", Time.currentTimeMillis(), Time.nanoTime());
    req.headers().set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.GZIP);
    ch.writeInbound(req);

    HttpResponse res = new BasicHttpResponse(req, HttpResponseStatus.NOT_MODIFIED);
    res.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
    ch.writeOutbound(res);
    ch.writeOutbound(LastHttpContent.EMPTY_LAST_CONTENT);

    assertEmptyResponse(ch);
  }

  @Test
  public void testConnect200Response() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new TestEncoder());
    HttpRequest req = new BasicFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.CONNECT,
        "google.com:80",
        Time.currentTimeMillis(),
        Time.nanoTime());
    ch.writeInbound(req);

    HttpResponse res = new BasicHttpResponse(req, HttpResponseStatus.OK);
    res.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
    ch.writeOutbound(res);
    ch.writeOutbound(LastHttpContent.EMPTY_LAST_CONTENT);

    assertEmptyResponse(ch);
  }

  @Test
  public void testConnectFailureResponse() throws Exception {
    String content = "Not allowed by configuration";

    EmbeddedChannel ch = new EmbeddedChannel(new TestEncoder());
    HttpRequest req = new BasicFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.CONNECT,
        "google.com:80",
        Time.currentTimeMillis(),
        Time.nanoTime());
    ch.writeInbound(req);

    HttpResponse res = new BasicHttpResponse(req, HttpResponseStatus.METHOD_NOT_ALLOWED);
    res.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
    ch.writeOutbound(res);
    ch.writeOutbound(new DefaultHttpContent(encodeString(content, StandardCharsets.UTF_8)));
    ch.writeOutbound(LastHttpContent.EMPTY_LAST_CONTENT);

    assertEncodedResponse(ch);
    Object o = ch.readOutbound();
    Assert.assertTrue(o instanceof HttpContent);
    HttpContent chunk = (HttpContent) o;
    Assert.assertEquals(chunk.content().toString(StandardCharsets.US_ASCII), "28");
    chunk.release();

    chunk = ch.readOutbound();
    Assert.assertTrue(chunk.content().isReadable());
    Assert.assertEquals(chunk.content().toString(StandardCharsets.US_ASCII), "0");
    chunk.release();

    chunk = ch.readOutbound();
    Assert.assertTrue(chunk instanceof LastHttpContent);
    chunk.release();
    Assert.assertNull(ch.readOutbound());
  }

  @Test
  public void testHttp1_0() throws Exception {
    EmbeddedChannel ch = new EmbeddedChannel(new TestEncoder());
    FullHttpRequest req =
        new BasicFullHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, "/", Time.currentTimeMillis(), Time.nanoTime());
    Assert.assertTrue(ch.writeInbound(req));

    HttpResponse res = new BasicHttpResponse(req, HttpResponseStatus.OK);
    res.headers().set(HttpHeaderNames.CONTENT_LENGTH, HttpHeaderValues.ZERO);
    Assert.assertTrue(ch.writeOutbound(res));
    Assert.assertTrue(ch.writeOutbound(LastHttpContent.EMPTY_LAST_CONTENT));
    Assert.assertTrue(ch.finish());

    FullHttpRequest request = ch.readInbound();
    Assert.assertTrue(request.release());
    Assert.assertNull(ch.readInbound());

    HttpResponse response = ch.readOutbound();
    Assert.assertSame(response, res);

    LastHttpContent content = ch.readOutbound();
    Assert.assertSame(content, LastHttpContent.EMPTY_LAST_CONTENT);
    content.release();
    Assert.assertNull(ch.readOutbound());
  }

  @Test
  public void testCleanupThrows() {
    BasicHttpContentEncoder encoder = new BasicHttpContentEncoder() {
      @Override
      protected Result beginEncode(HttpMessage headers, String acceptEncoding) throws Exception {
        return new Result("myencoding", new EmbeddedChannel(new ChannelInboundHandlerAdapter() {
          @Override
          public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            ctx.fireExceptionCaught(new EncoderException());
            ctx.fireChannelInactive();
          }
        }));
      }
    };

    final AtomicBoolean channelInactiveCalled = new AtomicBoolean();
    EmbeddedChannel channel = new EmbeddedChannel(encoder, new ChannelInboundHandlerAdapter() {
      @Override
      public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Assert.assertTrue(channelInactiveCalled.compareAndSet(false, true));
        super.channelInactive(ctx);
      }
    });
    HttpRequest req =
        new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/", Time.currentTimeMillis(), Time.nanoTime());
    Assert.assertTrue(channel.writeInbound(req));
    Assert.assertTrue(channel.writeOutbound(new BasicHttpResponse(req, HttpResponseStatus.OK)));
    HttpContent content = new DefaultHttpContent(POOLED_ALLOCATOR.buffer().writeZero(10));
    Assert.assertTrue(channel.writeOutbound(content));
    Assert.assertEquals(content.refCnt(), 1);
    try {
      channel.finishAndReleaseAll();
      Assert.fail();
    } catch (CodecException expected) {
      // expected
    }
    Assert.assertTrue(channelInactiveCalled.get());
    Assert.assertEquals(content.refCnt(), 0);
  }

  private static void assertEmptyResponse(EmbeddedChannel ch) {
    Object o = ch.readOutbound();
    Assert.assertTrue(o instanceof HttpResponse);
    ;

    HttpResponse res = (HttpResponse) o;
    Assert.assertFalse(res instanceof HttpContent);
    Assert.assertEquals(res.headers().get(HttpHeaderNames.TRANSFER_ENCODING), "chunked");
    Assert.assertNull(res.headers().get(HttpHeaderNames.CONTENT_LENGTH));

    HttpContent chunk = ch.readOutbound();
    Assert.assertTrue(chunk instanceof LastHttpContent);
    chunk.release();
    Assert.assertNull(ch.readOutbound());
  }

  private static void assertEncodedResponse(EmbeddedChannel ch) {
    Object o = ch.readOutbound();
    Assert.assertTrue(o instanceof HttpResponse);

    HttpResponse res = (HttpResponse) o;
    Assert.assertFalse(res instanceof HttpContent);
    Assert.assertEquals(res.headers().get(HttpHeaderNames.TRANSFER_ENCODING), "chunked");
    Assert.assertNull(res.headers().get(HttpHeaderNames.CONTENT_LENGTH));
    Assert.assertEquals(res.headers().get(HttpHeaderNames.CONTENT_ENCODING), "test");
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
