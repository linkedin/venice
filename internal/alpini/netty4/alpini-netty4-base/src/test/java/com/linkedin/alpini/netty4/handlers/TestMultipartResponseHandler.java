package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.netty4.misc.BasicFullHttpMultiPart;
import com.linkedin.alpini.netty4.misc.FullHttpMultiPart;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import java.nio.charset.StandardCharsets;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 5/8/17.
 */
@Test(groups = { "unit", "unit-leak", "leak" }, singleThreaded = true)
public final class TestMultipartResponseHandler extends AbstractLeakDetect {
  @Test
  public void testBasicResponse() {

    EmbeddedChannel channel = new EmbeddedChannel(new HttpResponseEncoder(), new MultipartResponseHandler());
    channel.config().setAllocator(POOLED_ALLOCATOR);

    HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

    HttpUtil.setContentLength(responseHeader, 0);

    channel.writeOutbound(responseHeader, LastHttpContent.EMPTY_LAST_CONTENT);

    ByteBuf output = channel.readOutbound();

    Assert.assertEquals(
        output.toString(StandardCharsets.US_ASCII),
        "HTTP/1.1 200 OK\r\n" + "content-length: 0\r\n" + "\r\n");
    ReferenceCountUtil.release(output);
  }

  @Test
  public void testBasicResponseWithContent() {

    EmbeddedChannel channel = new EmbeddedChannel(new HttpResponseEncoder(), new MultipartResponseHandler());
    channel.config().setAllocator(POOLED_ALLOCATOR);

    HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    HttpContent responseContent = new DefaultHttpContent(encodeString("Hello World!\n", StandardCharsets.US_ASCII));

    HttpUtil.setContentLength(responseHeader, responseContent.content().readableBytes());

    channel.writeOutbound(responseHeader, responseContent, LastHttpContent.EMPTY_LAST_CONTENT);

    ByteBuf output = channel.readOutbound();

    Assert.assertEquals(
        output.toString(StandardCharsets.US_ASCII),
        "HTTP/1.1 200 OK\r\n" + "content-length: 13\r\n" + "\r\n");
    output.release();

    ByteBuf content = channel.readOutbound();
    Assert.assertEquals(content.toString(StandardCharsets.US_ASCII), "Hello World!\n");
    content.release();
  }

  @Test
  public void testMultipartResponse() {

    EmbeddedChannel channel = new EmbeddedChannel(new HttpResponseEncoder(), new MultipartResponseHandler());
    channel.config().setAllocator(POOLED_ALLOCATOR);

    HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

    responseHeader.headers().set(HttpHeaderNames.CONTENT_TYPE, "multipart/mixed; boundary=--1234567890987654321");

    FullHttpMultiPart part1 = new BasicFullHttpMultiPart(encodeString("Hello World!\n", StandardCharsets.US_ASCII));
    part1.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");

    channel.writeOutbound(responseHeader, part1, LastHttpContent.EMPTY_LAST_CONTENT);

    ByteBuf output = channel.readOutbound();

    Assert.assertEquals(
        output.toString(StandardCharsets.US_ASCII),
        "HTTP/1.1 200 OK\r\n" + "content-type: multipart/mixed; boundary=--1234567890987654321\r\n"
            + "transfer-encoding: chunked\r\n" + "\r\n");
    output.release();

    output = channel.readOutbound();
    Assert.assertEquals(output.toString(StandardCharsets.US_ASCII), "3f\r\n");
    ReferenceCountUtil.release(output);

    output = channel.readOutbound();
    Assert.assertEquals(
        output.toString(StandardCharsets.US_ASCII),
        "content-type: text/plain\r\n" + "content-length: 13\r\n" + "\r\n" + "Hello World!\n" + "\r\n");
    ReferenceCountUtil.release(output);

    output = channel.readOutbound();
    Assert.assertEquals(output.toString(StandardCharsets.US_ASCII), "\r\n");
    ReferenceCountUtil.release(output);

    output = channel.readOutbound();
    Assert.assertEquals(output.toString(StandardCharsets.US_ASCII), "1b\r\n");
    ReferenceCountUtil.release(output);

    output = channel.readOutbound();
    Assert.assertEquals(output.toString(StandardCharsets.US_ASCII), "----1234567890987654321--\r\n");
    ReferenceCountUtil.release(output);

    output = channel.readOutbound();
    Assert.assertEquals(output.toString(StandardCharsets.US_ASCII), "\r\n");
    ReferenceCountUtil.release(output);

    output = channel.readOutbound();
    Assert.assertEquals(output.toString(StandardCharsets.US_ASCII), "0\r\n\r\n");
    ReferenceCountUtil.release(output);

    Assert.assertNull(channel.readOutbound());
  }

  @Test
  public void testMultipartResponse2() {

    EmbeddedChannel channel = new EmbeddedChannel(new HttpResponseEncoder(), new MultipartResponseHandler());
    channel.config().setAllocator(POOLED_ALLOCATOR);

    HttpResponse responseHeader = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

    responseHeader.headers().set(HttpHeaderNames.CONTENT_TYPE, "multipart/mixed; boundary=--1234567890987654321");

    FullHttpMultiPart part1 = new BasicFullHttpMultiPart(encodeString("Hello World!\n", StandardCharsets.US_ASCII));
    part1.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");

    FullHttpMultiPart part2 = new BasicFullHttpMultiPart(encodeString("This is a test\n", StandardCharsets.US_ASCII));
    part1.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");

    channel.writeOutbound(responseHeader, part1, part2, LastHttpContent.EMPTY_LAST_CONTENT);

    ByteBuf output = channel.readOutbound();

    Assert.assertEquals(
        output.toString(StandardCharsets.US_ASCII),
        "HTTP/1.1 200 OK\r\n" + "content-type: multipart/mixed; boundary=--1234567890987654321\r\n"
            + "transfer-encoding: chunked\r\n" + "\r\n");
    output.release();

    ByteBuf content = channel.readOutbound();
    Assert.assertEquals(content.toString(StandardCharsets.US_ASCII), "3f\r\n");
    ReferenceCountUtil.release(content);

    content = channel.readOutbound();
    Assert.assertEquals(
        content.toString(StandardCharsets.US_ASCII),
        "content-type: text/plain\r\n" + "content-length: 13\r\n" + "\r\n" + "Hello World!\n" + "\r\n");
    ReferenceCountUtil.release(content);

    content = channel.readOutbound();
    Assert.assertEquals(content.toString(StandardCharsets.US_ASCII), "\r\n");
    ReferenceCountUtil.release(content);

    content = channel.readOutbound();
    Assert.assertEquals(content.toString(StandardCharsets.US_ASCII), "19\r\n");
    ReferenceCountUtil.release(content);

    content = channel.readOutbound();
    Assert.assertEquals(content.toString(StandardCharsets.US_ASCII), "----1234567890987654321\r\n");
    ReferenceCountUtil.release(content);

    content = channel.readOutbound();
    Assert.assertEquals(content.toString(StandardCharsets.US_ASCII), "\r\n");
    ReferenceCountUtil.release(content);

    content = channel.readOutbound();
    Assert.assertEquals(content.toString(StandardCharsets.US_ASCII), "49\r\n");
    ReferenceCountUtil.release(content);

    content = channel.readOutbound();
    Assert.assertEquals(
        content.toString(StandardCharsets.US_ASCII),
        "content-type: application/binary\r\n" + "content-length: 15\r\n" + "\r\n" + "This is a test\n" + "\r\n");
    ReferenceCountUtil.release(content);

    content = channel.readOutbound();
    Assert.assertEquals(content.toString(StandardCharsets.US_ASCII), "\r\n");
    ReferenceCountUtil.release(content);

    content = channel.readOutbound();
    Assert.assertEquals(content.toString(StandardCharsets.US_ASCII), "1b\r\n");
    ReferenceCountUtil.release(content);

    content = channel.readOutbound();
    Assert.assertEquals(content.toString(StandardCharsets.US_ASCII), "----1234567890987654321--\r\n");
    ReferenceCountUtil.release(content);

    content = channel.readOutbound();
    Assert.assertEquals(content.toString(StandardCharsets.US_ASCII), "\r\n");
    ReferenceCountUtil.release(content);

    content = channel.readOutbound();
    Assert.assertEquals(content.toString(StandardCharsets.US_ASCII), "0\r\n\r\n");
    ReferenceCountUtil.release(content);

    Assert.assertNull(channel.readOutbound());
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
