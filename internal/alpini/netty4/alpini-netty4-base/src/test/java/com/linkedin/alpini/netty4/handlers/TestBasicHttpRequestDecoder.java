package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.netty4.misc.BasicHttpRequestDecoder;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestBasicHttpRequestDecoder {
  private static final byte[] CONTENT_CRLF_DELIMITERS = createContent("\r\n");
  private static final byte[] CONTENT_LF_DELIMITERS = createContent("\n");
  private static final byte[] CONTENT_MIXED_DELIMITERS = createContent("\r\n", "\n");
  private static final int CONTENT_LENGTH = 8;

  private static byte[] createContent(String... lineDelimiters) {
    String lineDelimiter;
    String lineDelimiter2;
    if (lineDelimiters.length == 2) {
      lineDelimiter = lineDelimiters[0];
      lineDelimiter2 = lineDelimiters[1];
    } else {
      lineDelimiter = lineDelimiters[0];
      lineDelimiter2 = lineDelimiters[0];
    }
    return ("GET /some/path?foo=bar&wibble=eek HTTP/1.1" + "\r\n" + "Upgrade: WebSocket" + lineDelimiter2
        + "Connection: Upgrade" + lineDelimiter + "Host: localhost" + lineDelimiter2 + "Origin: http://localhost:8080"
        + lineDelimiter + "Sec-WebSocket-Key1: 10  28 8V7 8 48     0" + lineDelimiter2
        + "Sec-WebSocket-Key2: 8 Xt754O3Q3QW 0   _60" + lineDelimiter + "Content-Length: " + CONTENT_LENGTH
        + lineDelimiter2 + "\r\n" + "12345678").getBytes(CharsetUtil.US_ASCII);
  }

  @Test
  public void testDecodeWholeRequestAtOnceCRLFDelimiters() {
    testDecodeWholeRequestAtOnce(CONTENT_CRLF_DELIMITERS);
  }

  @Test
  public void testDecodeWholeRequestAtOnceLFDelimiters() {
    testDecodeWholeRequestAtOnce(CONTENT_LF_DELIMITERS);
  }

  @Test
  public void testDecodeWholeRequestAtOnceMixedDelimiters() {
    testDecodeWholeRequestAtOnce(CONTENT_MIXED_DELIMITERS);
  }

  private static void testDecodeWholeRequestAtOnce(byte[] content) {
    EmbeddedChannel channel = new EmbeddedChannel(new BasicHttpRequestDecoder());
    Assert.assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(content)));
    HttpRequest req = channel.readInbound();
    Assert.assertNotNull(req);
    checkHeaders(req.headers());
    LastHttpContent c = channel.readInbound();
    Assert.assertEquals(CONTENT_LENGTH, c.content().readableBytes());
    Assert.assertEquals(
        Unpooled.wrappedBuffer(content, content.length - CONTENT_LENGTH, CONTENT_LENGTH),
        c.content().readSlice(CONTENT_LENGTH));
    c.release();

    Assert.assertFalse(channel.finish());
    Assert.assertNull(channel.readInbound());
  }

  private static void checkHeaders(HttpHeaders headers) {
    Assert.assertEquals(7, headers.names().size());
    checkHeader(headers, "Upgrade", "WebSocket");
    checkHeader(headers, "Connection", "Upgrade");
    checkHeader(headers, "Host", "localhost");
    checkHeader(headers, "Origin", "http://localhost:8080");
    checkHeader(headers, "Sec-WebSocket-Key1", "10  28 8V7 8 48     0");
    checkHeader(headers, "Sec-WebSocket-Key2", "8 Xt754O3Q3QW 0   _60");
    checkHeader(headers, "Content-Length", String.valueOf(CONTENT_LENGTH));
  }

  private static void checkHeader(HttpHeaders headers, String name, String value) {
    List<String> header1 = headers.getAll(name);
    Assert.assertEquals(1, header1.size());
    Assert.assertEquals(value, header1.get(0));
  }

  @Test
  public void testDecodeWholeRequestInMultipleStepsCRLFDelimiters() {
    testDecodeWholeRequestInMultipleSteps(CONTENT_CRLF_DELIMITERS);
  }

  @Test
  public void testDecodeWholeRequestInMultipleStepsLFDelimiters() {
    testDecodeWholeRequestInMultipleSteps(CONTENT_LF_DELIMITERS);
  }

  @Test
  public void testDecodeWholeRequestInMultipleStepsMixedDelimiters() {
    testDecodeWholeRequestInMultipleSteps(CONTENT_MIXED_DELIMITERS);
  }

  private static void testDecodeWholeRequestInMultipleSteps(byte[] content) {
    for (int i = 1; i < content.length; i++) {
      testDecodeWholeRequestInMultipleSteps(content, i);
    }
  }

  private static void testDecodeWholeRequestInMultipleSteps(byte[] content, int fragmentSize) {
    EmbeddedChannel channel = new EmbeddedChannel(new BasicHttpRequestDecoder());
    int headerLength = content.length - CONTENT_LENGTH;

    // split up the header
    for (int a = 0; a < headerLength;) {
      int amount = fragmentSize;
      if (a + amount > headerLength) {
        amount = headerLength - a;
      }

      // if header is done it should produce a HttpRequest
      channel.writeInbound(Unpooled.wrappedBuffer(content, a, amount));
      a += amount;
    }

    for (int i = CONTENT_LENGTH; i > 0; i--) {
      // Should produce HttpContent
      channel.writeInbound(Unpooled.wrappedBuffer(content, content.length - i, 1));
    }

    HttpRequest req = channel.readInbound();
    Assert.assertNotNull(req);
    checkHeaders(req.headers());

    for (int i = CONTENT_LENGTH; i > 1; i--) {
      HttpContent c = channel.readInbound();
      Assert.assertEquals(1, c.content().readableBytes());
      Assert.assertEquals(content[content.length - i], c.content().readByte());
      c.release();
    }

    LastHttpContent c = channel.readInbound();
    Assert.assertEquals(1, c.content().readableBytes());
    Assert.assertEquals(content[content.length - 1], c.content().readByte());
    c.release();

    Assert.assertFalse(channel.finish());
    Assert.assertNull(channel.readInbound());
  }

  @Test
  public void testEmptyHeaderValue() {
    EmbeddedChannel channel = new EmbeddedChannel(new BasicHttpRequestDecoder());
    String crlf = "\r\n";
    String request = "GET /some/path HTTP/1.1" + crlf + "Host: localhost" + crlf + "EmptyHeader:" + crlf + crlf;
    channel.writeInbound(Unpooled.wrappedBuffer(request.getBytes(CharsetUtil.US_ASCII)));
    HttpRequest req = channel.readInbound();
    Assert.assertEquals("", req.headers().get("EmptyHeader"));
  }

  @Test
  public void test100Continue() {
    BasicHttpRequestDecoder decoder = new BasicHttpRequestDecoder();
    EmbeddedChannel channel = new EmbeddedChannel(decoder);
    String oversized = "PUT /file HTTP/1.1\r\n" + "Expect: 100-continue\r\n" + "Content-Length: 1048576000\r\n\r\n";

    channel.writeInbound(Unpooled.copiedBuffer(oversized, CharsetUtil.US_ASCII));
    Assert.assertTrue(channel.readInbound() instanceof HttpRequest);

    // At this point, we assume that we sent '413 Entity Too Large' to the peer without closing the connection
    // so that the client can try again.
    decoder.reset();

    String query = "GET /max-file-size HTTP/1.1\r\n\r\n";
    channel.writeInbound(Unpooled.copiedBuffer(query, CharsetUtil.US_ASCII));
    Assert.assertTrue(channel.readInbound() instanceof HttpRequest);
    Assert.assertTrue(channel.readInbound() instanceof LastHttpContent);

    Assert.assertFalse(channel.finish());
  }

  @Test
  public void test100ContinueWithBadClient() {
    BasicHttpRequestDecoder decoder = new BasicHttpRequestDecoder();
    EmbeddedChannel channel = new EmbeddedChannel(decoder);
    String oversized = "PUT /file HTTP/1.1\r\n" + "Expect: 100-continue\r\n" + "Content-Length: 1048576000\r\n\r\n"
        + "WAY_TOO_LARGE_DATA_BEGINS";

    channel.writeInbound(Unpooled.copiedBuffer(oversized, CharsetUtil.US_ASCII));
    Assert.assertTrue(channel.readInbound() instanceof HttpRequest);

    HttpContent prematureData = channel.readInbound();
    prematureData.release();

    Assert.assertNull(channel.readInbound());

    // At this point, we assume that we sent '413 Entity Too Large' to the peer without closing the connection
    // so that the client can try again.
    decoder.reset();

    String query = "GET /max-file-size HTTP/1.1\r\n\r\n";
    channel.writeInbound(Unpooled.copiedBuffer(query, CharsetUtil.US_ASCII));
    Assert.assertTrue(channel.readInbound() instanceof HttpRequest);
    Assert.assertTrue(channel.readInbound() instanceof LastHttpContent);

    Assert.assertFalse(channel.finish());
  }
}
