package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.netty4.misc.BasicFullHttpMultiPart;
import com.linkedin.alpini.netty4.misc.FullHttpMultiPart;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.AsciiString;
import io.netty.util.ReferenceCountUtil;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.mail.MessagingException;
import javax.mail.internet.InternetHeaders;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;
import org.apache.logging.log4j.LogManager;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 3/23/17.
 */
@Test(groups = { "unit", "unit-leak", "leak" }, singleThreaded = true)
public final class TestHttpMultiPartContentCodec extends AbstractLeakDetect {
  private EmbeddedChannel makeTestChannel() {
    int maxInitialLineLength = 1024;
    int maxHeaderSize = 8192;
    int maxChunkSize = 16384;
    int chunkSize = 8000;
    int maxPartContentLength = 10240;
    boolean validateHeaders = false;

    return new EmbeddedChannel(
        new LoggingHandler("Egress", LogLevel.INFO),
        new BasicHttpServerCodec(maxInitialLineLength, maxHeaderSize, maxChunkSize, validateHeaders),
        new HttpByteBufContentChunker(maxChunkSize, chunkSize),
        new HttpMultiPartContentCodec(maxPartContentLength),
        new LoggingHandler(TestHttpMultiPartContentCodec.class, LogLevel.INFO));
  }

  private EmbeddedChannel makeTestAggregatedChannel() {
    int maxInitialLineLength = 1024;
    int maxHeaderSize = 8192;
    int maxChunkSize = 16384;
    int chunkSize = 8000;
    int maxPartContentLength = 10240;
    boolean validateHeaders = false;

    return new EmbeddedChannel(
        new LoggingHandler("Egress", LogLevel.INFO),
        new BasicHttpServerCodec(maxInitialLineLength, maxHeaderSize, maxChunkSize, validateHeaders),
        new HttpByteBufContentChunker(maxChunkSize, chunkSize),
        new HttpMultiPartContentCodec(maxPartContentLength),
        new BasicHttpNonMultiPartAggregator(maxPartContentLength),
        new LoggingHandler(TestHttpMultiPartContentCodec.class, LogLevel.INFO));
  }

  private EmbeddedChannel makeTestHttpAggregatedChannel() {
    int maxInitialLineLength = 1024;
    int maxHeaderSize = 8192;
    int maxChunkSize = 16384;
    int chunkSize = 8000;
    int maxPartContentLength = 10240;
    boolean validateHeaders = false;

    return new EmbeddedChannel(
        new Log4J2LoggingHandler("Egress", LogLevel.INFO),
        new BasicHttpServerCodec(maxInitialLineLength, maxHeaderSize, maxChunkSize, validateHeaders),
        new HttpObjectAggregator(maxPartContentLength),
        new HttpByteBufContentChunker(maxChunkSize, chunkSize),
        new HttpMultiPartContentCodec(maxPartContentLength),
        new Log4J2LoggingHandler(HttpMultiPartContentCodec.class, LogLevel.INFO),
        new BasicHttpNonMultiPartAggregator(maxPartContentLength),
        new Log4J2LoggingHandler(TestHttpMultiPartContentCodec.class, LogLevel.INFO));
  }

  static void assertRelease(Object o) {
    Assert.assertTrue(ReferenceCountUtil.release(o));
  }

  @Test
  public void basicTestMultiPart() {
    EmbeddedChannel channel = makeTestChannel();

    Assert.assertTrue(
        channel.writeInbound(
            encodeString(
                "GET /Foo/bar/12345 HTTP/1.1\r\n" + "Host: foo-host.example.org\r\n"
                    + "Content-type: multipart/mixed; boundary=Part_123456\r\n" + "Content-length: "
                    + (0xf5 - 0x85 + 33) + "\r\n" + "\r\n" + "Ignore this fuzz\r\n" + "--Part_123456\r\n"
                    + "Content-type: text/plain\r\n" + "\r\n" + "Hello world\r\n" + "--Part_123456\r\n"
                    + "Content-type: text/plain\r\n" + "\r\n" + "Part two!\r\n" + "--Part_123456--\r\n" + "\r\n",
                StandardCharsets.US_ASCII)));

    HttpObject object = channel.readInbound();
    Assert.assertTrue(object instanceof HttpRequest);
    Assert.assertFalse(object instanceof FullHttpRequest);

    object = channel.readInbound();
    Assert.assertTrue(object instanceof FullHttpMultiPart);
    Assert.assertEquals(((FullHttpMultiPart) object).content().toString(StandardCharsets.US_ASCII), "Hello world\r\n");
    assertRelease(object);

    object = channel.readInbound();
    Assert.assertTrue(object instanceof FullHttpMultiPart);
    Assert.assertEquals(((FullHttpMultiPart) object).content().toString(StandardCharsets.US_ASCII), "Part two!\r\n");
    assertRelease(object);

    object = channel.readInbound();
    Assert.assertTrue(object instanceof LastHttpContent);
    ReferenceCountUtil.release(object);

    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, false);
    response.headers()
        .set(
            HttpHeaderNames.CONTENT_TYPE,
            "multipart/mixed; boundary=\"--=Part_8928ea38-13cb-5d79-66ac-553475448f79\"");

    FullHttpMultiPart part1 = new BasicFullHttpMultiPart(
        ByteBufUtil.encodeString(POOLED_ALLOCATOR, CharBuffer.wrap("Test Part 1"), StandardCharsets.US_ASCII));
    FullHttpMultiPart part2 = new BasicFullHttpMultiPart(
        ByteBufUtil.encodeString(POOLED_ALLOCATOR, CharBuffer.wrap("Test Part 2"), StandardCharsets.US_ASCII));
    FullHttpMultiPart part3 = new BasicFullHttpMultiPart(
        ByteBufUtil.encodeString(POOLED_ALLOCATOR, CharBuffer.wrap("Test Part 3"), StandardCharsets.US_ASCII));

    part1.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
    part1.headers().set(HttpHeaderNames.LOCATION, "/Foo/bar/12345/1");
    part2.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
    part2.headers().set(HttpHeaderNames.LOCATION, "/Foo/bar/12345/2");
    part3.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
    part3.headers().set(HttpHeaderNames.LOCATION, "/Foo/bar/12345/3");

    HttpUtil.setContentLength(part2, part2.content().readableBytes());

    Assert.assertTrue(channel.writeOutbound(response, part1, part2, part3, LastHttpContent.EMPTY_LAST_CONTENT));

    ByteBuf out = Unpooled.EMPTY_BUFFER;
    ByteBuf buf;
    while ((buf = channel.readOutbound()) != null) {
      out = ByteToMessageDecoder.COMPOSITE_CUMULATOR.cumulate(POOLED_ALLOCATOR, out, buf);
    }

    LogManager.getLogger(getClass()).info("Output = {}", ByteBufUtil.prettyHexDump(out));
    Assert.assertEquals(
        out.toString(StandardCharsets.US_ASCII),
        "HTTP/1.1 200 OK\r\n"
            + "content-type: multipart/mixed; boundary=\"--=Part_8928ea38-13cb-5d79-66ac-553475448f79\"\r\n"
            + "transfer-encoding: chunked\r\n" + "\r\n" + "75\r\n" + "\r\n"
            + "----=Part_8928ea38-13cb-5d79-66ac-553475448f79\r\n" + "content-type: text/plain\r\n"
            + "location: /Foo/bar/12345/1\r\n" + "\r\n" + "Test Part 1\r\n" + "89\r\n" + "\r\n"
            + "----=Part_8928ea38-13cb-5d79-66ac-553475448f79\r\n" + "content-type: text/plain\r\n"
            + "location: /Foo/bar/12345/2\r\n" + "content-length: 11\r\n" + "\r\n" + "Test Part 2\r\n" + "75\r\n"
            + "\r\n" + "----=Part_8928ea38-13cb-5d79-66ac-553475448f79\r\n" + "content-type: text/plain\r\n"
            + "location: /Foo/bar/12345/3\r\n" + "\r\n" + "Test Part 3\r\n" + "34\r\n" + "\r\n"
            + "----=Part_8928ea38-13cb-5d79-66ac-553475448f79--\r\n" + "\r\n" + "0\r\n" + "\r\n");
    assertRelease(out);
    channel.finishAndReleaseAll();
  }

  @Test
  public void basicTestNonMultiPart() {
    EmbeddedChannel channel = makeTestChannel();

    Assert.assertTrue(
        channel.writeInbound(
            encodeString(
                "GET /Foo/bar/12345 HTTP/1.1\r\n" + "Host: foo-host.example.org\r\n" + "Content-type: text/plain\r\n"
                    + "Content-length: " + (0xf5 - 0x85 + 33) + "\r\n" + "\r\n" + "Ignore this fuzz\r\n"
                    + "--Part_123456\r\n" + "Content-type: text/plain\r\n" + "\r\n" + "Hello world\r\n"
                    + "--Part_123456\r\n" + "Content-type: text/plain\r\n" + "\r\n" + "Part two!\r\n"
                    + "--Part_123456--\r\n" + "\r\n",
                StandardCharsets.US_ASCII)));

    HttpObject object = channel.readInbound();
    Assert.assertTrue(object instanceof HttpRequest);
    Assert.assertFalse(object instanceof FullHttpRequest);

    object = channel.readInbound();
    Assert.assertTrue(object instanceof LastHttpContent);

    Assert.assertEquals(
        ((LastHttpContent) object).content().toString(StandardCharsets.US_ASCII),
        "Ignore this fuzz\r\n" + "--Part_123456\r\n" + "Content-type: text/plain\r\n" + "\r\n" + "Hello world\r\n"
            + "--Part_123456\r\n" + "Content-type: text/plain\r\n" + "\r\n" + "Part two!\r\n" + "--Part_123456--\r\n");
    assertRelease(object);

    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, false);
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");

    ByteBuf content = encodeString("This is a simple test response\r\n", StandardCharsets.US_ASCII);

    HttpUtil.setContentLength(response, content.readableBytes());

    Assert.assertTrue(channel.writeOutbound(response, content, LastHttpContent.EMPTY_LAST_CONTENT));

    ByteBuf out = Unpooled.EMPTY_BUFFER;
    ByteBuf buf;
    while ((buf = channel.readOutbound()) != null) {
      out = ByteToMessageDecoder.COMPOSITE_CUMULATOR.cumulate(POOLED_ALLOCATOR, out, buf);
    }

    LogManager.getLogger(getClass()).info("Output = {}", ByteBufUtil.prettyHexDump(out));
    Assert.assertEquals(
        out.toString(StandardCharsets.US_ASCII),
        "HTTP/1.1 200 OK\r\n" + "content-type: text/plain\r\n" + "content-length: 32\r\n" + "\r\n"
            + "This is a simple test response\r\n");
    assertRelease(out);
    channel.finishAndReleaseAll();
  }

  @Test(invocationCount = 5)
  public void basicTestMultiPartAggregated() {
    testMultiPartAggregated(makeTestAggregatedChannel());
  }

  @Test(invocationCount = 5)
  public void basicTestNonMultiPartAggregated() {
    testNonMultiPartAggregated(makeTestAggregatedChannel());
  }

  @Test
  public void basicTestMultiPartHttpAggregatedBoundary() {
    testMultiPartBoundary(makeTestHttpAggregatedChannel());
  }

  @Test
  public void basicTestMultiPartHttpAggregated() {
    testMultiPartAggregated(makeTestHttpAggregatedChannel());
  }

  @Test
  public void basicTestNonMultiPartHttpAggregated() {
    testNonMultiPartAggregated(makeTestHttpAggregatedChannel());
  }

  @Test
  public void fullTestMultiPartHttpAggregated() throws IOException, MessagingException {
    testNestedMultiPartBoundary(makeTestHttpAggregatedChannel());
  }

  private void testMultiPartAggregated(EmbeddedChannel channel) {

    Assert.assertTrue(
        channel.writeInbound(
            encodeString(
                "GET /Foo/bar/12345 HTTP/1.1\r\n" + "Host: foo-host.example.org\r\n"
                    + "Content-type: multipart/mixed; boundary=Part_123456\r\n" + "Content-length: "
                    + (0xf5 - 0x85 + 15) + "\r\n" + "\r\n" + "--Part_123456\r\n" + "Content-type: text/plain\r\n"
                    + "\r\n" + "Hello world\r\n" + "--Part_123456\r\n" + "Content-type: text/plain\r\n" + "\r\n"
                    + "Part two!\r\n" + "--Part_123456--\r\n" + "\r\n",
                StandardCharsets.US_ASCII)));

    HttpObject object = channel.readInbound();
    Assert.assertTrue(object instanceof HttpRequest);
    Assert.assertFalse(object instanceof FullHttpRequest);

    object = channel.readInbound();
    Assert.assertTrue(object instanceof FullHttpMultiPart);
    Assert.assertEquals(((FullHttpMultiPart) object).content().toString(StandardCharsets.US_ASCII), "Hello world\r\n");
    assertRelease(object);

    object = channel.readInbound();
    Assert.assertTrue(object instanceof FullHttpMultiPart);
    Assert.assertEquals(((FullHttpMultiPart) object).content().toString(StandardCharsets.US_ASCII), "Part two!\r\n");
    assertRelease(object);

    object = channel.readInbound();
    Assert.assertTrue(object instanceof LastHttpContent);
    ReferenceCountUtil.release(object);

    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, false);
    response.headers()
        .set(
            HttpHeaderNames.CONTENT_TYPE,
            "multipart/mixed; boundary=\"--=Part_8928ea38-13cb-5d79-66ac-553475448f79\"");

    FullHttpMultiPart part1 = new BasicFullHttpMultiPart(encodeString("Test Part 1", StandardCharsets.US_ASCII));
    FullHttpMultiPart part2 = new BasicFullHttpMultiPart(encodeString("Test Part 2", StandardCharsets.US_ASCII));
    FullHttpMultiPart part3 = new BasicFullHttpMultiPart(encodeString("Test Part 3", StandardCharsets.US_ASCII));

    part1.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
    part1.headers().set(HttpHeaderNames.LOCATION, "/Foo/bar/12345/1");
    part2.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
    part2.headers().set(HttpHeaderNames.LOCATION, "/Foo/bar/12345/2");
    part3.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
    part3.headers().set(HttpHeaderNames.LOCATION, "/Foo/bar/12345/3");

    HttpUtil.setContentLength(part2, part2.content().readableBytes());

    Assert.assertTrue(channel.writeOutbound(response, part1, part2, part3, LastHttpContent.EMPTY_LAST_CONTENT));

    ByteBuf out = Unpooled.EMPTY_BUFFER;
    ByteBuf buf;
    while ((buf = channel.readOutbound()) != null) {
      out = ByteToMessageDecoder.COMPOSITE_CUMULATOR.cumulate(POOLED_ALLOCATOR, out, buf);
    }

    LogManager.getLogger(getClass()).info("Output = {}", ByteBufUtil.prettyHexDump(out));
    Assert.assertEquals(
        out.toString(StandardCharsets.US_ASCII),
        "HTTP/1.1 200 OK\r\n"
            + "content-type: multipart/mixed; boundary=\"--=Part_8928ea38-13cb-5d79-66ac-553475448f79\"\r\n"
            + "transfer-encoding: chunked\r\n" + "\r\n" + "75\r\n" + "\r\n"
            + "----=Part_8928ea38-13cb-5d79-66ac-553475448f79\r\n" + "content-type: text/plain\r\n"
            + "location: /Foo/bar/12345/1\r\n" + "\r\n" + "Test Part 1\r\n" + "89\r\n" + "\r\n"
            + "----=Part_8928ea38-13cb-5d79-66ac-553475448f79\r\n" + "content-type: text/plain\r\n"
            + "location: /Foo/bar/12345/2\r\n" + "content-length: 11\r\n" + "\r\n" + "Test Part 2\r\n" + "75\r\n"
            + "\r\n" + "----=Part_8928ea38-13cb-5d79-66ac-553475448f79\r\n" + "content-type: text/plain\r\n"
            + "location: /Foo/bar/12345/3\r\n" + "\r\n" + "Test Part 3\r\n" + "34\r\n" + "\r\n"
            + "----=Part_8928ea38-13cb-5d79-66ac-553475448f79--\r\n" + "\r\n" + "0\r\n" + "\r\n");
    assertRelease(out);
    channel.finishAndReleaseAll();
  }

  private void testNonMultiPartAggregated(EmbeddedChannel channel) {

    Assert.assertTrue(
        channel.writeInbound(
            encodeString(
                "GET /Foo/bar/12345 HTTP/1.1\r\n" + "Host: foo-host.example.org\r\n" + "Content-type: text/plain\r\n"
                    + "Content-length: " + (0xf5 - 0x85 + 15) + "\r\n" + "\r\n" + "--Part_123456\r\n"
                    + "Content-type: text/plain\r\n" + "\r\n" + "Hello world\r\n" + "--Part_123456\r\n"
                    + "Content-type: text/plain\r\n" + "\r\n" + "Part two!\r\n" + "--Part_123456--\r\n" + "\r\n",
                StandardCharsets.US_ASCII)));

    HttpObject object = channel.readInbound();
    Assert.assertTrue(object instanceof FullHttpRequest);
    Assert.assertEquals(
        ((HttpContent) object).content().toString(StandardCharsets.US_ASCII),
        "--Part_123456\r\n" + "Content-type: text/plain\r\n" + "\r\n" + "Hello world\r\n" + "--Part_123456\r\n"
            + "Content-type: text/plain\r\n" + "\r\n" + "Part two!\r\n" + "--Part_123456--\r\n");
    assertRelease(object);

    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, false);
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");

    ByteBuf content = encodeString("This is a simple test response\r\n", StandardCharsets.US_ASCII);

    HttpUtil.setContentLength(response, content.readableBytes());

    Assert.assertTrue(channel.writeOutbound(response, content.retainedDuplicate(), LastHttpContent.EMPTY_LAST_CONTENT));

    ByteBuf out = Unpooled.EMPTY_BUFFER;
    ByteBuf buf;
    while ((buf = channel.readOutbound()) != null) {
      out = ByteToMessageDecoder.COMPOSITE_CUMULATOR.cumulate(POOLED_ALLOCATOR, out, buf);
    }

    LogManager.getLogger(getClass()).info("Output = {}", ByteBufUtil.prettyHexDump(out));
    Assert.assertEquals(
        out.toString(StandardCharsets.US_ASCII),
        "HTTP/1.1 200 OK\r\n" + "content-type: text/plain\r\n" + "content-length: 32\r\n" + "\r\n"
            + "This is a simple test response\r\n");
    assertRelease(out);
    assertRelease(content);
    channel.finishAndReleaseAll();
  }

  private void testMultiPartBoundary(EmbeddedChannel channel) {

    Assert.assertTrue(
        channel.writeInbound(
            encodeString(
                "GET /Foo/bar/12345 HTTP/1.1\r\n" + "Host: foo-host.example.org\r\n"
                    + "Content-type: multipart/mixed; boundary=Part_123456\r\n" + "Content-length: "
                    + (0xf5 - 0x85 + 15) + "\r\n" + "\r\n" + "--Part_123456\r\n" + "Content-type: text/plain\r\n"
                    + "\r\n" + "Hello world\r\n" + "--Part_123456\r\n" + "Content-type: text/plain\r\n" + "\r\n"
                    + "Part two!\r\n" + "--Part_123456--\r\n" + "\r\n",
                StandardCharsets.US_ASCII)));

    HttpObject object = channel.readInbound();
    Assert.assertTrue(object instanceof HttpRequest);
    Assert.assertFalse(object instanceof FullHttpRequest);

    object = channel.readInbound();
    Assert.assertTrue(object instanceof FullHttpMultiPart);
    Assert.assertEquals(((FullHttpMultiPart) object).content().toString(StandardCharsets.US_ASCII), "Hello world\r\n");
    assertRelease(object);

    object = channel.readInbound();
    Assert.assertTrue(object instanceof FullHttpMultiPart);
    Assert.assertEquals(((FullHttpMultiPart) object).content().toString(StandardCharsets.US_ASCII), "Part two!\r\n");
    assertRelease(object);

    object = channel.readInbound();
    Assert.assertTrue(object instanceof LastHttpContent);
    ReferenceCountUtil.release(object);

    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, false);
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "multipart/mixed");

    FullHttpMultiPart part1 = new BasicFullHttpMultiPart(encodeString("Test Part 1", StandardCharsets.US_ASCII));
    FullHttpMultiPart part2 = new BasicFullHttpMultiPart(encodeString("Test Part 2", StandardCharsets.US_ASCII));
    FullHttpMultiPart part3 = new BasicFullHttpMultiPart(encodeString("Test Part 3", StandardCharsets.US_ASCII));

    part1.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
    part1.headers().set(HttpHeaderNames.LOCATION, "/Foo/bar/12345/1");
    part2.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
    part2.headers().set(HttpHeaderNames.LOCATION, "/Foo/bar/12345/2");
    part3.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
    part3.headers().set(HttpHeaderNames.LOCATION, "/Foo/bar/12345/3");

    HttpUtil.setContentLength(part2, part2.content().readableBytes());

    Assert.assertTrue(channel.writeOutbound(response, part1, part2, part3, LastHttpContent.EMPTY_LAST_CONTENT));

    ByteBuf out = Unpooled.EMPTY_BUFFER;
    ByteBuf buf;
    while ((buf = channel.readOutbound()) != null) {
      out = ByteToMessageDecoder.COMPOSITE_CUMULATOR.cumulate(POOLED_ALLOCATOR, out, buf);
    }

    LogManager.getLogger(getClass()).info("Output = {}", ByteBufUtil.prettyHexDump(out));
    String content = out.toString(StandardCharsets.US_ASCII);

    Matcher m = Pattern.compile("\"--=Part_(........-....-....-....-............)\"").matcher(content);
    Assert.assertTrue(m.find());
    String boundary = m.group(1);

    Assert.assertEquals(
        content,
        "HTTP/1.1 200 OK\r\n" + "content-type: multipart/mixed; boundary=\"--=Part_" + boundary + "\"\r\n"
            + "transfer-encoding: chunked\r\n" + "\r\n" + "75\r\n" + "\r\n" + "----=Part_" + boundary + "\r\n"
            + "content-type: text/plain\r\n" + "location: /Foo/bar/12345/1\r\n" + "\r\n" + "Test Part 1\r\n" + "89\r\n"
            + "\r\n" + "----=Part_" + boundary + "\r\n" + "content-type: text/plain\r\n"
            + "location: /Foo/bar/12345/2\r\n" + "content-length: 11\r\n" + "\r\n" + "Test Part 2\r\n" + "75\r\n"
            + "\r\n" + "----=Part_" + boundary + "\r\n" + "content-type: text/plain\r\n"
            + "location: /Foo/bar/12345/3\r\n" + "\r\n" + "Test Part 3\r\n" + "34\r\n" + "\r\n" + "----=Part_"
            + boundary + "--\r\n" + "\r\n" + "0\r\n" + "\r\n");
    assertRelease(out);
    channel.finishAndReleaseAll();
  }

  private MimeBodyPart makePart(String type, byte[] bytes) throws MessagingException {
    InternetHeaders headers = new InternetHeaders();
    headers.setHeader("Content-Type", type);
    headers.setHeader("Content-Length", Integer.toString(bytes.length));
    return new MimeBodyPart(headers, bytes);
  }

  private void testNestedMultiPartBoundary(EmbeddedChannel channel) throws MessagingException, IOException {
    ByteBuf requestBytes = POOLED_ALLOCATOR.buffer();
    AsciiString innerContent;
    {
      MimeMultipart mp = new MimeMultipart();
      mp.addBodyPart(makePart("text/plain", "Hello World".getBytes()));
      mp.addBodyPart(new MimeBodyPart(new InternetHeaders(), "Hello World2".getBytes()));
      mp.addBodyPart(makePart("text/plain", new byte[0]));

      ByteArrayOutputStream innerBytes = new ByteArrayOutputStream();
      mp.writeTo(innerBytes);
      innerContent = new AsciiString(innerBytes.toByteArray());

      mp.addBodyPart(makePart(mp.getContentType(), innerBytes.toByteArray()));

      mp.addBodyPart(makePart("text/plain", "Goodbyte world\r\n".getBytes()));

      ByteBuf content = POOLED_ALLOCATOR.buffer();
      mp.writeTo(new ByteBufOutputStream(content));

      ByteBufUtil.writeAscii(
          requestBytes,
          "GET /foo/bar/54321 HTTP/1.1\r\n" + "Host: foo-host.example.org\r\n" + "Content-Type: ");
      ByteBufUtil.writeAscii(requestBytes, mp.getContentType());
      ByteBufUtil.writeAscii(requestBytes, "\r\n" + "Content-length: ");
      ByteBufUtil.writeAscii(requestBytes, Integer.toString(content.readableBytes()));
      ByteBufUtil.writeAscii(requestBytes, "\r\n\r\n");
      requestBytes.writeBytes(content);

      content.release();
    }

    Assert.assertTrue(channel.writeInbound(requestBytes));

    HttpObject object = channel.readInbound();
    Assert.assertTrue(object instanceof HttpRequest);
    Assert.assertFalse(object instanceof FullHttpRequest);

    object = channel.readInbound();
    Assert.assertTrue(object instanceof FullHttpMultiPart);
    Assert.assertEquals(((FullHttpMultiPart) object).content().toString(StandardCharsets.US_ASCII), "Hello World");
    assertRelease(object);

    object = channel.readInbound();
    Assert.assertTrue(object instanceof FullHttpMultiPart);
    Assert.assertEquals(((FullHttpMultiPart) object).content().toString(StandardCharsets.US_ASCII), "Hello World2\r\n");
    assertRelease(object);

    object = channel.readInbound();
    Assert.assertTrue(object instanceof FullHttpMultiPart);
    Assert.assertEquals(((FullHttpMultiPart) object).content().toString(StandardCharsets.US_ASCII), "");
    assertRelease(object);

    object = channel.readInbound();
    Assert.assertTrue(object instanceof FullHttpMultiPart);
    Assert.assertEquals(
        ((FullHttpMultiPart) object).content().toString(StandardCharsets.US_ASCII),
        innerContent.toString());
    assertRelease(object);

    object = channel.readInbound();
    Assert.assertTrue(object instanceof FullHttpMultiPart);
    Assert
        .assertEquals(((FullHttpMultiPart) object).content().toString(StandardCharsets.US_ASCII), "Goodbyte world\r\n");
    assertRelease(object);

    object = channel.readInbound();
    Assert.assertTrue(object instanceof LastHttpContent);
    ReferenceCountUtil.release(object);

    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, false);
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "multipart/mixed");

    FullHttpMultiPart part1 = new BasicFullHttpMultiPart(encodeString("Test Part 1", StandardCharsets.US_ASCII));
    FullHttpMultiPart part2 = new BasicFullHttpMultiPart(encodeString("Test Part 2", StandardCharsets.US_ASCII));
    FullHttpMultiPart part3 = new BasicFullHttpMultiPart(encodeString("Test Part 3", StandardCharsets.US_ASCII));

    part1.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
    part1.headers().set(HttpHeaderNames.LOCATION, "/Foo/bar/12345/1");
    part2.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
    part2.headers().set(HttpHeaderNames.LOCATION, "/Foo/bar/12345/2");
    part3.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
    part3.headers().set(HttpHeaderNames.LOCATION, "/Foo/bar/12345/3");

    HttpUtil.setContentLength(part2, part2.content().readableBytes());

    Assert.assertTrue(channel.writeOutbound(response, part1, part2, part3, LastHttpContent.EMPTY_LAST_CONTENT));

    ByteBuf out = Unpooled.EMPTY_BUFFER;
    ByteBuf buf;
    while ((buf = channel.readOutbound()) != null) {
      out = ByteToMessageDecoder.COMPOSITE_CUMULATOR.cumulate(POOLED_ALLOCATOR, out, buf);
    }

    LogManager.getLogger(getClass()).info("Output = {}", ByteBufUtil.prettyHexDump(out));
    String content = out.toString(StandardCharsets.US_ASCII);

    Matcher m = Pattern.compile("\"--=Part_(........-....-....-....-............)\"").matcher(content);
    Assert.assertTrue(m.find());
    String boundary = m.group(1);

    Assert.assertEquals(
        content,
        "HTTP/1.1 200 OK\r\n" + "content-type: multipart/mixed; boundary=\"--=Part_" + boundary + "\"\r\n"
            + "transfer-encoding: chunked\r\n" + "\r\n" + "75\r\n" + "\r\n" + "----=Part_" + boundary + "\r\n"
            + "content-type: text/plain\r\n" + "location: /Foo/bar/12345/1\r\n" + "\r\n" + "Test Part 1\r\n" + "89\r\n"
            + "\r\n" + "----=Part_" + boundary + "\r\n" + "content-type: text/plain\r\n"
            + "location: /Foo/bar/12345/2\r\n" + "content-length: 11\r\n" + "\r\n" + "Test Part 2\r\n" + "75\r\n"
            + "\r\n" + "----=Part_" + boundary + "\r\n" + "content-type: text/plain\r\n"
            + "location: /Foo/bar/12345/3\r\n" + "\r\n" + "Test Part 3\r\n" + "34\r\n" + "\r\n" + "----=Part_"
            + boundary + "--\r\n" + "\r\n" + "0\r\n" + "\r\n");
    assertRelease(out);
    channel.finishAndReleaseAll();
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
