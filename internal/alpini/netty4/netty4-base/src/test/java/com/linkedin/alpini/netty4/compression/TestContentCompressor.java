package com.linkedin.alpini.netty4.compression;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.alpini.base.concurrency.NamedThreadFactory;
import com.linkedin.alpini.netty4.handlers.AbstractLeakDetect;
import com.linkedin.alpini.netty4.handlers.HttpClientResponseHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.AsciiString;
import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Promise;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 4/12/17.
 */
@Test(groups = { "unit", "unit-leak", "leak" }, singleThreaded = true)
public final class TestContentCompressor extends AbstractLeakDetect {
  private final Logger _log = LogManager.getLogger(getClass());
  private EventLoopGroup _group;
  private EventLoopGroup _compressorGroup;

  @BeforeClass
  public void beforeClass() {
    _group = new DefaultEventLoopGroup();
    _compressorGroup =
        new DefaultEventLoopGroup(1, Executors.newSingleThreadExecutor(new NamedThreadFactory("testCompressor")));
  }

  @AfterClass
  public void afterClass() {
    Optional.ofNullable(_group).ifPresent(EventLoopGroup::shutdownGracefully);
    Optional.ofNullable(_compressorGroup).ifPresent(EventLoopGroup::shutdownGracefully);
  }

  static void assertRelease(ReferenceCounted referenceCounted) throws InterruptedException {
    Thread.sleep(100);
    Assert.assertTrue(referenceCounted.release());
  }

  boolean shouldSnappyCompress(String acceptEncoding) {
    return CompressionUtils.SNAPPY.contentEquals(ContentCompressor.preferredEncoding(acceptEncoding));
  }

  @Test
  public void testBasicshouldSnappyCompress() {
    // Should compress cases
    Assert.assertTrue(shouldSnappyCompress("snappy"));
    Assert.assertTrue(shouldSnappyCompress("snappy, gzip"));
    Assert.assertTrue(shouldSnappyCompress("snappy, gzip, deflate"));
    Assert.assertTrue(shouldSnappyCompress("snappy, deflate, gzip"));
    Assert.assertTrue(shouldSnappyCompress("snappy, deflate"));
    Assert.assertTrue(shouldSnappyCompress("snappy, huffman"));
    Assert.assertTrue(shouldSnappyCompress("snappy, nonexistent"));
    Assert.assertTrue(shouldSnappyCompress("nonexistent, snappy"));
    Assert.assertTrue(shouldSnappyCompress("huffman, snappy"));

    // Shouldn't compress cases
    Assert.assertFalse(shouldSnappyCompress(null));
    Assert.assertFalse(shouldSnappyCompress("deflate, gzip"));
    Assert.assertFalse(shouldSnappyCompress("deflate, gzip, snappy"));
    Assert.assertFalse(shouldSnappyCompress("deflate, snappy, gzip"));
    Assert.assertFalse(shouldSnappyCompress("gzip, deflate, snappy"));
  }

  @Test
  public void testBasicQValues() {
    // Should compress cases
    Assert.assertTrue(shouldSnappyCompress("snappy;q=1"));
    Assert.assertTrue(shouldSnappyCompress("snappy, gzip;q=0.8"));
    Assert.assertTrue(shouldSnappyCompress("snappy, gzip;q=0.9"));
    Assert.assertTrue(shouldSnappyCompress("snappy;q=0.9, gzip;q=0.8"));
    Assert.assertTrue(shouldSnappyCompress("snappy;q=0.9, gzip;q=0.9"));
    Assert.assertTrue(shouldSnappyCompress("snappy, gzip;q=0.9, deflate;q=0.2"));
    Assert.assertTrue(shouldSnappyCompress("snappy, gzip;q=0.9, deflate;q=1"));
    Assert.assertTrue(shouldSnappyCompress("snappy;q=0.3, gzip;q=0.2, deflate;q=0.2"));
    Assert.assertTrue(shouldSnappyCompress("snappy;q=0.3, gzip;q=0.2, deflate;q=0.2"));
    Assert.assertTrue(shouldSnappyCompress("gzip;q=0.2,snappy;q=0.3, deflate;q=0.2"));
    Assert.assertTrue(shouldSnappyCompress("gzip;q=0.9,snappy, deflate;q=0.2"));

    // Shouldn't compress cases
    Assert.assertFalse(shouldSnappyCompress("deflate;q=0.9, gzip;q=0.2"));
    Assert.assertFalse(shouldSnappyCompress("deflate;q=0.2, gzip;q=0.9, snappy;q=0.2"));
    Assert.assertFalse(shouldSnappyCompress("snappy;q=0.7, gzip;q=0.8"));
    Assert.assertFalse(shouldSnappyCompress("snappy;q=0.9, gzip"));
    Assert.assertFalse(shouldSnappyCompress("snappy;q=0.9, gzip, deflate"));

  }

  enum Encoding {
    CHUNKED, FULL
  }

  @DataProvider
  public Object[][] encodings() {
    return new Object[][] { new Object[] { CompressionUtils.DEFLATE, Encoding.CHUNKED, null, null },
        new Object[] { CompressionUtils.DEFLATE, Encoding.FULL, null, null },
        new Object[] { CompressionUtils.DEFLATE, Encoding.CHUNKED, _compressorGroup, null },
        new Object[] { CompressionUtils.DEFLATE, Encoding.FULL, _compressorGroup, null },
        new Object[] { CompressionUtils.X_DEFLATE, Encoding.CHUNKED, null, null },
        new Object[] { CompressionUtils.X_DEFLATE, Encoding.FULL, null, null },
        new Object[] { CompressionUtils.X_DEFLATE, Encoding.CHUNKED, _compressorGroup, null },
        new Object[] { CompressionUtils.X_DEFLATE, Encoding.FULL, _compressorGroup, null },
        new Object[] { CompressionUtils.GZIP, Encoding.CHUNKED, null, null },
        new Object[] { CompressionUtils.GZIP, Encoding.FULL, null, null },
        new Object[] { CompressionUtils.GZIP, Encoding.CHUNKED, _compressorGroup, null },
        new Object[] { CompressionUtils.GZIP, Encoding.FULL, _compressorGroup, null },
        new Object[] { CompressionUtils.X_GZIP, Encoding.CHUNKED, null, null },
        new Object[] { CompressionUtils.X_GZIP, Encoding.FULL, null, null },
        new Object[] { CompressionUtils.X_GZIP, Encoding.CHUNKED, _compressorGroup, null },
        new Object[] { CompressionUtils.X_GZIP, Encoding.FULL, _compressorGroup, null },
        new Object[] { CompressionUtils.SNAPPY, Encoding.CHUNKED, null, null },
        new Object[] { CompressionUtils.SNAPPY, Encoding.FULL, null, null },
        new Object[] { CompressionUtils.SNAPPY, Encoding.CHUNKED, _compressorGroup, null },
        new Object[] { CompressionUtils.SNAPPY, Encoding.FULL, _compressorGroup, null },
        new Object[] { CompressionUtils.X_SNAPPY, Encoding.CHUNKED, null, null },
        new Object[] { CompressionUtils.X_SNAPPY, Encoding.FULL, null, null },
        new Object[] { CompressionUtils.X_SNAPPY, Encoding.CHUNKED, _compressorGroup, null },
        new Object[] { CompressionUtils.X_SNAPPY, Encoding.FULL, _compressorGroup, null },
        new Object[] { CompressionUtils.SNAPPY_FRAMED, Encoding.CHUNKED, null, null },
        new Object[] { CompressionUtils.SNAPPY_FRAMED, Encoding.FULL, null, null },
        new Object[] { CompressionUtils.SNAPPY_FRAMED, Encoding.CHUNKED, _compressorGroup, null },
        new Object[] { CompressionUtils.SNAPPY_FRAMED, Encoding.FULL, _compressorGroup, null },

        new Object[] { CompressionUtils.DEFLATE, Encoding.CHUNKED, null, "gzip,deflate" },
        new Object[] { CompressionUtils.DEFLATE, Encoding.FULL, null, "gzip" },
        new Object[] { CompressionUtils.DEFLATE, Encoding.CHUNKED, _compressorGroup, "deflate" },
        new Object[] { CompressionUtils.DEFLATE, Encoding.FULL, _compressorGroup, "none" },
        new Object[] { CompressionUtils.X_DEFLATE, Encoding.CHUNKED, null, "gzip,deflate" },
        new Object[] { CompressionUtils.X_DEFLATE, Encoding.FULL, null, "gzip" },
        new Object[] { CompressionUtils.X_DEFLATE, Encoding.CHUNKED, _compressorGroup, "deflate" },
        new Object[] { CompressionUtils.X_DEFLATE, Encoding.FULL, _compressorGroup, "none" },
        new Object[] { CompressionUtils.GZIP, Encoding.CHUNKED, null, "gzip,deflate" },
        new Object[] { CompressionUtils.GZIP, Encoding.FULL, null, "gzip" },
        new Object[] { CompressionUtils.GZIP, Encoding.CHUNKED, _compressorGroup, "bzip2" },
        new Object[] { CompressionUtils.GZIP, Encoding.FULL, _compressorGroup, "none" },
        new Object[] { CompressionUtils.X_GZIP, Encoding.CHUNKED, null, "gzip" },
        new Object[] { CompressionUtils.X_GZIP, Encoding.FULL, null, "gzip,deflate" },
        new Object[] { CompressionUtils.X_GZIP, Encoding.CHUNKED, _compressorGroup, "deflate" },
        new Object[] { CompressionUtils.X_GZIP, Encoding.FULL, _compressorGroup, "none" },
        new Object[] { CompressionUtils.SNAPPY, Encoding.CHUNKED, null, "gzip,deflate" },
        new Object[] { CompressionUtils.SNAPPY, Encoding.FULL, null, "gzip,deflate" },
        new Object[] { CompressionUtils.SNAPPY, Encoding.CHUNKED, _compressorGroup, "gzip,deflate" },
        new Object[] { CompressionUtils.SNAPPY, Encoding.FULL, _compressorGroup, "gzip,deflate" },
        new Object[] { CompressionUtils.X_SNAPPY, Encoding.CHUNKED, null, "gzip,deflate" },
        new Object[] { CompressionUtils.X_SNAPPY, Encoding.FULL, null, "deflate" },
        new Object[] { CompressionUtils.X_SNAPPY, Encoding.CHUNKED, _compressorGroup, "deflate" },
        new Object[] { CompressionUtils.X_SNAPPY, Encoding.FULL, _compressorGroup, "none" },
        new Object[] { CompressionUtils.SNAPPY_FRAMED, Encoding.CHUNKED, null, "gzip,deflate" },
        new Object[] { CompressionUtils.SNAPPY_FRAMED, Encoding.FULL, null, "gzip" },
        new Object[] { CompressionUtils.SNAPPY_FRAMED, Encoding.CHUNKED, _compressorGroup, "deflate" },
        new Object[] { CompressionUtils.SNAPPY_FRAMED, Encoding.FULL, _compressorGroup, "none" }, };
  }

  @Test(dataProvider = "encodings")
  public void testLocalCompressor(
      AsciiString encoding,
      Encoding chunked,
      EventExecutorGroup executor,
      String supportedEncodings) throws InterruptedException {
    ChannelGroup channelGroup = new DefaultChannelGroup(_group.next());
    try {
      ServerBootstrap sb = new ServerBootstrap().group(_group)
          .channel(LocalServerChannel.class)
          .childHandler(new ChannelInitializer<LocalChannel>() {
            @Override
            protected void initChannel(LocalChannel ch) throws Exception {
              ch.pipeline()
                  .addLast(
                      new LoggingHandler("server:edge", LogLevel.DEBUG),
                      new HttpServerCodec(),
                      new ContentCompressor(supportedEncodings == null ? null : supportedEncodings, executor),
                      new ContentDecompressor(),
                      new HttpObjectAggregator(1024 * 1024),
                      // new LoggingHandler("server:inner", LogLevel.DEBUG),
                      new SimpleChannelInboundHandler<FullHttpRequest>() {
                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
                          ByteBuf response = ctx.alloc().buffer(msg.content().readableBytes());
                          response.writeBytes(
                              new StringBuilder(msg.content().toString(StandardCharsets.US_ASCII)).reverse()
                                  .toString()
                                  .getBytes(StandardCharsets.US_ASCII));

                          FullHttpResponse responseHeader =
                              new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, response, false);
                          HttpUtil.setContentLength(responseHeader, response.readableBytes());
                          HttpUtil.setTransferEncodingChunked(responseHeader, chunked == Encoding.CHUNKED);
                          HttpUtil.setKeepAlive(responseHeader, true);

                          ctx.writeAndFlush(responseHeader);
                        }
                      });
            }
          });

      Bootstrap cb =
          new Bootstrap().group(_group).channel(LocalChannel.class).handler(new ChannelInitializer<LocalChannel>() {
            @Override
            protected void initChannel(LocalChannel ch) throws Exception {
              ch.pipeline()
                  .addLast(
                      // new LoggingHandler("client:edge", LogLevel.DEBUG),
                      new HttpClientCodec(),
                      new ContentCompressor() {
                        @Override
                        protected CharSequence requestEncoding0(HttpRequest request) {
                          return encoding;
                        }
                      },
                      new ContentDecompressor(),
                      new HttpObjectAggregator(1024 * 1024),
                      // new LoggingHandler("client:inner", LogLevel.DEBUG),
                      new HttpClientResponseHandler());
            }
          });

      LocalAddress addr = new LocalAddress("testContentCompressor");

      channelGroup.add(sb.bind(addr).sync().channel());

      Channel ch = cb.connect(addr).sync().channel();
      channelGroup.add(ch);
      Promise<FullHttpResponse> response = ch.eventLoop().newPromise();

      class Request extends DefaultFullHttpRequest implements HttpClientResponseHandler.ResponseConsumer {
        public Request(HttpVersion httpVersion, HttpMethod method, String uri, ByteBuf content) {
          super(httpVersion, method, uri, content);
        }

        @Override
        public Consumer<Object> responseConsumer() {
          return r -> {
            _log.info("response: {} {}", r, "");
            if (r instanceof Throwable) {
              response.setFailure((Throwable) r);
            } else {
              response.setSuccess(((FullHttpResponse) r).retain());
            }
          };
        }
      }

      ByteBuf requestBytes = ch.alloc().buffer();
      for (int i = 0; i < 1000; i++) {
        requestBytes.writeBytes("urn:li:pretend-id:".getBytes(StandardCharsets.US_ASCII));
        requestBytes
            .writeBytes(Integer.toString(ThreadLocalRandom.current().nextInt()).getBytes(StandardCharsets.US_ASCII));
        requestBytes.writeByte(',');
      }

      HttpRequest request =
          new Request(HttpVersion.HTTP_1_1, HttpMethod.POST, "/reverse/", requestBytes.retainedDuplicate());
      HttpUtil.setContentLength(request, requestBytes.readableBytes());
      HttpUtil.setTransferEncodingChunked(request, chunked == Encoding.CHUNKED);
      HttpUtil.setKeepAlive(request, true);
      request.headers().set(HttpHeaderNames.ACCEPT_ENCODING, encoding);
      _log.info("WriteAndFlush");
      ch.writeAndFlush(request);
      String expect = new StringBuilder(requestBytes.toString(StandardCharsets.US_ASCII)).reverse().toString();
      ch.write(new DefaultHttpContent(requestBytes.slice()));
      _log.info("Wait for response");

      Assert.assertTrue(response.await(5, TimeUnit.SECONDS));

      FullHttpResponse httpResponse = response.getNow();

      Assert.assertEquals(httpResponse.status(), HttpResponseStatus.OK);

      Assert.assertEquals(httpResponse.content().toString(StandardCharsets.US_ASCII), expect);

      assertRelease(httpResponse);
    } finally {
      channelGroup.close().sync();
    }
  }

  @Test
  public void testSmallResponse() throws InterruptedException {

    EmbeddedChannel ch = new EmbeddedChannel(new ContentCompressor());

    FullHttpRequest request =
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/foo", Unpooled.EMPTY_BUFFER);
    request.headers().set(HttpHeaderNames.ACCEPT_ENCODING, "snappy");
    ch.writeInbound(request);
    ch.flushInbound();

    Assert.assertSame(ch.readInbound(), request);

    FullHttpResponse response = new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1,
        HttpResponseStatus.OK,
        ByteBufUtil.writeAscii(POOLED_ALLOCATOR, "A tiny response, too small to compress"));
    HttpUtil.setContentLength(response, response.content().readableBytes());
    response.headers().set(HttpHeaderNames.CONTENT_ENCODING, HttpHeaderValues.IDENTITY);

    ch.writeOutbound(response);
    ch.flushOutbound();

    Assert.assertSame(ch.readOutbound(), response);

    Assert
        .assertEquals(response.content().toString(StandardCharsets.US_ASCII), "A tiny response, too small to compress");
    Assert.assertFalse(response.headers().contains(HttpHeaderNames.CONTENT_ENCODING));

    assertRelease(response);
    ch.finishAndReleaseAll();
  }

  @Test
  public void testAlreadyAnyEncoded() throws InterruptedException {

    EmbeddedChannel ch = new EmbeddedChannel(new ContentCompressor());

    FullHttpRequest request =
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/foo", Unpooled.EMPTY_BUFFER);
    request.headers().set(HttpHeaderNames.ACCEPT_ENCODING, "*");
    ch.writeInbound(request);
    ch.flushInbound();

    Assert.assertSame(ch.readInbound(), request);

    String content = IntStream.range(1, 1000).mapToObj(String::valueOf).collect(Collectors.joining("-"));
    FullHttpResponse response = new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1,
        HttpResponseStatus.OK,
        ByteBufUtil.writeAscii(POOLED_ALLOCATOR, content));
    HttpUtil.setContentLength(response, response.content().readableBytes());

    // lets pretend that this is a snappy encoded response.
    response.headers().set(HttpHeaderNames.CONTENT_ENCODING, CompressionUtils.SNAPPY);

    ch.writeOutbound(response);
    ch.flushOutbound();

    Assert.assertSame(ch.readOutbound(), response);

    Assert.assertEquals(response.content().toString(StandardCharsets.US_ASCII), content);
    Assert.assertEquals(response.headers().get(HttpHeaderNames.CONTENT_ENCODING), CompressionUtils.SNAPPY_ENCODING);

    assertRelease(response);
    ch.finishAndReleaseAll();
  }

  @Test
  public void testAlreadySpecificEncoded() throws InterruptedException {

    EmbeddedChannel ch = new EmbeddedChannel(new ContentCompressor(6));

    FullHttpRequest request =
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/foo", Unpooled.EMPTY_BUFFER);
    request.headers().set(HttpHeaderNames.ACCEPT_ENCODING, "snappy,identity");
    ch.writeInbound(request);
    ch.flushInbound();

    Assert.assertSame(ch.readInbound(), request);

    String content = IntStream.range(1, 1000).mapToObj(String::valueOf).collect(Collectors.joining("-"));
    FullHttpResponse response = new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1,
        HttpResponseStatus.OK,
        ByteBufUtil.writeAscii(POOLED_ALLOCATOR, content));
    HttpUtil.setContentLength(response, response.content().readableBytes());

    // lets pretend that this is a snappy encoded response.
    response.headers().set(HttpHeaderNames.CONTENT_ENCODING, CompressionUtils.SNAPPY);

    ch.writeOutbound(response);
    ch.flushOutbound();

    Assert.assertSame(ch.readOutbound(), response);

    Assert.assertEquals(response.content().toString(StandardCharsets.US_ASCII), content);
    Assert.assertEquals(response.headers().get(HttpHeaderNames.CONTENT_ENCODING), CompressionUtils.SNAPPY_ENCODING);

    assertRelease(response);
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
