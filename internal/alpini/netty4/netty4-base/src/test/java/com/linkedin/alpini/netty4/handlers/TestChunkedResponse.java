package com.linkedin.alpini.netty4.handlers;

import static io.netty.handler.codec.http.HttpMethod.*;

import com.linkedin.alpini.netty4.misc.BasicFullHttpResponse;
import com.linkedin.alpini.netty4.misc.ChunkedHttpResponse;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.Promise;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 5/16/17.
 */
@Test(groups = { "unit", "unit-leak", "leak" }, singleThreaded = true)
public final class TestChunkedResponse extends AbstractLeakDetect {
  private static final Logger LOG = LogManager.getLogger(TestChunkedResponse.class);

  @Test
  public void testChunkedResponses() throws InterruptedException {
    // org.apache.log4j.BasicConfigurator.configure();
    EventLoopGroup eventLoop = new DefaultEventLoopGroup(1);
    ServerChannel serverChannel = null;
    try {

      LocalAddress localAddress = new LocalAddress("testChunkedResponses");

      ServerBootstrap serverBootstrap = new ServerBootstrap().group(eventLoop)
          .channel(LocalServerChannel.class)
          .childOption(ChannelOption.ALLOCATOR, POOLED_ALLOCATOR)
          .childHandler(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
              ch.pipeline()
                  .addLast(
                      new BasicHttpServerCodec(),
                      new BasicHttpObjectAggregator(81920),

                      // This handler limits the size of the sent chunks to 8192 by breaking it into 4000 byte chunks
                      new ChunkedResponseLimiter(8192, 4000),

                      // This handler handles chunked responses which implement ChunkedHttpResponse
                      new ChunkedResponseHandler(),

                      new LoggingHandler("server", LogLevel.DEBUG),

                      new SimpleChannelInboundHandler<FullHttpRequest>() {
                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request)
                            throws Exception {

                          class ChunkedResponse extends BasicFullHttpResponse implements ChunkedHttpResponse {
                            private final Iterator<HttpContent> _contentIterator;

                            private ChunkedResponse(
                                HttpRequest httpRequest,
                                HttpResponseStatus status,
                                Iterator<HttpContent> contentIterator) {
                              super(httpRequest, status, Unpooled.EMPTY_BUFFER);
                              HttpUtil.setTransferEncodingChunked(this, true);
                              _contentIterator = Objects.requireNonNull(contentIterator);
                            }

                            @Override
                            public void writeChunkedContent(
                                ChannelHandlerContext ctx,
                                Promise<LastHttpContent> writePromise) throws IOException {

                              if (_contentIterator.hasNext()) {

                                HttpContent chunk = _contentIterator.next();

                                if (chunk instanceof LastHttpContent) {
                                  writePromise.setSuccess((LastHttpContent) chunk);
                                  return;
                                }

                                assert chunk.content().readableBytes() > 0;
                                assert !(chunk instanceof HttpMessage);

                                ctx.writeAndFlush(chunk).addListener(future -> {
                                  if (future.isSuccess()) {
                                    writeChunkedContent(ctx, writePromise);
                                  } else {
                                    writePromise.setFailure(future.cause());
                                  }
                                });
                              } else {
                                writePromise.setSuccess(LastHttpContent.EMPTY_LAST_CONTENT);
                              }
                            }
                          }

                          HttpResponse response = new ChunkedResponse(
                              request,
                              HttpResponseStatus.OK,
                              Arrays
                                  .<HttpContent>asList(
                                      new DefaultHttpContent(
                                          encodeString("This is a simple chunk\n", StandardCharsets.UTF_8)),
                                      new DefaultHttpContent(encodeString(request.uri(), StandardCharsets.UTF_8)),
                                      LastHttpContent.EMPTY_LAST_CONTENT)
                                  .iterator());

                          ctx.writeAndFlush(response);
                        }
                      });
            }
          });

      serverChannel = (ServerChannel) serverBootstrap.bind(localAddress).syncUninterruptibly().channel();
      BlockingQueue<FullHttpResponse> responses = new LinkedBlockingQueue<>();

      Bootstrap bootstrap =
          new Bootstrap().group(eventLoop).channel(LocalChannel.class).handler(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
              ch.pipeline()
                  .addLast(
                      new HttpClientCodec(),
                      new LoggingHandler("client", LogLevel.DEBUG),
                      new HttpObjectAggregator(81920),
                      new SimpleChannelInboundHandler<FullHttpResponse>() {
                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) throws Exception {
                          responses.add(msg.retainedDuplicate());
                        }
                      });
            }
          });

      Channel ch = bootstrap.connect(localAddress).syncUninterruptibly().channel();

      HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, GET, "/hello/world");

      LOG.debug("Sending request: {}", request);
      ch.writeAndFlush(request).syncUninterruptibly();

      FullHttpResponse response = responses.take();
      LOG.debug("Received response: {}", response);

      Assert.assertEquals(response.status(), HttpResponseStatus.OK);

      Assert.assertEquals(response.content().toString(StandardCharsets.UTF_8), "This is a simple chunk\n/hello/world");

      response.release();
    } finally {
      Optional.ofNullable(serverChannel).ifPresent(Channel::close);
      eventLoop.shutdownGracefully();
    }
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
