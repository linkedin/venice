package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.ByteBufAsciiString;
import com.linkedin.alpini.base.misc.HeaderUtils;
import com.linkedin.alpini.netty4.misc.FullHttpMultiPart;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;
import javax.mail.MessagingException;
import javax.mail.internet.InternetHeaders;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 3/16/18.
 */
@Test(groups = { "unit", "unit-leak", "leak" }, singleThreaded = true)
public final class TestHttpClientResponseHandler extends AbstractLeakDetect {
  private static final Logger LOG = LogManager.getLogger(TestHttpClientResponseHandler.class);

  @Test(invocationCount = 5, expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "message does not implement ResponseConsumer")
  public void testBadRequest() throws InterruptedException {
    EmbeddedChannel ch = new EmbeddedChannel(new HttpClientResponseHandler());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    FullHttpRequest request =
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/", POOLED_ALLOCATOR.buffer());
    try {
      ch.writeAndFlush(request).sync();
    } finally {
      request.release();
      ch.finishAndReleaseAll();
    }
  }

  @Test(invocationCount = 5, expectedExceptions = NoSuchElementException.class, expectedExceptionsMessageRegExp = "Received an unexpected message: DefaultFullHttpResponse.*")
  public void testUnexpectedResponse() throws InterruptedException {
    EmbeddedChannel ch = new EmbeddedChannel(new HttpClientResponseHandler());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    FullHttpResponse response =
        new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, POOLED_ALLOCATOR.buffer());
    try {
      ch.writeOneInbound(response).sync();
      Assert.fail();
    } finally {
      Assert.assertEquals(response.refCnt(), 0);
      ch.finishAndReleaseAll();
    }
  }

  @Test(invocationCount = 5)
  public void testBasicResponse() throws InterruptedException {
    EmbeddedChannel ch = new EmbeddedChannel(new HttpClientResponseHandler());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    try {
      AtomicReference<CompletableFuture<Object>> future = new AtomicReference<>();

      class Request extends DefaultFullHttpRequest implements HttpClientResponseHandler.ResponseConsumer {
        Request(HttpMethod method, String uri) {
          super(HttpVersion.HTTP_1_1, method, uri, POOLED_ALLOCATOR.buffer());
        }

        @Override
        public Consumer<Object> responseConsumer() {
          return object -> {
            if (object instanceof Throwable) {
              future.get().completeExceptionally((Throwable) object);
            } else {
              future.get().complete(ReferenceCountUtil.retain(object));
            }
          };
        }
      }

      for (int i = 5; i > 0; i--) {
        future.set(new CompletableFuture<>());
        Request request = new Request(HttpMethod.GET, "/");
        ch.writeAndFlush(request).sync();
        Assert.assertSame(ch.readOutbound(), request);
        Assert.assertTrue(request.release());

        Assert.assertFalse(future.get().isDone());

        FullHttpResponse response = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            HttpResponseStatus.OK,
            encodeString("Simple response", StandardCharsets.UTF_8));
        HttpUtil.setContentLength(response, response.content().readableBytes());
        ch.writeOneInbound(response);

        Assert.assertTrue(future.get().isDone());
        Assert.assertSame(future.get().getNow(null), response);
        Assert.assertTrue(response.release());
      }
    } finally {
      ch.finishAndReleaseAll();
    }
  }

  @Test(invocationCount = 5)
  public void testBasicResponseAutoRead() throws InterruptedException {
    EmbeddedChannel ch = new EmbeddedChannel(new HttpClientResponseHandler(Boolean.TRUE::booleanValue));
    ch.config().setAllocator(POOLED_ALLOCATOR);
    ch.config().setAutoRead(false);
    try {
      AtomicReference<CompletableFuture<Object>> future = new AtomicReference<>();

      class Request extends DefaultFullHttpRequest implements HttpClientResponseHandler.ResponseConsumer {
        Request(HttpMethod method, String uri) {
          super(HttpVersion.HTTP_1_1, method, uri, POOLED_ALLOCATOR.buffer());
        }

        @Override
        public Consumer<Object> responseConsumer() {
          return object -> {
            if (object instanceof Throwable) {
              future.get().completeExceptionally((Throwable) object);
            } else {
              future.get().complete(ReferenceCountUtil.retain(object));
            }
          };
        }
      }

      for (int i = 5; i > 0; i--) {
        future.set(new CompletableFuture<>());
        Request request = new Request(HttpMethod.GET, "/");
        ch.writeAndFlush(request).sync();
        Assert.assertSame(ch.readOutbound(), request);
        Assert.assertTrue(request.release());
        Assert.assertTrue(ch.config().isAutoRead());

        Assert.assertFalse(future.get().isDone());

        FullHttpResponse response = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            HttpResponseStatus.OK,
            encodeString("Simple response", StandardCharsets.UTF_8));
        HttpUtil.setContentLength(response, response.content().readableBytes());
        ch.writeOneInbound(response);

        Assert.assertTrue(future.get().isDone());
        Assert.assertSame(future.get().getNow(null), response);
        Assert.assertTrue(response.release());
        Assert.assertFalse(ch.config().isAutoRead());
      }
    } finally {
      ch.finishAndReleaseAll();
    }
  }

  @Test(invocationCount = 5)
  public void testMultipartResponse() throws InterruptedException, MessagingException, IOException {
    EmbeddedChannel ch = new EmbeddedChannel(
        new HttpResponseDecoder(),
        new HttpMultiPartContentDecoder(8192, 8192, 20480),
        new BasicHttpNonMultiPartAggregator(20480),
        new HttpClientResponseHandler());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    try {
      List<Object> responses = new LinkedList<>();

      class Request extends DefaultFullHttpRequest implements HttpClientResponseHandler.ResponseConsumer {
        Request(HttpMethod method, String uri) {
          super(HttpVersion.HTTP_1_1, method, uri, POOLED_ALLOCATOR.buffer());
        }

        @Override
        public Consumer<Object> responseConsumer() {
          return object -> responses.add(ReferenceCountUtil.retain(object));
        }
      }

      for (int i = 50; i > 0; i--) {
        Request request = new Request(HttpMethod.GET, "/");
        ch.writeAndFlush(request).sync();
        Assert.assertSame(ch.readOutbound(), request);
        Assert.assertTrue(request.release());

        Assert.assertTrue(responses.isEmpty());

        MimeMultipart multipart = new MimeMultipart();

        String part1 = "Hello world " + ThreadLocalRandom.current().nextLong();
        String part2 = "Hello world " + ThreadLocalRandom.current().nextLong();
        StringBuilder part3 = new StringBuilder("Hello world ");
        for (int limit = 2048 + ThreadLocalRandom.current().nextInt(10240); part3.length() < limit;) {

          // Throw in some deliberate fuzz which is nearly identical to a real boundary
          if (ThreadLocalRandom.current().nextInt(100) == 42) {
            part3.append(multipart.getContentType().replace("boundary=\"", "\r\n--"));
          }

          part3.append(ThreadLocalRandom.current().nextLong());
        }

        InternetHeaders headers1 = new InternetHeaders();
        headers1.setHeader("Content-Type", "text/plain");
        headers1.setHeader("Content-Length", Integer.toString(part1.length()));
        multipart.addBodyPart(new MimeBodyPart(headers1, part1.getBytes()));

        InternetHeaders headers2 = new InternetHeaders();
        headers2.setHeader("Content-Type", "text/plain");
        multipart.addBodyPart(new MimeBodyPart(headers2, part2.getBytes()));

        InternetHeaders headers3 = new InternetHeaders();
        headers3.setHeader("Content-Type", "text/plain");
        headers3.setHeader("Content-Length", Integer.toString(part3.length()));
        multipart.addBodyPart(new MimeBodyPart(headers3, part3.toString().getBytes()));

        ByteBuf head = POOLED_ALLOCATOR.buffer();
        ByteBuf body = POOLED_ALLOCATOR.buffer();

        if ((i & 1) == 1) {
          // Sometimes the storage node adds CR/LF to the start of the response content
          body.writeBytes("\r\n".getBytes());
        }

        multipart.writeTo(new ByteBufOutputStream(body));
        head.writeBytes("HTTP/1.0 200 OK\r\n".getBytes());
        head.writeBytes("Content-Type: ".getBytes());
        head.writeBytes(multipart.getContentType().getBytes());
        head.writeBytes("\r\n".getBytes());
        head.writeBytes("Content-Length: ".getBytes());
        head.writeBytes(Integer.toString(body.readableBytes()).getBytes());
        head.writeBytes("\r\n".getBytes());
        head.writeBytes("\r\n".getBytes());
        head.writeBytes(body);
        body.release();

        LOG.debug("Outbound content:\n{}", ByteBufUtil.prettyHexDump(head));
        ch.writeOneInbound(head.readBytes(Math.min(512, head.readableBytes())));
        Assert.assertFalse(responses.isEmpty());
        while (head.isReadable()) {
          ch.writeOneInbound(head.readBytes(Math.min(512, head.readableBytes())));
        }
        head.release();

        // ch.runPendingTasks();

        Assert.assertEquals(responses.size(), 5);
        Assert.assertTrue(responses.get(0) instanceof HttpResponse);
        Assert.assertTrue(responses.get(1) instanceof FullHttpMultiPart);
        Assert
            .assertEquals(((FullHttpMultiPart) responses.get(1)).content().toString(StandardCharsets.US_ASCII), part1);
        Assert.assertTrue(responses.get(2) instanceof FullHttpMultiPart);
        // This part has no Content-Length so will include the trailing CR/LF
        Assert.assertEquals(
            ((FullHttpMultiPart) responses.get(2)).content().toString(StandardCharsets.US_ASCII),
            part2 + "\r\n");
        Assert.assertTrue(responses.get(3) instanceof FullHttpMultiPart);
        Assert.assertEquals(
            ((FullHttpMultiPart) responses.get(3)).content().toString(StandardCharsets.US_ASCII),
            part3.toString());
        Assert.assertTrue(responses.get(4) instanceof LastHttpContent);

        responses.forEach(ReferenceCountUtil::release);
        responses.clear();
      }
    } finally {
      ch.finishAndReleaseAll();
    }
  }

  @Test(invocationCount = 5)
  public void testMultipartPassthroughResponse() throws InterruptedException, MessagingException, IOException {
    Set<String> skipDecodeBoundaries = new HashSet<>();

    EmbeddedChannel ch =
        new EmbeddedChannel(new HttpResponseDecoder(), new HttpMultiPartContentDecoder(8192, 8192, 20480) {
          @Override
          protected boolean checkUnwrapBoundary(Channel ch, String boundary) {
            return !skipDecodeBoundaries.contains(boundary);
          }
        }, new BasicHttpNonMultiPartAggregator(20480), new HttpClientResponseHandler());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    try {
      List<Object> responses = new LinkedList<>();

      class Request extends DefaultFullHttpRequest implements HttpClientResponseHandler.ResponseConsumer {
        Request(HttpMethod method, String uri) {
          super(HttpVersion.HTTP_1_1, method, uri, POOLED_ALLOCATOR.buffer());
        }

        @Override
        public Consumer<Object> responseConsumer() {
          return object -> responses.add(ReferenceCountUtil.retain(object));
        }
      }

      for (int i = 50; i > 0; i--) {
        Request request = new Request(HttpMethod.GET, "/");
        ch.writeAndFlush(request).sync();
        Assert.assertSame(ch.readOutbound(), request);
        Assert.assertTrue(request.release());

        Assert.assertTrue(responses.isEmpty());

        MimeMultipart multipart = new MimeMultipart();

        String part1 = "Hello world " + ThreadLocalRandom.current().nextLong();
        String part2 = "Hello world " + ThreadLocalRandom.current().nextLong();
        StringBuilder part3 = new StringBuilder("Hello world ");
        for (int limit = 2048 + ThreadLocalRandom.current().nextInt(10240); part3.length() < limit;) {

          // Throw in some deliberate fuzz which is nearly identical to a real boundary
          if (ThreadLocalRandom.current().nextInt(100) == 42) {
            part3.append(multipart.getContentType().replace("boundary=\"", "\r\n--"));
          }

          part3.append(ThreadLocalRandom.current().nextLong());
        }

        InternetHeaders headers1 = new InternetHeaders();
        headers1.setHeader("Content-Type", "text/plain");
        headers1.setHeader("Content-Length", Integer.toString(part1.length()));
        multipart.addBodyPart(new MimeBodyPart(headers1, part1.getBytes()));

        InternetHeaders headers2 = new InternetHeaders();
        headers2.setHeader("Content-Type", "text/plain");
        multipart.addBodyPart(new MimeBodyPart(headers2, part2.getBytes()));

        InternetHeaders headers3 = new InternetHeaders();
        headers3.setHeader("Content-Type", "text/plain");
        headers3.setHeader("Content-Length", Integer.toString(part3.length()));
        multipart.addBodyPart(new MimeBodyPart(headers3, part3.toString().getBytes()));

        ByteBuf head = POOLED_ALLOCATOR.buffer();
        ByteBuf body = POOLED_ALLOCATOR.buffer();

        if ((i & 1) == 1) {
          // Sometimes the storage node adds CR/LF to the start of the response content
          body.writeBytes("\r\n".getBytes());
        }

        multipart.writeTo(new ByteBufOutputStream(body));
        HeaderUtils.ContentType contentType = HeaderUtils.parseContentType(multipart.getContentType());
        final int contentLength = body.readableBytes();

        head.writeBytes("HTTP/1.0 200 OK\r\n".getBytes());
        head.writeBytes("Content-Type: ".getBytes());
        head.writeBytes(contentType.toString().getBytes());
        head.writeBytes("\r\n".getBytes());
        head.writeBytes("Content-Length: ".getBytes());
        head.writeBytes(Integer.toString(contentLength).getBytes());
        head.writeBytes("\r\n".getBytes());
        head.writeBytes("\r\n".getBytes());
        String bodyString = body.toString(StandardCharsets.ISO_8859_1);
        head.writeBytes(body);
        body.release();

        StreamSupport
            .stream(
                Spliterators
                    .spliteratorUnknownSize(contentType.parameters(), Spliterator.NONNULL | Spliterator.IMMUTABLE),
                false)
            .filter(param -> "boundary".equals(param.getKey()))
            .map(Map.Entry::getValue)
            .findFirst()
            .ifPresent(skipDecodeBoundaries::add);

        LOG.debug("Outbound content:\n{}", ByteBufUtil.prettyHexDump(head));
        ch.writeOneInbound(head.readBytes(Math.min(512, head.readableBytes())));
        Assert.assertFalse(responses.isEmpty());
        while (head.isReadable()) {
          ch.writeOneInbound(head.readBytes(Math.min(512, head.readableBytes())));
        }
        head.release();

        // ch.runPendingTasks();

        Iterator<Object> responsesIterator = responses.iterator();

        Assert.assertTrue(responsesIterator.next() instanceof HttpResponse);
        int responseContentLength = 0;
        StringBuilder responseContent = new StringBuilder();

        for (;;) {
          HttpContent content = (HttpContent) responsesIterator.next();
          responseContentLength += content.content().readableBytes();
          responseContent.append(
              ByteBufAsciiString
                  .slice(content.content(), content.content().readerIndex(), content.content().readableBytes()));
          if (content instanceof LastHttpContent) {
            break;
          }
        }

        Assert.assertFalse(responsesIterator.hasNext());
        Assert.assertEquals(responseContentLength, contentLength);
        Assert.assertEquals(responseContent.toString(), bodyString);

        responses.forEach(ReferenceCountUtil::release);
        responses.clear();
      }
    } finally {
      ch.finishAndReleaseAll();
    }
  }

  @Test(invocationCount = 5)
  public void testExceptionFired() throws InterruptedException {
    Exception expectedException = new IllegalStateException("Expected");
    EmbeddedChannel ch = new EmbeddedChannel(new SimpleChannelInboundHandler<FullHttpResponse>() {
      @Override
      protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) throws Exception {
        throw expectedException;
      }
    }, new HttpClientResponseHandler());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    try {
      CompletableFuture<Object> future = new CompletableFuture<>();

      class Request extends DefaultFullHttpRequest implements HttpClientResponseHandler.ResponseConsumer {
        Request(HttpMethod method, String uri) {
          super(HttpVersion.HTTP_1_1, method, uri, POOLED_ALLOCATOR.buffer());
        }

        @Override
        public Consumer<Object> responseConsumer() {
          return future::complete;
        }
      }

      Request request = new Request(HttpMethod.GET, "/");
      ch.writeAndFlush(request).sync();
      Assert.assertSame(ch.readOutbound(), request);
      Assert.assertTrue(request.release());

      Assert.assertFalse(future.isDone());

      FullHttpResponse response =
          new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, POOLED_ALLOCATOR.buffer());
      ch.writeOneInbound(response);

      Assert.assertTrue(future.isDone());
      Assert.assertSame(future.getNow(null), expectedException);
      Assert.assertFalse(ch.isOpen());

      Assert.assertEquals(response.refCnt(), 0);
    } finally {
      ch.finishAndReleaseAll();
    }
  }

  @Test(invocationCount = 5)
  public void testExceptionFiredClosed() throws InterruptedException {
    Exception[] closeException = { new ClosedChannelException() };
    EmbeddedChannel ch = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
      @Override
      public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        super.close(ctx, ctx.newPromise());
        // super.close(ctx, promise);
        Exception ex = closeException[0];
        closeException[0] = null;
        throw ex;
      }
    }, new HttpClientResponseHandler());
    ch.config().setAllocator(POOLED_ALLOCATOR);
    try {
      CompletableFuture<Object> future = new CompletableFuture<>();

      class Request extends DefaultFullHttpRequest implements HttpClientResponseHandler.ResponseConsumer {
        Request(HttpMethod method, String uri) {
          super(HttpVersion.HTTP_1_1, method, uri, POOLED_ALLOCATOR.buffer());
        }

        @Override
        public Consumer<Object> responseConsumer() {
          return obj -> {
            future.complete(obj);
            ch.close();
            throw new RuntimeException();
          };
        }
      }

      Request request = new Request(HttpMethod.GET, "/");
      ch.writeAndFlush(request).sync();
      Assert.assertSame(ch.readOutbound(), request);
      Assert.assertTrue(request.release());

      Assert.assertFalse(future.isDone());

      FullHttpResponse response =
          new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, POOLED_ALLOCATOR.buffer());
      ch.writeOneInbound(response);
      Assert.assertNull(closeException[0]);

      Assert.assertTrue(future.isDone());
      Assert.assertSame(future.getNow(null), response);
      Assert.assertFalse(ch.isOpen());

      Assert.assertEquals(response.refCnt(), 0);
    } finally {
      ch.finishAndReleaseAll();
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
