package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.concurrency.AsyncFutureListener;
import com.linkedin.alpini.base.concurrency.AsyncPromise;
import com.linkedin.alpini.base.misc.ExceptionUtil;
import com.linkedin.alpini.base.misc.ExceptionWithStatus;
import com.linkedin.alpini.base.misc.LeakDetect;
import com.linkedin.alpini.netty4.misc.BasicFullHttpResponse;
import com.linkedin.alpini.netty4.misc.BasicHttpRequest;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.SingleThreadEventLoop;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.util.AsciiString;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.Future;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.ref.PhantomReference;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
@ChannelHandler.Sharable
public class AsyncFullHttpRequestHandler extends ChannelInitializer<Channel> {
  private static final Logger LOG = LogManager.getLogger(AsyncFullHttpRequestHandler.class);

  private static final IllegalStateException REFERENCE_LOST =
      ExceptionUtil.withoutStackTrace(new IllegalStateException("Reference lost"));

  final static HttpResponseStatus SERVICE_SHUTDOWN =
      new HttpResponseStatus(HttpResponseStatus.SERVICE_UNAVAILABLE.code(), "Service Shutdown");

  public interface RequestHandler {
    @Nonnull
    AsyncFuture<FullHttpResponse> handler(@Nonnull ChannelHandlerContext ctx, @Nonnull FullHttpRequest request);
  }

  private final RequestHandler _handler;
  private final BooleanSupplier _shutdownFlag;
  private final BooleanSupplier _busyAutoReadDisable;

  public AsyncFullHttpRequestHandler(@Nonnull RequestHandler handler, @Nonnull BooleanSupplier shutdownFlag) {
    this(handler, shutdownFlag, Boolean.FALSE::booleanValue);
  }

  public AsyncFullHttpRequestHandler(
      @Nonnull RequestHandler handler,
      @Nonnull BooleanSupplier shutdownFlag,
      @Nonnull BooleanSupplier busyAutoReadDisable) {
    _handler = Objects.requireNonNull(handler, "handler");
    _shutdownFlag = Objects.requireNonNull(shutdownFlag, "shutdownFlag");
    _busyAutoReadDisable = Objects.requireNonNull(busyAutoReadDisable, "busyAutoReadDisable");
  }

  @Override
  protected void initChannel(Channel ch) throws Exception {
    ch.pipeline().replace(this, "async-full-http-request-handler", new Handler());
  }

  protected ChannelFuture writeAndFlushResponse(ChannelHandlerContext ctx, FullHttpResponse msg) {
    return ctx.writeAndFlush(msg);
  }

  private class Handler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private AsyncPromise<Void> _nextSignal = AsyncFuture.success(null);
    private boolean _shutdown;
    private final AtomicReference<PhantomReference<AsyncFutureListener<?>>> _phantom = new AtomicReference<>();

    private boolean isShutdown() {
      return _shutdown || _shutdownFlag.getAsBoolean();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
      // TODO : Uncomment once we fix logging
      // LOG.debug("channelRead0 id:{}, msg:{}", ctx.channel().id(), msg);

      AsyncPromise<Void> start = _nextSignal;
      AsyncPromise<Void> complete = _nextSignal = AsyncFuture.deferred(false); // SUPPRESS CHECKSTYLE InnerAssignment
      BasicHttpRequest request = new BasicHttpRequest(msg);

      if (isShutdown()) {
        _shutdown = true;
        HttpUtil.setKeepAlive(request, false);
      }

      AsyncFutureListener<FullHttpResponse> sendResponse = data -> {
        // TODO : Uncomment once we fix logging
        // LOG.debug("channelRead0 continue id:{}", ctx.channel().id());
        assert data.isSuccess();
        if (!ctx.channel().isOpen()) {
          int streamId = -1;
          try {
            if (ctx.channel() instanceof Http2StreamChannel) {
              streamId = ((Http2StreamChannel) ctx.channel()).stream().id();
            }
          } catch (Exception e) {
            // ignore
          }
          LOG.info(
              "Client closed connection before response was sent {} streamId {} ",
              ctx.channel().remoteAddress(),
              streamId);
          data.getNow().release();
          return;
        }
        FullHttpResponse response = data.getNow();
        boolean keepAlive = HttpUtil.isKeepAlive(request) && !isShutdown();
        HttpUtil.setKeepAlive(response, keepAlive);
        writeAndFlushResponse(ctx, response).addListener(writeFuture -> {
          if (!writeFuture.isSuccess()) {
            LOG.log(
                levelFor(ExceptionUtil.unwrapCompletion(writeFuture.cause())),
                "Error writing to channel {} {}",
                ctx.channel().id(),
                ctx.channel().remoteAddress(),
                writeFuture.cause());
            EventLoop eventLoop = ctx.channel().eventLoop();
            Runnable releaseTask = () -> {
              // This task must occur later on a clean stack because there may be code in
              // finally clauses which would expect to decrement the ref count.
              if (response.refCnt() > 0) {
                ReferenceCountUtil.safeRelease(response);
              }
            };
            if (eventLoop instanceof SingleThreadEventLoop) {
              ((SingleThreadEventLoop) eventLoop).executeAfterEventLoopIteration(releaseTask);
            } else {
              eventLoop.execute(releaseTask);
            }
          }
          if (!keepAlive && ctx.channel().isOpen()) {
            ctx.close().addListener(closeFuture -> {
              setPromise(complete, writeFuture);
            });
          } else {
            setPromise(complete, writeFuture);
          }
        });
      };

      AsyncPromise<FullHttpResponse> promise = AsyncFuture.<FullHttpResponse>deferred(false).addListener(data -> {
        // TODO : Uncomment once we fix logging
        // LOG.debug("channelRead0 wait id:{}", ctx.channel().id());
        start.addListener(ignored -> sendResponse.operationComplete(data));
      });

      try {
        if (_shutdown) {
          FullHttpResponse response = new BasicFullHttpResponse(request, SERVICE_SHUTDOWN);
          setSuccessOrRelease(promise, response);
        } else {
          AsyncFutureListener<FullHttpResponse> responseListener = response -> {
            FullHttpResponse fullHttpResponse;
            if (response.isSuccess()) {
              fullHttpResponse = response.getNow();
            } else {
              fullHttpResponse = buildErrorResponse(request, response.getCause());
            }
            setSuccessOrRelease(promise, fullHttpResponse);
          };

          PhantomReference<AsyncFutureListener<?>> phantom =
              LeakDetect.newReference(responseListener, () -> ctx.executor().execute(() -> {
                // Avoid constructing the exception if the promise is already done.
                if (!promise.isDone()) {
                  FullHttpResponse errorResponse = buildErrorResponse(request, REFERENCE_LOST);
                  setSuccessOrRelease(promise, errorResponse);
                }
              }));

          ChannelConfig config = ctx.channel().config();
          promise.addListener(future -> {
            phantom.clear();
            _phantom.compareAndSet(phantom, null);
            config.setAutoRead(true);
          });

          _phantom.lazySet(phantom);

          if (_busyAutoReadDisable.getAsBoolean()) {
            // we don't need to read more from the client until we have a response.
            config.setAutoRead(false);
          }

          // TODO: Uncomment once we figure out why determining log level takes more than 1ms
          // LOG.debug("channelRead0 call id:{}", ctx.channel().id());
          _handler.handler(ctx, msg).addListener(responseListener);
          // LOG.debug("channelRead0 rtn id:{}", ctx.channel().id());
        }
      } catch (Throwable ex) {
        if (!promise.isDone()) {
          FullHttpResponse errorResponse = buildErrorResponse(request, ex);
          setSuccessOrRelease(promise, errorResponse);
        } else {
          LOG.debug("Exception occurred after done", ex);
        }
      }
    }
  }

  private static Level levelFor(Throwable cause) {
    if (cause instanceof ClosedChannelException || cause instanceof TooLongFrameException) {
      return Level.DEBUG;
    }
    if (cause instanceof IOException && AsciiString.contains(cause.getMessage(), "Broken pipe")) {
      return Level.DEBUG;
    }
    return Level.WARN;
  }

  private static <T extends ReferenceCounted> void setSuccessOrRelease(AsyncPromise<T> promise, T value) {
    if (!promise.setSuccess(value)) {
      value.release();
    }
  }

  private static void setPromise(AsyncPromise<Void> promise, Future<?> future) {
    if (future.isSuccess()) {
      promise.setSuccess(null);
    } else {
      promise.setFailure(future.cause());
    }
  }

  protected @Nonnull HttpResponseStatus getResponseStatus(@Nonnull Throwable ex) {
    return ex instanceof ExceptionWithStatus
        ? ((ExceptionWithStatus) ex).status(HttpResponseStatus.class).orElse(HttpResponseStatus.INTERNAL_SERVER_ERROR)
        : HttpResponseStatus.INTERNAL_SERVER_ERROR;
  }

  protected @Nonnull FullHttpResponse buildErrorResponse(@Nonnull HttpRequest msg, @Nonnull Throwable ex) {
    return buildErrorResponse(msg, ex, getResponseStatus(ex));
  }

  protected @Nonnull FullHttpResponse buildErrorResponse(
      @Nonnull HttpRequest msg,
      @Nonnull Throwable ex,
      @Nonnull HttpResponseStatus responseStatus) {
    StringWriter stringWriter = new StringWriter();
    try (PrintWriter printWriter = new PrintWriter(stringWriter)) {
      printWriter.append("<html><head><title>")
          .append(responseStatus.reasonPhrase().toUpperCase())
          .append("</title></head><body><h1>")
          .append(responseStatus.reasonPhrase())
          .append("</h1><pre>");
      ex.printStackTrace(printWriter);
      printWriter.write("</pre></body></html>");
    }

    FullHttpResponse response = new BasicFullHttpResponse(
        msg,
        responseStatus,
        Unpooled.copiedBuffer(stringWriter.getBuffer(), StandardCharsets.UTF_8));
    HttpUtil.setContentLength(response, response.content().readableBytes());
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html");

    return response;
  }
}
