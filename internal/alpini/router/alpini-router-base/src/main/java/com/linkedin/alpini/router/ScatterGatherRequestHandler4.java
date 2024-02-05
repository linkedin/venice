package com.linkedin.alpini.router;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.concurrency.RunOnce;
import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.alpini.base.misc.ExceptionUtil;
import com.linkedin.alpini.base.misc.HeaderNames;
import com.linkedin.alpini.base.misc.HeaderUtils;
import com.linkedin.alpini.base.misc.Headers;
import com.linkedin.alpini.base.misc.ImmutableMapEntry;
import com.linkedin.alpini.base.misc.Metrics;
import com.linkedin.alpini.base.misc.Pair;
import com.linkedin.alpini.netty4.handlers.AsyncFullHttpRequestHandler;
import com.linkedin.alpini.netty4.misc.BasicFullHttpMultiPart;
import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.alpini.netty4.misc.BasicFullHttpResponse;
import com.linkedin.alpini.netty4.misc.BasicHeaders;
import com.linkedin.alpini.netty4.misc.ChunkedHttpResponse;
import com.linkedin.alpini.netty4.misc.MultipartHttpResponse;
import com.linkedin.alpini.router.api.ResourcePath;
import com.linkedin.alpini.router.api.RouterTimeoutProcessor;
import com.linkedin.alpini.router.api.ScatterGatherHelper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.AsciiString;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class ScatterGatherRequestHandler4<H, P extends ResourcePath<K>, K, R> extends
    ScatterGatherRequestHandlerImpl<H, P, K, R, ChannelHandlerContext, BasicFullHttpRequest, FullHttpResponse, HttpResponseStatus, ScatterGatherHelper<H, P, K, R, BasicFullHttpRequest, FullHttpResponse, HttpResponseStatus>>
    implements AsyncFullHttpRequestHandler.RequestHandler {
  public static final AsciiString X_STACKTRACE = AsciiString.of("text/x-stacktrace");

  public ScatterGatherRequestHandler4(
      @Nonnull ScatterGatherHelper<H, P, K, R, BasicFullHttpRequest, FullHttpResponse, HttpResponseStatus> scatterGatherHelper,
      @Nonnull RouterTimeoutProcessor timeoutProcessor,
      @Nonnull Executor executor) {
    super(scatterGatherHelper, timeoutProcessor);
  }

  @Deprecated
  public ScatterGatherRequestHandler4(
      @Nonnull ScatterGatherHelper<H, P, K, R, BasicFullHttpRequest, FullHttpResponse, HttpResponseStatus> scatterGatherHelper,
      @Nonnull TimeoutProcessor timeoutProcessor,
      @Nonnull Executor executor) {
    super(scatterGatherHelper, timeoutProcessor);
  }

  protected @Override @Nonnull BasicFullHttpRequest retainRequest(@Nonnull BasicFullHttpRequest request) {
    return request.retainedDuplicate();
  }

  protected @Override boolean releaseRequest(BasicFullHttpRequest request) {
    if (request != null && request.refCnt() > 0) {
      try {
        return request.release();
      } catch (IllegalReferenceCountException ex) {
        LOG.warn("releaseRequest {} : {}", request.getRequestId(), ex.getMessage());
      }
    }
    return true;
  }

  protected @Override boolean releaseResponse(FullHttpResponse response) {
    if (response != null && response.refCnt() > 0) {
      try {
        return response.release();
      } catch (IllegalReferenceCountException ex) {
        LOG.warn("releaseRequest {} : {}", response, ex.getMessage());
      }
    }
    return true;
  }

  @Nonnull
  @Override
  public AsyncFuture<FullHttpResponse> handler(@Nonnull ChannelHandlerContext ctx, @Nonnull FullHttpRequest request) {
    try {
      BasicFullHttpRequest fullHttpRequest = prepareRequest((BasicFullHttpRequest) request);
      LOG.debug("before handler refCnf={}", fullHttpRequest.refCnt());
      try {
        DecoderResult decoderResult = request.decoderResult();
        if (decoderResult == null || !decoderResult.isSuccess()) {
          return AsyncFuture.success(
              buildErrorResponse(
                  fullHttpRequest,
                  badRequest(),
                  "HTTP decoder error: " + decoderResult,
                  decoderResult == null
                      ? null
                      : ScatterGatherRequestHandlerImpl.unwrapCompletion(decoderResult.cause())));
        }
        return handler(ctx, fullHttpRequest);
      } finally {
        LOG.debug("after handler refCnf={}", fullHttpRequest.refCnt());
        releaseRequest(fullHttpRequest);
      }
    } catch (Throwable ex) {
      LOG.debug("Unhandled exception", ex);
      return AsyncFuture.failed(ex);
    }
  }

  @Override
  protected Runnable timeout(ChannelHandlerContext ctx, Runnable task) {
    return RunOnce.make(() -> ctx.executor().execute(task));
  }

  @Override
  protected Executor executor(ChannelHandlerContext ctx) {
    return ctx.executor();
  }

  @Override
  protected HttpResponseStatus statusOf(int code) {
    return HttpResponseStatus.valueOf(code);
  }

  @Override
  protected HttpResponseStatus multiStatus() {
    return HttpResponseStatus.MULTI_STATUS;
  }

  @Override
  protected HttpResponseStatus badRequest() {
    return HttpResponseStatus.BAD_REQUEST;
  }

  @Override
  protected HttpResponseStatus gatewayTimeout() {
    return HttpResponseStatus.GATEWAY_TIMEOUT;
  }

  @Override
  protected HttpResponseStatus tooManyRequests() {
    return HttpResponseStatus.TOO_MANY_REQUESTS;
  }

  @Override
  protected HttpResponseStatus serviceUnavailable() {
    return HttpResponseStatus.SERVICE_UNAVAILABLE;
  }

  @Override
  protected HttpResponseStatus internalServerError() {
    return HttpResponseStatus.INTERNAL_SERVER_ERROR;
  }

  @Override
  protected boolean isSuccessStatus(HttpResponseStatus httpResponseStatus) {
    return getScatterGatherHelper().isSuccessStatus(httpResponseStatus.code());
  }

  @Override
  protected boolean isRequestRetriable(@Nonnull P path, @Nonnull R role, HttpResponseStatus httpResponseStatus) {
    return getScatterGatherHelper().isRequestRetriable(path, role, httpResponseStatus);
  }

  @Override
  protected boolean isServiceUnavailable(HttpResponseStatus httpResponseStatus) {
    return serviceUnavailable().equals(httpResponseStatus);
  }

  @Override
  protected String getReasonPhrase(HttpResponseStatus httpResponseStatus) {
    return httpResponseStatus.reasonPhrase();
  }

  @Override
  protected int getResponseCode(FullHttpResponse response) {
    return response.status().code();
  }

  @Override
  protected int getResponseReadable(FullHttpResponse response) {
    return response.content().readableBytes();
  }

  @Override
  protected boolean hasErrorInStorageNodeResponse(FullHttpResponse response) {
    return (response.status().code() >= 500)
        || Boolean.parseBoolean(response.headers().getAsString(HeaderNames.X_ERROR_IN_RESPONSE));
  }

  @Override
  protected Headers getResponseHeaders(FullHttpResponse response) {
    if (response.trailingHeaders().isEmpty()) {
      return new BasicHeaders(response.headers());
    } else {
      return new BasicHeaders(response.trailingHeaders());
    }
  }

  @Override
  protected void setKeepAlive(FullHttpResponse response, boolean keepAlive) {
    HttpUtil.setKeepAlive(response, keepAlive);
  }

  @Nonnull
  @Override
  protected FullHttpResponse buildResponse(
      @Nonnull BasicFullHttpRequest request,
      Metrics metrics,
      @Nonnull List<FullHttpResponse> gatheredResponses) {

    if (gatheredResponses.isEmpty() || gatheredResponses.size() > 1 && gatheredResponses.stream()
        .anyMatch(response -> response.status().code() == HttpResponseStatus.NO_CONTENT.code())) {
      List<HttpHeaders> headers = new ArrayList<>(gatheredResponses.size());
      List<FullHttpResponse> remaining = gatheredResponses.stream().filter(response -> {
        if (response.status().code() == HttpResponseStatus.NO_CONTENT.code()) {
          headers.add(response.headers());
          return false;
        }
        return true;
      }).collect(Collectors.toList());
      if (remaining.isEmpty()) {
        FullHttpResponse message =
            new BasicFullHttpResponse(request, HttpResponseStatus.NO_CONTENT, Unpooled.EMPTY_BUFFER);

        getScatterGatherHelper().decorateResponse(getResponseHeaders(message), request.getRequestHeaders(), metrics);

        return message;
      }

      return buildResponse(request, metrics, remaining);
    }

    if (gatheredResponses.size() == 1) {
      FullHttpResponse message = gatheredResponses.get(0);

      getScatterGatherHelper().decorateResponse(getResponseHeaders(message), request.getRequestHeaders(), metrics);

      return message;

    } else {
      FullHttpResponse response = new MultipartResponse(request, multiStatus()) {
        @Override
        public void writeChunkedContent(ChannelHandlerContext ctx, Promise<LastHttpContent> writePromise)
            throws IOException {

          List<Pair<MultipartHttpResponse, Promise<LastHttpContent>>> multipartResponses = new LinkedList<>();
          List<Pair<ChunkedHttpResponse, Promise<LastHttpContent>>> chunkedResponses = new LinkedList<>();

          ChannelFuture future = null;
          for (FullHttpResponse r: gatheredResponses) {
            if (r instanceof ChunkedHttpResponse) {
              if (r instanceof MultipartHttpResponse) {
                multipartResponses
                    .add(Pair.make((MultipartHttpResponse) r, ImmediateEventExecutor.INSTANCE.newPromise()));
              } else {
                chunkedResponses.add(Pair.make((ChunkedHttpResponse) r, ImmediateEventExecutor.INSTANCE.newPromise()));
              }
            } else {
              BasicFullHttpMultiPart part = new BasicFullHttpMultiPart(this, r.content(), r.headers());
              hasErrorInStorageNodeResponse(r);
              if (!part.headers().contains(HeaderNames.X_MULTIPART_CONTENT_STATUS)) {
                part.setStatus(r.status());
              }
              future = ctx.write(part);
            }
          }
          if (future != null) {
            ctx.flush();
          } else {
            future = ctx.newSucceededFuture();
          }

          if (!multipartResponses.isEmpty()) {
            ChannelFuture f = future;
            ChannelPromise promise = ctx.newPromise();

            multipartResponses.forEach(p -> f.addListener(fp -> {
              if (fp.isSuccess()) {
                LOG.debug("writeChunkedContent {}", p.getFirst());
                p.getFirst().writeChunkedContent(ctx, p.getSecond());
              } else {
                promise.setFailure(fp.cause());
              }
            }));

            GenericFutureListener<Future<LastHttpContent>> listener = cf -> {
              if (cf.isSuccess()) {
                multipartResponses.stream().map(Pair::getSecond).filter(p -> !p.isDone()).findAny().orElseGet(() -> {
                  promise.setSuccess();
                  return null;
                });
              }
            };

            multipartResponses.stream().map(Pair::getSecond).forEach(p -> p.addListener(listener));

            future = promise;
          }

          if (!chunkedResponses.isEmpty()) {
            Promise<LastHttpContent> first = ImmediateEventExecutor.INSTANCE.newPromise();
            AtomicReference<Promise<LastHttpContent>> f = new AtomicReference<>(first);
            chunkedResponses.forEach(p -> f.getAndSet(p.getSecond()).addListener((Future<LastHttpContent> fp) -> {
              if (fp.isSuccess()) {
                LOG.debug("writeChunkedContent {}", p.getFirst());
                p.getFirst().writeChunkedContent(ctx, p.getSecond());
              } else {
                p.getSecond().setFailure(fp.cause());
              }
            }));

            if (f.get() != first) {
              future.addListener(fp -> {
                if (fp.isSuccess()) {
                  first.setSuccess(null);
                } else {
                  first.setFailure(fp.cause());
                }
              });
              ChannelPromise cp = ctx.newPromise();
              future = cp;
              f.get().addListener((Future<LastHttpContent> fp) -> {
                if (fp.isSuccess()) {
                  cp.setSuccess();
                } else {
                  cp.setFailure(fp.cause());
                }
              });
            }
          }

          future.addListener(f -> {
            if (f.isSuccess()) {
              LastHttpContent lastContent = new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER);
              getScatterGatherHelper().decorateResponse(
                  new BasicHeaders(lastContent.trailingHeaders()),
                  request.getRequestHeaders(),
                  metrics);

              writePromise.setSuccess(lastContent);
            } else {
              writePromise.setFailure(f.cause());
            }
          });
        }
      };

      response.headers()
          .set(
              HeaderNames.CONTENT_TYPE,
              HeaderUtils.buildContentType(
                  "multipart",
                  "mixed",
                  Collections.singleton(ImmutableMapEntry.make("boundary", request.getRequestId().toString()))));

      HttpUtil.setTransferEncodingChunked(response, true);

      return response;
    }
  }

  private abstract class MultipartResponse extends BasicFullHttpResponse implements MultipartHttpResponse {
    /**
     * Creates a new instance.
     *
     * @param request the request for which this response is responding
     * @param status  the status of this response
     */
    MultipartResponse(HttpRequest request, HttpResponseStatus status) {
      super(request, status, Unpooled.EMPTY_BUFFER);
    }
  }

  @Override
  protected boolean isTooLongFrameException(Throwable cause) {
    return cause instanceof TooLongFrameException;
  }

  @Nonnull
  @Override
  protected FullHttpResponse buildErrorResponse(
      @Nonnull BasicFullHttpRequest request,
      @Nonnull HttpResponseStatus status,
      String contentMessage,
      Throwable ex) {
    boolean returnStackTrace = false;
    ByteBuf contentByteBuf;
    if (getScatterGatherHelper().isEnableStackTraceResponseForException() && ex != null) {
      contentByteBuf = Unpooled.wrappedBuffer(ExceptionUtil.getStackTrace(ex).getBytes(StandardCharsets.UTF_8));
      returnStackTrace = true;
    } else {
      contentByteBuf = Unpooled.EMPTY_BUFFER;
    }
    FullHttpResponse response = new BasicFullHttpResponse(request, status, contentByteBuf);

    if (ex != null) {
      response.headers().set(HeaderNames.X_ERROR_CLASS, ex.getClass().getName());
      if (contentMessage == null && ex.getMessage() != null) {
        response.headers().set(HeaderNames.X_ERROR_MESSAGE, HeaderUtils.cleanHeaderValue(ex.getMessage()));
      }
      if (ex.getCause() != null && ex.getCause() != ex) {
        response.headers().set(HeaderNames.X_ERROR_CAUSE_CLASS, ex.getCause().getClass().getName());
        if (ex.getCause().getMessage() != null) {
          response.headers()
              .set(HeaderNames.X_ERROR_CAUSE_MESSAGE, HeaderUtils.cleanHeaderValue(ex.getCause().getMessage()));
        }
      }
    }

    response.headers().set(HeaderNames.X_MULTIPART_CONTENT_STATUS, HeaderUtils.cleanHeaderValue(status.toString()));
    if (returnStackTrace) {
      response.headers().set(HeaderNames.CONTENT_TYPE, X_STACKTRACE);
    }

    if (contentMessage != null) {
      response.headers().set(HeaderNames.X_ERROR_MESSAGE, HeaderUtils.cleanHeaderValue(contentMessage));
    }

    HttpUtil.setContentLength(response, contentByteBuf.readableBytes());

    return response;
  }
}
