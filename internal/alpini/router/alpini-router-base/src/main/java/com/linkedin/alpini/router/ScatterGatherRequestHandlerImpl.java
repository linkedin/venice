package com.linkedin.alpini.router;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.concurrency.AsyncFutureListener;
import com.linkedin.alpini.base.concurrency.AsyncPromise;
import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.alpini.base.concurrency.NamedThreadFactory;
import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.alpini.base.concurrency.impl.CancelledAsyncFuture;
import com.linkedin.alpini.base.concurrency.impl.SuccessAsyncFuture;
import com.linkedin.alpini.base.misc.BasicRequest;
import com.linkedin.alpini.base.misc.ExceptionWithStatus;
import com.linkedin.alpini.base.misc.Headers;
import com.linkedin.alpini.base.misc.Http2TooManyStreamsException;
import com.linkedin.alpini.base.misc.MetricNames;
import com.linkedin.alpini.base.misc.Metrics;
import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.base.misc.TimeValue;
import com.linkedin.alpini.netty4.misc.Http2Utils;
import com.linkedin.alpini.router.api.HostHealthMonitor;
import com.linkedin.alpini.router.api.ResourcePath;
import com.linkedin.alpini.router.api.ResourcePathParser;
import com.linkedin.alpini.router.api.RouterException;
import com.linkedin.alpini.router.api.RouterTimeoutProcessor;
import com.linkedin.alpini.router.api.Scatter;
import com.linkedin.alpini.router.api.ScatterGatherHelper;
import com.linkedin.alpini.router.api.ScatterGatherRequest;
import com.linkedin.alpini.router.monitoring.ScatterGatherStats;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.concurrent.EventExecutor;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public abstract class ScatterGatherRequestHandlerImpl<H, P extends ResourcePath<K>, K, R, CHC, BHS extends BasicRequest, HR, HRS extends HttpResponseStatus, SCATTER_GATHER_HELPER extends ScatterGatherHelper<H, P, K, R, BHS, HR, HRS>>
    extends ScatterGatherRequestHandler<H, P, K, R> {
  /** Use a named thread factory for the cancel threads */
  private static final ThreadFactory CANCEL_FACTORY = new NamedThreadFactory("scatterGather-cancel");

  /** Limit the maximum number of cores which will be performing cancellation tasks */
  private static final Semaphore CANCEL_CONCURRENCY = new Semaphore(4); // MAX 4 cores

  /** To reduce contention in the worker queue, we have seperate worker queues for each io worker */
  private static final ThreadLocal<Executor> CANCEL_EXECUTOR = ThreadLocal.withInitial(() -> new Executor() {
    private final Executor _executor = Executors.newSingleThreadExecutor(CANCEL_FACTORY);

    @Override
    public void execute(@Nonnull Runnable command) {
      _executor.execute(() -> {
        CANCEL_CONCURRENCY.acquireUninterruptibly();
        try {
          command.run();
        } finally {
          CANCEL_CONCURRENCY.release();
        }
      });
    }
  });

  private final @Nonnull SCATTER_GATHER_HELPER _scatterGatherHelper;

  protected ScatterGatherRequestHandlerImpl(
      @Nonnull SCATTER_GATHER_HELPER scatterGatherHelper,
      @Nonnull TimeoutProcessor timeoutProcessor) {
    super(timeoutProcessor);
    _scatterGatherHelper = Objects.requireNonNull(scatterGatherHelper, "scatterGatherHelper");
  }

  protected ScatterGatherRequestHandlerImpl(
      @Nonnull SCATTER_GATHER_HELPER scatterGatherHelper,
      @Nonnull RouterTimeoutProcessor timeoutProcessor) {
    super(timeoutProcessor);
    _scatterGatherHelper = Objects.requireNonNull(scatterGatherHelper, "scatterGatherHelper");
  }

  public final @Nonnull SCATTER_GATHER_HELPER getScatterGatherHelper() {
    return _scatterGatherHelper;
  }

  public static void setMetric(Metrics metric, @Nonnull MetricNames metricName, @Nonnull Supplier<TimeValue> supplier) {
    if (metric != null) {
      metric.setMetric(metricName, supplier.get());
    }
  }

  protected abstract Runnable timeout(CHC ctx, Runnable task);

  protected Runnable timeout(CHC ctx, String msg, AsyncPromise<Void> timeoutFuture) {
    return timeout(ctx, msg, timeoutFuture, null);
  }

  protected <T> Runnable timeout(CHC ctx, String msg, AsyncPromise<T> timeoutFuture, T value) {
    return timeout(ctx, () -> {
      LOG.debug(msg);
      timeoutFuture.setSuccess(value);
    });
  }

  protected abstract Executor executor(CHC ctx);

  /**
   * Returns an {@linkplain Executor} that is suitable for passing into the {@linkplain CompletionStage}.{@literal *Async()}
   * methods.
   * @param ctx Channel handler context
   * @return Executor
   */
  private Executor stageExecutor(CHC ctx) {
    return stageExecutor(executor(ctx));
  }

  /**
   * Returns an {@linkplain Executor} that is suitable for passing into the {@linkplain CompletionStage}.{@literal *Async()}
   * methods. If the executor is an {@linkplain EventExecutor} instance, the returned executor would check if the current
   * thread is in the executor's event loop and execute immediately if it is.
   * @param executor executor
   * @return Executor
   */
  private Executor stageExecutor(Executor executor) {
    if (executor instanceof EventExecutor) {
      EventExecutor eventLoop = (EventExecutor) executor;
      return runnable -> {
        if (eventLoop.inEventLoop()) {
          runnable.run();
        } else {
          eventLoop.execute(runnable);
        }
      };
    }
    return executor;
  }

  private static void setFailure(AsyncPromise<?> promise, Throwable cause, String message) {
    if (!promise.setFailure(cause)) {
      LOG.warn("{}", message, cause);
    }
  }

  protected @Nonnull AsyncFuture<HR> handler(@Nonnull CHC ctx, @Nonnull BHS request) throws Exception {
    Metrics m = null;
    AsyncPromise<HR> promise = AsyncFuture.deferred(false);
    try {
      LOG.debug("[{}] handler", request.getRequestId());
      final Metrics m2 = (m = _scatterGatherHelper.initializeMetrics(request)); // SUPPRESS CHECKSTYLE InnerAssignment
      CompletableFuture.completedFuture(retainRequest(request))
          .thenCompose(r -> handler0(ctx, m2, r))
          .exceptionally(ex -> {
            try {
              return handlerException(request, m2, ex);
            } finally {
              // If an exception occurred, the reference count may not have been decremented by handler1()
              boolean released = releaseRequest(request);
              LOG.debug("after handlerException, released={}", released);
            }
          })
          .whenComplete((response, ex) -> {
            if (response != null && promise.setSuccess(response)) {
              return;
            }
            if (ex != null) {
              setFailure(promise, unwrapCompletion(ex), "unexpected exception");
              return;
            }
            if (response != null) {
              releaseResponse(response);
            }
            if (!promise.isDone()) {
              setFailure(promise, new IllegalStateException(), "unexpected exception");
            }
          });
    } catch (Throwable ex) {
      setFailure(promise, unwrapCompletion(ex), "unexpected exception");
    }
    return promise;
  }

  protected static Throwable unwrapCompletion(Throwable ex) {
    // we don't want this method ever returning null when a non-null throwable is passed in.
    while (ex instanceof CompletionException && ex.getCause() != null) {
      ex = ex.getCause();
    }
    return ex;
  }

  private HR handlerException(@Nonnull BHS request, Metrics m, @Nonnull Throwable ex) {
    LOG.debug("[{}] handlerException", request.getRequestId(), ex);
    return _scatterGatherHelper.aggregateResponse(
        request,
        m,
        Collections.singletonList(buildExceptionResponse(request, ex)),
        this::buildResponse);
  }

  private @Nonnull P parseResourceUri(@Nonnull String uri, @Nonnull BHS request) {
    try {
      return _scatterGatherHelper.parseResourceUri(uri, request);
    } catch (RouterException e) {
      LOG.debug("Failed to parse URI: {}", uri, e);
      throw new CompletionException(e);
    }
  }

  private @Nonnull CompletionStage<Scatter<H, P, K>> scatter(
      @Nonnull String requestMethod,
      @Nonnull P path,
      @Nonnull Headers headers,
      @Nonnull HostHealthMonitor<H> hostHealthMonitor,
      Metrics metrics,
      String initialHost) {
    try {
      return getScatterGatherHelper().scatter(requestMethod, path, headers, hostHealthMonitor, metrics, initialHost);
    } catch (RouterException e) {
      LOG.debug("Exception in scatter", e);
      throw new CompletionException(e);
    }
  }

  private @Nonnull CompletionStage<HR> handler0(@Nonnull CHC ctx, Metrics m, @Nonnull BHS request) {

    long beforeParseUri = Time.nanoTime();
    P path = parseResourceUri(request.getUri(), request);
    long afterParseUri = Time.nanoTime();
    if (m != null) {
      m.setPath(path);
    }

    final @Nonnull ScatterGatherStats.Delta stats =
        (_scatterGatherHelper.getScatterGatherStatsByPath(path)).new Delta();

    stats.incrementTotalRequestsReceived();

    long requestTimeout = _scatterGatherHelper.getRequestTimeout(request.getRequestHeaders());
    long requestDeadline = request.getRequestTimestamp() + requestTimeout;

    AsyncFuture<LongSupplier> longTailMilliseconds = _scatterGatherHelper.getLongTailRetryMilliseconds(path, request);

    long beforeScatter = Time.nanoTime();

    HostHealthMonitor<H> hostHealthMonitor;
    if (!_scatterGatherHelper.isReqRedirectionAllowedForQuery() && isQueryRequest(request.getUri())) {
      // Always declaring hosts as healthy essentially blocks redirection
      hostHealthMonitor = (ignore1, ignore2) -> true;
    } else {
      hostHealthMonitor = _scatterGatherHelper::isHostHealthy;
    }
    return scatter(request.getMethodName(), path, request.getRequestHeaders(), hostHealthMonitor, m, null)
        .thenComposeAsync(
            scatter -> handler1(
                ctx,
                m,
                request,
                requestDeadline,
                stats,
                longTailMilliseconds,
                afterParseUri,
                beforeParseUri,
                beforeScatter,
                scatter),
            stageExecutor(ctx));
  }

  private @Nonnull CompletionStage<HR> handler1(
      @Nonnull CHC ctx,
      Metrics m,
      @Nonnull BHS request,
      long requestDeadline,
      @Nonnull ScatterGatherStats.Delta stats,
      @Nonnull AsyncFuture<LongSupplier> longTailMilliseconds,
      long afterParseUri,
      long beforeParseUri,
      long beforeScatter,
      @Nonnull Scatter<H, P, K> scatter) {
    long afterScatter = Time.nanoTime();
    // TODO : Uncomment once we figure out why determining log level takes time.
    // LOG.debug("[{}] scatter {}", request.getRequestId(), scatter);

    setMetric(
        m,
        MetricNames.ROUTER_PARSE_URI,
        () -> new TimeValue(afterParseUri - beforeParseUri, TimeUnit.NANOSECONDS));
    setMetric(
        m,
        MetricNames.ROUTER_ROUTING_TIME,
        () -> new TimeValue(afterScatter - beforeScatter, TimeUnit.NANOSECONDS));

    boolean retryableRequest = !longTailMilliseconds.isDone() || longTailMilliseconds.isSuccess();

    long dispatchMinimumMillis = _scatterGatherHelper.getDispatchMinimumMillis();

    long requestTimeoutMillis = requestDeadline - Time.currentTimeMillis();
    long requestReceiveToHandle = Time.currentTimeMillis() - request.getRequestTimestamp();
    if (requestReceiveToHandle > 100) {
      LOG.debug(
          "unexpected lag from receive to handler: {}, {}, requestTimeoutMillis: {}",
          request.getMethodName(),
          requestReceiveToHandle,
          requestTimeoutMillis);
    }

    // If we have less than MINIMUM_SERVICE_MILLISECONDS, we do not bother forwarding the requests.
    Runnable timeoutCancel;
    AsyncPromise<Void> timeoutFuture;
    AsyncPromise<HRS> longTailTimeoutFuture;

    if (requestTimeoutMillis <= dispatchMinimumMillis) {
      LOG.debug(
          "requestTimeoutMillis={} dispatchMinimumMillis={}, requestDeadline={}",
          requestTimeoutMillis,
          dispatchMinimumMillis,
          requestDeadline);
      timeoutFuture = new SuccessAsyncFuture<>(null);
      longTailTimeoutFuture = CancelledAsyncFuture.getInstance();
      timeoutCancel = NOP;
    } else {
      timeoutFuture = AsyncFuture.deferred(true);
      longTailTimeoutFuture = AsyncFuture.deferred(true);
      RouterTimeoutProcessor.TimeoutFuture timeout = _timeoutProcessor.schedule(
          timeout(ctx, "request timeout", timeoutFuture),
          requestTimeoutMillis - dispatchMinimumMillis,
          TimeUnit.MILLISECONDS);
      timeoutCancel = timeout::cancel;
    }

    Executor contextExecutor = executor(ctx);
    List<AsyncFuture<List<HR>>> responseFutures =
        new ArrayList<>(scatter.getOnlineRequestCount() + scatter.getOfflineRequestCount());

    appendErrorsForOfflinePartitions(request, scatter, responseFutures);

    for (ScatterGatherRequest<H, K> part: scatter.getOnlineRequests()) {
      final P partPath = scatter.pathFor(part);
      AsyncPromise<H> hostFuture = AsyncFuture.deferred(false);

      AsyncPromise<List<HR>> responseFuture = AsyncFuture.deferred(false);
      responseFutures.add(responseFuture);

      AsyncPromise<HRS> retryFuture = retryableRequest && !timeoutFuture.isDone()
          ? AsyncFuture.deferred(false)
          : CancelledAsyncFuture.getInstance();
      if (AsyncFuture.getStatus(retryFuture) != AsyncFuture.Status.INFLIGHT || timeoutFuture.isDone()) {
        // FIXME: when reaching here and it timed out already, can we skip further dispatching?
        LOG.debug(
            "handler1 before dispatch(...): retryableRequest: {}, timeoutFuture: {}, retryFuture: {}",
            retryableRequest,
            AsyncFuture.getStatus(timeoutFuture),
            AsyncFuture.getStatus(retryFuture));
        // TODO: build a proper timeout response and skip dispatch
      }
      dispatch(
          scatter,
          part,
          partPath,
          request,
          hostFuture,
          responseFuture,
          retryFuture,
          timeoutFuture,
          contextExecutor);

      if (!retryFuture.isCancelled()) {
        // Prepare long-tail handling if either condition are met: (1) long-tail experiment decision are not made yet;
        // (2) long-tail experiment decision are made and the chosen experiment is not "None".
        if (!longTailMilliseconds.isDone()
            || (longTailMilliseconds.isSuccess() && longTailMilliseconds.getNow().getAsLong() >= 0)) {
          // in-flight retryFuture will be completed once longTailTimeoutFuture is done
          longTailTimeoutFuture.addListener(retryFuture);
        }

        // Ensure that the retryFuture is completed if the responseFuture is completed
        responseFuture.addListener(f -> {
          if (!retryFuture.isDone()) {
            retryFuture.setSuccess(statusOf(200));
          }
        });

        retryFuture.addListener(
            prepareRetry(
                hostFuture,
                partPath,
                request,
                scatter.getRole(),
                responseFuture,
                timeoutFuture,
                contextExecutor,
                _scatterGatherHelper::isHostHealthy,
                stats,
                m));
      }
    }

    stats.incrementFanoutRequestsSent(scatter.getOnlineRequestCount());

    // We are done sending out requests...
    // We can decrement the reference count obtained before the call to handler0()
    releaseRequest(request);

    return gatherResponses(
        ctx,
        request,
        responseFutures,
        longTailMilliseconds,
        m,
        stats,
        requestDeadline,
        timeoutCancel,
        longTailTimeoutFuture);
  }

  CompletableFuture<HR> gatherResponses(
      CHC ctx,
      @Nonnull BHS request,
      List<AsyncFuture<List<HR>>> responseFutures,
      AsyncFuture<LongSupplier> longTailMilliseconds,
      Metrics m,
      ScatterGatherStats.Delta stats,
      long requestDeadline,
      Runnable timeoutCancel,
      AsyncPromise<HRS> longTailTimeoutFuture) {
    LOG.debug("[{}] gatherResponses", request.getRequestId());

    // Wait for all of the responses to come back.
    // context.getTimer().touch(getClass(), "before response latch waits");
    AsyncFuture<List<HR>> gatheredResponses = AsyncFuture.collect(responseFutures, false);
    gatheredResponses
        .addListener(ignored -> longTailTimeoutFuture.setFailure(CancelledAsyncFuture.getInstance().getCause()));

    if (!gatheredResponses.isDone() && !longTailTimeoutFuture.isCancelled() && (!longTailMilliseconds.isDone()
        || (longTailMilliseconds.isSuccess() && longTailMilliseconds.getNow().getAsLong() > 0))) {
      long longTailMinimumMillis = _scatterGatherHelper.getLongTailMinimumMillis();

      longTailMilliseconds.addListener(future -> {
        if (!future.isSuccess() || gatheredResponses.isDone()) {
          // we would not schedule longTailTimeout if all responses are already done
          return;
        }

        long longTailDelay = future.getNow().getAsLong();
        if (longTailDelay < 0) {
          // We would only get here if the long tail future was slow to tell us "None". (e.g., Lix calculation was slow)
          return;
        }

        longTailDelay = Math.max(longTailDelay, longTailMinimumMillis);

        long longTailDeadline = request.getRequestTimestamp() + longTailDelay;
        if (longTailDeadline >= requestDeadline - _scatterGatherHelper.getDispatchMinimumMillis()) {
          // If the user requested deadline occurs before the long tail latency handling
          // would kick in, do nothing.
          return;
        }

        long scheduleDelay = Math.max(1L, longTailDeadline - Time.currentTimeMillis());

        RouterTimeoutProcessor.TimeoutFuture timeout = _timeoutProcessor.schedule(
            timeout(ctx, "long tail timeout", longTailTimeoutFuture, gatewayTimeout()),
            scheduleDelay,
            TimeUnit.MILLISECONDS);

        // Cancel the timeout if the request completed before it is fired.
        gatheredResponses.addListener(f -> CANCEL_EXECUTOR.get().execute(timeout::cancel));
      });
    }

    CompletableFuture<HR> response = new CompletableFuture<>();
    long arrivalNanoseconds = request.getRequestNanos();
    long responseWaitStartNanos = Time.nanoTime();

    // BHS req = retainRequest(request); -- request will have refCnt of 0 at this point!
    BHS req = request;
    gatheredResponses.whenCompleteAsync((responseList, throwable) -> {
      // LOG.debug("[{}] respond", req.getRequestId());
      try {
        CANCEL_EXECUTOR.get().execute(timeoutCancel);
        long now = Time.nanoTime();

        setMetric(
            m,
            MetricNames.ROUTER_SERVER_TIME,
            () -> new TimeValue(now - arrivalNanoseconds, TimeUnit.NANOSECONDS));
        setMetric(
            m,
            MetricNames.ROUTER_RESPONSE_WAIT_TIME,
            () -> new TimeValue(now - responseWaitStartNanos, TimeUnit.NANOSECONDS));

        if (response.isDone()) {
          LOG.debug("[{}] response discarded", req.getRequestId());
          releaseResponses(gatheredResponses.getNow());
        } else if (throwable == null) {
          responseComplete(response, _scatterGatherHelper.aggregateResponse(req, m, responseList, this::buildResponse));
        } else {
          responseComplete(response, handlerException(req, m, throwable));
        }
      } catch (Throwable ex) {
        if (response.isCompletedExceptionally()) {
          LOG.warn("unhandled double exception: {} {}", req.getMethodName(), req.getUri(), ex);
          releaseResponses(gatheredResponses.getNow());
          return;
        }
        if (response.isDone()) {
          // If have a successful response but some exception occurred elsewhere, we can assume that the
          // gathered responses are consolidated in the successful response.
          // If the response completed exceptionally, we do need to release the responses.
          LOG.warn("unhandled exception: {} {}", req.getMethodName(), req.getUri(), ex);
          if (!response.isCompletedExceptionally()) {
            return;
          }
        } else {
          responseComplete(response, handlerException(req, m, ex));
        }
        releaseResponses(gatheredResponses.getNow());
      } finally {
        // releaseRequest(req);
        stats.apply();
      }
    }, stageExecutor(ctx));

    return response;
  }

  private void releaseResponses(List<HR> responseList) {
    if (responseList != null) {
      responseList.forEach(this::releaseResponse);
    }
  }

  private void responseComplete(CompletableFuture<HR> promise, @Nonnull HR response) {
    if (!promise.complete(response)) {
      releaseResponse(response);
    }
  }

  /**
   * This method checks if the request is a query request
   * @param uri request URI
   * @return boolean returning true if request is a query request
   */
  private boolean isQueryRequest(String uri) {
    int query = uri.indexOf('?');
    if (query < 0) {
      return false;
    }
    if (uri.regionMatches(query, "?query=", 0, 7)) {
      return true;
    }
    if (uri.indexOf("&query=", query) > 0) {
      return true;
    }
    return false;
  }

  protected abstract HRS statusOf(int code);

  protected abstract HRS multiStatus();

  protected abstract HRS badRequest();

  protected abstract HRS gatewayTimeout();

  protected abstract HRS tooManyRequests();

  protected abstract HRS serviceUnavailable();

  protected abstract HRS internalServerError();

  protected abstract boolean isSuccessStatus(HRS status);

  protected abstract boolean isRequestRetriable(P path, R role, HRS status);

  protected abstract boolean isServiceUnavailable(HRS status);

  protected abstract String getReasonPhrase(HRS status);

  protected @Nonnull BHS prepareRequest(@Nonnull BHS value) {
    return retainRequest(value);
  }

  protected @Nonnull BHS retainRequest(@Nonnull BHS value) {
    return value;
  }

  protected boolean releaseRequest(BHS value) {
    return false;
  }

  protected boolean releaseResponse(HR value) {
    return false;
  }

  protected boolean isLastAttempt(HRS status) {
    return !gatewayTimeout().equals(status);
  }

  protected @Nonnull AsyncFutureListener<HRS> prepareRetry(
      @Nonnull AsyncFuture<H> hostFuture,
      @Nonnull P path,
      @Nonnull BHS requestRef,
      @Nonnull R role,
      @Nonnull AsyncPromise<List<HR>> responseFuture,
      @Nonnull AsyncFuture<Void> timeoutFuture,
      @Nonnull Executor contextExecutor,
      @Nonnull HostHealthMonitor<H> hostHealthMonitor,
      @Nonnull ScatterGatherStats.Delta stats,
      Metrics m) {
    BHS request = retainRequest(requestRef);
    Executor stageExecutor = stageExecutor(contextExecutor);
    return new AsyncFutureListener<HRS>() {
      @Override
      public void operationComplete(AsyncFuture<HRS> retryFuture) {
        CompletableFuture.completedFuture(retryFuture)
            .thenAcceptAsync(this::operationComplete0, contextExecutor)
            .exceptionally(ex -> {
              setFailure(responseFuture, ex, "unhandled exception");
              return null;
            });
      }

      private void operationComplete0(AsyncFuture<HRS> retryFuture) {
        // retryFuture is in terminal state
        if (responseFuture.isDone() || (retryFuture.isSuccess() && isSuccessStatus(retryFuture.getNow()))) {
          // timeout already occurred or response is set or retryFuture is set with success code
          releaseRequest(request);
          // TODO: if timeoutFuture.isDone() || !retryFuture.isSuccess(), it may be a good idea to populate the response
          // if !responseFuture.isDone()
          return;
        }
        try {
          prepareRetry(retryFuture);
        } catch (Throwable ex) {
          setFailure(responseFuture, ex, "Exception when trying to perform retry");
        }
      }

      private HRS extractResponseStatus(Throwable cause) {
        HRS internalServerError = internalServerError();
        if (cause != null) {
          // noinspection unchecked
          Class<HRS> responseStatusClass = (Class<HRS>) internalServerError.getClass();
          do {
            if (cause instanceof ExceptionWithStatus) {
              ExceptionWithStatus exceptionWithStatus = (ExceptionWithStatus) cause;
              return exceptionWithStatus.status(responseStatusClass).orElse(internalServerError);
            }
            cause = cause.getCause();
          } while (cause != null);
        }
        return internalServerError;
      }

      private void prepareRetry(AsyncFuture<HRS> retryFuture) {
        HRS retryStatus =
            retryFuture.isSuccess() ? retryFuture.getNow() : extractResponseStatus(retryFuture.getCause());
        boolean lastAttempt = isLastAttempt(retryStatus); // SN's response future shows gateway timeout
        LOG.debug("[{}] prepareRetry {} lastAttempt={}", request.getRequestId(), path.getLocation(), lastAttempt);

        // Indicates if the request could be retried
        boolean isRequestRetriable = isRequestRetriable(path, role, retryStatus);

        // Indicate the current request is a retry request
        path.setRetryRequest(retryStatus);

        HostHealthMonitor<H> healthMonitor =
            _scatterGatherHelper.isEnableRetryRequestAlwaysUseADifferentHost() ? (H host, String partName) ->
        /*
         * Here passes a modified version of {@link HostHealthMonitor}, which just filters out the selected host of
         * the regular request.
         */
        !(hostFuture.isSuccess() && host.equals(hostFuture.getNow())) && hostHealthMonitor.isHostHealthy(host, partName)
                : (H host, String partName) -> hostFuture.isSuccess() && host.equals(hostFuture.getNow())
                    || hostHealthMonitor.isHostHealthy(host, partName);

        String initialHost = hostFuture.isSuccess() ? hostFuture.getNow().toString() : null;
        LOG.debug("Prepare Retry for request with initial host: {}", initialHost);

        scatter(request.getMethodName(), path, request.getRequestHeaders(), healthMonitor, m, initialHost)
            .whenCompleteAsync((scatter, throwable) -> {
              if (throwable != null) {
                if (lastAttempt) {
                  setFailure(responseFuture, throwable, "error in scatter");
                }
                /*
                 * Scattering could fail in some use case, such as the scattering for retry request could be aborted because
                 * of various reasons, and when it happens, we still need to make sure the request object will be released.
                 */
                boolean released = releaseRequest(request);
                LOG.debug("Retry after scatter, released={}", released);
                return;
              }

              boolean cannotRetry = scatter.getOnlineRequestCount() == 0 || !isRequestRetriable;
              if (!cannotRetry && hostFuture.isSuccess()) {
                for (ScatterGatherRequest<H, K> part: scatter.getOnlineRequests()) {
                  if (part.getHosts().size() == 1 && part.getHosts().contains(hostFuture.getNow())) {
                    cannotRetry = true;
                    break;
                  }
                  part.removeHost(hostFuture.getNow());
                }
              }
              if (timeoutFuture.isDone() && _scatterGatherHelper.disableRetryOnTimeout()) {
                LOG.debug(
                    "cannot retry: timeoutFuture: {}, retryFuture: {}",
                    AsyncFuture.getStatus(timeoutFuture),
                    retryStatus);
                cannotRetry = true;
              }
              if (cannotRetry) {
                // In the case where the retry was initiated by a 429 Too Many Requests response,
                // we must send the response to the client so that the client knows that the server
                // was too busy and that we were unable to retry the operation.
                // GATEWAY_TIMEOUT means that the retry was initiated by the router timeout:
                // In that case, we expect the timeout code to populate the server response.
                CompletionStage<?> stage = COMPLETED;
                if (lastAttempt) {
                  List<HR> responses = new LinkedList<>();
                  for (ScatterGatherRequest<H, K> part: scatter.getOnlineRequests()) {
                    stage = stage.thenCompose(
                        ignore -> appendErrorForEveryKey(
                            request,
                            responses,
                            retryStatus,
                            getReasonPhrase(retryStatus),
                            retryFuture.getCause(),
                            scatter.getPathParser(),
                            part,
                            scatter.pathFor(part)));
                  }
                  for (ScatterGatherRequest<H, K> part: scatter.getOfflineRequests()) {
                    stage = stage.thenCompose(
                        ignore -> appendErrorForEveryKey(
                            request,
                            responses,
                            serviceUnavailable(),
                            getReasonPhrase(serviceUnavailable()),
                            null,
                            scatter.getPathParser(),
                            part,
                            scatter.pathFor(part)));
                  }
                  stage.whenComplete((ignore, ex) -> {
                    if (ex != null) {
                      setFailure(responseFuture, ex, "failure building response");
                    }
                    if (!responseFuture.setSuccess(responses)) {
                      releaseResponses(responses);
                    }
                  });
                }
                stage.whenComplete((ignore, ex) -> {
                  boolean released = releaseRequest(request);
                  LOG.debug("cannotRetry released={}", released);
                });
                return;
              }

              incrementTotalRetries(stats, retryStatus);
              stats.incrementTotalRetriedKeys(path.getPartitionKeys().size());

              if (HttpResponseStatus.TOO_MANY_REQUESTS.equals(retryStatus)) {
                LOG.info("Long tail retry on TOO_MANY_REQUESTS for initial request {}", request);
                stats.incrementTotalRetriesOn429();
              }

              List<AsyncFuture<List<HR>>> responseFutures =
                  new ArrayList<>(scatter.getOnlineRequestCount() + scatter.getOfflineRequestCount());
              try {
                appendErrorsForOfflinePartitions(request, scatter, responseFutures);

                for (ScatterGatherRequest<H, K> part: scatter.getOnlineRequests()) {
                  final P partPath = scatter.pathFor(part);
                  // Mark this as a retry request
                  partPath.setRetryRequest();

                  AsyncPromise<H> retryHostFuture = AsyncFuture.deferred(false);

                  AsyncPromise<List<HR>> retryResponseFuture = AsyncFuture.deferred(false);
                  responseFutures.add(retryResponseFuture);
                  // a retry dispatch would set its retryFuture to be CANCELLED, indicating no further retry upon
                  // failure
                  AsyncPromise<HRS> retryFutureNew = CancelledAsyncFuture.getInstance();

                  if (timeoutFuture.isDone()) {
                    // TODO: when reaching here it timed out already, probably shouldn't call dispatch
                    LOG.debug(
                        "retry before dispatch(...):, unexpected timeoutFuture: {}, retryFuture: {}",
                        AsyncFuture.getStatus(timeoutFuture),
                        AsyncFuture.getStatus(retryFutureNew));
                  }
                  dispatch(
                      scatter,
                      part,
                      partPath,
                      request,
                      retryHostFuture,
                      retryResponseFuture,
                      retryFutureNew,
                      timeoutFuture,
                      contextExecutor);
                }

              } finally {
                boolean released = releaseRequest(request);
                LOG.debug("Retry after dispatch, released={}", released);

                AsyncFuture.collect(responseFutures, false).whenCompleteAsync((responses, failure) -> {
                  if (failure != null) {
                    incrementTotalRetriesError(stats, retryStatus);
                    if (lastAttempt) {
                      setFailure(responseFuture, failure, "Retry failure");
                    }
                    return;
                  }

                  boolean allSuccess = !responses.isEmpty()
                      && responses.stream().allMatch(r -> isSuccessStatus(statusOf(getResponseCode(r))));

                  if (!allSuccess) {
                    incrementTotalRetriesError(stats, retryStatus);
                  }

                  if ((lastAttempt || allSuccess) && responseFuture.setSuccess(responses)) {
                    if (allSuccess) {
                      incrementTotalRetriesWinner(stats, retryStatus);
                    }
                    return;
                  }

                  long contentBytes =
                      responses.stream().mapToInt(ScatterGatherRequestHandlerImpl.this::getResponseReadable).sum();
                  releaseResponses(responses);
                  stats.incrementTotalRetriesDiscarded(contentBytes);
                  LOG.debug("Long tail response discarded, contentBytes={}", contentBytes);
                }, stageExecutor);
              }
            }, stageExecutor);
      }
    };
  }

  protected void incrementTotalRetries(ScatterGatherStats.Delta stats, HRS responseStatus) {
    stats.incrementTotalRetries();
    if (isServiceUnavailable(responseStatus)) {
      stats.incrementTotalRetriesOn503();
    }
  }

  protected void incrementTotalRetriesError(ScatterGatherStats.Delta stats, HRS responseStatus) {
    stats.incrementTotalRetriesError();
    if (isServiceUnavailable(responseStatus)) {
      stats.incrementTotalRetriesOn503Error();
    }
  }

  protected void incrementTotalRetriesWinner(ScatterGatherStats.Delta stats, HRS responseStatus) {
    stats.incrementTotalRetriesWinner();
    if (isServiceUnavailable(responseStatus)) {
      stats.incrementTotalRetriesOn503Winner();
    }
  }

  protected abstract int getResponseCode(HR response);

  protected abstract int getResponseReadable(HR response);

  protected abstract boolean hasErrorInStorageNodeResponse(HR response);

  protected abstract Headers getResponseHeaders(HR response);

  protected abstract void setKeepAlive(HR response, boolean keepAlive);

  protected abstract @Nonnull HR buildResponse(
      @Nonnull BHS request,
      Metrics metrics,
      @Nonnull List<HR> gatheredResponses);

  private static final AsyncFuture<?> COMPLETED = AsyncFuture.success(null);

  protected void appendErrorsForOfflinePartitions(
      @Nonnull BHS request,
      @Nonnull Scatter<H, P, K> scatter,
      @Nonnull List<AsyncFuture<List<HR>>> responseFutures) {
    if (scatter.getOfflineRequestCount() > 0) {
      CompletionStage<?> stage = COMPLETED;
      List<HR> responseList = new LinkedList<>();

      for (ScatterGatherRequest<H, K> part: scatter.getOfflineRequests()) {
        stage = stage.thenCompose(
            ignore -> appendErrorForEveryKey(
                request,
                responseList,
                serviceUnavailable(),
                "No replica is available for this partition",
                null,
                scatter.getPathParser(),
                part,
                scatter.pathFor(part)));
      }
      responseFutures.add(AsyncFuture.of(stage.thenApply(ignore -> responseList), false));
    }
  }

  protected CompletionStage<?> appendErrorForEveryKey(
      @Nonnull BHS request,
      @Nonnull List<HR> responses,
      @Nonnull HRS status,
      String contentMessage,
      Throwable ex,
      @Nonnull ResourcePathParser<P, K> pathParser,
      @Nonnull ScatterGatherRequest<H, K> part,
      @Nonnull P basePath) {
    LOG.debug("appendErrorForEveryKey");
    CompletionStage<?> complete = COMPLETED;
    R roles = _scatterGatherHelper.parseRoles(request.getMethodName(), request.getRequestHeaders());
    StringBuilder contentMsg = new StringBuilder(contentMessage);

    if (part.getPartitionKeys().isEmpty()) {
      appendError(request, responses, status, contentMsg.append(", RoutingPolicy=").append(roles).toString(), ex);
    } else {
      // For requests with keys, send an error for each key. TODO: Consider if we could rip all of that out?
      complete = complete.thenCompose(aVoid -> {
        List<CompletableFuture<String>> list = new ArrayList<>(part.getPartitionKeys().size());
        for (K partitionKey: part.getPartitionKeys()) {
          list.add(
              CompletableFuture.completedFuture(partitionKey)
                  .thenApply(key -> pathParser.substitutePartitionKey(basePath, key))
                  .thenCompose(
                      pathForThisKey -> _scatterGatherHelper
                          .findPartitionName(pathForThisKey.getResourceName(), partitionKey))
                  .exceptionally(e -> {
                    LOG.info("Exception in appendErrorForEveryKey, key={}", partitionKey, e);
                    return null;
                  }));
        }
        return CompletableFuture.allOf(list.toArray(new CompletableFuture[0])).thenAccept(aVoid2 -> {
          HashSet<String> distinctSet = new HashSet<>();
          for (CompletableFuture<String> partitionName: list) {
            partitionName.thenAccept(key -> {
              if (key != null) {
                distinctSet.add(key);
              }
            });
          }
          distinctSet.forEach(partitionName -> {
            contentMsg.append(", PartitionName=").append(partitionName);
          });
        });
      }).thenAccept(aVoid -> {
        appendError(request, responses, status, contentMsg.append(", RoutingPolicy=").append(roles).toString(), ex);
      });
    }
    return complete;
  }

  protected void appendError(
      @Nonnull BHS request,
      @Nonnull List<HR> responses,
      @Nonnull HRS status,
      String contentMessage,
      Throwable ex) {
    LOG.debug("appendError");
    ex = unwrapCompletion(ex);
    responses.add(buildErrorResponse(request, status, contentMessage, ex));
  }

  protected abstract boolean isTooLongFrameException(Throwable cause);

  protected @Nonnull HR buildExceptionResponse(@Nonnull BHS request, @Nonnull Throwable cause) {
    LOG.debug("[{}] handlerException", request.getRequestId(), cause);
    cause = unwrapCompletion(cause);

    HR response;
    boolean closeChannel;

    if (cause instanceof ExceptionWithStatus) {
      ExceptionWithStatus rex = (ExceptionWithStatus) cause;
      if (rex.code() >= 500) {
        LOG.warn("RouterException 5XX exception caught", rex);
      }
      response = buildErrorResponse(request, statusOf(rex.code()), rex.getMessage(), rex);
      closeChannel = rex instanceof RouterException && ((RouterException) rex).shouldCloseChannel();
    } else if (isTooLongFrameException(cause)) {
      // Send a 400 error and close the channel
      response = buildErrorResponse(request, badRequest(), cause.getMessage(), cause);
      closeChannel = true;
    } else if (cause instanceof URISyntaxException) {
      URISyntaxException uex = (URISyntaxException) cause;
      response = buildErrorResponse(
          request,
          badRequest(),
          "Bad request path (" + uex.getInput() + "). " + uex.getMessage(),
          uex);
      closeChannel = false;
    } else if (Http2Utils.isTooManyActiveStreamsError(cause)) {
      response = buildErrorResponse(request, serviceUnavailable(), null, Http2TooManyStreamsException.INSTANCE);
      // No need to close the client connection
      closeChannel = false;
    } else {
      // Send a 500 error and close the channel
      LOG.error(
          "Unexpected exception caught in ScatterGatherRequestHandler.exceptionCaught. Sending error and closing client channel. ",
          cause);
      response = buildErrorResponse(request, internalServerError(), null, cause);
      closeChannel = true;
    }

    if (closeChannel) {
      // Signal the client that we will be closing the connection. This will typically set the "Connection" header to
      // "Close" for HTTP 1.1.
      setKeepAlive(response, false);
    }

    return response;
  }

  protected abstract @Nonnull HR buildErrorResponse(
      @Nonnull BHS request,
      @Nonnull HRS status,
      String contentMessage,
      Throwable ex);

  protected final void dispatch(
      @Nonnull Scatter<H, P, K> scatter,
      @Nonnull ScatterGatherRequest<H, K> part,
      @Nonnull P path,
      @Nonnull BHS request,
      @Nonnull AsyncPromise<H> hostSelected,
      @Nonnull AsyncPromise<List<HR>> responseFuture,
      @Nonnull AsyncPromise<HRS> retryFuture,
      @Nonnull AsyncFuture<Void> timeoutFuture,
      @Nonnull Executor contextExecutor) {
    try {
      _scatterGatherHelper.dispatch(
          scatter,
          part,
          path,
          request,
          hostSelected,
          responseFuture,
          retryFuture,
          timeoutFuture,
          contextExecutor);
    } catch (Throwable ex) {
      if (!responseFuture.isDone()) {
        List<HR> response = Collections.singletonList(buildExceptionResponse(request, ex));
        if (responseFuture.setSuccess(response)) {
          return;
        }
        releaseResponses(response);
      }
      LOG.warn("uncaught exception: {} {}", request.getMethodName(), request.getUri(), ex);
    }
  }
}
