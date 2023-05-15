package com.linkedin.alpini.router;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.concurrency.AsyncFutureListener;
import com.linkedin.alpini.base.concurrency.AsyncPromise;
import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.alpini.base.misc.BasicRequest;
import com.linkedin.alpini.base.misc.HeaderUtils;
import com.linkedin.alpini.base.misc.Headers;
import com.linkedin.alpini.base.misc.Metrics;
import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.base.registry.ResourceRegistry;
import com.linkedin.alpini.router.api.HostFinder;
import com.linkedin.alpini.router.api.HostHealthMonitor;
import com.linkedin.alpini.router.api.PartitionDispatchHandler;
import com.linkedin.alpini.router.api.PartitionFinder;
import com.linkedin.alpini.router.api.ResourcePath;
import com.linkedin.alpini.router.api.RoleFinder;
import com.linkedin.alpini.router.api.RouterException;
import com.linkedin.alpini.router.api.RouterTimeoutProcessor;
import com.linkedin.alpini.router.api.ScatterGatherHelper;
import com.linkedin.alpini.router.monitoring.ScatterGatherStats;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.IRetryAnalyzer;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 11/16/17.
 */
public class TestScatterGatherRequestHandlerImpl {
  static final Logger LOG = LogManager.getLogger(TestScatterGatherRequestHandlerImpl.class);

  @BeforeClass(groups = "unit")
  public void beforeClass() {
    // org.apache.log4j.BasicConfigurator.configure();
    // org.apache.log4j.LogManager.getRootLogger().setLevel(org.apache.log4j.Level.DEBUG);
  }

  @AfterMethod(groups = "unit")
  public void afterMethod() throws InterruptedException {
    Time.sleep(200L);
  }

  private interface Request extends BasicRequest {
  }

  private interface Response {
    Status status();

    int readableBytes();

    Headers headers();

    void setKeepAlive(boolean keepAlive);
  }

  private enum Status {
    OK(200), MULTI_STATUS(207), BAD_REQUEST(400), NOT_FOUND(404), TOO_BUSY(429), INTERNAL_SERVER_ERROR(500),
    GATEWAY_TIMEOUT(502), SERVICE_UNAVAILABLE(503);

    final int _code;

    Status(int code) {
      _code = code;
    }
  }

  private interface Context {
  }

  private static class TooLongFrameException extends IOException {
  }

  private interface ResponseBuilder {
    Response buildResponse(@Nonnull Request request, Metrics metrics, @Nonnull List<Response> gatheredResponses);
  }

  private interface ErrorBuilder {
    Response buildErrorResponse(
        @Nonnull Request request,
        @Nonnull Status status,
        String contentMessage,
        Throwable ex,
        @Nonnull Map<String, String> errorHeaders);
  }

  private <H, P extends ResourcePath<K>, K, R, HELPER extends ScatterGatherHelper<H, P, K, R, Request, Response, Status>> ScatterGatherRequestHandlerImpl<H, P, K, R, Context, Request, Response, Status, HELPER> buildTestHandler(
      @Nonnull HELPER helper,
      @Nonnull TimeoutProcessor timeoutProcessor,
      @Nonnull ResponseBuilder responseBuilder,
      @Nonnull ErrorBuilder errorBuilder) {

    class TestHandler extends ScatterGatherRequestHandlerImpl<H, P, K, R, Context, Request, Response, Status, HELPER> {
      private TestHandler(@Nonnull HELPER scatterGatherHelper, @Nonnull RouterTimeoutProcessor timeoutProcessor) {
        super(scatterGatherHelper, timeoutProcessor);
      }

      @Override
      protected Runnable timeout(Context ctx, Runnable task) {
        return task;
      }

      @Override
      protected Executor executor(Context ctx) {
        return Runnable::run;
      }

      @Override
      protected Status statusOf(int code) {
        return Stream.of(Status.values())
            .filter(s -> s._code == code)
            .findFirst()
            .orElseThrow(IllegalArgumentException::new);
      }

      @Override
      protected Status multiStatus() {
        return Status.MULTI_STATUS;
      }

      @Override
      protected Status badRequest() {
        return Status.BAD_REQUEST;
      }

      @Override
      protected Status gatewayTimeout() {
        return Status.GATEWAY_TIMEOUT;
      }

      @Override
      protected Status tooManyRequests() {
        return Status.TOO_BUSY;
      }

      @Override
      protected Status serviceUnavailable() {
        return Status.SERVICE_UNAVAILABLE;
      }

      @Override
      protected Status internalServerError() {
        return Status.INTERNAL_SERVER_ERROR;
      }

      @Override
      protected boolean isSuccessStatus(Status status) {
        return status._code >= Status.OK._code && status._code < Status.INTERNAL_SERVER_ERROR._code
            && status._code != 429;
      }

      @Override
      protected boolean isRequestRetriable(@Nonnull P path, @Nonnull R role, @Nonnull Status status) {
        return getScatterGatherHelper().isRequestRetriable(path, role, status);
      }

      @Override
      protected boolean isServiceUnavailable(Status status) {
        return serviceUnavailable().equals(status);
      }

      @Override
      protected String getReasonPhrase(Status status) {
        return status.name();
      }

      @Override
      protected int getResponseCode(Response response) {
        return response.status()._code;
      }

      @Override
      protected int getResponseReadable(Response response) {
        return response.readableBytes();
      }

      @Override
      protected boolean hasErrorInStorageNodeResponse(Response response) {
        return response.status()._code >= Status.INTERNAL_SERVER_ERROR._code;
      }

      @Override
      protected Headers getResponseHeaders(Response response) {
        return response.headers();
      }

      @Override
      protected void setKeepAlive(Response response, boolean keepAlive) {
        response.setKeepAlive(keepAlive);
      }

      @Nonnull
      @Override
      protected Response buildResponse(
          @Nonnull Request request,
          Metrics metrics,
          @Nonnull List<Response> gatheredResponses) {
        return responseBuilder.buildResponse(request, metrics, gatheredResponses);
      }

      @Override
      protected boolean isTooLongFrameException(Throwable cause) {
        return cause instanceof TooLongFrameException;
      }

      @Nonnull
      @Override
      protected Response buildErrorResponse(
          @Nonnull Request request,
          @Nonnull Status status,
          String contentMessage,
          Throwable ex,
          @Nonnull Map<String, String> errorHeaders) {
        return errorBuilder.buildErrorResponse(request, status, contentMessage, ex, errorHeaders);
      }
    }

    return new TestHandler(helper, RouterTimeoutProcessor.adapt(timeoutProcessor));
  }

  @Test(groups = "unit")
  public void testException404InParseResourceUri() throws Exception {
    ResourceRegistry registry = new ResourceRegistry();
    TimeoutProcessor timeoutProcessor = new TimeoutProcessor(registry);

    ResponseBuilder responseBuilder = (request, metrics, gatheredResponses) -> {
      if (gatheredResponses.size() == 1) {
        return gatheredResponses.iterator().next();
      }
      throw new UnsupportedOperationException();
    };

    ErrorBuilder errorBuilder = (request, status, contentMessage, ex, errorHeaders) -> {
      Response response = mock(Response.class);
      Mockito.when(response.status()).thenReturn(status);
      return response;
    };

    Context ctx = mock(Context.class);
    Request testRequest = mock(Request.class);
    Mockito.when(testRequest.getUri()).thenReturn("/foo");
    Mockito.when(testRequest.getRequestId()).thenReturn(HeaderUtils.randomWeakUUID());

    AsyncFuture<Response> handlerResponseFuture = buildTestHandler(
        ScatterGatherHelper.builder()
            .roleFinder(new RoleFinderImpl())
            .pathParser(new FailPathParserImpl())
            .partitionFinder(new PartitionFinderImpl())
            .hostFinder(new HostFinderImpl())
            .dispatchHandler(
                (PartitionDispatchHandler<InetSocketAddress, Path, String, Request, Response, Status>) (
                    scatter,
                    part,
                    path,
                    request,
                    hostSelected,
                    responseFuture,
                    retryFuture,
                    timeoutFuture,
                    contextExecutor) -> Assert.fail("Should not get here"))
            .build(),
        timeoutProcessor,
        responseBuilder,
        errorBuilder).handler(ctx, testRequest);

    Assert.assertTrue(handlerResponseFuture.isSuccess());
    Assert.assertSame(handlerResponseFuture.getNow().status(), Status.NOT_FOUND);

    registry.shutdown();
    registry.waitForShutdown();
  }

  private AsyncFutureListener setupTestRetryFailure(Path mockRetryPath, AsyncPromise mockResponseFuture)
      throws Exception {
    ScatterGatherHelper mockScatterGatherHelper = mock(ScatterGatherHelper.class);
    CompletableFuture exceptionalFuture = new CompletableFuture();
    exceptionalFuture.completeExceptionally(new RuntimeException());
    doReturn(exceptionalFuture).when(mockScatterGatherHelper).scatter(any(), any(), any(), any(), any(), any());
    return buildTestHandler(
        mockScatterGatherHelper,
        mock(TimeoutProcessor.class),
        mock(ResponseBuilder.class),
        mock(ErrorBuilder.class)).prepareRetry(
            mock(AsyncFuture.class),
            mockRetryPath,
            mock(TestScatterGatherRequestHandlerImpl.Request.class),
            mock(Object.class),
            mockResponseFuture,
            mock(AsyncFuture.class),
            Runnable::run,
            mock(HostHealthMonitor.class),
            mock(ScatterGatherStats.Delta.class),
            mock(Metrics.class));
  }

  @Test(groups = "unit")
  public void testRetryFailure() throws Exception {
    TestScatterGatherRequestHandlerImpl.Path mockRetryPath = mock(TestScatterGatherRequestHandlerImpl.Path.class);
    AsyncPromise mockResponseFuture = mock(AsyncPromise.class);
    setupTestRetryFailure(mockRetryPath, mockResponseFuture)
        .operationComplete(AsyncFuture.success(Status.GATEWAY_TIMEOUT));
    verify(mockRetryPath).setRetryRequest();
    verify(mockResponseFuture, never()).setSuccess(any());
  }

  @Test(groups = "unit")
  public void testRetryFailureByRuntimeException() throws Exception {
    TestScatterGatherRequestHandlerImpl.Path mockRetryPath = mock(TestScatterGatherRequestHandlerImpl.Path.class);
    AsyncPromise mockResponseFuture = mock(AsyncPromise.class);
    setupTestRetryFailure(mockRetryPath, mockResponseFuture)
        .operationComplete(AsyncFuture.failed(new NullPointerException()));
    verify(mockRetryPath).setRetryRequest();
    verify(mockResponseFuture, never()).setSuccess(any());
  }

  @Test(groups = "unit")
  public void testRetryFailureByExceptionWithStatus() throws Exception {
    TestScatterGatherRequestHandlerImpl.Path mockRetryPath = mock(TestScatterGatherRequestHandlerImpl.Path.class);
    AsyncPromise mockResponseFuture = mock(AsyncPromise.class);
    setupTestRetryFailure(mockRetryPath, mockResponseFuture).operationComplete(
        AsyncFuture.failed(
            new RouterException(
                Status.class,
                Status.GATEWAY_TIMEOUT,
                Status.GATEWAY_TIMEOUT._code,
                Status.GATEWAY_TIMEOUT.name(),
                false)));
    verify(mockRetryPath).setRetryRequest();
    verify(mockResponseFuture, never()).setSuccess(any());
  }

  @Test(groups = "unit")
  public void testPrepareRetryOn503() throws Exception {
    TestScatterGatherRequestHandlerImpl.Path mockRetryPath = mock(TestScatterGatherRequestHandlerImpl.Path.class);
    AsyncPromise hostFuture = mock(AsyncPromise.class);
    Object mockObject = mock(Object.class);
    ScatterGatherHelper mockScatterGatherHelper = mock(ScatterGatherHelper.class);
    CompletableFuture exceptionalFuture = new CompletableFuture();
    exceptionalFuture.completeExceptionally(new RuntimeException());
    doReturn(exceptionalFuture).when(mockScatterGatherHelper).scatter(any(), any(), any(), any(), any(), any());
    AsyncPromise mockResponseFuture = mock(AsyncPromise.class);
    buildTestHandler(
        mockScatterGatherHelper,
        mock(TimeoutProcessor.class),
        mock(ResponseBuilder.class),
        mock(ErrorBuilder.class))
            .prepareRetry(
                hostFuture,
                mockRetryPath,
                mock(TestScatterGatherRequestHandlerImpl.Request.class),
                mockObject,
                mockResponseFuture,
                mock(AsyncFuture.class),
                Runnable::run,
                mock(HostHealthMonitor.class),
                mock(ScatterGatherStats.Delta.class),
                mock(Metrics.class))
            .operationComplete(AsyncFuture.success(Status.SERVICE_UNAVAILABLE));

    // hostFuture should be set as we retry the request
    verify(hostFuture).isSuccess();
    verify(mockRetryPath).setRetryRequest();
    verify(mockResponseFuture, never()).setSuccess(any());
  }

  @Test(groups = "unit")
  public void testDoubleFailure() throws Exception {
    ResponseBuilder responseBuilder = (request, metrics, responses) -> {
      if (responses.size() == 1) {
        return responses.iterator().next();
      }
      throw new UnsupportedOperationException();
    };

    ErrorBuilder errorBuilder = (request, status, contentMessage, ex, errorHeaders) -> {
      Response response = mock(Response.class);
      Mockito.when(response.status()).thenReturn(status);
      return response;
    };

    AtomicInteger attemptCount = new AtomicInteger(0);
    PartitionDispatchHandler dispatchHandler =
        (scatter, part, path, request, hostSelected, responseFuture, retryFuture, timeoutFuture, contextExecutor) -> {
          attemptCount.incrementAndGet();
          hostSelected.setSuccess(part.getHosts().get(0));
          if (retryFuture.setSuccess(Status.SERVICE_UNAVAILABLE)) {
            return;
          }
          Response response = mock(Response.class);
          Mockito.when(response.status()).thenReturn(Status.INTERNAL_SERVER_ERROR);
          Mockito.when(response.headers()).thenReturn(mock(Headers.class));
          responseFuture.setSuccess(Collections.singletonList(response));
        };

    ScatterGatherHelper scatterGather = ScatterGatherHelper.builder()
        .roleFinder(new RoleFinderImpl())
        .pathParser(new DummyParser())
        .partitionFinder(new PartitionFinderImpl())
        .hostFinder(new HostFinder2Impl())
        .longTailRetrySupplier((resourceName, methodName) -> AsyncFuture.success(() -> 100L))
        .dispatchHandler(dispatchHandler)
        .build();

    ResourceRegistry registry = new ResourceRegistry();
    TimeoutProcessor timeoutProcessor = new TimeoutProcessor(registry);
    ScatterGatherRequestHandlerImpl requestHandler =
        buildTestHandler(scatterGather, timeoutProcessor, responseBuilder, errorBuilder);

    Context ctx = mock(Context.class);
    Request request = mock(Request.class);
    Mockito.when(request.getUri()).thenReturn("/foo");
    Mockito.when(request.getMethodName()).thenReturn("GET");
    Mockito.when(request.getRequestHeaders()).thenReturn(mock(Headers.class));
    Mockito.when(request.getRequestId()).thenReturn(HeaderUtils.randomWeakUUID());
    Mockito.when(request.getRequestTimestamp()).thenReturn(Time.currentTimeMillis());

    AsyncFuture<Response> responseFuture = requestHandler.handler(ctx, request);
    Assert.assertSame(responseFuture.get(5, TimeUnit.SECONDS).status(), Status.INTERNAL_SERVER_ERROR);
    Assert.assertSame(attemptCount.get(), 2);

    registry.shutdown();
    registry.waitForShutdown();
  }

  @Test(groups = "unit")
  public void testExceptionInErrorBuilder() throws Exception {
    ResourceRegistry registry = new ResourceRegistry();
    TimeoutProcessor timeoutProcessor = new TimeoutProcessor(registry);

    ResponseBuilder responseBuilder = (request, metrics, gatheredResponses) -> {
      if (gatheredResponses.size() == 1) {
        return gatheredResponses.iterator().next();
      }
      throw new UnsupportedOperationException();
    };

    NullPointerException npe = new NullPointerException("Foo");

    ErrorBuilder errorBuilder = (request, status, contentMessage, ex, errorHeaders) -> {
      throw npe;
    };

    Context ctx = mock(Context.class);
    Request testRequest = mock(Request.class);
    Mockito.when(testRequest.getUri()).thenReturn("/foo");
    Mockito.when(testRequest.getRequestId()).thenReturn(HeaderUtils.randomWeakUUID());

    AsyncFuture<Response> handlerResponseFuture = buildTestHandler(
        ScatterGatherHelper.builder()
            .roleFinder(new RoleFinderImpl())
            .pathParser(new FailPathParserImpl())
            .partitionFinder(new PartitionFinderImpl())
            .hostFinder(new HostFinderImpl())
            .dispatchHandler(
                (PartitionDispatchHandler<InetSocketAddress, Path, String, Request, Response, Status>) (
                    scatter,
                    part,
                    path,
                    request,
                    hostSelected,
                    responseFuture,
                    retryFuture,
                    timeoutFuture,
                    contextExecutor) -> Assert.fail("Should not get here"))
            .build(),
        timeoutProcessor,
        responseBuilder,
        errorBuilder).handler(ctx, testRequest);

    Assert.assertFalse(handlerResponseFuture.isSuccess());
    Assert.assertSame(handlerResponseFuture.getCause(), npe);

    registry.shutdown();
    registry.waitForShutdown();
  }

  @Test(groups = "unit")
  public void testException503InScatter() throws Exception {
    ResourceRegistry registry = new ResourceRegistry();
    TimeoutProcessor timeoutProcessor = new TimeoutProcessor(registry);

    ResponseBuilder responseBuilder = (request, metrics, gatheredResponses) -> {
      if (gatheredResponses.size() == 1) {
        return gatheredResponses.iterator().next();
      }
      throw new UnsupportedOperationException();
    };

    ErrorBuilder errorBuilder = (request, status, contentMessage, ex, errorHeaders) -> {
      Response response = mock(Response.class);
      Mockito.when(response.status()).thenReturn(status);
      return response;
    };

    Context ctx = mock(Context.class);
    Request testRequest = mock(Request.class);
    Mockito.when(testRequest.getUri()).thenReturn("/foo");
    Mockito.when(testRequest.getMethodName()).thenReturn("GET");
    Mockito.when(testRequest.getRequestHeaders()).thenReturn(mock(Headers.class));
    Mockito.when(testRequest.getRequestId()).thenReturn(HeaderUtils.randomWeakUUID());

    AsyncFuture<Response> handlerResponseFuture = buildTestHandler(
        ScatterGatherHelper.builder()
            .roleFinder(new RoleFinderImpl())
            .pathParser(new DummyParser())
            .partitionFinder(new PartitionFinderImpl())
            .hostFinder(new FailHostFinder())
            .dispatchHandler(
                (PartitionDispatchHandler<InetSocketAddress, Path, String, Request, Response, Status>) (
                    scatter,
                    part,
                    path,
                    request,
                    hostSelected,
                    responseFuture,
                    retryFuture,
                    timeoutFuture,
                    contextExecutor) -> Assert.fail("Should not get here"))
            .build(),
        timeoutProcessor,
        responseBuilder,
        errorBuilder).handler(ctx, testRequest);

    Assert.assertTrue(handlerResponseFuture.isSuccess());
    Assert.assertSame(handlerResponseFuture.getNow().status(), Status.SERVICE_UNAVAILABLE);
  }

  @Test(groups = "unit")
  public void testException500InDispatch() throws Exception {
    ResourceRegistry registry = new ResourceRegistry();
    TimeoutProcessor timeoutProcessor = new TimeoutProcessor(registry);

    ResponseBuilder responseBuilder = (request, metrics, gatheredResponses) -> {
      if (gatheredResponses.size() == 1) {
        return gatheredResponses.iterator().next();
      }
      throw new UnsupportedOperationException();
    };

    class ExpectedException extends IllegalStateException {
      private ExpectedException(String msg) {
        super(msg);
      }
    }

    ErrorBuilder errorBuilder = (request, status, contentMessage, ex, errorHeaders) -> {
      Response response = mock(Response.class);
      Mockito.when(response.status()).thenReturn(status);
      Assert.assertTrue(ex instanceof ExpectedException);
      Assert.assertEquals(ex.getMessage(), "Should get here");
      return response;
    };

    Context ctx = mock(Context.class);
    Request testRequest = mock(Request.class);
    Mockito.when(testRequest.getUri()).thenReturn("/foo");
    Mockito.when(testRequest.getMethodName()).thenReturn("GET");
    Mockito.when(testRequest.getRequestHeaders()).thenReturn(mock(Headers.class));
    Mockito.when(testRequest.getRequestId()).thenReturn(HeaderUtils.randomWeakUUID());

    AsyncFuture<Response> handlerResponseFuture = buildTestHandler(
        ScatterGatherHelper.builder()
            .roleFinder(new RoleFinderImpl())
            .pathParser(new DummyParser())
            .partitionFinder(new PartitionFinderImpl())
            .hostFinder(new HostFinderImpl())
            .metricsProvider(request -> new Metrics())
            .responseMetrics(response -> new Metrics())
            .dispatchHandler(
                (PartitionDispatchHandler<InetSocketAddress, Path, String, Request, Response, Status>) (
                    scatter,
                    part,
                    path,
                    request,
                    hostSelected,
                    responseFuture,
                    retryFuture,
                    timeoutFuture,
                    contextExecutor) -> {
                  throw new ExpectedException("Should get here");
                })
            .build(),
        timeoutProcessor,
        responseBuilder,
        errorBuilder).handler(ctx, testRequest);

    Assert.assertTrue(handlerResponseFuture.isSuccess());
    Assert.assertSame(handlerResponseFuture.getNow().status(), Status.INTERNAL_SERVER_ERROR);

    registry.shutdown();
    registry.waitForShutdown();
  }

  @Test(groups = "unit")
  public void testTimeoutInDispatch() throws Exception {
    ResourceRegistry registry = new ResourceRegistry();
    TimeoutProcessor timeoutProcessor = new TimeoutProcessor(registry);
    List<AsyncPromise<List<Response>>> holder = new LinkedList<>();

    ResponseBuilder responseBuilder = (request, metrics, gatheredResponses) -> {
      if (gatheredResponses.size() == 1) {
        return gatheredResponses.iterator().next();
      }
      throw new UnsupportedOperationException();
    };

    class ExpectedException extends IllegalStateException {
      private ExpectedException(String msg) {
        super(msg);
      }
    }

    ErrorBuilder errorBuilder = (request, status, contentMessage, ex, errorHeaders) -> {
      Response response = mock(Response.class);
      Mockito.when(response.status()).thenReturn(status);
      Assert.assertTrue(ex instanceof ExpectedException);
      Assert.assertEquals(ex.getMessage(), "Should get here");
      return response;
    };

    Context ctx = mock(Context.class);
    Request testRequest = mock(Request.class);
    Mockito.when(testRequest.getUri()).thenReturn("/foo");
    Mockito.when(testRequest.getMethodName()).thenReturn("GET");
    Mockito.when(testRequest.getRequestHeaders()).thenReturn(mock(Headers.class));
    Mockito.when(testRequest.getRequestId()).thenReturn(HeaderUtils.randomWeakUUID());
    Mockito.when(testRequest.getRequestTimestamp()).thenReturn(Time.currentTimeMillis());

    Response timeoutResponse = mock(Response.class);
    Mockito.when(timeoutResponse.status()).thenReturn(Status.GATEWAY_TIMEOUT);
    Mockito.when(timeoutResponse.headers()).thenReturn(mock(Headers.class));

    CountDownLatch timeoutLatch = new CountDownLatch(1);

    AsyncFuture<Response> handlerResponseFuture = buildTestHandler(
        ScatterGatherHelper.builder()
            .roleFinder(new RoleFinderImpl())
            .pathParser(new DummyParser())
            .partitionFinder(new PartitionFinderImpl())
            .hostFinder(new HostFinderImpl())
            .metricsProvider(request -> new Metrics())
            .responseMetrics(response -> new Metrics())
            .requestTimeout(header -> 1000L)
            .longTailRetrySupplier((resourceName, methodName) -> AsyncFuture.success(() -> 100L))
            .dispatchHandler(
                (PartitionDispatchHandler<InetSocketAddress, Path, String, Request, Response, Status>) (
                    scatter,
                    part,
                    path,
                    request,
                    hostSelected,
                    responseFuture,
                    retryFuture,
                    timeoutFuture,
                    contextExecutor) -> {
                  hostSelected.setSuccess(part.getHosts().get(0));
                  timeoutFuture.addListener(timeout -> {
                    if (timeout.isSuccess()) {
                      timeoutLatch.countDown();
                    }
                  });
                  holder.add(responseFuture);
                })
            .build(),
        timeoutProcessor,
        responseBuilder,
        errorBuilder).handler(ctx, testRequest);

    if (timeoutLatch.await(5, TimeUnit.SECONDS)) {
      holder.forEach(future -> future.setSuccess(Collections.singletonList(timeoutResponse)));
    }

    Assert.assertTrue(handlerResponseFuture.isSuccess());
    Assert.assertSame(handlerResponseFuture.getNow().status(), Status.GATEWAY_TIMEOUT);

    registry.shutdown();
    registry.waitForShutdown();
  }

  @Test(groups = "unit")
  public void test200InDispatch() throws Exception {
    ResourceRegistry registry = new ResourceRegistry();
    TimeoutProcessor timeoutProcessor = new TimeoutProcessor(registry);

    ResponseBuilder responseBuilder = (request, metrics, gatheredResponses) -> {
      if (gatheredResponses.size() == 1) {
        return gatheredResponses.iterator().next();
      }
      throw new UnsupportedOperationException();
    };

    ErrorBuilder errorBuilder = (request, status, contentMessage, ex, errorHeaders) -> {
      Assert.fail("Should not get here");
      return mock(Response.class);
    };

    Context ctx = mock(Context.class);
    Request testRequest = mock(Request.class);
    Mockito.when(testRequest.getUri()).thenReturn("/foo");
    Mockito.when(testRequest.getMethodName()).thenReturn("GET");
    Mockito.when(testRequest.getRequestHeaders()).thenReturn(mock(Headers.class));
    Mockito.when(testRequest.getRequestId()).thenReturn(HeaderUtils.randomWeakUUID());

    Response testResponse = mock(Response.class);
    Mockito.when(testResponse.status()).thenReturn(Status.OK);
    Mockito.when(testResponse.headers()).thenReturn(mock(Headers.class));

    AsyncFuture<Response> handlerResponseFuture = buildTestHandler(
        ScatterGatherHelper.builder()
            .roleFinder(new RoleFinderImpl())
            .pathParser(new DummyParser())
            .partitionFinder(new PartitionFinderImpl())
            .hostFinder(new HostFinderImpl())
            .metricsProvider(request -> new Metrics())
            .responseMetrics(response -> new Metrics())
            .dispatchHandler(
                (PartitionDispatchHandler<InetSocketAddress, Path, String, Request, Response, Status>) (
                    scatter,
                    part,
                    path,
                    request,
                    hostSelected,
                    responseFuture,
                    retryFuture,
                    timeoutFuture,
                    contextExecutor) -> {
                  hostSelected.setSuccess(part.getHosts().get(0));
                  responseFuture.setSuccess(Collections.singletonList(testResponse));
                })
            .build(),
        timeoutProcessor,
        responseBuilder,
        errorBuilder).handler(ctx, testRequest);

    Assert.assertTrue(handlerResponseFuture.isSuccess());
    Assert.assertSame(handlerResponseFuture.getNow().status(), Status.OK);
  }

  @Test(groups = "unit", retryAnalyzer = Retry.class)
  public void test200LongTailCannotRetry() throws Exception {
    ResourceRegistry registry = new ResourceRegistry();
    TimeoutProcessor timeoutProcessor = new TimeoutProcessor(registry);

    ResponseBuilder responseBuilder = (request, metrics, gatheredResponses) -> {
      if (gatheredResponses.size() == 1) {
        return gatheredResponses.iterator().next();
      }
      throw new UnsupportedOperationException();
    };

    ErrorBuilder errorBuilder = (request, status, contentMessage, ex, errorHeaders) -> {
      Assert.fail("Should not get here");
      return mock(Response.class);
    };

    Context ctx = mock(Context.class);
    Request testRequest = mock(Request.class);
    Mockito.when(testRequest.getUri()).thenReturn("/foo");
    Mockito.when(testRequest.getMethodName()).thenReturn("GET");
    Mockito.when(testRequest.getRequestHeaders()).thenReturn(mock(Headers.class));
    Mockito.when(testRequest.getRequestId()).thenReturn(HeaderUtils.randomWeakUUID());
    Mockito.when(testRequest.getRequestNanos()).thenReturn(Time.nanoTime());
    Mockito.when(testRequest.getRequestTimestamp()).thenReturn(Time.currentTimeMillis());

    Response testResponse1 = mock(Response.class);
    Mockito.when(testResponse1.status()).thenReturn(Status.NOT_FOUND);
    Mockito.when(testResponse1.headers()).thenReturn(mock(Headers.class));

    List<AsyncPromise<List<Response>>> responsePromiseList = new LinkedList<>();
    CountDownLatch latch = new CountDownLatch(2);

    AsyncFuture<Response> handlerResponseFuture = buildTestHandler(
        ScatterGatherHelper.builder()
            .roleFinder(new RoleFinderImpl())
            .pathParser(new DummyParser())
            .partitionFinder(new PartitionFinderImpl())
            .hostFinder(new HostFinderImpl())
            .metricsProvider(request -> new Metrics())
            .responseMetrics(response -> new Metrics())
            .requestTimeout(header -> 10000L)
            .longTailRetrySupplier((resourceName, methodName) -> AsyncFuture.success(() -> 100L))
            .dispatchHandler(
                (PartitionDispatchHandler<InetSocketAddress, Path, String, Request, Response, Status>) (
                    scatter,
                    part,
                    path,
                    request,
                    hostSelected,
                    responseFuture,
                    retryFuture,
                    timeoutFuture,
                    contextExecutor) -> {
                  hostSelected.setSuccess(part.getHosts().get(0));
                  responsePromiseList.add(responseFuture);
                  latch.countDown();
                })
            .build(),
        timeoutProcessor,
        responseBuilder,
        errorBuilder).handler(ctx, testRequest);

    Assert.assertEquals(responsePromiseList.size(), 1);
    Assert.assertFalse(Time.await(latch, 1000L, TimeUnit.MILLISECONDS), "latch");
    Assert.assertEquals(responsePromiseList.size(), 1);

    responsePromiseList.get(0).setSuccess(Collections.singletonList(testResponse1));

    Assert.assertTrue(
        Time.await((Time.Awaitable) handlerResponseFuture, 5L, TimeUnit.MILLISECONDS),
        "handlerResponseFuture");

    Assert.assertTrue(handlerResponseFuture.isSuccess());
    Assert.assertSame(handlerResponseFuture.getNow().status(), Status.NOT_FOUND);

    registry.shutdown();
    registry.waitForShutdown();
  }

  // test needs retry because it can fail due to timing.
  @Test(groups = "unit", retryAnalyzer = Retry.class)
  public void test200LongTailDispatch() throws Exception {
    ResourceRegistry registry = new ResourceRegistry();
    TimeoutProcessor timeoutProcessor = new TimeoutProcessor(registry);

    ResponseBuilder responseBuilder = (request, metrics, gatheredResponses) -> {
      if (gatheredResponses.size() == 1) {
        return gatheredResponses.iterator().next();
      }
      throw new UnsupportedOperationException();
    };

    ErrorBuilder errorBuilder = (request, status, contentMessage, ex, errorHeaders) -> {
      Assert.fail("Should not get here");
      return mock(Response.class);
    };

    Context ctx = mock(Context.class);
    Request testRequest = mock(Request.class);
    Mockito.when(testRequest.getUri()).thenReturn("/foo");
    Mockito.when(testRequest.getMethodName()).thenReturn("GET");
    Mockito.when(testRequest.getRequestHeaders()).thenReturn(mock(Headers.class));
    Mockito.when(testRequest.getRequestId()).thenReturn(HeaderUtils.randomWeakUUID());
    Mockito.when(testRequest.getRequestNanos()).thenReturn(Time.nanoTime());
    Mockito.when(testRequest.getRequestTimestamp()).thenReturn(Time.currentTimeMillis());

    Response testResponse1 = mock(Response.class);
    Mockito.when(testResponse1.status()).thenReturn(Status.NOT_FOUND);
    Mockito.when(testResponse1.headers()).thenReturn(mock(Headers.class));

    Response testResponse2 = mock(Response.class);
    Mockito.when(testResponse2.status()).thenReturn(Status.OK);
    Mockito.when(testResponse2.headers()).thenReturn(mock(Headers.class));

    List<AsyncPromise<List<Response>>> responsePromiseList = new LinkedList<>();
    CountDownLatch latch = new CountDownLatch(2);

    AsyncFuture<Response> handlerResponseFuture = buildTestHandler(
        ScatterGatherHelper.builder()
            .roleFinder(new RoleFinderImpl())
            .pathParser(new DummyParser())
            .partitionFinder(new PartitionFinderImpl())
            .hostFinder(new HostFinder2Impl())
            .metricsProvider(request -> new Metrics())
            .responseMetrics(response -> new Metrics())
            .requestTimeout(header -> 1000L)
            .longTailRetrySupplier((resourceName, methodName) -> AsyncFuture.success(() -> 100L))
            .dispatchHandler(
                (PartitionDispatchHandler<InetSocketAddress, Path, String, Request, Response, Status>) (
                    scatter,
                    part,
                    path,
                    request,
                    hostSelected,
                    responseFuture,
                    retryFuture,
                    timeoutFuture,
                    contextExecutor) -> {
                  hostSelected.setSuccess(part.getHosts().get(0));
                  responsePromiseList.add(responseFuture);
                  latch.countDown();
                })
            .build(),
        timeoutProcessor,
        responseBuilder,
        errorBuilder).handler(ctx, testRequest);

    Assert.assertEquals(responsePromiseList.size(), 1);
    Assert.assertTrue(Time.await(latch, 120L, TimeUnit.MILLISECONDS), "latch");
    Assert.assertEquals(responsePromiseList.size(), 2);

    responsePromiseList.get(1).setSuccess(Collections.singletonList(testResponse2));

    Assert.assertTrue(
        Time.await((Time.Awaitable) handlerResponseFuture, 5L, TimeUnit.MILLISECONDS),
        "handlerResponseFuture");

    Assert.assertTrue(handlerResponseFuture.isSuccess());
    Assert.assertSame(handlerResponseFuture.getNow().status(), Status.OK);

    responsePromiseList.get(0).setSuccess(Collections.singletonList(testResponse1));
    Time.sleep(50);

    registry.shutdown();
    registry.waitForShutdown();
  }

  // test needs retry because it can fail due to timing.
  @Test(groups = "unit", retryAnalyzer = Retry.class)
  public void test429LongTailRetry() throws Exception {
    ResourceRegistry registry = new ResourceRegistry();
    TimeoutProcessor timeoutProcessor = new TimeoutProcessor(registry);

    ResponseBuilder responseBuilder = (request, metrics, gatheredResponses) -> {
      if (gatheredResponses.size() == 1) {
        return gatheredResponses.iterator().next();
      }
      throw new UnsupportedOperationException();
    };

    ErrorBuilder errorBuilder = (request, status, contentMessage, ex, errorHeaders) -> {
      Assert.fail("Should not get here");
      return mock(Response.class);
    };

    Context ctx = mock(Context.class);
    Request testRequest = mock(Request.class);
    Mockito.when(testRequest.getUri()).thenReturn("/foo");
    Mockito.when(testRequest.getMethodName()).thenReturn("GET");
    Mockito.when(testRequest.getRequestHeaders()).thenReturn(mock(Headers.class));
    Mockito.when(testRequest.getRequestId()).thenReturn(HeaderUtils.randomWeakUUID());
    Mockito.when(testRequest.getRequestNanos()).thenReturn(Time.nanoTime());
    Mockito.when(testRequest.getRequestTimestamp()).thenReturn(Time.currentTimeMillis());

    Response testResponse2 = mock(Response.class);
    Mockito.when(testResponse2.status()).thenReturn(Status.OK);
    Mockito.when(testResponse2.headers()).thenReturn(mock(Headers.class));

    LinkedList<InetSocketAddress> hostList = new LinkedList<>();
    CountDownLatch latch = new CountDownLatch(2);

    AtomicInteger count = new AtomicInteger();

    AsyncFuture<Response> handlerResponseFuture = buildTestHandler(
        ScatterGatherHelper.builder()
            .roleFinder(new RoleFinderImpl())
            .pathParser(new DummyParser())
            .partitionFinder(new PartitionFinderImpl())
            .hostFinder(new HostFinder2Impl())
            .metricsProvider(request -> new Metrics())
            .responseMetrics(response -> new Metrics())
            .requestTimeout(header -> 1000L)
            .longTailRetrySupplier((resourceName, methodName) -> AsyncFuture.success(() -> 100L))
            .enableRetryRequestAlwaysUseADifferentHost(() -> true)
            .setRequestRetriableChecker((path, routingPolicy, responseStatus) -> Status.TOO_BUSY.equals(responseStatus))
            .dispatchHandler(
                (PartitionDispatchHandler<InetSocketAddress, Path, String, Request, Response, Status>) (
                    scatter,
                    part,
                    path,
                    request,
                    hostSelected,
                    responseFuture,
                    retryFuture,
                    timeoutFuture,
                    contextExecutor) -> {
                  hostSelected.setSuccess(part.getHosts().get(0));
                  hostList.add(hostSelected.getNow());
                  switch (count.getAndIncrement()) {
                    case 0:
                      retryFuture.setSuccess(Status.TOO_BUSY);
                      break;
                    case 1:
                      retryFuture.setSuccess(Status.OK);
                      responseFuture.setSuccess(Collections.singletonList(testResponse2));
                      break;
                    default:
                      Assert.fail("shouldn't get here");
                  }
                  latch.countDown();
                })
            .build(),
        timeoutProcessor,
        responseBuilder,
        errorBuilder).handler(ctx, testRequest);

    Assert.assertTrue(Time.await(latch, 120L, TimeUnit.MILLISECONDS), "latch");

    Assert.assertEquals(hostList.size(), 2);
    Assert.assertEquals(hostList.stream().distinct().count(), 2);

    Assert.assertTrue(
        Time.await((Time.Awaitable) handlerResponseFuture, 5L, TimeUnit.MILLISECONDS),
        "handlerResponseFuture");

    Assert.assertTrue(handlerResponseFuture.isSuccess());
    Assert.assertSame(handlerResponseFuture.getNow().status(), Status.OK);

    Time.sleep(50);

    registry.shutdown();
    registry.waitForShutdown();
  }

  // test needs retry because it can fail due to timing.
  @Test(groups = "unit", retryAnalyzer = Retry.class)
  public void test429NoLongTailRetry() throws Exception {
    ResourceRegistry registry = new ResourceRegistry();
    TimeoutProcessor timeoutProcessor = new TimeoutProcessor(registry);

    ResponseBuilder responseBuilder = (request, metrics, gatheredResponses) -> {
      if (gatheredResponses.size() == 1) {
        return gatheredResponses.iterator().next();
      }
      throw new UnsupportedOperationException();
    };

    Response testResponse1 = mock(Response.class);
    Mockito.when(testResponse1.status()).thenReturn(Status.TOO_BUSY);
    Mockito.when(testResponse1.headers()).thenReturn(mock(Headers.class));
    AtomicInteger errorCount = new AtomicInteger();

    ErrorBuilder errorBuilder = (request, status, contentMessage, ex, errorHeaders) -> {
      errorCount.incrementAndGet();
      Assert.assertSame(status, Status.SERVICE_UNAVAILABLE);
      return testResponse1;
    };

    Context ctx = mock(Context.class);
    Request testRequest = mock(Request.class);
    Mockito.when(testRequest.getUri()).thenReturn("/foo");
    Mockito.when(testRequest.getMethodName()).thenReturn("GET");
    Mockito.when(testRequest.getRequestHeaders()).thenReturn(mock(Headers.class));
    Mockito.when(testRequest.getRequestId()).thenReturn(HeaderUtils.randomWeakUUID());
    Mockito.when(testRequest.getRequestNanos()).thenReturn(Time.nanoTime());
    Mockito.when(testRequest.getRequestTimestamp()).thenReturn(Time.currentTimeMillis());

    LinkedList<InetSocketAddress> hostList = new LinkedList<>();
    CountDownLatch latch = new CountDownLatch(1);

    AtomicInteger count = new AtomicInteger();

    AsyncFuture<Response> handlerResponseFuture = buildTestHandler(
        ScatterGatherHelper.builder()
            .roleFinder(new RoleFinderImpl())
            .pathParser(new DummyParser())
            .partitionFinder(new PartitionFinderImpl())
            .hostFinder(new HostFinder4Impl())
            .metricsProvider(request -> new Metrics())
            .responseMetrics(response -> new Metrics())
            .requestTimeout(header -> 1000L)
            .longTailRetrySupplier((resourceName, methodName) -> AsyncFuture.success(() -> 100L))
            .enableRetryRequestAlwaysUseADifferentHost(() -> true)
            .setRequestRetriableChecker((path, routingPolicy, responseStatus) -> Status.TOO_BUSY.equals(responseStatus))
            .dispatchHandler(
                (PartitionDispatchHandler<InetSocketAddress, Path, String, Request, Response, Status>) (
                    scatter,
                    part,
                    path,
                    request,
                    hostSelected,
                    responseFuture,
                    retryFuture,
                    timeoutFuture,
                    contextExecutor) -> {
                  hostSelected.setSuccess(part.getHosts().get(0));
                  hostList.add(hostSelected.getNow());
                  switch (count.getAndIncrement()) {
                    case 0:
                      retryFuture.setSuccess(Status.TOO_BUSY);
                      break;
                    default:
                      Assert.fail("shouldn't get here");
                  }
                  latch.countDown();
                })
            .build(),
        timeoutProcessor,
        responseBuilder,
        errorBuilder).handler(ctx, testRequest);

    Assert.assertTrue(Time.await(latch, 120L, TimeUnit.MILLISECONDS), "latch");

    Assert.assertEquals(hostList.size(), 1);

    Assert.assertTrue(
        Time.await((Time.Awaitable) handlerResponseFuture, 5L, TimeUnit.MILLISECONDS),
        "handlerResponseFuture");

    Assert.assertTrue(handlerResponseFuture.isSuccess());
    Assert.assertSame(handlerResponseFuture.getNow().status(), Status.TOO_BUSY);
    Assert.assertEquals(errorCount.get(), 1);

    Time.sleep(50);

    registry.shutdown();
    registry.waitForShutdown();
  }

  @Test(groups = "unit", retryAnalyzer = Retry.class)
  public void test429RetryNotAllowed() throws Exception {
    ResourceRegistry registry = new ResourceRegistry();
    TimeoutProcessor timeoutProcessor = new TimeoutProcessor(registry);

    ResponseBuilder responseBuilder = (request, metrics, gatheredResponses) -> {
      if (gatheredResponses.size() == 1) {
        return gatheredResponses.iterator().next();
      }
      throw new UnsupportedOperationException();
    };

    Response testResponse1 = mock(Response.class);
    Mockito.when(testResponse1.status()).thenReturn(Status.TOO_BUSY);
    Mockito.when(testResponse1.headers()).thenReturn(mock(Headers.class));
    AtomicInteger errorCount = new AtomicInteger();

    ErrorBuilder errorBuilder = (request, status, contentMessage, ex, errorHeaders) -> {
      errorCount.incrementAndGet();
      Assert.assertSame(status, Status.TOO_BUSY);
      return testResponse1;
    };

    Context ctx = mock(Context.class);
    Request testRequest = mock(Request.class);
    Mockito.when(testRequest.getUri()).thenReturn("/foo");
    Mockito.when(testRequest.getMethodName()).thenReturn("GET");
    Mockito.when(testRequest.getRequestHeaders()).thenReturn(mock(Headers.class));
    Mockito.when(testRequest.getRequestId()).thenReturn(HeaderUtils.randomWeakUUID());
    Mockito.when(testRequest.getRequestNanos()).thenReturn(Time.nanoTime());
    Mockito.when(testRequest.getRequestTimestamp()).thenReturn(Time.currentTimeMillis());

    LinkedList<InetSocketAddress> hostList = new LinkedList<>();
    CountDownLatch latch = new CountDownLatch(1);

    AtomicInteger count = new AtomicInteger();

    AsyncFuture<Response> handlerResponseFuture = buildTestHandler(
        ScatterGatherHelper.builder()
            .roleFinder(new RoleFinderImpl())
            .pathParser(new DummyParser())
            .partitionFinder(new PartitionFinderImpl())
            .hostFinder(new HostFinder2Impl())
            .metricsProvider(request -> new Metrics())
            .responseMetrics(response -> new Metrics())
            .requestTimeout(header -> 1000L)
            .longTailRetrySupplier((resourceName, methodName) -> AsyncFuture.success(() -> 100L))
            .enableRetryRequestAlwaysUseADifferentHost(() -> true)
            .setRequestRetriableChecker(
                (path, routingPolicy, responseStatus) -> !Status.TOO_BUSY.equals(responseStatus))
            .dispatchHandler(
                (PartitionDispatchHandler<InetSocketAddress, Path, String, Request, Response, Status>) (
                    scatter,
                    part,
                    path,
                    request,
                    hostSelected,
                    responseFuture,
                    retryFuture,
                    timeoutFuture,
                    contextExecutor) -> {
                  hostSelected.setSuccess(part.getHosts().get(0));
                  hostList.add(hostSelected.getNow());
                  switch (count.getAndIncrement()) {
                    case 0:
                      retryFuture.setSuccess(Status.TOO_BUSY);
                      break;
                    default:
                      Assert.fail("shouldn't get here");
                  }
                  latch.countDown();
                })
            .build(),
        timeoutProcessor,
        responseBuilder,
        errorBuilder).handler(ctx, testRequest);

    Assert.assertTrue(Time.await(latch, 120L, TimeUnit.MILLISECONDS), "latch");

    Assert.assertEquals(hostList.size(), 1);

    Assert.assertTrue(
        Time.await((Time.Awaitable) handlerResponseFuture, 5L, TimeUnit.MILLISECONDS),
        "handlerResponseFuture");

    Assert.assertTrue(handlerResponseFuture.isSuccess());
    Assert.assertSame(handlerResponseFuture.getNow().status(), Status.TOO_BUSY);
    Assert.assertEquals(errorCount.get(), 1);

    Time.sleep(50);

    registry.shutdown();
    registry.waitForShutdown();
  }

  @Test(groups = "unit", retryAnalyzer = Retry.class)
  public void test503Retry() throws Exception {
    ResourceRegistry registry = new ResourceRegistry();
    TimeoutProcessor timeoutProcessor = new TimeoutProcessor(registry);

    ResponseBuilder responseBuilder = (request, metrics, gatheredResponses) -> {
      if (gatheredResponses.size() == 1) {
        return gatheredResponses.iterator().next();
      }
      throw new UnsupportedOperationException();
    };

    ErrorBuilder errorBuilder = (request, status, contentMessage, ex, errorHeaders) -> {
      Assert.fail("Should not get here");
      return mock(Response.class);
    };

    Context ctx = mock(Context.class);
    Request testRequest = mock(Request.class);
    Mockito.when(testRequest.getUri()).thenReturn("/foo");
    Mockito.when(testRequest.getMethodName()).thenReturn("GET");
    Mockito.when(testRequest.getRequestHeaders()).thenReturn(mock(Headers.class));
    Mockito.when(testRequest.getRequestId()).thenReturn(HeaderUtils.randomWeakUUID());
    Mockito.when(testRequest.getRequestNanos()).thenReturn(Time.nanoTime());
    Mockito.when(testRequest.getRequestTimestamp()).thenReturn(Time.currentTimeMillis());

    Response testResponse2 = mock(Response.class);
    Mockito.when(testResponse2.status()).thenReturn(Status.OK);
    Mockito.when(testResponse2.headers()).thenReturn(mock(Headers.class));

    LinkedList<InetSocketAddress> hostList = new LinkedList<>();
    CountDownLatch latch = new CountDownLatch(2);

    AtomicInteger count = new AtomicInteger();

    AsyncFuture<Response> handlerResponseFuture = buildTestHandler(
        ScatterGatherHelper.builder()
            .roleFinder(new RoleFinderImpl())
            .pathParser(new DummyParser())
            .partitionFinder(new PartitionFinderImpl())
            .hostFinder(new HostFinder2Impl())
            .metricsProvider(request -> new Metrics())
            .responseMetrics(response -> new Metrics())
            .requestTimeout(header -> 1000L)
            .longTailRetrySupplier((resourceName, methodName) -> AsyncFuture.success(() -> 100L))
            .enableRetryRequestAlwaysUseADifferentHost(() -> true)
            .setRequestRetriableChecker(
                (path, routingPolicy, responseStatus) -> Status.SERVICE_UNAVAILABLE.equals(responseStatus))
            .dispatchHandler(
                (PartitionDispatchHandler<InetSocketAddress, Path, String, Request, Response, Status>) (
                    scatter,
                    part,
                    path,
                    request,
                    hostSelected,
                    responseFuture,
                    retryFuture,
                    timeoutFuture,
                    contextExecutor) -> {
                  hostSelected.setSuccess(part.getHosts().get(0));
                  hostList.add(hostSelected.getNow());
                  switch (count.getAndIncrement()) {
                    case 0:
                      retryFuture.setSuccess(Status.SERVICE_UNAVAILABLE);
                      break;
                    case 1:
                      retryFuture.setSuccess(Status.OK);
                      responseFuture.setSuccess(Collections.singletonList(testResponse2));
                      break;
                    default:
                      Assert.fail("shouldn't get here");
                  }
                  latch.countDown();
                })
            .build(),
        timeoutProcessor,
        responseBuilder,
        errorBuilder).handler(ctx, testRequest);

    Assert.assertTrue(Time.await(latch, 120L, TimeUnit.MILLISECONDS), "latch");

    Assert.assertEquals(hostList.size(), 2);
    Assert.assertEquals(hostList.stream().distinct().count(), 2);

    Assert.assertTrue(
        Time.await((Time.Awaitable) handlerResponseFuture, 5L, TimeUnit.MILLISECONDS),
        "handlerResponseFuture");

    Assert.assertTrue(handlerResponseFuture.isSuccess());
    Assert.assertSame(handlerResponseFuture.getNow().status(), Status.OK);

    Time.sleep(50);

    registry.shutdown();
    registry.waitForShutdown();
  }

  @Test(groups = "unit", retryAnalyzer = Retry.class)
  public void test503RetryNotAllowed() throws Exception {
    ResourceRegistry registry = new ResourceRegistry();
    TimeoutProcessor timeoutProcessor = new TimeoutProcessor(registry);

    ResponseBuilder responseBuilder = (request, metrics, gatheredResponses) -> {
      if (gatheredResponses.size() == 1) {
        return gatheredResponses.iterator().next();
      }
      throw new UnsupportedOperationException();
    };

    Response testResponse1 = mock(Response.class);
    Mockito.when(testResponse1.status()).thenReturn(Status.SERVICE_UNAVAILABLE);
    Mockito.when(testResponse1.headers()).thenReturn(mock(Headers.class));
    AtomicInteger errorCount = new AtomicInteger();

    ErrorBuilder errorBuilder = (request, status, contentMessage, ex, errorHeaders) -> {
      errorCount.incrementAndGet();
      Assert.assertSame(status, Status.SERVICE_UNAVAILABLE);
      return testResponse1;
    };

    Context ctx = mock(Context.class);
    Request testRequest = mock(Request.class);
    Mockito.when(testRequest.getUri()).thenReturn("/foo");
    Mockito.when(testRequest.getMethodName()).thenReturn("GET");
    Mockito.when(testRequest.getRequestHeaders()).thenReturn(mock(Headers.class));
    Mockito.when(testRequest.getRequestId()).thenReturn(HeaderUtils.randomWeakUUID());
    Mockito.when(testRequest.getRequestNanos()).thenReturn(Time.nanoTime());
    Mockito.when(testRequest.getRequestTimestamp()).thenReturn(Time.currentTimeMillis());

    LinkedList<InetSocketAddress> hostList = new LinkedList<>();
    CountDownLatch latch = new CountDownLatch(1);

    AtomicInteger count = new AtomicInteger();

    AsyncFuture<Response> handlerResponseFuture = buildTestHandler(
        ScatterGatherHelper.builder()
            .roleFinder(new RoleFinderImpl())
            .pathParser(new DummyParser())
            .partitionFinder(new PartitionFinderImpl())
            .hostFinder(new HostFinder2Impl())
            .metricsProvider(request -> new Metrics())
            .responseMetrics(response -> new Metrics())
            .requestTimeout(header -> 1000L)
            .longTailRetrySupplier((resourceName, methodName) -> AsyncFuture.success(() -> 100L))
            .enableRetryRequestAlwaysUseADifferentHost(() -> true)
            .setRequestRetriableChecker(
                (path, routingPolicy, responseStatus) -> !Status.SERVICE_UNAVAILABLE.equals(responseStatus))
            .dispatchHandler(
                (PartitionDispatchHandler<InetSocketAddress, Path, String, Request, Response, Status>) (
                    scatter,
                    part,
                    path,
                    request,
                    hostSelected,
                    responseFuture,
                    retryFuture,
                    timeoutFuture,
                    contextExecutor) -> {
                  hostSelected.setSuccess(part.getHosts().get(0));
                  hostList.add(hostSelected.getNow());
                  switch (count.getAndIncrement()) {
                    case 0:
                      retryFuture.setSuccess(Status.SERVICE_UNAVAILABLE);
                      break;
                    default:
                      Assert.fail("shouldn't get here");
                  }
                  latch.countDown();
                })
            .build(),
        timeoutProcessor,
        responseBuilder,
        errorBuilder).handler(ctx, testRequest);

    Assert.assertTrue(Time.await(latch, 120L, TimeUnit.MILLISECONDS), "latch");

    Assert.assertEquals(hostList.size(), 1);

    Assert.assertTrue(
        Time.await((Time.Awaitable) handlerResponseFuture, 5L, TimeUnit.MILLISECONDS),
        "handlerResponseFuture");

    Assert.assertTrue(handlerResponseFuture.isSuccess());
    Assert.assertSame(handlerResponseFuture.getNow().status(), Status.SERVICE_UNAVAILABLE);
    Assert.assertEquals(errorCount.get(), 1);

    Time.sleep(50);

    registry.shutdown();
    registry.waitForShutdown();
  }

  @Test(groups = "unit")
  public void testQueryRedirectionAllowed() throws Exception {
    ResourceRegistry registry = new ResourceRegistry();
    TimeoutProcessor timeoutProcessor = new TimeoutProcessor(registry);

    ResponseBuilder responseBuilder = (request, metrics, gatheredResponses) -> {
      if (gatheredResponses.size() == 1) {
        return gatheredResponses.iterator().next();
      }
      throw new UnsupportedOperationException();
    };

    ErrorBuilder errorBuilder = (request, status, contentMessage, ex, errorHeaders) -> {
      Assert.fail("Should not get here");
      return mock(Response.class);
    };

    Context ctx = mock(Context.class);
    Request testRequest = mock(Request.class);
    Mockito.when(testRequest.getUri()).thenReturn("/foo/bar/?query=abc");
    Mockito.when(testRequest.getMethodName()).thenReturn("GET");
    Mockito.when(testRequest.getRequestHeaders()).thenReturn(mock(Headers.class));
    Mockito.when(testRequest.getRequestId()).thenReturn(HeaderUtils.randomWeakUUID());

    Response testResponse = mock(Response.class);
    Mockito.when(testResponse.status()).thenReturn(Status.OK);
    Mockito.when(testResponse.headers()).thenReturn(mock(Headers.class));

    AsyncFuture<Response> handlerResponseFuture = buildTestHandler(
        ScatterGatherHelper.builder()
            .roleFinder(new RoleFinderImpl())
            .pathParser(new DummyParser())
            .partitionFinder(new PartitionFinderImpl())
            .hostFinder(new HostFinder2Impl())
            .hostHealthMonitor((host, ignored) -> {
              InetSocketAddress host1;
              host1 = new InetSocketAddress("127.0.0.1", 10000);
              // Always declares host1 as unhealthy and host2 as healthy
              return !host.equals(host1);
            })
            .setIsReqRedirectionAllowedForQuery(() -> true) // Request redirection allowed for query
            .metricsProvider(request -> new Metrics())
            .responseMetrics(response -> new Metrics())
            .dispatchHandler(
                (PartitionDispatchHandler<InetSocketAddress, Path, String, Request, Response, Status>) (
                    scatter,
                    part,
                    path,
                    request,
                    hostSelected,
                    responseFuture,
                    retryFuture,
                    timeoutFuture,
                    contextExecutor) -> {
                  // Unhealthy host will not be included in the hostsList
                  Assert.assertEquals(part.getHosts().size(), 1);
                  hostSelected.setSuccess(part.getHosts().get(0));
                  responseFuture.setSuccess(Collections.singletonList(testResponse));
                })
            .build(),
        timeoutProcessor,
        responseBuilder,
        errorBuilder).handler(ctx, testRequest);

    Assert.assertTrue(handlerResponseFuture.isSuccess());
    Assert.assertSame(handlerResponseFuture.getNow().status(), Status.OK);
  }

  @Test(groups = "unit")
  public void testQueryRedirectionNotAllowed() throws Exception {
    ResourceRegistry registry = new ResourceRegistry();
    TimeoutProcessor timeoutProcessor = new TimeoutProcessor(registry);

    ResponseBuilder responseBuilder = (request, metrics, gatheredResponses) -> {
      if (gatheredResponses.size() == 1) {
        return gatheredResponses.iterator().next();
      }
      throw new UnsupportedOperationException();
    };

    ErrorBuilder errorBuilder = (request, status, contentMessage, ex, errorHeaders) -> {
      Assert.fail("Should not get here");
      return mock(Response.class);
    };

    Context ctx = mock(Context.class);
    Request testRequest = mock(Request.class);
    Mockito.when(testRequest.getUri()).thenReturn("/foo/bar/?query=abc");
    Mockito.when(testRequest.getMethodName()).thenReturn("GET");
    Mockito.when(testRequest.getRequestHeaders()).thenReturn(mock(Headers.class));
    Mockito.when(testRequest.getRequestId()).thenReturn(HeaderUtils.randomWeakUUID());

    Response testResponse = mock(Response.class);
    Mockito.when(testResponse.status()).thenReturn(Status.OK);
    Mockito.when(testResponse.headers()).thenReturn(mock(Headers.class));

    AsyncFuture<Response> handlerResponseFuture = buildTestHandler(
        ScatterGatherHelper.builder()
            .roleFinder(new RoleFinderImpl())
            .pathParser(new DummyParser())
            .partitionFinder(new PartitionFinderImpl())
            .hostFinder(new HostFinder2Impl())
            .hostHealthMonitor((host, ignored) -> {
              InetSocketAddress host1;
              host1 = new InetSocketAddress("127.0.0.1", 10000);
              // Always declares host1 as unhealthy and host2 as healthy
              return !host.equals(host1);
            })
            .setIsReqRedirectionAllowedForQuery(() -> false) // Request redirection not allowed for query
            .metricsProvider(request -> new Metrics())
            .responseMetrics(response -> new Metrics())
            .dispatchHandler(
                (PartitionDispatchHandler<InetSocketAddress, Path, String, Request, Response, Status>) (
                    scatter,
                    part,
                    path,
                    request,
                    hostSelected,
                    responseFuture,
                    retryFuture,
                    timeoutFuture,
                    contextExecutor) -> {
                  // Unhealthy host will be included in the hostsList
                  Assert.assertEquals(part.getHosts().size(), 2);
                  hostSelected.setSuccess(part.getHosts().get(0));
                  responseFuture.setSuccess(Collections.singletonList(testResponse));
                })
            .build(),
        timeoutProcessor,
        responseBuilder,
        errorBuilder).handler(ctx, testRequest);

    Assert.assertTrue(handlerResponseFuture.isSuccess());
    Assert.assertSame(handlerResponseFuture.getNow().status(), Status.OK);
  }

  // test needs retry because it can fail due to timing.
  @Test(groups = "unit", retryAnalyzer = Retry.class)
  public void test404LongTailTooLateDispatch() throws Exception {
    ResourceRegistry registry = new ResourceRegistry();
    TimeoutProcessor timeoutProcessor = new TimeoutProcessor(registry);

    ResponseBuilder responseBuilder = (request, metrics, gatheredResponses) -> {
      if (gatheredResponses.size() == 1) {
        return gatheredResponses.iterator().next();
      }
      throw new UnsupportedOperationException();
    };

    ErrorBuilder errorBuilder = (request, status, contentMessage, ex, errorHeaders) -> {
      Assert.fail("Should not get here");
      return mock(Response.class);
    };

    Context ctx = mock(Context.class);
    Request testRequest = mock(Request.class);
    Mockito.when(testRequest.getUri()).thenReturn("/foo");
    Mockito.when(testRequest.getMethodName()).thenReturn("GET");
    Mockito.when(testRequest.getRequestHeaders()).thenReturn(mock(Headers.class));
    Mockito.when(testRequest.getRequestId()).thenReturn(HeaderUtils.randomWeakUUID());
    Mockito.when(testRequest.getRequestNanos()).thenReturn(Time.nanoTime());
    Mockito.when(testRequest.getRequestTimestamp()).thenReturn(Time.currentTimeMillis());

    Response testResponse1 = mock(Response.class);
    Mockito.when(testResponse1.status()).thenReturn(Status.NOT_FOUND);
    Mockito.when(testResponse1.headers()).thenReturn(mock(Headers.class));

    Response testResponse2 = mock(Response.class);
    Mockito.when(testResponse2.status()).thenReturn(Status.OK);
    Mockito.when(testResponse2.headers()).thenReturn(mock(Headers.class));

    List<AsyncPromise<List<Response>>> responsePromiseList = new LinkedList<>();
    CountDownLatch latch = new CountDownLatch(2);

    AsyncFuture<Response> handlerResponseFuture = buildTestHandler(
        ScatterGatherHelper.builder()
            .roleFinder(new RoleFinderImpl())
            .pathParser(new DummyParser())
            .partitionFinder(new PartitionFinderImpl())
            .hostFinder(new HostFinder2Impl())
            .metricsProvider(request -> new Metrics())
            .responseMetrics(response -> new Metrics())
            .requestTimeout(header -> 1000L)
            .longTailRetrySupplier((resourceName, methodName) -> AsyncFuture.success(() -> 100L))
            .dispatchHandler(
                (PartitionDispatchHandler<InetSocketAddress, Path, String, Request, Response, Status>) (
                    scatter,
                    part,
                    path,
                    request,
                    hostSelected,
                    responseFuture,
                    retryFuture,
                    timeoutFuture,
                    contextExecutor) -> {
                  hostSelected.setSuccess(part.getHosts().get(0));
                  responsePromiseList.add(responseFuture);
                  latch.countDown();
                })
            .build(),
        timeoutProcessor,
        responseBuilder,
        errorBuilder).handler(ctx, testRequest);

    Assert.assertEquals(responsePromiseList.size(), 1);
    Assert.assertTrue(Time.await(latch, 120L, TimeUnit.MILLISECONDS), "latch");
    Assert.assertEquals(responsePromiseList.size(), 2);

    responsePromiseList.get(0).setSuccess(Collections.singletonList(testResponse1));

    Assert.assertTrue(
        Time.await((Time.Awaitable) handlerResponseFuture, 5L, TimeUnit.MILLISECONDS),
        "handlerResponseFuture");

    Assert.assertTrue(handlerResponseFuture.isSuccess());
    Assert.assertSame(handlerResponseFuture.getNow().status(), Status.NOT_FOUND);

    responsePromiseList.get(1).setSuccess(Collections.singletonList(testResponse2));
    Time.sleep(50);

    registry.shutdown();
    registry.waitForShutdown();
  }

  @Test(groups = "unit")
  public void testNoHost() throws Exception {
    ResourceRegistry registry = new ResourceRegistry();
    TimeoutProcessor timeoutProcessor = new TimeoutProcessor(registry);

    ResponseBuilder responseBuilder = (request, metrics, gatheredResponses) -> {
      if (gatheredResponses.size() == 1) {
        return gatheredResponses.iterator().next();
      }
      throw new UnsupportedOperationException();
    };

    Response errorResponse = mock(Response.class);
    Mockito.when(errorResponse.status()).thenReturn(Status.SERVICE_UNAVAILABLE);
    Mockito.when(errorResponse.headers()).thenReturn(mock(Headers.class));

    AtomicInteger errorCount = new AtomicInteger();

    ErrorBuilder errorBuilder = (request, status, contentMessage, ex, errorHeaders) -> {
      Assert.assertEquals(status, Status.SERVICE_UNAVAILABLE);
      Assert.assertNull(ex);
      errorCount.incrementAndGet();
      return errorResponse;
    };

    Context ctx = mock(Context.class);
    Request testRequest = mock(Request.class);
    Mockito.when(testRequest.getUri()).thenReturn("/foo");
    Mockito.when(testRequest.getMethodName()).thenReturn("GET");
    Mockito.when(testRequest.getRequestHeaders()).thenReturn(mock(Headers.class));
    Mockito.when(testRequest.getRequestId()).thenReturn(HeaderUtils.randomWeakUUID());

    AtomicInteger count = new AtomicInteger();

    AsyncFuture<Response> handlerResponseFuture = buildTestHandler(
        ScatterGatherHelper.builder()
            .roleFinder(new RoleFinderImpl())
            .pathParser(new DummyParser())
            .partitionFinder(new PartitionFinderImpl())
            .hostFinder(new HostFinder3Impl())
            .metricsProvider(request -> new Metrics())
            .responseMetrics(response -> new Metrics())
            .dispatchHandler(
                (PartitionDispatchHandler<InetSocketAddress, Path, String, Request, Response, Status>) (
                    scatter,
                    part,
                    path,
                    request,
                    hostSelected,
                    responseFuture,
                    retryFuture,
                    timeoutFuture,
                    contextExecutor) -> {
                  count.incrementAndGet();
                  Assert.fail("Should never get here");
                })
            .build(),
        timeoutProcessor,
        responseBuilder,
        errorBuilder).handler(ctx, testRequest);

    Assert.assertTrue(handlerResponseFuture.isSuccess());
    Assert.assertSame(handlerResponseFuture.getNow().status(), Status.SERVICE_UNAVAILABLE);

    Assert.assertEquals(count.get(), 0);
    Assert.assertEquals(errorCount.get(), 1);
  }

  @Test(groups = "unit")
  public void testNoHost2() throws Exception {
    ResourceRegistry registry = new ResourceRegistry();
    TimeoutProcessor timeoutProcessor = new TimeoutProcessor(registry);

    ResponseBuilder responseBuilder = (request, metrics, gatheredResponses) -> {
      if (gatheredResponses.size() == 1) {
        return gatheredResponses.iterator().next();
      }
      throw new UnsupportedOperationException();
    };

    Response errorResponse = mock(Response.class);
    Mockito.when(errorResponse.status()).thenReturn(Status.SERVICE_UNAVAILABLE);
    Mockito.when(errorResponse.headers()).thenReturn(mock(Headers.class));

    AtomicInteger errorCount = new AtomicInteger();

    ErrorBuilder errorBuilder = (request, status, contentMessage, ex, errorHeaders) -> {
      Assert.assertEquals(status, Status.SERVICE_UNAVAILABLE);
      Assert.assertNull(ex);
      errorCount.incrementAndGet();
      return errorResponse;
    };

    Context ctx = mock(Context.class);
    Request testRequest = mock(Request.class);
    Mockito.when(testRequest.getUri()).thenReturn("/foo");
    Mockito.when(testRequest.getMethodName()).thenReturn("GET");
    Mockito.when(testRequest.getRequestHeaders()).thenReturn(mock(Headers.class));
    Mockito.when(testRequest.getRequestId()).thenReturn(HeaderUtils.randomWeakUUID());

    AtomicInteger count = new AtomicInteger();

    AsyncFuture<Response> handlerResponseFuture = buildTestHandler(
        ScatterGatherHelper.builder()
            .roleFinder(new RoleFinderImpl())
            .pathParser(new DummyParser2())
            .partitionFinder(new PartitionFinderImpl())
            .hostFinder(new HostFinder3Impl())
            .metricsProvider(request -> new Metrics())
            .responseMetrics(response -> new Metrics())
            .dispatchHandler(
                (PartitionDispatchHandler<InetSocketAddress, Path, String, Request, Response, Status>) (
                    scatter,
                    part,
                    path,
                    request,
                    hostSelected,
                    responseFuture,
                    retryFuture,
                    timeoutFuture,
                    contextExecutor) -> {
                  count.incrementAndGet();
                  Assert.fail("Should never get here");
                })
            .build(),
        timeoutProcessor,
        responseBuilder,
        errorBuilder).handler(ctx, testRequest);

    Assert.assertTrue(handlerResponseFuture.isSuccess());
    Assert.assertSame(handlerResponseFuture.getNow().status(), Status.SERVICE_UNAVAILABLE);

    Assert.assertEquals(count.get(), 0);
    Assert.assertEquals(errorCount.get(), 1);
  }

  @Test(groups = "unit")
  public void testIncrementTotalRetriesCounts() {
    ScatterGatherHelper scatterGatherHelper = mock(ScatterGatherHelper.class);
    TimeoutProcessor timeoutProcessor = mock(TimeoutProcessor.class);
    Executor executor = mock(Executor.class);
    ScatterGatherRequestHandlerImpl scatterGatherRequestHandler =
        new ScatterGatherRequestHandler4(scatterGatherHelper, timeoutProcessor, executor);

    ScatterGatherStats stats = new ScatterGatherStats();
    ScatterGatherStats.Delta delta = stats.new Delta();

    // 503 retry count will not increase if response status is not 503
    scatterGatherRequestHandler.incrementTotalRetries(delta, HttpResponseStatus.TOO_MANY_REQUESTS);
    scatterGatherRequestHandler.incrementTotalRetriesWinner(delta, HttpResponseStatus.TOO_MANY_REQUESTS);
    scatterGatherRequestHandler.incrementTotalRetriesError(delta, HttpResponseStatus.TOO_MANY_REQUESTS);
    delta.apply();

    Assert.assertEquals(stats.getTotalRetries(), 1);
    Assert.assertEquals(stats.getTotalRetriesWinner(), 1);
    Assert.assertEquals(stats.getTotalRetriesError(), 1);
    Assert.assertEquals(stats.getTotalRetriesOn503(), 0);
    Assert.assertEquals(stats.getTotalRetriesOn503Winner(), 0);
    Assert.assertEquals(stats.getTotalRetriesOn503Error(), 0);

    // 503 retry count will increase if response status is 503
    scatterGatherRequestHandler.incrementTotalRetries(delta, HttpResponseStatus.SERVICE_UNAVAILABLE);
    scatterGatherRequestHandler.incrementTotalRetriesWinner(delta, HttpResponseStatus.SERVICE_UNAVAILABLE);
    scatterGatherRequestHandler.incrementTotalRetriesError(delta, HttpResponseStatus.SERVICE_UNAVAILABLE);
    delta.apply();

    Assert.assertEquals(stats.getTotalRetries(), 2);
    Assert.assertEquals(stats.getTotalRetriesWinner(), 2);
    Assert.assertEquals(stats.getTotalRetriesError(), 2);
    Assert.assertEquals(stats.getTotalRetriesOn503(), 1);
    Assert.assertEquals(stats.getTotalRetriesOn503Winner(), 1);
    Assert.assertEquals(stats.getTotalRetriesOn503Error(), 1);
  }

  /**
   * These tests fail under high concurrency, which unfortunately, the tests execute the subproject tests in parallel
   */
  public static class Retry implements IRetryAnalyzer {
    int _attempts = 8;

    @Override
    public boolean retry(ITestResult result) {
      if (!result.isSuccess() && _attempts-- > 0) {
        result.setStatus(ITestResult.SUCCESS_PERCENTAGE_FAILURE);
        try {
          Time.sleep(1000 + ThreadLocalRandom.current().nextInt(10000));
        } catch (InterruptedException e) {
          // Ignored;
        }
        LOG.info("retrying test", result.getThrowable());
        return true;
      }
      return false;
    }
  }

  class DummyParser implements com.linkedin.alpini.router.api.ResourcePathParser<Path, String> {
    @Nonnull
    @Override
    public Path parseResourceUri(@Nonnull String uri) throws RouterException {
      return new Path(uri, "", "/");
    }

    @Nonnull
    @Override
    public Path substitutePartitionKey(@Nonnull Path path, String s) {
      return path;
    }

    @Nonnull
    @Override
    public Path substitutePartitionKey(@Nonnull Path path, @Nonnull Collection<String> s) {
      return path;
    }
  }

  class DummyParser2 implements com.linkedin.alpini.router.api.ResourcePathParser<Path, String> {
    @Nonnull
    @Override
    public Path parseResourceUri(@Nonnull String uri) throws RouterException {
      return new Path(uri, Collections.emptyList(), "/");
    }

    @Nonnull
    @Override
    public Path substitutePartitionKey(@Nonnull Path path, String s) {
      return path;
    }

    @Nonnull
    @Override
    public Path substitutePartitionKey(@Nonnull Path path, @Nonnull Collection<String> s) {
      return path;
    }
  }

  class FailHostFinder implements HostFinder<InetSocketAddress, List<List<State>>> {
    @Nonnull
    @Override
    public List<InetSocketAddress> findHosts(
        @Nonnull String requestMethod,
        @Nonnull String resourceName,
        @Nonnull String partitionName,
        @Nonnull HostHealthMonitor<InetSocketAddress> hostHealthMonitor,
        @Nonnull List<List<State>> roles) throws RouterException {
      throw new RouterException(Status.class, Status.SERVICE_UNAVAILABLE, 503, "Foo", false);
    }

    @Nonnull
    @Override
    public Collection<InetSocketAddress> findAllHosts(List<List<State>> roles) throws RouterException {
      throw new IllegalStateException();
    }
  }

  private class Path implements ResourcePath<String> {
    private final String resource;
    private final List<String> partitionKey;
    private final String remainder;

    public Path(String resource, String partitionKey, String remainder) {
      this(resource, Collections.singletonList(Objects.requireNonNull(partitionKey, "partitionKey")), remainder);
    }

    public Path(String resource, List<String> partitionKey, String remainder) {
      this.resource = Objects.requireNonNull(resource, "resource");
      this.partitionKey = Objects.requireNonNull(partitionKey, "partitionKey");
      this.remainder = remainder;
      if (!resource.startsWith("/") || resource.endsWith("/")) {
        throw new IllegalArgumentException("Resource must start with '/' and must not end with '/'");
      }
      if (remainder != null && !remainder.startsWith("/")) {
        throw new IllegalArgumentException("Remainder must start with '/'");
      }
    }

    private String getRemainder() {
      return remainder != null ? remainder : "";
    }

    @Nonnull
    @Override
    public String getLocation() {
      Iterator<String> it = partitionKey.iterator();
      if (!it.hasNext()) {
        return resource + "/*" + getRemainder();
      }
      String first = it.next();
      if (!it.hasNext()) {
        return resource + "/" + first + getRemainder();
      }
      int capacity = resource.length() + 1 + partitionKey.stream().mapToInt(String::length).sum();
      StringBuilder sb = new StringBuilder(capacity);
      sb.append(resource).append("/(").append(first);
      do {
        sb.append(",").append(it.next());
      } while (it.hasNext());
      return sb.append(")").append(getRemainder()).toString();
    }

    @Nonnull
    @Override
    public Collection<String> getPartitionKeys() {
      return partitionKey;
    }

    @Nonnull
    @Override
    public String getResourceName() {
      return resource;
    }
  }

  private class FailPathParserImpl implements com.linkedin.alpini.router.api.ResourcePathParser<Path, String> {
    @Nonnull
    @Override
    public Path parseResourceUri(@Nonnull String uri) throws RouterException {
      throw new RouterException(Status.class, Status.NOT_FOUND, 404, "Not found", false);
    }

    @Nonnull
    @Override
    public Path substitutePartitionKey(@Nonnull Path p, String s) {
      throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public Path substitutePartitionKey(@Nonnull Path p, @Nonnull Collection<String> s) {
      throw new UnsupportedOperationException();
    }
  }

  private class PartitionFinderImpl implements PartitionFinder<String> {
    @Nonnull
    @Override
    public String findPartitionName(@Nonnull String resourceName, @Nonnull String partitionKey) throws RouterException {
      return "shard_0";
    }

    @Nonnull
    @Override
    public List<String> getAllPartitionNames(@Nonnull String resourceName) throws RouterException {
      return Collections.singletonList("shard_0");
    }

    @Override
    public int getNumPartitions(@Nonnull String resourceName) throws RouterException {
      return 1;
    }

    @Override
    public int findPartitionNumber(@Nonnull String partitionKey, int numPartitions, String storeName, int versionNumber)
        throws RouterException {
      return 0;
    }
  }

  private enum State {
    LEADER, FOLLOWER
  }

  private class RoleFinderImpl implements RoleFinder<List<List<State>>> {
    @Nonnull
    @Override
    public List<List<State>> parseRole(@Nonnull String httpMethod, @Nonnull Headers httpHeaders) {
      List<List<State>> list = new ArrayList<>(2);
      list.add(Collections.singletonList(State.LEADER));
      list.add(Collections.singletonList(State.FOLLOWER));
      return Collections.unmodifiableList(list);
    }
  }

  private class HostFinderImpl implements HostFinder<InetSocketAddress, List<List<State>>> {
    private final ArrayList<InetSocketAddress> hosts;

    private HostFinderImpl() {
      hosts = new ArrayList<>(2);
      hosts.add(new InetSocketAddress("127.0.0.1", 10000));
      hosts.add(new InetSocketAddress("127.0.0.1", 10001));
    }

    @Nonnull
    @Override
    public List<InetSocketAddress> findHosts(
        @Nonnull String requestMethod,
        @Nonnull String resourceName,
        @Nonnull String partitionName,
        @Nonnull HostHealthMonitor<InetSocketAddress> hostHealthMonitor,
        @Nonnull List<List<State>> roles) throws RouterException {
      return Collections
          .singletonList(hosts.get((int) ((0xffffffffL & (long) partitionName.hashCode()) % hosts.size())));
    }

    @Nonnull
    @Override
    public Collection<InetSocketAddress> findAllHosts(@Nonnull String resourceName, List<List<State>> roles)
        throws RouterException {
      return findAllHosts(roles);
    }

    @Nonnull
    @Override
    public Collection<InetSocketAddress> findAllHosts(List<List<State>> roles) throws RouterException {
      return Collections.unmodifiableCollection(hosts);
    }
  }

  private class HostFinder2Impl implements HostFinder<InetSocketAddress, List<List<State>>> {
    private final ArrayList<InetSocketAddress> hosts;

    private HostFinder2Impl() {
      hosts = new ArrayList<>(2);
      hosts.add(new InetSocketAddress("127.0.0.1", 10000));
      hosts.add(new InetSocketAddress("127.0.0.1", 10001));
    }

    @Nonnull
    @Override
    public List<InetSocketAddress> findHosts(
        @Nonnull String requestMethod,
        @Nonnull String resourceName,
        @Nonnull String partitionName,
        @Nonnull HostHealthMonitor<InetSocketAddress> hostHealthMonitor,
        @Nonnull List<List<State>> roles) throws RouterException {
      return new ArrayList<>(hosts);
    }

    @Nonnull
    @Override
    public Collection<InetSocketAddress> findAllHosts(@Nonnull String resourceName, List<List<State>> roles)
        throws RouterException {
      return findAllHosts(roles);
    }

    @Nonnull
    @Override
    public Collection<InetSocketAddress> findAllHosts(List<List<State>> roles) throws RouterException {
      return Collections.unmodifiableCollection(hosts);
    }
  }

  private class HostFinder3Impl implements HostFinder<InetSocketAddress, List<List<State>>> {
    @Nonnull
    @Override
    public List<InetSocketAddress> findHosts(
        @Nonnull String requestMethod,
        @Nonnull String resourceName,
        @Nonnull String partitionName,
        @Nonnull HostHealthMonitor<InetSocketAddress> hostHealthMonitor,
        @Nonnull List<List<State>> roles) throws RouterException {
      return Collections.emptyList();
    }

    @Nonnull
    @Override
    public Collection<InetSocketAddress> findAllHosts(@Nonnull String resourceName, List<List<State>> roles)
        throws RouterException {
      return findAllHosts(roles);
    }

    @Nonnull
    @Override
    public Collection<InetSocketAddress> findAllHosts(List<List<State>> roles) throws RouterException {
      return Collections.emptySet();
    }
  }

  private class HostFinder4Impl implements HostFinder<InetSocketAddress, List<List<State>>> {
    private final List<InetSocketAddress> hosts;

    private HostFinder4Impl() {
      hosts = Collections.singletonList(new InetSocketAddress("127.0.0.1", 10000));
    }

    @Nonnull
    @Override
    public List<InetSocketAddress> findHosts(
        @Nonnull String requestMethod,
        @Nonnull String resourceName,
        @Nonnull String partitionName,
        @Nonnull HostHealthMonitor<InetSocketAddress> hostHealthMonitor,
        @Nonnull List<List<State>> roles) throws RouterException {
      return new ArrayList<>(hosts);
    }

    @Nonnull
    @Override
    public Collection<InetSocketAddress> findAllHosts(@Nonnull String resourceName, List<List<State>> roles)
        throws RouterException {
      return findAllHosts(roles);
    }

    @Nonnull
    @Override
    public Collection<InetSocketAddress> findAllHosts(List<List<State>> roles) throws RouterException {
      return Collections.unmodifiableCollection(hosts);
    }
  }
}
