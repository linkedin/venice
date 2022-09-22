package com.linkedin.alpini.router;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.alpini.base.misc.HeaderNames;
import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.base.registry.ResourceRegistry;
import com.linkedin.alpini.base.registry.ShutdownableExecutors;
import com.linkedin.alpini.netty4.handlers.AsyncFullHttpRequestHandler;
import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.alpini.router.api.HostHealthMonitor;
import com.linkedin.alpini.router.api.ResourcePath;
import com.linkedin.alpini.router.api.RouterException;
import com.linkedin.alpini.router.api.ScatterGatherHelper;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import javax.annotation.Nonnull;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 1/8/18.
 */
public class TestScatterGatherRequestHandler4 {
  enum State {
    LEADER, FOLLOWER
  }

  private static final List<List<State>> EMPTY_ROLES = Collections.emptyList();

  @Test(groups = "unit")
  public void testDecodeError() {

    class Path implements ResourcePath<String> {
      @Nonnull
      @Override
      public String getLocation() {
        throw new IllegalStateException();
      }

      @Nonnull
      @Override
      public Collection<String> getPartitionKeys() {
        throw new IllegalStateException();
      }

      @Nonnull
      @Override
      public String getResourceName() {
        throw new IllegalStateException();
      }
    }

    class Parser implements com.linkedin.alpini.router.api.ResourcePathParser<Path, String> {
      @Nonnull
      @Override
      public Path parseResourceUri(@Nonnull String uri) throws RouterException {
        throw new IllegalStateException();
      }

      @Nonnull
      @Override
      public Path substitutePartitionKey(@Nonnull Path path, String s) {
        throw new IllegalStateException();
      }

      @Nonnull
      @Override
      public Path substitutePartitionKey(@Nonnull Path path, @Nonnull Collection<String> s) {
        throw new IllegalStateException();
      }
    }

    class PartitionFinder implements com.linkedin.alpini.router.api.PartitionFinder<String> {
      @Nonnull
      @Override
      public String findPartitionName(@Nonnull String resourceName, @Nonnull String partitionKey)
          throws RouterException {
        throw new IllegalStateException();
      }

      @Nonnull
      @Override
      public List<String> getAllPartitionNames(@Nonnull String resourceName) throws RouterException {
        throw new IllegalStateException();
      }

      @Override
      public int getNumPartitions(@Nonnull String resourceName) throws RouterException {
        throw new IllegalStateException();
      }
    }

    class HostFinder implements com.linkedin.alpini.router.api.HostFinder<InetSocketAddress, List<List<State>>> {
      @Nonnull
      @Override
      public List<InetSocketAddress> findHosts(
          @Nonnull String requestMethod,
          @Nonnull String resourceName,
          @Nonnull String partitionName,
          @Nonnull HostHealthMonitor<InetSocketAddress> hostHealthMonitor,
          @Nonnull List<List<State>> roles) throws RouterException {
        throw new IllegalStateException();
      }

      @Nonnull
      @Override
      public Collection<InetSocketAddress> findAllHosts(@Nonnull String resourceName, List<List<State>> roles)
          throws RouterException {
        throw new IllegalStateException();
      }

      @Nonnull
      @Override
      public Collection<InetSocketAddress> findAllHosts(List<List<State>> roles) throws RouterException {
        throw new IllegalStateException();
      }
    }

    ResourceRegistry registry = new ResourceRegistry();
    TimeoutProcessor timeoutProcessor = new TimeoutProcessor(registry);
    Executor executor = registry.factory(ShutdownableExecutors.class).newSingleThreadExecutor();

    AsyncFullHttpRequestHandler.RequestHandler handler = new ScatterGatherRequestHandler4(
        ScatterGatherHelper.builder()
            .roleFinder((method, httpHeaders) -> EMPTY_ROLES)
            .pathParser(new Parser())
            .partitionFinder(new PartitionFinder())
            .hostFinder(new HostFinder())
            .dispatchHandler(
                (
                    scatter,
                    part,
                    path,
                    request,
                    hostSelected,
                    responseFuture,
                    retryFuture,
                    timeoutFuture,
                    contextExecutor) -> {
                  throw new IllegalStateException();
                })
            .build(),
        timeoutProcessor,
        executor);

    class ReasonException extends RuntimeException {
      private ReasonException(String s) {
        super(s);
      }
    }

    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    BasicFullHttpRequest request = new BasicFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        "bad-request",
        Time.currentTimeMillis(),
        Time.nanoTime());
    request.setDecoderResult(DecoderResult.failure(new ReasonException("Test Foo")));
    AsyncFuture<FullHttpResponse> response = handler.handler(ctx, request);
    Assert.assertTrue(response.isSuccess());
    Assert.assertEquals(response.getNow().status().code(), 400);
    Assert.assertEquals(response.getNow().headers().get(HeaderNames.X_ERROR_CLASS), ReasonException.class.getName());
    Assert.assertEquals(
        response.getNow().headers().get(HeaderNames.X_ERROR_MESSAGE),
        "HTTP decoder error: failure(com.linkedin.alpini.router.TestScatterGatherRequestHandler4$1ReasonException: Test Foo)");
  }
}
