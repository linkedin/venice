package com.linkedin.alpini.router.impl;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.alpini.base.registry.ResourceRegistry;
import com.linkedin.alpini.base.registry.ShutdownableResource;
import com.linkedin.alpini.router.api.Netty;
import com.linkedin.alpini.router.api.ResourcePath;
import com.linkedin.alpini.router.api.RouterTimeoutProcessor;
import com.linkedin.alpini.router.api.ScatterGatherHelper;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface Router extends ShutdownableResource {
  Netty nettyVersion();

  AsyncFuture<SocketAddress> start(@Nonnull SocketAddress address);

  AsyncFuture<SocketAddress> getLocalAddress();

  AsyncFuture<Void> setAcceptConnection(boolean enabled);

  int getConnectedCount();

  int getActiveStreams();

  long getRstErrorCount();

  static <H, P extends ResourcePath<K>, K, R> Builder builder(
      @Nonnull ScatterGatherHelper<H, P, K, R, ?, ?, ?> scatterGatherHelper) {
    String builderClassName;
    switch (scatterGatherHelper.dispatcherNettyVersion()) {
      case NETTY_3:
        builderClassName = "com.linkedin.alpini.router.impl.netty3.Router3";
        break;
      case NETTY_4_1:
        builderClassName = "com.linkedin.alpini.router.impl.netty4.Router4";
        break;
      default:
        throw new IllegalStateException();
    }
    try {
      return Class.forName(builderClassName, false, Router.class.getClassLoader())
          .asSubclass(Builder.class)
          .getConstructor(ScatterGatherHelper.class)
          .newInstance(scatterGatherHelper);
    } catch (Exception ex) {
      throw new IllegalStateException(ex);
    }
  }

  interface Builder {
    int MINIMUM_MAX_HEADER_SIZE = 256;
    int MINIMUM_MAX_INITIAL_LINE_LENGTH = 256;
    int MINIMUM_MAX_CHUNK_SIZE = 256;
    long MINIMUM_MAX_CONTENT_LENGTH = 0L;
    long MINIMUM_IDLE_TIMEOUT_MILLIS = 1000L;
    long HANDSHAKE_TIMEOUT_MILLIS = 60000L;

    Netty nettyVersion();

    Builder name(@Nonnull String name);

    Builder resourceRegistry(@Nonnull ResourceRegistry resourceRegistry);

    Builder threadFactory(@Nonnull ThreadFactory threadFactory);

    Builder serverSocketChannel(@Nonnull Class<?> serverSocketChannel);

    Builder bossPoolSize(@Nonnegative int bossPoolSize);

    Builder ioWorkerPoolSize(@Nonnegative int ioWorkerPoolSize);

    Builder executor(@Nonnull Executor executor);

    Builder workerExecutor(@Nonnull Executor workerExecutor);

    Builder appWorkerCorePoolSize(@Nonnegative int corePoolSize);

    Builder appWorkerChannelMaxMemorySize(@Nonnegative long maxChannelMemorySize);

    Builder appWorkerTotalMaxMemorySize(@Nonnegative long maxTotalMemorySize);

    Builder appWorkerKeepAliveSeconds(@Nonnegative long appWorkerKeepAliveSeconds);

    Builder timeoutProcessor(@Nonnull RouterTimeoutProcessor timeoutProcessor);

    default Builder timeoutProcessor(@Nonnull TimeoutProcessor timeoutProcessor) {
      return timeoutProcessor(RouterTimeoutProcessor.adapt(timeoutProcessor));
    }

    Builder connectionLimit(@Nonnegative int connectionLimit);

    Builder connectionLimit(@Nonnull IntSupplier connectionLimit);

    Builder serverSocketOptions(Map<String, Object> serverSocketOptions);

    Builder serverSocketOptions(@Nonnull String key, Object value);

    Builder maxHeaderSize(@Nonnegative int maxHeaderSize);

    Builder maxInitialLineLength(@Nonnegative int maxInitialLineLength);

    Builder maxChunkSize(@Nonnegative int maxChunkSize);

    Builder maxContentLength(@Nonnegative long maxContentLength);

    Builder idleTimeout(@Nonnegative long time, @Nonnull TimeUnit unit);

    Builder handshakeTimeout(@Nonnegative long time, @Nonnull TimeUnit unit);

    /* HTTP/2 Settings */
    default Builder enableInboundHttp2(boolean enableHttp2) {
      // Do Nothing
      return this;
    }

    default Builder http2MaxConcurrentStreams(int maxConcurrentStreams) {
      // Do Nothing
      return this;
    }

    default Builder http2MaxFrameSize(int http2MaxFrameSize) {
      // Do Nothing
      return this;
    }

    default Builder http2InitialWindowSize(int http2InitialWindowSize) {
      // Do Nothing
      return this;
    }

    default Builder http2HeaderTableSize(int http2HeaderTableSize) {
      // Do Nothing
      return this;
    }

    default Builder http2MaxHeaderListSize(int http2MaxHeaderListSize) {
      // Do Nothing
      return this;
    }

    default Builder useCustomMultiplexHandler(boolean useCustomMultiplexHandler) {
      // Do Nothing
      return this;
    }

    default <T> Builder addBootstrapFilter(Function<T, T> function) {
      // Do nothing
      return this;
    }

    <CHANNEL_HANDLER> PipelineFactory<CHANNEL_HANDLER> pipelineFactory(@Nonnull Class<CHANNEL_HANDLER> handlerClass);

    <CHANNEL_PIPELINE> Builder beforeHttpServerCodec(
        @Nonnull Class<CHANNEL_PIPELINE> pipelineClass,
        @Nonnull Consumer<CHANNEL_PIPELINE> consumer);

    <CHANNEL_PIPELINE> Builder beforeChunkAggregator(
        @Nonnull Class<CHANNEL_PIPELINE> pipelineClass,
        @Nonnull Consumer<CHANNEL_PIPELINE> consumer);

    <CHANNEL_PIPELINE> Builder beforeIdleStateHandler(
        @Nonnull Class<CHANNEL_PIPELINE> pipelineClass,
        @Nonnull Consumer<CHANNEL_PIPELINE> consumer);

    <CHANNEL_PIPELINE> Builder beforeHttpRequestHandler(
        @Nonnull Class<CHANNEL_PIPELINE> pipelineClass,
        @Nonnull Consumer<CHANNEL_PIPELINE> consumer);

    <POOL_TYPE> Builder ioWorkerPoolBuilder(
        @Nonnull Class<POOL_TYPE> poolClass,
        @Nonnull Function<Executor, POOL_TYPE> builder);

    <POOL_TYPE> Builder bossPoolBuilder(
        @Nonnull Class<POOL_TYPE> poolClass,
        @Nonnull Function<Executor, POOL_TYPE> builder);

    @CheckReturnValue
    Router build();
  }

  interface PipelineFactory<CHANNEL_HANDLER> {
    PipelineFactory<CHANNEL_HANDLER> addBeforeHttpServerCodec(
        @Nonnull String name,
        @Nonnull Supplier<? extends CHANNEL_HANDLER> supplier);

    PipelineFactory<CHANNEL_HANDLER> addBeforeChunkAggregator(
        @Nonnull String name,
        @Nonnull Supplier<? extends CHANNEL_HANDLER> supplier);

    PipelineFactory<CHANNEL_HANDLER> addBeforeIdleStateHandler(
        @Nonnull String name,
        @Nonnull Supplier<? extends CHANNEL_HANDLER> supplier);

    PipelineFactory<CHANNEL_HANDLER> addBeforeHttpRequestHandler(
        @Nonnull String name,
        @Nonnull Supplier<? extends CHANNEL_HANDLER> supplier);

    Builder builder();
  }

}
