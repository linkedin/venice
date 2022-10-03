package com.linkedin.alpini.router.impl.netty4;

import com.linkedin.alpini.netty4.handlers.BasicServerChannelInitializer;
import com.linkedin.alpini.netty4.handlers.ConnectionLimitHandler;
import com.linkedin.alpini.netty4.handlers.Http2SettingsFrameLogger;
import com.linkedin.alpini.netty4.http2.Http2PipelineInitializer;
import com.linkedin.alpini.netty4.misc.EventGroupLoopSupplier;
import com.linkedin.alpini.netty4.misc.NettyUtils;
import com.linkedin.alpini.router.ScatterGatherRequestHandler4;
import com.linkedin.alpini.router.api.ResourcePath;
import com.linkedin.alpini.router.impl.RouterPipelineFactory;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http2.ActiveStreamsCountHandler;
import io.netty.util.Timer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class Router4PipelineFactory<C extends Channel> extends
    BasicServerChannelInitializer<C, Router4PipelineFactory<C>> implements RouterPipelineFactory<ChannelHandler> {
  private List<Consumer<ChannelPipeline>> _beforeHttpServerCodec = new ArrayList<>();
  private List<Consumer<ChannelPipeline>> _beforeChunkAggregator = new ArrayList<>();
  private List<Consumer<ChannelPipeline>> _beforeIdleStateHandler = new ArrayList<>();
  private List<Consumer<ChannelPipeline>> _beforeHttpRequestHandler = new ArrayList<>();
  private Http2PipelineInitializer.BuilderSupplier _http2PipelineInitializerBuilder =
      Http2PipelineInitializer.DEFAULT_BUILDER;

  public <H, P extends ResourcePath<K>, K, R> Router4PipelineFactory(
      @Nonnull ConnectionLimitHandler connectionLimit,
      @Nonnull ActiveStreamsCountHandler activeStreamsCountHandler,
      @Nonnull Http2SettingsFrameLogger http2SettingsFrameLogger,
      // @Nonnull ExecutionHandler executionHandler,
      @Nonnull Timer idleTimer,
      @Nonnull BooleanSupplier shutdownFlag,
      @Nonnull BooleanSupplier busyAutoReadDisable,
      @Nonnull ScatterGatherRequestHandler4<H, P, K, R> scatterGatherRequestHandler) {
    super(
        connectionLimit,
        activeStreamsCountHandler,
        http2SettingsFrameLogger,
        /*executionHandler,*/ idleTimer,
        shutdownFlag,
        busyAutoReadDisable,
        scatterGatherRequestHandler);
    _beforeHttpServerCodec.add(
        channelPipeline -> scatterGatherRequestHandler.getScatterGatherHelper()
            .forEachBeforeHttpServerCodec(
                entry -> channelPipeline.addLast(
                    NettyUtils.executorGroup(channelPipeline),
                    entry.getFirst(),
                    (ChannelHandler) entry.getSecond())));
    _beforeChunkAggregator.add(
        channelPipeline -> scatterGatherRequestHandler.getScatterGatherHelper()
            .forEachBeforeChunkAggregator(
                entry -> channelPipeline.addLast(
                    NettyUtils.executorGroup(channelPipeline),
                    entry.getFirst(),
                    (ChannelHandler) entry.getSecond())));
    _beforeIdleStateHandler.add(
        channelPipeline -> scatterGatherRequestHandler.getScatterGatherHelper()
            .forEachBeforeIdleStateHandler(
                entry -> channelPipeline.addLast(
                    NettyUtils.executorGroup(channelPipeline),
                    entry.getFirst(),
                    (ChannelHandler) entry.getSecond())));
    _beforeHttpRequestHandler.add(
        channelPipeline -> scatterGatherRequestHandler.getScatterGatherHelper()
            .forEachBeforeHttpRequestHandler(
                entry -> channelPipeline.addLast(
                    NettyUtils.executorGroup(channelPipeline),
                    entry.getFirst(),
                    (ChannelHandler) entry.getSecond())));
  }

  void addLast(ChannelPipeline channelPipeline, String handlerName, ChannelHandler channelHandler) {
    if (channelHandler instanceof EventGroupLoopSupplier) {
      channelPipeline
          .addLast(((EventGroupLoopSupplier) channelHandler).getEventLoopGroup(), handlerName, channelHandler);
    } else {
      channelPipeline.addLast(NettyUtils.executorGroup(channelPipeline), handlerName, channelHandler);
    }
  }

  private Router4PipelineFactory<C> add(
      List<Consumer<ChannelPipeline>> list,
      Consumer<ChannelPipeline> pipelineConsumer) {
    Objects.requireNonNull(pipelineConsumer, "pipelineConsumer");
    list.add(pipelineConsumer);
    return factory();
  }

  private Router4PipelineFactory<C> add(
      List<Consumer<ChannelPipeline>> list,
      String name,
      Supplier<? extends ChannelHandler> supplier) {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(supplier, "supplier");
    return add(
        list,
        channelPipeline -> Optional.ofNullable(supplier.get())
            .ifPresent(handler -> channelPipeline.addLast(NettyUtils.executorGroup(channelPipeline), name, handler)));
  }

  public Router4PipelineFactory<C> addBeforeHttpServerCodec(
      @Nonnull String name,
      @Nonnull Supplier<? extends ChannelHandler> supplier) {
    return add(_beforeHttpServerCodec, name, supplier);
  }

  public Router4PipelineFactory<C> addBeforeHttpServerCodec(@Nonnull Consumer<ChannelPipeline> pipelineConsumer) {
    return add(_beforeHttpServerCodec, pipelineConsumer);
  }

  public Router4PipelineFactory<C> addBeforeChunkAggregator(String name, Supplier<? extends ChannelHandler> supplier) {
    return add(_beforeChunkAggregator, name, supplier);
  }

  public Router4PipelineFactory<C> addBeforeChunkAggregator(@Nonnull Consumer<ChannelPipeline> pipelineConsumer) {
    return add(_beforeChunkAggregator, pipelineConsumer);
  }

  public Router4PipelineFactory<C> addBeforeIdleStateHandler(String name, Supplier<? extends ChannelHandler> supplier) {
    return add(_beforeIdleStateHandler, name, supplier);
  }

  public Router4PipelineFactory<C> addBeforeIdleStateHandler(@Nonnull Consumer<ChannelPipeline> pipelineConsumer) {
    return add(_beforeIdleStateHandler, pipelineConsumer);
  }

  public Router4PipelineFactory<C> addBeforeHttpRequestHandler(
      String name,
      Supplier<? extends ChannelHandler> supplier) {
    return add(_beforeHttpRequestHandler, name, supplier);
  }

  public Router4PipelineFactory<C> addBeforeHttpRequestHandler(@Nonnull Consumer<ChannelPipeline> pipelineConsumer) {
    return add(_beforeHttpRequestHandler, pipelineConsumer);
  }

  private static void apply(List<Consumer<ChannelPipeline>> list, ChannelPipeline pipeline) {
    list.forEach(consumer -> consumer.accept(pipeline));
  }

  public Router4PipelineFactory<C> setHttp2PipelineInitializer(Http2PipelineInitializer.BuilderSupplier supplier) {
    _http2PipelineInitializerBuilder = Optional.ofNullable(supplier).orElse(Http2PipelineInitializer.DEFAULT_BUILDER);
    return factory();
  }

  @Override
  protected Http2PipelineInitializer.BuilderSupplier getHttp2PipelineInitializerBuilderSupplier() {
    return _http2PipelineInitializerBuilder;
  }

  @Override
  protected void beforeHttpServerCodec(@Nonnull ChannelPipeline pipeline) {
    super.beforeHttpServerCodec(pipeline);
    apply(_beforeHttpServerCodec, pipeline);
  }

  @Override
  protected void beforeChunkAggregator(@Nonnull ChannelPipeline pipeline) {
    super.beforeChunkAggregator(pipeline);
    apply(_beforeChunkAggregator, pipeline);
  }

  @Override
  protected void beforeIdleStateHandler(@Nonnull ChannelPipeline pipeline) {
    super.beforeIdleStateHandler(pipeline);
    apply(_beforeIdleStateHandler, pipeline);
  }

  @Override
  protected void beforeHttpRequestHandler(@Nonnull ChannelPipeline pipeline) {
    super.beforeHttpRequestHandler(pipeline);
    apply(_beforeHttpRequestHandler, pipeline);
  }
}
