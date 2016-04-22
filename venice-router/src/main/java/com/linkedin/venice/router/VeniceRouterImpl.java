package com.linkedin.venice.router;

import com.linkedin.ddsstorage.base.concurrency.TimeoutProcessor;
import com.linkedin.ddsstorage.netty3.handlers.ConnectionLimitUpstreamHandler;
import com.linkedin.ddsstorage.router.ScatterGatherRequestHandler;
import com.linkedin.ddsstorage.router.api.ResourcePath;
import com.linkedin.ddsstorage.router.api.ScatterGatherHelper;
import com.linkedin.ddsstorage.router.impl.RouterImpl;
import com.linkedin.ddsstorage.router.impl.RouterPipelineFactory;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.socket.nio.NioServerBossPool;
import org.jboss.netty.channel.socket.nio.NioWorkerPool;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.util.Timer;


/**
 * Created by mwise on 4/22/16.
 */
public class VeniceRouterImpl extends RouterImpl {

  private final ChannelHandler beforeHttpHandler;

  public <H, P extends ResourcePath<K>, K, R> VeniceRouterImpl(
      String name,
      NioServerBossPool serverBossPool,
      NioWorkerPool ioWorkerPool,
      ExecutionHandler workerExecutor,
      ConnectionLimitUpstreamHandler connectionLimit,
      TimeoutProcessor timeoutProcessor,
      Timer nettyTimer, Map<String, Object> serverSocketOptions,
      ScatterGatherHelper<H, P, K, R> scatterGatherHelper,
      ChannelHandler beforeHttpHandler) {
    super(name,
        serverBossPool,
        ioWorkerPool,
        workerExecutor,
        connectionLimit,
        timeoutProcessor,
        nettyTimer,
        serverSocketOptions,
        scatterGatherHelper);
    this.beforeHttpHandler = beforeHttpHandler;
  }

  @Override
  protected <H, P extends ResourcePath<K>, K, R> RouterPipelineFactory constructRouterPipelineFactory(
      @Nonnull ScatterGatherRequestHandler<H, P, K, R> scatterGatherRequestHandler) {
    RouterPipelineFactory factory = new RouterPipelineFactory(
        getConnectionLimit(),
        getWorkerExecutionHandler(),
        getNettyTimer(),
        this::isShutdown,
        scatterGatherRequestHandler);
    factory.addBeforeHttpRequestHandler("extra-venice-handler", new Supplier<ChannelHandler>() {
      @Override
      public ChannelHandler get() {
        return beforeHttpHandler;
      }
    });
    return factory;
  }
}
