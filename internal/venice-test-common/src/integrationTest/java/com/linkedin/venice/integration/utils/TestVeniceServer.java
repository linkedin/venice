package com.linkedin.venice.integration.utils;

import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.storage.DiskHealthCheckService;
import com.linkedin.davinci.storage.MetadataRetriever;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.acl.StaticAccessController;
import com.linkedin.venice.cleaner.ResourceReadUsageTracker;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.listener.ListenerService;
import com.linkedin.venice.listener.StorageReadRequestsHandler;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.server.VeniceServer;
import com.linkedin.venice.server.VeniceServerContext;
import io.netty.channel.ChannelHandlerContext;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;


public class TestVeniceServer extends VeniceServer {
  public interface RequestHandler {
    boolean handleRequest(ChannelHandlerContext context, Object message);
  }

  private AtomicReference<RequestHandler> requestHandler = new AtomicReference<>();

  public TestVeniceServer(VeniceServerContext serverContext) {
    super(serverContext);
  }

  @Override
  protected ListenerService createListenerService(
      StorageEngineRepository storageEngineRepository,
      ReadOnlyStoreRepository storeMetadataRepository,
      ReadOnlySchemaRepository schemaRepository,
      CompletableFuture<HelixCustomizedViewOfflinePushRepository> customizedViewRepository,
      MetadataRetriever metadataRetriever,
      VeniceServerConfig serverConfig,
      MetricsRepository metricsRepository,
      Optional<SSLFactory> sslFactory,
      Optional<StaticAccessController> routerAccessController,
      Optional<DynamicAccessController> storeAccessController,
      DiskHealthCheckService diskHealthService,
      StorageEngineBackedCompressorFactory compressorFactory,
      Optional<ResourceReadUsageTracker> resourceReadUsageTracker) {

    return new ListenerService(
        storageEngineRepository,
        storeMetadataRepository,
        schemaRepository,
        customizedViewRepository,
        metadataRetriever,
        serverConfig,
        metricsRepository,
        sslFactory,
        routerAccessController,
        storeAccessController,
        diskHealthService,
        compressorFactory,
        resourceReadUsageTracker) {
      @Override
      protected StorageReadRequestsHandler createRequestHandler(
          ThreadPoolExecutor executor,
          ThreadPoolExecutor computeExecutor,
          StorageEngineRepository storageEngineRepository,
          ReadOnlyStoreRepository metadataRepository,
          ReadOnlySchemaRepository schemaRepository,
          MetadataRetriever metadataRetriever,
          DiskHealthCheckService diskHealthService,
          boolean fastAvroEnabled,
          boolean parallelBatchGetEnabled,
          int parallelBatchGetChunkSize,
          StorageEngineBackedCompressorFactory compressorFactory,
          Optional<ResourceReadUsageTracker> resourceReadUsageTracker) {

        return new StorageReadRequestsHandler(
            executor,
            computeExecutor,
            storageEngineRepository,
            metadataRepository,
            schemaRepository,
            metadataRetriever,
            diskHealthService,
            fastAvroEnabled,
            parallelBatchGetEnabled,
            parallelBatchGetChunkSize,
            serverConfig,
            compressorFactory,
            resourceReadUsageTracker) {
          @Override
          public void channelRead(ChannelHandlerContext context, Object message) throws Exception {
            RequestHandler handler = requestHandler.get();
            if (handler == null || !handler.handleRequest(context, message)) {
              super.channelRead(context, message);
            }
          }
        };
      }

    };
  }

  public void setRequestHandler(RequestHandler handler) {
    requestHandler.set(handler);
  }
}
