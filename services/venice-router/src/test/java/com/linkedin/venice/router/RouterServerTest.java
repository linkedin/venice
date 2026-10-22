package com.linkedin.venice.router;

import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.CLUSTER_TO_D2;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.LISTENER_PORT;
import static com.linkedin.venice.ConfigKeys.LISTENER_SSL_PORT;
import static com.linkedin.venice.ConfigKeys.ROUTER_ENABLE_SSL;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.alpini.base.registry.ResourceRegistry;
import com.linkedin.alpini.router.impl.Router;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.helix.HelixLiveInstanceMonitor;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreViewConfigRepositoryAdapter;
import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.router.api.DictionaryRetrievalService;
import com.linkedin.venice.router.api.VeniceDispatcher;
import com.linkedin.venice.router.httpclient.StorageNodeClient;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.VeniceProperties;
import io.netty.channel.MultithreadEventLoopGroup;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.Optional;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class RouterServerTest {
  @DataProvider
  public Object[][] readerInitialized() {
    return new Object[][] { { true }, { false } };
  }

  @Test(dataProvider = "readerInitialized")
  public void testShutdownClosesReaderBeforeClearingRepositories(boolean readerInitialized) throws Exception {
    VeniceProperties properties = new PropertyBuilder().put(CLUSTER_NAME, "test-cluster")
        .put(LISTENER_PORT, 1)
        .put(LISTENER_SSL_PORT, 2)
        .put(ZOOKEEPER_ADDRESS, "localhost:1234")
        .put(KAFKA_BOOTSTRAP_SERVERS, "localhost:2345")
        .put(CLUSTER_TO_D2, "test-cluster:test-d2")
        .put(ROUTER_ENABLE_SSL, false)
        .build();
    ShutdownResources resources = new ShutdownResources();
    MetricsRepository metrics = new MetricsRepository();
    try (MockedConstruction<ZkClient> zkClients = mockConstruction(ZkClient.class);
        MockedConstruction<PushStatusStoreReader> readers = mockConstruction(PushStatusStoreReader.class)) {
      resources.server = readerInitialized
          ? new RouterServer(
              properties,
              Collections.emptyList(),
              Optional.empty(),
              Optional.empty(),
              metrics,
              null,
              "test-discovery")
          : new RouterServer(
              properties,
              mock(HelixCustomizedViewOfflinePushRepository.class),
              Optional.empty(),
              mock(HelixReadOnlyStoreRepository.class),
              mock(HelixReadOnlySchemaRepository.class),
              mock(HelixReadOnlyStoreViewConfigRepositoryAdapter.class),
              Collections.emptyList(),
              Optional.empty(),
              mock(HelixLiveInstanceMonitor.class));
      try (AutoCloseable injected = MockitoAnnotations.openMocks(resources)) {
        when(resources.inFlightRequestStat.getInFlightRequestRate()).thenReturn(1.0, 0.0);
        resources.server.stopInner();

        if (readerInitialized) {
          assertEquals(readers.constructed().size(), 1);
          PushStatusStoreReader reader = readers.constructed().get(0);
          InOrder order = inOrder(
              resources.inFlightRequestStat,
              resources.secureRouter,
              reader,
              resources.metadataRepository,
              resources.schemaRepository,
              resources.storeConfigRepository);
          order.verify(resources.inFlightRequestStat, times(2)).getInFlightRequestRate();
          order.verify(resources.secureRouter).waitForShutdown();
          order.verify(reader).close();
          order.verify(resources.metadataRepository).clear();
          order.verify(resources.schemaRepository).clear();
          order.verify(resources.storeConfigRepository).clear();
        } else {
          assertEquals(readers.constructed().size(), 0);
        }
      }
    } finally {
      metrics.close();
    }
  }

  private static class ShutdownResources {
    @InjectMocks
    private RouterServer server;
    @Mock
    private HelixCustomizedViewOfflinePushRepository routingDataRepository;
    @Mock
    private HelixReadOnlyStoreRepository metadataRepository;
    @Mock
    private HelixReadOnlySchemaRepository schemaRepository;
    @Mock
    private HelixReadOnlyStoreViewConfigRepositoryAdapter storeConfigRepository;
    @Mock
    private HelixLiveInstanceMonitor liveInstanceMonitor;
    @Mock
    private StorageNodeClient storageNodeClient;
    @Mock
    private MultithreadEventLoopGroup workerEventLoopGroup;
    @Mock
    private MultithreadEventLoopGroup serverEventLoopGroup;
    @Mock
    private VeniceDispatcher dispatcher;
    @Mock
    private Router secureRouter;
    @Mock
    private ResourceRegistry registry;
    @Mock
    private ZkRoutersClusterManager routersClusterManager;
    @Mock
    private DictionaryRetrievalService dictionaryRetrievalService;
    @Mock
    private TimeoutProcessor timeoutProcessor;
    @Mock
    private InFlightRequestStat inFlightRequestStat;
  }
}
