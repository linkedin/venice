package com.linkedin.davinci.helix;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.ingestion.IngestionBackend;
import com.linkedin.davinci.stats.ParticipantStateTransitionStats;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatMonitoringService;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.TestUtils;
import io.tehuti.metrics.MetricsRepository;
import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.helix.model.Message;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class LeaderFollowerParticipantModelFactoryTest {
  private IngestionBackend mockIngestionBackend;
  private VeniceConfigLoader mockConfigLoader;
  private VeniceServerConfig mockServerConfig;
  private VeniceStoreVersionConfig mockStoreConfig;
  private ReadOnlyStoreRepository mockReadOnlyStoreRepository;
  private HelixCustomizedViewOfflinePushRepository mockCustomizedViewRepository;
  private Store mockStore;
  private int testPartition = 0;
  private String resourceName = Version.composeKafkaTopic("testStore", 1);

  private Message mockMessage;

  private LeaderFollowerPartitionStateModelFactory factory;
  private ExecutorService executorService;

  @BeforeClass
  void setUp() {
    executorService = Executors.newCachedThreadPool(new DaemonThreadFactory("venice-unittest"));
  }

  @AfterClass
  void cleanUp() throws Exception {
    TestUtils.shutdownExecutor(executorService);
  }

  @BeforeMethod
  public void setupTestCase() {
    mockIngestionBackend = mock(IngestionBackend.class);
    mockConfigLoader = mock(VeniceConfigLoader.class);
    mockServerConfig = mock(VeniceServerConfig.class);
    mockStoreConfig = mock(VeniceStoreVersionConfig.class);
    mockReadOnlyStoreRepository = mock(ReadOnlyStoreRepository.class);
    mockCustomizedViewRepository = mock(HelixCustomizedViewOfflinePushRepository.class);
    mockStore = mock(Store.class);
    Mockito.when(mockConfigLoader.getVeniceServerConfig()).thenReturn(mockServerConfig);
    Mockito.when(mockConfigLoader.getStoreConfig(resourceName)).thenReturn(mockStoreConfig);
    Mockito.when(mockStoreConfig.getStoreVersionName()).thenReturn(resourceName);
    Mockito.when(mockReadOnlyStoreRepository.getStore(Version.parseStoreFromKafkaTopicName(resourceName)))
        .thenReturn(mockStore);
    Mockito.when(mockStore.getBootstrapToOnlineTimeoutInHours()).thenReturn(Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS);

    mockMessage = mock(Message.class);
    Mockito.when(mockMessage.getResourceName()).thenReturn(resourceName);
    VeniceServerConfig veniceServerConfig = mock(VeniceServerConfig.class);
    doReturn(new HashSet<>()).when(veniceServerConfig).getRegionNames();
    doReturn("local").when(veniceServerConfig).getRegionName();
    doReturn(Duration.ofSeconds(5)).when(veniceServerConfig).getServerMaxWaitForVersionInfo();
    factory = new LeaderFollowerPartitionStateModelFactory(
        mockIngestionBackend,
        mockConfigLoader,
        this.executorService,
        mock(ParticipantStateTransitionStats.class),
        mockReadOnlyStoreRepository,
        null,
        null,
        new HeartbeatMonitoringService(
            new MetricsRepository(),
            mockReadOnlyStoreRepository,
            veniceServerConfig,
            null,
            CompletableFuture.completedFuture(mockCustomizedViewRepository)));
  }

  @Test
  public void testLeaderFollowerStateModelCanBeBuiltWithoutErrors() {
    /**
     * No exception is expected from building a simple state model; building a state model should remain as simple
     * as possible, we shouldn't bind state model creation with the healthiness of other components like ZK.
     */
    String partitionName = HelixUtils.getPartitionName(resourceName, testPartition);
    factory.createNewStateModel(resourceName, partitionName);
  }

  @Test
  public void testLeaderFollowerStateModelCanBeBuiltWhenMetaRepoThrows() {
    String partitionName = HelixUtils.getPartitionName(resourceName, testPartition);
    Mockito.when(mockReadOnlyStoreRepository.getStore(Version.parseStoreFromKafkaTopicName(resourceName)))
        .thenThrow(new VeniceException());
    factory.createNewStateModel(resourceName, partitionName);
  }
}
