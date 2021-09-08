package com.linkedin.davinci.helix;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.ingestion.VeniceIngestionBackend;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.HelixUtils;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class LeaderFollowerParticipantModelFactoryTest {
  private VeniceIngestionBackend mockIngestionBackend;
  private VeniceConfigLoader mockConfigLoader;
  private VeniceServerConfig mockServerConfig;
  private VeniceStoreConfig mockStoreConfig;
  private ReadOnlyStoreRepository mockReadOnlyStoreRepository;
  private Store mockStore;
  private int testPartition = 0;
  private String resourceName = Version.composeKafkaTopic("testStore", 1);

  private Message mockMessage;

  private LeaderFollowerPartitionStateModelFactory factory;
  private ExecutorService executorService;

  @BeforeClass
  void setup() {
    executorService = Executors.newCachedThreadPool(new DaemonThreadFactory("venice-unittest"));
  }

  @AfterClass
  void cleanup() {
    executorService.shutdownNow();
  }

  @BeforeMethod
  public void setupTestCase() {
    mockIngestionBackend = Mockito.mock(VeniceIngestionBackend.class);
    mockConfigLoader = Mockito.mock(VeniceConfigLoader.class);
    mockServerConfig = Mockito.mock(VeniceServerConfig.class);
    mockStoreConfig = Mockito.mock(VeniceStoreConfig.class);
    mockReadOnlyStoreRepository = Mockito.mock(ReadOnlyStoreRepository.class);
    mockStore = Mockito.mock(Store.class);
    Mockito.when(mockConfigLoader.getVeniceServerConfig()).thenReturn(mockServerConfig);
    Mockito.when(mockConfigLoader.getStoreConfig(resourceName)).thenReturn(mockStoreConfig);
    Mockito.when(mockStoreConfig.getStoreName()).thenReturn(resourceName);
    Mockito.when(mockReadOnlyStoreRepository.getStore(Version.parseStoreFromKafkaTopicName(resourceName)))
        .thenReturn(mockStore);
    Mockito.when(mockStore.getBootstrapToOnlineTimeoutInHours()).thenReturn(Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS);

    mockMessage = Mockito.mock(Message.class);
    Mockito.when(mockMessage.getResourceName()).thenReturn(resourceName);

    factory = new LeaderFollowerPartitionStateModelFactory(
        mockIngestionBackend,
        mockConfigLoader,
        this.executorService,
        mockReadOnlyStoreRepository, Optional.empty(), null);
  }

  @Test
  public void testLeaderFollowerStateModelCanBeBuiltWithoutErrors() {
    /**
     * No exception is expected from building a simple state model; building a state model should remain as simple
     * as possible, we shouldn't bind state model creation with the healthiness of other components like ZK.
     */
    String partitionName = HelixUtils.getPartitionName(resourceName, testPartition);
    StateModel stateModel = factory.createNewStateModel(resourceName, partitionName);
  }

  @Test
  public void testLeaderFollowerStateModelCanBeBuiltWhenMetaRepoThrows() {
    String partitionName = HelixUtils.getPartitionName(resourceName, testPartition);
    Mockito.when(mockReadOnlyStoreRepository.getStore(Version.parseStoreFromKafkaTopicName(resourceName))).thenThrow(new VeniceException());
    StateModel stateModel = factory.createNewStateModel(resourceName, partitionName);
  }
}
