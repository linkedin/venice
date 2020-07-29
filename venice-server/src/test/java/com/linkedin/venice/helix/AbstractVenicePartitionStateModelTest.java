package com.linkedin.venice.helix;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.kafka.consumer.StoreIngestionService;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.stats.AggStoreIngestionStats;
import com.linkedin.venice.stats.AggVersionedStorageIngestionStats;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.utils.TestUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.customizedstate.CustomizedStateProvider;
import org.apache.helix.model.Message;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;


public abstract class AbstractVenicePartitionStateModelTest<MODEL_TYPE extends AbstractParticipantModel,
    NOTIFIER_TYPE extends StateModelNotifier> {
  protected StoreIngestionService mockStoreIngestionService;
  protected StorageService mockStorageService;
  protected VeniceStoreConfig mockStoreConfig;
  protected int testPartition = 0;

  protected Message mockMessage;
  protected NotificationContext mockContext;

  protected MODEL_TYPE testStateModel;

  protected NOTIFIER_TYPE mockNotifier;
  protected ReadOnlyStoreRepository mockReadOnlyStoreRepository;
  protected Store mockStore;
  protected String storeName;
  protected int version = 1;
  protected String resourceName;
  protected String instanceName;

  protected AggStoreIngestionStats mockAggStoreIngestionStats;
  protected AggVersionedStorageIngestionStats mockAggVersionedStorageIngestionStats;
  protected SafeHelixManager mockManager;
  protected HelixManager mockHelixManager;
  protected HelixPartitionStatusAccessor mockPushStatusAccessor;
  protected CustomizedStateProvider mockCustomizedStateProvider;

  @BeforeMethod
  public void setUp() {
    this.storeName =  TestUtils.getUniqueString("stateModelTestStore");
    this.resourceName = Version.composeKafkaTopic(storeName, version);
    this.instanceName = "testInsatnce";

    mockStoreIngestionService = Mockito.mock(StoreIngestionService.class);
    mockStorageService = Mockito.mock(StorageService.class);
    mockStoreConfig = Mockito.mock(VeniceStoreConfig.class);

    mockMessage = Mockito.mock(Message.class);
    mockContext = Mockito.mock(NotificationContext.class);

    mockNotifier = getNotifier();
    mockReadOnlyStoreRepository = Mockito.mock(ReadOnlyStoreRepository.class);
    mockStore = Mockito.mock(Store.class);

    mockManager = Mockito.mock(SafeHelixManager.class);
    mockHelixManager = Mockito.mock(HelixManager.class);

    Mockito.when(mockMessage.getResourceName()).thenReturn(resourceName);
    Mockito.when(mockReadOnlyStoreRepository.getStore(Version.parseStoreFromKafkaTopicName(resourceName)))
        .thenReturn(mockStore);
    Mockito.when(mockStore.getBootstrapToOnlineTimeoutInHours()).thenReturn(Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS);

    Mockito.when(mockStoreIngestionService.getAggStoreIngestionStats()).thenReturn(mockAggStoreIngestionStats);
    Mockito.when(mockStoreIngestionService.getAggVersionedStorageIngestionStats()).thenReturn(mockAggVersionedStorageIngestionStats);

    Mockito.when(mockManager.getOriginalManager()).thenReturn(mockHelixManager);
    Mockito.when(mockManager.getInstanceName()).thenReturn(instanceName);

    mockCustomizedStateProvider = Mockito.mock(CustomizedStateProvider.class);
    mockPushStatusAccessor = Mockito.mock(HelixPartitionStatusAccessor.class);
    mockPushStatusAccessor.setCustomizedStateProvider(mockCustomizedStateProvider);

    testStateModel = getParticipantStateModel();
  }

  protected abstract MODEL_TYPE getParticipantStateModel();

  protected abstract NOTIFIER_TYPE getNotifier();
}
