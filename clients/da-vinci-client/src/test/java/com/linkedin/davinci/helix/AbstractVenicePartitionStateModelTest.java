package com.linkedin.davinci.helix;

import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.ingestion.VeniceIngestionBackend;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.stats.AggVersionedIngestionStats;
import com.linkedin.davinci.stats.ParticipantStateTransitionStats;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.customizedstate.CustomizedStateProvider;
import org.apache.helix.model.Message;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;


public abstract class AbstractVenicePartitionStateModelTest<MODEL_TYPE extends AbstractPartitionStateModel, NOTIFIER_TYPE extends StateModelIngestionProgressNotifier> {
  protected KafkaStoreIngestionService mockStoreIngestionService;
  protected StorageService mockStorageService;
  protected VeniceIngestionBackend mockIngestionBackend;
  protected VeniceStoreVersionConfig mockStoreConfig;
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

  protected AggVersionedIngestionStats mockAggVersionedIngestionStats;
  protected SafeHelixManager mockManager;
  protected HelixManager mockHelixManager;
  protected HelixPartitionStatusAccessor mockPushStatusAccessor;
  protected CustomizedStateProvider mockCustomizedStateProvider;
  protected ParticipantStateTransitionStats mockParticipantStateTransitionStats;

  @BeforeMethod
  public void setUp() {
    this.storeName = Utils.getUniqueString("stateModelTestStore");
    this.resourceName = Version.composeKafkaTopic(storeName, version);
    this.instanceName = "testInstance";

    mockStoreIngestionService = Mockito.mock(KafkaStoreIngestionService.class);
    mockStorageService = Mockito.mock(StorageService.class);
    mockIngestionBackend = Mockito.mock(VeniceIngestionBackend.class);
    Mockito.when(mockIngestionBackend.getStorageService()).thenReturn(mockStorageService);
    Mockito.when(mockIngestionBackend.getStoreIngestionService()).thenReturn(mockStoreIngestionService);
    mockStoreConfig = Mockito.mock(VeniceStoreVersionConfig.class);
    Mockito.when(mockStoreConfig.getPartitionGracefulDropDelaySeconds()).thenReturn(1); // 1 second.
    Mockito.when(mockStoreConfig.getStoreVersionName()).thenReturn(resourceName);
    mockParticipantStateTransitionStats = Mockito.mock(ParticipantStateTransitionStats.class);

    mockAggVersionedIngestionStats = Mockito.mock(AggVersionedIngestionStats.class);
    mockMessage = Mockito.mock(Message.class);
    mockContext = Mockito.mock(NotificationContext.class);

    mockNotifier = getNotifier();
    mockReadOnlyStoreRepository = Mockito.mock(ReadOnlyStoreRepository.class);
    mockStore = Mockito.mock(Store.class);

    mockManager = Mockito.mock(SafeHelixManager.class);
    mockHelixManager = Mockito.mock(HelixManager.class);

    Mockito.when(mockMessage.getResourceName()).thenReturn(resourceName);
    Mockito.when(mockReadOnlyStoreRepository.getStoreOrThrow(Version.parseStoreFromKafkaTopicName(resourceName)))
        .thenReturn(mockStore);
    Mockito.when(mockStore.getBootstrapToOnlineTimeoutInHours()).thenReturn(Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS);

    Mockito.when(mockStoreIngestionService.getAggVersionedIngestionStats()).thenReturn(mockAggVersionedIngestionStats);

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
