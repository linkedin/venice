package com.linkedin.davinci.helix;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.ingestion.IngestionBackend;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.stats.AggVersionedIngestionStats;
import com.linkedin.davinci.stats.ParticipantStateTransitionStats;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.VeniceStoreType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import java.util.concurrent.CompletableFuture;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.customizedstate.CustomizedStateProvider;
import org.apache.helix.model.Message;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public abstract class AbstractVenicePartitionStateModelTest<MODEL_TYPE extends AbstractPartitionStateModel, NOTIFIER_TYPE extends LeaderFollowerIngestionProgressNotifier> {
  protected KafkaStoreIngestionService mockStoreIngestionService;
  protected IngestionBackend mockIngestionBackend;
  protected VeniceStoreVersionConfig mockStoreConfig;
  protected int testPartition = 0;

  protected Message mockMessage;
  protected Message mockSystemStoreMessage;
  protected NotificationContext mockContext;

  protected MODEL_TYPE testStateModel;

  protected NOTIFIER_TYPE mockNotifier;
  protected ReadOnlyStoreRepository mockReadOnlyStoreRepository;
  protected Store mockStore;
  protected Store mockSystemStore;
  protected String storeName;
  protected String systemStoreName;
  protected int version = 1;
  protected String resourceName;
  protected String systemStoreResourceName;
  protected String instanceName;

  protected AggVersionedIngestionStats mockAggVersionedIngestionStats;
  protected SafeHelixManager mockManager;
  protected HelixManager mockHelixManager;
  protected HelixPartitionStatusAccessor mockPushStatusAccessor;
  protected CustomizedStateProvider mockCustomizedStateProvider;
  protected ParticipantStateTransitionStats mockParticipantStateTransitionStats;

  @BeforeMethod
  public void setUp() throws InterruptedException {
    this.storeName = Utils.getUniqueString("stateModelTestStore");
    this.systemStoreName =
        VeniceSystemStoreUtils.getDaVinciPushStatusStoreName(Utils.getUniqueString("stateModelTestStore"));
    this.resourceName = Version.composeKafkaTopic(storeName, version);
    this.systemStoreResourceName = Version.composeKafkaTopic(systemStoreName, version);
    this.instanceName = "testInstance";

    mockStoreIngestionService = Mockito.mock(KafkaStoreIngestionService.class);
    mockIngestionBackend = Mockito.mock(IngestionBackend.class);
    when(mockIngestionBackend.getStoreIngestionService()).thenReturn(mockStoreIngestionService);
    when(
        mockIngestionBackend
            .dropStoragePartitionGracefully(Mockito.any(), Mockito.anyInt(), Mockito.anyInt(), Mockito.anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
    mockStoreConfig = Mockito.mock(VeniceStoreVersionConfig.class);
    when(mockStoreConfig.getPartitionGracefulDropDelaySeconds()).thenReturn(1); // 1 second.
    when(mockStoreConfig.getStoreVersionName()).thenReturn(resourceName);
    mockParticipantStateTransitionStats = Mockito.mock(ParticipantStateTransitionStats.class);

    mockAggVersionedIngestionStats = Mockito.mock(AggVersionedIngestionStats.class);
    mockMessage = Mockito.mock(Message.class);
    mockSystemStoreMessage = Mockito.mock(Message.class);
    mockContext = Mockito.mock(NotificationContext.class);

    mockNotifier = getNotifier();
    mockReadOnlyStoreRepository = Mockito.mock(ReadOnlyStoreRepository.class);
    mockStore = Mockito.mock(Store.class);
    mockSystemStore = Mockito.mock(Store.class);
    mockManager = Mockito.mock(SafeHelixManager.class);
    mockHelixManager = Mockito.mock(HelixManager.class);

    when(mockMessage.getResourceName()).thenReturn(resourceName);
    when(mockSystemStoreMessage.getResourceName()).thenReturn(systemStoreResourceName);
    when(mockReadOnlyStoreRepository.getStoreOrThrow(Version.parseStoreFromKafkaTopicName(resourceName)))
        .thenReturn(mockStore);
    when(mockReadOnlyStoreRepository.getStoreOrThrow(Version.parseStoreFromKafkaTopicName(systemStoreResourceName)))
        .thenReturn(mockSystemStore);
    when(mockReadOnlyStoreRepository.getStore(storeName)).thenReturn(mockStore);
    when(mockReadOnlyStoreRepository.getStore(systemStoreName)).thenReturn(mockSystemStore);
    when(mockStore.getBootstrapToOnlineTimeoutInHours()).thenReturn(Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS);
    when(mockSystemStore.getBootstrapToOnlineTimeoutInHours()).thenReturn(Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS);

    when(mockStoreIngestionService.getAggVersionedIngestionStats()).thenReturn(mockAggVersionedIngestionStats);

    when(mockManager.getOriginalManager()).thenReturn(mockHelixManager);
    when(mockManager.getInstanceName()).thenReturn(instanceName);

    mockCustomizedStateProvider = Mockito.mock(CustomizedStateProvider.class);
    mockPushStatusAccessor = Mockito.mock(HelixPartitionStatusAccessor.class);
    mockPushStatusAccessor.setCustomizedStateProvider(mockCustomizedStateProvider);

    testStateModel = getParticipantStateModel();
  }

  protected abstract MODEL_TYPE getParticipantStateModel();

  protected abstract NOTIFIER_TYPE getNotifier() throws InterruptedException;

  @Test
  public void testGetStoreVersionRole() {
    // Case 1: Current version
    when(mockStore.getCurrentVersion()).thenReturn(version);
    String role = testStateModel.getStoreVersionRole();
    assertEquals(role, "CURRENT", "Case 1: Current version");

    // Case 2: Future version
    when(mockStore.getCurrentVersion()).thenReturn(version - 1);
    role = testStateModel.getStoreVersionRole();
    assertEquals(role, "FUTURE", "Case 2: Future version");

    // Case 3: Backup version
    when(mockStore.getCurrentVersion()).thenReturn(version + 1);
    role = testStateModel.getStoreVersionRole();
    assertEquals(role, "BACKUP", "Case 3: Backup version");

    // Case 4: Store not found returns empty
    when(mockReadOnlyStoreRepository.getStore(storeName)).thenReturn(null);
    role = testStateModel.getStoreVersionRole();
    assertEquals(role, "", "Case 4: Store not found returns empty");
  }

  @Test
  public void testGetStoreType() {
    // Case 1: System store
    when(mockStore.isSystemStore()).thenReturn(true);
    VeniceStoreType storeType = testStateModel.getStoreVersionType();
    assertEquals(storeType, VeniceStoreType.SYSTEM, "Case 1: System store");

    // Case 2: Hybrid user store (need new instance to clear cache)
    MODEL_TYPE freshModel = getParticipantStateModel();
    Version mockVersion = Mockito.mock(Version.class);
    when(mockStore.isSystemStore()).thenReturn(false);
    when(mockStore.getVersion(version)).thenReturn(mockVersion);
    when(mockVersion.isHybrid()).thenReturn(true);
    storeType = freshModel.getStoreVersionType();
    assertEquals(storeType, VeniceStoreType.HYBRID, "Case 2: Hybrid store");

    // Case 3: Batch-only user store
    freshModel = getParticipantStateModel();
    when(mockStore.isSystemStore()).thenReturn(false);
    when(mockStore.getVersion(version)).thenReturn(mockVersion);
    when(mockVersion.isHybrid()).thenReturn(false);
    storeType = freshModel.getStoreVersionType();
    assertEquals(storeType, VeniceStoreType.BATCH, "Case 3: Batch store");
  }

  @Test
  public void testGetReplicaTypeDescription() {
    // Case 1: System store future version
    when(mockStore.isSystemStore()).thenReturn(true);
    when(mockStore.getCurrentVersion()).thenReturn(version - 1);
    String description = testStateModel.getReplicaTypeDescription();
    assertEquals(description, "SYSTEM store future version", "Case 1: System store future version");

    // Case 2: Hybrid store current version
    MODEL_TYPE freshModel = getParticipantStateModel();
    Version mockVersion = Mockito.mock(Version.class);
    when(mockStore.isSystemStore()).thenReturn(false);
    when(mockStore.getVersion(version)).thenReturn(mockVersion);
    when(mockVersion.isHybrid()).thenReturn(true);
    when(mockStore.getCurrentVersion()).thenReturn(version);
    description = freshModel.getReplicaTypeDescription();
    assertEquals(description, "HYBRID store current version", "Case 2: Hybrid store current version");

    // Case 3: Batch store backup version
    freshModel = getParticipantStateModel();
    when(mockStore.isSystemStore()).thenReturn(false);
    when(mockStore.getVersion(version)).thenReturn(mockVersion);
    when(mockVersion.isHybrid()).thenReturn(false);
    when(mockStore.getCurrentVersion()).thenReturn(version + 1);
    description = freshModel.getReplicaTypeDescription();
    assertEquals(description, "BATCH store backup version", "Case 3: Batch store backup version");

    // Case 4: Unknown store returns generic description
    freshModel = getParticipantStateModel();
    when(mockReadOnlyStoreRepository.getStore(storeName)).thenReturn(null);
    description = freshModel.getReplicaTypeDescription();
    assertEquals(description, "BATCH store", "Case 4: Unknown store");
  }
}
