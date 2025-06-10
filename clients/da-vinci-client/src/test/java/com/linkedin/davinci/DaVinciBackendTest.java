package com.linkedin.davinci;

import static com.linkedin.venice.ConfigKeys.DA_VINCI_SUBSCRIBE_ON_DISK_PARTITIONS_AUTOMATICALLY;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_OTHER;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.ERROR;
import static com.linkedin.venice.utils.DataProviderUtils.BOOLEAN;
import static com.linkedin.venice.utils.DataProviderUtils.allPermutationGenerator;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.stats.AggVersionedStorageEngineStats;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.exceptions.DiskLimitExhaustedException;
import com.linkedin.venice.exceptions.MemoryLimitExhaustedException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.SubscriptionBasedReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.VeniceProperties;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class DaVinciBackendTest {
  @DataProvider(name = "DvcErrorExecutionStatusAndBoolean")
  public static Object[][] dvcErrorExecutionStatusAndBoolean() {
    return allPermutationGenerator((permutation) -> {
      ExecutionStatus status = (ExecutionStatus) permutation[0];
      return status.isDVCIngestionError();
    }, ExecutionStatus.values(), BOOLEAN);
  }

  @Test(dataProvider = "DvcErrorExecutionStatusAndBoolean")
  public void testGetDaVinciErrorStatus(
      ExecutionStatus executionStatus,
      boolean useDaVinciSpecificExecutionStatusForError) {
    VeniceException veniceException;
    switch (executionStatus) {
      case DVC_INGESTION_ERROR_DISK_FULL:
        veniceException = new DiskLimitExhaustedException("test");
        break;
      case DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED:
        veniceException = new MemoryLimitExhaustedException("test");
        break;
      case DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES:
      case DVC_INGESTION_ERROR_OTHER:
        veniceException = new VeniceException("test");
        break;
      default:
        fail("Unexpected execution status: " + executionStatus);
        return;
    }
    if (useDaVinciSpecificExecutionStatusForError) {
      assertEquals(
          DaVinciBackend.getDaVinciErrorStatus(veniceException, useDaVinciSpecificExecutionStatusForError),
          executionStatus.equals(ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES)
              ? DVC_INGESTION_ERROR_OTHER
              : executionStatus);
    } else {
      assertEquals(
          DaVinciBackend.getDaVinciErrorStatus(veniceException, useDaVinciSpecificExecutionStatusForError),
          ERROR);
    }
  }

  @Test(dataProvider = "DvcErrorExecutionStatusAndBoolean")
  public void testGetDaVinciErrorStatusNested(
      ExecutionStatus executionStatus,
      boolean useDaVinciSpecificExecutionStatusForError) {
    VeniceException veniceException;
    switch (executionStatus) {
      case DVC_INGESTION_ERROR_DISK_FULL:
        veniceException = new VeniceException(new DiskLimitExhaustedException("test"));
        break;
      case DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED:
        veniceException = new VeniceException(new MemoryLimitExhaustedException("test"));
        break;
      case DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES:
      case DVC_INGESTION_ERROR_OTHER:
        veniceException = new VeniceException("test");
        break;
      default:
        fail("Unexpected execution status: " + executionStatus);
        return;
    }
    if (useDaVinciSpecificExecutionStatusForError) {
      assertEquals(
          DaVinciBackend.getDaVinciErrorStatus(veniceException, useDaVinciSpecificExecutionStatusForError),
          executionStatus.equals(ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES)
              ? DVC_INGESTION_ERROR_OTHER
              : executionStatus);
    } else {
      assertEquals(
          DaVinciBackend.getDaVinciErrorStatus(veniceException, useDaVinciSpecificExecutionStatusForError),
          ERROR);
    }
  }

  @Test(dataProvider = "DvcErrorExecutionStatusAndBoolean")
  public void testGetDaVinciErrorStatusWithInvalidCases(
      ExecutionStatus executionStatus,
      boolean useDaVinciSpecificExecutionStatusForError) {
    VeniceException veniceException;
    switch (executionStatus) {
      case DVC_INGESTION_ERROR_DISK_FULL:
      case DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED:
      case DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES:
      case DVC_INGESTION_ERROR_OTHER:
        veniceException = new VeniceException("test");
        break;
      default:
        fail("Unexpected execution status: " + executionStatus);
        return;
    }

    if (useDaVinciSpecificExecutionStatusForError) {
      assertEquals(
          DaVinciBackend.getDaVinciErrorStatus(veniceException, useDaVinciSpecificExecutionStatusForError),
          DVC_INGESTION_ERROR_OTHER);

    } else {
      assertEquals(
          DaVinciBackend.getDaVinciErrorStatus(veniceException, useDaVinciSpecificExecutionStatusForError),
          ERROR);
    }
  }

  @Test
  public void testBootstrappingAwareCompletableFuture()
      throws ExecutionException, InterruptedException, TimeoutException {
    DaVinciBackend backend = mock(DaVinciBackend.class);

    when(backend.hasCurrentVersionBootstrapping()).thenReturn(true).thenReturn(false);

    DaVinciBackend.BootstrappingAwareCompletableFuture future =
        new DaVinciBackend.BootstrappingAwareCompletableFuture(backend);
    future.getBootstrappingFuture().get(10, TimeUnit.SECONDS);
    verify(backend, times(2)).hasCurrentVersionBootstrapping();
  }

  @Test
  public void testBootstrappingSubscription()
      throws IllegalAccessException, NoSuchFieldException, NoSuchMethodException, InvocationTargetException {
    // TODO: Make this into a real backend, rather than a mock, and tear down all the reflection stuff below...
    DaVinciBackend backend = mock(DaVinciBackend.class);
    StorageService mockStorageService = mock(StorageService.class);

    StorageEngineRepository mockStorageEngineRepository = mock(StorageEngineRepository.class);
    StorageEngine storageEngine = mock(StorageEngine.class);
    String resourceName = "test_store_v1";
    when(storageEngine.getStoreVersionName()).thenReturn(resourceName);

    List<StorageEngine> localStorageEngines = new ArrayList<>();
    localStorageEngines.add(storageEngine);

    when(backend.getStorageService()).thenReturn(mockStorageService);
    when(mockStorageService.getStorageEngineRepository()).thenReturn(mockStorageEngineRepository);
    when(mockStorageService.getStorageEngine(resourceName)).thenReturn(storageEngine);
    when(mockStorageEngineRepository.getAllLocalStorageEngines()).thenReturn(localStorageEngines);
    when(backend.isIsolatedIngestion()).thenReturn(false);

    List<Integer> userPartitionList = new ArrayList<>();
    userPartitionList.add(0);
    userPartitionList.add(1);
    userPartitionList.add(2);
    when(mockStorageService.getUserPartitions(resourceName)).thenReturn(userPartitionList);

    HashSet<Integer> backendSubscription = new HashSet<>();
    backendSubscription.add(0);
    backendSubscription.add(1);

    StoreBackend mockStoreBackend = mock(StoreBackend.class);
    when(backend.getStoreOrThrow(Version.parseStoreFromKafkaTopicName(resourceName))).thenReturn(mockStoreBackend);
    ComplementSet<Integer> backendSubscriptionSet = ComplementSet.wrap(backendSubscription);
    when(mockStoreBackend.getSubscription()).thenReturn(backendSubscriptionSet);

    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        ComplementSet<Integer> partitions = invocation.getArgument(0);
        mockStoreBackend.getSubscription().addAll(partitions);
        return null;
      }
    }).when(mockStoreBackend).subscribe(any(), any());

    Version mockVersion = mock(Version.class);
    Store mockStore = mock(Store.class);
    SubscriptionBasedReadOnlyStoreRepository mockStoreRepository = mock(SubscriptionBasedReadOnlyStoreRepository.class);
    Field storeRepositoryField = DaVinciBackend.class.getDeclaredField("storeRepository");
    storeRepositoryField.setAccessible(true);
    storeRepositoryField.set(backend, mockStoreRepository);
    when(mockStoreRepository.getStoreOrThrow(Version.parseStoreFromKafkaTopicName(resourceName))).thenReturn(mockStore);
    when(mockStore.getVersion(Version.parseVersionFromKafkaTopicName(resourceName))).thenReturn(mockVersion);

    VeniceConfigLoader mockConfigLoader = mock(VeniceConfigLoader.class);
    Field configLoaderField = DaVinciBackend.class.getDeclaredField("configLoader");
    configLoaderField.setAccessible(true);
    configLoaderField.set(backend, mockConfigLoader);
    VeniceProperties mockCombinedProperties = mock(VeniceProperties.class);
    when(mockConfigLoader.getCombinedProperties()).thenReturn(mockCombinedProperties);

    AggVersionedStorageEngineStats mockAggVersionedStorageEngineStats = mock(AggVersionedStorageEngineStats.class);
    Field aggVersionedStorageEngineStatsField = DaVinciBackend.class.getDeclaredField("aggVersionedStorageEngineStats");
    aggVersionedStorageEngineStatsField.setAccessible(true);
    aggVersionedStorageEngineStatsField.set(backend, mockAggVersionedStorageEngineStats);

    KafkaStoreIngestionService storeIngestionService = mock(KafkaStoreIngestionService.class);
    Field ingestionServiceField = DaVinciBackend.class.getDeclaredField("ingestionService");
    ingestionServiceField.setAccessible(true);
    ingestionServiceField.set(backend, storeIngestionService);

    Method bootstrapMethod = DaVinciBackend.class.getDeclaredMethod("bootstrap");
    bootstrapMethod.setAccessible(true);

    // DA_VINCI_SUBSCRIBE_ON_DISK_PARTITIONS_AUTOMATICALLY == false
    when(mockCombinedProperties.getBoolean(DA_VINCI_SUBSCRIBE_ON_DISK_PARTITIONS_AUTOMATICALLY, true))
        .thenReturn(false);
    bootstrapMethod.invoke(backend);

    ComplementSet<Integer> subscription = mockStoreBackend.getSubscription();
    assertTrue(subscription.contains(0));
    assertTrue(subscription.contains(1));
    assertFalse(subscription.contains(2));

    // DA_VINCI_SUBSCRIBE_ON_DISK_PARTITIONS_AUTOMATICALLY == true
    when(mockCombinedProperties.getBoolean(DA_VINCI_SUBSCRIBE_ON_DISK_PARTITIONS_AUTOMATICALLY, true)).thenReturn(true);
    bootstrapMethod.invoke(backend);

    subscription = mockStoreBackend.getSubscription();
    assertTrue(subscription.contains(0));
    assertTrue(subscription.contains(1));
    assertTrue(subscription.contains(2));
  }
}
