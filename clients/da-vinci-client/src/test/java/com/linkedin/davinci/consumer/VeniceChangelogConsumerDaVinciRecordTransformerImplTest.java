package com.linkedin.davinci.consumer;

import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.CONSUMER_METRIC_ENTITIES;
import static com.linkedin.venice.client.store.ClientConfig.DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME;
import static com.linkedin.venice.stats.ClientType.CHANGE_DATA_CAPTURE_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.FAIL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerRecordMetadata;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.davinci.client.SeekableDaVinciClient;
import com.linkedin.davinci.consumer.stats.BasicConsumerStats;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.lazy.Lazy;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.avro.Schema;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceChangelogConsumerDaVinciRecordTransformerImplTest {
  private static final String TEST_STORE_NAME = "test_store";
  private static final String TEST_ZOOKEEPER_ADDRESS = "test_zookeeper";
  public static final String D2_SERVICE_NAME = "ChildController";
  private static final String TEST_BOOTSTRAP_FILE_SYSTEM_PATH = "/export/content/data/change-capture";
  private static final long TEST_DB_SYNC_BYTES_INTERVAL = 1000L;
  private static final int PARTITION_COUNT = 3;
  private static final int POLL_TIMEOUT = 1;
  private static final int CURRENT_STORE_VERSION = 1;
  private static final int FUTURE_STORE_VERSION = 2;
  private static final int MAX_BUFFER_SIZE = 10;
  private static final int value = 2;
  private static final Lazy<Integer> lazyValue = Lazy.of(() -> value);
  private static final DaVinciRecordTransformerRecordMetadata recordMetadata =
      new DaVinciRecordTransformerRecordMetadata(-1, 0, PubSubSymbolicPosition.EARLIEST, -1, null);

  private Schema keySchema;
  private Schema valueSchema;
  private VeniceChangelogConsumerDaVinciRecordTransformerImpl<Integer, Integer> statefulVeniceChangelogConsumer;
  private VeniceChangelogConsumerDaVinciRecordTransformerImpl.DaVinciRecordTransformerChangelogConsumer recordTransformer;
  private VeniceChangelogConsumerDaVinciRecordTransformerImpl.DaVinciRecordTransformerChangelogConsumer futureRecordTransformer;
  private ChangelogClientConfig changelogClientConfig;
  private DaVinciRecordTransformerConfig mockDaVinciRecordTransformerConfig;
  private SeekableDaVinciClient mockDaVinciClient;
  private CompletableFuture<Void> daVinciClientSubscribeFuture;
  private List<Lazy<Integer>> keys;
  private BasicConsumerStats changeCaptureStats;
  private Set<Integer> partitionSet;
  private VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory;

  @BeforeMethod
  public void setUp() throws NoSuchFieldException, IllegalAccessException {
    SchemaReader mockSchemaReader = mock(SchemaReader.class);
    keySchema = Schema.create(Schema.Type.INT);
    doReturn(keySchema).when(mockSchemaReader).getKeySchema();
    valueSchema = Schema.create(Schema.Type.INT);
    doReturn(valueSchema).when(mockSchemaReader).getValueSchema(1);

    D2ControllerClient mockD2ControllerClient = mock(D2ControllerClient.class);

    changelogClientConfig = new ChangelogClientConfig<>().setD2ControllerClient(mockD2ControllerClient)
        .setSchemaReader(mockSchemaReader)
        .setStoreName(TEST_STORE_NAME)
        .setBootstrapFileSystemPath(TEST_BOOTSTRAP_FILE_SYSTEM_PATH)
        .setControllerD2ServiceName(D2_SERVICE_NAME)
        .setD2ServiceName(DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .setConsumerProperties(new Properties())
        .setLocalD2ZkHosts(TEST_ZOOKEEPER_ADDRESS)
        .setDatabaseSyncBytesInterval(TEST_DB_SYNC_BYTES_INTERVAL)
        .setD2Client(mock(D2Client.class))
        .setShouldCompactMessages(true);
    assertEquals(changelogClientConfig.getMaxBufferSize(), 1000, "Default max buffer size should be 1000");
    changelogClientConfig.setMaxBufferSize(MAX_BUFFER_SIZE);
    changelogClientConfig.getInnerClientConfig()
        .setMetricsRepository(getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true));

    veniceChangelogConsumerClientFactory = spy(new VeniceChangelogConsumerClientFactory(changelogClientConfig, null));
    statefulVeniceChangelogConsumer = spy(
        new VeniceChangelogConsumerDaVinciRecordTransformerImpl<>(
            changelogClientConfig,
            veniceChangelogConsumerClientFactory));
    assertFalse(
        statefulVeniceChangelogConsumer.getRecordTransformerConfig().isRecordTransformationEnabled(),
        "Record transformation should be disabled.");

    mockDaVinciRecordTransformerConfig = mock(DaVinciRecordTransformerConfig.class);
    recordTransformer = statefulVeniceChangelogConsumer.new DaVinciRecordTransformerChangelogConsumer(TEST_STORE_NAME,
        CURRENT_STORE_VERSION, keySchema, valueSchema, valueSchema, mockDaVinciRecordTransformerConfig);
    futureRecordTransformer = statefulVeniceChangelogConsumer.new DaVinciRecordTransformerChangelogConsumer(
        TEST_STORE_NAME, FUTURE_STORE_VERSION, keySchema, valueSchema, valueSchema, mockDaVinciRecordTransformerConfig);

    // Replace daVinciClient with a mock
    mockDaVinciClient = mock(SeekableDaVinciClient.class);
    daVinciClientSubscribeFuture = new CompletableFuture<>();
    when(mockDaVinciClient.getPartitionCount()).thenReturn(PARTITION_COUNT);
    when(mockDaVinciClient.subscribe(any())).thenReturn(daVinciClientSubscribeFuture);

    AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
      try {
        Field daVinciClientField =
            VeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("daVinciClient");
        daVinciClientField.setAccessible(true);
        daVinciClientField.set(statefulVeniceChangelogConsumer, mockDaVinciClient);

        Field changeCaptureStatsField =
            VeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("changeCaptureStats");
        changeCaptureStatsField.setAccessible(true);
        changeCaptureStats = spy((BasicConsumerStats) changeCaptureStatsField.get(statefulVeniceChangelogConsumer));
        changeCaptureStatsField.set(statefulVeniceChangelogConsumer, changeCaptureStats);

      } catch (NoSuchFieldException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      return null;
    });

    keys = new ArrayList<>();
    partitionSet = new HashSet<>();
    for (int i = 0; i < PARTITION_COUNT; i++) {
      int tempI = i;
      keys.add(Lazy.of(() -> tempI));
      partitionSet.add(i);
    }
  }

  @Test
  public void testStartAllPartitions() throws ExecutionException, InterruptedException {
    // Completable future should finish even when no records are consumed after timeout
    statefulVeniceChangelogConsumer.setStartTimeout(1);
    statefulVeniceChangelogConsumer.start().get();

    assertTrue(statefulVeniceChangelogConsumer.isStarted(), "isStarted should be true");

    verify(mockDaVinciClient).start();
    assertEquals(statefulVeniceChangelogConsumer.getSubscribedPartitions(), partitionSet);
  }

  @Test
  public void testStartWithEmptyPartitions() {
    statefulVeniceChangelogConsumer.start(Collections.emptySet());
    assertTrue(statefulVeniceChangelogConsumer.isStarted(), "isStarted should be true");

    verify(mockDaVinciClient).start();
    assertEquals(statefulVeniceChangelogConsumer.getSubscribedPartitions(), partitionSet);
  }

  @Test
  public void testStartSpecificPartitions() {
    Set<Integer> partitionSet = Collections.singleton(1);
    statefulVeniceChangelogConsumer.start(partitionSet);
    assertTrue(statefulVeniceChangelogConsumer.isStarted(), "isStarted should be true");

    verify(mockDaVinciClient).start();
    assertEquals(statefulVeniceChangelogConsumer.getSubscribedPartitions(), partitionSet);
  }

  @Test
  public void testStartMultipleTimes() {
    Set<Integer> partitionSet = Collections.singleton(1);
    statefulVeniceChangelogConsumer.start();
    onStartVersionIngestionHelper(true, true);
    assertEquals(statefulVeniceChangelogConsumer.getLastHeartbeatPerPartition().size(), PARTITION_COUNT);

    assertThrows(VeniceClientException.class, () -> statefulVeniceChangelogConsumer.start());
    assertThrows(VeniceException.class, () -> statefulVeniceChangelogConsumer.start(partitionSet));

    statefulVeniceChangelogConsumer.unsubscribeAll();
    verify(statefulVeniceChangelogConsumer).clearPartitionState(eq(Collections.emptySet()));
    assertTrue(statefulVeniceChangelogConsumer.getLastHeartbeatPerPartition().isEmpty());

    statefulVeniceChangelogConsumer.start();
    onStartVersionIngestionHelper(true, true);
    statefulVeniceChangelogConsumer.unsubscribe(partitionSet);
    verify(statefulVeniceChangelogConsumer).clearPartitionState(partitionSet);
    assertFalse(statefulVeniceChangelogConsumer.getLastHeartbeatPerPartition().isEmpty());
    assertEquals(statefulVeniceChangelogConsumer.getLastHeartbeatPerPartition().size(), PARTITION_COUNT - 1);

    statefulVeniceChangelogConsumer.start(partitionSet);
  }

  @Test
  public void testStop() throws Exception {
    statefulVeniceChangelogConsumer.start();
    assertTrue(statefulVeniceChangelogConsumer.isStarted(), "isStarted should be true");

    statefulVeniceChangelogConsumer.stop();
    assertFalse(statefulVeniceChangelogConsumer.isStarted(), "isStarted should be false");

    verify(mockDaVinciClient).close();
    verify(statefulVeniceChangelogConsumer).clearPartitionState(eq(Collections.emptySet()));
    verify(veniceChangelogConsumerClientFactory).deregisterClient(changelogClientConfig.getConsumerName());
  }

  @Test
  public void testPutAndDelete() {
    statefulVeniceChangelogConsumer.start();
    onStartVersionIngestionHelper(true, true);

    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      recordTransformer.processPut(keys.get(partitionId), lazyValue, partitionId, recordMetadata);
    }
    verifyPuts(value, false);

    // Verify deletes
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      recordTransformer.processDelete(keys.get(partitionId), partitionId, recordMetadata);
    }
    verifyDeletes();
  }

  @Test
  public void testVersionSwap() {
    statefulVeniceChangelogConsumer.start();
    onStartVersionIngestionHelper(true, true);
    // Setting isCurrentVersion to true to verify that the next current version doesn't start serving immediately.
    // It should only serve when it processes the VSM.
    onStartVersionIngestionHelper(false, true);

    List<Lazy<Integer>> keys = new ArrayList<>();
    for (int i = 0; i < PARTITION_COUNT; i++) {
      int tempI = i;
      keys.add(Lazy.of(() -> tempI));
    }

    int currentVersionValue = 2;
    int futureVersionValue = 3;
    Lazy<Integer> lazyCurrentVersionValueValue = Lazy.of(() -> currentVersionValue);
    Lazy<Integer> lazyFutureVersionValueValue = Lazy.of(() -> futureVersionValue);

    // Verify it only contains current version values
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      recordTransformer.processPut(keys.get(partitionId), lazyCurrentVersionValueValue, partitionId, recordMetadata);
      futureRecordTransformer
          .processPut(keys.get(partitionId), lazyFutureVersionValueValue, partitionId, recordMetadata);
    }
    verifyPuts(currentVersionValue, false);

    // Verify compaction
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      recordTransformer.processPut(keys.get(partitionId), lazyCurrentVersionValueValue, partitionId, recordMetadata);
      recordTransformer.processPut(keys.get(partitionId), lazyCurrentVersionValueValue, partitionId, recordMetadata);
    }
    verifyPuts(currentVersionValue, true);

    // Verify only the future version is allowed to perform the version swap
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      recordTransformer.onVersionSwap(CURRENT_STORE_VERSION, FUTURE_STORE_VERSION, partitionId);
      recordTransformer.processPut(keys.get(partitionId), lazyCurrentVersionValueValue, partitionId, recordMetadata);
      futureRecordTransformer
          .processPut(keys.get(partitionId), lazyFutureVersionValueValue, partitionId, recordMetadata);
    }
    verifyPuts(currentVersionValue, false);

    // Perform a version swap from the future version and verify the buffer only contains FUTURE_STORE_VERSION's values
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      futureRecordTransformer.onVersionSwap(CURRENT_STORE_VERSION, FUTURE_STORE_VERSION, partitionId);
      recordTransformer.processPut(keys.get(partitionId), lazyCurrentVersionValueValue, partitionId, recordMetadata);
      futureRecordTransformer
          .processPut(keys.get(partitionId), lazyFutureVersionValueValue, partitionId, recordMetadata);
    }

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      verify(changeCaptureStats, times(PARTITION_COUNT))
          .emitVersionSwapCountMetrics(VeniceResponseStatusCategory.SUCCESS);
    });

    verifyPuts(futureVersionValue, false);
  }

  @Test
  public void testVersionSwapFailure() throws IllegalAccessException, NoSuchFieldException {
    Field partitionToVersionToServeField =
        VeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("partitionToVersionToServe");
    partitionToVersionToServeField.setAccessible(true);
    partitionToVersionToServeField.set(statefulVeniceChangelogConsumer, null);

    assertThrows(
        Exception.class,
        () -> futureRecordTransformer.onVersionSwap(CURRENT_STORE_VERSION, FUTURE_STORE_VERSION, 0));
    verify(changeCaptureStats).emitVersionSwapCountMetrics(FAIL);
  }

  @Test
  public void testCompletableFutureFromStart() {
    CompletableFuture startCompletableFuture = statefulVeniceChangelogConsumer.start();
    onStartVersionIngestionHelper(true, true);
    onStartVersionIngestionHelper(false, false);

    // CompletableFuture should not be finished until a record has been pushed to the buffer by the current version
    assertFalse(startCompletableFuture.isDone());

    // Future version should not cause the CompletableFuture to complete
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      futureRecordTransformer.processPut(keys.get(partitionId), lazyValue, partitionId, recordMetadata);
    }
    assertFalse(startCompletableFuture.isDone());

    // CompletableFuture should be finished when the current version produces to the buffer
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      recordTransformer.processPut(keys.get(partitionId), lazyValue, partitionId, recordMetadata);
    }
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      assertTrue(startCompletableFuture.isDone());
    });
  }

  @Test
  public void testCompletableFutureFromStartException() {
    assertFalse(statefulVeniceChangelogConsumer.isCaughtUp());

    CompletableFuture startCompletableFuture = statefulVeniceChangelogConsumer.start();
    onStartVersionIngestionHelper(true, true);

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      verify(mockDaVinciClient).subscribe(partitionSet);
      assertFalse(startCompletableFuture.isDone());
    });
    assertFalse(statefulVeniceChangelogConsumer.isCaughtUp());

    daVinciClientSubscribeFuture.completeExceptionally(new VeniceException("Test exception"));
    assertFalse(statefulVeniceChangelogConsumer.isCaughtUp());

    assertThrows(ExecutionException.class, startCompletableFuture::get);
  }

  @Test
  public void testTransformResult() {
    int value = 2;
    int partitionId = 0;
    Lazy<Integer> lazyValue = Lazy.of(() -> value);
    DaVinciRecordTransformerResult.Result result =
        recordTransformer.transform(keys.get(partitionId), lazyValue, partitionId, recordMetadata).getResult();
    assertSame(result, DaVinciRecordTransformerResult.Result.UNCHANGED);
  }

  @Test
  public void testMaxBufferSize() throws NoSuchFieldException, IllegalAccessException, InterruptedException {
    ReentrantLock bufferLock = spy(new ReentrantLock());
    Condition bufferIsFullCondition = spy(bufferLock.newCondition());

    Field bufferLockField = VeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("bufferLock");
    bufferLockField.setAccessible(true);
    bufferLockField.set(statefulVeniceChangelogConsumer, bufferLock);

    Field bufferIsFullConditionField =
        VeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("bufferIsFullCondition");
    bufferIsFullConditionField.setAccessible(true);
    bufferIsFullConditionField.set(statefulVeniceChangelogConsumer, bufferIsFullCondition);

    assertEquals(changelogClientConfig.getMaxBufferSize(), MAX_BUFFER_SIZE);

    statefulVeniceChangelogConsumer.start();
    onStartVersionIngestionHelper(true, true);

    int partitionId = 1;

    List<CompletableFuture> completableFutureList = new ArrayList<>();
    for (int i = 0; i <= MAX_BUFFER_SIZE; i++) {
      completableFutureList.add(CompletableFuture.supplyAsync(() -> {
        recordTransformer.processPut(keys.get(partitionId), lazyValue, partitionId, recordMetadata);
        return null;
      }));
    }

    // Buffer is full signal should be hit
    verify(bufferLock, timeout(1000).atLeastOnce()).lock();
    verify(bufferLock, timeout(1000L).atLeastOnce()).unlock();
    verify(bufferIsFullCondition, atLeastOnce()).signal();

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      // Verify every CompletableFuture in completableFutureList is completed besides one
      int completedFutures = 0;
      int uncompletedFutures = 0;
      for (int i = 0; i <= MAX_BUFFER_SIZE; i++) {
        if (completableFutureList.get(i).isDone()) {
          completedFutures += 1;
        } else {
          uncompletedFutures += 1;
        }
      }
      assertEquals(completedFutures, MAX_BUFFER_SIZE);
      assertEquals(uncompletedFutures, 1);
    });

    reset(bufferLock);
    reset(bufferIsFullCondition);

    // Buffer is full, so poll shouldn't await on the buffer full condition
    int timeoutInMs = 100;
    statefulVeniceChangelogConsumer.poll(timeoutInMs);
    verify(bufferIsFullCondition, never()).await(timeoutInMs, TimeUnit.MILLISECONDS);

    reset(bufferLock);
    reset(bufferIsFullCondition);

    // Empty the buffer and verify that all CompletableFutures are done
    statefulVeniceChangelogConsumer.poll(timeoutInMs);
    verify(bufferLock).lock();
    verify(bufferLock).unlock();
    // Buffer isn't full, so poll should await on the buffer is full condition and timeout
    verify(bufferIsFullCondition).await(timeoutInMs, TimeUnit.MILLISECONDS);

    for (int i = 0; i <= MAX_BUFFER_SIZE; i++) {
      assertTrue(completableFutureList.get(i).isDone());
    }

    reset(bufferLock);
    reset(bufferIsFullCondition);

    /*
     * Test the case where the buffer isn't full initially, so poll awaits on the condition and doesn't hit the timeout
     * due to the condition being signaled after the processPut calls fill up the buffer.
     */
    CompletableFuture.supplyAsync(() -> {
      statefulVeniceChangelogConsumer.poll(timeoutInMs);
      return null;
    });

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      verify(bufferIsFullCondition).await(timeoutInMs, TimeUnit.MILLISECONDS);
    });

    for (int i = 0; i < MAX_BUFFER_SIZE; i++) {
      CompletableFuture.supplyAsync(() -> {
        recordTransformer.processPut(keys.get(partitionId), lazyValue, partitionId, recordMetadata);
        return null;
      });
    }

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      verify(bufferIsFullCondition).signal();
    });
  }

  @Test
  public void testPollFailure() throws NoSuchFieldException, IllegalAccessException {
    VeniceChangelogConsumerDaVinciRecordTransformerImpl statefulVeniceChangelogConsumer =
        mock(VeniceChangelogConsumerDaVinciRecordTransformerImpl.class);

    Field changeCaptureStatsField =
        VeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("changeCaptureStats");
    changeCaptureStatsField.setAccessible(true);
    BasicConsumerStats consumerStats = mock(BasicConsumerStats.class);
    changeCaptureStatsField.set(statefulVeniceChangelogConsumer, consumerStats);

    doCallRealMethod().when(statefulVeniceChangelogConsumer).poll(POLL_TIMEOUT);
    assertThrows(Exception.class, () -> statefulVeniceChangelogConsumer.poll(POLL_TIMEOUT));

    verify(consumerStats).emitPollCountMetrics(FAIL);
    verify(consumerStats, times(0)).emitRecordsConsumedCountMetrics(anyInt());
  }

  @Test
  public void testMetricReportingThread() {
    statefulVeniceChangelogConsumer.setBackgroundReporterThreadSleepIntervalSeconds(1L);

    verify(changeCaptureStats, times(0)).emitCurrentConsumingVersionMetrics(anyInt(), anyInt());
    verify(changeCaptureStats, times(0)).emitHeartBeatDelayMetrics(anyLong());
    assertEquals(statefulVeniceChangelogConsumer.getLastHeartbeatPerPartition().size(), 0);
    statefulVeniceChangelogConsumer.start();

    onStartVersionIngestionHelper(true, true);
    onStartVersionIngestionHelper(true, false);

    int partitionId = 0;
    recordTransformer.processPut(keys.get(partitionId), lazyValue, partitionId, recordMetadata);
    recordTransformer.onHeartbeat(partitionId, 1L);

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      verify(changeCaptureStats, atLeastOnce())
          .emitCurrentConsumingVersionMetrics(CURRENT_STORE_VERSION, CURRENT_STORE_VERSION);
      verify(changeCaptureStats, atLeastOnce()).emitHeartBeatDelayMetrics(anyLong());
    });
    assertEquals(statefulVeniceChangelogConsumer.getLastHeartbeatPerPartition().size(), partitionSet.size());
    assertTrue(
        statefulVeniceChangelogConsumer.getLastHeartbeatPerPartition().get(partitionId) < System.currentTimeMillis());

    // Perform version swap on one partition
    futureRecordTransformer.onVersionSwap(CURRENT_STORE_VERSION, FUTURE_STORE_VERSION, 0);
    clearInvocations(changeCaptureStats);
    futureRecordTransformer.onHeartbeat(partitionId, 2L);

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      verify(changeCaptureStats, atLeastOnce())
          .emitCurrentConsumingVersionMetrics(CURRENT_STORE_VERSION, FUTURE_STORE_VERSION);
      verify(changeCaptureStats, atLeastOnce()).emitHeartBeatDelayMetrics(anyLong());
    });
  }

  @Test
  public void testIsCaughtUp() {
    assertFalse(statefulVeniceChangelogConsumer.isCaughtUp());

    CompletableFuture startCompletableFuture = statefulVeniceChangelogConsumer.start(partitionSet);
    onStartVersionIngestionHelper(true, true);

    // Add records for all but 1 partition to complete the start future, but to not complete the subscribe future.
    for (int partitionId = 0; partitionId < PARTITION_COUNT - 1; partitionId++) {
      recordTransformer.processPut(keys.get(partitionId), lazyValue, partitionId, recordMetadata);
    }

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      verify(mockDaVinciClient).subscribe(partitionSet);
      assertTrue(startCompletableFuture.isDone());
    });
    assertFalse(statefulVeniceChangelogConsumer.isCaughtUp());

    // Add record for last partition
    recordTransformer.processPut(keys.get(PARTITION_COUNT - 1), lazyValue, PARTITION_COUNT - 1, recordMetadata);
    daVinciClientSubscribeFuture.complete(null);

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      assertTrue(statefulVeniceChangelogConsumer.isCaughtUp());
    });
  }

  private void verifyPuts(int value, boolean compactionEvent) {
    clearInvocations(changeCaptureStats);
    Collection<PubSubMessage<Integer, ChangeEvent<Integer>, VeniceChangeCoordinate>> pubSubMessages =
        statefulVeniceChangelogConsumer.poll(POLL_TIMEOUT);

    if (compactionEvent) {
      verify(changeCaptureStats).emitRecordsConsumedCountMetrics(PARTITION_COUNT * 2);
    } else {
      verify(changeCaptureStats).emitRecordsConsumedCountMetrics(PARTITION_COUNT);
    }
    verify(changeCaptureStats).emitPollCountMetrics(VeniceResponseStatusCategory.SUCCESS);
    assertEquals(pubSubMessages.size(), PARTITION_COUNT);

    int i = 0;
    for (PubSubMessage<Integer, ChangeEvent<Integer>, VeniceChangeCoordinate> message: pubSubMessages) {
      assertEquals((int) message.getKey(), i);
      ChangeEvent<Integer> changeEvent = message.getValue();
      assertNull(changeEvent.getPreviousValue());
      assertEquals((int) changeEvent.getCurrentValue(), value);
      assertEquals(message.getPayloadSize(), -1);
      assertNotNull(message.getPosition());
      assertEquals(message.getPubSubMessageTime(), 0);
      i++;
    }

    clearInvocations(changeCaptureStats);
    assertEquals(statefulVeniceChangelogConsumer.poll(POLL_TIMEOUT).size(), 0, "Buffer should be empty");
    verify(changeCaptureStats).emitRecordsConsumedCountMetrics(0);
    verify(changeCaptureStats).emitPollCountMetrics(VeniceResponseStatusCategory.SUCCESS);
  }

  private void verifyDeletes() {
    clearInvocations(changeCaptureStats);
    Collection<PubSubMessage<Integer, ChangeEvent<Integer>, VeniceChangeCoordinate>> pubSubMessages =
        statefulVeniceChangelogConsumer.poll(POLL_TIMEOUT);
    verify(changeCaptureStats).emitRecordsConsumedCountMetrics(PARTITION_COUNT);
    verify(changeCaptureStats).emitPollCountMetrics(VeniceResponseStatusCategory.SUCCESS);
    assertEquals(pubSubMessages.size(), PARTITION_COUNT);

    int i = 0;
    for (PubSubMessage<Integer, ChangeEvent<Integer>, VeniceChangeCoordinate> message: pubSubMessages) {
      assertEquals((int) message.getKey(), i);
      ChangeEvent<Integer> changeEvent = message.getValue();
      assertNull(changeEvent.getPreviousValue());
      assertNull(changeEvent.getCurrentValue());
      i++;
    }

    clearInvocations(changeCaptureStats);
    assertEquals(statefulVeniceChangelogConsumer.poll(POLL_TIMEOUT).size(), 0, "Buffer should be empty");
    verify(changeCaptureStats).emitRecordsConsumedCountMetrics(0);
    verify(changeCaptureStats).emitPollCountMetrics(VeniceResponseStatusCategory.SUCCESS);
  }

  private void onStartVersionIngestionHelper(boolean currentRecordTransformer, boolean isCurrentVersion) {
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      if (currentRecordTransformer) {
        recordTransformer.onStartVersionIngestion(partitionId, isCurrentVersion);
      } else {
        futureRecordTransformer.onStartVersionIngestion(partitionId, isCurrentVersion);
      }
    }
  }
}
