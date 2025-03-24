package com.linkedin.davinci.consumer;

import static com.linkedin.venice.client.store.ClientConfig.DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.lazy.Lazy;
import io.tehuti.metrics.MetricsRepository;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.avro.Schema;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImplTest {
  private static final String TEST_STORE_NAME = "test_store";
  private static final String TEST_ZOOKEEPER_ADDRESS = "test_zookeeper";
  public static final String D2_SERVICE_NAME = "ChildController";
  private static final String TEST_BOOTSTRAP_FILE_SYSTEM_PATH = "/export/content/data/change-capture";
  private static final long TEST_ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES = 1024L;
  private static final long TEST_DB_SYNC_BYTES_INTERVAL = 1000L;
  private static final int PARTITION_COUNT = 3;
  private static final int POLL_TIMEOUT = 1;
  private static final int CURRENT_STORE_VERSION = 1;
  private static final int FUTURE_STORE_VERSION = 2;
  private static final int MAX_BUFFER_SIZE = 10;

  private Schema keySchema;
  private Schema valueSchema;
  private BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl<Integer, Integer> bootstrappingVeniceChangelogConsumer;
  private BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl.DaVinciRecordTransformerBootstrappingChangelogConsumer recordTransformer;
  private ChangelogClientConfig changelogClientConfig;
  private DaVinciRecordTransformerConfig mockDaVinciRecordTransformerConfig;
  private DaVinciClient mockDaVinciClient;
  private List<Lazy<Integer>> keys;

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
        .setRocksDBBlockCacheSizeInBytes(TEST_ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES)
        .setDatabaseSyncBytesInterval(TEST_DB_SYNC_BYTES_INTERVAL)
        .setD2Client(mock(D2Client.class))
        .setShouldCompactMessages(true)
        .setIsExperimentalClientEnabled(true);
    assertEquals(changelogClientConfig.getMaxBufferSize(), 1000, "Default max buffer size should be 1000");
    changelogClientConfig.setMaxBufferSize(MAX_BUFFER_SIZE);
    changelogClientConfig.getInnerClientConfig().setMetricsRepository(new MetricsRepository());

    bootstrappingVeniceChangelogConsumer =
        new BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl<>(changelogClientConfig);
    assertTrue(
        bootstrappingVeniceChangelogConsumer.getRecordTransformerConfig().shouldSkipCompatibilityChecks(),
        "Skip compatability checks for DVRT should be enabled.");

    mockDaVinciRecordTransformerConfig = mock(DaVinciRecordTransformerConfig.class);
    recordTransformer = bootstrappingVeniceChangelogConsumer.new DaVinciRecordTransformerBootstrappingChangelogConsumer(
        CURRENT_STORE_VERSION, keySchema, valueSchema, valueSchema, mockDaVinciRecordTransformerConfig);

    // Replace daVinciClient with a mock
    mockDaVinciClient = mock(DaVinciClient.class);
    when(mockDaVinciClient.getPartitionCount()).thenReturn(PARTITION_COUNT);
    when(mockDaVinciClient.subscribe(any())).thenReturn(mock(CompletableFuture.class));

    AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
      try {
        Field daVinciClientField =
            BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("daVinciClient");
        daVinciClientField.setAccessible(true);
        daVinciClientField.set(bootstrappingVeniceChangelogConsumer, mockDaVinciClient);
      } catch (NoSuchFieldException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      return null;
    });

    keys = new ArrayList<>();
    for (int i = 0; i < PARTITION_COUNT; i++) {
      int tempI = i;
      keys.add(Lazy.of(() -> tempI));
    }
  }

  @Test
  public void testStartAllPartitions() throws IllegalAccessException, NoSuchFieldException {
    bootstrappingVeniceChangelogConsumer.start();

    Field isStartedField =
        BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("isStarted");
    isStartedField.setAccessible(true);
    assertTrue((Boolean) isStartedField.get(bootstrappingVeniceChangelogConsumer), "isStarted should be true");

    verify(mockDaVinciClient).start();

    Set<Integer> partitionSet = new HashSet<>();
    for (int i = 0; i < PARTITION_COUNT; i++) {
      partitionSet.add(i);
    }

    Field subscribedPartitionsField =
        BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("subscribedPartitions");
    subscribedPartitionsField.setAccessible(true);
    assertEquals(subscribedPartitionsField.get(bootstrappingVeniceChangelogConsumer), partitionSet);
  }

  @Test
  public void testStartSpecificPartitions() throws IllegalAccessException, NoSuchFieldException {
    Set<Integer> partitionSet = Collections.singleton(1);
    bootstrappingVeniceChangelogConsumer.start(partitionSet);

    Field isStartedField =
        BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("isStarted");
    isStartedField.setAccessible(true);
    assertTrue((Boolean) isStartedField.get(bootstrappingVeniceChangelogConsumer), "isStarted should be true");

    verify(mockDaVinciClient).start();

    Field subscribedPartitionsField =
        BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("subscribedPartitions");
    subscribedPartitionsField.setAccessible(true);
    assertEquals(subscribedPartitionsField.get(bootstrappingVeniceChangelogConsumer), partitionSet);
  }

  @Test
  public void testStop() throws Exception {
    CachingDaVinciClientFactory daVinciClientFactoryMock = mock(CachingDaVinciClientFactory.class);
    Field daVinciClientFactoryField =
        BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("daVinciClientFactory");
    daVinciClientFactoryField.setAccessible(true);
    daVinciClientFactoryField.set(bootstrappingVeniceChangelogConsumer, daVinciClientFactoryMock);

    bootstrappingVeniceChangelogConsumer.start();

    Field isStartedField =
        BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("isStarted");
    isStartedField.setAccessible(true);
    assertTrue((Boolean) isStartedField.get(bootstrappingVeniceChangelogConsumer), "isStarted should be true");

    bootstrappingVeniceChangelogConsumer.stop();
    assertFalse((Boolean) isStartedField.get(bootstrappingVeniceChangelogConsumer), "isStarted should be false");

    verify(daVinciClientFactoryMock).close();
  }

  @Test
  public void testPutAndDelete() {
    bootstrappingVeniceChangelogConsumer.start();
    recordTransformer.onStartVersionIngestion(true);

    int value = 2;
    Lazy<Integer> lazyValue = Lazy.of(() -> value);

    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      recordTransformer.processPut(keys.get(partitionId), lazyValue, partitionId);
    }
    verifyPuts(value);

    // Verify deletes
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      recordTransformer.processDelete(keys.get(partitionId), partitionId);
    }
    verifyDeletes();
  }

  @Test
  public void testVersionSwap() {
    BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl.DaVinciRecordTransformerBootstrappingChangelogConsumer futureRecordTransformer =
        bootstrappingVeniceChangelogConsumer.new DaVinciRecordTransformerBootstrappingChangelogConsumer(
            FUTURE_STORE_VERSION, keySchema, valueSchema, valueSchema, mockDaVinciRecordTransformerConfig);

    bootstrappingVeniceChangelogConsumer.start();
    recordTransformer.onStartVersionIngestion(true);
    // Setting this to true to verify that the next current version doesn't start serving immediately.
    // It should only serve when it processes the VSM.
    futureRecordTransformer.onStartVersionIngestion(true);

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
      recordTransformer.processPut(keys.get(partitionId), lazyCurrentVersionValueValue, partitionId);
      futureRecordTransformer.processPut(keys.get(partitionId), lazyFutureVersionValueValue, partitionId);
    }
    verifyPuts(currentVersionValue);

    // Verify compaction
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      recordTransformer.processPut(keys.get(partitionId), lazyCurrentVersionValueValue, partitionId);
      recordTransformer.processPut(keys.get(partitionId), lazyCurrentVersionValueValue, partitionId);
    }
    verifyPuts(currentVersionValue);

    // Verify only the future version is allowed to perform the version swap
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      recordTransformer.onVersionSwap(CURRENT_STORE_VERSION, FUTURE_STORE_VERSION, partitionId);
      recordTransformer.processPut(keys.get(partitionId), lazyCurrentVersionValueValue, partitionId);
      futureRecordTransformer.processPut(keys.get(partitionId), lazyFutureVersionValueValue, partitionId);
    }
    verifyPuts(currentVersionValue);

    // Perform a version swap from the future version and verify the buffer only contains FUTURE_STORE_VERSION's values
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      futureRecordTransformer.onVersionSwap(CURRENT_STORE_VERSION, FUTURE_STORE_VERSION, partitionId);
      recordTransformer.processPut(keys.get(partitionId), lazyCurrentVersionValueValue, partitionId);
      futureRecordTransformer.processPut(keys.get(partitionId), lazyFutureVersionValueValue, partitionId);
    }
    verifyPuts(futureVersionValue);
  }

  @Test
  public void testCompletableFutureFromStart() {
    DaVinciRecordTransformer futureRecordTransformer =
        bootstrappingVeniceChangelogConsumer.new DaVinciRecordTransformerBootstrappingChangelogConsumer(
            FUTURE_STORE_VERSION, keySchema, valueSchema, valueSchema, mockDaVinciRecordTransformerConfig);

    CompletableFuture startCompletableFuture = bootstrappingVeniceChangelogConsumer.start();
    recordTransformer.onStartVersionIngestion(true);
    futureRecordTransformer.onStartVersionIngestion(false);

    // CompletableFuture should not be finished until a record has been pushed to the buffer by the current version
    assertFalse(startCompletableFuture.isDone());

    int value = 2;
    Lazy<Integer> lazyValue = Lazy.of(() -> value);

    // Future version should not cause the CompletableFuture to complete
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      futureRecordTransformer.processPut(keys.get(partitionId), lazyValue, partitionId);
    }
    assertFalse(startCompletableFuture.isDone());

    // CompletableFuture should be finished when the current version produces to the buffer
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      recordTransformer.processPut(keys.get(partitionId), lazyValue, partitionId);
    }
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      assertTrue(startCompletableFuture.isDone());
    });
  }

  @Test
  public void testTransformResult() {
    int value = 2;
    int partitionId = 0;
    Lazy<Integer> lazyValue = Lazy.of(() -> value);
    DaVinciRecordTransformerResult.Result result =
        recordTransformer.transform(keys.get(partitionId), lazyValue, partitionId).getResult();
    assertSame(result, DaVinciRecordTransformerResult.Result.UNCHANGED);
  }

  @Test
  public void testMaxBufferSize() throws NoSuchFieldException, IllegalAccessException, InterruptedException {
    ReentrantLock bufferLock = Mockito.spy(new ReentrantLock());
    Condition bufferIsFullCondition = Mockito.spy(bufferLock.newCondition());

    Field bufferLockField =
        BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("bufferLock");
    bufferLockField.setAccessible(true);
    bufferLockField.set(bootstrappingVeniceChangelogConsumer, bufferLock);

    Field bufferIsFullConditionField = BootstrappingVeniceChangelogConsumerDaVinciRecordTransformerImpl.class
        .getDeclaredField("bufferIsFullCondition");
    bufferIsFullConditionField.setAccessible(true);
    bufferIsFullConditionField.set(bootstrappingVeniceChangelogConsumer, bufferIsFullCondition);

    assertEquals(changelogClientConfig.getMaxBufferSize(), MAX_BUFFER_SIZE);

    bootstrappingVeniceChangelogConsumer.start();
    recordTransformer.onStartVersionIngestion(true);

    int partitionId = 1;
    int value = 2;
    Lazy<Integer> lazyValue = Lazy.of(() -> value);

    List<CompletableFuture> completableFutureList = new ArrayList<>();
    for (int i = 0; i <= MAX_BUFFER_SIZE; i++) {
      completableFutureList.add(CompletableFuture.supplyAsync(() -> {
        recordTransformer.processPut(keys.get(partitionId), lazyValue, partitionId);
        return null;
      }));
    }

    // Buffer is full signal should be hit
    verify(bufferLock, atLeastOnce()).lock();
    verify(bufferLock, atLeastOnce()).unlock();
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
    bootstrappingVeniceChangelogConsumer.poll(timeoutInMs);
    verify(bufferIsFullCondition, never()).await(timeoutInMs, TimeUnit.MILLISECONDS);

    reset(bufferLock);
    reset(bufferIsFullCondition);

    // Empty the buffer and verify that all CompletableFutures are done
    bootstrappingVeniceChangelogConsumer.poll(timeoutInMs);
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
      bootstrappingVeniceChangelogConsumer.poll(timeoutInMs);
      return null;
    });

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      verify(bufferIsFullCondition).await(timeoutInMs, TimeUnit.MILLISECONDS);
    });

    for (int i = 0; i < MAX_BUFFER_SIZE; i++) {
      CompletableFuture.supplyAsync(() -> {
        recordTransformer.processPut(keys.get(partitionId), lazyValue, partitionId);
        return null;
      });
    }

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      verify(bufferIsFullCondition).signal();
    });
  }

  private void verifyPuts(int value) {
    Collection<PubSubMessage<Integer, ChangeEvent<Integer>, VeniceChangeCoordinate>> pubSubMessages =
        bootstrappingVeniceChangelogConsumer.poll(POLL_TIMEOUT);
    assertEquals(pubSubMessages.size(), PARTITION_COUNT);
    int i = 0;
    for (PubSubMessage<Integer, ChangeEvent<Integer>, VeniceChangeCoordinate> message: pubSubMessages) {
      assertEquals((int) message.getKey(), i);
      ChangeEvent<Integer> changeEvent = message.getValue();
      assertNull(changeEvent.getPreviousValue());
      assertEquals((int) changeEvent.getCurrentValue(), value);
      i++;
    }
    assertEquals(bootstrappingVeniceChangelogConsumer.poll(POLL_TIMEOUT).size(), 0, "Buffer should be empty");
  }

  private void verifyDeletes() {
    Collection<PubSubMessage<Integer, ChangeEvent<Integer>, VeniceChangeCoordinate>> pubSubMessages =
        bootstrappingVeniceChangelogConsumer.poll(POLL_TIMEOUT);
    assertEquals(pubSubMessages.size(), PARTITION_COUNT);
    int i = 0;
    for (PubSubMessage<Integer, ChangeEvent<Integer>, VeniceChangeCoordinate> message: pubSubMessages) {
      assertEquals((int) message.getKey(), i);
      ChangeEvent<Integer> changeEvent = message.getValue();
      assertNull(changeEvent.getPreviousValue());
      assertNull(changeEvent.getCurrentValue());
      i++;
    }
    assertEquals(bootstrappingVeniceChangelogConsumer.poll(POLL_TIMEOUT).size(), 0, "Buffer should be empty");
  }
}
