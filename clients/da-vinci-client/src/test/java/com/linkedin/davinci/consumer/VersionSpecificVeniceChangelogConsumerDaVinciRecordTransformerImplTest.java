package com.linkedin.davinci.consumer;

import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.CONSUMER_METRIC_ENTITIES;
import static com.linkedin.venice.client.store.ClientConfig.DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME;
import static com.linkedin.venice.stats.ClientType.CHANGE_DATA_CAPTURE_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerRecordMetadata;
import com.linkedin.davinci.client.SeekableDaVinciClient;
import com.linkedin.davinci.consumer.stats.BasicConsumerStats;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VersionSpecificVeniceChangelogConsumerDaVinciRecordTransformerImplTest {
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
      new DaVinciRecordTransformerRecordMetadata(-1, 0, PubSubSymbolicPosition.EARLIEST, -1, null, -1);

  private Schema keySchema;
  private Schema valueSchema;
  private VeniceChangelogConsumerDaVinciRecordTransformerImpl<Integer, Integer> versionSpecificVeniceChangelogConsumer;
  private VeniceChangelogConsumerDaVinciRecordTransformerImpl.DaVinciRecordTransformerChangelogConsumer recordTransformer;
  private ChangelogClientConfig changelogClientConfig;
  private DaVinciRecordTransformerConfig mockDaVinciRecordTransformerConfig;
  private ControlMessage mockControlMessage;
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
        .setControllerD2ServiceName(D2_SERVICE_NAME)
        .setD2ServiceName(DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .setConsumerProperties(new Properties())
        .setLocalD2ZkHosts(TEST_ZOOKEEPER_ADDRESS)
        .setDatabaseSyncBytesInterval(TEST_DB_SYNC_BYTES_INTERVAL)
        .setD2Client(mock(D2Client.class))
        .setShouldCompactMessages(true)
        .setStoreVersion(CURRENT_STORE_VERSION)
        .setIncludeControlMessages(true);
    assertEquals(changelogClientConfig.getMaxBufferSize(), 1000, "Default max buffer size should be 1000");
    changelogClientConfig.setMaxBufferSize(MAX_BUFFER_SIZE);
    changelogClientConfig.getInnerClientConfig()
        .setMetricsRepository(getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true));

    veniceChangelogConsumerClientFactory = spy(new VeniceChangelogConsumerClientFactory(changelogClientConfig, null));
    versionSpecificVeniceChangelogConsumer = spy(
        new VeniceChangelogConsumerDaVinciRecordTransformerImpl<>(
            changelogClientConfig,
            veniceChangelogConsumerClientFactory));
    assertFalse(
        versionSpecificVeniceChangelogConsumer.getRecordTransformerConfig().isRecordTransformationEnabled(),
        "Record transformation should be disabled.");

    mockDaVinciRecordTransformerConfig = mock(DaVinciRecordTransformerConfig.class);
    recordTransformer =
        versionSpecificVeniceChangelogConsumer.new DaVinciRecordTransformerChangelogConsumer(TEST_STORE_NAME,
            CURRENT_STORE_VERSION, keySchema, valueSchema, valueSchema, mockDaVinciRecordTransformerConfig);
    mockControlMessage = mock(ControlMessage.class);
    when(mockControlMessage.getControlMessageType()).thenReturn(ControlMessageType.END_OF_PUSH.getValue());

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
        daVinciClientField.set(versionSpecificVeniceChangelogConsumer, mockDaVinciClient);

        Field changeCaptureStatsField =
            VeniceChangelogConsumerDaVinciRecordTransformerImpl.class.getDeclaredField("changeCaptureStats");
        changeCaptureStatsField.setAccessible(true);
        changeCaptureStats =
            spy((BasicConsumerStats) changeCaptureStatsField.get(versionSpecificVeniceChangelogConsumer));
        changeCaptureStatsField.set(versionSpecificVeniceChangelogConsumer, changeCaptureStats);

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

  @AfterMethod
  public void tearDown() {
    versionSpecificVeniceChangelogConsumer.close();
  }

  private ControlMessage createEndOfPushControlMessage() {
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.setControlMessageType(ControlMessageType.END_OF_PUSH.getValue());
    return controlMessage;
  }

  @Test
  public void testPutAndDelete() {
    versionSpecificVeniceChangelogConsumer.start();
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      recordTransformer.onStartVersionIngestion(partitionId, true);
    }

    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      recordTransformer.processPut(keys.get(partitionId), lazyValue, partitionId, recordMetadata);
      recordTransformer
          .onControlMessage(partitionId, recordMetadata.getPubSubPosition(), createEndOfPushControlMessage(), 0);
    }

    verifyPuts();

    // Verify deletes
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      recordTransformer.processDelete(keys.get(partitionId), partitionId, recordMetadata);
    }
    verifyDeletes();
  }

  @Test
  public void testCompactionWithControlMessages() {
    int targetPartitionId = 0;
    versionSpecificVeniceChangelogConsumer.start();

    recordTransformer.onStartVersionIngestion(targetPartitionId, true);

    ControlMessage versionSwapControlMessage = new ControlMessage();
    versionSwapControlMessage.setControlMessageType(ControlMessageType.VERSION_SWAP.getValue());

    // Messages: put(0, value), put(1, value), EOP, put(2, value), VS, put(1, value+1), put(0, value+2)
    recordTransformer.processPut(keys.get(0), Lazy.of(() -> value), targetPartitionId, recordMetadata);
    recordTransformer.processPut(keys.get(1), Lazy.of(() -> value), targetPartitionId, recordMetadata);
    recordTransformer
        .onControlMessage(targetPartitionId, recordMetadata.getPubSubPosition(), createEndOfPushControlMessage(), 0);
    recordTransformer.processPut(keys.get(2), Lazy.of(() -> value), targetPartitionId, recordMetadata);
    recordTransformer
        .onControlMessage(targetPartitionId, recordMetadata.getPubSubPosition(), versionSwapControlMessage, 0);
    recordTransformer.processPut(keys.get(1), Lazy.of(() -> value + 1), targetPartitionId, recordMetadata);
    recordTransformer.processPut(keys.get(0), Lazy.of(() -> value + 2), targetPartitionId, recordMetadata);

    clearInvocations(changeCaptureStats);
    Collection<PubSubMessage<Integer, ChangeEvent<Integer>, VeniceChangeCoordinate>> pubSubMessages =
        versionSpecificVeniceChangelogConsumer.poll(POLL_TIMEOUT);

    verify(changeCaptureStats).emitRecordsConsumedCountMetrics(7);
    verify(changeCaptureStats).emitPollCountMetrics(VeniceResponseStatusCategory.SUCCESS);
    assertEquals(pubSubMessages.size(), 5); // 5 messages expected after compaction

    // Verify the messages
    ArrayList<PubSubMessage<Integer, ChangeEvent<Integer>, VeniceChangeCoordinate>> messageList =
        new ArrayList<>(pubSubMessages);

    // Output: EOP, put(2, value), VS, put(1, value+1), put(0, value+2)
    assertNull(messageList.get(0).getKey()); // EOP control message
    assertEquals(
        ((ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Integer>>) messageList.get(0)).getControlMessage()
            .getControlMessageType(),
        ControlMessageType.END_OF_PUSH.getValue());
    assertEquals((int) messageList.get(1).getKey(), 2); // put(2, value)
    assertEquals((int) messageList.get(1).getValue().getCurrentValue(), value);
    assertNull(messageList.get(2).getKey()); // VS control message
    assertEquals(
        ((ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Integer>>) messageList.get(2)).getControlMessage()
            .getControlMessageType(),
        ControlMessageType.VERSION_SWAP.getValue());
    assertEquals((int) messageList.get(3).getKey(), 1); // put(1, value+1)
    assertEquals((int) messageList.get(3).getValue().getCurrentValue(), value + 1);
    assertEquals((int) messageList.get(4).getKey(), 0); // put(0, value+2)
    assertEquals((int) messageList.get(4).getValue().getCurrentValue(), value + 2);
  }

  @Test
  public void testHeartbeatControlMessage() {
    int targetPartitionId = 0;
    versionSpecificVeniceChangelogConsumer.start();
    recordTransformer.onStartVersionIngestion(targetPartitionId, true);

    ControlMessage heartbeat = new ControlMessage();
    heartbeat.setControlMessageType(ControlMessageType.START_OF_SEGMENT.getValue());
    long heartbeatTimestamp = 1000;
    recordTransformer
        .onControlMessage(targetPartitionId, recordMetadata.getPubSubPosition(), heartbeat, heartbeatTimestamp);

    Collection<PubSubMessage<Integer, ChangeEvent<Integer>, VeniceChangeCoordinate>> pubSubMessages =
        versionSpecificVeniceChangelogConsumer.poll(POLL_TIMEOUT);
    assertEquals(pubSubMessages.size(), 1);
    ImmutableChangeCapturePubSubMessage message =
        (ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Integer>>) pubSubMessages.iterator().next();
    assertEquals(message.getControlMessage().getControlMessageType(), ControlMessageType.START_OF_SEGMENT.getValue());
    assertEquals(message.getPubSubMessageTime(), heartbeatTimestamp);
  }

  @Test
  public void testBatchStoreSyntheticHeartbeatEnabledOnEndVersionIngestion() {
    when(mockDaVinciClient.isHybrid()).thenReturn(false);
    versionSpecificVeniceChangelogConsumer.start();

    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      recordTransformer.onStartVersionIngestion(partitionId, true);
    }

    // Before ingestion completes, synthetic heartbeat should not be enabled
    assertFalse(versionSpecificVeniceChangelogConsumer.isSyntheticHeartbeatEnabled());

    // Simulate batch ingestion completion
    recordTransformer.onEndVersionIngestion(CURRENT_STORE_VERSION);

    assertTrue(versionSpecificVeniceChangelogConsumer.isSyntheticHeartbeatEnabled());
  }

  @Test
  public void testHybridStoreSyntheticHeartbeatNotEnabled() {
    when(mockDaVinciClient.isHybrid()).thenReturn(true);
    versionSpecificVeniceChangelogConsumer.start();

    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      recordTransformer.onStartVersionIngestion(partitionId, true);
    }

    // Simulate ingestion completion — should NOT enable synthetic heartbeats since the store is hybrid
    recordTransformer.onEndVersionIngestion(CURRENT_STORE_VERSION);
    assertFalse(versionSpecificVeniceChangelogConsumer.isSyntheticHeartbeatEnabled());
  }

  @Test
  public void testSyntheticHeartbeatDisabledOnUnsubscribeAll() {
    when(mockDaVinciClient.isHybrid()).thenReturn(false);
    versionSpecificVeniceChangelogConsumer.start();

    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      recordTransformer.onStartVersionIngestion(partitionId, true);
    }
    recordTransformer.onEndVersionIngestion(CURRENT_STORE_VERSION);
    assertTrue(versionSpecificVeniceChangelogConsumer.isSyntheticHeartbeatEnabled());

    // Unsubscribe all partitions should disable synthetic heartbeats
    versionSpecificVeniceChangelogConsumer.unsubscribeAll();
    assertFalse(versionSpecificVeniceChangelogConsumer.isSyntheticHeartbeatEnabled());
  }

  @Test
  public void testPutsAndEopCoexistWithSyntheticHeartbeats() {
    when(mockDaVinciClient.isHybrid()).thenReturn(false);
    changelogClientConfig.setBackgroundReporterThreadSleepIntervalInSeconds(1L);
    versionSpecificVeniceChangelogConsumer.start();

    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      recordTransformer.onStartVersionIngestion(partitionId, true);
    }

    // Puts and EOP happen during ingestion
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      recordTransformer.processPut(keys.get(partitionId), lazyValue, partitionId, recordMetadata);
      recordTransformer
          .onControlMessage(partitionId, recordMetadata.getPubSubPosition(), createEndOfPushControlMessage(), 0);
    }

    // onEndVersionIngestion fires after ingestion completes
    recordTransformer.onEndVersionIngestion(CURRENT_STORE_VERSION);
    assertTrue(versionSpecificVeniceChangelogConsumer.isSyntheticHeartbeatEnabled());

    // Accumulate messages across polls — data/EOP arrive first, synthetic heartbeats come later
    Map<Integer, PubSubMessage<Integer, ChangeEvent<Integer>, VeniceChangeCoordinate>> dataMessages = new HashMap<>();
    Map<Integer, PubSubMessage<Integer, ChangeEvent<Integer>, VeniceChangeCoordinate>> eopMessages = new HashMap<>();
    AtomicInteger syntheticHeartbeatCount = new AtomicInteger(0);
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      Collection<PubSubMessage<Integer, ChangeEvent<Integer>, VeniceChangeCoordinate>> messages =
          versionSpecificVeniceChangelogConsumer.poll(POLL_TIMEOUT);
      for (PubSubMessage<Integer, ChangeEvent<Integer>, VeniceChangeCoordinate> message: messages) {
        if (message.getKey() != null) {
          dataMessages.put(message.getKey(), message);
        } else {
          ControlMessage controlMessage =
              ((ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Integer>>) message).getControlMessage();
          if (controlMessage.getControlMessageType() == ControlMessageType.START_OF_SEGMENT.getValue()) {
            syntheticHeartbeatCount.incrementAndGet();
          } else if (controlMessage.getControlMessageType() == ControlMessageType.END_OF_PUSH.getValue()) {
            eopMessages.put(message.getPartition(), message);
          }
        }
      }
      assertEquals(dataMessages.size(), PARTITION_COUNT, "Expected one put per partition");
      assertEquals(eopMessages.size(), PARTITION_COUNT, "Expected one EOP per partition");
      assertTrue(
          syntheticHeartbeatCount.get() >= PARTITION_COUNT,
          "Expected at least one synthetic heartbeat per partition");
    });
  }

  @Test
  public void testSyntheticHeartbeatUpdatesHeartbeatTimestamps() {
    when(mockDaVinciClient.isHybrid()).thenReturn(false);
    changelogClientConfig.setBackgroundReporterThreadSleepIntervalInSeconds(1L);
    versionSpecificVeniceChangelogConsumer.start();

    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      recordTransformer.onStartVersionIngestion(partitionId, true);
    }
    recordTransformer.onEndVersionIngestion(CURRENT_STORE_VERSION);

    // Put a message to trigger the startLatch and start the BackgroundReporterThread
    recordTransformer.processPut(keys.get(0), lazyValue, 0, recordMetadata);

    // Wait for recordStats to update heartbeat timestamps and emit metrics
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      Map<Integer, Long> heartbeats = versionSpecificVeniceChangelogConsumer.getLastHeartbeatPerPartition();
      assertEquals(heartbeats.size(), PARTITION_COUNT, "Expected heartbeats for all partitions");
      long now = System.currentTimeMillis();
      for (Map.Entry<Integer, Long> entry: heartbeats.entrySet()) {
        long lag = now - entry.getValue();
        assertTrue(
            lag < TimeUnit.MINUTES.toMillis(1),
            "Heartbeat for partition " + entry.getKey() + " is stale: " + lag + "ms");
      }
      verify(changeCaptureStats, atLeastOnce()).emitHeartBeatDelayMetrics(anyLong());
    });
  }

  @Test
  public void testSyntheticHeartbeatInsertsControlMessagesIntoBuffer() {
    when(mockDaVinciClient.isHybrid()).thenReturn(false);
    changelogClientConfig.setBackgroundReporterThreadSleepIntervalInSeconds(1L);
    versionSpecificVeniceChangelogConsumer.start();

    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      recordTransformer.onStartVersionIngestion(partitionId, true);
    }
    recordTransformer.onEndVersionIngestion(CURRENT_STORE_VERSION);

    // Put a message to trigger the startLatch and start the BackgroundReporterThread
    recordTransformer.processPut(keys.get(0), lazyValue, 0, recordMetadata);
    // Drain the initial put message
    versionSpecificVeniceChangelogConsumer.poll(POLL_TIMEOUT);

    // Wait for synthetic heartbeat control messages to appear in the poll buffer
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      Collection<PubSubMessage<Integer, ChangeEvent<Integer>, VeniceChangeCoordinate>> messages =
          versionSpecificVeniceChangelogConsumer.poll(POLL_TIMEOUT);
      assertFalse(messages.isEmpty(), "Expected synthetic heartbeat messages in buffer");
      for (PubSubMessage<Integer, ChangeEvent<Integer>, VeniceChangeCoordinate> message: messages) {
        assertNull(message.getKey(), "Synthetic heartbeat should have null key");
        ControlMessage controlMessage =
            ((ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Integer>>) message).getControlMessage();
        assertNotNull(controlMessage, "Synthetic heartbeat should have a control message");
        assertEquals(
            controlMessage.getControlMessageType(),
            ControlMessageType.START_OF_SEGMENT.getValue(),
            "Synthetic heartbeat should be START_OF_SEGMENT");
        assertTrue(message.getPubSubMessageTime() > 0, "Synthetic heartbeat should have a positive timestamp");
      }
    });
  }

  @Test
  public void testSubscribeToDeletedStoreStartsGracefully() {
    // Store deleted: daVinciClient.start() throws VeniceClientException wrapping VeniceNoStoreException synchronously
    doThrow(new VeniceClientException(new VeniceNoStoreException(TEST_STORE_NAME))).when(mockDaVinciClient).start();

    CompletableFuture<Void> startFuture = versionSpecificVeniceChangelogConsumer.start();

    assertTrue(startFuture.isDone(), "Start future should already be done (no leaked startFuture waiting on latch)");
    assertFalse(startFuture.isCompletedExceptionally(), "Start future should not have completed exceptionally");
    assertTrue(versionSpecificVeniceChangelogConsumer.isCaughtUp(), "Consumer should be caught up");
  }

  @Test
  public void testSubscribeToRetiredVersionStartsGracefully() {
    // Version retired: daVinciClient.subscribe() throws VeniceClientException synchronously from getVersion()
    when(mockDaVinciClient.subscribe(any())).thenThrow(
        new VeniceClientException(
            "Version: " + CURRENT_STORE_VERSION + " does not exist for store: " + TEST_STORE_NAME));

    CompletableFuture<Void> startFuture = versionSpecificVeniceChangelogConsumer.start();

    // Future should be an already-completed future, not a pending startFuture waiting on startLatch
    assertTrue(startFuture.isDone(), "Start future should already be done (no leaked startFuture waiting on latch)");
    assertFalse(startFuture.isCompletedExceptionally(), "Start future should not have completed exceptionally");
    assertTrue(versionSpecificVeniceChangelogConsumer.isCaughtUp(), "Consumer should be caught up");
  }

  @Test
  public void testSeekToCheckpointOnRetiredVersionStartsGracefully() {
    // Version retired: daVinciClient.seekToCheckpoint() throws VeniceClientException synchronously from getVersion()
    when(mockDaVinciClient.seekToCheckpoint(any())).thenThrow(
        new VeniceClientException(
            "Version: " + CURRENT_STORE_VERSION + " does not exist for store: " + TEST_STORE_NAME));

    Set<VeniceChangeCoordinate> checkpoints =
        Collections.singleton(new VeniceChangeCoordinate(TEST_STORE_NAME + "_v1", PubSubSymbolicPosition.EARLIEST, 0));
    CompletableFuture<Void> seekFuture = versionSpecificVeniceChangelogConsumer.seekToCheckpoint(checkpoints);

    assertTrue(seekFuture.isDone(), "Seek future should already be done (no leaked startFuture waiting on latch)");
    assertFalse(seekFuture.isCompletedExceptionally(), "Seek future should not have completed exceptionally");
    assertTrue(versionSpecificVeniceChangelogConsumer.isCaughtUp(), "Consumer should be caught up");
  }

  private void verifyPuts() {
    clearInvocations(changeCaptureStats);
    Collection<PubSubMessage<Integer, ChangeEvent<Integer>, VeniceChangeCoordinate>> pubSubMessages =
        versionSpecificVeniceChangelogConsumer.poll(POLL_TIMEOUT);

    verify(changeCaptureStats).emitRecordsConsumedCountMetrics(PARTITION_COUNT * 2);
    verify(changeCaptureStats).emitPollCountMetrics(VeniceResponseStatusCategory.SUCCESS);
    assertEquals(pubSubMessages.size(), PARTITION_COUNT * 2); // Each partition has one put and one control message

    int recordCounter = 0;
    int controlMessageCounter = 0;
    for (PubSubMessage<Integer, ChangeEvent<Integer>, VeniceChangeCoordinate> message: pubSubMessages) {
      if (message.getKey() == null) {
        // Control message
        controlMessageCounter++;
        continue;
      }
      assertEquals((int) message.getKey(), recordCounter);
      ChangeEvent<Integer> changeEvent = message.getValue();
      assertNull(changeEvent.getPreviousValue());
      assertEquals((int) changeEvent.getCurrentValue(), value);
      assertEquals(message.getPayloadSize(), -1);
      assertNotNull(message.getPosition());
      assertEquals(message.getPubSubMessageTime(), 0);
      recordCounter++;
    }
    assertEquals(controlMessageCounter, PARTITION_COUNT, "Control message count mismatch");

    clearInvocations(changeCaptureStats);
    assertEquals(versionSpecificVeniceChangelogConsumer.poll(POLL_TIMEOUT).size(), 0, "Buffer should be empty");
    verify(changeCaptureStats).emitRecordsConsumedCountMetrics(0);
    verify(changeCaptureStats).emitPollCountMetrics(VeniceResponseStatusCategory.SUCCESS);
  }

  private void verifyDeletes() {
    clearInvocations(changeCaptureStats);
    Collection<PubSubMessage<Integer, ChangeEvent<Integer>, VeniceChangeCoordinate>> pubSubMessages =
        versionSpecificVeniceChangelogConsumer.poll(POLL_TIMEOUT);
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
    assertEquals(versionSpecificVeniceChangelogConsumer.poll(POLL_TIMEOUT).size(), 0, "Buffer should be empty");
    verify(changeCaptureStats).emitRecordsConsumedCountMetrics(0);
    verify(changeCaptureStats).emitPollCountMetrics(VeniceResponseStatusCategory.SUCCESS);
  }
}
