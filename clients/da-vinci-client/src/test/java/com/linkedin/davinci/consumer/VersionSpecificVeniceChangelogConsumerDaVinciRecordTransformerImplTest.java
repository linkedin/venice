package com.linkedin.davinci.consumer;

import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.CONSUMER_METRIC_ENTITIES;
import static com.linkedin.venice.client.store.ClientConfig.DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME;
import static com.linkedin.venice.stats.ClientType.CHANGE_DATA_CAPTURE_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerRecordMetadata;
import com.linkedin.davinci.client.SeekableDaVinciClient;
import com.linkedin.davinci.consumer.stats.BasicConsumerStats;
import com.linkedin.davinci.replication.merge.RmdSerDe;
import com.linkedin.davinci.replication.merge.StringAnnotatedStoreSchemaCache;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.utils.lazy.Lazy;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
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
  private String replicationMetadataSchemaString;

  @BeforeMethod
  public void setUp() throws NoSuchFieldException, IllegalAccessException {
    SchemaReader mockSchemaReader = mock(SchemaReader.class);
    keySchema = Schema.create(Schema.Type.INT);
    doReturn(keySchema).when(mockSchemaReader).getKeySchema();
    valueSchema = Schema.create(Schema.Type.INT);
    doReturn(valueSchema).when(mockSchemaReader).getValueSchema(1);

    D2ControllerClient mockD2ControllerClient = mock(D2ControllerClient.class);
    MultiSchemaResponse mockRmdSchemaResponse = mock(MultiSchemaResponse.class);
    doReturn(mockRmdSchemaResponse).when(mockD2ControllerClient).getAllReplicationMetadataSchemas(TEST_STORE_NAME);
    doReturn(false).when(mockRmdSchemaResponse).isError();
    replicationMetadataSchemaString = RmdSchemaGenerator.generateMetadataSchema(valueSchema, 1).toString();
    MultiSchemaResponse.Schema mockRmdSchema = mock(MultiSchemaResponse.Schema.class);
    MultiSchemaResponse.Schema[] rmdSchemas = new MultiSchemaResponse.Schema[1];
    rmdSchemas[0] = mockRmdSchema;
    doReturn(rmdSchemas).when(mockRmdSchemaResponse).getSchemas();
    doReturn(1).when(mockRmdSchema).getRmdValueSchemaId();
    doReturn(1).when(mockRmdSchema).getId();
    doReturn(replicationMetadataSchemaString).when(mockRmdSchema).getSchemaStr();

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
        .setShouldCompactMessages(true)
        .setStoreVersion(CURRENT_STORE_VERSION)
        .setIncludeControlMessages(true)
        .setDeserializeReplicationMetadata(true);
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

  @Test
  public void testPutAndDelete() {
    versionSpecificVeniceChangelogConsumer.start();
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      recordTransformer.onStartVersionIngestion(partitionId, true);
    }

    ControlMessage controlMessage = new ControlMessage();
    controlMessage.setControlMessageType(ControlMessageType.END_OF_PUSH.getValue());
    for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
      recordTransformer.processPut(keys.get(partitionId), lazyValue, partitionId, recordMetadata);
      recordTransformer.onControlMessage(partitionId, recordMetadata.getPubSubPosition(), controlMessage, 0);
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

    ControlMessage endOfPushControlMessage = new ControlMessage();
    endOfPushControlMessage.setControlMessageType(ControlMessageType.END_OF_PUSH.getValue());

    ControlMessage versionSwapControlMessage = new ControlMessage();
    versionSwapControlMessage.setControlMessageType(ControlMessageType.VERSION_SWAP.getValue());

    // Messages: put(0, value), put(1, value), EOP, put(2, value), VS, put(1, value+1), put(0, value+2)
    recordTransformer.processPut(keys.get(0), Lazy.of(() -> value), targetPartitionId, recordMetadata);
    recordTransformer.processPut(keys.get(1), Lazy.of(() -> value), targetPartitionId, recordMetadata);
    recordTransformer
        .onControlMessage(targetPartitionId, recordMetadata.getPubSubPosition(), endOfPushControlMessage, 0);
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
  public void testDeserializedReplicationMetadata() {
    Schema rmdSchema = new Schema.Parser().parse(replicationMetadataSchemaString);
    long rmdTimestamp = 2000;
    GenericRecord rmdRecord = new GenericData.Record(rmdSchema);
    rmdRecord.put("timestamp", rmdTimestamp);
    rmdRecord.put("replication_checkpoint_vector", Arrays.asList(1L, 2L, 3L));
    ReadOnlySchemaRepository schemaRepository = mock(ReadOnlySchemaRepository.class);
    RmdSchemaEntry rmdSchemaEntry = mock(RmdSchemaEntry.class);
    doReturn(rmdSchema).when(rmdSchemaEntry).getSchema();
    doReturn(rmdSchemaEntry).when(schemaRepository).getReplicationMetadataSchema(TEST_STORE_NAME, 1, 1);
    StringAnnotatedStoreSchemaCache stringAnnotatedStoreSchemaCache =
        new StringAnnotatedStoreSchemaCache(TEST_STORE_NAME, schemaRepository);
    RmdSerDe rmdSerDe = new RmdSerDe(stringAnnotatedStoreSchemaCache, 1);
    ByteBuffer serializedRmdRecord = rmdSerDe.serializeRmdRecord(1, rmdRecord);

    int targetPartitionId = 0;
    versionSpecificVeniceChangelogConsumer.start();
    recordTransformer.onStartVersionIngestion(targetPartitionId, true);
    DaVinciRecordTransformerRecordMetadata recordMetadataWithRmd =
        new DaVinciRecordTransformerRecordMetadata(1, 0, PubSubSymbolicPosition.EARLIEST, -1, serializedRmdRecord, 1);
    recordTransformer.processPut(keys.get(0), Lazy.of(() -> value), targetPartitionId, recordMetadataWithRmd);

    Collection<PubSubMessage<Integer, ChangeEvent<Integer>, VeniceChangeCoordinate>> pubSubMessages =
        versionSpecificVeniceChangelogConsumer.poll(POLL_TIMEOUT);
    assertEquals(pubSubMessages.size(), 1);
    ImmutableChangeCapturePubSubMessage message =
        (ImmutableChangeCapturePubSubMessage<Integer, ChangeEvent<Integer>>) pubSubMessages.iterator().next();
    assertNotNull(message.getReplicationMetadataPayload());
    assertNotNull(message.getDeserializedReplicationMetadata());
    assertEquals(message.getDeserializedReplicationMetadata().get("timestamp"), rmdTimestamp);
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
