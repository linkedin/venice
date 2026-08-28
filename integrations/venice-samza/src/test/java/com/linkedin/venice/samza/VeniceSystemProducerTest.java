package com.linkedin.venice.samza;

import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.VALIDATE_VENICE_INTERNAL_SCHEMA_VERSION;
import static com.linkedin.venice.ConfigKeys.VENICE_PARTITIONERS;
import static com.linkedin.venice.VeniceConstants.SYSTEM_PROPERTY_FOR_APP_RUNNING_REGION;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_BUFFER_MEMORY;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.RouterBasedPushMonitor;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.BatchingVeniceWriter;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterHook;
import com.linkedin.venice.writer.VeniceWriterOptions;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class VeniceSystemProducerTest {
  @Test
  public void testPartialUpdateConversion() {
    VeniceSystemProducer producerInDC0 = new VeniceSystemProducer(
        new VeniceSystemProducerConfig.Builder().setStoreName("test_store")
            .setPushType(Version.PushType.BATCH)
            .setSamzaJobId("push-job-id-1")
            .setRunningFabric("dc-0")
            .setFactory(mock(VeniceSystemFactory.class))
            .setVeniceChildD2ZkHost("zookeeper.com:2181")
            .setPrimaryControllerColoD2ZKHost("zookeeper.com:2181")
            .setPrimaryControllerD2ServiceName("ChildController")
            .build());

    MultiSchemaResponse.Schema mockBaseSchema = new MultiSchemaResponse.Schema();
    mockBaseSchema.setSchemaStr(
        "{\"type\":\"record\",\"name\":\"nameRecord\",\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"firstName\",\"type\":\"string\",\"default\":\"\"},{\"name\":\"lastName\",\"type\":\"string\",\"default\":\"\"},{\"name\":\"age\",\"type\":\"int\",\"default\":-1}]}");
    mockBaseSchema.setId(1);
    mockBaseSchema.setDerivedSchemaId(-1);

    MultiSchemaResponse.Schema mockDerivedSchema = new MultiSchemaResponse.Schema();
    mockDerivedSchema.setSchemaStr(
        "{\"type\":\"record\",\"name\":\"nameRecordWriteOpRecord\",\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"firstName\",\"type\":[{\"type\":\"record\",\"name\":\"NoOp\",\"fields\":[]},\"string\"],\"default\":{}},{\"name\":\"lastName\",\"type\":[\"NoOp\",\"string\"],\"default\":{}},{\"name\":\"age\",\"type\":[\"NoOp\",\"int\"],\"default\":{}}]}");
    mockDerivedSchema.setId(1);
    mockDerivedSchema.setDerivedSchemaId(1);

    // build partial update
    char[] chars = new char[5];
    Arrays.fill(chars, 'f');
    String firstName = new String(chars);
    Arrays.fill(chars, 'l');
    String lastName = new String(chars);

    UpdateBuilder updateBuilder = new UpdateBuilderImpl(Schema.parse(mockDerivedSchema.getSchemaStr()));
    updateBuilder.setNewFieldValue("firstName", firstName);
    updateBuilder.setNewFieldValue("lastName", lastName);
    GenericRecord partialUpdateRecord = updateBuilder.build();

    // Test that we throw an exception if we can't find compatible schemas
    D2ControllerClient blankMockControllerclient = mock(D2ControllerClient.class);
    MultiSchemaResponse blankResponse = new MultiSchemaResponse();
    blankResponse.setSchemas(new MultiSchemaResponse.Schema[] {});
    when(blankMockControllerclient.getAllValueAndDerivedSchema(anyString())).thenReturn(blankResponse);
    producerInDC0.setControllerClient(blankMockControllerclient);

    Assert.assertThrows(
        () -> producerInDC0.convertPartialUpdateToFullPut(new Pair<Integer, Integer>(1, 1), partialUpdateRecord));

    // Set up the mock controller client that returns the schemas we need
    MultiSchemaResponse response = new MultiSchemaResponse();
    response.setSchemas(new MultiSchemaResponse.Schema[] { mockBaseSchema, mockDerivedSchema });
    D2ControllerClient mockControllerClient = mock(D2ControllerClient.class);
    when(mockControllerClient.getAllValueAndDerivedSchema(anyString())).thenReturn(response);
    producerInDC0.setControllerClient(mockControllerClient);

    // Verify partial update conversion
    GenericRecord result = (GenericRecord) producerInDC0
        .convertPartialUpdateToFullPut(new Pair<Integer, Integer>(1, 1), partialUpdateRecord);
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getSchema().toString(), mockBaseSchema.getSchemaStr());
    Assert.assertEquals(result.get("firstName"), partialUpdateRecord.get("firstName"));
    Assert.assertEquals(result.get("lastName"), partialUpdateRecord.get("lastName"));
    Assert.assertEquals(result.get("age"), -1);

    OutgoingMessageEnvelope envelope =
        new OutgoingMessageEnvelope(new SystemStream("venice", "storeName"), "key1", partialUpdateRecord);

    Assert.assertThrows(() -> producerInDC0.send("venice", envelope));
    producerInDC0.stop();
  }

  @Test(dataProvider = "BatchOrStreamReprocessing")
  public void testGetVeniceWriter(Version.PushType pushType) {
    VeniceSystemProducer producerInDC0 = new VeniceSystemProducer(
        new VeniceSystemProducerConfig.Builder().setStoreName("test_store")
            .setPushType(pushType)
            .setSamzaJobId("push-job-id-1")
            .setRunningFabric("dc-0")
            .setFactory(mock(VeniceSystemFactory.class))
            .setVeniceChildD2ZkHost("zookeeper.com:2181")
            .setPrimaryControllerColoD2ZKHost("zookeeper.com:2181")
            .setPrimaryControllerD2ServiceName("ChildController")
            .build());

    VeniceSystemProducer veniceSystemProducerSpy = spy(producerInDC0);

    VeniceWriter<byte[], byte[], byte[]> veniceWriterMock = mock(VeniceWriter.class);
    ArgumentCaptor<Properties> propertiesArgumentCaptor = ArgumentCaptor.forClass(Properties.class);
    ArgumentCaptor<VeniceWriterOptions> veniceWriterOptionsArgumentCaptor =
        ArgumentCaptor.forClass(VeniceWriterOptions.class);

    doReturn(veniceWriterMock).when(veniceSystemProducerSpy)
        .constructVeniceWriter(propertiesArgumentCaptor.capture(), veniceWriterOptionsArgumentCaptor.capture());

    VersionCreationResponse versionCreationResponse = new VersionCreationResponse();
    versionCreationResponse.setKafkaBootstrapServers("venice-kafka.db:2023");
    versionCreationResponse.setPartitions(2);
    versionCreationResponse.setKafkaTopic("test_store_v1");

    AbstractVeniceWriter<byte[], byte[], byte[]> resultantVeniceWriter =
        veniceSystemProducerSpy.getVeniceWriter(versionCreationResponse);

    Properties capturedProperties = propertiesArgumentCaptor.getValue();
    VeniceWriterOptions capturedVwo = veniceWriterOptionsArgumentCaptor.getValue();

    assertNotNull(resultantVeniceWriter);
    assertEquals(resultantVeniceWriter, veniceWriterMock);
    assertEquals(capturedProperties.getProperty(KAFKA_BOOTSTRAP_SERVERS), "venice-kafka.db:2023");
    assertEquals(capturedVwo.getTopicName(), "test_store_v1");
    if (pushType != Version.PushType.BATCH && pushType != Version.PushType.STREAM_REPROCESSING) {
      // invoke create venice write without partition count
      assertNull(capturedVwo.getPartitionCount());
    } else {
      assertNotNull(capturedVwo.getPartitionCount());
      assertEquals((int) capturedVwo.getPartitionCount(), 2);
    }
  }

  @Test
  public void testWriterHookPassedThroughToVeniceWriter() {
    VeniceWriterHook mockHook = mock(VeniceWriterHook.class);
    VeniceSystemProducer producer = new VeniceSystemProducer(
        new VeniceSystemProducerConfig.Builder().setStoreName("test_store")
            .setPushType(Version.PushType.BATCH)
            .setSamzaJobId("push-job-id-1")
            .setRunningFabric("dc-0")
            .setFactory(mock(VeniceSystemFactory.class))
            .setVeniceChildD2ZkHost("zookeeper.com:2181")
            .setPrimaryControllerColoD2ZKHost("zookeeper.com:2181")
            .setPrimaryControllerD2ServiceName("ChildController")
            .setWriterHook(mockHook)
            .build());

    VeniceSystemProducer producerSpy = spy(producer);
    ArgumentCaptor<VeniceWriterOptions> optionsCaptor = ArgumentCaptor.forClass(VeniceWriterOptions.class);
    VeniceWriter<byte[], byte[], byte[]> mockWriter = mock(VeniceWriter.class);
    doReturn(mockWriter).when(producerSpy).constructVeniceWriter(any(), optionsCaptor.capture());

    VersionCreationResponse versionCreationResponse = new VersionCreationResponse();
    versionCreationResponse.setKafkaBootstrapServers("kafka:9092");
    versionCreationResponse.setPartitions(1);
    versionCreationResponse.setKafkaTopic("test_store_v1");

    producerSpy.getVeniceWriter(versionCreationResponse);

    assertEquals(optionsCaptor.getValue().getWriterHook(), mockHook);
  }

  @Test
  public void testWriterHookCalledForPutAndDeleteViaSystemProducer() {
    VeniceWriterHook mockHook = mock(VeniceWriterHook.class);

    PubSubProducerAdapter mockPubSubProducer = mock(PubSubProducerAdapter.class);
    java.util.concurrent.CompletableFuture mockFuture = mock(java.util.concurrent.CompletableFuture.class);
    when(mockPubSubProducer.sendMessage(any(), any(), any(), any(), any(), any())).thenReturn(mockFuture);
    VeniceWriter<byte[], byte[], byte[]> realWriter = new VeniceWriter(
        new VeniceWriterOptions.Builder("test_store_rt").setPartitionCount(1).setWriterHook(mockHook).build(),
        VeniceProperties.empty(),
        mockPubSubProducer);

    ControllerClient mockControllerClient = buildMockControllerClient(1, -1);
    VeniceSystemProducer producerSpy = buildStartedProducerSpy(mockControllerClient, realWriter);

    awaitSubmitted(producerSpy.send("myKey", "myValue"));
    verify(mockHook).onBeforeProduce(eq(VeniceWriterHook.OperationType.PUT), anyInt(), anyInt());

    awaitSubmitted(producerSpy.send((Object) "myKey", null));
    verify(mockHook).onBeforeProduce(eq(VeniceWriterHook.OperationType.DELETE), anyInt(), eq(0));

    producerSpy.stop();
  }

  @Test
  public void testWriterHookCalledForUpdateViaSystemProducer() {
    VeniceWriterHook mockHook = mock(VeniceWriterHook.class);

    PubSubProducerAdapter mockPubSubProducer = mock(PubSubProducerAdapter.class);
    java.util.concurrent.CompletableFuture mockFuture = mock(java.util.concurrent.CompletableFuture.class);
    when(mockPubSubProducer.sendMessage(any(), any(), any(), any(), any(), any())).thenReturn(mockFuture);
    VeniceWriter<byte[], byte[], byte[]> realWriter = new VeniceWriter(
        new VeniceWriterOptions.Builder("test_store_rt").setPartitionCount(1).setWriterHook(mockHook).build(),
        VeniceProperties.empty(),
        mockPubSubProducer);

    ControllerClient mockControllerClient = buildMockControllerClient(1, 1, true, "test_store_rt");
    VeniceSystemProducer producerSpy = buildStartedProducerSpy(mockControllerClient, realWriter);

    awaitSubmitted(producerSpy.send("myKey", "myValue"));
    verify(mockHook).onBeforeProduce(eq(VeniceWriterHook.OperationType.UPDATE), anyInt(), anyInt());

    producerSpy.stop();
  }

  @Test(dataProvider = "BatchOrStreamReprocessing")
  public void testSendThrowsExceptionForError(Version.PushType pushType) {
    VeniceSystemProducer producerInDC0 = new VeniceSystemProducer(
        new VeniceSystemProducerConfig.Builder().setStoreName("test_store")
            .setPushType(pushType)
            .setSamzaJobId("push-job-id-1")
            .setRunningFabric("dc-0")
            .setFactory(mock(VeniceSystemFactory.class))
            .setDiscoveryUrl("discoveryUrl")
            .build());
    VeniceSystemProducer mockveniceSystemProducer = spy(producerInDC0);
    doNothing().when(mockveniceSystemProducer).setupClientsAndReInitProvider();
    doNothing().when(mockveniceSystemProducer).refreshSchemaCache();
    doNothing().when(mockveniceSystemProducer).getKeySchema();
    ControllerClient mockControllerClient = mock(ControllerClient.class);

    VersionCreationResponse mockVersionCreationResponse = new VersionCreationResponse();
    // set correct topicName for different pushType
    if (pushType == Version.PushType.BATCH) {
      mockVersionCreationResponse.setKafkaTopic("test_store_v1");
    } else if (pushType == Version.PushType.STREAM_REPROCESSING) {
      mockVersionCreationResponse.setKafkaTopic("test_store_v1_sr");
    }
    when(
        mockControllerClient.requestTopicForWrites(
            anyString(),
            anyLong(),
            any(),
            anyString(),
            anyBoolean(),
            anyBoolean(),
            anyBoolean(),
            any(),
            any(),
            any(),
            anyBoolean(),
            anyLong())).thenReturn(mockVersionCreationResponse);

    StoreResponse mockStoreResponse = new StoreResponse();
    StoreInfo mockStoreInfo = new StoreInfo();
    List<Version> versions = new ArrayList<>();
    versions.add(new VersionImpl("test_store", 0, "test_store_v1"));
    mockStoreInfo.setVersions(versions);
    mockStoreResponse.setStore(mockStoreInfo);
    when(mockControllerClient.getStore(anyString())).thenReturn(mockStoreResponse);

    VeniceWriter<byte[], byte[], byte[]> mockVeniceWriter = mock(VeniceWriter.class);
    doReturn(mockVeniceWriter).when(mockveniceSystemProducer).getVeniceWriter(any());
    mockveniceSystemProducer.setControllerClient(mockControllerClient);
    mockveniceSystemProducer.start();
    RouterBasedPushMonitor mockPushMonitor = mock(RouterBasedPushMonitor.class);
    mockveniceSystemProducer.setPushMonitor(mockPushMonitor);

    when(mockPushMonitor.getCurrentStatus()).thenReturn(ExecutionStatus.ERROR);
    doAnswer(invocation -> null).when(mockveniceSystemProducer).send((Object) any(), (Object) any());
    try {
      mockveniceSystemProducer.send(
          "test",
          new OutgoingMessageEnvelope(new SystemStream("venice", "test_store"), "key1", new byte[] { 1, 2, 3 }));
      if (pushType == Version.PushType.STREAM_REPROCESSING) {
        fail();
      }
    } catch (Exception e) {
      if (pushType != Version.PushType.STREAM_REPROCESSING) {
        fail();
      }
      assertTrue(e.getMessage().contains("is in error state"));
    }

    when(mockPushMonitor.getCurrentStatus()).thenReturn(ExecutionStatus.DVC_INGESTION_ERROR_OTHER);
    doAnswer(invocation -> null).when(mockveniceSystemProducer).send((Object) any(), (Object) any());
    try {
      mockveniceSystemProducer.send(
          "test",
          new OutgoingMessageEnvelope(new SystemStream("venice", "test_store"), "key1", new byte[] { 1, 2, 3 }));
      if (pushType == Version.PushType.STREAM_REPROCESSING) {
        fail();
      }
    } catch (Exception e) {
      if (pushType != Version.PushType.STREAM_REPROCESSING) {
        fail();
      }
      assertTrue(e.getMessage().contains("is in error state"));
    }

    when(mockPushMonitor.getCurrentStatus()).thenReturn(ExecutionStatus.COMPLETED);
    doAnswer(invocation -> null).when(mockveniceSystemProducer).send((Object) any(), (Object) any());
    try {
      mockveniceSystemProducer.send(
          "test",
          new OutgoingMessageEnvelope(new SystemStream("venice", "test_store"), "key1", new byte[] { 1, 2, 3 }));
    } catch (Exception e) {
      fail();
    }

    mockveniceSystemProducer.stop();
  }

  @DataProvider(name = "BatchOrStreamReprocessing")
  public Version.PushType[] batchOrStreamReprocessing() {
    return new Version.PushType[] { Version.PushType.BATCH, Version.PushType.STREAM_REPROCESSING,
        Version.PushType.STREAM, Version.PushType.INCREMENTAL };
  }

  @Test
  public void testExtractConcurrentProducerConfig() {
    Properties properties = new Properties();
    properties.put(VeniceWriter.PRODUCER_THREAD_COUNT, "2");
    properties.put(VeniceWriter.PRODUCER_QUEUE_SIZE, "102400000");

    VeniceWriterOptions.Builder builder = new VeniceWriterOptions.Builder("test_rt");
    VeniceSystemProducer.extractConcurrentProducerConfig(properties, builder);
    VeniceWriterOptions options = builder.build();
    assertEquals(options.getProducerThreadCount(), 2);
    assertEquals(options.getProducerQueueSize(), 102400000);
    assertEquals(properties.getProperty(KAFKA_BUFFER_MEMORY), "8388608");

    /**
     * if {@link KAFKA_BUFFER_MEMORY} is specified, {@link VeniceSystemProducer} shouldn't override.
     */

    properties = new Properties();
    properties.put(VeniceWriter.PRODUCER_THREAD_COUNT, "2");
    properties.put(VeniceWriter.PRODUCER_QUEUE_SIZE, "102400000");
    properties.put(KAFKA_BUFFER_MEMORY, "10240");

    builder = new VeniceWriterOptions.Builder("test_rt");
    VeniceSystemProducer.extractConcurrentProducerConfig(properties, builder);
    options = builder.build();
    assertEquals(options.getProducerThreadCount(), 2);
    assertEquals(options.getProducerQueueSize(), 102400000);
    assertEquals(properties.getProperty(KAFKA_BUFFER_MEMORY), "10240");
  }

  @Test
  public void testGetProducerRejectsPartialD2Clients() {
    VeniceSystemFactory factory = new VeniceSystemFactory();
    Config config = mock(Config.class);

    when(config.get(VeniceSystemFactory.DEPLOYMENT_ID)).thenReturn("test-job-id");
    when(config.get(VeniceSystemFactory.VENICE_CONTROLLER_DISCOVERY_URL)).thenReturn(null);
    when(config.get(VeniceSystemFactory.VENICE_PARENT_D2_ZK_HOSTS)).thenReturn("parent-zk:2181");
    when(config.get(VeniceSystemFactory.VENICE_CHILD_D2_ZK_HOSTS)).thenReturn("child-zk:2181");
    when(config.get(VeniceSystemFactory.VENICE_CHILD_CONTROLLER_D2_SERVICE)).thenReturn("ChildController");
    when(config.get(VeniceSystemFactory.VENICE_PARENT_CONTROLLER_D2_SERVICE)).thenReturn("ParentController");
    when(config.get(SYSTEM_PROPERTY_FOR_APP_RUNNING_REGION)).thenReturn("test-fabric");
    when(config.getBoolean(VALIDATE_VENICE_INTERNAL_SCHEMA_VERSION, true)).thenReturn(true);
    when(config.getBoolean(SSL_ENABLED, true)).thenReturn(false);
    when(config.get(VENICE_PARTITIONERS)).thenReturn(null);

    D2Client mockD2Client = mock(D2Client.class);
    Assert.assertThrows(
        SamzaException.class,
        () -> factory.getProducer("testSystem", "testStore", false, "STREAM", config, mockD2Client, null));
    Assert.assertThrows(
        SamzaException.class,
        () -> factory.getProducer("testSystem", "testStore", false, "STREAM", config, null, mockD2Client));
  }

  @Test
  public void testGetProducerWithD2ClientBranch() {
    VeniceSystemFactory factory = spy(new VeniceSystemFactory());
    Config config = mock(Config.class);
    D2Client mockChildD2Client = mock(D2Client.class);
    D2Client mockPrimaryD2Client = mock(D2Client.class);

    when(config.get(VeniceSystemFactory.DEPLOYMENT_ID)).thenReturn("test-job-id");
    when(config.get(VeniceSystemFactory.VENICE_CONTROLLER_DISCOVERY_URL)).thenReturn(null);
    when(config.get(VeniceSystemFactory.VENICE_PARENT_D2_ZK_HOSTS)).thenReturn("parent-zk:2181");
    when(config.get(VeniceSystemFactory.VENICE_CHILD_D2_ZK_HOSTS)).thenReturn("child-zk:2181");
    when(config.get(VeniceSystemFactory.VENICE_CHILD_CONTROLLER_D2_SERVICE)).thenReturn("ChildController");
    when(config.get(VeniceSystemFactory.VENICE_PARENT_CONTROLLER_D2_SERVICE)).thenReturn("ParentController");
    when(config.get(SYSTEM_PROPERTY_FOR_APP_RUNNING_REGION)).thenReturn("test-fabric");
    when(config.getBoolean(VALIDATE_VENICE_INTERNAL_SCHEMA_VERSION, true)).thenReturn(true);
    when(config.getBoolean(SSL_ENABLED, true)).thenReturn(false);
    when(config.get(VENICE_PARTITIONERS)).thenReturn(null);

    ArgumentCaptor<VeniceSystemProducerConfig> configCaptor = ArgumentCaptor.forClass(VeniceSystemProducerConfig.class);
    VeniceSystemProducer mockProducer = mock(VeniceSystemProducer.class);
    doReturn(mockProducer).when(factory).createSystemProducer(any(VeniceSystemProducerConfig.class));

    // D2Client branch: both D2 clients provided
    SystemProducer result1 =
        factory.getProducer("testSystem", "testStore", false, "STREAM", config, mockChildD2Client, mockPrimaryD2Client);
    assertNotNull(result1);

    verify(factory).createSystemProducer(configCaptor.capture());
    VeniceSystemProducerConfig capturedConfig = configCaptor.getValue();
    assertEquals(capturedConfig.getProvidedChildColoD2Client(), mockChildD2Client);
    assertEquals(capturedConfig.getProvidedPrimaryControllerColoD2Client(), mockPrimaryD2Client);
    assertNull(capturedConfig.getVeniceChildD2ZkHost());
    assertNull(capturedConfig.getPrimaryControllerColoD2ZKHost());
  }

  @Test
  public void testGetProducerWithZkHostBranch() {
    VeniceSystemFactory factory = spy(new VeniceSystemFactory());
    Config config = mock(Config.class);

    when(config.get(VeniceSystemFactory.DEPLOYMENT_ID)).thenReturn("test-job-id");
    when(config.get(VeniceSystemFactory.VENICE_CONTROLLER_DISCOVERY_URL)).thenReturn(null);
    when(config.get(VeniceSystemFactory.VENICE_PARENT_D2_ZK_HOSTS)).thenReturn("parent-zk:2181");
    when(config.get(VeniceSystemFactory.VENICE_CHILD_D2_ZK_HOSTS)).thenReturn("child-zk:2181");
    when(config.get(VeniceSystemFactory.VENICE_CHILD_CONTROLLER_D2_SERVICE)).thenReturn("ChildController");
    when(config.get(VeniceSystemFactory.VENICE_PARENT_CONTROLLER_D2_SERVICE)).thenReturn("ParentController");
    when(config.get(SYSTEM_PROPERTY_FOR_APP_RUNNING_REGION)).thenReturn("test-fabric");
    when(config.getBoolean(VALIDATE_VENICE_INTERNAL_SCHEMA_VERSION, true)).thenReturn(true);
    when(config.getBoolean(SSL_ENABLED, true)).thenReturn(false);
    when(config.get(VENICE_PARTITIONERS)).thenReturn(null);

    ArgumentCaptor<VeniceSystemProducerConfig> configCaptor = ArgumentCaptor.forClass(VeniceSystemProducerConfig.class);
    VeniceSystemProducer mockProducer = mock(VeniceSystemProducer.class);
    doReturn(mockProducer).when(factory).createSystemProducer(any(VeniceSystemProducerConfig.class));

    // ZK branch: no D2 clients, non-aggregate uses child ZK hosts
    SystemProducer result2 = factory.getProducer("testSystem", "testStore", false, "STREAM", config);
    assertNotNull(result2);

    verify(factory).createSystemProducer(configCaptor.capture());
    VeniceSystemProducerConfig capturedConfig = configCaptor.getValue();
    assertEquals(capturedConfig.getVeniceChildD2ZkHost(), "child-zk:2181");
    assertEquals(capturedConfig.getPrimaryControllerColoD2ZKHost(), "child-zk:2181");
    assertEquals(capturedConfig.getPrimaryControllerD2ServiceName(), "ChildController");
    assertNull(capturedConfig.getProvidedChildColoD2Client());
    assertNull(capturedConfig.getProvidedPrimaryControllerColoD2Client());
  }

  /**
   * Awaits the async STREAM dispatch submission so the writer interaction becomes observable. The protected
   * {@code send(Object, Object)} returns after bounded queue admission without waiting for the writer, so tests
   * that verify the writer/hook call must first await submission to stay deterministic.
   */
  private static void awaitSubmitted(java.util.concurrent.CompletableFuture<Void> future) {
    VeniceSystemProducerWriteCommand.awaitSubmission(future);
  }

  private VeniceSystemProducer buildStartedProducerSpy(
      ControllerClient mockControllerClient,
      AbstractVeniceWriter<byte[], byte[], byte[]> mockWriter) {
    return buildStartedProducerSpy(mockControllerClient, mockWriter, null);
  }

  private VeniceSystemProducer buildStartedProducerSpy(
      ControllerClient mockControllerClient,
      AbstractVeniceWriter<byte[], byte[], byte[]> mockWriter,
      Config samzaConfig) {
    VeniceSystemProducerConfig.Builder builder = new VeniceSystemProducerConfig.Builder().setStoreName("test_store")
        .setPushType(Version.PushType.STREAM)
        .setSamzaJobId("push-job-id-1")
        .setRunningFabric("dc-0")
        .setFactory(mock(VeniceSystemFactory.class))
        .setDiscoveryUrl("discoveryUrl");
    if (samzaConfig != null) {
      builder.setSamzaConfig(samzaConfig);
    }
    VeniceSystemProducer producerSpy = spy(new VeniceSystemProducer(builder.build()));
    doNothing().when(producerSpy).setupClientsAndReInitProvider();
    doNothing().when(producerSpy).refreshSchemaCache();
    producerSpy.setControllerClient(mockControllerClient);
    doReturn(mockWriter).when(producerSpy).getVeniceWriter(any());
    producerSpy.start();
    return producerSpy;
  }

  /**
   * Real {@link VeniceWriter} + a {@link PubSubProducerAdapter} whose first {@code sendMessage} (the lazy
   * START_OF_SEGMENT control record) blocks. Proves the async STREAM contract end-to-end through
   * VeniceSystemProducer: the protected {@code send(Object, Object)} returns after bounded queue admission while
   * the writer is still blocked (durability and submission both incomplete), whereas the public {@code put}
   * (which awaits submission) stays blocked until the writer submission returns. Same partition => same stripe =>
   * strict FIFO, so releasing the segment unblocks both in order.
   */
  @Test(timeOut = 30_000)
  public void streamSendIsAsyncWhileFirstSegmentBlocksAndPublicPutWaitsForSubmission() throws Exception {
    CountDownLatch firstSendEntered = new CountDownLatch(1);
    CountDownLatch releaseFirstSend = new CountDownLatch(1);
    AtomicInteger sendCount = new AtomicInteger();
    java.util.concurrent.atomic.AtomicReference<Object> firstKey = new java.util.concurrent.atomic.AtomicReference<>();
    java.util.concurrent.atomic.AtomicReference<Object> firstEnvelope =
        new java.util.concurrent.atomic.AtomicReference<>();

    PubSubProducerAdapter blockingAdapter = mock(PubSubProducerAdapter.class);
    when(blockingAdapter.sendMessage(any(), any(), any(), any(), any(), any())).thenAnswer(invocation -> {
      if (sendCount.getAndIncrement() == 0) {
        firstKey.set(invocation.getArgument(2));
        firstEnvelope.set(invocation.getArgument(3));
        firstSendEntered.countDown();
        assertTrue(releaseFirstSend.await(20, TimeUnit.SECONDS), "first sendMessage was never released");
      }
      PubSubProducerCallback callback = invocation.getArgument(5);
      PubSubProduceResult result = mock(PubSubProduceResult.class);
      if (callback != null) {
        callback.onCompletion(result, null);
      }
      return java.util.concurrent.CompletableFuture.completedFuture(result);
    });
    VeniceWriter<byte[], byte[], byte[]> realWriter = new VeniceWriter(
        new VeniceWriterOptions.Builder("test_store_rt").setPartitionCount(1).build(),
        VeniceProperties.empty(),
        blockingAdapter);
    VeniceSystemProducer producerSpy =
        buildStartedProducerSpy(buildMockControllerClient(1, -1), (AbstractVeniceWriter) realWriter);

    // Protected send is async: it returns after queue admission even though the worker is blocked on the segment.
    java.util.concurrent.CompletableFuture<Void> asyncFuture = producerSpy.send("asyncKey", "asyncValue");
    assertTrue(firstSendEntered.await(20, TimeUnit.SECONDS), "worker never reached the blocked writer");
    // The blocked send is the lazy START_OF_SEGMENT control record, not the data record: assert it explicitly
    // rather than assuming ordering by index.
    assertTrue(((KafkaKey) firstKey.get()).isControlMessage(), "first blocked send must be a control message");
    ControlMessage blockedControlMessage = (ControlMessage) ((KafkaMessageEnvelope) firstEnvelope.get()).payloadUnion;
    assertEquals(
        ControlMessageType.valueOf(blockedControlMessage),
        ControlMessageType.START_OF_SEGMENT,
        "first blocked send must be START_OF_SEGMENT");
    VeniceSystemProducerWriteCommand.DurableWriteFuture durable =
        (VeniceSystemProducerWriteCommand.DurableWriteFuture) asyncFuture;
    assertFalse(durable.getSubmissionFuture().isDone(), "submission completed while writer was blocked");
    assertFalse(asyncFuture.isDone(), "durable completed while writer was blocked");

    // Public put awaits submission: it must stay blocked (FIFO behind the blocked record) until release.
    CountDownLatch putReturned = new CountDownLatch(1);
    Thread putThread = new Thread(() -> {
      producerSpy.put("blockKey", "blockValue");
      putReturned.countDown();
    });
    putThread.start();
    assertFalse(putReturned.await(300, TimeUnit.MILLISECONDS), "public put returned before writer submission");

    releaseFirstSend.countDown();
    assertTrue(putReturned.await(20, TimeUnit.SECONDS), "public put never returned after release");
    durable.getSubmissionFuture().get(20, TimeUnit.SECONDS);
    asyncFuture.get(20, TimeUnit.SECONDS);
    putThread.join(TimeUnit.SECONDS.toMillis(20));

    producerSpy.stop();
  }

  /**
   * Public {@code put}, public {@code delete}, and the Samza envelope {@code send} all call the protected
   * {@code send(Object, Object)} exactly once and then wait through submission (durability handoff), whereas a
   * foreign (non-durable) future returned by an override must be a no-op wait so the public op returns
   * immediately without waiting or casting.
   */
  @Test(timeOut = 30_000)
  public void publicPutDeleteAndEnvelopeSendWaitForSubmissionAndForeignFutureDoesNot() throws Exception {
    VeniceSystemProducer producerSpy =
        buildStartedProducerSpy(buildMockControllerClient(1, -1), (AbstractVeniceWriter) mock(VeniceWriter.class));
    AtomicInteger sendInvocations = new AtomicInteger();
    java.util.concurrent.BlockingQueue<VeniceSystemProducerWriteCommand> issued =
        new java.util.concurrent.LinkedBlockingQueue<>();
    doAnswer(invocation -> {
      sendInvocations.incrementAndGet();
      VeniceSystemProducerWriteCommand command =
          VeniceSystemProducerWriteCommand.put(new byte[] { 1 }, new byte[] { 2 }, 1, 0L);
      issued.add(command);
      return command.getDurableFuture();
    }).when(producerSpy).send((Object) any(), any());

    SystemStream systemStream = new SystemStream("venice", "test_store");
    assertPublicOpWaitsForSubmission(() -> producerSpy.put("k", "v"), issued);
    assertPublicOpWaitsForSubmission(() -> producerSpy.delete("k"), issued);
    assertPublicOpWaitsForSubmission(
        () -> producerSpy.send("src", new OutgoingMessageEnvelope(systemStream, "k", "v")),
        issued);
    assertEquals(sendInvocations.get(), 3, "each public op must call protected send exactly once");

    // A foreign future from an override must not cause an internal wait/cast: the public put returns at once.
    java.util.concurrent.CompletableFuture<Void> foreign = new java.util.concurrent.CompletableFuture<>();
    doReturn(foreign).when(producerSpy).send((Object) any(), any());
    CountDownLatch putReturned = new CountDownLatch(1);
    Thread foreignPut = new Thread(() -> {
      producerSpy.put("k", "v");
      putReturned.countDown();
    });
    foreignPut.start();
    assertTrue(putReturned.await(20, TimeUnit.SECONDS), "public put must not wait on a foreign future");
    foreignPut.join();
    assertFalse(foreign.isDone(), "a foreign future must be left untouched");

    producerSpy.stop();
  }

  private void assertPublicOpWaitsForSubmission(
      Runnable publicOp,
      java.util.concurrent.BlockingQueue<VeniceSystemProducerWriteCommand> issued) throws Exception {
    CountDownLatch returned = new CountDownLatch(1);
    Thread t = new Thread(() -> {
      publicOp.run();
      returned.countDown();
    });
    t.start();
    VeniceSystemProducerWriteCommand command = issued.poll(20, TimeUnit.SECONDS);
    assertNotNull(command, "protected send was not invoked by the public op");
    assertFalse(returned.await(300, TimeUnit.MILLISECONDS), "public op returned before submission completed");
    command.finishSubmission(null);
    assertTrue(returned.await(20, TimeUnit.SECONDS), "public op did not return after submission completed");
    t.join();
  }

  /**
   * Invalid worker configs must fail fast at start() rather than silently defaulting: a negative worker count, a
   * nonpositive queue capacity, and a malformed integer are all rejected with {@link SamzaException}.
   */
  @Test(timeOut = 30_000)
  public void streamRejectsInvalidWorkerConfig() {
    assertStartRejectsConfig(VeniceSystemProducerWriteDispatcher.WORKER_COUNT_CONFIG, "-1");
    assertStartRejectsConfig(VeniceSystemProducerWriteDispatcher.WORKER_COUNT_CONFIG, "notAnInt");
    assertStartRejectsConfig(VeniceSystemProducerWriteDispatcher.WORKER_QUEUE_CAPACITY_CONFIG, "0");
  }

  private void assertStartRejectsConfig(String key, String value) {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(key, value);
    VeniceSystemProducerConfig.Builder builder = new VeniceSystemProducerConfig.Builder().setStoreName("test_store")
        .setPushType(Version.PushType.STREAM)
        .setSamzaJobId("push-job-id-1")
        .setRunningFabric("dc-0")
        .setFactory(mock(VeniceSystemFactory.class))
        .setDiscoveryUrl("discoveryUrl")
        .setSamzaConfig(new MapConfig(configMap));
    VeniceSystemProducer producerSpy = spy(new VeniceSystemProducer(builder.build()));
    doNothing().when(producerSpy).setupClientsAndReInitProvider();
    doNothing().when(producerSpy).refreshSchemaCache();
    producerSpy.setControllerClient(buildMockControllerClient(1, -1));
    doReturn(mock(VeniceWriter.class)).when(producerSpy).getVeniceWriter(any());
    try {
      producerSpy.start();
      fail("start() must reject invalid config " + key + "=" + value);
    } catch (SamzaException expected) {
      // expected: invalid config must not silently default.
    }
    // Validation runs before client setup and writer allocation, so neither was reached.
    verify(producerSpy, never()).setupClientsAndReInitProvider();
    verify(producerSpy, never()).getVeniceWriter(any());
    // The producer did not silently start inline: a retry re-validates and rejects again rather than proceeding.
    try {
      producerSpy.start();
      fail("a retry after invalid config must re-validate, not silently start inline");
    } catch (SamzaException expected) {
      // expected: still rejected, so no inline fallback slipped through.
    }
    verify(producerSpy, never()).getVeniceWriter(any());
  }

  /**
   * A worker count of 0 is the kill switch: no dispatcher is created, so writes run fully inline on the caller
   * thread exactly as before. The inline path returns a plain (non-durable) future, so the writer op is observable
   * immediately without awaiting submission.
   */
  @Test(timeOut = 30_000)
  public void streamWithZeroWorkersIsFullyInlineKillSwitch() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(VeniceSystemProducerWriteDispatcher.WORKER_COUNT_CONFIG, "0");

    VeniceWriter<byte[], byte[], byte[]> mockWriter = mock(VeniceWriter.class);
    VeniceSystemProducer producerSpy = buildStartedProducerSpy(
        buildMockControllerClient(1, -1),
        (AbstractVeniceWriter) mockWriter,
        new MapConfig(configMap));

    java.util.concurrent.CompletableFuture<Void> future = producerSpy.send("inlineKey", "inlineValue");
    // Inline: the writer was already called synchronously on this thread, before any await.
    verify(mockWriter).put(any(), any(), eq(1), anyLong(), any());
    assertFalse(
        future instanceof VeniceSystemProducerWriteCommand.DurableWriteFuture,
        "kill switch produced a durable future");

    producerSpy.stop();
  }

  /**
   * Focused batching-enabled coverage: a batching writer routes through the same async dispatcher unchanged. Proves
   * dispatch (worker calls the batching writer's op via partition routing), flush (fence + writer flush), and stop
   * (drain without closing the writer) preserve existing behavior for the batching path.
   */
  @Test(timeOut = 30_000)
  public void streamBatchingWriterDispatchFlushAndStopPreserveBehavior() {
    BatchingVeniceWriter<byte[], byte[], byte[]> batchingWriter = mock(BatchingVeniceWriter.class);
    when(batchingWriter.getPartitionId(any())).thenReturn(0);
    when(batchingWriter.put(any(), any(), anyInt(), anyLong(), any()))
        .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(mock(PubSubProduceResult.class)));

    VeniceSystemProducer producerSpy =
        buildStartedProducerSpy(buildMockControllerClient(1, -1), (AbstractVeniceWriter) batchingWriter);

    awaitSubmitted(producerSpy.send("batchKey", "batchValue"));
    verify(batchingWriter).put(any(), any(), eq(1), anyLong(), any());

    // Flush is the checkpoint fence: it must flush the underlying batching writer.
    producerSpy.flush("source");
    verify(batchingWriter).flush();

    // Stop drains workers and is idempotent; baseline writer close is owned by the producer, not the dispatcher.
    producerSpy.stop();
    producerSpy.stop();
  }

  /**
   * Item B: an interrupted {@code stop()} must still drain losslessly and close the writer before returning, then
   * restore the interrupt. A worker is blocked inside {@code writer.put}; the caller enters {@code stop()} already
   * interrupted. Graceful drain never force-interrupts the worker, so {@code stop()} does not return (and the writer
   * is not closed) until the worker is released. Once it returns, the writer has been closed and the interrupt is set.
   */
  @Test(timeOut = 30_000)
  public void interruptedStopStillClosesWriterThenRestoresInterrupt() throws Exception {
    CountDownLatch workerInPut = new CountDownLatch(1);
    CountDownLatch releasePut = new CountDownLatch(1);
    VeniceWriter<byte[], byte[], byte[]> mockWriter = mock(VeniceWriter.class);
    when(mockWriter.put(any(), any(), anyInt(), anyLong(), any())).thenAnswer(invocation -> {
      workerInPut.countDown();
      releasePut.await();
      return java.util.concurrent.CompletableFuture.completedFuture(mock(PubSubProduceResult.class));
    });

    VeniceSystemProducer producerSpy =
        buildStartedProducerSpy(buildMockControllerClient(1, -1), (AbstractVeniceWriter) mockWriter);

    // Admit one async write; the worker enters writer.put and blocks there.
    producerSpy.send("k", "v");
    assertTrue(workerInPut.await(10, TimeUnit.SECONDS), "worker never entered writer.put");

    java.util.concurrent.atomic.AtomicBoolean interruptSetOnReturn = new java.util.concurrent.atomic.AtomicBoolean();
    CountDownLatch stopReturned = new CountDownLatch(1);
    Thread stopper = new Thread(() -> {
      Thread.currentThread().interrupt(); // enter stop() already interrupted
      producerSpy.stop();
      interruptSetOnReturn.set(Thread.currentThread().isInterrupted());
      stopReturned.countDown();
    });
    stopper.start();

    // stop() must keep draining (not return, not close the writer) while the worker is still running.
    assertFalse(stopReturned.await(300, TimeUnit.MILLISECONDS), "stop() returned before the worker drained");
    verify(mockWriter, never()).close();

    // Release the worker: the drain completes, the writer is closed, stop() returns with the interrupt restored.
    releasePut.countDown();
    assertTrue(stopReturned.await(10, TimeUnit.SECONDS), "stop() never returned after the worker drained");
    verify(mockWriter).close();
    assertTrue(interruptSetOnReturn.get(), "stop() dropped the observed interrupt instead of restoring it");
  }

  @Test(timeOut = 30_000)
  public void interruptedIdleStopStillClosesWriterThenRestoresInterrupt() throws Exception {
    // The idle / already-drained dispatcher case: the worker drain returns immediately and never throws
    // InterruptedException, so only capturing the entry interrupt at the top of stop() preserves it. stop()
    // must still close the writer with the interrupt clear, then restore the interrupt on return.
    VeniceWriter<byte[], byte[], byte[]> mockWriter = mock(VeniceWriter.class);
    when(mockWriter.put(any(), any(), anyInt(), anyLong(), any()))
        .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(mock(PubSubProduceResult.class)));

    VeniceSystemProducer producerSpy =
        buildStartedProducerSpy(buildMockControllerClient(1, -1), (AbstractVeniceWriter) mockWriter);

    java.util.concurrent.atomic.AtomicBoolean interruptSetOnReturn = new java.util.concurrent.atomic.AtomicBoolean();
    CountDownLatch stopReturned = new CountDownLatch(1);
    Thread stopper = new Thread(() -> {
      Thread.currentThread().interrupt(); // enter stop() already interrupted, with an idle dispatcher
      producerSpy.stop();
      interruptSetOnReturn.set(Thread.currentThread().isInterrupted());
      stopReturned.countDown();
    });
    stopper.start();

    assertTrue(stopReturned.await(10, TimeUnit.SECONDS), "idle stop() never returned");
    stopper.join();
    verify(mockWriter).close();
    assertTrue(interruptSetOnReturn.get(), "idle stop() dropped the entry interrupt instead of restoring it");
  }

  private ControllerClient buildMockControllerClient(int valueSchemaId, int derivedSchemaId) {
    return buildMockControllerClient(valueSchemaId, derivedSchemaId, false, "test_store_rt");
  }

  private ControllerClient buildMockControllerClient(
      int valueSchemaId,
      int derivedSchemaId,
      boolean writeComputationEnabled,
      String kafkaTopic) {
    ControllerClient mockControllerClient = mock(ControllerClient.class);

    SchemaResponse keySchemaResponse = new SchemaResponse();
    keySchemaResponse.setSchemaStr("\"string\"");
    when(mockControllerClient.getKeySchema(anyString())).thenReturn(keySchemaResponse);

    VersionCreationResponse vcr = new VersionCreationResponse();
    vcr.setKafkaTopic(kafkaTopic);
    vcr.setKafkaBootstrapServers("kafka:9092");
    when(
        mockControllerClient.requestTopicForWrites(
            anyString(),
            anyLong(),
            any(),
            anyString(),
            anyBoolean(),
            anyBoolean(),
            anyBoolean(),
            any(),
            any(),
            any(),
            anyBoolean(),
            anyLong())).thenReturn(vcr);

    StoreResponse storeResponse = new StoreResponse();
    StoreInfo storeInfo = new StoreInfo();
    storeInfo.setVersions(new ArrayList<>());
    storeInfo.setWriteComputationEnabled(writeComputationEnabled);
    storeResponse.setStore(storeInfo);
    when(mockControllerClient.getStore(anyString())).thenReturn(storeResponse);

    SchemaResponse valueSchemaResponse = new SchemaResponse();
    valueSchemaResponse.setId(valueSchemaId);
    valueSchemaResponse.setDerivedSchemaId(derivedSchemaId);
    when(mockControllerClient.getValueOrDerivedSchemaId(anyString(), anyString())).thenReturn(valueSchemaResponse);

    return mockControllerClient;
  }

  @Test
  public void testSendCallsWriterPut() {
    VeniceWriter<byte[], byte[], byte[]> mockWriter = mock(VeniceWriter.class);
    ControllerClient mockControllerClient = buildMockControllerClient(1, -1);
    VeniceSystemProducer producerSpy = buildStartedProducerSpy(mockControllerClient, mockWriter);

    awaitSubmitted(producerSpy.send("myKey", "myValue"));

    verify(mockWriter).put(any(), any(), eq(1), anyLong(), any());
    producerSpy.stop();
  }

  @Test
  public void testSendCallsWriterDelete() {
    VeniceWriter<byte[], byte[], byte[]> mockWriter = mock(VeniceWriter.class);
    ControllerClient mockControllerClient = buildMockControllerClient(1, -1);
    VeniceSystemProducer producerSpy = buildStartedProducerSpy(mockControllerClient, mockWriter);

    awaitSubmitted(producerSpy.send((Object) "myKey", null));

    verify(mockWriter).delete(any(), anyLong(), any());
    producerSpy.stop();
  }

  @Test
  public void testSendCallsWriterUpdate() {
    VeniceWriter<byte[], byte[], byte[]> mockWriter = mock(VeniceWriter.class);
    ControllerClient mockControllerClient = buildMockControllerClient(1, 1, true, "test_store_rt");
    VeniceSystemProducer producerSpy = buildStartedProducerSpy(mockControllerClient, mockWriter);

    awaitSubmitted(producerSpy.send("myKey", "myValue"));

    verify(mockWriter).update(any(), any(), eq(1), eq(1), anyLong(), any());
    producerSpy.stop();
  }

  @Test
  public void testBuilderSucceedsWithDiscoveryUrl() {
    VeniceSystemProducer producer = new VeniceSystemProducer(
        new VeniceSystemProducerConfig.Builder().setStoreName("store")
            .setPushType(Version.PushType.STREAM)
            .setSamzaJobId("job-id")
            .setRunningFabric("dc-0")
            .setFactory(mock(VeniceSystemFactory.class))
            .setDiscoveryUrl("http://discovery")
            .build());
    assertNotNull(producer);
  }

  @Test
  public void testBuilderSucceedsWithZkHosts() {
    VeniceSystemProducer producer = new VeniceSystemProducer(
        new VeniceSystemProducerConfig.Builder().setStoreName("store")
            .setPushType(Version.PushType.STREAM)
            .setSamzaJobId("job-id")
            .setRunningFabric("dc-0")
            .setFactory(mock(VeniceSystemFactory.class))
            .setVeniceChildD2ZkHost("zk:2181")
            .setPrimaryControllerColoD2ZKHost("zk:2181")
            .setPrimaryControllerD2ServiceName("ChildController")
            .build());
    assertNotNull(producer);
  }

  @Test
  public void testBuilderSucceedsWithD2Clients() {
    VeniceSystemProducer producer = new VeniceSystemProducer(
        new VeniceSystemProducerConfig.Builder().setStoreName("store")
            .setPushType(Version.PushType.STREAM)
            .setSamzaJobId("job-id")
            .setRunningFabric("dc-0")
            .setFactory(mock(VeniceSystemFactory.class))
            .setProvidedChildColoD2Client(mock(D2Client.class))
            .setProvidedPrimaryControllerColoD2Client(mock(D2Client.class))
            .setPrimaryControllerD2ServiceName("ChildController")
            .build());
    assertNotNull(producer);
  }
}
