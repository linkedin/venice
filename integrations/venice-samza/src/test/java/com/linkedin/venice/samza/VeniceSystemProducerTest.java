package com.linkedin.venice.samza;

import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.VALIDATE_VENICE_INTERNAL_SCHEMA_VERSION;
import static com.linkedin.venice.ConfigKeys.VENICE_PARTITIONERS;
import static com.linkedin.venice.VeniceConstants.SYSTEM_PROPERTY_FOR_APP_RUNNING_REGION;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_BUFFER_MEMORY;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
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
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.RouterBasedPushMonitor;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterHook;
import com.linkedin.venice.writer.VeniceWriterOptions;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.samza.config.Config;
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
            .setVerifyLatestProtocolPresent(true)
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
            .setVerifyLatestProtocolPresent(true)
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
            .setVerifyLatestProtocolPresent(true)
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
  public void testWriterHookCalledWhenProducingViaSystemProducer() {
    VeniceWriterHook mockHook = mock(VeniceWriterHook.class);

    // Build a real VeniceWriter with the hook and a mock PubSub producer
    PubSubProducerAdapter mockPubSubProducer = mock(PubSubProducerAdapter.class);
    java.util.concurrent.CompletableFuture mockFuture = mock(java.util.concurrent.CompletableFuture.class);
    when(mockPubSubProducer.sendMessage(any(), any(), any(), any(), any(), any())).thenReturn(mockFuture);
    VeniceWriter<byte[], byte[], byte[]> realWriter = new VeniceWriter(
        new VeniceWriterOptions.Builder("test_store_rt").setPartitionCount(1).setWriterHook(mockHook).build(),
        VeniceProperties.empty(),
        mockPubSubProducer);

    ControllerClient mockControllerClient = buildMockControllerClient(1, -1);
    VeniceSystemProducer producerSpy = buildStartedProducerSpy(mockControllerClient, realWriter);

    producerSpy.send("myKey", "myValue");

    verify(mockHook).onBeforeProduce(any(int.class), any(int.class));
    producerSpy.stop();
  }

  @Test(dataProvider = "BatchOrStreamReprocessing")
  public void testSendThrowsExceptionForError(Version.PushType pushType) {
    VeniceSystemProducer producerInDC0 = new VeniceSystemProducer(
        new VeniceSystemProducerConfig.Builder().setStoreName("test_store")
            .setPushType(pushType)
            .setSamzaJobId("push-job-id-1")
            .setRunningFabric("dc-0")
            .setVerifyLatestProtocolPresent(true)
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

  private VeniceSystemProducer buildStartedProducerSpy(
      ControllerClient mockControllerClient,
      VeniceWriter<byte[], byte[], byte[]> mockWriter) {
    VeniceSystemProducer producer = new VeniceSystemProducer(
        new VeniceSystemProducerConfig.Builder().setStoreName("test_store")
            .setPushType(Version.PushType.STREAM)
            .setSamzaJobId("push-job-id-1")
            .setRunningFabric("dc-0")
            .setVerifyLatestProtocolPresent(true)
            .setDiscoveryUrl("discoveryUrl")
            .build());
    VeniceSystemProducer producerSpy = spy(producer);
    doNothing().when(producerSpy).setupClientsAndReInitProvider();
    doNothing().when(producerSpy).refreshSchemaCache();
    producerSpy.setControllerClient(mockControllerClient);
    doReturn(mockWriter).when(producerSpy).getVeniceWriter(any());
    producerSpy.start();
    return producerSpy;
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

    producerSpy.send("myKey", "myValue");

    verify(mockWriter).put(any(), any(), eq(1), anyLong(), any());
    producerSpy.stop();
  }

  @Test
  public void testSendCallsWriterDelete() {
    VeniceWriter<byte[], byte[], byte[]> mockWriter = mock(VeniceWriter.class);
    ControllerClient mockControllerClient = buildMockControllerClient(1, -1);
    VeniceSystemProducer producerSpy = buildStartedProducerSpy(mockControllerClient, mockWriter);

    producerSpy.send((Object) "myKey", null);

    verify(mockWriter).delete(any(), anyLong(), any());
    producerSpy.stop();
  }

  @Test
  public void testSendCallsWriterUpdate() {
    VeniceWriter<byte[], byte[], byte[]> mockWriter = mock(VeniceWriter.class);
    ControllerClient mockControllerClient = buildMockControllerClient(1, 1, true, "test_store_rt");
    VeniceSystemProducer producerSpy = buildStartedProducerSpy(mockControllerClient, mockWriter);

    producerSpy.send("myKey", "myValue");

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
            .setVerifyLatestProtocolPresent(true)
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
            .setVerifyLatestProtocolPresent(true)
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
            .setVerifyLatestProtocolPresent(true)
            .setProvidedChildColoD2Client(mock(D2Client.class))
            .setProvidedPrimaryControllerColoD2Client(mock(D2Client.class))
            .setPrimaryControllerD2ServiceName("ChildController")
            .build());
    assertNotNull(producer);
  }
}
