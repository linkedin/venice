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
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.RouterBasedPushMonitor;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
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
        "zookeeper.com:2181",
        "zookeeper.com:2181",
        "ChildController",
        "test_store",
        Version.PushType.BATCH,
        "push-job-id-1",
        "dc-0",
        true,
        null,
        Optional.empty(),
        Optional.empty(),
        SystemTime.INSTANCE);

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
        "zookeeper.com:2181",
        "zookeeper.com:2181",
        "ChildController",
        "test_store",
        pushType,
        "push-job-id-1",
        "dc-0",
        true,
        null,
        Optional.empty(),
        Optional.empty(),
        SystemTime.INSTANCE);

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

  @Test(dataProvider = "BatchOrStreamReprocessing")
  public void testSendThrowsExceptionForError(Version.PushType pushType) {
    VeniceSystemProducer producerInDC0 = new VeniceSystemProducer(
        "discoveryUrl",
        "test_store",
        pushType,
        "push-job-id-1",
        "dc-0",
        true,
        null,
        Optional.empty(),
        Optional.empty(),
        SystemTime.INSTANCE);
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
  public void testVeniceSystemProducerWithMixedD2Clients() {
    // Create mock D2Client instance for primary controller only
    D2Client mockPrimaryControllerColoD2Client = mock(D2Client.class);

    // Create mock VeniceSystemFactory
    VeniceSystemFactory veniceSystemFactory = new VeniceSystemFactory();

    // Create VeniceSystemProducer with one provided D2Client and one null
    VeniceSystemProducer producer = veniceSystemFactory.createSystemProducer(
        null, // Will be created using ZK host
        mockPrimaryControllerColoD2Client,
        "primaryServiceName",
        "testStore",
        Version.PushType.STREAM,
        "testJobId",
        "testFabric",
        false,
        mock(Config.class),
        Optional.empty(),
        Optional.empty());

    try {
      producer.start();
      // Verify that the provided D2Client instance is accessible
      fail("Should throw IllegalStateException when starting with null D2Client");
    } catch (IllegalStateException e) {
      // Expected exception, as one D2Client is null
      assertTrue(
          e.getMessage().contains("Cannot create child colo D2Client: no D2Client provided and no ZK host available"));
    } finally {
      producer.stop();
    }
  }

  @Test
  public void testGetProducerWithD2ClientBranches() {
    VeniceSystemFactory factory = spy(new VeniceSystemFactory());
    Config config = mock(Config.class);
    D2Client mockChildD2Client = mock(D2Client.class);
    D2Client mockPrimaryD2Client = mock(D2Client.class);

    // Setup required configs for non-discovery URL path
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

    // Test case 1: Both D2 clients are provided (first branch)
    // Mock the createSystemProducer method to verify which overload is called
    VeniceSystemProducer mockProducer1 = mock(VeniceSystemProducer.class);

    doReturn(mockProducer1).when(factory)
        .createSystemProducer(
            eq(mockChildD2Client),
            eq(mockPrimaryD2Client),
            anyString(),
            anyString(),
            any(Version.PushType.class),
            anyString(),
            anyString(),
            anyBoolean(),
            any(Config.class),
            any(Optional.class),
            any(Optional.class));

    SystemProducer result1 =
        factory.getProducer("testSystem", "testStore", false, "STREAM", config, mockChildD2Client, mockPrimaryD2Client);

    assertNotNull(result1);
    assertEquals(result1, mockProducer1);

    // Test case 2: No D2 clients are provided (second branch)
    VeniceSystemProducer mockProducer2 = mock(VeniceSystemProducer.class);
    doReturn(mockProducer2).when(factory)
        .createSystemProducer(
            eq("child-zk:2181"),
            eq("child-zk:2181"), // primaryControllerColoD2ZKHost for non-aggregate
            eq("ChildController"),
            eq("testStore"),
            eq(Version.PushType.STREAM),
            eq("test-job-id"),
            eq("test-fabric"),
            eq(true),
            any(Optional.class),
            any(Optional.class));

    SystemProducer result2 = factory.getProducer("testSystem", "testStore", false, "STREAM", config);

    assertNotNull(result2);
    assertEquals(result2, mockProducer2);
  }

  private VeniceSystemProducer buildStartedProducerSpy(
      ControllerClient mockControllerClient,
      VeniceWriter<byte[], byte[], byte[]> mockWriter) {
    VeniceSystemProducer producer = new VeniceSystemProducer(
        "discoveryUrl",
        "test_store",
        Version.PushType.STREAM,
        "push-job-id-1",
        "dc-0",
        true,
        null,
        Optional.empty(),
        Optional.empty(),
        SystemTime.INSTANCE);
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
  public void testPrepareRecordForPut() {
    VeniceWriter<byte[], byte[], byte[]> mockWriter = mock(VeniceWriter.class);
    ControllerClient mockControllerClient = buildMockControllerClient(1, -1);
    VeniceSystemProducer producerSpy = buildStartedProducerSpy(mockControllerClient, mockWriter);

    SerializedRecord record = producerSpy.prepareRecord("myKey", "myValue");

    assertNotNull(record.getSerializedKey());
    assertTrue(record.getSerializedKey().length > 0);
    assertNotNull(record.getSerializedValue());
    assertTrue(record.getSerializedValue().length > 0);
    assertEquals(record.getValueSchemaId(), 1);
    assertEquals(record.getDerivedSchemaId(), -1);
    producerSpy.stop();
  }

  @Test
  public void testPrepareRecordForDelete() {
    VeniceWriter<byte[], byte[], byte[]> mockWriter = mock(VeniceWriter.class);
    ControllerClient mockControllerClient = buildMockControllerClient(1, -1);
    VeniceSystemProducer producerSpy = buildStartedProducerSpy(mockControllerClient, mockWriter);

    SerializedRecord record = producerSpy.prepareRecord("myKey", null);

    assertNotNull(record.getSerializedKey());
    assertTrue(record.getSerializedKey().length > 0);
    assertNull(record.getSerializedValue());
    producerSpy.stop();
  }

  @Test
  public void testPrepareRecordWithTimestamp() {
    VeniceWriter<byte[], byte[], byte[]> mockWriter = mock(VeniceWriter.class);
    ControllerClient mockControllerClient = buildMockControllerClient(1, -1);
    VeniceSystemProducer producerSpy = buildStartedProducerSpy(mockControllerClient, mockWriter);

    long expectedTimestamp = 12345L;
    SerializedRecord record =
        producerSpy.prepareRecord("myKey", new VeniceObjectWithTimestamp("myValue", expectedTimestamp));

    assertEquals(record.getLogicalTimestamp(), expectedTimestamp);
    assertNotNull(record.getSerializedValue());
    producerSpy.stop();
  }

  @Test
  public void testSendSerializedRecordCallsWriterPut() {
    VeniceWriter<byte[], byte[], byte[]> mockWriter = mock(VeniceWriter.class);
    ControllerClient mockControllerClient = buildMockControllerClient(1, -1);
    VeniceSystemProducer producerSpy = buildStartedProducerSpy(mockControllerClient, mockWriter);

    byte[] key = new byte[] { 1 };
    byte[] value = new byte[] { 2 };
    SerializedRecord record = new SerializedRecord(key, value, 1, -1, 0L);
    producerSpy.send(record);

    verify(mockWriter).put(eq(key), eq(value), eq(1), anyLong(), any());
    producerSpy.stop();
  }

  @Test
  public void testSendSerializedRecordCallsWriterDelete() {
    VeniceWriter<byte[], byte[], byte[]> mockWriter = mock(VeniceWriter.class);
    ControllerClient mockControllerClient = buildMockControllerClient(1, -1);
    VeniceSystemProducer producerSpy = buildStartedProducerSpy(mockControllerClient, mockWriter);

    byte[] key = new byte[] { 1 };
    SerializedRecord record = new SerializedRecord(key, null, -1, -1, 0L);
    producerSpy.send(record);

    verify(mockWriter).delete(eq(key), anyLong(), any());
    producerSpy.stop();
  }

  @Test
  public void testSendSerializedRecordThrowsWhenRecordIsNull() {
    VeniceWriter<byte[], byte[], byte[]> mockWriter = mock(VeniceWriter.class);
    ControllerClient mockControllerClient = buildMockControllerClient(1, -1);
    VeniceSystemProducer producerSpy = buildStartedProducerSpy(mockControllerClient, mockWriter);

    Assert.assertThrows(() -> producerSpy.send((SerializedRecord) null));
    producerSpy.stop();
  }

  @Test
  public void testSendSerializedRecordThrowsWhenKeyIsNull() {
    VeniceWriter<byte[], byte[], byte[]> mockWriter = mock(VeniceWriter.class);
    ControllerClient mockControllerClient = buildMockControllerClient(1, -1);
    VeniceSystemProducer producerSpy = buildStartedProducerSpy(mockControllerClient, mockWriter);

    SerializedRecord record = new SerializedRecord(null, new byte[] { 2 }, 1, -1, 0L);
    Assert.assertThrows(() -> producerSpy.send(record));
    producerSpy.stop();
  }

  @Test
  public void testSendSerializedRecordThrowsWhenDeleteRecordHasSchemaIds() {
    VeniceWriter<byte[], byte[], byte[]> mockWriter = mock(VeniceWriter.class);
    ControllerClient mockControllerClient = buildMockControllerClient(1, -1);
    VeniceSystemProducer producerSpy = buildStartedProducerSpy(mockControllerClient, mockWriter);

    SerializedRecord record = new SerializedRecord(new byte[] { 1 }, null, 1, -1, 0L);
    Assert.assertThrows(() -> producerSpy.send(record));
    producerSpy.stop();
  }

  @Test
  public void testSendSerializedRecordThrowsWhenNonDeleteRecordHasInvalidSchemaIds() {
    VeniceWriter<byte[], byte[], byte[]> mockWriter = mock(VeniceWriter.class);
    ControllerClient mockControllerClient = buildMockControllerClient(1, -1);
    VeniceSystemProducer producerSpy = buildStartedProducerSpy(mockControllerClient, mockWriter);

    SerializedRecord invalidValueSchemaRecord = new SerializedRecord(new byte[] { 1 }, new byte[] { 2 }, -1, -1, 0L);
    Assert.assertThrows(() -> producerSpy.send(invalidValueSchemaRecord));

    SerializedRecord invalidDerivedSchemaRecord = new SerializedRecord(new byte[] { 1 }, new byte[] { 2 }, 1, 0, 0L);
    Assert.assertThrows(() -> producerSpy.send(invalidDerivedSchemaRecord));
    producerSpy.stop();
  }

  @Test
  public void testSendSerializedRecordCallsWriterUpdateWhenWriteComputeEnabled() {
    VeniceWriter<byte[], byte[], byte[]> mockWriter = mock(VeniceWriter.class);
    ControllerClient mockControllerClient = buildMockControllerClient(1, 1, true, "test_store_rt");
    VeniceSystemProducer producerSpy = buildStartedProducerSpy(mockControllerClient, mockWriter);

    byte[] key = new byte[] { 1 };
    byte[] value = new byte[] { 2 };
    SerializedRecord record = new SerializedRecord(key, value, 1, 1, 123L);
    producerSpy.send(record);

    verify(mockWriter).update(eq(key), eq(value), eq(1), eq(1), eq(123L), any());
    producerSpy.stop();
  }

  @Test
  public void testSendSerializedRecordThrowsForWriteComputeWhenDisabled() {
    VeniceWriter<byte[], byte[], byte[]> mockWriter = mock(VeniceWriter.class);
    ControllerClient mockControllerClient = buildMockControllerClient(1, 1, false, "test_store_rt");
    VeniceSystemProducer producerSpy = buildStartedProducerSpy(mockControllerClient, mockWriter);

    byte[] key = new byte[] { 1 };
    byte[] value = new byte[] { 2 };
    SerializedRecord record = new SerializedRecord(key, value, 1, 1, 123L);

    Assert.assertThrows(() -> producerSpy.send(record));
    producerSpy.stop();
  }

  @Test
  public void testPrepareRecordConvertsWriteComputeForVersionedTopic() {
    VeniceWriter<byte[], byte[], byte[]> mockWriter = mock(VeniceWriter.class);
    ControllerClient mockControllerClient = buildMockControllerClient(1, 1, true, "test_store_v1");
    VeniceSystemProducer producerSpy = buildStartedProducerSpy(mockControllerClient, mockWriter);
    doReturn("convertedValue").when(producerSpy).convertPartialUpdateToFullPut(any(), eq("partialUpdateCandidate"));

    SerializedRecord record = producerSpy.prepareRecord("myKey", "partialUpdateCandidate");

    assertEquals(record.getValueSchemaId(), 1);
    assertEquals(record.getDerivedSchemaId(), -1);
    verify(producerSpy).convertPartialUpdateToFullPut(any(), eq("partialUpdateCandidate"));
    producerSpy.stop();
  }

  @Test
  public void testSendObjectDelegatesToPrepareRecordAndSendSerializedRecord() {
    VeniceWriter<byte[], byte[], byte[]> mockWriter = mock(VeniceWriter.class);
    ControllerClient mockControllerClient = buildMockControllerClient(1, -1);
    VeniceSystemProducer producerSpy = buildStartedProducerSpy(mockControllerClient, mockWriter);

    producerSpy.send("myKey", "myValue");

    verify(producerSpy).prepareRecord(eq("myKey"), eq("myValue"));
    verify(mockWriter).put(any(), any(), eq(1), anyLong(), any());
    producerSpy.stop();
  }
}
