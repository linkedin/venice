package com.linkedin.venice.samza;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.samza.system.OutgoingMessageEnvelope;
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

    VeniceWriter<byte[], byte[], byte[]> resultantVeniceWriter =
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

  @DataProvider(name = "BatchOrStreamReprocessing")
  public Version.PushType[] batchOrStreamReprocessing() {
    return new Version.PushType[] { Version.PushType.BATCH, Version.PushType.STREAM_REPROCESSING,
        Version.PushType.STREAM, Version.PushType.INCREMENTAL };
  }
}
