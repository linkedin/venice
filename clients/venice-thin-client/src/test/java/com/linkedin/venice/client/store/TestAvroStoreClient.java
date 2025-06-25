package com.linkedin.venice.client.store;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.RouterBackedSchemaReader;
import com.linkedin.venice.client.store.schemas.TestKeyRecord;
import com.linkedin.venice.client.store.schemas.TestValueRecord;
import com.linkedin.venice.client.store.schemas.TestValueRecordWithMoreFields;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.client.utils.StoreClientTestUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Test
public class TestAvroStoreClient {
  private static final String STORE_NAME = "test-store";
  private static final String KEY_SCHEMA_STR = TestKeyRecord.SCHEMA$.toString();

  private TransportClient mockTransportClient;

  private AvroGenericStoreClientImpl genericStoreClient;

  @BeforeClass
  public void setUp() throws VeniceClientException, IOException {
    mockTransportClient = mock(TransportClient.class);
    doReturn(mockTransportClient).when(mockTransportClient).getCopyIfNotUsableInCallback();

    byte[] schemaResponseInBytes = StoreClientTestUtils.constructSchemaResponseInBytes(STORE_NAME, 1, KEY_SCHEMA_STR);
    StoreClientTestUtils.setupSchemaResponse(
        mockTransportClient,
        schemaResponseInBytes,
        RouterBackedSchemaReader.TYPE_KEY_SCHEMA + "/" + STORE_NAME);
    genericStoreClient =
        new AvroGenericStoreClientImpl(mockTransportClient, ClientConfig.defaultGenericClientConfig(STORE_NAME));
  }

  @BeforeMethod
  public void setUpMetricsRepo() {

  }

  @AfterMethod
  public void cleanUp() {
    genericStoreClient.close();
  }

  @Test
  public void testGet() {
    genericStoreClient.start();

    TestKeyRecord testKey;
    testKey = new TestKeyRecord();
    testKey.long_field = 0l;
    testKey.string_field = "";

    String b64key =
        Base64.getUrlEncoder().encodeToString(StoreClientTestUtils.serializeRecord(testKey, TestKeyRecord.SCHEMA$));
    CompletableFuture<TransportClientResponse> transportFuture = new CompletableFuture();
    transportFuture.complete(new TransportClientResponse(-1, CompressionStrategy.NO_OP, null));
    doReturn(transportFuture).when(mockTransportClient)
        .get(
            eq(
                AbstractAvroStoreClient.TYPE_STORAGE + "/" + STORE_NAME + "/" + b64key
                    + AbstractAvroStoreClient.B64_FORMAT),
            any());

    genericStoreClient.get(testKey);
    // schema queries + key lookup
    verify(mockTransportClient, atLeast(2)).get(any());
  }

  @Test
  public void testFetchRecordDeserializer() throws IOException {
    // Setup multi-schema response to simulate a field deletion of the int_field
    Map<Integer, Schema> schemas = new HashMap<>();
    schemas.put(1, TestValueRecordWithMoreFields.SCHEMA$);
    schemas.put(2, TestValueRecord.SCHEMA$);

    StoreClientTestUtils.setupMultiValueSchemaResponse(mockTransportClient, STORE_NAME, schemas);

    genericStoreClient.start();

    AvroSpecificStoreClientImpl specificStoreClient = new AvroSpecificStoreClientImpl(
        mockTransportClient,
        ClientConfig.defaultSpecificClientConfig(STORE_NAME, TestValueRecordWithMoreFields.class));

    specificStoreClient.start();
    RecordDeserializer specificRecordDeserializer = specificStoreClient.getDataRecordDeserializer(1);

    TestValueRecordWithMoreFields testValue;
    testValue = new TestValueRecordWithMoreFields();
    testValue.long_field = 0L;
    testValue.string_field = "";
    testValue.int_field = 5;

    byte[] testValueInBytes = StoreClientTestUtils.serializeRecord(testValue, TestValueRecordWithMoreFields.SCHEMA$);

    // Test deserialization
    // Generic record deserialization
    Assert.assertEquals(genericStoreClient.getSchemaReader().getLatestValueSchemaId().intValue(), 2);
    RecordDeserializer genericRecordDeserializer = genericStoreClient.getDataRecordDeserializer(1);
    Object genericTestValue = genericRecordDeserializer.deserialize(testValueInBytes);
    Assert.assertTrue(genericTestValue instanceof GenericData.Record);
    Assert.assertEquals(
        ((GenericData.Record) genericTestValue).get("int_field"),
        5,
        "we are still suppose to get the value for the deleted field since it was written with schema id 1");

    // Specific record deserialization
    Assert
        .assertTrue(specificRecordDeserializer.deserialize(testValueInBytes) instanceof TestValueRecordWithMoreFields);

    specificStoreClient.close();
  }

  @Test
  public void testDeserializeWriterSchemaMissingReaderNamespace() throws IOException {
    Schema schemaWithoutNamespace = Utils.getSchemaFromResource("testSchemaWithoutNamespace.avsc");

    Map<Integer, Schema> valueSchemas = new HashMap<>();
    valueSchemas.put(1, schemaWithoutNamespace);
    StoreClientTestUtils.setupMultiValueSchemaResponse(mockTransportClient, STORE_NAME, valueSchemas);

    AvroSpecificStoreClientImpl specificStoreClient = new AvroSpecificStoreClientImpl(
        mockTransportClient,
        ClientConfig.defaultSpecificClientConfig(STORE_NAME, NamespaceTest.class));
    specificStoreClient.start();

    RecordDeserializer<NamespaceTest> deserializer = specificStoreClient.getDataRecordDeserializer(1);
    SpecificData.Record record = new SpecificData.Record(schemaWithoutNamespace);
    record.put("foo", AvroCompatibilityHelper.newEnumSymbol(schemaWithoutNamespace.getField("foo").schema(), "B"));
    String testString = "test";
    record.put("boo", testString);
    byte[] bytes = StoreClientTestUtils.serializeRecord(record, schemaWithoutNamespace);
    NamespaceTest result = deserializer.deserialize(bytes);
    Assert.assertEquals(
        result.getFoo(),
        EnumType.B,
        "Deserialized object field value should match with the value that was originally set");
    Assert.assertEquals(
        result.getBoo().toString(),
        testString,
        "Deserialized object field value should match with the value that was originally set");
  }
}
