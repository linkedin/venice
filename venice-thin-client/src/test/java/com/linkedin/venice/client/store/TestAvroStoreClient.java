package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.utils.StoreClientTestUtils;
import com.linkedin.venice.client.store.schemas.TestKeyRecord;
import com.linkedin.venice.client.store.schemas.TestValueRecord;
import com.linkedin.venice.client.store.schemas.TestValueRecordWithMoreFields;

import static org.mockito.Mockito.*;

import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.utils.Utils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.LinkedinAvroMigrationHelper;
import org.apache.avro.specific.SpecificData;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class TestAvroStoreClient {
  private static final String STORE_NAME = "test-store";
  private static final String KEY_SCHEMA_STR = TestKeyRecord.SCHEMA$.toString();

  private TransportClient mockTransportClient;

  private AvroGenericStoreClientImpl genericStoreClient;

  @BeforeClass
  public void setUp() throws VeniceClientException, IOException,
  ExecutionException, InterruptedException{
    mockTransportClient = mock(TransportClient.class);
    doReturn(mockTransportClient).when(mockTransportClient).getCopyIfNotUsableInCallback();

    byte[] schemaResponseInBytes =
        StoreClientTestUtils.constructSchemaResponseInBytes(STORE_NAME, 1, KEY_SCHEMA_STR);
    CompletableFuture<TransportClientResponse> mockFuture = mock(CompletableFuture.class);
    doReturn(new TransportClientResponse(SchemaData.INVALID_VALUE_SCHEMA_ID, schemaResponseInBytes)).when(mockFuture).get();
    doReturn(mockFuture).when(mockTransportClient).get(SchemaReader.TYPE_KEY_SCHEMA + "/" + STORE_NAME);

    CompletableFuture<byte[]> mockValueFuture = mock(CompletableFuture.class);
    doReturn(schemaResponseInBytes).when(mockValueFuture).get();

    doReturn(mockValueFuture).when(mockFuture).handle(any());

    genericStoreClient = new AvroGenericStoreClientImpl(mockTransportClient, ClientConfig.defaultGenericClientConfig(STORE_NAME));
  }

  @BeforeMethod
  public void setUpMetricsRepo() {

  }

  @AfterMethod
  public void cleanUp() {
    genericStoreClient.close();
  }

  @Test
  public void testStartClient() throws VeniceClientException {
    genericStoreClient.start();
  }

  @Test(dependsOnMethods = { "testStartClient" })
  public void testGet() throws ExecutionException, InterruptedException {
    genericStoreClient.start();

    TestKeyRecord testKey;
    testKey = new TestKeyRecord();
    testKey.long_field = 0l;
    testKey.string_field = "";

    String b64key = Base64.getUrlEncoder()
        .encodeToString(StoreClientTestUtils.serializeRecord(testKey, TestKeyRecord.SCHEMA$));
    CompletableFuture<TransportClientResponse> mockFuture = mock(CompletableFuture.class);
    doReturn(new TransportClientResponse(-1, null)).when(mockFuture).get();
    doReturn(mockFuture).when(mockTransportClient)
        .get(eq(AbstractAvroStoreClient.TYPE_STORAGE + "/" + STORE_NAME + "/" +
            b64key + AbstractAvroStoreClient.B64_FORMAT), any());

    genericStoreClient.get(testKey);
    // schema queries + key lookup
    verify(mockTransportClient, atLeast(2)).get(any());
  }

  @Test(dependsOnMethods = { "testStartClient" })
  public void testFetchRecordDeserializer() throws IOException, ExecutionException, InterruptedException {
    Map schemas = new HashMap<>();
    schemas.put(1, TestValueRecord.SCHEMA$.toString());
    schemas.put(2, TestValueRecordWithMoreFields.SCHEMA$.toString());
    byte[] multiSchemasInBytes =
        StoreClientTestUtils.constructMultiSchemaResponseInBytes(STORE_NAME, schemas);
    CompletableFuture<TransportClientResponse> mockTransportFuture = mock(CompletableFuture.class);
    doReturn(new TransportClientResponse(-1, multiSchemasInBytes)).when(mockTransportFuture).get();
    CompletableFuture<byte[]> mockValueFuture = mock(CompletableFuture.class);
    doReturn(multiSchemasInBytes).when(mockValueFuture).get();
    doReturn(mockValueFuture).when(mockTransportFuture).handle(any());

    doReturn(mockTransportFuture).when(mockTransportClient).get(SchemaReader.TYPE_VALUE_SCHEMA + "/" + STORE_NAME);

    genericStoreClient.start();
    RecordDeserializer genericRecordDeserializer =  genericStoreClient.getDataRecordDeserializer(1);

    byte[] schemaResponseInBytes =
        StoreClientTestUtils.constructSchemaResponseInBytes(STORE_NAME, 1, TestValueRecord.SCHEMA$.toString());
    mockTransportFuture = mock(CompletableFuture.class);
    doReturn(new TransportClientResponse(-1, schemaResponseInBytes)).when(mockTransportFuture).get();
    doReturn(mockTransportFuture).when(mockTransportClient)
        .get(SchemaReader.TYPE_VALUE_SCHEMA + "/" + STORE_NAME + "/" + "1");
    mockValueFuture = mock(CompletableFuture.class);
    doReturn(schemaResponseInBytes).when(mockValueFuture).get();
    doReturn(mockValueFuture).when(mockTransportFuture).handle(any());

    AvroSpecificStoreClientImpl specificStoreClient = new AvroSpecificStoreClientImpl(mockTransportClient,
        ClientConfig.defaultSpecificClientConfig(STORE_NAME, TestValueRecord.class));

    specificStoreClient.start();
    RecordDeserializer specificRecordDeserializer = specificStoreClient.getDataRecordDeserializer(1);

    TestValueRecord testValue;
    testValue = new TestValueRecord();
    testValue.long_field = 0l;
    testValue.string_field = "";

    byte[] testValueInBytes = StoreClientTestUtils.serializeRecord(testValue, TestValueRecord.SCHEMA$);

    Object genericTestValue = genericRecordDeserializer.deserialize(testValueInBytes);
    Assert.assertTrue(genericTestValue instanceof GenericData.Record);
    //we are supposed to get the default value for the missing field
    Assert.assertEquals(((GenericData.Record) genericTestValue).get("int_field"), 10);

    Assert.assertTrue(specificRecordDeserializer.deserialize(testValueInBytes) instanceof TestValueRecord);

    specificStoreClient.close();
  }

  @Test
  public void testDeserializeWriterSchemaMissingReaderNamespace()
      throws IOException, ExecutionException, InterruptedException {
    Schema schemaWithoutNamespace = Utils.getSchemaFromResource("testSchemaWithoutNamespace.avsc");
    byte[] schemaResponseInBytes =
        StoreClientTestUtils.constructSchemaResponseInBytes(STORE_NAME, 1, schemaWithoutNamespace.toString());
    CompletableFuture<TransportClientResponse> mockTransportFuture = mock(CompletableFuture.class);
    CompletableFuture<byte[]> mockValueFuture = mock(CompletableFuture.class);
    mockTransportFuture = mock(CompletableFuture.class);
    doReturn(mockTransportFuture).when(mockTransportClient)
        .get(SchemaReader.TYPE_VALUE_SCHEMA + "/" + STORE_NAME + "/" + "1");
    mockValueFuture = mock(CompletableFuture.class);
    doReturn(schemaResponseInBytes).when(mockValueFuture).get();
    doReturn(mockValueFuture).when(mockTransportFuture).handle(any());

    AvroSpecificStoreClientImpl specificStoreClient = new AvroSpecificStoreClientImpl(mockTransportClient,
        ClientConfig.defaultSpecificClientConfig(STORE_NAME, NamespaceTest.class));
    specificStoreClient.start();

    RecordDeserializer<NamespaceTest> deserializer = specificStoreClient.getDataRecordDeserializer(1);
    SpecificData.Record record = new SpecificData.Record(schemaWithoutNamespace);
    record.put("foo", LinkedinAvroMigrationHelper.newEnumSymbol(
        schemaWithoutNamespace.getField("foo").schema(), "B"));
    String testString = "test";
    record.put("boo", testString);
    byte[] bytes = StoreClientTestUtils.serializeRecord(record, schemaWithoutNamespace);
    NamespaceTest result = deserializer.deserialize(bytes);
    Assert.assertEquals(result.getFoo(), EnumType.B,
        "Deserialized object field value should match with the value that was originally set");
    Assert.assertEquals(result.getBoo().toString(), testString,
        "Deserialized object field value should match with the value that was originally set");
  }
}
