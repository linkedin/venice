package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.serializer.RecordDeserializer;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.utils.StoreClientTestUtils;
import com.linkedin.venice.client.store.schemas.TestKeyRecord;
import com.linkedin.venice.client.store.schemas.TestValueRecord;
import com.linkedin.venice.client.store.schemas.TestValueRecordWithMoreFields;

import static org.mockito.Mockito.*;

import com.linkedin.venice.tehuti.MockTehutiReporter;
import io.tehuti.metrics.MetricsRepository;
import org.apache.avro.generic.GenericData;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TestAvroStoreClient {
  private static final String STORE_NAME = "test-store";
  private static final String KEY_SCHEMA_STR = TestKeyRecord.SCHEMA$.toString();

  private TransportClient mockTransportClient;
  private MetricsRepository mockMetricsRepository;

  private AvroGenericStoreClientImpl genericStoreClient;

  @BeforeClass
  public void setUp() throws VeniceClientException, IOException,
  ExecutionException, InterruptedException{
    mockTransportClient = mock(TransportClient.class);
    doReturn(mockTransportClient).when(mockTransportClient).getCopyIfNotUsableInCallback();

    mockMetricsRepository = new MetricsRepository();
    MockTehutiReporter mockTehutiReporter = new MockTehutiReporter();
    mockMetricsRepository.addReporter(mockTehutiReporter);

    byte[] schemaResponseInBytes =
        StoreClientTestUtils.constructSchemaResponseInBytes(STORE_NAME, 1, KEY_SCHEMA_STR);
    Future<byte[]> mockFuture = mock(Future.class);
    doReturn(schemaResponseInBytes).when(mockFuture).get();
    doReturn(mockFuture).when(mockTransportClient).getRaw(SchemaReader.TYPE_KEY_SCHEMA + "/" + STORE_NAME);

    genericStoreClient = new AvroGenericStoreClientImpl(mockTransportClient, STORE_NAME, mockMetricsRepository);
  }

  @AfterMethod
  public void cleanUp() {
    genericStoreClient.close();
  }

  @Test
  public void testStartClient() throws VeniceClientException {
    genericStoreClient.start();
    verify(mockTransportClient).getRaw(SchemaReader.TYPE_KEY_SCHEMA + "/" + STORE_NAME);
  }

  @Test(dependsOnMethods = { "testStartClient" })
  public void TestGet() {
    genericStoreClient.start();

    TestKeyRecord testKey;
    testKey = new TestKeyRecord();
    testKey.long_field = 0l;
    testKey.string_field = "";

    String b64key = Base64.getUrlEncoder()
        .encodeToString(StoreClientTestUtils.serializeRecord(testKey, TestKeyRecord.SCHEMA$));
    doReturn(null).when(mockTransportClient)
        .get(eq(AbstractAvroStoreClient.STORAGE_TYPE + "/" + STORE_NAME + "/" +
            b64key + AbstractAvroStoreClient.B64_FORMAT), any());

    genericStoreClient.get(testKey);
    verify(mockTransportClient).get(any(), any());
  }

  @Test(dependsOnMethods = { "testStartClient" })
  public void testFetchRecordDeserializer() throws IOException, ExecutionException, InterruptedException {
    Map schemas = new HashMap<>();
    schemas.put(1, TestValueRecord.SCHEMA$.toString());
    schemas.put(2, TestValueRecordWithMoreFields.SCHEMA$.toString());
    byte[] multiSchemasInBytes =
        StoreClientTestUtils.constructMultiSchemaResponseInBytes(STORE_NAME, schemas);
    Future<byte[]> mockFuture = mock(Future.class);
    doReturn(multiSchemasInBytes).when(mockFuture).get();
    doReturn(mockFuture).when(mockTransportClient).getRaw(SchemaReader.TYPE_VALUE_SCHEMA + "/" + STORE_NAME);

    genericStoreClient.start();
    RecordDeserializer genericRecordDeserializer =  genericStoreClient.fetch(1);

    byte[] schemaResponseInBytes =
        StoreClientTestUtils.constructSchemaResponseInBytes(STORE_NAME, 1, TestValueRecord.SCHEMA$.toString());
    mockFuture = mock(Future.class);
    doReturn(schemaResponseInBytes).when(mockFuture).get();
    doReturn(mockFuture).when(mockTransportClient)
        .getRaw(SchemaReader.TYPE_VALUE_SCHEMA + "/" + STORE_NAME + "/" + "1");
    AvroSpecificStoreClientImpl specificStoreClient =
        new AvroSpecificStoreClientImpl(mockTransportClient, STORE_NAME, TestValueRecord.class, mockMetricsRepository);

    specificStoreClient.start();
    RecordDeserializer specificRecordDeserializer = specificStoreClient.fetch(1);

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
}
