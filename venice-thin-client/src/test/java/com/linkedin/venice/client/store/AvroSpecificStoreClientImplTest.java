package com.linkedin.venice.client.store;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.store.schemas.TestKeyRecord;
import com.linkedin.venice.client.store.schemas.TestValueRecord;
import com.linkedin.venice.client.store.schemas.TestValueRecordWithMoreFields;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.HttpTransportClient;
import com.linkedin.venice.client.utils.StoreClientTestUtils;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.MockD2ServerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import io.netty.handler.codec.http.FullHttpResponse;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class AvroSpecificStoreClientImplTest {
  private Logger logger = Logger.getLogger(AvroSpecificStoreClientImplTest.class);
  private MockD2ServerWrapper routerServer;
  private String routerHost;
  private int port;

  private String storeName = "test_store";
  private String defaultKeySchemaStr = TestKeyRecord.SCHEMA$.toString();
  private D2Client d2Client;

  private Map<String, AvroSpecificStoreClient<TestKeyRecord, TestValueRecord>> storeClients = new HashMap<>();
  private AbstractAvroStoreClient<TestKeyRecord, TestValueRecord> someStoreClient;

  @BeforeTest
  public void setUp() throws Exception {
    routerServer = ServiceFactory.getMockD2Server("Mock-router-server");
    routerHost = routerServer.getHost();
    port = routerServer.getPort();
  }

  @AfterTest
  public void cleanUp() throws Exception {
    routerServer.close();
  }

  @BeforeMethod
  public void setupStoreClient() throws VeniceClientException, IOException {
    routerServer.clearResponseMapping();
    // Push key schema
    FullHttpResponse schemaResponse = StoreClientTestUtils.constructHttpSchemaResponse(storeName, 1, defaultKeySchemaStr);
    String keySchemaPath = "/" + SchemaReader.TYPE_KEY_SCHEMA + "/" + storeName;
    routerServer.addResponseForUri(keySchemaPath, schemaResponse);

    // http based client
    String routerUrl = "http://" + routerHost + ":" + port + "/";
    AvroSpecificStoreClient<TestKeyRecord, TestValueRecord> httpStoreClient = AvroStoreClientFactory.getAndStartAvroSpecificStoreClient(
      routerUrl, storeName, TestValueRecord.class);
    storeClients.put(HttpTransportClient.class.getSimpleName(),httpStoreClient);
    // d2 based client
    d2Client = D2TestUtils.getAndStartD2Client(routerServer.getZkAddress());
    AvroSpecificStoreClient<TestKeyRecord, TestValueRecord> d2StoreClient = AvroStoreClientFactory.getAndStartAvroSpecificStoreClient(
      D2TestUtils.DEFAULT_TEST_SERVICE_NAME, d2Client, storeName, TestValueRecord.class);
    storeClients.put(D2TransportClient.class.getSimpleName(),d2StoreClient);

    DelegatingStoreClient<TestKeyRecord, TestValueRecord> delegatingStoreClient =
        (DelegatingStoreClient<TestKeyRecord, TestValueRecord>)httpStoreClient;
    someStoreClient = (AbstractAvroStoreClient<TestKeyRecord, TestValueRecord>)delegatingStoreClient.getInnerStoreClient();
  }

  @AfterMethod
  public void closeStoreClient() {
    for (AvroSpecificStoreClient<TestKeyRecord, TestValueRecord> storeClient : storeClients.values()) {
      if (null != storeClient) {
        storeClient.close();
      }
    }
    storeClients.clear();
  }

  @Test
  public void getByStoreKeyTest() throws IOException, VeniceClientException, ExecutionException, InterruptedException {
    int valueSchemaId = 1;
    String valueSchemaStr = TestValueRecord.SCHEMA$.toString();
    Map<Integer, String> valueSchemaEntries = new HashMap<>();
    valueSchemaEntries.put(valueSchemaId, valueSchemaStr);

    // Push value schema
    FullHttpResponse valueSchemaResponse = StoreClientTestUtils.constructHttpSchemaResponse(storeName, valueSchemaId, valueSchemaStr);
    String valueSchemaPath = "/" + SchemaReader.TYPE_VALUE_SCHEMA + "/" + storeName + "/" + valueSchemaId;
    routerServer.addResponseForUri(valueSchemaPath, valueSchemaResponse);

    FullHttpResponse multiValueSchemaResponse = StoreClientTestUtils.constructHttpMultiSchemaResponse(storeName, valueSchemaEntries);
    String multiValueSchemaPath = "/" + SchemaReader.TYPE_VALUE_SCHEMA + "/" + storeName;
    routerServer.addResponseForUri(multiValueSchemaPath, multiValueSchemaResponse);

    // Push store record
    TestKeyRecord testKey = new TestKeyRecord();
    testKey.long_field = 100;
    testKey.string_field = "test_key";

    String storeRequestPath = "/" + someStoreClient.getRequestPathByKey(testKey);
    TestValueRecord testValue = new TestValueRecord();
    testValue.long_field = 1000;
    testValue.string_field = "test_value";
    byte[] valueByteArray = StoreClientTestUtils.serializeRecord(testValue, testValue.getSchema());
    FullHttpResponse valueResponse = StoreClientTestUtils.constructStoreResponse(valueSchemaId, valueByteArray);
    routerServer.addResponseForUri(storeRequestPath, valueResponse);

    for (Map.Entry<String, AvroSpecificStoreClient<TestKeyRecord, TestValueRecord>> entry : storeClients.entrySet()) {
      logger.info("Execute test for transport client: " + entry.getKey());
      TestValueRecord actual = entry.getValue().get(testKey).get();
      Assert.assertEquals(actual.long_field, testValue.long_field);
      Assert.assertEquals(actual.string_field.toString(), testValue.string_field);
    }
  }

  @Test
  public void getByStoreKeyTestWithDifferentSchema() throws IOException, VeniceClientException, ExecutionException, InterruptedException {
    int valueSchemaId1 = 1;
    String valueSchemaStr1 = TestValueRecord.SCHEMA$.toString();
    int valueSchemaId2 = 2;
    String valueSchemaStr2 = TestValueRecordWithMoreFields.SCHEMA$.toString();
    Map<Integer, String> valueSchemaEntries = new HashMap<>();
    valueSchemaEntries.put(valueSchemaId1, valueSchemaStr1);
    valueSchemaEntries.put(valueSchemaId2, valueSchemaStr2);

    // Push value schema
    FullHttpResponse valueSchemaResponse1 = StoreClientTestUtils.constructHttpSchemaResponse(storeName, valueSchemaId1, valueSchemaStr1);
    String valueSchemaPath1 = "/" + SchemaReader.TYPE_VALUE_SCHEMA + "/" + storeName + "/" + valueSchemaId1;
    routerServer.addResponseForUri(valueSchemaPath1, valueSchemaResponse1);
    FullHttpResponse valueSchemaResponse2 = StoreClientTestUtils.constructHttpSchemaResponse(storeName, valueSchemaId2, valueSchemaStr2);
    String valueSchemaPath2 = "/" + SchemaReader.TYPE_VALUE_SCHEMA + "/" + storeName + "/" + valueSchemaId2;
    routerServer.addResponseForUri(valueSchemaPath2, valueSchemaResponse2);

    FullHttpResponse multiValueSchemaResponse = StoreClientTestUtils.constructHttpMultiSchemaResponse(storeName, valueSchemaEntries);
    String multiValueSchemaPath = "/" + SchemaReader.TYPE_VALUE_SCHEMA + "/" + storeName;
    routerServer.addResponseForUri(multiValueSchemaPath, multiValueSchemaResponse);

    // Push store record
    TestKeyRecord testKey = new TestKeyRecord();
    testKey.long_field = 100;
    testKey.string_field = "test_key";
    String storeRequestPath = "/" + someStoreClient.getRequestPathByKey(testKey);
    TestValueRecordWithMoreFields testValue = new TestValueRecordWithMoreFields();
    testValue.long_field = 1000;
    testValue.string_field = "test_value";
    testValue.int_field = 10;
    byte[] valueByteArray = StoreClientTestUtils.serializeRecord(testValue, testValue.getSchema());
    FullHttpResponse valueResponse = StoreClientTestUtils.constructStoreResponse(valueSchemaId2, valueByteArray);
    routerServer.addResponseForUri(storeRequestPath, valueResponse);

    for (Map.Entry<String, AvroSpecificStoreClient<TestKeyRecord, TestValueRecord>> entry : storeClients.entrySet()) {
      logger.info("Execute test for transport client: " + entry.getKey());
      TestValueRecord actual = entry.getValue().get(testKey).get();
      Assert.assertEquals(actual.long_field, testValue.long_field);
      Assert.assertEquals(actual.string_field.toString(), testValue.string_field);
    }
  }
}
