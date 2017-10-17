package com.linkedin.venice.client.store;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.HttpTransportClient;
import com.linkedin.venice.client.store.transport.TransportClientCallback;
import com.linkedin.venice.client.utils.StoreClientTestUtils;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.MockD2ServerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import io.netty.handler.codec.http.FullHttpResponse;
import java.util.HashSet;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

public class AvroGenericStoreClientImplTest {
  private Logger logger = Logger.getLogger(AvroGenericStoreClientImplTest.class);
  private MockD2ServerWrapper routerServer;
  private String routerHost;
  private int port;

  private ObjectMapper mapper = new ObjectMapper();
  private String storeName = "test_store";
  private String defaultKeySchemaStr = "\"string\"";

  private D2Client d2Client;

  private Map<String, AvroGenericStoreClient<String, Object>> storeClients = new HashMap<>();
  private AbstractAvroStoreClient<String, Object> someStoreClient;

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
    // Push key schema: string
    FullHttpResponse schemaResponse = StoreClientTestUtils.constructHttpSchemaResponse(storeName, 1, defaultKeySchemaStr);
    String keySchemaPath = "/" + SchemaReader.TYPE_KEY_SCHEMA + "/" + storeName;
    routerServer.addResponseForUri(keySchemaPath, schemaResponse);
    String clusterDiscoveryPath = "/"+ D2ServiceDiscovery.TYPE_D2_SERVICE_DISCOVERY+"/"+storeName;

    routerServer.addResponseForUri(clusterDiscoveryPath, StoreClientTestUtils.constructHttpClusterDiscoveryResponse(storeName, "test_cluster", D2TestUtils.DEFAULT_TEST_SERVICE_NAME));
    // http based client
    String routerUrl = "http://" + routerHost + ":" + port + "/";
    AvroGenericStoreClient<String, Object> httpStoreClient =
        ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl));
    storeClients.put(HttpTransportClient.class.getSimpleName(), httpStoreClient);
    // d2 based client
    d2Client = D2TestUtils.getAndStartD2Client(routerServer.getZkAddress());
    AvroGenericStoreClient<String, Object> d2StoreClient =
        ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName)
            .setD2ServiceName(D2TestUtils.DEFAULT_TEST_SERVICE_NAME).setD2Client(d2Client));
    storeClients.put(D2TransportClient.class.getSimpleName(), d2StoreClient);
    DelegatingStoreClient<String, Object> delegatingStoreClient = (DelegatingStoreClient<String, Object>)httpStoreClient;
    someStoreClient = (AbstractAvroStoreClient<String, Object>)delegatingStoreClient.getInnerStoreClient();
  }

  @AfterMethod
  public void closeStoreClient() {
    for (AvroGenericStoreClient<String, Object> storeClient : storeClients.values()) {
      if (null != storeClient) {
        storeClient.close();
      }
    }
    storeClients.clear();
  }

  @Test
  public void getByRequestPathTest() throws VeniceClientException, ExecutionException, InterruptedException, IOException {
    String keySchemaPath = SchemaReader.TYPE_KEY_SCHEMA + "/" + storeName;
    for (Map.Entry<String, AvroGenericStoreClient<String, Object>> entry : storeClients.entrySet()) {
      logger.info("Execute test for transport client: " + entry.getKey());
      byte[] byteResponse = ((InternalAvroStoreClient<String, Object>)entry.getValue()).getRaw(keySchemaPath).get();
      SchemaResponse ret = mapper.readValue(byteResponse, SchemaResponse.class);
      Assert.assertEquals(ret.getName(), storeName);
      Assert.assertEquals(ret.getId(), 1);
      Assert.assertEquals(ret.getSchemaStr(), defaultKeySchemaStr);
    }
  }

  @Test
  public void getByRequestPathTestWithNonExistingPath() throws VeniceClientException, ExecutionException, InterruptedException, IOException {
    String nonExistingPath = "sdfwirwoer";
    for (Map.Entry<String, AvroGenericStoreClient<String, Object>> entry : storeClients.entrySet()) {
      logger.info("Execute test for transport client: " + entry.getKey());
      byte[] byteResponse = ((InternalAvroStoreClient<String, Object>)entry.getValue()).getRaw(nonExistingPath).get();
      Assert.assertNull(byteResponse);
    }
  }


  @Test
  public void getByStoreKeyTest() throws IOException, VeniceClientException, ExecutionException, InterruptedException {
    int valueSchemaId = 1;
    String valueSchemaStr = "{\n" +
        "\t\"type\": \"record\",\n" +
        "\t\"name\": \"test\",\n" +
        "\t\"fields\" : [\n" +
        "\t \t{\"name\": \"a\", \"type\": \"long\"},\n" +
        "\t\t{\"name\": \"b\", \"type\": \"string\"}\n" +
        "\t]\n" +
        "}";
    Map<Integer, String> valueSchemaEntries = new HashMap<>();
    valueSchemaEntries.put(valueSchemaId, valueSchemaStr);

    // Push value schema
    FullHttpResponse valueSchemaResponse = StoreClientTestUtils.constructHttpSchemaResponse(storeName, valueSchemaId, valueSchemaStr);
    String valueSchemaPath = "/" + SchemaReader.TYPE_VALUE_SCHEMA + "/" + storeName + "/" + valueSchemaId;
    routerServer.addResponseForUri(valueSchemaPath, valueSchemaResponse);

    FullHttpResponse multiValueSchemaResponse = StoreClientTestUtils.constructHttpMultiSchemaResponse(storeName, valueSchemaEntries);
    String multiValueSchemaPath = "/" + SchemaReader.TYPE_VALUE_SCHEMA + "/" + storeName;
    routerServer.addResponseForUri(multiValueSchemaPath, multiValueSchemaResponse);

    String key = "test_key";
    Schema valueSchema = Schema.parse(valueSchemaStr);
    GenericData.Record valueRecord = new GenericData.Record(valueSchema);
    valueRecord.put("a", 100l);
    valueRecord.put("b", "test_b_value");
    byte[] valueArray = StoreClientTestUtils.serializeRecord(valueRecord, valueSchema);
    FullHttpResponse valueResponse = StoreClientTestUtils.constructStoreResponse(valueSchemaId, valueArray);
    String storeRequestPath = "/" + someStoreClient.getRequestPathByKey(key);
    routerServer.addResponseForUri(storeRequestPath, valueResponse);

    for (Map.Entry<String, AvroGenericStoreClient<String, Object>> entry : storeClients.entrySet()) {
      logger.info("Execute test for transport client: " + entry.getKey());
      Object value = entry.getValue().get(key).get();
      Assert.assertTrue(value instanceof GenericData.Record);
      GenericData.Record recordValue = (GenericData.Record) value;
      Assert.assertEquals(recordValue.get("a"), 100l);
      Assert.assertEquals(recordValue.get("b").toString(), "test_b_value");
    }
  }

  @Test
  public void getByStoreKeyTestWithNonExistingKey() throws Throwable {
    String key = "test_key";
    for (Map.Entry<String, AvroGenericStoreClient<String, Object>> entry : storeClients.entrySet()) {
      logger.info("Execute test for transport client: " + entry.getKey());
      Object value = entry.getValue().get(key).get();
      Assert.assertNull(value);
    }
  }

  @Test
  public void getByStoreKeyTestWithNonExistingSchemaId() throws Throwable {
    String keyStr = "test_key";
    int valueSchemaId = 1;
    String valueStr = "test_value";
    String valueSchemaStr = "\"long\"";
    Map<Integer, String> valueSchemaEntries = new HashMap<>();
    valueSchemaEntries.put(valueSchemaId, valueSchemaStr);

    // Push value schema
    FullHttpResponse valueSchemaResponse = StoreClientTestUtils.constructHttpSchemaResponse(storeName, valueSchemaId, valueSchemaStr);
    String valueSchemaPath = "/" + SchemaReader.TYPE_VALUE_SCHEMA + "/" + storeName + "/" + valueSchemaId;
    routerServer.addResponseForUri(valueSchemaPath, valueSchemaResponse);
    FullHttpResponse multiValueSchemaResponse = StoreClientTestUtils.constructHttpMultiSchemaResponse(storeName, valueSchemaEntries);
    String multiValueSchemaPath = "/" + SchemaReader.TYPE_VALUE_SCHEMA + "/" + storeName;
    routerServer.addResponseForUri(multiValueSchemaPath, multiValueSchemaResponse);

    int nonExistingSchemaId = 2;
    FullHttpResponse valueResponse = StoreClientTestUtils.constructStoreResponse(nonExistingSchemaId, valueStr.getBytes());
    String storeRequestPath = "/" + someStoreClient.getRequestPathByKey(keyStr);
    routerServer.addResponseForUri(storeRequestPath, valueResponse);
    for (Map.Entry<String, AvroGenericStoreClient<String, Object>> entry : storeClients.entrySet()) {
      logger.info("Execute test for transport client: " + entry.getKey());
      try {
        entry.getValue().get(keyStr).get();
      } catch (ExecutionException e) {
        Assert.assertTrue(e.getCause() instanceof VeniceClientException);
        Assert.assertTrue(e.getCause().getMessage().contains("Failed to get value schema for store: test_store and id: 2"));
        continue;
      } catch (Throwable t) {
      }
      Assert.assertTrue(false, "There should be a VeniceClientException here");
    }
  }

  @Test
  public void getByStoreKeyTestWithNoSchemaAvailable() throws Throwable {
    String keyStr = "test_key";
    int valueSchemaId = 1;
    String valueStr = "test_value";
    String valueSchemaStr = "\"long\"";
    Map<Integer, String> valueSchemaEntries = new HashMap<>();
    valueSchemaEntries.put(valueSchemaId, valueSchemaStr);

    int nonExistingSchemaId = 2;
    FullHttpResponse valueResponse = StoreClientTestUtils.constructStoreResponse(nonExistingSchemaId, valueStr.getBytes());
    String storeRequestPath = "/" + someStoreClient.getRequestPathByKey(keyStr);
    routerServer.addResponseForUri(storeRequestPath, valueResponse);
    for (Map.Entry<String, AvroGenericStoreClient<String, Object>> entry : storeClients.entrySet()) {
      logger.info("Execute test for transport client: " + entry.getKey());
      try {
        entry.getValue().get(keyStr).get();
      } catch (ExecutionException e) {
        Assert.assertTrue(e.getCause() instanceof VeniceClientException);
        Assert.assertTrue(e.getCause().getMessage().contains("Failed to get latest value schema for store: test_store"));
        continue;
      } catch (Throwable t) {
      }
      Assert.assertTrue(false, "There should be a VeniceClientException here");
    }
  }

  @Test
  public void getByStoreKeyTestWithoutSchemaIdHeader() throws Throwable {
    String keyStr = "test_key";
    int valueSchemaId = 1;
    String valueStr = "test_value";
    String valueSchemaStr = "\"long\"";
    Map<Integer, String> valueSchemaEntries = new HashMap<>();
    valueSchemaEntries.put(valueSchemaId, valueSchemaStr);

    FullHttpResponse valueResponse = StoreClientTestUtils.constructStoreResponse(valueSchemaId, valueStr.getBytes());
    valueResponse.headers().remove(TransportClientCallback.HEADER_VENICE_SCHEMA_ID);
    String storeRequestPath = "/" + someStoreClient.getRequestPathByKey(keyStr);
    routerServer.addResponseForUri(storeRequestPath, valueResponse);
    for (Map.Entry<String, AvroGenericStoreClient<String, Object>> entry : storeClients.entrySet()) {
      logger.info("Execute test for transport client: " + entry.getKey());
      try {
        entry.getValue().get(keyStr).get();
      } catch (ExecutionException e) {
        Assert.assertTrue(e.getCause() instanceof VeniceClientException);
        Assert.assertTrue(e.getCause().getMessage().contains("No valid schema id received"));
        continue;
      } catch (Throwable t) {
      }
      Assert.assertTrue(false, "There should be a VeniceClientException here");
    }
  }

  @Test
  public void getByStoreKeyTestWithDifferentSchemaId() throws IOException, VeniceClientException, ExecutionException, InterruptedException {
    Map<Integer, String> valueSchemaEntries = new HashMap<>();
    int valueSchemaId1 = 1;
    String valueSchemaStr1 = "{\n" +
        "\t\"type\": \"record\",\n" +
        "\t\"name\": \"test\",\n" +
        "\t\"fields\" : [\n" +
        "\t \t{\"name\": \"a\", \"type\": \"long\"},\n" +
        "\t\t{\"name\": \"b\", \"type\": \"string\"}\n" +
        "\t]\n" +
        "}";
    valueSchemaEntries.put(valueSchemaId1, valueSchemaStr1);
    int valueSchemaId2 = 2;
    String valueSchemaStr2 = "{\n" +
        "\t\"type\": \"record\",\n" +
        "\t\"name\": \"test\",\n" +
        "\t\"fields\" : [\n" +
        "\t \t{\"name\": \"a\", \"type\": \"long\"},\n" +
        "\t\t{\"name\": \"b\", \"type\": \"string\"},\n" +
        "\t\t{\"name\": \"c\", \"type\": \"string\", \"default\": \"c_default_value\"}\n" +
        "\t]\n" +
        "}";
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

    String key = "test_key";
    Schema valueSchema = Schema.parse(valueSchemaStr1);
    GenericData.Record valueRecord = new GenericData.Record(valueSchema);
    valueRecord.put("a", 100l);
    valueRecord.put("b", "test_b_value");
    byte[] valueArray = StoreClientTestUtils.serializeRecord(valueRecord, valueSchema);
    FullHttpResponse valueResponse = StoreClientTestUtils.constructStoreResponse(valueSchemaId1, valueArray);
    String storeRequestPath = "/" + someStoreClient.getRequestPathByKey(key);
    routerServer.addResponseForUri(storeRequestPath, valueResponse);

    for (Map.Entry<String, AvroGenericStoreClient<String, Object>> entry : storeClients.entrySet()) {
      logger.info("Execute test for transport client: " + entry.getKey());
      Object value = entry.getValue().get(key).get();
      Assert.assertTrue(value instanceof GenericData.Record);
      GenericData.Record recordValue = (GenericData.Record) value;
      Assert.assertEquals(recordValue.get("a"), 100l);
      Assert.assertEquals(recordValue.get("b").toString(), "test_b_value");
      Assert.assertEquals(recordValue.get("c").toString(), "c_default_value");
    }
  }

  private Set setupSchemaAndRequest(int valueSchemaId, String valueSchemaStr) throws IOException {
    Map<Integer, String> valueSchemaEntries = new HashMap<>();
    valueSchemaEntries.put(valueSchemaId, valueSchemaStr);

    // Push value schema
    FullHttpResponse valueSchemaResponse = StoreClientTestUtils.constructHttpSchemaResponse(storeName, valueSchemaId, valueSchemaStr);
    String valueSchemaPath = "/" + SchemaReader.TYPE_VALUE_SCHEMA + "/" + storeName + "/" + valueSchemaId;
    routerServer.addResponseForUri(valueSchemaPath, valueSchemaResponse);
    FullHttpResponse multiValueSchemaResponse = StoreClientTestUtils.constructHttpMultiSchemaResponse(storeName, valueSchemaEntries);
    String multiValueSchemaPath = "/" + SchemaReader.TYPE_VALUE_SCHEMA + "/" + storeName;
    routerServer.addResponseForUri(multiValueSchemaPath, multiValueSchemaResponse);

    Set<String> keys = new TreeSet<>();
    keys.add("key1");
    keys.add("key0");
    keys.add("key2");
    keys.add("key4");
    keys.add("key3");

    return keys;
  }

  @Test
  public void testMultiGet() throws IOException, ExecutionException, InterruptedException {
    int valueSchemaId = 1;
    String valueSchemaStr = "\"string\"";
    Set<String> keys = setupSchemaAndRequest(valueSchemaId, valueSchemaStr);
    // Construct MultiGetResponse
    RecordSerializer<Object> keySerializer = SerializerDeserializerFactory.getAvroGenericSerializer(Schema.parse(valueSchemaStr));
    List<Object> records = new ArrayList<>();
    MultiGetResponseRecordV1 dataRecord1 = new MultiGetResponseRecordV1();
    dataRecord1.keyIndex = 1;
    dataRecord1.schemaId = valueSchemaId;
    dataRecord1.value = ByteBuffer.wrap(keySerializer.serialize("value1"));
    records.add(dataRecord1);

    MultiGetResponseRecordV1 dataRecord3 = new MultiGetResponseRecordV1();
    dataRecord3.keyIndex = 3;
    dataRecord3.schemaId = valueSchemaId;
    dataRecord3.value = ByteBuffer.wrap(keySerializer.serialize("value3"));
    records.add(dataRecord3);
    // Serialize MultiGetResponse
    RecordSerializer<Object> responseSerializer = SerializerDeserializerFactory.getAvroGenericSerializer(MultiGetResponseRecordV1.SCHEMA$);
    byte[] responseBytes = responseSerializer.serializeObjects(records);
    int responseSchemaId = 1;

    FullHttpResponse httpResponse = StoreClientTestUtils.constructStoreResponse(responseSchemaId, responseBytes);
    routerServer.addResponseForUri("/" + AbstractAvroStoreClient.TYPE_STORAGE + "/" + storeName, httpResponse);

    for (Map.Entry<String, AvroGenericStoreClient<String, Object>> entry : storeClients.entrySet()) {
      logger.info("Execute test for transport client: " + entry.getKey());
      Map<String, Object> result = entry.getValue().batchGet(keys).get();
      Assert.assertFalse(result.containsKey("key0"));
      Assert.assertFalse(result.containsKey("key2"));
      Assert.assertFalse(result.containsKey("key4"));
      Assert.assertEquals(result.get("key1").toString(), "value1");
      Assert.assertEquals(result.get("key3").toString(), "value3");
    }
  }

  @Test
  public void testMultiGetWithNonExistingDataSchemaId() throws IOException, InterruptedException {
    int valueSchemaId = 1;
    String valueSchemaStr = "\"string\"";
    Set<String> keys = setupSchemaAndRequest(valueSchemaId, valueSchemaStr);

    int nonExistingDataSchemaId = 100;
    // Construct MultiGetResponse
    RecordSerializer<Object> keySerializer = SerializerDeserializerFactory.getAvroGenericSerializer(Schema.parse(valueSchemaStr));
    List<Object> records = new ArrayList<>();
    MultiGetResponseRecordV1 dataRecord1 = new MultiGetResponseRecordV1();
    dataRecord1.keyIndex = 1;
    dataRecord1.schemaId = nonExistingDataSchemaId;
    dataRecord1.value = ByteBuffer.wrap(keySerializer.serialize("value1"));
    records.add(dataRecord1);

    // Serialize MultiGetResponse
    RecordSerializer<Object> responseSerializer = SerializerDeserializerFactory.getAvroGenericSerializer(MultiGetResponseRecordV1.SCHEMA$);
    byte[] responseBytes = responseSerializer.serializeObjects(records);
    int responseSchemaId = 1;

    FullHttpResponse httpResponse = StoreClientTestUtils.constructStoreResponse(responseSchemaId, responseBytes);
    routerServer.addResponseForUri("/" + AbstractAvroStoreClient.TYPE_STORAGE + "/" + storeName, httpResponse);

    for (Map.Entry<String, AvroGenericStoreClient<String, Object>> entry : storeClients.entrySet()) {
      logger.info("Execute test for transport client: " + entry.getKey());
      try {
        Map<String, Object> result = entry.getValue().batchGet(keys).get();
        Assert.fail("Should receive exception here because of non-existing data schema id");
      } catch (ExecutionException e) {
        // expected
      }
    }
  }

  @Test
  public void testMultiGetWithEmptyKeySet() throws IOException, ExecutionException, InterruptedException {
    int valueSchemaId = 1;
    String valueSchemaStr = "\"string\"";
    Set<String> keys = new HashSet<>();
    // Construct MultiGetResponse
    RecordSerializer<Object> keySerializer = SerializerDeserializerFactory.getAvroGenericSerializer(Schema.parse(valueSchemaStr));
    List<Object> records = new ArrayList<>();
    MultiGetResponseRecordV1 dataRecord1 = new MultiGetResponseRecordV1();
    dataRecord1.keyIndex = 1;
    dataRecord1.schemaId = valueSchemaId;
    dataRecord1.value = ByteBuffer.wrap(keySerializer.serialize("value1"));
    records.add(dataRecord1);

    // Serialize MultiGetResponse
    RecordSerializer<Object> responseSerializer = SerializerDeserializerFactory.getAvroGenericSerializer(MultiGetResponseRecordV1.SCHEMA$);
    byte[] responseBytes = responseSerializer.serializeObjects(records);
    int responseSchemaId = 1;

    FullHttpResponse httpResponse = StoreClientTestUtils.constructStoreResponse(responseSchemaId, responseBytes);
    routerServer.addResponseForUri("/" + AbstractAvroStoreClient.TYPE_STORAGE + "/" + storeName, httpResponse);

    for (Map.Entry<String, AvroGenericStoreClient<String, Object>> entry : storeClients.entrySet()) {
      logger.info("Execute test for transport client: " + entry.getKey());
      Map<String, Object> result = entry.getValue().batchGet(keys).get();
      // Batch get request with empty key set shouldn't be sent to server side
      Assert.assertTrue(result.isEmpty());
    }
  }
}
