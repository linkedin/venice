package com.linkedin.venice.client.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.RouterBackedSchemaReader;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.HttpTransportClient;
import com.linkedin.venice.client.utils.StoreClientTestUtils;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.MockD2ServerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.netty.handler.codec.http.FullHttpResponse;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class AvroGenericStoreClientImplTest {
  private static final Logger LOGGER = LogManager.getLogger(AvroGenericStoreClientImplTest.class);

  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
  private final String storeName = "test_store";
  private final String defaultKeySchemaStr = "\"string\"";
  private final Map<String, AvroGenericStoreClient<String, Object>> storeClients = new HashMap<>();
  private final Map<AvroGenericStoreClient, MetricsRepository> storeClientMetricsRepositories = new HashMap<>();
  private AbstractAvroStoreClient<String, Object> someStoreClient;

  private MockD2ServerWrapper routerServer;
  private String routerHost;
  private int port;
  private D2Client d2Client;
  private String d2ServiceName;

  @BeforeTest
  public void setUp() {
    d2ServiceName = Utils.getUniqueString("VeniceRouter");
    routerServer = ServiceFactory.getMockD2Server("Mock-router-server", d2ServiceName);
    routerHost = routerServer.getHost();
    port = routerServer.getPort();
  }

  @AfterTest
  public void cleanUp() {
    routerServer.close();
  }

  @BeforeMethod
  public void setupStoreClient() throws VeniceClientException, IOException {
    routerServer.clearResponseMapping();
    // Push key schema: string
    FullHttpResponse schemaResponse =
        StoreClientTestUtils.constructHttpSchemaResponse(storeName, 1, defaultKeySchemaStr);
    String keySchemaPath = "/" + RouterBackedSchemaReader.TYPE_KEY_SCHEMA + "/" + storeName;
    routerServer.addResponseForUri(keySchemaPath, schemaResponse);
    String clusterDiscoveryPath = "/" + D2ServiceDiscovery.TYPE_D2_SERVICE_DISCOVERY + "/" + storeName;

    routerServer.addResponseForUri(
        clusterDiscoveryPath,
        StoreClientTestUtils.constructHttpClusterDiscoveryResponse(storeName, "test_cluster", d2ServiceName));
    // http based client
    String routerUrl = "http://" + routerHost + ":" + port + "/";
    MetricsRepository httpClientMetricsRepository = new MetricsRepository();
    AvroGenericStoreClient<String, Object> httpStoreClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(routerUrl)
            .setMetricsRepository(httpClientMetricsRepository));
    storeClients.put(HttpTransportClient.class.getSimpleName(), httpStoreClient);
    storeClientMetricsRepositories.put(httpStoreClient, httpClientMetricsRepository);
    // d2 based client
    d2Client = D2TestUtils.getAndStartD2Client(routerServer.getZkAddress());
    MetricsRepository d2ClientMetricsRepository = new MetricsRepository();
    AvroGenericStoreClient<String, Object> d2StoreClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setD2ServiceName(d2ServiceName)
            .setD2Client(d2Client)
            .setMetricsRepository(d2ClientMetricsRepository));
    storeClients.put(D2TransportClient.class.getSimpleName(), d2StoreClient);
    storeClientMetricsRepositories.put(d2StoreClient, d2ClientMetricsRepository);
    // test store client with fast avro
    MetricsRepository d2ClientWithFastAvroMetricsRepository = new MetricsRepository();
    AvroGenericStoreClient<String, Object> d2StoreClientWithFastAvro = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setD2ServiceName(d2ServiceName)
            .setD2Client(d2Client)
            .setMetricsRepository(d2ClientWithFastAvroMetricsRepository)
            .setUseFastAvro(true));
    storeClients.put(D2TransportClient.class.getSimpleName() + "-fast_avro", d2StoreClientWithFastAvro);
    storeClientMetricsRepositories.put(d2StoreClientWithFastAvro, d2ClientWithFastAvroMetricsRepository);
    DelegatingStoreClient<String, Object> delegatingStoreClient =
        (DelegatingStoreClient<String, Object>) httpStoreClient;
    someStoreClient = (AbstractAvroStoreClient<String, Object>) delegatingStoreClient.getInnerStoreClient();
  }

  @AfterMethod
  public void closeStoreClient() {
    for (AvroGenericStoreClient<String, Object> storeClient: storeClients.values()) {
      Utils.closeQuietlyWithErrorLogged(storeClient);
    }
    storeClients.clear();
    if (d2Client != null) {
      D2ClientUtils.shutdownClient(d2Client);
    }
  }

  @Test
  public void testWarmupDuringStartPhaseForD2ClientBasedStoreClient()
      throws InterruptedException, ExecutionException, IOException {
    D2Client lateStartD2Client = D2TestUtils.getD2Client(routerServer.getZkAddress(), false);
    MetricsRepository metricsRepository = new MetricsRepository();
    try (AvroGenericStoreClient<String, Object> d2StoreClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setD2ServiceName(d2ServiceName)
            .setD2Client(lateStartD2Client)
            .setMetricsRepository(metricsRepository))) {
      // Register metrics repository per store client for further metrics verification
      storeClientMetricsRepositories.put(d2StoreClient, metricsRepository);
      // Start d2 client after starting store client
      D2TestUtils.startD2Client(lateStartD2Client);
      Map<String, AvroGenericStoreClient<String, Object>> storeClientMap = new HashMap<>();
      storeClientMap.put(D2TransportClient.class.getName(), d2StoreClient);
      getByStoreKeyTest(storeClientMap);
    } finally {
      D2ClientUtils.shutdownClient(lateStartD2Client);
    }
  }

  @Test
  public void getSchemaTest() throws Exception {
    int valueSchemaId = 1;
    String valueSchemaStr = "{\n" + "\t\"type\": \"record\",\n" + "\t\"name\": \"test\",\n" + "\t\"fields\" : [\n"
        + "\t \t{\"name\": \"a\", \"type\": \"long\"},\n" + "\t\t{\"name\": \"b\", \"type\": \"string\"}\n" + "\t]\n"
        + "}";
    Map<Integer, String> valueSchemaEntries = new HashMap<>();
    valueSchemaEntries.put(valueSchemaId, valueSchemaStr);

    // Push value schema
    FullHttpResponse valueSchemaResponse =
        StoreClientTestUtils.constructHttpSchemaResponse(storeName, valueSchemaId, valueSchemaStr);
    String valueSchemaPath = "/" + RouterBackedSchemaReader.TYPE_VALUE_SCHEMA + "/" + storeName + "/" + valueSchemaId;
    routerServer.addResponseForUri(valueSchemaPath, valueSchemaResponse);

    FullHttpResponse multiValueSchemaResponse =
        StoreClientTestUtils.constructHttpMultiSchemaResponse(storeName, valueSchemaEntries);
    String multiValueSchemaPath = "/" + RouterBackedSchemaReader.TYPE_VALUE_SCHEMA + "/" + storeName;
    routerServer.addResponseForUri(multiValueSchemaPath, multiValueSchemaResponse);
    for (Map.Entry<String, AvroGenericStoreClient<String, Object>> entry: storeClients.entrySet()) {
      LOGGER.info("Execute test for transport client: {}", entry.getKey());
      Assert.assertEquals(entry.getValue().getKeySchema(), Schema.parse(defaultKeySchemaStr));
      Assert.assertEquals(entry.getValue().getLatestValueSchema(), Schema.parse(valueSchemaStr));
    }
  }

  @Test
  public void getByRequestPathTest()
      throws VeniceClientException, ExecutionException, InterruptedException, IOException {
    String keySchemaPath = RouterBackedSchemaReader.TYPE_KEY_SCHEMA + "/" + storeName;
    for (Map.Entry<String, AvroGenericStoreClient<String, Object>> entry: storeClients.entrySet()) {
      LOGGER.info("Execute test for transport client: {}", entry.getKey());
      byte[] byteResponse = ((InternalAvroStoreClient<String, Object>) entry.getValue()).getRaw(keySchemaPath).get();
      SchemaResponse ret = OBJECT_MAPPER.readValue(byteResponse, SchemaResponse.class);
      Assert.assertEquals(ret.getName(), storeName);
      Assert.assertEquals(ret.getId(), 1);
      Assert.assertEquals(ret.getSchemaStr(), defaultKeySchemaStr);
    }
  }

  @Test
  public void getByRequestPathTestWithNonExistingPath()
      throws VeniceClientException, ExecutionException, InterruptedException, IOException {
    String nonExistingPath = "sdfwirwoer";
    for (Map.Entry<String, AvroGenericStoreClient<String, Object>> entry: storeClients.entrySet()) {
      LOGGER.info("Execute test for transport client: {}", entry.getKey());
      byte[] byteResponse = ((InternalAvroStoreClient<String, Object>) entry.getValue()).getRaw(nonExistingPath).get();
      Assert.assertNull(byteResponse);
    }
  }

  @Test
  public void getByStoreKeyTest() throws IOException, VeniceClientException, ExecutionException, InterruptedException {
    getByStoreKeyTest(storeClients);
  }

  private void getByStoreKeyTest(Map<String, AvroGenericStoreClient<String, Object>> storeClientMap)
      throws IOException, VeniceClientException, ExecutionException, InterruptedException {
    int valueSchemaId = 1;
    String valueSchemaStr = "{\n" + "\t\"type\": \"record\",\n" + "\t\"name\": \"test\",\n" + "\t\"fields\" : [\n"
        + "\t \t{\"name\": \"a\", \"type\": \"long\"},\n" + "\t\t{\"name\": \"b\", \"type\": \"string\"}\n" + "\t]\n"
        + "}";
    Map<Integer, String> valueSchemaEntries = new HashMap<>();
    valueSchemaEntries.put(valueSchemaId, valueSchemaStr);

    // Push value schema
    FullHttpResponse valueSchemaResponse =
        StoreClientTestUtils.constructHttpSchemaResponse(storeName, valueSchemaId, valueSchemaStr);
    String valueSchemaPath = "/" + RouterBackedSchemaReader.TYPE_VALUE_SCHEMA + "/" + storeName + "/" + valueSchemaId;
    routerServer.addResponseForUri(valueSchemaPath, valueSchemaResponse);

    FullHttpResponse multiValueSchemaResponse =
        StoreClientTestUtils.constructHttpMultiSchemaResponse(storeName, valueSchemaEntries);
    String multiValueSchemaPath = "/" + RouterBackedSchemaReader.TYPE_VALUE_SCHEMA + "/" + storeName;
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

    for (Map.Entry<String, AvroGenericStoreClient<String, Object>> entry: storeClientMap.entrySet()) {
      LOGGER.info("Execute test for transport client: " + entry.getKey());
      Object value = entry.getValue().get(key).get();
      Assert.assertTrue(value instanceof GenericData.Record);
      GenericData.Record recordValue = (GenericData.Record) value;
      Assert.assertEquals(recordValue.get("a"), 100l);
      Assert.assertEquals(recordValue.get("b").toString(), "test_b_value");

      testMetric(entry.getValue(), RequestType.SINGLE_GET);
    }
  }

  @Test
  public void getByStoreKeyTestWithNonExistingKey() throws Throwable {
    String key = "test_key";
    for (Map.Entry<String, AvroGenericStoreClient<String, Object>> entry: storeClients.entrySet()) {
      LOGGER.info("Execute test for transport client: {}", entry.getKey());
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
    FullHttpResponse valueSchemaResponse =
        StoreClientTestUtils.constructHttpSchemaResponse(storeName, valueSchemaId, valueSchemaStr);
    String valueSchemaPath = "/" + RouterBackedSchemaReader.TYPE_VALUE_SCHEMA + "/" + storeName + "/" + valueSchemaId;
    routerServer.addResponseForUri(valueSchemaPath, valueSchemaResponse);
    FullHttpResponse multiValueSchemaResponse =
        StoreClientTestUtils.constructHttpMultiSchemaResponse(storeName, valueSchemaEntries);
    String multiValueSchemaPath = "/" + RouterBackedSchemaReader.TYPE_VALUE_SCHEMA + "/" + storeName;
    routerServer.addResponseForUri(multiValueSchemaPath, multiValueSchemaResponse);

    int nonExistingSchemaId = 2;
    FullHttpResponse valueResponse =
        StoreClientTestUtils.constructStoreResponse(nonExistingSchemaId, valueStr.getBytes());
    String storeRequestPath = "/" + someStoreClient.getRequestPathByKey(keyStr);
    routerServer.addResponseForUri(storeRequestPath, valueResponse);
    for (Map.Entry<String, AvroGenericStoreClient<String, Object>> entry: storeClients.entrySet()) {
      LOGGER.info("Execute test for transport client: {}", entry.getKey());
      try {
        entry.getValue().get(keyStr).get();
      } catch (ExecutionException e) {
        Assert.assertTrue(e.getCause() instanceof VeniceClientException);
        Assert.assertTrue(
            e.getCause().getMessage().contains("Failed to get value schema for store: test_store and id: 2"));
        continue;
      } catch (Throwable t) {
      }
      Assert.assertTrue(false, "There should be a VeniceClientException here");
    }
  }

  @Test
  public void getByStoreKeyTestWithNoSchemaAvailable() throws Throwable {
    final int TEST_ITERATIONS = 100;

    String keyStr = "test_key";
    String valueStr = "test_value";

    int nonExistingSchemaId = 2;
    FullHttpResponse valueResponse =
        StoreClientTestUtils.constructStoreResponse(nonExistingSchemaId, valueStr.getBytes());
    String storeRequestPath = "/" + someStoreClient.getRequestPathByKey(keyStr);
    routerServer.addResponseForUri(storeRequestPath, valueResponse);
    for (int i = 0; i < TEST_ITERATIONS; i++) {
      LOGGER.info("Iteration: {}", i);
      for (Map.Entry<String, AvroGenericStoreClient<String, Object>> entry: storeClients.entrySet()) {
        LOGGER.trace("Execute test for transport client: {}", entry.getKey());
        try {
          entry.getValue().get(keyStr).get();
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          boolean causeOfCorrectType = cause instanceof VeniceClientException;
          boolean correctMessage =
              cause.getMessage().contains("Failed to get latest value schema for store: test_store");
          if (!causeOfCorrectType || !correctMessage) {
            LOGGER.error(
                "Received ExecutionException, as expected, but it doesn't have the right characteristics. Logging stacktrace. Client: {}",
                entry.getKey(),
                e);
          }
          Assert.assertTrue(
              causeOfCorrectType,
              "Expected to get a VeniceClientException but instead got a " + cause.getClass().getSimpleName());
          Assert.assertTrue(
              correctMessage,
              "Expected to get an exception message containing 'Failed to get latest value schema for store: test_store', but instead got the following message:"
                  + cause.getMessage());
          continue;
        } catch (Throwable t) {
          LOGGER.error("Received a Throwable other than an ExecutionException from {}", entry.getKey(), t);
          Assert.fail("Received a Throwable other than an ExecutionException! Type: " + t.getClass().getSimpleName());
        }
        Assert.fail(
            "There should have been a VeniceClientException by now, but did not receive any from " + entry.getKey());
      }
    }
  }

  @Test
  public void getByStoreKeyTestWithoutSchemaIdHeader() throws Throwable {
    String keyStr = "test_key";
    int valueSchemaId = 1;
    String valueStr = "test_value";

    FullHttpResponse valueResponse = StoreClientTestUtils.constructStoreResponse(valueSchemaId, valueStr.getBytes());
    valueResponse.headers().remove(HttpConstants.VENICE_SCHEMA_ID);
    String storeRequestPath = "/" + someStoreClient.getRequestPathByKey(keyStr);
    routerServer.addResponseForUri(storeRequestPath, valueResponse);
    for (Map.Entry<String, AvroGenericStoreClient<String, Object>> entry: storeClients.entrySet()) {
      LOGGER.info("Execute test for transport client: {}", entry.getKey());
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
  public void getByStoreKeyTestWithDifferentSchemaId()
      throws IOException, VeniceClientException, ExecutionException, InterruptedException {
    Map<Integer, String> valueSchemaEntries = new HashMap<>();
    int valueSchemaId1 = 1;
    String valueSchemaStr1 = "{\n" + "\t\"type\": \"record\",\n" + "\t\"name\": \"test\",\n" + "\t\"fields\" : [\n"
        + "\t \t{\"name\": \"a\", \"type\": \"long\"},\n" + "\t\t{\"name\": \"b\", \"type\": \"string\"}\n" + "\t]\n"
        + "}";
    valueSchemaEntries.put(valueSchemaId1, valueSchemaStr1);
    int valueSchemaId2 = 2;
    String valueSchemaStr2 = "{\n" + "\t\"type\": \"record\",\n" + "\t\"name\": \"test\",\n" + "\t\"fields\" : [\n"
        + "\t \t{\"name\": \"a\", \"type\": \"long\"},\n" + "\t\t{\"name\": \"b\", \"type\": \"string\"},\n"
        + "\t\t{\"name\": \"c\", \"type\": \"string\", \"default\": \"c_default_value\"}\n" + "\t]\n" + "}";
    valueSchemaEntries.put(valueSchemaId2, valueSchemaStr2);

    // Push value schema
    FullHttpResponse valueSchemaResponse1 =
        StoreClientTestUtils.constructHttpSchemaResponse(storeName, valueSchemaId1, valueSchemaStr1);
    String valueSchemaPath1 = "/" + RouterBackedSchemaReader.TYPE_VALUE_SCHEMA + "/" + storeName + "/" + valueSchemaId1;
    routerServer.addResponseForUri(valueSchemaPath1, valueSchemaResponse1);
    FullHttpResponse valueSchemaResponse2 =
        StoreClientTestUtils.constructHttpSchemaResponse(storeName, valueSchemaId2, valueSchemaStr2);
    String valueSchemaPath2 = "/" + RouterBackedSchemaReader.TYPE_VALUE_SCHEMA + "/" + storeName + "/" + valueSchemaId2;
    routerServer.addResponseForUri(valueSchemaPath2, valueSchemaResponse2);

    FullHttpResponse multiValueSchemaResponse =
        StoreClientTestUtils.constructHttpMultiSchemaResponse(storeName, valueSchemaEntries);
    String multiValueSchemaPath = "/" + RouterBackedSchemaReader.TYPE_VALUE_SCHEMA + "/" + storeName;
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

    String key2 = "test_key_2";
    Schema valueSchema2 = Schema.parse(valueSchemaStr2);
    GenericData.Record valueRecord2 = new GenericData.Record(valueSchema2);
    valueRecord2.put("a", 102l);
    valueRecord2.put("b", "test_b_value_2");
    valueRecord2.put("c", "test_c_value_2");
    byte[] valueArray2 = StoreClientTestUtils.serializeRecord(valueRecord2, valueSchema2);
    FullHttpResponse valueResponse2 = StoreClientTestUtils.constructStoreResponse(valueSchemaId2, valueArray2);
    String storeRequestPath2 = "/" + someStoreClient.getRequestPathByKey(key2);
    routerServer.addResponseForUri(storeRequestPath2, valueResponse2);

    for (Map.Entry<String, AvroGenericStoreClient<String, Object>> entry: storeClients.entrySet()) {
      LOGGER.info("Execute test for transport client: {}", entry.getKey());

      // Query value 1 while not having encountered any schema yet.
      // The current logic will always pull all the value schemas if no schema is available yet.
      Object value = entry.getValue().get(key).get();
      Assert.assertTrue(value instanceof GenericData.Record);
      GenericData.Record recordValue = (GenericData.Record) value;
      Assert.assertEquals(recordValue.get("a"), 100l);
      Assert.assertEquals(recordValue.get("b").toString(), "test_b_value");
      Assert.assertEquals(recordValue.get("c").toString(), "c_default_value");

      // Query value 2 while having already encountered schema v1 but not schema v2 yet.
      Object value2 = entry.getValue().get(key2).get();
      Assert.assertTrue(value2 instanceof GenericData.Record);
      GenericData.Record recordValue2 = (GenericData.Record) value2;
      Assert.assertEquals(recordValue2.get("a"), 102l);
      Assert.assertEquals(recordValue2.get("b").toString(), "test_b_value_2");
      Assert.assertEquals(recordValue2.get("c").toString(), "test_c_value_2");
    }
  }

  private Set setupSchemaAndRequest(int valueSchemaId, String valueSchemaStr) throws IOException {
    Map<Integer, String> valueSchemaEntries = new HashMap<>();
    valueSchemaEntries.put(valueSchemaId, valueSchemaStr);

    // Push value schema
    FullHttpResponse valueSchemaResponse =
        StoreClientTestUtils.constructHttpSchemaResponse(storeName, valueSchemaId, valueSchemaStr);
    String valueSchemaPath = "/" + RouterBackedSchemaReader.TYPE_VALUE_SCHEMA + "/" + storeName + "/" + valueSchemaId;
    routerServer.addResponseForUri(valueSchemaPath, valueSchemaResponse);
    FullHttpResponse multiValueSchemaResponse =
        StoreClientTestUtils.constructHttpMultiSchemaResponse(storeName, valueSchemaEntries);
    String multiValueSchemaPath = "/" + RouterBackedSchemaReader.TYPE_VALUE_SCHEMA + "/" + storeName;
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
    RecordSerializer<Object> keySerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(Schema.parse(valueSchemaStr));
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
    RecordSerializer<Object> responseSerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(MultiGetResponseRecordV1.SCHEMA$);
    byte[] responseBytes = responseSerializer.serializeObjects(records);
    int responseSchemaId = 1;

    FullHttpResponse httpResponse = StoreClientTestUtils.constructStoreResponse(responseSchemaId, responseBytes);
    routerServer.addResponseForUri("/" + AbstractAvroStoreClient.TYPE_STORAGE + "/" + storeName, httpResponse);

    for (Map.Entry<String, AvroGenericStoreClient<String, Object>> entry: storeClients.entrySet()) {
      LOGGER.info("Execute test for transport client: {}", entry.getKey());
      Map<String, Object> result = entry.getValue().batchGet(keys).get();
      Assert.assertFalse(result.containsKey("key0"));
      Assert.assertFalse(result.containsKey("key2"));
      Assert.assertFalse(result.containsKey("key4"));
      Assert.assertEquals(result.get("key1").toString(), "value1");
      Assert.assertEquals(result.get("key3").toString(), "value3");

      TestUtils.waitForNonDeterministicAssertion(
          3,
          TimeUnit.SECONDS,
          () -> testMetric(entry.getValue(), RequestType.MULTI_GET_STREAMING));
    }
  }

  private void testMetric(AvroGenericStoreClient client, RequestType requestType) {
    MetricsRepository repository = storeClientMetricsRepositories.get(client);
    Map<String, ? extends Metric> metrics = repository.metrics();
    String metricPrefix = "." + storeName + "--" + requestType.getMetricPrefix();
    Metric requestMetric = metrics.get(metricPrefix + "request.OccurrenceRate");
    Metric healthyRequestMetric = metrics.get(metricPrefix + "healthy_request.OccurrenceRate");
    Metric unhealthyRequestMetric = metrics.get(metricPrefix + "unhealthy_request.OccurrenceRate");
    Metric requestSerializationTimeMetric = metrics.get(metricPrefix + "request_serialization_time.Avg");
    Metric requestSubmissionToResponseHandlingTimeMetric =
        metrics.get(metricPrefix + "request_submission_to_response_handling_time.Avg");
    Metric responseDeserializationTimeMetric = metrics.get(metricPrefix + "response_deserialization_time.Avg");
    Metric requestSerializationTimeMetric99 = metrics.get(metricPrefix + "request_serialization_time.99thPercentile");
    Metric requestSubmissionToResponseHandlingTimeMetric99 =
        metrics.get(metricPrefix + "request_submission_to_response_handling_time.99thPercentile");
    Metric responseDeserializationTimeMetric99 =
        metrics.get(metricPrefix + "response_deserialization_time.99thPercentile");

    Assert.assertTrue(requestMetric.value() > 0.0);
    Assert.assertTrue(healthyRequestMetric.value() > 0.0);
    Assert.assertEquals(unhealthyRequestMetric.value(), 0.0);
    Assert.assertTrue(requestSerializationTimeMetric.value() > 0.0);
    Assert.assertTrue(requestSubmissionToResponseHandlingTimeMetric.value() > 0.0);

    /**
     * Response deserialization metric is being tracked after the result future is completed.
     * Check {@link com.linkedin.venice.client.store.deserialization.BlockingDeserializer#deserialize}
     * and other {@link com.linkedin.venice.client.store.deserialization.BatchDeserializer} implementations as well.
     * To make sure the metric is available during verification, the following wait is necessary.
     */
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      Double responseDeserializationTimeMetricValue = responseDeserializationTimeMetric.value();
      Assert.assertFalse(responseDeserializationTimeMetricValue.isNaN());
    });
    Assert.assertTrue(responseDeserializationTimeMetric.value() > 0.0);
    Assert.assertTrue(responseDeserializationTimeMetric99.value() > 0.0);

    Assert.assertTrue(requestSerializationTimeMetric99.value() > 0.0);
    Assert.assertTrue(requestSubmissionToResponseHandlingTimeMetric99.value() > 0.0);
  }

  @Test
  public void testMultiGetWithNonExistingDataSchemaId() throws IOException, InterruptedException {
    int valueSchemaId = 1;
    String valueSchemaStr = "\"string\"";
    Set<String> keys = setupSchemaAndRequest(valueSchemaId, valueSchemaStr);

    int nonExistingDataSchemaId = 100;
    // Construct MultiGetResponse
    RecordSerializer<Object> keySerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(Schema.parse(valueSchemaStr));
    List<Object> records = new ArrayList<>();
    MultiGetResponseRecordV1 dataRecord1 = new MultiGetResponseRecordV1();
    dataRecord1.keyIndex = 1;
    dataRecord1.schemaId = nonExistingDataSchemaId;
    dataRecord1.value = ByteBuffer.wrap(keySerializer.serialize("value1"));
    records.add(dataRecord1);

    // Serialize MultiGetResponse
    RecordSerializer<Object> responseSerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(MultiGetResponseRecordV1.SCHEMA$);
    byte[] responseBytes = responseSerializer.serializeObjects(records);
    int responseSchemaId = 1;

    FullHttpResponse httpResponse = StoreClientTestUtils.constructStoreResponse(responseSchemaId, responseBytes);
    routerServer.addResponseForUri("/" + AbstractAvroStoreClient.TYPE_STORAGE + "/" + storeName, httpResponse);

    for (Map.Entry<String, AvroGenericStoreClient<String, Object>> entry: storeClients.entrySet()) {
      LOGGER.info("Execute test for transport client: {}", entry.getKey());
      try {
        entry.getValue().batchGet(keys).get(10, TimeUnit.SECONDS);
        Assert.fail("Should receive exception here because of non-existing data schema id");
      } catch (ExecutionException e) {
        // expected
      } catch (TimeoutException e) {
        throw new VeniceException(e);
      }
    }
  }

  @Test
  public void testMultiGetWithEmptyKeySet() throws IOException, ExecutionException, InterruptedException {
    int valueSchemaId = 1;
    String valueSchemaStr = "\"string\"";
    Set<String> keys = new HashSet<>();
    // Construct MultiGetResponse
    RecordSerializer<Object> keySerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(Schema.parse(valueSchemaStr));
    List<Object> records = new ArrayList<>();
    MultiGetResponseRecordV1 dataRecord1 = new MultiGetResponseRecordV1();
    dataRecord1.keyIndex = 1;
    dataRecord1.schemaId = valueSchemaId;
    dataRecord1.value = ByteBuffer.wrap(keySerializer.serialize("value1"));
    records.add(dataRecord1);

    // Serialize MultiGetResponse
    RecordSerializer<Object> responseSerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(MultiGetResponseRecordV1.SCHEMA$);
    byte[] responseBytes = responseSerializer.serializeObjects(records);
    int responseSchemaId = 1;

    FullHttpResponse httpResponse = StoreClientTestUtils.constructStoreResponse(responseSchemaId, responseBytes);
    routerServer.addResponseForUri("/" + AbstractAvroStoreClient.TYPE_STORAGE + "/" + storeName, httpResponse);

    for (Map.Entry<String, AvroGenericStoreClient<String, Object>> entry: storeClients.entrySet()) {
      LOGGER.info("Execute test for transport client: {}", entry.getKey());
      Map<String, Object> result = entry.getValue().batchGet(keys).get();
      // Batch get request with empty key set shouldn't be sent to server side
      Assert.assertTrue(result.isEmpty());
    }
  }
}
