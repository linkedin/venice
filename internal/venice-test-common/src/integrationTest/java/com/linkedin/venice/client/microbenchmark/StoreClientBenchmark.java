package com.linkedin.venice.client.microbenchmark;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.RouterBackedSchemaReader;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.D2ServiceDiscovery;
import com.linkedin.venice.client.utils.StoreClientTestUtils;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.MockD2ServerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.Utils;
import io.netty.handler.codec.http.FullHttpResponse;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class StoreClientBenchmark {
  private MockD2ServerWrapper routerServer;

  private static String storeName = "test_store";
  private static String defaultKeySchemaStr = "\"string\"";
  private static String valueSchemaStr = "{\n" + "    \"fields\": [\n" + "        {\n"
      + "            \"name\": \"floatField1\",\n" + "            \"type\": \"float\"\n" + "        },\n"
      + "        {\n" + "            \"name\": \"intField1\",\n" + "            \"type\": \"int\"\n" + "        },\n"
      + "        {\n" + "            \"name\": \"intField2\",\n" + "            \"type\": \"int\"\n" + "        },\n"
      + "        {\n" + "            \"name\": \"intField3\",\n" + "            \"type\": \"int\"\n" + "        },\n"
      + "        {\n" + "            \"name\": \"stringField1\",\n" + "            \"type\": \"string\"\n"
      + "        },\n" + "        {\n" + "            \"name\": \"floatField2\",\n"
      + "            \"type\": \"float\"\n" + "        },\n" + "        {\n"
      + "            \"name\": \"floatField3\",\n" + "            \"type\": \"float\"\n" + "        }\n" + "    ],\n"
      + "    \"name\": \"TestValueRecord\",\n" + "    \"namespace\": \"com.linkedin.venice.schemas\",\n"
      + "    \"type\": \"record\"\n" + "}";

  private static final int KEY_CNT = 2000;

  private D2Client d2Client;
  private AvroGenericStoreClient<String, Object> d2StoreClient;
  private String d2ServiceName;

  @BeforeTest
  public void setUp() throws Exception {
    d2ServiceName = Utils.getUniqueString("VeniceRouter");
    routerServer = ServiceFactory.getMockD2Server("Mock-router-server", d2ServiceName);
    setupStoreClient();
  }

  @AfterTest
  public void cleanUp() throws Exception {
    closeStoreClient();
    routerServer.close();
  }

  public byte[] getBatchGetResponse(int recordCnt) {
    Schema valueSchema = Schema.parse(valueSchemaStr);
    List<MultiGetResponseRecordV1> records = new ArrayList<>();
    RecordSerializer<GenericData.Record> recordSerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(valueSchema);
    for (int i = 0; i < recordCnt; ++i) {
      GenericData.Record record = new GenericData.Record(valueSchema);
      record.put("floatField1", 1000.0f);
      record.put("intField1", 1000);
      record.put("intField2", 1000);
      record.put("intField3", 1000);
      record.put("stringField1", "test visit model");
      record.put("floatField2", 1000.0f);
      record.put("floatField3", 1000.0f);
      MultiGetResponseRecordV1 recordV1 = new MultiGetResponseRecordV1();
      recordV1.schemaId = 1;
      recordV1.keyIndex = i;
      recordV1.value = ByteBuffer.wrap(recordSerializer.serialize(record));
      records.add(recordV1);
    }
    RecordSerializer<MultiGetResponseRecordV1> serializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(MultiGetResponseRecordV1.SCHEMA$);
    return serializer.serializeObjects(records);
  }

  public Set<String> getBatchGetKeys(int keyCnt) {
    Set<String> keySet = new HashSet<>();
    for (int i = 0; i < keyCnt; ++i) {
      keySet.add("test_key_" + i);
    }
    return keySet;
  }

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

    // Push value schemas
    int valueSchemaId = 1;
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

    // Push data
    String batchGetPath = "/storage/" + storeName;
    FullHttpResponse batchGetResponse = StoreClientTestUtils.constructStoreResponse(1, getBatchGetResponse(KEY_CNT));
    routerServer.addResponseForUri(batchGetPath, batchGetResponse);

    // d2 based client
    d2Client = D2TestUtils.getAndStartD2Client(routerServer.getZkAddress());
    MetricsRepository d2ClientMetricsRepository = new MetricsRepository();
    d2StoreClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setD2ServiceName(d2ServiceName)
            .setD2Client(d2Client)
            .setMetricsRepository(d2ClientMetricsRepository)
            .setUseFastAvro(true));
  }

  public void closeStoreClient() {
    d2StoreClient.close();
    D2ClientUtils.shutdownClient(d2Client);
  }

  public AvroGenericStoreClient<String, Object> getD2StoreClient(boolean useFastAvro) {
    MetricsRepository d2ClientMetricsRepository = new MetricsRepository();
    return ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setD2ServiceName(d2ServiceName)
            .setD2Client(d2Client)
            .setMetricsRepository(d2ClientMetricsRepository)
            .setUseFastAvro(useFastAvro));
  }

  @DataProvider(name = "useFastAvroOptionsProvider")
  public Object[][] useFastAvroOptions() {
    // Benchmark should be run separately to get consistent result since each test will consume a lot of CPU resources.
    return new Object[][] { { true } };
    // return new Object[][] { { false } };
  }

  @Test(dataProvider = "useFastAvroOptionsProvider", enabled = false)
  public void testStoreClientWithMultiThreads(boolean useFastAvro) {
    d2StoreClient = getD2StoreClient(useFastAvro);
    long startTs = System.currentTimeMillis();

    int threadCnt = 20;
    ExecutorService executorService = Executors.newFixedThreadPool(threadCnt);
    AtomicInteger totalFetchCnt = new AtomicInteger();
    for (int i = 0; i < threadCnt; ++i) {
      executorService.submit(() -> {
        while (true) {
          try {
            d2StoreClient.batchGet(getBatchGetKeys(KEY_CNT)).get();
            totalFetchCnt.incrementAndGet();
          } catch (InterruptedException e) {
            break;
          } catch (ExecutionException e) {
            e.printStackTrace();
            break;
          }
        }
      });
    }

    executorService.shutdownNow();
    System.out.println(
        "This test with useFastAvro: " + useFastAvro + " took " + (System.currentTimeMillis() - startTs)
            + "ms, and it finished " + totalFetchCnt.get() + " requests");
  }
}
