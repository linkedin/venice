package com.linkedin.venice.storagenode;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.deserialization.BatchDeserializerType;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.serializer.AvroGenericDeserializer;
import com.linkedin.venice.tehuti.MetricsUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class StorageNodeComputeTest {
  private static final Logger LOGGER = Logger.getLogger(StorageNodeComputeTest.class);

  private VeniceClusterWrapper veniceCluster;
  private int valueSchemaId;
  private String storeName;

  private String routerAddr;
  private VeniceKafkaSerializer keySerializer;
  private VeniceKafkaSerializer valueSerializer;

  private static final String valueSchemaForCompute = "{" +
      "  \"namespace\": \"example.compute\",    " +
      "  \"type\": \"record\",        " +
      "  \"name\": \"MemberFeature\",       " +
      "  \"fields\": [        " +
      "         { \"name\": \"id\", \"type\": \"string\" },             " +
      "         { \"name\": \"name\", \"type\": \"string\" },           " +
      "         { \"name\": \"namemap\", \"type\":  {\"type\" : \"map\", \"values\" : \"int\" }},           " +
      "         { \"name\": \"member_feature\", \"type\": { \"type\": \"array\", \"items\": \"float\" } }        " +
      "  ]       " +
      " }       ";

  @BeforeClass(alwaysRun = true)
  public void setUp() throws InterruptedException, ExecutionException, VeniceClientException {
    veniceCluster = ServiceFactory.getVeniceCluster(1, 1, 0, 2, 100, false, false);
    // Add one more server with fast-avro enabled
    Properties serverProperties = new Properties();
    serverProperties.put(ConfigKeys.SERVER_COMPUTE_FAST_AVRO_ENABLED, true);
    veniceCluster.addVeniceServer(new Properties(), serverProperties);

    // To trigger long-tail retry
    Properties routerProperties = new Properties();
    routerProperties.put(ConfigKeys.ROUTER_LONG_TAIL_RETRY_FOR_SINGLE_GET_THRESHOLD_MS, 1);
    routerProperties.put(ConfigKeys.ROUTER_LONG_TAIL_RETRY_FOR_BATCH_GET_THRESHOLD_MS, "1-:1");
    veniceCluster.addVeniceRouter(routerProperties);
    routerAddr = "http://" + veniceCluster.getVeniceRouters().get(0).getAddress();

    String keySchema = "\"string\"";

    // Create test store
    VersionCreationResponse creationResponse = veniceCluster.getNewStoreVersion(keySchema, valueSchemaForCompute);
    storeName = Version.parseStoreFromKafkaTopicName(creationResponse.getKafkaTopic());
    valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

    // TODO: Make serializers parameterized so we test them all.
    keySerializer = new VeniceAvroKafkaSerializer(keySchema);
    valueSerializer = new VeniceAvroKafkaSerializer(valueSchemaForCompute);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    if (veniceCluster != null) {
      veniceCluster.close();
    }
  }

  @DataProvider(name = "testPermutations")
  public static Object[][] testPermutations() {
    // Config dimensions:
    CompressionStrategy[] compressionStrategies = new CompressionStrategy[]{CompressionStrategy.NO_OP, CompressionStrategy.GZIP};
    List<BatchDeserializerType> batchDeserializerTypes = Arrays.stream(BatchDeserializerType.values())
        .filter(batchGetDeserializerType -> batchGetDeserializerType != BatchDeserializerType.BLACK_HOLE)
        .collect(Collectors.toList());
    List<AvroGenericDeserializer.IterableImpl> iterableImplementations = Arrays.stream(AvroGenericDeserializer.IterableImpl.values())
        // LAZY_WITH_REPLAY_SUPPORT is only intended for the back end, so not super relevant here. Skipped to speed up the test.
        .filter(iterable -> iterable != AvroGenericDeserializer.IterableImpl.LAZY_WITH_REPLAY_SUPPORT)
        .collect(Collectors.toList());
    boolean[] yesAndNo = new boolean[]{true, false};

    List<Object[]> returnList = new ArrayList<>();
    for (CompressionStrategy compressionStrategy : compressionStrategies) {
      for (BatchDeserializerType batchDeserializerType : batchDeserializerTypes) {
        for (AvroGenericDeserializer.IterableImpl iterableImpl : iterableImplementations) {
          for (boolean fastAvro : yesAndNo) {
            for (boolean valueLargerThan1MB : yesAndNo) {
              returnList.add(new Object[]{compressionStrategy, batchDeserializerType, iterableImpl, fastAvro, valueLargerThan1MB});
            }
          }
        }
      }
    }
    Object[][] valuesToReturn= new Object[returnList.size()][5];
    return returnList.toArray(valuesToReturn);
  }

  @Test(timeOut = 30000, dataProvider = "testPermutations")
  public void testCompute(
      CompressionStrategy compressionStrategy,
      BatchDeserializerType batchDeserializerType,
      AvroGenericDeserializer.IterableImpl iterableImpl,
      boolean fastAvro,
      boolean valueLargerThan1MB) throws Exception {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setCompressionStrategy(compressionStrategy);
    params.setReadComputationEnabled(true);
    params.setChunkingEnabled(valueLargerThan1MB);
    veniceCluster.updateStore(storeName, params);

    VersionCreationResponse newVersion = veniceCluster.getNewVersion(storeName, 1024);
    final int pushVersion = newVersion.getVersion();
    String topic = newVersion.getKafkaTopic();

    VeniceWriterFactory vwFactory =
        TestUtils.getVeniceTestWriterFactory(veniceCluster.getKafka().getAddress());
    try (VeniceWriter<Object, byte[], byte[]> veniceWriter =
        vwFactory.createVeniceWriter(topic, keySerializer, new DefaultSerializer(), valueLargerThan1MB);
        AvroGenericStoreClient<String, Object> storeClient = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName)
                .setVeniceURL(routerAddr)
                .setBatchDeserializerType(batchDeserializerType)
                .setMultiGetEnvelopeIterableImpl(iterableImpl)
                .setUseFastAvro(fastAvro))) {

      String keyPrefix = "key_";
      String valuePrefix = "value_";
      pushSyntheticDataForCompute(topic, keyPrefix, valuePrefix, 100, veniceCluster,
          veniceWriter, pushVersion, compressionStrategy, valueLargerThan1MB);

      // Run multiple rounds
      int rounds = 100;
      int cur = 0;
      int keyCount = 10;
      while (cur++ < rounds) {
        Set<String> keySet = new HashSet<>();
        for (int i = 0; i < keyCount; ++i) {
          keySet.add(keyPrefix + i);
        }
        keySet.add("unknown_key");
        List<Float> p = Arrays.asList(100.0f, 0.1f);
        List<Float> cosP = Arrays.asList(123.4f, 5.6f);
        List<Float> hadamardP = Arrays.asList(135.7f, 246.8f);

        Map<String, GenericRecord> computeResult = storeClient.compute()
            .project("id")
            .dotProduct("member_feature", p, "member_score")
            .cosineSimilarity("member_feature", cosP, "cosine_similarity_result")
            .hadamardProduct("member_feature", hadamardP, "hadamard_product_result")
//          .count("namemap", "namemap_count")
//          .count("member_feature", "member_feature_count")
            .execute(keySet)
            /**
             * Added 2s timeout as a safety net as ideally each request should take sub-second.
             */
            .get(2, TimeUnit.SECONDS);
        Assert.assertEquals(computeResult.size(), 10);

        for (Map.Entry<String, GenericRecord> entry : computeResult.entrySet()) {
          int keyIdx = getKeyIndex(entry.getKey(), keyPrefix);
          // check projection result
          Assert.assertEquals(entry.getValue().get("id"), new Utf8(valuePrefix + keyIdx));
          // check dotProduct result; should be double for V1 request
          Assert.assertEquals(entry.getValue().get("member_score"), (float)(p.get(0) * (keyIdx + 1) + p.get(1) * ((keyIdx + 1) * 10)));

          // check cosine similarity result; should be double for V1 request
          float dotProductResult = (float) cosP.get(0) * (float)(keyIdx + 1) + cosP.get(1) * (float)((keyIdx + 1) * 10);
          float valueVectorMagnitude = (float) Math.sqrt(((float)(keyIdx + 1) * (float)(keyIdx + 1) + ((float)(keyIdx + 1) * 10.0f) * ((float)(keyIdx + 1) * 10.0f)));
          float parameterVectorMagnitude = (float) Math.sqrt((float)(cosP.get(0) * cosP.get(0) + cosP.get(1) * cosP.get(1)));
          float expectedCosineSimilarity = dotProductResult / (parameterVectorMagnitude * valueVectorMagnitude);
          Assert.assertEquals((float) entry.getValue().get("cosine_similarity_result"), expectedCosineSimilarity, 0.000001f);
         // Assert.assertEquals((int) entry.getValue().get("member_feature_count"),  2);
         // Assert.assertEquals((int) entry.getValue().get("namemap_count"),  0);

          // check hadamard product
          List<Float> hadamardProductResult = new ArrayList<>(2);
          hadamardProductResult.add(hadamardP.get(0) * (float)(keyIdx + 1));
          hadamardProductResult.add(hadamardP.get(1) * (float)((keyIdx + 1) * 10));
          Assert.assertEquals(entry.getValue().get("hadamard_product_result"), hadamardProductResult);
        }
      }

      /**
       * 10 keys, 1 dot product, 1 cosine similarity, 1 hadamard product per request, 100 rounds; considering retries,
       * compute operation counts should be higher.
       */
      Assert.assertTrue(MetricsUtils.getSum("." + storeName + "--compute_dot_product_count.Total", veniceCluster.getVeniceServers()) >= rounds * keyCount);
      Assert.assertTrue(MetricsUtils.getSum("." + storeName + "--compute_cosine_similarity_count.Total", veniceCluster.getVeniceServers()) >= rounds * keyCount);
      Assert.assertTrue(MetricsUtils.getSum("." + storeName + "--compute_hadamard_product_count.Total", veniceCluster.getVeniceServers()) >= rounds * keyCount);

      // Check retry requests
      Assert.assertTrue(MetricsUtils.getSum(".total--compute_retry_count.LambdaStat", veniceCluster.getVeniceRouters()) > 0,
          "After " + rounds + " reads, there should be some compute retry requests");
    }
  }

  /**
   * The goal of this test is to find the breaking point at which a compute request gets split into more than 1 part.
   */
  @Test(timeOut = 30000, groups = {"flaky"})
  public void testComputeRequestSize() throws Exception {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setReadComputationEnabled(true);
    veniceCluster.updateStore(storeName, params);

    VersionCreationResponse newVersion = veniceCluster.getNewVersion(storeName, 1024);
    final int pushVersion = newVersion.getVersion();

    try (VeniceWriter<Object, byte[], byte[]> veniceWriter = TestUtils.getVeniceTestWriterFactory(veniceCluster.getKafka().getAddress())
        .createVeniceWriter(newVersion.getKafkaTopic(), keySerializer, new DefaultSerializer());
        AvroGenericStoreClient<String, Object> storeClient = ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(routerAddr))) {

      String keyPrefix = "key_";
      String valuePrefix = "value_";
      int numberOfRecords = 10;
      pushSyntheticDataForCompute(newVersion.getKafkaTopic(), keyPrefix, valuePrefix, numberOfRecords, veniceCluster,
          veniceWriter, pushVersion, CompressionStrategy.NO_OP, false);

      // Run multiple rounds
      int rounds = 100;
      int cur = 0;
      while (cur++ < rounds) {
        Set<String> keySet = new HashSet<>();
        for (int i = 0; i < numberOfRecords; ++i) {
          keySet.add(keyPrefix + i);
        }
        keySet.add("unknown_key");
        List<Float> p = new ArrayList<>();
        for (int i = 0; i < cur; i++) {
          p.add((float) (i * 0.1));
        }

        Map<String, GenericRecord> computeResult = storeClient.compute()
            .dotProduct("member_feature", p, "member_score")
            .execute(keySet)
            /**
             * Added 2s timeout as a safety net as ideally each request should take sub-second.
             */
            .get(2, TimeUnit.SECONDS);
        Assert.assertEquals(computeResult.size(), numberOfRecords);

        LOGGER.info("Current round: " + cur + "/" + rounds
            + "\n - max part_count: "
            + MetricsUtils.getMax(".total--compute_request_part_count.Max", veniceCluster.getVeniceServers())
            + "\n - min part_count: "
            + MetricsUtils.getMin(".total--compute_request_part_count.Min", veniceCluster.getVeniceServers())
            + "\n - avg part_count: "
            + MetricsUtils.getAvg(".total--compute_request_part_count.Avg", veniceCluster.getVeniceServers())
            + "\n - max request size: "
            + MetricsUtils.getMax(".total--compute_request_size_in_bytes.Max", veniceCluster.getVeniceServers())
            + "\n - min request size: "
            + MetricsUtils.getMin(".total--compute_request_size_in_bytes.Min", veniceCluster.getVeniceServers())
            + "\n - avg request size: "
            + MetricsUtils.getAvg(".total--compute_request_size_in_bytes.Avg", veniceCluster.getVeniceServers())
        );

        Assert.assertEquals(MetricsUtils.getMax(".total--compute_request_part_count.Max", veniceCluster.getVeniceServers()), 1.0,
            "Expected a max of one part in compute request");
      }
    }
  }

  private int getKeyIndex(String key, String keyPrefix) {
    if (!key.startsWith(keyPrefix)) {
      return -1;
    }
    return Integer.valueOf(key.substring(keyPrefix.length()));
  }

  private void pushSyntheticDataForCompute(String topic, String keyPrefix, String valuePrefix, int numOfRecords,
                                           VeniceClusterWrapper veniceCluster, VeniceWriter<Object, byte[], byte[]> veniceWriter,
                                           int pushVersion, CompressionStrategy compressionStrategy, boolean valueLargerThan1MB) throws Exception {
    veniceWriter.broadcastStartOfPush(false, valueLargerThan1MB, compressionStrategy, new HashMap<>());
    Schema valueSchema = Schema.parse(valueSchemaForCompute);
    // Insert test record and wait synchronously for it to succeed
    for (int i = 0; i < numOfRecords; ++i) {
      GenericRecord value = new GenericData.Record(valueSchema);
      value.put("id", valuePrefix + i);

      String name = valuePrefix + i + "_name";
      if (valueLargerThan1MB) {
        char[] chars = new char[1024 * 1024 + 1];
        Arrays.fill(chars, Integer.toString(i).charAt(0));
        name = new String(chars);
      }
      value.put("name", name);
      value.put("namemap", Collections.emptyMap());

      List<Float> features = new ArrayList<>();
      features.add(Float.valueOf((float)(i + 1)));
      features.add(Float.valueOf((float)((i + 1) * 10)));
      value.put("member_feature", features);

      byte[] compressedValue = CompressorFactory.getCompressor(compressionStrategy).compress(valueSerializer.serialize(topic, value));
      veniceWriter.put(keyPrefix + i, compressedValue, valueSchemaId).get();
    }
    // Write end of push message to make node become ONLINE from BOOTSTRAP
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    // Wait for storage node to finish consuming, and new version to be activated
    String controllerUrl = veniceCluster.getAllControllersURLs();
    try (ControllerClient controllerClient = new ControllerClient(veniceCluster.getClusterName(), controllerUrl)) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        String status = controllerClient.queryJobStatus(topic).getStatus();
        if (status.equals(ExecutionStatus.ERROR.name())) {
          // Not recoverable (at least not without re-pushing), so not worth spinning our wheels until the timeout.
          throw new VeniceException("Push failed.");
        }

        int currentVersion = controllerClient.getStore(storeName).getStore().getCurrentVersion();
        // Refresh router metadata once new version is pushed, so that the router sees the latest store version.
        if (currentVersion == pushVersion) {
          veniceCluster.refreshAllRouterMetaData();
        }
        Assert.assertEquals(currentVersion, pushVersion, "New version not online yet.");
      });
    }
  }
}
