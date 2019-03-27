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
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.serializer.AvroGenericDeserializer;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.writer.VeniceWriter;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class StorageNodeComputeTest {
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
      "         { \"name\": \"member_feature\", \"type\": { \"type\": \"array\", \"items\": \"float\" } }        " +
      "  ]       " +
      " }       ";

  @BeforeClass
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

  @AfterClass
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
            returnList.add(new Object[]{compressionStrategy, batchDeserializerType, iterableImpl, fastAvro});
          }
        }
      }
    }
    Object[][] valuesToReturn= new Object[returnList.size()][4];
    return returnList.toArray(valuesToReturn);
  }

  @Test (timeOut = 30000, dataProvider = "testPermutations")
  public void testCompute(
      CompressionStrategy compressionStrategy,
      BatchDeserializerType batchDeserializerType,
      AvroGenericDeserializer.IterableImpl iterableImpl,
      boolean fastAvro) throws Exception {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setCompressionStrategy(compressionStrategy);
    params.setReadComputationEnabled(true);
    veniceCluster.updateStore(storeName, params);

    VersionCreationResponse newVersion = veniceCluster.getNewVersion(storeName, 1024);
    final int pushVersion = newVersion.getVersion();

    try (VeniceWriter<Object, byte[]> veniceWriter = TestUtils.getVeniceTestWriterFactory(veniceCluster.getKafka().getAddress())
        .getVeniceWriter(newVersion.getKafkaTopic(), keySerializer, new DefaultSerializer());
        AvroGenericStoreClient<String, Object> storeClient = ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(routerAddr)
            .setBatchDeserializerType(batchDeserializerType)
            .setMultiGetEnvelopeIterableImpl(iterableImpl)
            .setUseFastAvro(fastAvro))) {

      String keyPrefix = "key_";
      String valuePrefix = "value_";
      pushSyntheticDataForCompute(newVersion.getKafkaTopic(), keyPrefix, valuePrefix, 100, veniceCluster,
          veniceWriter, pushVersion, compressionStrategy);

      // Run multiple rounds
      int rounds = 100;
      int cur = 0;
      while (cur++ < rounds) {
        Set<String> keySet = new HashSet<>();
        for (int i = 0; i < 10; ++i) {
          keySet.add(keyPrefix + i);
        }
        keySet.add("unknown_key");
        List<Float> p = Arrays.asList(100.0f, 0.1f);
        List<Float> cosP = Arrays.asList(123.4f, 5.6f);
        Map<String, GenericRecord> computeResult = (Map<String, GenericRecord>) storeClient.compute()
            .project("id")
            .dotProduct("member_feature", p, "member_score")
            .cosineSimilarity("member_feature", cosP, "cosine_similarity_result")
            .execute(keySet)
            /**
             * Background around this timeout:
             *
             * This is a test which re-uses the same store multiple times, pushes and then queries data,
             * but does not wait for the routers to be updated before querying the data. Obviously, the
             * test itself can be fixed by adding retries, but in this case, it seems that it uncovers a
             * real issue: the client should not time out, but rather, then router's exception should be
             * propagated back to the client and the client should fail fast.
             *
             * The router sees one of two exceptions:
             *
             * 1. In the first few iterations, if the router has not seen even one store-version yet, then
             *    it fails with this:
             *
             *    com.linkedin.venice.exceptions.VeniceNoHelixResourceException: There is no version for store 'venice-store-1557853651072-1905802900'.  Please push data to that store
             *
             * 2. In later iterations, if the router is hanging on to an old store-version which is 2 or
             *    more versions behind the current one, and the version the router knows about got deleted
             *    on the SN, then it fails with this:
             *
             *    com.linkedin.venice.exceptions.VeniceNoHelixResourceException: Resource 'venice-store-1557853651072-1905802900_v10' does not exist
             *
             * Setting the timeout here helps make the test fail faster, otherwise, the test failure is
             * confusing, indicating that it timed out waiting for some resources to close (which seems
             * inaccurate).
             *
             * TODO: Find out why some requests time out instead of failing fast.
             */
            .get(5, TimeUnit.SECONDS);
        Assert.assertEquals(computeResult.size(), 10);
        for (Map.Entry<String, GenericRecord> entry : computeResult.entrySet()) {
          int keyIdx = getKeyIndex(entry.getKey(), keyPrefix);
          // check projection result
          Assert.assertEquals(entry.getValue().get("id"), new Utf8(valuePrefix + keyIdx));
          // check dotProduct result
          Assert.assertEquals(entry.getValue().get("member_score"), (double) (p.get(0) * keyIdx + p.get(1) * (keyIdx * 10)));

          // check cosine similarity result
          double dotProductResult = (double) (cosP.get(0) * (float)keyIdx + cosP.get(1) * (float)(keyIdx * 10));
          double valueVectorMagnitude = Math.sqrt((double) ((float)keyIdx * (float)keyIdx + ((float)keyIdx * 10.0f) * ((float)keyIdx * 10.0f)));
          double parameterVectorMagnitude = Math.sqrt((double) (cosP.get(0) * cosP.get(0) + cosP.get(1) * cosP.get(1)));
          double expectedCosineSimilarity = dotProductResult / (parameterVectorMagnitude * valueVectorMagnitude);
          Assert.assertEquals((double)entry.getValue().get("cosine_similarity_result"), expectedCosineSimilarity, 0.000001d);
        }
      }

      // Check retry requests
      double computeRetries = 0;
      for (VeniceRouterWrapper veniceRouterWrapper : veniceCluster.getVeniceRouters()) {
        MetricsRepository metricsRepository = veniceRouterWrapper.getMetricsRepository();
        Map<String, ? extends Metric> metrics = metricsRepository.metrics();
        if (metrics.containsKey(".total--compute_retry_count.LambdaStat")) {
          computeRetries += metrics.get(".total--compute_retry_count.LambdaStat").value();
        }
      }

      Assert.assertTrue(computeRetries > 0, "After " + rounds + " reads, there should be some compute retry requests");
    }
  }

  private int getKeyIndex(String key, String keyPrefix) {
    if (!key.startsWith(keyPrefix)) {
      return -1;
    }
    return Integer.valueOf(key.substring(keyPrefix.length()));
  }

  private void pushSyntheticDataForCompute(String topic, String keyPrefix, String valuePrefix, int numOfRecords,
                                           VeniceClusterWrapper veniceCluster, VeniceWriter<Object, byte[]> veniceWriter,
                                           int pushVersion, CompressionStrategy compressionStrategy) throws Exception {
    veniceWriter.broadcastStartOfPush(false, false, compressionStrategy, new HashMap<>());
    Schema valueSchema = Schema.parse(valueSchemaForCompute);
    // Insert test record and wait synchronously for it to succeed
    for (int i = 0; i < numOfRecords; ++i) {
      GenericRecord value = new GenericData.Record(valueSchema);
      value.put("id", valuePrefix + i);
      value.put("name", valuePrefix + i + "_name");
      List<Float> features = new ArrayList<>();
      features.add(Float.valueOf((float)i));
      features.add(Float.valueOf((float)(i * 10)));
      value.put("member_feature", features);

      byte[] compressedValue = CompressorFactory.getCompressor(compressionStrategy).compress(valueSerializer.serialize(topic, value));
      veniceWriter.put(keyPrefix + i, compressedValue, valueSchemaId).get();
    }
    // Write end of push message to make node become ONLINE from BOOTSTRAP
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    // Wait for storage node to finish consuming, and new version to be activated
    String controllerUrl = veniceCluster.getAllControllersURLs();
    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
      int currentVersion = ControllerClient.getStore(controllerUrl, veniceCluster.getClusterName(), storeName).getStore().getCurrentVersion();
      return currentVersion == pushVersion;
    });
  }
}
