package com.linkedin.venice.storagenode;

import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_KB;
import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_MB;

import com.github.luben.zstd.ZstdDictTrainer;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.ComputeGenericRecord;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.tehuti.MetricsUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.ValueSize;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class StorageNodeComputeTest {
  private static final Logger LOGGER = LogManager.getLogger(StorageNodeComputeTest.class);

  enum AvroImpl {
    VANILLA_AVRO(false), FAST_AVRO(true);

    final boolean config;

    AvroImpl(boolean config) {
      this.config = config;
    }
  }

  private VeniceClusterWrapper veniceCluster;
  private AvroGenericStoreClient<String, Object> client;
  private int valueSchemaId;
  private String storeName;

  private String routerAddr;
  private VeniceKafkaSerializer keySerializer;
  private VeniceKafkaSerializer valueSerializer;

  private CompressorFactory compressorFactory;

  private static final String VALUE_SCHEMA_FOR_COMPUTE = "{" + "  \"namespace\": \"example.compute\",    "
      + "  \"type\": \"record\",        " + "  \"name\": \"MemberFeature\",       " + "  \"fields\": [        "
      + "         { \"name\": \"id\", \"type\": \"string\" },             "
      + "         { \"name\": \"name\", \"type\": \"string\" },           "
      + "         { \"name\": \"namemap\", \"type\":  {\"type\" : \"map\", \"values\" : \"int\" }},           "
      + "         { \"name\": \"member_feature\", \"type\": { \"type\": \"array\", \"items\": \"float\" } }        "
      + "  ]       " + " }       ";

  @BeforeClass(alwaysRun = true)
  public void setUp() throws InterruptedException, ExecutionException, VeniceClientException {
    veniceCluster = ServiceFactory.getVeniceCluster(1, 1, 0, 2, 100, false, false);
    // Add one more server with all the bells and whistles: fast-avro, parallel batch get
    Properties serverProperties = new Properties();
    serverProperties.put(ConfigKeys.SERVER_COMPUTE_FAST_AVRO_ENABLED, true);
    serverProperties.put(ConfigKeys.SERVER_ENABLE_PARALLEL_BATCH_GET, true);
    serverProperties.put(ConfigKeys.SERVER_PARALLEL_BATCH_GET_CHUNK_SIZE, 100);
    veniceCluster.addVeniceServer(new Properties(), serverProperties);

    // To trigger long-tail retry
    Properties routerProperties = new Properties();
    routerProperties.put(ConfigKeys.ROUTER_LONG_TAIL_RETRY_FOR_SINGLE_GET_THRESHOLD_MS, 1);
    routerProperties.put(ConfigKeys.ROUTER_LONG_TAIL_RETRY_FOR_BATCH_GET_THRESHOLD_MS, "1-:1");
    routerProperties.put(ConfigKeys.ROUTER_SMART_LONG_TAIL_RETRY_ENABLED, false);
    veniceCluster.addVeniceRouter(routerProperties);
    routerAddr = "http://" + veniceCluster.getVeniceRouters().get(0).getAddress();

    String keySchema = "\"string\"";

    // Create test store
    VersionCreationResponse creationResponse = veniceCluster.getNewStoreVersion(keySchema, VALUE_SCHEMA_FOR_COMPUTE);
    storeName = Version.parseStoreFromKafkaTopicName(creationResponse.getKafkaTopic());
    valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

    // TODO: Make serializers parameterized so we test them all.
    keySerializer = new VeniceAvroKafkaSerializer(keySchema);
    valueSerializer = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_FOR_COMPUTE);

    client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerAddr).setUseFastAvro(true));

    compressorFactory = new CompressorFactory();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    if (veniceCluster != null) {
      veniceCluster.close();
    }
    Utils.closeQuietlyWithErrorLogged(client);
    Utils.closeQuietlyWithErrorLogged(compressorFactory);
  }

  @DataProvider(name = "testPermutations")
  public static Object[][] testPermutations() {
    // Config dimensions:
    CompressionStrategy[] compressionStrategies = new CompressionStrategy[] { CompressionStrategy.NO_OP,
        CompressionStrategy.GZIP, CompressionStrategy.ZSTD_WITH_DICT };

    List<Object[]> returnList = new ArrayList<>();
    for (CompressionStrategy compressionStrategy: compressionStrategies) {
      for (ValueSize valueLargerThan1MB: ValueSize.values()) {
        returnList.add(new Object[] { compressionStrategy, valueLargerThan1MB });
      }
    }
    Object[][] valuesToReturn = new Object[returnList.size()][5];
    return returnList.toArray(valuesToReturn);
  }

  @Test(timeOut = 30000, dataProvider = "testPermutations")
  public void testCompute(CompressionStrategy compressionStrategy, ValueSize valueLargerThan1MB) throws Exception {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setCompressionStrategy(compressionStrategy);
    params.setReadComputationEnabled(true);
    params.setChunkingEnabled(valueLargerThan1MB.config);
    ControllerResponse controllerResponse = veniceCluster.updateStore(storeName, params);
    Assert.assertFalse(controllerResponse.isError(), "Error updating store settings: " + controllerResponse.getError());

    VersionCreationResponse newVersion = veniceCluster.getNewVersion(storeName, false);
    Assert.assertFalse(newVersion.isError(), "Error creation new version: " + newVersion.getError());
    final int pushVersion = newVersion.getVersion();
    String topic = newVersion.getKafkaTopic();
    int keyCount = 10;
    String keyPrefix = "key_";
    String valuePrefix = "value_";

    PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
        veniceCluster.getPubSubBrokerWrapper().getPubSubClientsFactory().getProducerAdapterFactory();
    VeniceWriterFactory vwFactory = IntegrationTestPushUtils
        .getVeniceWriterFactory(veniceCluster.getPubSubBrokerWrapper(), pubSubProducerAdapterFactory);
    try (VeniceWriter<Object, byte[], byte[]> veniceWriter = vwFactory.createVeniceWriter(
        new VeniceWriterOptions.Builder(topic).setKeySerializer(keySerializer)
            .setValueSerializer(new DefaultSerializer())
            .setChunkingEnabled(valueLargerThan1MB.config)
            .build())) {
      pushSyntheticDataForCompute(
          topic,
          keyPrefix,
          valuePrefix,
          keyCount,
          veniceCluster,
          veniceWriter,
          pushVersion,
          compressionStrategy,
          valueLargerThan1MB.config);
    }

    // Run multiple rounds
    int rounds = 100;
    int cur = 0;
    while (cur++ < rounds) {
      Set<String> keySet = new HashSet<>();
      for (int i = 0; i < keyCount; ++i) {
        keySet.add(keyPrefix + i);
      }
      keySet.add("unknown_key");
      List<Float> p = Arrays.asList(100.0f, 0.1f);
      List<Float> cosP = Arrays.asList(123.4f, 5.6f);
      List<Float> hadamardP = Arrays.asList(135.7f, 246.8f);

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
        Map<String, ComputeGenericRecord> computeResult = client.compute()
            .project("id")
            .dotProduct("member_feature", p, "member_score")
            .cosineSimilarity("member_feature", cosP, "cosine_similarity_result")
            .hadamardProduct("member_feature", hadamardP, "hadamard_product_result")
            .count("namemap", "namemap_count")
            .count("member_feature", "member_feature_count")
            .execute(keySet)
            /**
             * Added 2s timeout as a safety net as ideally each request should take sub-second.
             */
            .get(2, TimeUnit.SECONDS);

        Assert.assertEquals(computeResult.size(), 10);

        computeResult.forEach((key, value) -> {
          int keyIdx = getKeyIndex(key, keyPrefix);
          // check projection result
          Assert.assertEquals(value.get("id"), new Utf8(valuePrefix + keyIdx));
          // check dotProduct result; should be double for V1 request
          Assert.assertEquals(value.get("member_score"), (p.get(0) * (keyIdx + 1) + p.get(1) * ((keyIdx + 1) * 10)));

          // check cosine similarity result; should be double for V1 request
          float dotProductResult = cosP.get(0) * (float) (keyIdx + 1) + cosP.get(1) * (float) ((keyIdx + 1) * 10);
          float valueVectorMagnitude = (float) Math.sqrt(
              ((float) (keyIdx + 1) * (float) (keyIdx + 1)
                  + ((float) (keyIdx + 1) * 10.0f) * ((float) (keyIdx + 1) * 10.0f)));
          float parameterVectorMagnitude = (float) Math.sqrt((cosP.get(0) * cosP.get(0) + cosP.get(1) * cosP.get(1)));
          float expectedCosineSimilarity = dotProductResult / (parameterVectorMagnitude * valueVectorMagnitude);
          Assert.assertEquals((float) value.get("cosine_similarity_result"), expectedCosineSimilarity, 0.000001f);
          Assert.assertEquals((int) value.get("member_feature_count"), 2);
          Assert.assertEquals((int) value.get("namemap_count"), 0);

          // check hadamard product
          List<Float> hadamardProductResult = new ArrayList<>(2);
          hadamardProductResult.add(hadamardP.get(0) * (float) (keyIdx + 1));
          hadamardProductResult.add(hadamardP.get(1) * (float) ((keyIdx + 1) * 10));
          Assert.assertEquals(value.get("hadamard_product_result"), hadamardProductResult);
        });
      });
    }

    /**
     * 10 keys, 1 dot product, 1 cosine similarity, 1 hadamard product per request, 100 rounds; considering retries,
     * compute operation counts should be higher.
     */
    Assert.assertTrue(
        MetricsUtils.getSum(
            "." + storeName + "--compute_dot_product_count.Total",
            veniceCluster.getVeniceServers()) >= rounds * keyCount);
    Assert.assertTrue(
        MetricsUtils.getSum(
            "." + storeName + "--compute_cosine_similarity_count.Total",
            veniceCluster.getVeniceServers()) >= rounds * keyCount);
    Assert.assertTrue(
        MetricsUtils.getSum(
            "." + storeName + "--compute_hadamard_product_count.Total",
            veniceCluster.getVeniceServers()) >= rounds * keyCount);

    // Check retry requests
    Assert.assertTrue(
        MetricsUtils.getSum(".total--compute_streaming_retry_count.LambdaStat", veniceCluster.getVeniceRouters()) > 0,
        "After " + rounds + " reads, there should be some compute retry requests");
  }

  /**
   * The goal of this test is to find the breaking point at which a compute request gets split into more than 1 part.
   */
  @Test(timeOut = 30000, groups = { "flaky" })
  public void testComputeRequestSize() throws Exception {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setReadComputationEnabled(true);
    veniceCluster.updateStore(storeName, params);

    VersionCreationResponse newVersion = veniceCluster.getNewVersion(storeName);
    final int pushVersion = newVersion.getVersion();

    try (
        PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
            veniceCluster.getPubSubBrokerWrapper().getPubSubClientsFactory().getProducerAdapterFactory();
        VeniceWriter<Object, byte[], byte[]> veniceWriter = IntegrationTestPushUtils
            .getVeniceWriterFactory(veniceCluster.getPubSubBrokerWrapper(), pubSubProducerAdapterFactory)
            .createVeniceWriter(
                new VeniceWriterOptions.Builder(newVersion.getKafkaTopic()).setKeySerializer(keySerializer).build());
        AvroGenericStoreClient<String, Object> storeClient = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerAddr))) {

      String keyPrefix = "key_";
      String valuePrefix = "value_";
      int numberOfRecords = 10;
      pushSyntheticDataForCompute(
          newVersion.getKafkaTopic(),
          keyPrefix,
          valuePrefix,
          numberOfRecords,
          veniceCluster,
          veniceWriter,
          pushVersion,
          CompressionStrategy.NO_OP,
          false);

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

        Map<String, ComputeGenericRecord> computeResult = storeClient.compute()
            .dotProduct("member_feature", p, "member_score")
            .execute(keySet)
            /**
             * Added 2s timeout as a safety net as ideally each request should take sub-second.
             */
            .get(2, TimeUnit.SECONDS);
        Assert.assertEquals(computeResult.size(), numberOfRecords);

        LOGGER.info(
            "Current round: {}/{}\n - max part_count: {}\n - min part_count: {}\n - avg part_count: {}\n - max request size: {}\n - min request size: {}\n - avg request size: {}",
            cur,
            rounds,
            MetricsUtils.getMax(".total--compute_request_part_count.Max", veniceCluster.getVeniceServers()),
            MetricsUtils.getMin(".total--compute_request_part_count.Min", veniceCluster.getVeniceServers()),
            MetricsUtils.getAvg(".total--compute_request_part_count.Avg", veniceCluster.getVeniceServers()),
            MetricsUtils.getMax(".total--compute_request_size_in_bytes.Max", veniceCluster.getVeniceServers()),
            MetricsUtils.getMin(".total--compute_request_size_in_bytes.Min", veniceCluster.getVeniceServers()),
            MetricsUtils.getAvg(".total--compute_request_size_in_bytes.Avg", veniceCluster.getVeniceServers()));

        Assert.assertEquals(
            MetricsUtils.getMax(".total--compute_request_part_count.Max", veniceCluster.getVeniceServers()),
            1.0,
            "Expected a max of one part in compute request");
      }
    }
  }

  private int getKeyIndex(String key, String keyPrefix) {
    if (!key.startsWith(keyPrefix)) {
      return -1;
    }
    return Integer.parseInt(key.substring(keyPrefix.length()));
  }

  private void pushSyntheticDataForCompute(
      String topic,
      String keyPrefix,
      String valuePrefix,
      int numOfRecords,
      VeniceClusterWrapper veniceCluster,
      VeniceWriter<Object, byte[], byte[]> veniceWriter,
      int pushVersion,
      CompressionStrategy compressionStrategy,
      boolean valueLargerThan1MB) throws Exception {
    Schema valueSchema = Schema.parse(VALUE_SCHEMA_FOR_COMPUTE);
    // Insert test record
    List<byte[]> values = new ArrayList<>(numOfRecords);
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
      features.add(Float.valueOf((float) (i + 1)));
      features.add(Float.valueOf((float) ((i + 1) * 10)));
      value.put("member_feature", features);
      values.add(i, valueSerializer.serialize(topic, value));
    }

    Optional<ByteBuffer> compressionDictionary = Optional.empty();
    VeniceCompressor compressor;
    if (compressionStrategy.equals(CompressionStrategy.ZSTD_WITH_DICT)) {
      ZstdDictTrainer trainer = new ZstdDictTrainer(200 * BYTES_PER_MB, 100 * BYTES_PER_KB);
      for (byte[] value: values) {
        trainer.addSample(value);
      }

      // Not using trainSamplesDirect since we need byte[] to create compressor.
      byte[] compressionDictionaryBytes = trainer.trainSamples();
      compressionDictionary = Optional.of(ByteBuffer.wrap(compressionDictionaryBytes));

      compressor = compressorFactory
          .createVersionSpecificCompressorIfNotExist(compressionStrategy, topic, compressionDictionaryBytes);
    } else {
      compressor = compressorFactory.getCompressor(compressionStrategy);
    }

    veniceWriter
        .broadcastStartOfPush(false, valueLargerThan1MB, compressionStrategy, compressionDictionary, new HashMap<>());

    Future[] writerFutures = new Future[numOfRecords];
    for (int i = 0; i < numOfRecords; i++) {
      byte[] compressedValue = compressor.compress(values.get(i));
      writerFutures[i] = veniceWriter.put(keyPrefix + i, compressedValue, valueSchemaId);
    }

    // wait synchronously for them to succeed
    for (int i = 0; i < numOfRecords; ++i) {
      writerFutures[i].get();
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
