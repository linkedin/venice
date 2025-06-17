package com.linkedin.venice.storagenode;

import static com.linkedin.venice.VeniceConstants.VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericReadComputeStoreClient;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.ComputeGenericRecord;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compute.ComputeUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class ReadComputeValidationTest {
  private static final long TIMEOUT = 1 * Time.MS_PER_MINUTE;
  private static final String VALUE_PREFIX = "id_";
  private VeniceClusterWrapper veniceCluster;
  private String storeName;
  private String routerAddr;
  private VeniceKafkaSerializer keySerializer;
  private VeniceKafkaSerializer valueSerializer;
  private VeniceKafkaSerializer valueSerializer2;
  private VeniceKafkaSerializer valueSerializerSwapped;
  private CompressorFactory compressorFactory;

  private static final List<Float> MF_EMBEDDING = generateRandomFloatList(100);
  private static final List<Float> COMPANIES_EMBEDDING = generateRandomFloatList(100);
  private static final List<Float> PYMK_COSINE_SIMILARITY_EMBEDDING = generateRandomFloatList(100);

  private static final String VALUE_SCHEMA_FOR_COMPUTE = "{" + "  \"namespace\": \"example.compute\",    "
      + "  \"type\": \"record\",        " + "  \"name\": \"MemberFeature\",       " + "  \"fields\": [        "
      + "         { \"name\": \"id\", \"type\": \"string\" },             "
      + "         { \"name\": \"name\", \"type\": \"string\" },           "
      + "         {   \"default\": [], \n  \"name\": \"companiesEmbedding\",  \"type\": {  \"items\": \"float\",  \"type\": \"array\"   }  }, "
      + "         { \"name\": \"member_feature\", \"type\": { \"type\": \"array\", \"items\": \"float\" } }        "
      + "  ]       " + " }       ";

  private static final String VALUE_SCHEMA_FOR_COMPUTE_2 = "{" + "  \"namespace\": \"example.compute\",    "
      + "  \"type\": \"record\",        " + "  \"name\": \"MemberFeature\",       " + "  \"fields\": [        "
      + "         { \"name\": \"id\", \"type\": \"string\" },             "
      + "         { \"name\": \"name\", \"type\": \"string\" },           "
      + "         { \"name\": \"member_feature\", \"type\": { \"type\": \"array\", \"items\": \"float\" } }        "
      + "  ]       " + " }       ";

  private static final String VALUE_SCHEMA_FOR_COMPUTE_SWAPPED = "{" + "  \"namespace\": \"example.compute\",    "
      + "  \"type\": \"record\",        " + "  \"name\": \"MemberFeature\",       " + "  \"fields\": [        "
      + "         { \"name\": \"id\", \"type\": \"string\" },             "
      + "         { \"name\": \"name\", \"type\": \"string\" },           "
      + "         { \"name\": \"member_feature\", \"type\": { \"type\": \"array\", \"items\": \"float\" } },        "
      + "         {   \"default\": [], \n  \"name\": \"companiesEmbedding\",  \"type\": {  \"items\": \"float\",  \"type\": \"array\"   }  } "
      + "  ]       " + " }       ";

  @BeforeClass(alwaysRun = true)
  public void setUp() throws VeniceClientException {
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(1)
        .numberOfRouters(0)
        .replicationFactor(2)
        .partitionSize(100)
        .sslToStorageNodes(false)
        .sslToKafka(false)
        .build();
    veniceCluster = ServiceFactory.getVeniceCluster(options);
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

    String keySchema = "\"int\"";

    // Create test store
    VersionCreationResponse creationResponse = veniceCluster.getNewStoreVersion(keySchema, VALUE_SCHEMA_FOR_COMPUTE);
    storeName = Version.parseStoreFromKafkaTopicName(creationResponse.getKafkaTopic());

    // TODO: Make serializers parameterized so we test them all.
    keySerializer = new VeniceAvroKafkaSerializer(keySchema);
    valueSerializer = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_FOR_COMPUTE);
    valueSerializer2 = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_FOR_COMPUTE_2);
    valueSerializerSwapped = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_FOR_COMPUTE_SWAPPED);

    compressorFactory = new CompressorFactory();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    if (veniceCluster != null) {
      veniceCluster.close();
    }
    Utils.closeQuietlyWithErrorLogged(compressorFactory);
  }

  @Test(timeOut = TIMEOUT)
  public void testComputeMissingField() throws Exception {
    CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;
    boolean fastAvro = true;
    boolean valueLargerThan1MB = false;
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setCompressionStrategy(compressionStrategy);
    params.setReadComputationEnabled(true);
    params.setChunkingEnabled(valueLargerThan1MB);
    veniceCluster.updateStore(storeName, params);

    VersionCreationResponse newVersion = veniceCluster.getNewVersion(storeName);
    final int pushVersion = newVersion.getVersion();
    String topic = newVersion.getKafkaTopic();
    PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
        veniceCluster.getPubSubBrokerWrapper().getPubSubClientsFactory().getProducerAdapterFactory();
    VeniceWriterFactory vwFactory = IntegrationTestPushUtils
        .getVeniceWriterFactory(veniceCluster.getPubSubBrokerWrapper(), pubSubProducerAdapterFactory);
    try (
        VeniceWriter<Object, byte[], byte[]> veniceWriter = vwFactory.createVeniceWriter(
            new VeniceWriterOptions.Builder(topic).setKeyPayloadSerializer(keySerializer)
                .setValuePayloadSerializer(new DefaultSerializer())
                .setChunkingEnabled(valueLargerThan1MB)
                .build());
        AvroGenericStoreClient<Integer, Object> storeClient = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerAddr).setUseFastAvro(fastAvro))) {

      pushSyntheticDataToStore(
          topic,
          100,
          veniceWriter,
          pushVersion,
          VALUE_SCHEMA_FOR_COMPUTE,
          valueSerializer,
          false,
          1);

      Set<Integer> keySet = new HashSet<>();
      keySet.add(1);
      keySet.add(2);
      storeClient.compute()
          .cosineSimilarity("companiesEmbedding", PYMK_COSINE_SIMILARITY_EMBEDDING, "companiesEmbedding_score")
          .cosineSimilarity("member_feature", PYMK_COSINE_SIMILARITY_EMBEDDING, "member_feature_score")
          .execute(keySet)
          .get();
      ControllerClient controllerClient = new ControllerClient(
          veniceCluster.getClusterName(),
          veniceCluster.getRandomVeniceController().getControllerUrl());
      controllerClient.addValueSchema(storeName, VALUE_SCHEMA_FOR_COMPUTE_2);
      // Restart the server to get new schemas
      veniceCluster.stopAndRestartVeniceServer(veniceCluster.getVeniceServers().get(0).getPort());

      VersionCreationResponse newVersion2 = veniceCluster.getNewVersion(storeName);
      final int pushVersion2 = newVersion2.getVersion();
      String topic2 = newVersion2.getKafkaTopic();
      VeniceWriter<Object, byte[], byte[]> veniceWriter2 = vwFactory.createVeniceWriter(
          new VeniceWriterOptions.Builder(topic2).setKeyPayloadSerializer(keySerializer)
              .setValuePayloadSerializer(new DefaultSerializer())
              .setChunkingEnabled(valueLargerThan1MB)
              .build());
      pushSyntheticDataToStore(
          topic2,
          100,
          veniceWriter2,
          pushVersion2,
          VALUE_SCHEMA_FOR_COMPUTE_2,
          valueSerializer2,
          true,
          2);
      // Restart the server to get new schemas
      veniceCluster.stopAndRestartVeniceServer(veniceCluster.getVeniceServers().get(0).getPort());

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
        Map<Integer, ComputeGenericRecord> computeResult = storeClient.compute()
            .cosineSimilarity("companiesEmbedding", PYMK_COSINE_SIMILARITY_EMBEDDING, "companiesEmbedding_score")
            .cosineSimilarity("member_feature", PYMK_COSINE_SIMILARITY_EMBEDDING, "member_feature_score")
            .execute(keySet)
            .get();

        // Venice Server won't report any missing field error since it would always use the schema passed by the client
        // to decode the value.
        computeResult.forEach(
            (key, value) -> Assert.assertEquals(
                ((HashMap<String, String>) value.get(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME)).size(),
                0));
      });
    }
  }

  @Test(timeOut = TIMEOUT)
  public void testComputeSwappedFields() throws Exception {
    CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;
    boolean fastAvro = true;
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setCompressionStrategy(compressionStrategy);
    params.setReadComputationEnabled(true);
    params.setChunkingEnabled(false);
    veniceCluster.updateStore(storeName, params);

    VersionCreationResponse newVersion = veniceCluster.getNewVersion(storeName);
    final int pushVersion = newVersion.getVersion();
    String topic = newVersion.getKafkaTopic();
    PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
        veniceCluster.getPubSubBrokerWrapper().getPubSubClientsFactory().getProducerAdapterFactory();
    VeniceWriterFactory vwFactory = IntegrationTestPushUtils
        .getVeniceWriterFactory(veniceCluster.getPubSubBrokerWrapper(), pubSubProducerAdapterFactory);
    try (
        VeniceWriter<Object, byte[], byte[]> veniceWriter = vwFactory
            .createVeniceWriter(new VeniceWriterOptions.Builder(topic).setKeyPayloadSerializer(keySerializer).build());
        AvroGenericStoreClient<Integer, Object> storeClient = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerAddr).setUseFastAvro(fastAvro))) {

      pushSyntheticDataToStore(
          topic,
          100,
          veniceWriter,
          pushVersion,
          VALUE_SCHEMA_FOR_COMPUTE_SWAPPED,
          valueSerializer,
          false,
          1);

      Set<Integer> keySet = new HashSet<>();
      keySet.add(1);
      keySet.add(2);
      storeClient.compute()
          .cosineSimilarity("companiesEmbedding", PYMK_COSINE_SIMILARITY_EMBEDDING, "companiesEmbedding_score")
          .cosineSimilarity("member_feature", PYMK_COSINE_SIMILARITY_EMBEDDING, "member_feature_score")
          .execute(keySet)
          .get();
      ControllerClient controllerClient = new ControllerClient(
          veniceCluster.getClusterName(),
          veniceCluster.getRandomVeniceController().getControllerUrl());
      controllerClient.addValueSchema(storeName, VALUE_SCHEMA_FOR_COMPUTE_2);
      // Restart the server to get new schemas
      veniceCluster.stopAndRestartVeniceServer(veniceCluster.getVeniceServers().get(0).getPort());

      VersionCreationResponse newVersion2 = veniceCluster.getNewVersion(storeName);
      final int pushVersion2 = newVersion2.getVersion();
      String topic2 = newVersion2.getKafkaTopic();
      VeniceWriter<Object, byte[], byte[]> veniceWriter2 = vwFactory
          .createVeniceWriter(new VeniceWriterOptions.Builder(topic2).setKeyPayloadSerializer(keySerializer).build());
      pushSyntheticDataToStore(
          topic2,
          100,
          veniceWriter2,
          pushVersion2,
          VALUE_SCHEMA_FOR_COMPUTE_SWAPPED,
          valueSerializerSwapped,
          false,
          2);
      // Restart the server to get new schemas
      veniceCluster.stopAndRestartVeniceServer(veniceCluster.getVeniceServers().get(0).getPort());

      Map<Integer, ComputeGenericRecord> computeResult =
          storeClient.compute().project("member_feature").execute(keySet).get();

      computeResult.forEach(
          (key, value) -> Assert
              .assertEquals(((HashMap<String, String>) value.get(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME)).size(), 0));
    }
  }

  /**
   * The QT-FDS compliant schema allows nullable list field in a schema. This method tests that the dot product, cosine
   * similarity, and hadamard product works with nullable list field.
   *
   * @throws Exception
   */
  @Test(timeOut = TIMEOUT)
  public void testComputeOnStoreWithQTFDScompliantSchema() throws Exception {
    String keySchema = "\"int\"";
    String valueSchemaWithNullableListField = "{" + "  \"namespace\": \"example.compute\",    "
        + "  \"type\": \"record\",        " + "  \"name\": \"MemberFeature\",       " + "  \"fields\": [        "
        + "   {\"name\": \"id\", \"type\": \"string\" },             "
        + "   {\"name\": \"name\", \"type\": \"string\" },           "
        + "   {\"name\": \"member_feature\", \"type\": [\"null\",{\"type\":\"array\",\"items\":\"float\"}],\"default\": null}"
        + // nullable field
        "  ] " + " }  ";
    VeniceAvroKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(keySchema);
    VeniceAvroKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(valueSchemaWithNullableListField);

    // Create store with a version
    VersionCreationResponse creationResponse =
        veniceCluster.getNewStoreVersion(keySchema, valueSchemaWithNullableListField);
    Assert.assertFalse(creationResponse.isError());
    final String topic = creationResponse.getKafkaTopic();

    // Update the store and enable read-compute
    final String storeName = Version.parseStoreFromKafkaTopicName(creationResponse.getKafkaTopic());
    CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;

    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setCompressionStrategy(compressionStrategy);
    params.setReadComputationEnabled(true);
    params.setChunkingEnabled(false);
    ControllerResponse controllerResponse = veniceCluster.updateStore(storeName, params);
    Assert.assertFalse(controllerResponse.isError());

    // Create synthetic value records to write to the store
    Schema valueSchema = Schema.parse(valueSchemaWithNullableListField);
    GenericRecord value1 = new GenericData.Record(valueSchema);
    List<Float> memberFeatureEmbedding = Arrays.asList(1.0f, 2.0f, 3.0f);
    final int key1 = 1;
    final int key2 = 2;
    value1.put("id", "1");
    value1.put("name", "companiesEmbedding");
    value1.put("member_feature", memberFeatureEmbedding);

    GenericRecord value2 = new GenericData.Record(valueSchema);
    value2.put("id", "2");
    value2.put("name", "companiesEmbedding");
    value2.put("member_feature", null); // Null value instead of a list

    Map<Integer, GenericRecord> valuesByKey = new HashMap<>(2);
    valuesByKey.put(key1, value1);
    valuesByKey.put(key2, value2);
    PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
        veniceCluster.getPubSubBrokerWrapper().getPubSubClientsFactory().getProducerAdapterFactory();
    VeniceWriterFactory vwFactory = IntegrationTestPushUtils
        .getVeniceWriterFactory(veniceCluster.getPubSubBrokerWrapper(), pubSubProducerAdapterFactory);
    try (
        VeniceWriter<Object, byte[], byte[]> veniceWriter = vwFactory
            .createVeniceWriter(new VeniceWriterOptions.Builder(topic).setKeyPayloadSerializer(keySerializer).build());
        AvroGenericStoreClient<Integer, Object> storeClient = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerAddr).setUseFastAvro(false))) {

      // Write synthetic value records to the store
      pushRecordsToStore(topic, valuesByKey, veniceWriter, valueSerializer, 1);

      Set<Integer> keySet = Utils.setOf(key1, key2);

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
        Map<Integer, ComputeGenericRecord> computeResult = storeClient.compute()
            .dotProduct("member_feature", memberFeatureEmbedding, "dot_product_result")
            .hadamardProduct("member_feature", memberFeatureEmbedding, "hadamard_product_result")
            .cosineSimilarity("member_feature", memberFeatureEmbedding, "cosine_similarity_result")
            .execute(keySet)
            .get();

        // Expect no error
        computeResult.forEach(
            (key, value) -> Assert.assertEquals(
                ((HashMap<String, String>) value.get(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME)).size(),
                0));
        // Results for key 2 should be all null since the nullable field in the value of key 2 is null
        Assert.assertNull(computeResult.get(key2).get("dot_product_result"));
        Assert.assertNull(computeResult.get(key2).get("hadamard_product_result"));
        Assert.assertNull(computeResult.get(key2).get("cosine_similarity_result"));

        // Results for key 1 should be non-null since the nullable field in the value of key 1 is non-null
        Assert.assertEquals(
            computeResult.get(key1).get("dot_product_result"),
            ComputeUtils.dotProduct(memberFeatureEmbedding, memberFeatureEmbedding));
        Assert.assertEquals(
            computeResult.get(key1).get("hadamard_product_result"),
            ComputeUtils.hadamardProduct(memberFeatureEmbedding, memberFeatureEmbedding));
        Assert.assertEquals(computeResult.get(key1).get("cosine_similarity_result"), 1.0f); // Cosine similarity between
                                                                                            // a vector and itself is
                                                                                            // 1.0

        // Count on a null field should fail
        Exception expectedException = null;
        try {
          computeResult = storeClient.compute().count("member_feature", "count_result").execute(keySet).get();
        } catch (Exception e) {
          expectedException = e;
        }
        Assert.assertNotNull(expectedException);
        Assert.assertTrue(expectedException instanceof VeniceClientException);
        Assert.assertEquals(((VeniceClientException) expectedException).getErrorType(), ErrorType.GENERAL_ERROR);
        Assert.assertEquals(expectedException.getMessage(), "COUNT field: member_feature isn't 'ARRAY' or 'MAP' type");
      });
    }
  }

  private void pushRecordsToStore(
      String topic,
      Map<Integer, GenericRecord> valuesByKey,
      VeniceWriter<Object, byte[], byte[]> veniceWriter,
      VeniceKafkaSerializer serializer,
      int valueSchemaId) throws Exception {
    veniceWriter.broadcastStartOfPush(false, false, CompressionStrategy.NO_OP, Collections.emptyMap());

    for (Map.Entry<Integer, GenericRecord> keyValue: valuesByKey.entrySet()) {
      byte[] compressedValue = compressorFactory.getCompressor(CompressionStrategy.NO_OP)
          .compress(serializer.serialize(topic, keyValue.getValue()));
      veniceWriter.put(keyValue.getKey(), compressedValue, valueSchemaId).get();
    }
    // Write end of push message to make node become ONLINE from BOOTSTRAP
    veniceWriter.broadcastEndOfPush(Collections.emptyMap());
  }

  private void pushSyntheticDataToStore(
      String topic,
      int numOfRecords,
      VeniceWriter<Object, byte[], byte[]> veniceWriter,
      int pushVersion,
      String schema,
      VeniceKafkaSerializer serializer,
      boolean skip,
      int valueSchemaId) throws Exception {
    veniceWriter.broadcastStartOfPush(false, false, CompressionStrategy.NO_OP, new HashMap<>());
    Schema valueSchema = Schema.parse(schema);
    // Insert test record and wait synchronously for it to succeed
    for (int i = 0; i < numOfRecords; ++i) {
      GenericRecord value = new GenericData.Record(valueSchema);
      value.put("id", VALUE_PREFIX + i);
      value.put("name", "companiesEmbedding");
      if (!skip) {
        value.put("companiesEmbedding", COMPANIES_EMBEDDING);
      }
      value.put("member_feature", MF_EMBEDDING);
      byte[] compressedValue =
          compressorFactory.getCompressor(CompressionStrategy.NO_OP).compress(serializer.serialize(topic, value));
      veniceWriter.put(i, compressedValue, valueSchemaId).get();
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

  private static List<Float> generateRandomFloatList(int listSize) {
    ThreadLocalRandom rand = ThreadLocalRandom.current();
    List<Float> feature = new ArrayList<>(listSize);
    for (int i = 0; i < listSize; i++) {
      feature.add(rand.nextFloat());
    }
    return feature;
  }

  /**
   * Test countGroupByValue aggregation functionality.
   * This test creates a store with array and map fields, then performs
   * count group by value aggregation on these fields.
   */
  @Test(timeOut = TIMEOUT)
  public void testCountGroupByValueAggregation() throws Exception {
    String keySchema = "\"int\"";
    String valueSchemaWithArrayAndMap = "{" + "  \"namespace\": \"example.aggregation\",    "
        + "  \"type\": \"record\",        " + "  \"name\": \"UserProfile\",       " + "  \"fields\": [        "
        + "         { \"name\": \"id\", \"type\": \"string\" },             "
        + "         { \"name\": \"name\", \"type\": \"string\" },           "
        + "         { \"name\": \"skills\", \"type\": { \"type\": \"array\", \"items\": \"string\" } },        "
        + "         { \"name\": \"preferences\", \"type\": { \"type\": \"map\", \"values\": \"string\" } }        "
        + "  ]       " + " }       ";

    VeniceAvroKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(keySchema);
    VeniceAvroKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(valueSchemaWithArrayAndMap);

    // Create store with a version
    VersionCreationResponse creationResponse = veniceCluster.getNewStoreVersion(keySchema, valueSchemaWithArrayAndMap);
    Assert.assertFalse(creationResponse.isError());
    final String topic = creationResponse.getKafkaTopic();
    final String storeName = Version.parseStoreFromKafkaTopicName(creationResponse.getKafkaTopic());

    // Update the store to enable read compute
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setCompressionStrategy(CompressionStrategy.NO_OP);
    params.setReadComputationEnabled(true);
    params.setChunkingEnabled(false);
    ControllerResponse controllerResponse = veniceCluster.updateStore(storeName, params);
    Assert.assertFalse(controllerResponse.isError());

    // Create test data
    Schema valueSchema = Schema.parse(valueSchemaWithArrayAndMap);
    Map<Integer, GenericRecord> valuesByKey = new HashMap<>();

    // User 1: skills=[Java, Python, Java], preferences={theme: dark_mode, language: english}
    GenericRecord value1 = new GenericData.Record(valueSchema);
    value1.put("id", "1");
    value1.put("name", "Alice");
    value1.put("skills", Arrays.asList("Java", "Python", "Java"));
    Map<CharSequence, CharSequence> prefs1 = new HashMap<>();
    prefs1.put("theme", "dark_mode");
    prefs1.put("language", "english");
    value1.put("preferences", prefs1);
    valuesByKey.put(1, value1);

    // User 2: skills=[Python, JavaScript], preferences={theme: light_mode, language: english}
    GenericRecord value2 = new GenericData.Record(valueSchema);
    value2.put("id", "2");
    value2.put("name", "Bob");
    value2.put("skills", Arrays.asList("Python", "JavaScript"));
    Map<CharSequence, CharSequence> prefs2 = new HashMap<>();
    prefs2.put("theme", "light_mode");
    prefs2.put("language", "english");
    value2.put("preferences", prefs2);
    valuesByKey.put(2, value2);

    // User 3: skills=[Java, JavaScript, Go], preferences={theme: dark_mode, language: spanish}
    GenericRecord value3 = new GenericData.Record(valueSchema);
    value3.put("id", "3");
    value3.put("name", "Charlie");
    value3.put("skills", Arrays.asList("Java", "JavaScript", "Go"));
    Map<CharSequence, CharSequence> prefs3 = new HashMap<>();
    prefs3.put("theme", "dark_mode");
    prefs3.put("language", "spanish");
    value3.put("preferences", prefs3);
    valuesByKey.put(3, value3);

    PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
        veniceCluster.getPubSubBrokerWrapper().getPubSubClientsFactory().getProducerAdapterFactory();
    VeniceWriterFactory vwFactory = IntegrationTestPushUtils
        .getVeniceWriterFactory(veniceCluster.getPubSubBrokerWrapper(), pubSubProducerAdapterFactory);

    try (
        VeniceWriter<Object, byte[], byte[]> veniceWriter = vwFactory
            .createVeniceWriter(new VeniceWriterOptions.Builder(topic).setKeyPayloadSerializer(keySerializer).build());
        AvroGenericStoreClient<Integer, Object> storeClient = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerAddr))) {

      // Cast to AvroGenericReadComputeStoreClient to access computeAggregation method
      AvroGenericReadComputeStoreClient<Integer, Object> computeStoreClient =
          (AvroGenericReadComputeStoreClient<Integer, Object>) storeClient;

      // Write test data to the store
      pushRecordsToStore(topic, valuesByKey, veniceWriter, valueSerializer, 1);

      Set<Integer> keySet = new HashSet<>(Arrays.asList(1, 2, 3));

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
        // Test countGroupByValue on skills array field
        com.linkedin.venice.client.store.ComputeAggregationResponse skillsAggResponse =
            computeStoreClient.computeAggregation().countGroupByValue(3, "skills").execute(keySet).get();

        Map<String, Integer> skillCounts = skillsAggResponse.getValueToCount("skills");
        Assert.assertNotNull(skillCounts);

        // Debug: Print the actual counts
        System.out.println("Skill counts: " + skillCounts);

        // Check that we have some results
        Assert.assertTrue(skillCounts.size() > 0, "Skill counts should not be empty");

        // Expected counts: Java=3 (2 from user1 + 1 from user3), Python=2, JavaScript=2, Go=1
        // Since we asked for top 3, we should have at most 3 entries
        Assert.assertTrue(skillCounts.size() <= 3, "Should have at most top 3 entries");

        // Verify specific count values
        Assert.assertEquals(skillCounts.get("Java"), Integer.valueOf(3), "Java count should be 3");
        Assert.assertEquals(skillCounts.get("Python"), Integer.valueOf(2), "Python count should be 2");
        Assert.assertEquals(skillCounts.get("JavaScript"), Integer.valueOf(2), "JavaScript count should be 2");

        // Go should not be included since we only asked for top 3 and it has count=1
        Assert.assertFalse(skillCounts.containsKey("Go"), "Go should not be included in top 3");

        // Verify ordering (Java should be first with highest count)
        List<Map.Entry<String, Integer>> skillEntries = new ArrayList<>(skillCounts.entrySet());
        Assert.assertEquals(skillEntries.get(0).getKey(), "Java", "Java should be first (highest count)");
        Assert.assertEquals(skillEntries.get(0).getValue(), Integer.valueOf(3), "Java should have count 3");

        // Test countGroupByValue on preferences map field
        com.linkedin.venice.client.store.ComputeAggregationResponse prefsAggResponse =
            computeStoreClient.computeAggregation().countGroupByValue(5, "preferences").execute(keySet).get();

        Map<String, Integer> prefCounts = prefsAggResponse.getValueToCount("preferences");
        Assert.assertNotNull(prefCounts);

        // Debug: Print the actual counts
        System.out.println("Preference counts: " + prefCounts);

        // Check that we have some results
        Assert.assertTrue(prefCounts.size() > 0, "Preference counts should not be empty");

        // Expected counts: dark_mode=2, english=2, light_mode=1, spanish=1
        // Since we asked for top 5, we should have all 4 entries
        Assert.assertTrue(prefCounts.size() <= 5, "Should have at most top 5 entries");
        Assert.assertEquals(prefCounts.size(), 4, "Should have exactly 4 preference values");

        // Verify specific count values
        Assert.assertEquals(prefCounts.get("dark_mode"), Integer.valueOf(2), "dark_mode count should be 2");
        Assert.assertEquals(prefCounts.get("english"), Integer.valueOf(2), "english count should be 2");
        Assert.assertEquals(prefCounts.get("light_mode"), Integer.valueOf(1), "light_mode count should be 1");
        Assert.assertEquals(prefCounts.get("spanish"), Integer.valueOf(1), "spanish count should be 1");
      });
    }
  }
}
