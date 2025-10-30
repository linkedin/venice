package com.linkedin.venice.storagenode;

import static com.linkedin.venice.VeniceConstants.VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericReadComputeStoreClient;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.ComputeAggregationResponse;
import com.linkedin.venice.client.store.ComputeGenericRecord;
import com.linkedin.venice.client.store.predicate.DoublePredicate;
import com.linkedin.venice.client.store.predicate.FloatPredicate;
import com.linkedin.venice.client.store.predicate.IntPredicate;
import com.linkedin.venice.client.store.predicate.LongPredicate;
import com.linkedin.venice.client.store.predicate.Predicate;
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

  // Field constants for better readability
  private static final String JOB_TYPE_FIELD = "jobType";
  private static final String LOCATION_FIELD = "location";
  private static final String ID_FIELD = "id";

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
        Assert.assertTrue(
            expectedException.getMessage().contains("COUNT field: member_feature isn't 'ARRAY' or 'MAP' type"));
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
   * This test creates a store with simple string fields, then performs
   * count group by value aggregation on these fields.
   */
  @Test(timeOut = TIMEOUT)
  public void testCountGroupByValueAggregation() throws Exception {
    String keySchema = "\"int\"";
    String valueSchemaSimple = "{" + "  \"namespace\": \"example.aggregation\",    " + "  \"type\": \"record\",        "
        + "  \"name\": \"JobProfile\",       " + "  \"fields\": [        "
        + "         { \"name\": \"id\", \"type\": \"string\" },             "
        + "         { \"name\": \"jobType\", \"type\": \"string\" },           "
        + "         { \"name\": \"location\", \"type\": \"string\" }        " + "  ]       " + " }       ";

    VeniceAvroKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(keySchema);
    VeniceAvroKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(valueSchemaSimple);

    // Create store with a version
    VersionCreationResponse creationResponse = veniceCluster.getNewStoreVersion(keySchema, valueSchemaSimple);
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
    Schema valueSchema = Schema.parse(valueSchemaSimple);
    Map<Integer, GenericRecord> valuesByKey = new HashMap<>();

    // Create test job records using helper method
    valuesByKey.put(1, createJobRecord(valueSchema, "1", "full-time", "remote"));
    valuesByKey.put(2, createJobRecord(valueSchema, "2", "part-time", "onsite"));
    valuesByKey.put(3, createJobRecord(valueSchema, "3", "full-time", "remote"));
    valuesByKey.put(4, createJobRecord(valueSchema, "4", "part-time", "hybrid"));
    valuesByKey.put(5, createJobRecord(valueSchema, "5", "full-time", "remote"));
    valuesByKey.put(6, createJobRecord(valueSchema, "6", "part-time", "onsite"));

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

      Set<Integer> keySet = new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6));

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
        // Test single field aggregation: countGroupByValue on jobType field
        ComputeAggregationResponse jobTypeAggResponse =
            computeStoreClient.computeAggregation().countGroupByValue(2, JOB_TYPE_FIELD).execute(keySet).get();

        Map<String, Integer> jobTypeCounts = jobTypeAggResponse.getValueToCount(JOB_TYPE_FIELD);
        Assert.assertNotNull(jobTypeCounts);

        // Expected counts: full-time=3, part-time=3
        Assert.assertEquals(jobTypeCounts.size(), 2, "Should have exactly 2 jobType values");
        Assert.assertEquals(jobTypeCounts.get("full-time"), Integer.valueOf(3), "full-time count should be 3");
        Assert.assertEquals(jobTypeCounts.get("part-time"), Integer.valueOf(3), "part-time count should be 3");

        // Test single field aggregation: countGroupByValue on location field, topK=2
        ComputeAggregationResponse locationAggResponse =
            computeStoreClient.computeAggregation().countGroupByValue(2, LOCATION_FIELD).execute(keySet).get();

        Map<String, Integer> locationCounts = locationAggResponse.getValueToCount(LOCATION_FIELD);
        Assert.assertNotNull(locationCounts);

        // Expected: remote=3, onsite=2 (hybrid=1 should be excluded by topK=2)
        Assert.assertEquals(locationCounts.size(), 2, "Should have exactly 2 location values");
        Assert.assertEquals(locationCounts.get("remote"), Integer.valueOf(3), "remote count should be 3");
        Assert.assertEquals(locationCounts.get("onsite"), Integer.valueOf(2), "onsite count should be 2");
        Assert.assertFalse(locationCounts.containsKey("hybrid"), "hybrid should not be included in top 2");

        // Test multi-field aggregation: countGroupByValue on both jobType and location fields
        ComputeAggregationResponse multiFieldAggResponse = computeStoreClient.computeAggregation()
            .countGroupByValue(3, JOB_TYPE_FIELD, LOCATION_FIELD)
            .execute(keySet)
            .get();

        // Verify jobType aggregation results from multi-field response
        Map<String, Integer> multiFieldJobTypeCounts = multiFieldAggResponse.getValueToCount(JOB_TYPE_FIELD);
        Assert.assertNotNull(multiFieldJobTypeCounts);
        Assert.assertEquals(multiFieldJobTypeCounts.size(), 2, "Multi-field jobType should have exactly 2 values");
        Assert.assertEquals(
            multiFieldJobTypeCounts.get("full-time"),
            Integer.valueOf(3),
            "Multi-field full-time count should be 3");
        Assert.assertEquals(
            multiFieldJobTypeCounts.get("part-time"),
            Integer.valueOf(3),
            "Multi-field part-time count should be 3");

        // Verify location aggregation results from multi-field response
        Map<String, Integer> multiFieldLocationCounts = multiFieldAggResponse.getValueToCount(LOCATION_FIELD);
        Assert.assertNotNull(multiFieldLocationCounts);
        Assert.assertEquals(multiFieldLocationCounts.size(), 3, "Multi-field location should have exactly 3 values");
        Assert.assertEquals(
            multiFieldLocationCounts.get("remote"),
            Integer.valueOf(3),
            "Multi-field remote count should be 3");
        Assert.assertEquals(
            multiFieldLocationCounts.get("onsite"),
            Integer.valueOf(2),
            "Multi-field onsite count should be 2");
        Assert.assertEquals(
            multiFieldLocationCounts.get("hybrid"),
            Integer.valueOf(1),
            "Multi-field hybrid count should be 1");
      });
    }
  }

  /**
   * Test countGroupByBucket aggregation functionality.
   * This test creates a store with numeric fields, then performs
   * count group by bucket aggregation on these fields using different predicates.
   * Covers all available predicate methods for comprehensive testing.
   */
  @Test(timeOut = TIMEOUT)
  public void testCountGroupByBucketAggregation() throws Exception {
    String keySchema = "\"int\"";
    String valueSchemaWithNumericFields = "{" + "  \"namespace\": \"example.bucket.aggregation\",    "
        + "  \"type\": \"record\",        " + "  \"name\": \"EmployeeProfile\",       " + "  \"fields\": [        "
        + "         { \"name\": \"id\", \"type\": \"string\" },             "
        + "         { \"name\": \"name\", \"type\": \"string\" },           "
        + "         { \"name\": \"salary\", \"type\": \"float\" },        "
        + "         { \"name\": \"age\", \"type\": \"int\" },        "
        + "         { \"name\": \"score\", \"type\": \"double\" },        "
        + "         { \"name\": \"joinDate\", \"type\": \"long\" },        "
        + "         { \"name\": \"department\", \"type\": \"string\" }        " + "  ]       " + " }       ";

    VeniceAvroKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(keySchema);
    VeniceAvroKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(valueSchemaWithNumericFields);

    // Create store with a version
    VersionCreationResponse creationResponse =
        veniceCluster.getNewStoreVersion(keySchema, valueSchemaWithNumericFields);
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
    Schema valueSchema = Schema.parse(valueSchemaWithNumericFields);
    Map<Integer, GenericRecord> valuesByKey = new HashMap<>();

    // Create test employee records using helper method
    // Employee 1: salary=45000.0f, age=25, score=85.5, joinDate=1609459200000L (2021-01-01), department="Engineering"
    valuesByKey
        .put(1, createEmployeeRecord(valueSchema, "1", "Alice", 45000.0f, 25, 85.5, 1609459200000L, "Engineering"));

    // Employee 2: salary=75000.0f, age=35, score=92.0, joinDate=1640995200000L (2022-01-01), department="Sales"
    valuesByKey.put(2, createEmployeeRecord(valueSchema, "2", "Bob", 75000.0f, 35, 92.0, 1640995200000L, "Sales"));

    // Employee 3: salary=55000.0f, age=28, score=78.5, joinDate=1672531200000L (2023-01-01), department="Engineering"
    valuesByKey
        .put(3, createEmployeeRecord(valueSchema, "3", "Charlie", 55000.0f, 28, 78.5, 1672531200000L, "Engineering"));

    // Employee 4: salary=95000.0f, age=42, score=88.0, joinDate=1704067200000L (2024-01-01), department="Marketing"
    valuesByKey
        .put(4, createEmployeeRecord(valueSchema, "4", "Diana", 95000.0f, 42, 88.0, 1704067200000L, "Marketing"));

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

      Set<Integer> keySet = new HashSet<>(Arrays.asList(1, 2, 3, 4));

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
        // Test countGroupByBucket on salary field with FloatPredicate - all methods
        Map<String, Predicate<Float>> salaryBuckets = new HashMap<>();
        salaryBuckets.put("low", FloatPredicate.lowerThan(50000.0f));
        salaryBuckets.put("medium", FloatPredicate.greaterOrEquals(50000.0f));
        salaryBuckets.put("high", FloatPredicate.greaterThan(80000.0f));
        salaryBuckets.put("equal_55000", FloatPredicate.equalTo(55000.0f, 1000.0f));
        salaryBuckets.put("any_of_45000_75000", FloatPredicate.anyOf(45000.0f, 75000.0f));

        ComputeAggregationResponse salaryAggResponse =
            computeStoreClient.computeAggregation().countGroupByBucket(salaryBuckets, "salary").execute(keySet).get();

        Map<String, Integer> salaryBucketCounts = salaryAggResponse.getBucketNameToCount("salary");
        Assert.assertNotNull(salaryBucketCounts);

        // Salary (float) bucket counts
        // Data: 45000, 75000, 55000, 95000
        // low: <50000 -> 45000 (1)
        // medium: >=50000 -> 75000, 55000, 95000 (3)
        // high: >80000 -> 95000 (1)
        // equal_55000: ==55000 -> 55000 (1)
        // any_of_45000_75000: 45000, 75000 (2)
        Assert.assertEquals(salaryBucketCounts.get("low"), Integer.valueOf(1), "Low salary count should be 1");
        Assert.assertEquals(salaryBucketCounts.get("medium"), Integer.valueOf(3), "Medium salary count should be 3");
        Assert.assertEquals(salaryBucketCounts.get("high"), Integer.valueOf(1), "High salary count should be 1");
        Assert.assertEquals(
            salaryBucketCounts.get("equal_55000"),
            Integer.valueOf(1),
            "Equal 55000 salary count should be 1");
        Assert.assertEquals(
            salaryBucketCounts.get("any_of_45000_75000"),
            Integer.valueOf(2),
            "Any of 45000,75000 salary count should be 2");

        // Test countGroupByBucket on age field with IntPredicate - all methods
        Map<String, Predicate<Integer>> ageBuckets = new HashMap<>();
        ageBuckets.put("young", IntPredicate.lowerThan(30));
        ageBuckets.put("mid_career", IntPredicate.greaterOrEquals(30));
        ageBuckets.put("senior", IntPredicate.greaterThan(40));
        ageBuckets.put("equal_35", IntPredicate.equalTo(35));
        ageBuckets.put("lower_or_equal_35", IntPredicate.lowerOrEquals(35));
        ageBuckets.put("any_of_25_42", IntPredicate.anyOf(25, 42));

        ComputeAggregationResponse ageAggResponse =
            computeStoreClient.computeAggregation().countGroupByBucket(ageBuckets, "age").execute(keySet).get();

        Map<String, Integer> ageBucketCounts = ageAggResponse.getBucketNameToCount("age");
        Assert.assertNotNull(ageBucketCounts);

        // Age (int) bucket counts
        // Data: 25, 35, 28, 42
        // young: <30 -> 25, 28 (2)
        // mid_career: >=30 -> 35, 42 (2)
        // senior: >40 -> 42 (1)
        // equal_35: ==35 -> 35 (1)
        // lower_or_equal_35: <=35 -> 25, 28, 35 (3)
        // any_of_25_42: 25, 42 (2)
        Assert.assertEquals(ageBucketCounts.get("young"), Integer.valueOf(2), "Young age count should be 2");
        Assert.assertEquals(ageBucketCounts.get("mid_career"), Integer.valueOf(2), "Mid career age count should be 2");
        Assert.assertEquals(ageBucketCounts.get("senior"), Integer.valueOf(1), "Senior age count should be 1");
        Assert.assertEquals(ageBucketCounts.get("equal_35"), Integer.valueOf(1), "Equal 35 age count should be 1");
        Assert.assertEquals(
            ageBucketCounts.get("lower_or_equal_35"),
            Integer.valueOf(3),
            "Lower or equal 35 age count should be 3");
        Assert.assertEquals(
            ageBucketCounts.get("any_of_25_42"),
            Integer.valueOf(2),
            "Any of 25,42 age count should be 2");

        // Test countGroupByBucket on score field with DoublePredicate - all methods
        Map<String, Predicate<Double>> scoreBuckets = new HashMap<>();
        scoreBuckets.put("excellent", DoublePredicate.greaterThan(90.0));
        scoreBuckets.put("good", DoublePredicate.greaterThan(80.0));
        scoreBuckets.put("average", DoublePredicate.lowerOrEquals(80.0));
        scoreBuckets.put("equal_85_5", DoublePredicate.equalTo(85.5, 0.1));
        scoreBuckets.put("lower_or_equal_92", DoublePredicate.lowerOrEquals(92.0));
        scoreBuckets.put("any_of_78_5_92_0", DoublePredicate.anyOf(78.5, 92.0));

        ComputeAggregationResponse scoreAggResponse =
            computeStoreClient.computeAggregation().countGroupByBucket(scoreBuckets, "score").execute(keySet).get();

        Map<String, Integer> scoreBucketCounts = scoreAggResponse.getBucketNameToCount("score");
        Assert.assertNotNull(scoreBucketCounts);

        // Score (double) bucket counts
        // Data: 85.5, 92.0, 78.5, 88.0
        // excellent: >90.0 -> 92.0 (1)
        // good: >80.0 -> 85.5, 92.0, 88.0 (3)
        // average: <=80.0 -> 78.5 (1)
        // equal_85_5: ==85.5 -> 85.5 (1)
        // lower_or_equal_92: <=92.0 -> 85.5, 78.5, 88.0, 92.0 (4)
        // any_of_78_5_92_0: 78.5, 92.0 (2)
        Assert
            .assertEquals(scoreBucketCounts.get("excellent"), Integer.valueOf(1), "Excellent score count should be 1");
        Assert.assertEquals(scoreBucketCounts.get("good"), Integer.valueOf(3), "Good score count should be 3");
        Assert.assertEquals(scoreBucketCounts.get("average"), Integer.valueOf(1), "Average score count should be 1");
        Assert.assertEquals(
            scoreBucketCounts.get("equal_85_5"),
            Integer.valueOf(1),
            "Equal 85.5 score count should be 1");
        Assert.assertEquals(
            scoreBucketCounts.get("lower_or_equal_92"),
            Integer.valueOf(4),
            "Lower or equal 92.0 score count should be 4");
        Assert.assertEquals(
            scoreBucketCounts.get("any_of_78_5_92_0"),
            Integer.valueOf(2),
            "Any of 78.5,92.0 score count should be 2");

        // Test countGroupByBucket on joinDate field with LongPredicate - all methods
        Map<String, Predicate<Long>> joinDateBuckets = new HashMap<>();
        joinDateBuckets.put("early", LongPredicate.lowerThan(1640995200000L)); // Before
                                                                               // 2022
        joinDateBuckets.put("recent", LongPredicate.greaterOrEquals(1640995200000L)); // 2022
                                                                                      // and
                                                                                      // later
        joinDateBuckets.put("very_recent", LongPredicate.greaterThan(1672531200000L)); // After
                                                                                       // 2023
        joinDateBuckets.put("equal_2022", LongPredicate.equalTo(1640995200000L));
        joinDateBuckets.put("lower_or_equal_1640995200000", LongPredicate.lowerOrEquals(1640995200000L));
        joinDateBuckets.put("any_of_1609459200000_1704067200000", LongPredicate.anyOf(1609459200000L, 1704067200000L));

        ComputeAggregationResponse joinDateAggResponse = computeStoreClient.computeAggregation()
            .countGroupByBucket(joinDateBuckets, "joinDate")
            .execute(keySet)
            .get();

        Map<String, Integer> joinDateBucketCounts = joinDateAggResponse.getBucketNameToCount("joinDate");
        Assert.assertNotNull(joinDateBucketCounts);

        // Join date (long) bucket counts
        // Data: 1609459200000L, 1640995200000L, 1672531200000L, 1704067200000L
        // early: <1640995200000L -> 1609459200000L (1)
        // recent: >=1640995200000L -> 1640995200000L, 1672531200000L, 1704067200000L (3)
        // very_recent: >1672531200000L -> 1704067200000L (1)
        // equal_2022: ==1640995200000L -> 1640995200000L (1)
        // lower_or_equal_1640995200000: <=1640995200000L -> 1609459200000L, 1640995200000L (2)
        // any_of_1609459200000_1704067200000: 1609459200000L, 1704067200000L (2)
        Assert.assertEquals(joinDateBucketCounts.get("early"), Integer.valueOf(1), "Early join date count should be 1");
        Assert
            .assertEquals(joinDateBucketCounts.get("recent"), Integer.valueOf(3), "Recent join date count should be 3");
        Assert.assertEquals(
            joinDateBucketCounts.get("very_recent"),
            Integer.valueOf(1),
            "Very recent join date count should be 1");
        Assert.assertEquals(
            joinDateBucketCounts.get("equal_2022"),
            Integer.valueOf(1),
            "Equal 2022 join date count should be 1");
        Assert.assertEquals(
            joinDateBucketCounts.get("lower_or_equal_1640995200000"),
            Integer.valueOf(2),
            "Lower or equal 1640995200000 join date count should be 2");
        Assert.assertEquals(
            joinDateBucketCounts.get("any_of_1609459200000_1704067200000"),
            Integer.valueOf(2),
            "Any of 1609459200000,1704067200000 join date count should be 2");

        // Test countGroupByBucket on department field with StringPredicate - all methods
        Map<String, Predicate<String>> departmentBuckets = new HashMap<>();
        departmentBuckets.put("engineering", Predicate.equalTo("Engineering"));
        departmentBuckets.put("any_of_eng_sales", Predicate.anyOf("Engineering", "Sales"));

        ComputeAggregationResponse departmentAggResponse = computeStoreClient.computeAggregation()
            .countGroupByBucket(departmentBuckets, "department")
            .execute(keySet)
            .get();

        Map<String, Integer> departmentBucketCounts = departmentAggResponse.getBucketNameToCount("department");
        Assert.assertNotNull(departmentBucketCounts);

        // Expected counts: engineering=2 (Engineering appears twice), any_of_eng_sales=3 (Engineering twice + Sales
        // once)
        Assert.assertEquals(
            departmentBucketCounts.get("engineering"),
            Integer.valueOf(2),
            "Engineering count should be 2");
        Assert.assertEquals(
            departmentBucketCounts.get("any_of_eng_sales"),
            Integer.valueOf(3),
            "Any of Engineering,Sales count should be 3");

        // Test complex predicate combinations
        Map<String, Predicate<Integer>> complexBuckets = new HashMap<>();
        complexBuckets.put("and_combination", Predicate.and(IntPredicate.greaterThan(20), IntPredicate.lowerThan(40)));
        complexBuckets.put("or_combination", Predicate.or(IntPredicate.equalTo(25), IntPredicate.equalTo(42)));

        ComputeAggregationResponse complexAggResponse =
            computeStoreClient.computeAggregation().countGroupByBucket(complexBuckets, "age").execute(keySet).get();

        Map<String, Integer> complexBucketCounts = complexAggResponse.getBucketNameToCount("age");
        Assert.assertNotNull(complexBucketCounts);

        // Expected: and_combination=3 (25, 28, 35), or_combination=2 (25, 42)
        Assert.assertEquals(
            complexBucketCounts.get("and_combination"),
            Integer.valueOf(3),
            "And combination count should be 3");
        Assert.assertEquals(
            complexBucketCounts.get("or_combination"),
            Integer.valueOf(2),
            "Or combination count should be 2");
      });
    }
  }

  /**
   * Creates a job record with the given parameters.
   * This helper method reduces code duplication when creating test data.
   */
  private GenericRecord createJobRecord(Schema valueSchema, String id, String jobType, String location) {
    GenericRecord record = new GenericData.Record(valueSchema);
    record.put(ID_FIELD, id);
    record.put(JOB_TYPE_FIELD, jobType);
    record.put(LOCATION_FIELD, location);
    return record;
  }

  /**
   * Creates an employee record with the given parameters.
   * This helper method reduces code duplication when creating test data.
   */
  private GenericRecord createEmployeeRecord(
      Schema valueSchema,
      String id,
      String name,
      float salary,
      int age,
      double score,
      long joinDate,
      String department) {
    GenericRecord record = new GenericData.Record(valueSchema);
    record.put("id", id);
    record.put("name", name);
    record.put("salary", salary);
    record.put("age", age);
    record.put("score", score);
    record.put("joinDate", joinDate);
    record.put("department", department);
    return record;
  }

  /**
   * Test error handling when using unsupported predicates.
   * This test verifies that the client properly throws exceptions when
   * encountering predicates that cannot be handled by the aggregation system.
   */
  @Test(timeOut = TIMEOUT)
  public void testUnsupportedPredicateErrorHandling() throws Exception {
    String keySchema = "\"int\"";
    String valueSchemaWithNumericFields = "{" + "  \"namespace\": \"example.error.testing\",    "
        + "  \"type\": \"record\",        " + "  \"name\": \"EmployeeProfile\",       " + "  \"fields\": [        "
        + "         { \"name\": \"id\", \"type\": \"string\" },             "
        + "         { \"name\": \"name\", \"type\": \"string\" },           "
        + "         { \"name\": \"salary\", \"type\": \"float\" },        "
        + "         { \"name\": \"age\", \"type\": \"int\" },        "
        + "         { \"name\": \"score\", \"type\": \"double\" },        "
        + "         { \"name\": \"joinDate\", \"type\": \"long\" },        "
        + "         { \"name\": \"department\", \"type\": \"string\" }        " + "  ]       " + " }       ";

    VeniceAvroKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(keySchema);
    VeniceAvroKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(valueSchemaWithNumericFields);

    // Create store with a version
    VersionCreationResponse creationResponse =
        veniceCluster.getNewStoreVersion(keySchema, valueSchemaWithNumericFields);
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
    Schema valueSchema = Schema.parse(valueSchemaWithNumericFields);
    Map<Integer, GenericRecord> valuesByKey = new HashMap<>();

    // Create a simple test record
    valuesByKey
        .put(1, createEmployeeRecord(valueSchema, "1", "Alice", 45000.0f, 25, 85.5, 1609459200000L, "Engineering"));

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

      Set<Integer> keySet = new HashSet<>(Arrays.asList(1));

      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
        // Test 1: Try to use a predicate on a non-existent field
        Map<String, Predicate<String>> invalidFieldBuckets = new HashMap<>();
        invalidFieldBuckets.put("invalid", Predicate.equalTo("test"));

        try {
          computeStoreClient.computeAggregation()
              .countGroupByBucket(invalidFieldBuckets, "nonExistentField")
              .execute(keySet)
              .get();
          Assert.fail("Should have thrown an exception for non-existent field");
        } catch (Exception e) {
          // Expected to throw an exception
          Assert.assertTrue(
              e instanceof VeniceClientException,
              "Should throw VeniceClientException for non-existent field");
          Assert.assertTrue(
              e.getMessage().contains("nonExistentField") || e.getMessage().contains("field not found"),
              "Exception message should mention the non-existent field");
        }

        // Test 2: Try to use a predicate type that doesn't match the field type
        Map<String, Predicate<Integer>> typeMismatchBuckets = new HashMap<>();
        typeMismatchBuckets.put("mismatch", IntPredicate.equalTo(25));

        try {
          computeStoreClient.computeAggregation()
              .countGroupByBucket(typeMismatchBuckets, "salary") // salary is float, not int
              .execute(keySet)
              .get();
          Assert.fail("Should have thrown an exception for type mismatch");
        } catch (Exception e) {
          // Expected to throw an exception
          Assert.assertTrue(e instanceof VeniceClientException, "Should throw VeniceClientException for type mismatch");
          Assert.assertTrue(
              e.getMessage().contains("salary") || e.getMessage().contains("type"),
              "Exception message should mention the field or type mismatch");
        }

        // Test 3: Try to use an unsupported predicate combination
        Map<String, Predicate<Integer>> unsupportedBuckets = new HashMap<>();
        // Create a deeply nested predicate that might not be supported
        Predicate<Integer> complexPredicate = Predicate.and(
            IntPredicate.greaterThan(20),
            Predicate
                .or(IntPredicate.equalTo(25), Predicate.and(IntPredicate.lowerThan(30), IntPredicate.greaterThan(15))));
        unsupportedBuckets.put("complex", complexPredicate);

        try {
          computeStoreClient.computeAggregation().countGroupByBucket(unsupportedBuckets, "age").execute(keySet).get();
          // This might succeed or fail depending on implementation
          // We're testing that the system handles complex predicates gracefully
          // If it succeeds, that's fine - complex predicates are supported
        } catch (Exception e) {
          // If it fails, that's also acceptable - complex predicates might not be supported
          Assert.assertTrue(
              e instanceof VeniceClientException || e instanceof RuntimeException,
              "Should throw appropriate exception for unsupported complex predicate");
        }

        // Test 4: Try to use an empty bucket map
        Map<String, Predicate<Integer>> emptyBuckets = new HashMap<>();

        try {
          computeStoreClient.computeAggregation().countGroupByBucket(emptyBuckets, "age").execute(keySet).get();
          Assert.fail("Should have thrown an exception for empty buckets");
        } catch (Exception e) {
          // Expected to throw an exception
          Assert.assertTrue(
              e instanceof VeniceClientException || e instanceof IllegalArgumentException,
              "Should throw appropriate exception for empty buckets");
          Assert.assertTrue(
              e.getMessage().contains("empty") || e.getMessage().contains("bucket")
                  || e.getMessage().contains("invalid"),
              "Exception message should mention empty buckets or invalid input");
        }
      });
    }
  }
}
