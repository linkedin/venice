package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.VeniceConstants.VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME;
import static com.linkedin.venice.client.store.predicate.PredicateBuilder.and;
import static com.linkedin.venice.client.store.predicate.PredicateBuilder.equalTo;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static org.testng.Assert.assertThrows;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.client.AvroGenericDaVinciClient;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.NonLocalAccessPolicy;
import com.linkedin.davinci.client.StorageClass;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.ComputeGenericRecord;
import com.linkedin.venice.client.store.predicate.Predicate;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.streaming.VeniceResponseMap;
import com.linkedin.venice.compute.ComputeOperationUtils;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.DaVinciTestContext;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import io.tehuti.metrics.MetricsRepository;
import java.lang.reflect.Method;
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
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DaVinciComputeTest {
  private static final Logger LOGGER = LogManager.getLogger(DaVinciComputeTest.class);
  private static final int TEST_TIMEOUT = 120_000; // ms

  private VeniceClusterWrapper cluster;
  private D2Client d2Client;
  private final List<Float> mfEmbedding = generateRandomFloatList(100);
  private final List<Float> companiesEmbedding = generateRandomFloatList(100);
  private final List<Float> pymkCosineSimilarityEmbedding = generateRandomFloatList(100);

  private static final String KEY_PREFIX = "key_";
  private static final String VALUE_PREFIX = "id_";
  private static final int MAX_KEY_LIMIT = 1000;
  private static final String NON_EXISTING_KEY1 = "a_unknown_key";
  private static final String NON_EXISTING_KEY2 = "z_unknown_key";
  private static final int NON_EXISTING_KEY_NUM = 2;

  private static final String VALUE_SCHEMA_FOR_COMPUTE = "{" + "  \"namespace\": \"example.compute\",    "
      + "  \"type\": \"record\",        " + "  \"name\": \"MemberFeature\",       " + "  \"fields\": [        "
      + "         { \"name\": \"id\", \"type\": \"string\" },             "
      + "         { \"name\": \"name\", \"type\": \"string\" },           "
      + "         {   \"default\": [], \n  \"name\": \"companiesEmbedding\",  \"type\": {  \"items\": \"float\",  \"type\": \"array\"   }  }, "
      + "         { \"name\": \"member_feature\", \"type\": { \"type\": \"array\", \"items\": \"float\" } }        "
      + "  ]       " + " }       ";

  private static final String VALUE_SCHEMA_FOR_COMPUTE_MISSING_FIELD = "{" + "  \"namespace\": \"example.compute\",    "
      + "  \"type\": \"record\",        " + "  \"name\": \"MemberFeature\",       " + "  \"fields\": [        "
      + "         { \"name\": \"id\", \"type\": \"string\" },             "
      + "         { \"name\": \"name\", \"type\": \"string\" },           "
      + "         { \"name\": \"member_feature\", \"type\": { \"type\": \"array\", \"items\": \"float\" } }        "
      + "  ]       " + " }       ";

  private static final String VALUE_SCHEMA_FOR_COMPUTE_NULLABLE_LIST_FIELD = "{"
      + "  \"namespace\": \"example.compute\",    " + "  \"type\": \"record\",        "
      + "  \"name\": \"MemberFeature\",       " + "  \"fields\": [        "
      + "   {\"name\": \"id\", \"type\": \"string\" },             "
      + "   {\"name\": \"name\", \"type\": \"string\" },           "
      + "   {\"name\": \"member_feature\", \"type\": [\"null\",{\"type\":\"array\",\"items\":\"float\"}],\"default\": null}"
      + // nullable field
      "  ] " + " }  ";

  private static final String VALUE_SCHEMA_FOR_COMPUTE_SWAPPED = "{" + "  \"namespace\": \"example.compute\",    "
      + "  \"type\": \"record\",        " + "  \"name\": \"MemberFeature\",       " + "  \"fields\": [        "
      + "         { \"name\": \"id\", \"type\": \"string\" },             "
      + "         { \"name\": \"name\", \"type\": \"string\" },           "
      + "         { \"name\": \"member_feature\", \"type\": { \"type\": \"array\", \"items\": \"float\" } },        "
      + "         {   \"default\": [], \n  \"name\": \"companiesEmbedding\",  \"type\": {  \"items\": \"float\",  \"type\": \"array\"   }  } "
      + "  ]       " + " }       ";

  private static final String KEY_SCHEMA_STEAMING_COMPUTE = "\"string\"";

  private static final String VALUE_SCHEMA_STREAMING_COMPUTE = "{\n" + "\"type\": \"record\",\n"
      + "\"name\": \"test_value_schema\",\n" + "\"fields\": [\n" + "  {\"name\": \"int_field\", \"type\": \"int\"},\n"
      + "  {\"name\": \"float_field\", \"type\": \"float\"}\n" + "]\n" + "}";

  private static final String KEY_SCHEMA_PARTIAL_KEY_LOOKUP = "{" + "\"type\":\"record\"," + "\"name\":\"KeyRecord\","
      + "\"namespace\":\"example.partialKeyLookup\"," + "\"fields\":[ " + "   {\"name\":\"id\",\"type\":\"string\"},"
      + "   {\"name\":\"companyId\",\"type\":\"int\"}, " + "   {\"name\":\"name\",\"type\":\"string\"} " + " ]" + "}";

  @BeforeClass
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties clusterConfig = new Properties();
    clusterConfig.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);
    cluster = ServiceFactory.getVeniceCluster(1, 2, 1, 1, 100, false, false, clusterConfig);
    d2Client = new D2ClientBuilder().setZkHosts(cluster.getZk().getAddress())
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2Client);
  }

  @AfterClass
  public void cleanUp() {
    if (d2Client != null) {
      D2ClientUtils.shutdownClient(d2Client);
    }
    Utils.closeQuietlyWithErrorLogged(cluster);
  }

  @AfterMethod
  public void verifyPostConditions(Method method) {
    try {
      assertThrows(NullPointerException.class, AvroGenericDaVinciClient::getBackend);
    } catch (AssertionError e) {
      throw new AssertionError(method.getName() + " leaked DaVinciBackend.", e);
    }
  }

  // TODO This test seems to have a dependency issue. Running it gives more information, but needs some
  // refactoring
  // @Test(timeOut = TEST_TIMEOUT * 2)
  public void testComputeOnStoreWithQTFDScompliantSchema() throws Exception {
    final String storeName = Utils.getUniqueString("store");
    cluster.useControllerClient(
        client -> TestUtils.assertCommand(
            client.createNewStore(
                storeName,
                getClass().getName(),
                DEFAULT_KEY_SCHEMA,
                VALUE_SCHEMA_FOR_COMPUTE_NULLABLE_LIST_FIELD)));
    cluster.createMetaSystemStore(storeName);

    VersionCreationResponse newVersion = cluster.getNewVersion(storeName);
    String topic = newVersion.getKafkaTopic();
    VeniceWriterFactory vwFactory = TestUtils.getVeniceWriterFactory(cluster.getKafka().getAddress());

    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(DEFAULT_KEY_SCHEMA);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_FOR_COMPUTE_NULLABLE_LIST_FIELD);

    Map<Integer, ComputeGenericRecord> computeResult;
    final int key1 = 1;
    final int key2 = 2;
    Set<Integer> keySetForCompute = new HashSet<Integer>() {
      {
        add(key1);
        add(key2);
      }
    };

    DaVinciTestContext<Integer, Integer> daVinciTestContext =
        ServiceFactory.getGenericAvroDaVinciFactoryAndClientWithRetries(
            d2Client,
            new MetricsRepository(),
            Optional.empty(),
            cluster.getZk().getAddress(),
            storeName,
            new DaVinciConfig(),
            TestUtils.getIngestionIsolationPropertyMap());
    try (
        VeniceWriter<Object, Object, byte[]> veniceWriter = vwFactory.createVeniceWriter(
            new VeniceWriterOptions.Builder(topic).setKeySerializer(keySerializer)
                .setValueSerializer(valueSerializer)
                .build());
        CachingDaVinciClientFactory ignored = daVinciTestContext.getDaVinciClientFactory();
        DaVinciClient<Integer, Integer> client = daVinciTestContext.getDaVinciClient()) {

      // Write data to DaVinci Store and subscribe client
      Schema valueSchema = Schema.parse(VALUE_SCHEMA_FOR_COMPUTE_NULLABLE_LIST_FIELD);
      List<Float> memberFeatureEmbedding = Arrays.asList(1.0f, 2.0f, 3.0f);
      GenericRecord value1 = new GenericData.Record(valueSchema);
      GenericRecord value2 = new GenericData.Record(valueSchema);
      value1.put("id", "1");
      value1.put("name", "companiesEmbedding");
      value1.put("member_feature", memberFeatureEmbedding);
      value2.put("id", "2");
      value2.put("name", "companiesEmbedding");
      value2.put("member_feature", null); // Null value instead of a list
      Map<Integer, GenericRecord> valuesByKey = new HashMap<>(2);
      valuesByKey.put(key1, value1);
      valuesByKey.put(key2, value2);

      pushRecordsToStore(valuesByKey, veniceWriter, 1);
      client.subscribeAll().get();

      final Consumer<Map<Integer, ComputeGenericRecord>> assertComputeResults = (readComputeResult) -> {
        // Expect no error
        readComputeResult.forEach(
            (key, value) -> Assert.assertEquals(
                ((HashMap<String, String>) value.get(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME)).size(),
                0));
        // Results for key 2 should be all null since the nullable field in the value of key 2 is null
        Assert.assertNull(readComputeResult.get(key2).get("dot_product_result"));
        Assert.assertNull(readComputeResult.get(key2).get("hadamard_product_result"));
        Assert.assertNull(readComputeResult.get(key2).get("cosine_similarity_result"));
        // Results for key 1 should be non-null since the nullable field in the value of key 1 is non-null
        Assert.assertEquals(
            readComputeResult.get(key1).get("dot_product_result"),
            ComputeOperationUtils.dotProduct(memberFeatureEmbedding, memberFeatureEmbedding));
        Assert.assertEquals(
            readComputeResult.get(key1).get("hadamard_product_result"),
            ComputeOperationUtils.hadamardProduct(memberFeatureEmbedding, memberFeatureEmbedding));
        Assert.assertEquals(readComputeResult.get(key1).get("cosine_similarity_result"), 1.0f); // Cosine similarity
                                                                                                // between a vector and
                                                                                                // itself is 1.0
      };

      // Perform compute on client
      computeResult = client.compute()
          .dotProduct("member_feature", memberFeatureEmbedding, "dot_product_result")
          .hadamardProduct("member_feature", memberFeatureEmbedding, "hadamard_product_result")
          .cosineSimilarity("member_feature", memberFeatureEmbedding, "cosine_similarity_result")
          .execute(keySetForCompute)
          .get();

      assertComputeResults.accept(computeResult);

      computeResult = client.compute()
          .dotProduct("member_feature", memberFeatureEmbedding, "dot_product_result")
          .hadamardProduct("member_feature", memberFeatureEmbedding, "hadamard_product_result")
          .cosineSimilarity("member_feature", memberFeatureEmbedding, "cosine_similarity_result")
          .streamingExecute(keySetForCompute)
          .get();

      assertComputeResults.accept(computeResult);
      client.unsubscribeAll();
    }
  }

  @Test(timeOut = TEST_TIMEOUT * 2)
  public void testReadComputeMissingField() throws Exception {
    // Create DaVinci store
    final String storeName = Utils.getUniqueString("store");
    cluster.useControllerClient(
        client -> TestUtils.assertCommand(
            client.createNewStore(storeName, getClass().getName(), DEFAULT_KEY_SCHEMA, VALUE_SCHEMA_FOR_COMPUTE))

    );
    cluster.createMetaSystemStore(storeName);

    VersionCreationResponse newVersion = cluster.getNewVersion(storeName);
    String topic = newVersion.getKafkaTopic();
    VeniceWriterFactory vwFactory = TestUtils.getVeniceWriterFactory(cluster.getKafka().getAddress());

    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(DEFAULT_KEY_SCHEMA);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_FOR_COMPUTE);
    VeniceKafkaSerializer valueSerializerMissingField =
        new VeniceAvroKafkaSerializer(VALUE_SCHEMA_FOR_COMPUTE_MISSING_FIELD);

    int numRecords = 100;
    Map<Integer, ComputeGenericRecord> computeResult;
    Set<Integer> keySetForCompute = new HashSet<Integer>() {
      {
        add(1);
        add(2);
      }
    };

    DaVinciTestContext<Integer, Integer> daVinciTestContext =
        ServiceFactory.getGenericAvroDaVinciFactoryAndClientWithRetries(
            d2Client,
            new MetricsRepository(),
            Optional.empty(),
            cluster.getZk().getAddress(),
            storeName,
            new DaVinciConfig(),
            TestUtils.getIngestionIsolationPropertyMap());
    try (
        VeniceWriter<Object, Object, byte[]> writer = vwFactory.createVeniceWriter(
            new VeniceWriterOptions.Builder(topic).setKeySerializer(keySerializer)
                .setValueSerializer(valueSerializer)
                .build());
        CachingDaVinciClientFactory ignored = daVinciTestContext.getDaVinciClientFactory();
        DaVinciClient<Integer, Integer> client = daVinciTestContext.getDaVinciClient()) {

      // Write data to DaVinci Store and subscribe client
      pushSyntheticDataToStore(writer, VALUE_SCHEMA_FOR_COMPUTE, false, 1, numRecords);
      client.subscribeAll().get();

      /**
       * Perform compute on client with all fields
       *
       * This is necessary to add the compute result schema to {@link com.linkedin.davinci.client.AvroGenericDaVinciClient#computeResultSchemaCache}
       * and {@link com.linkedin.venice.client.store.AbstractAvroComputeRequestBuilder#RESULT_SCHEMA_CACHE}
       */
      computeResult = client.compute()
          .cosineSimilarity("companiesEmbedding", pymkCosineSimilarityEmbedding, "companiesEmbedding_score")
          .cosineSimilarity("member_feature", pymkCosineSimilarityEmbedding, "member_feature_score")
          .execute(keySetForCompute)
          .get();

      computeResult.forEach(
          (key, value) -> Assert
              .assertEquals(((HashMap<String, String>) value.get(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME)).size(), 0));

      computeResult = client.compute()
          .cosineSimilarity("companiesEmbedding", pymkCosineSimilarityEmbedding, "companiesEmbedding_score")
          .cosineSimilarity("member_feature", pymkCosineSimilarityEmbedding, "member_feature_score")
          .streamingExecute(keySetForCompute)
          .get();

      computeResult.forEach(
          (key, value) -> Assert
              .assertEquals(((HashMap<String, String>) value.get(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME)).size(), 0));

      client.unsubscribeAll();
    }

    // Add value schema for compute with a missing field
    cluster.useControllerClient(
        clientMissingField -> TestUtils
            .assertCommand(clientMissingField.addValueSchema(storeName, VALUE_SCHEMA_FOR_COMPUTE_MISSING_FIELD)));

    VersionCreationResponse newVersionMissingField = cluster.getNewVersion(storeName);
    String topicForMissingField = newVersionMissingField.getKafkaTopic();

    DaVinciTestContext<Integer, Integer> daVinciTestContext2 =
        ServiceFactory.getGenericAvroDaVinciFactoryAndClientWithRetries(
            d2Client,
            new MetricsRepository(),
            Optional.empty(),
            cluster.getZk().getAddress(),
            storeName,
            new DaVinciConfig(),
            TestUtils.getIngestionIsolationPropertyMap());

    try (
        VeniceWriter<Object, Object, byte[]> writerForMissingField = vwFactory.createVeniceWriter(
            new VeniceWriterOptions.Builder(topicForMissingField).setKeySerializer(keySerializer)
                .setValueSerializer(valueSerializerMissingField)
                .build());
        CachingDaVinciClientFactory factoryForMissingFieldClient = daVinciTestContext2.getDaVinciClientFactory();
        DaVinciClient<Integer, Integer> clientForMissingField = daVinciTestContext2.getDaVinciClient()) {

      // Write data to DaVinci store with a missing field and subscribe client
      pushSyntheticDataToStore(writerForMissingField, VALUE_SCHEMA_FOR_COMPUTE_MISSING_FIELD, true, 2, numRecords);
      clientForMissingField.subscribeAll().get();

      // Execute read compute on client which is missing the companiesEmbedding field
      computeResult = clientForMissingField.compute()
          .cosineSimilarity("companiesEmbedding", pymkCosineSimilarityEmbedding, "companiesEmbedding_score")
          .cosineSimilarity("member_feature", pymkCosineSimilarityEmbedding, "member_feature_score")
          .execute(keySetForCompute)
          .get();

      computeResult.forEach(
          (key, value) -> Assert
              .assertEquals(((HashMap<String, String>) value.get(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME)).size(), 1));

      computeResult = clientForMissingField.compute()
          .cosineSimilarity("companiesEmbedding", pymkCosineSimilarityEmbedding, "companiesEmbedding_score")
          .cosineSimilarity("member_feature", pymkCosineSimilarityEmbedding, "member_feature_score")
          .streamingExecute(keySetForCompute)
          .get();

      computeResult.forEach(
          (key, value) -> Assert
              .assertEquals(((HashMap<String, String>) value.get(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME)).size(), 1));

      clientForMissingField.unsubscribeAll();
    }
  }

  @Test(timeOut = TEST_TIMEOUT * 3)
  public void testReadComputeSwappedFields() throws Exception {
    // Create DaVinci store
    final String storeName = Utils.getUniqueString("store");
    cluster.useControllerClient(
        client -> TestUtils.assertCommand(
            client.createNewStore(
                storeName,
                getClass().getName(),
                DEFAULT_KEY_SCHEMA,
                VALUE_SCHEMA_FOR_COMPUTE_SWAPPED)));
    cluster.createMetaSystemStore(storeName);

    VersionCreationResponse newVersion = cluster.getNewVersion(storeName);
    String topic = newVersion.getKafkaTopic();
    VeniceWriterFactory vwFactory = TestUtils.getVeniceWriterFactory(cluster.getKafka().getAddress());

    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(DEFAULT_KEY_SCHEMA);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_FOR_COMPUTE);
    VeniceKafkaSerializer valueSerializerSwapped = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_FOR_COMPUTE_SWAPPED);

    int numRecords = 100;
    Map<Integer, ComputeGenericRecord> computeResult;
    Set<Integer> keySetForCompute = new HashSet<Integer>() {
      {
        add(1);
        add(2);
      }
    };

    DaVinciTestContext<Integer, Integer> daVinciTestContext =
        ServiceFactory.getGenericAvroDaVinciFactoryAndClientWithRetries(
            d2Client,
            new MetricsRepository(),
            Optional.empty(),
            cluster.getZk().getAddress(),
            storeName,
            new DaVinciConfig(),
            TestUtils.getIngestionIsolationPropertyMap());
    try (
        VeniceWriter<Object, Object, byte[]> writer = vwFactory.createVeniceWriter(
            new VeniceWriterOptions.Builder(topic).setKeySerializer(keySerializer)
                .setValueSerializer(valueSerializer)
                .build());
        CachingDaVinciClientFactory factory = daVinciTestContext.getDaVinciClientFactory();
        DaVinciClient<Integer, Integer> client = daVinciTestContext.getDaVinciClient()) {

      // Write data to DaVinci Store and subscribe client
      pushSyntheticDataToStore(writer, VALUE_SCHEMA_FOR_COMPUTE_SWAPPED, false, 1, numRecords);
      client.subscribeAll().get();

      /**
       * Perform compute on client with all fields
       *
       * This is necessary to add the compute result schema to {@link com.linkedin.davinci.client.AvroGenericDaVinciClient#computeResultSchemaCache}
       * and {@link com.linkedin.venice.client.store.AbstractAvroComputeRequestBuilder#RESULT_SCHEMA_CACHE}
       */
      computeResult = client.compute()
          .cosineSimilarity("companiesEmbedding", pymkCosineSimilarityEmbedding, "companiesEmbedding_score")
          .cosineSimilarity("member_feature", pymkCosineSimilarityEmbedding, "member_feature_score")
          .execute(keySetForCompute)
          .get();

      computeResult.forEach(
          (key, value) -> Assert
              .assertEquals(((HashMap<String, String>) value.get(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME)).size(), 0));
      client.unsubscribeAll();
    }

    // Add value schema for compute
    cluster.useControllerClient(
        clientMissingField -> TestUtils
            .assertCommand(clientMissingField.addValueSchema(storeName, VALUE_SCHEMA_FOR_COMPUTE_MISSING_FIELD)));

    VersionCreationResponse newVersionMissingField = cluster.getNewVersion(storeName);
    String topicForMissingField = newVersionMissingField.getKafkaTopic();

    DaVinciTestContext<Integer, Integer> daVinciTestContext2 =
        ServiceFactory.getGenericAvroDaVinciFactoryAndClientWithRetries(
            d2Client,
            new MetricsRepository(),
            Optional.empty(),
            cluster.getZk().getAddress(),
            storeName,
            new DaVinciConfig(),
            TestUtils.getIngestionIsolationPropertyMap());
    try (
        VeniceWriter<Object, Object, byte[]> writer2 = vwFactory.createVeniceWriter(
            new VeniceWriterOptions.Builder(topicForMissingField).setKeySerializer(keySerializer)
                .setValueSerializer(valueSerializerSwapped)
                .build());
        CachingDaVinciClientFactory factory2 = daVinciTestContext2.getDaVinciClientFactory();
        DaVinciClient<Integer, Integer> client2 = daVinciTestContext2.getDaVinciClient()) {

      // Write data to DaVinci store
      pushSyntheticDataToStore(writer2, VALUE_SCHEMA_FOR_COMPUTE_SWAPPED, false, 2, numRecords);
      client2.subscribeAll().get();

      // Execute read compute on new client
      computeResult = client2.compute()
          .cosineSimilarity("member_feature", pymkCosineSimilarityEmbedding, "member_feature_score")
          .execute(keySetForCompute)
          .get();

      computeResult.forEach(
          (key, value) -> Assert
              .assertEquals(((HashMap<String, String>) value.get(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME)).size(), 0));

      client2.unsubscribeAll();
    }
  }

  @Test(timeOut = TEST_TIMEOUT * 2)
  public void testComputeStreamingExecute() throws ExecutionException, InterruptedException {
    // Setup Store
    final String storeName = Utils.getUniqueString("store");
    cluster.useControllerClient(
        client -> TestUtils.assertCommand(
            client.createNewStore(
                storeName,
                getClass().getName(),
                KEY_SCHEMA_STEAMING_COMPUTE,
                VALUE_SCHEMA_STREAMING_COMPUTE)));
    cluster.createMetaSystemStore(storeName);

    VersionCreationResponse newVersion = cluster.getNewVersion(storeName);
    String topic = newVersion.getKafkaTopic();
    VeniceWriterFactory vwFactory = TestUtils.getVeniceWriterFactory(cluster.getKafka().getAddress());

    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(KEY_SCHEMA_STEAMING_COMPUTE);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_STREAMING_COMPUTE);

    int numRecords = 10000;
    // setup keyset to test
    Set<String> keySet = new TreeSet<>();
    /**
     * {@link NON_EXISTING_KEY1}: "a_unknown_key" will be with key index: 0 internally, and we want to verify
     * whether the code could handle non-existing key with key index: 0
     */
    keySet.add(NON_EXISTING_KEY1);
    for (int i = 0; i < MAX_KEY_LIMIT - NON_EXISTING_KEY_NUM; ++i) {
      keySet.add(KEY_PREFIX + i);
    }
    keySet.add(NON_EXISTING_KEY2);

    DaVinciConfig config = new DaVinciConfig();
    config.setNonLocalAccessPolicy(NonLocalAccessPolicy.QUERY_VENICE);

    DaVinciTestContext<String, Integer> daVinciTestContext =
        ServiceFactory.getGenericAvroDaVinciFactoryAndClientWithRetries(
            d2Client,
            new MetricsRepository(),
            Optional.empty(),
            cluster.getZk().getAddress(),
            storeName,
            config,
            TestUtils.getIngestionIsolationPropertyMap());
    try (VeniceWriter<Object, Object, byte[]> writer = vwFactory.createVeniceWriter(
        new VeniceWriterOptions.Builder(topic).setKeySerializer(keySerializer)
            .setValueSerializer(valueSerializer)
            .build());
        DaVinciClient<String, Integer> client = daVinciTestContext.getDaVinciClient()) {

      // push data to store and subscribe client
      pushDataToStoreForStreamingCompute(
          writer,
          VALUE_SCHEMA_STREAMING_COMPUTE,
          HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID,
          numRecords);
      client.subscribeAll().get();

      // Test compute with streaming execute using custom provided callback
      AtomicInteger computeResultCnt = new AtomicInteger(0);
      Map<String, ComputeGenericRecord> finalComputeResultMap = new VeniceConcurrentHashMap<>();
      CountDownLatch computeLatch = new CountDownLatch(1);

      client.compute()
          .project("int_field")
          .streamingExecute(keySet, new StreamingCallback<String, ComputeGenericRecord>() {
            @Override
            public void onRecordReceived(String key, ComputeGenericRecord value) {
              computeResultCnt.incrementAndGet();
              if (value != null) {
                finalComputeResultMap.put(key, value);
              }
            }

            @Override
            public void onCompletion(Optional<Exception> exception) {
              computeLatch.countDown();
              exception.ifPresent(e -> Assert.fail("Exception: " + e + " is not expected"));
            }
          });
      computeLatch.await();

      Assert.assertEquals(computeResultCnt.get(), MAX_KEY_LIMIT);
      Assert.assertEquals(finalComputeResultMap.size(), MAX_KEY_LIMIT - NON_EXISTING_KEY_NUM); // Without non-existing
                                                                                               // key
      verifyStreamingComputeResult(finalComputeResultMap);

      // Test compute with streaming execute using default callback
      CompletableFuture<VeniceResponseMap<String, ComputeGenericRecord>> computeFuture =
          client.compute().project("int_field").streamingExecute(keySet);
      Map<String, ComputeGenericRecord> computeResultMap = computeFuture.get();
      Assert.assertEquals(computeResultMap.size(), MAX_KEY_LIMIT - NON_EXISTING_KEY_NUM);
      verifyStreamingComputeResult(computeResultMap);

      client.unsubscribeAll();
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testPartialKeyLookupWithRocksDBBlockBasedTable() throws ExecutionException, InterruptedException {
    final String storeName = Utils.getUniqueString("store");
    cluster.useControllerClient(
        client -> TestUtils.assertCommand(
            client.createNewStore(
                storeName,
                getClass().getName(),
                KEY_SCHEMA_PARTIAL_KEY_LOOKUP,
                VALUE_SCHEMA_FOR_COMPUTE)));
    cluster.createMetaSystemStore(storeName);

    VersionCreationResponse newVersion = cluster.getNewVersion(storeName);
    String topic = newVersion.getKafkaTopic();
    VeniceWriterFactory vwFactory = TestUtils.getVeniceWriterFactory(cluster.getKafka().getAddress());

    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(KEY_SCHEMA_PARTIAL_KEY_LOOKUP);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_FOR_COMPUTE);

    MetricsRepository metricsRepository = new MetricsRepository();
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();

    VeniceProperties backendConfig = new PropertyBuilder().put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .build();

    int numRecords = 100;

    try (
        VeniceWriter<GenericRecord, GenericRecord, byte[]> writer = vwFactory.createVeniceWriter(
            new VeniceWriterOptions.Builder(topic).setKeySerializer(keySerializer)
                .setValueSerializer(valueSerializer)
                .build());
        CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
            d2Client,
            VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
            metricsRepository,
            backendConfig);
        DaVinciClient<GenericRecord, GenericRecord> client =
            factory.getAndStartGenericAvroClient(storeName, new DaVinciConfig().setStorageClass(StorageClass.DISK))) {

      pushSyntheticDataToStoreForPartialKeyLookup(
          writer,
          KEY_SCHEMA_PARTIAL_KEY_LOOKUP,
          VALUE_SCHEMA_FOR_COMPUTE,
          false,
          1,
          numRecords);
      client.subscribeAll().get();

      Map<GenericRecord, GenericRecord> finalComputeResultMap = new VeniceConcurrentHashMap<>();
      CountDownLatch computeLatch = new CountDownLatch(1);

      Predicate partialKey = and(equalTo("id", "key_abcdefgh_1"), equalTo("companyId", 0));

      Schema keySchema = new Schema.Parser().parse(KEY_SCHEMA_PARTIAL_KEY_LOOKUP);

      GenericData.Record key = new GenericData.Record(keySchema);
      key.put("id", "key_abcdefgh_1");
      key.put("companyId", 0);
      key.put("name", "name_4");

      GenericRecord result = client.get(key).get();
      Assert.assertNotNull(result);

      client.compute()
          .project("id", "name", "companiesEmbedding", "member_feature")
          .cosineSimilarity("companiesEmbedding", pymkCosineSimilarityEmbedding, "companiesEmbedding_score")
          .cosineSimilarity("member_feature", pymkCosineSimilarityEmbedding, "member_feature_score")
          .executeWithFilter(partialKey, new StreamingCallback<GenericRecord, GenericRecord>() {
            @Override
            public void onRecordReceived(GenericRecord key, GenericRecord value) {
              if (value != null) {
                finalComputeResultMap.put(key, value);
              }
            }

            @Override
            public void onCompletion(Optional<Exception> exception) {
              computeLatch.countDown();
              if (exception.isPresent()) {
                Assert.fail("Exception: " + exception.get() + " is not expected");
              }
            }
          });

      computeLatch.await();
      Assert.assertEquals(finalComputeResultMap.size(), 16);
      finalComputeResultMap.forEach((key1, value) -> {
        Assert.assertEquals(key1.getSchema(), keySchema);
        Assert.assertEquals(key1.get("id").toString(), "key_abcdefgh_1");
        Assert.assertEquals(key1.get("companyId"), 0);
        Assert.assertEquals(value.getSchema().getFields().size(), 7);
        Assert.assertEquals(((HashMap<String, String>) value.get(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME)).size(), 0);
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testPartialKeyLookupWithRocksDBPlainTable() throws ExecutionException, InterruptedException {
    final String storeName = Utils.getUniqueString("store");
    cluster.useControllerClient(
        client -> TestUtils.assertCommand(
            client.createNewStore(
                storeName,
                getClass().getName(),
                KEY_SCHEMA_PARTIAL_KEY_LOOKUP,
                VALUE_SCHEMA_FOR_COMPUTE)));
    cluster.createMetaSystemStore(storeName);

    VersionCreationResponse newVersion = cluster.getNewVersion(storeName);
    String topic = newVersion.getKafkaTopic();
    VeniceWriterFactory vwFactory = TestUtils.getVeniceWriterFactory(cluster.getKafka().getAddress());

    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(KEY_SCHEMA_PARTIAL_KEY_LOOKUP);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_FOR_COMPUTE);

    MetricsRepository metricsRepository = new MetricsRepository();
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();

    VeniceProperties backendConfig = new PropertyBuilder().put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .build();

    int numRecords = 100;

    try (
        VeniceWriter<GenericRecord, GenericRecord, byte[]> writer = vwFactory.createVeniceWriter(
            new VeniceWriterOptions.Builder(topic).setKeySerializer(keySerializer)
                .setValueSerializer(valueSerializer)
                .build());
        CachingDaVinciClientFactory factory = new CachingDaVinciClientFactory(
            d2Client,
            VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
            metricsRepository,
            backendConfig);
        DaVinciClient<GenericRecord, GenericRecord> client =
            factory.getAndStartGenericAvroClient(storeName, new DaVinciConfig())) {

      pushSyntheticDataToStoreForPartialKeyLookup(
          writer,
          KEY_SCHEMA_PARTIAL_KEY_LOOKUP,
          VALUE_SCHEMA_FOR_COMPUTE,
          false,
          1,
          numRecords);
      client.subscribeAll().get();

      Predicate partialKey = and(equalTo("id", "key_abcdefgh_1"), equalTo("companyId", 0));

      final boolean[] completed = { false };

      client.compute()
          .project("id", "name", "companiesEmbedding", "member_feature")
          .executeWithFilter(partialKey, new StreamingCallback<GenericRecord, GenericRecord>() {
            @Override
            public void onRecordReceived(GenericRecord key, GenericRecord value) {
              Assert.fail("No records should have been found from the store engine.");
            }

            @Override
            public void onCompletion(Optional<Exception> exception) {
              completed[0] = true;
              Assert.assertTrue(exception.isPresent());
              Assert.assertEquals(
                  exception.get().getMessage(),
                  "Get by key prefix is not supported with RocksDB PlainTable Format.");
            }
          });
      Assert.assertTrue(completed[0]);

      CountDownLatch computeLatch = new CountDownLatch(1);
      AtomicInteger recordCount = new AtomicInteger(0);
      client.compute()
          .project("id", "name", "companiesEmbedding", "member_feature")
          .executeWithFilter(null, new StreamingCallback<GenericRecord, GenericRecord>() {
            @Override
            public void onRecordReceived(GenericRecord key, GenericRecord value) {
              recordCount.incrementAndGet();
            }

            @Override
            public void onCompletion(Optional<Exception> exception) {
              computeLatch.countDown();
              exception.ifPresent(e -> Assert.fail("Exception: " + e + " is not expected"));
            }
          });
      computeLatch.await();
      Assert.assertEquals(recordCount.get(), numRecords, "Should receive all key-value pairs");
    }
  }

  private void pushSyntheticDataToStore(
      VeniceWriter<Object, Object, byte[]> writer,
      String schema,
      boolean skip,
      int valueSchemaId,
      int numRecords) throws ExecutionException, InterruptedException {
    writer.broadcastStartOfPush(Collections.emptyMap());
    Schema valueSchema = Schema.parse(schema);

    for (int i = 0; i < numRecords; ++i) {
      GenericRecord value = new GenericData.Record(valueSchema);
      value.put("id", VALUE_PREFIX + i);
      value.put("name", "companiesEmbedding");
      if (!skip) {
        value.put("companiesEmbedding", companiesEmbedding);
      }
      value.put("member_feature", mfEmbedding);
      writer.put(i, value, valueSchemaId).get();
    }
    writer.broadcastEndOfPush(Collections.emptyMap());
  }

  private void pushRecordsToStore(
      Map<Integer, GenericRecord> valuesByKey,
      VeniceWriter<Object, Object, byte[]> veniceWriter,
      int valueSchemaId) throws Exception {

    veniceWriter.broadcastStartOfPush(Collections.emptyMap());
    for (Map.Entry<Integer, GenericRecord> keyValue: valuesByKey.entrySet()) {
      veniceWriter.put(keyValue.getKey(), keyValue.getValue(), valueSchemaId).get();
    }
    veniceWriter.broadcastEndOfPush(Collections.emptyMap());
  }

  private void pushDataToStoreForStreamingCompute(
      VeniceWriter<Object, Object, byte[]> writer,
      String valueSchemaString,
      int valueSchemaId,
      int numRecords) throws ExecutionException, InterruptedException {
    writer.broadcastStartOfPush(Collections.emptyMap());
    Schema.Parser schemaParser = new Schema.Parser();
    Schema valueSchema = schemaParser.parse(valueSchemaString);

    for (int i = 0; i < numRecords; i++) {
      GenericRecord valueRecord = new GenericData.Record(valueSchema);
      valueRecord.put("int_field", i);
      valueRecord.put("float_field", i + 100.0f);
      writer.put(KEY_PREFIX + i, valueRecord, valueSchemaId).get();
    }

    writer.broadcastEndOfPush(Collections.emptyMap());
  }

  private void verifyStreamingComputeResult(Map<String, ComputeGenericRecord> resultMap) {
    for (int i = 0; i < MAX_KEY_LIMIT - NON_EXISTING_KEY_NUM; ++i) {
      String key = KEY_PREFIX + i;
      GenericRecord record = resultMap.get(key);
      Assert.assertEquals(record.get("int_field"), i);
      Assert.assertNull(record.get("float_field"));
    }
  }

  private void pushSyntheticDataToStoreForPartialKeyLookup(
      VeniceWriter<GenericRecord, GenericRecord, byte[]> writer,
      String keySchemaString,
      String valueSchemaString,
      boolean skip,
      int valueSchemaId,
      int numRecords) throws ExecutionException, InterruptedException {
    writer.broadcastStartOfPush(Collections.emptyMap());
    Schema keySchema = new Schema.Parser().parse(keySchemaString);
    Schema valueSchema = new Schema.Parser().parse(valueSchemaString);
    String keyIdFiller = "abcdefgh_";
    for (int i = 0; i < numRecords; ++i) {
      GenericRecord key = new GenericData.Record(keySchema);
      key.put("id", KEY_PREFIX + keyIdFiller + (i % 3));
      key.put("companyId", i % 2);
      key.put("name", "name_" + i);

      GenericRecord value = new GenericData.Record(valueSchema);
      value.put("id", VALUE_PREFIX + i);
      value.put("name", "companiesEmbedding");
      if (!skip) {
        value.put("companiesEmbedding", companiesEmbedding);
      }
      value.put("member_feature", mfEmbedding);
      writer.put(key, value, valueSchemaId).get();
    }
    writer.broadcastEndOfPush(Collections.emptyMap());
  }

  private List<Float> generateRandomFloatList(int listSize) {
    ThreadLocalRandom rand = ThreadLocalRandom.current();
    List<Float> feature = new ArrayList<>(listSize);
    for (int i = 0; i < listSize; i++) {
      feature.add(rand.nextFloat());
    }
    return feature;
  }

}
