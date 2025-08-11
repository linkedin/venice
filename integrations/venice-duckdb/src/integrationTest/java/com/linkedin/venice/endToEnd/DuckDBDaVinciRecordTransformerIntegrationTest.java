package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.DA_VINCI_CURRENT_VERSION_BOOTSTRAPPING_SPEEDUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.DA_VINCI_SUBSCRIBE_ON_DISK_PARTITIONS_AUTOMATICALLY;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.integration.utils.DaVinciTestContext.getCachingDaVinciClientFactory;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.DEFAULT_USER_DATA_RECORD_COUNT;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V1_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.SINGLE_FIELD_RECORD_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFile;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_STORE_NAME_PROP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerFunctionalInterface;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.duckdb.DuckDBDaVinciRecordTransformer;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.producer.online.OnlineProducerFactory;
import com.linkedin.venice.producer.online.OnlineVeniceProducer;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.PushInputSchemaBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * On DuckDB 1.1.3, this test works on mac but fails on the CI, likely due to: https://github.com/duckdb/duckdb-java/issues/14
 *
 * With a more recent snapshot release, where the above issue is fixed and merged, it works in both environments.
 *
 * Once there is an official release containing this fix, we could consider moving the integration tests back to
 * venice-test-common.
 */
public class DuckDBDaVinciRecordTransformerIntegrationTest {
  private static final Logger LOGGER = LogManager.getLogger(DuckDBDaVinciRecordTransformerIntegrationTest.class);
  private static final int TEST_TIMEOUT = 3 * Time.MS_PER_MINUTE;
  private VeniceClusterWrapper cluster;
  private D2Client d2Client;

  @BeforeClass
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties clusterConfig = new Properties();
    clusterConfig.put(PUSH_STATUS_STORE_ENABLED, true);
    clusterConfig.put(DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS, 3);
    this.cluster = ServiceFactory.getVeniceCluster(
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
            .numberOfServers(2)
            .numberOfRouters(1)
            .replicationFactor(2)
            .extraProperties(clusterConfig)
            .build());
    this.d2Client = new D2ClientBuilder().setZkHosts(this.cluster.getZk().getAddress())
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(this.d2Client);
  }

  @AfterClass
  public void cleanUp() {
    D2ClientUtils.shutdownClient(this.d2Client);
    Utils.closeQuietlyWithErrorLogged(this.cluster);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testRecordTransformer() throws Exception {
    DaVinciConfig clientConfig = new DaVinciConfig();

    File tmpDir = Utils.getTempDataDirectory();
    String storeName = Utils.getUniqueString("test-store");
    boolean pushStatusStoreEnabled = false;
    boolean chunkingEnabled = false;
    CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;

    setUpStore(storeName, pushStatusStoreEnabled, chunkingEnabled, compressionStrategy);

    VeniceProperties backendConfig = buildRecordTransformerBackendConfig(pushStatusStoreEnabled);
    MetricsRepository metricsRepository = new MetricsRepository();
    String duckDBUrl = "jdbc:duckdb:" + tmpDir.getAbsolutePath() + "/my_database.duckdb";

    try (CachingDaVinciClientFactory factory = getCachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig,
        cluster)) {
      Set<String> columnsToProject = Collections.emptySet();

      DaVinciRecordTransformerFunctionalInterface recordTransformerFunction =
          (storeVersion, keySchema, inputValueSchema, outputValueSchema, config) -> new DuckDBDaVinciRecordTransformer(
              storeVersion,
              keySchema,
              inputValueSchema,
              outputValueSchema,
              config,
              tmpDir.getAbsolutePath(),
              storeName,
              columnsToProject);

      DaVinciRecordTransformerConfig recordTransformerConfig =
          new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(recordTransformerFunction)
              .setStoreRecordsInDaVinci(false)
              .build();
      clientConfig.setRecordTransformerConfig(recordTransformerConfig);

      DaVinciClient<Integer, Object> clientWithRecordTransformer =
          factory.getAndStartGenericAvroClient(storeName, clientConfig);

      clientWithRecordTransformer.subscribeAll().get();

      assertRowCount(duckDBUrl, storeName, DEFAULT_USER_DATA_RECORD_COUNT, "subscribeAll() finishes!");

      try (OnlineVeniceProducer producer = OnlineProducerFactory.createProducer(
          ClientConfig.defaultGenericClientConfig(storeName)
              .setD2Client(d2Client)
              .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME),
          VeniceProperties.empty(),
          null)) {
        producer.asyncDelete(getKey(1)).get();
      }

      TestUtils.waitForNonDeterministicAssertion(
          10,
          TimeUnit.SECONDS,
          true,
          () -> assertRowCount(duckDBUrl, storeName, DEFAULT_USER_DATA_RECORD_COUNT - 1, "deleting 1 row!"));

      clientWithRecordTransformer.unsubscribeAll();
    }

    assertRowCount(duckDBUrl, storeName, DEFAULT_USER_DATA_RECORD_COUNT - 1, "DVC gets closed!");
  }

  private void assertRowCount(String duckDBUrl, String storeName, int expectedCount, String assertionErrorMsg)
      throws SQLException {
    String selectStatement = "SELECT count(*) FROM \"%s\";";
    try (Connection connection = DriverManager.getConnection(duckDBUrl);
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(String.format(selectStatement, storeName))) {
      assertTrue(rs.next());
      int rowCount = rs.getInt(1);
      assertEquals(
          rowCount,
          expectedCount,
          "The DB should contain " + expectedCount + " rows right after " + assertionErrorMsg);
    }
  }

  protected void setUpStore(
      String storeName,
      boolean useDVCPushStatusStore,
      boolean chunkingEnabled,
      CompressionStrategy compressionStrategy) throws IOException {

    File inputDir = getTempDataDirectory();
    Consumer<UpdateStoreQueryParams> paramsConsumer = params -> {};
    Consumer<Properties> propertiesConsumer = properties -> {};
    Schema pushRecordSchema = new PushInputSchemaBuilder().setKeySchema(SINGLE_FIELD_RECORD_SCHEMA)
        .setValueSchema(NAME_RECORD_V1_SCHEMA)
        .build();
    String firstName = "first_name_";
    String lastName = "last_name_";
    Schema valueSchema = writeSimpleAvroFile(inputDir, pushRecordSchema, i -> {
      GenericRecord keyValueRecord = new GenericData.Record(pushRecordSchema);
      GenericRecord key = getKey(i);
      keyValueRecord.put(DEFAULT_KEY_FIELD_PROP, key);
      GenericRecord valueRecord = new GenericData.Record(NAME_RECORD_V1_SCHEMA);
      valueRecord.put("firstName", firstName + i);
      valueRecord.put("lastName", lastName + i);
      keyValueRecord.put(DEFAULT_VALUE_FIELD_PROP, valueRecord); // Value
      return keyValueRecord;
    });
    String keySchemaStr = valueSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();

    // Setup VPJ job properties.
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties vpjProperties = defaultVPJProps(cluster, inputDirPath, storeName);
    propertiesConsumer.accept(vpjProperties);
    // Create & update store for test.
    final int numPartitions = 3;
    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setPartitionCount(numPartitions)
        .setChunkingEnabled(chunkingEnabled)
        .setCompressionStrategy(compressionStrategy)
        .setHybridOffsetLagThreshold(10)
        .setHybridRewindSeconds(1);

    paramsConsumer.accept(params);

    try (ControllerClient controllerClient =
        createStoreForJob(cluster, keySchemaStr, NAME_RECORD_V1_SCHEMA.toString(), vpjProperties)) {
      cluster.createMetaSystemStore(storeName);
      if (useDVCPushStatusStore) {
        cluster.createPushStatusSystemStore(storeName);
      }
      TestUtils.assertCommand(controllerClient.updateStore(storeName, params));
      SchemaResponse schemaResponse = controllerClient.addValueSchema(storeName, NAME_RECORD_V1_SCHEMA.toString());
      assertFalse(schemaResponse.isError());
      runVPJ(vpjProperties, 1, cluster);
    }
  }

  private GenericRecord getKey(Integer i) {
    GenericRecord key = new GenericData.Record(SINGLE_FIELD_RECORD_SCHEMA);
    key.put("key", i.toString());
    return key;
  }

  private static void runVPJ(Properties vpjProperties, int expectedVersionNumber, VeniceClusterWrapper cluster) {
    long vpjStart = System.currentTimeMillis();
    IntegrationTestPushUtils.runVPJ(vpjProperties);
    String storeName = (String) vpjProperties.get(VENICE_STORE_NAME_PROP);
    cluster.waitVersion(storeName, expectedVersionNumber);
    LOGGER.info("**TIME** VPJ" + expectedVersionNumber + " takes " + (System.currentTimeMillis() - vpjStart));
  }

  public VeniceProperties buildRecordTransformerBackendConfig(boolean pushStatusStoreEnabled) {
    String baseDataPath = Utils.getTempDataDirectory().getAbsolutePath();
    PropertyBuilder backendPropertyBuilder = new PropertyBuilder().put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, baseDataPath)
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
        .put(DA_VINCI_CURRENT_VERSION_BOOTSTRAPPING_SPEEDUP_ENABLED, true)
        .put(PUSH_STATUS_STORE_ENABLED, pushStatusStoreEnabled)
        .put(DA_VINCI_SUBSCRIBE_ON_DISK_PARTITIONS_AUTOMATICALLY, false)
        .put(DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS, 1000);

    if (pushStatusStoreEnabled) {
      backendPropertyBuilder.put(PUSH_STATUS_STORE_ENABLED, true).put(DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS, 1000);
    }

    return backendPropertyBuilder.build();
  }
}
