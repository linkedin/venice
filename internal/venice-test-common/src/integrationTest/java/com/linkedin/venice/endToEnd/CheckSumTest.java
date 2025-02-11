package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.SSL_TO_KAFKA_LEGACY;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.runVPJ;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendCustomSizeStreamingRecord;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class CheckSumTest {
  private static final Logger LOGGER = LogManager.getLogger(CheckSumTest.class);
  public static final int NUMBER_OF_SERVERS = 3;
  public static final int STREAMING_RECORD_SIZE = 1024;

  private VeniceClusterWrapper veniceCluster;
  protected final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    veniceCluster = setUpCluster();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }

  private VeniceClusterWrapper setUpCluster() {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(DEFAULT_MAX_NUMBER_OF_PARTITIONS, "5");
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(0)
        .numberOfRouters(1)
        .replicationFactor(1)
        .partitionSize(1000000)
        .sslToStorageNodes(false)
        .sslToKafka(false)
        .extraProperties(extraProperties)
        .build();
    VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(options);

    // Add Venice Router
    Properties routerProperties = new Properties();
    cluster.addVeniceRouter(routerProperties);

    // Add Venice Server
    Properties serverProperties = new Properties();
    serverProperties.setProperty(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB.name());
    serverProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1L));
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");

    serverProperties.setProperty(SSL_TO_KAFKA_LEGACY, "false");

    // Set the VeniceWriter to use the ADHASH checksum.
    serverProperties.setProperty(VeniceWriter.CHECK_SUM_TYPE, CheckSumType.ADHASH.name());

    for (int i = 0; i < NUMBER_OF_SERVERS; i++) {
      cluster.addVeniceServer(new Properties(), serverProperties);
    }
    return cluster;
  }

  /**
   * Test the checksum feature by creating two stores with different checksum types and producing records to them.
   * <ul>
   * <li> Set CHECK_SUM_TYPE to {@link CheckSumType#ADHASH} in the server properties.
   * <li> Create two stores with different checksum types in the VPJ.
   * <li> Verify that records can be read from the both stores.
   * <li> Produce records to the rt topic of the first store with CheckSumType set to {@link CheckSumType#ADHASH}.
   * <li> Produce records to the rt topic of the first store with CheckSumType set to {@link CheckSumType#MD5}.
   * <li> Check that the first store has all the records.
   * <li> Produce records to the rt topic of the second store with CheckSumType set to {@link CheckSumType#MD5}.
   * <li> Check that the second store has all the records.
   * </ul>
   */
  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testCheckSum() throws IOException {
    long streamingRewindSeconds = 25;
    long streamingMessageLag = 2;
    final String storeNameFirst = Utils.getUniqueString("hybrid-store-for-checksum-first");
    final String storeNameSecond = Utils.getUniqueString("hybrid-store-for-checksum-second");

    try (
        AvroGenericStoreClient<Object, Object> clientToFirstStore = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeNameFirst).setVeniceURL(veniceCluster.getRandomRouterURL()));
        AvroGenericStoreClient<Object, Object> clientToSecondStore = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeNameSecond)
                .setVeniceURL(veniceCluster.getRandomRouterURL()))) {

      LOGGER.info("Creating stores and versions for CheckSumTest");
      // Create stores and versions for each store.
      createStoresAndVersion(storeNameFirst, CheckSumType.ADHASH, streamingRewindSeconds, streamingMessageLag);
      createStoresAndVersion(storeNameSecond, CheckSumType.MD5, streamingRewindSeconds, streamingMessageLag);

      // Produce to the rt topic of the first store with CheckSumType set to ADHASH.
      produceToStoreRTTopic(storeNameFirst, 1, 10, CheckSumType.ADHASH);

      // Produce to the rt topic of the first store with CheckSumType set to MD5.
      produceToStoreRTTopic(storeNameFirst, 11, 20, CheckSumType.MD5);

      // Check that the first store has all the records.
      checkRecords(clientToFirstStore, 20);

      // Produce to the rt topic of the second store with CheckSumType set to MD5.
      produceToStoreRTTopic(storeNameSecond, 1, 10, CheckSumType.MD5);

      // Check that the second store has all the records.
      checkRecords(clientToSecondStore, 10);
    }
  }

  void checkRecords(AvroGenericStoreClient<Object, Object> client, int numberOfRecords) {
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, true, () -> {
      try {
        for (int i = 1; i <= numberOfRecords; i++) {
          checkLargeRecord(client, i);
          LOGGER.info("Store {}: Checked record {}", client.getStoreName(), i);
        }
      } catch (Exception e) {
        throw new VeniceException(e);
      }
    });
  }

  private void produceToStoreRTTopic(String storeName, int start, int end, CheckSumType checkSumType) {
    LOGGER.info(
        "Producing {} records to the rt topic of store {} with CheckSumType set to {}",
        end - start + 1,
        storeName,
        checkSumType);
    SystemProducer veniceProducer = getSamzaProducer(
        veniceCluster,
        storeName,
        Version.PushType.STREAM,
        Pair.create(VeniceWriter.CHECK_SUM_TYPE, checkSumType.name()));
    for (int i = start; i <= end; i++) {
      sendCustomSizeStreamingRecord(veniceProducer, storeName, i, STREAMING_RECORD_SIZE);
    }
    veniceProducer.stop();
  }

  private void createStoresAndVersion(
      String storeName,
      CheckSumType type,
      long streamingRewindSeconds,
      long streamingMessageLag) throws IOException {
    // Create store at parent, make it a hybrid store.
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir); // records 1-100
    Properties vpjProperties = defaultVPJProps(veniceCluster, inputDirPath, storeName);
    vpjProperties.setProperty(VeniceWriter.CHECK_SUM_TYPE, type.name());
    LOGGER.info("Creating VPJ {} with CheckSumType set to {}", storeName, type.name());
    try (
        ControllerClient controllerClient =
            createStoreForJob(veniceCluster.getClusterName(), recordSchema, vpjProperties);
        AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {

      ControllerResponse response = controllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setHybridRewindSeconds(streamingRewindSeconds)
              .setHybridOffsetLagThreshold(streamingMessageLag)
              .setPartitionCount(1)
              .setHybridOffsetLagThreshold(streamingMessageLag));

      Assert.assertFalse(response.isError());

      // Do a VPJ push
      runVPJ(vpjProperties, 1, controllerClient);

      // Verify some records (note, records 1-100 have been pushed)
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        try {
          for (int i = 1; i < 100; i++) {
            String key = Integer.toString(i);
            Object value = client.get(key).get();
            assertNotNull(value, "Key " + i + " should not be missing!");
            assertEquals(value.toString(), "test_name_" + key);
          }
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });
    }
  }

  private void checkLargeRecord(AvroGenericStoreClient client, int index)
      throws ExecutionException, InterruptedException {
    String key = Integer.toString(index);
    assert client != null;
    Object obj = client.get(key).get();
    String value = obj.toString();
    assertEquals(
        value.length(),
        STREAMING_RECORD_SIZE,
        "Expected a large record for key '" + key + "' but instead got: '" + value + "'.");

    String expectedChar = Integer.toString(index).substring(0, 1);
    for (int i = 0; i < value.length(); i++) {
      assertEquals(value.substring(i, i + 1), expectedChar);
    }
  }
}
