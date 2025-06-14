package com.linkedin.venice.restart;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.CHILD_DATA_CENTER_KAFKA_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.rocksdb.ReplicationMetadataRocksDBStoragePartition;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.TestVeniceServer;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.VeniceUserStoreType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.view.TestView;
import com.linkedin.venice.writer.PutMetadata;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.util.BytewiseComparator;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestRestartServerAfterDeletingSstFilesWithActiveActiveIngestion {
  private static final Logger LOGGER =
      LogManager.getLogger(TestRestartServerAfterDeletingSstFilesWithActiveActiveIngestion.class);
  private static final int TEST_TIMEOUT = 180 * Time.MS_PER_SECOND;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private static final int NUMBER_OF_COLOS = 2;
  // src colo to write data to
  private static final int SOURCE_COLO = 0;
  // non src colo to consume data from to validate kafka contents
  private static final int NON_SOURCE_COLO = 1;
  private final List<VeniceClusterWrapper> clusterWrappers = new ArrayList<>(NUMBER_OF_COLOS);
  private final Map<Integer, List<VeniceServerWrapper>> serverWrappers = new HashMap<>(NUMBER_OF_COLOS);
  private ControllerClient parentControllerClient;
  private AvroSerializer serializer;
  private final int NUMBER_OF_KEYS = 100;
  private final int NUMBER_OF_PARTITIONS = 1;
  private final int PARTITION_ID = 0;
  private final int NUMBER_OF_REPLICAS = 2;
  private int startKey = 0;
  private int newVersion = 0;
  private final String KEY_PREFIX = "key";
  private final String VALUE_PREFIX = "value";
  private final String VALUE_PREFIX_INC_PUSH = "value-inc";
  private final String STORE_NAME = Utils.getUniqueString("store");
  private final int numServers = 5;
  List<Integer> allIncPushKeys = new ArrayList<>(); // all keys ingested via incremental push
  List<Integer> allNonIncPushKeysUntilLastVersion = new ArrayList<>(); // all keys ingested only via batch push

  @BeforeClass(alwaysRun = true)
  public void setUp() throws Exception {
    String stringSchemaStr = "\"string\"";
    serializer = new AvroSerializer(AvroCompatibilityHelper.parse(stringSchemaStr));
    Properties serverProperties = new Properties();
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    serverProperties.put(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, true);
    serverProperties.put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB);
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    serverProperties.put(
        CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + DEFAULT_PARENT_DATA_CENTER_REGION_NAME,
        "localhost:" + TestUtils.getFreePort());
    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(NUMBER_OF_COLOS)
            .numberOfClusters(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(2)
            .numberOfServers(numServers)
            .numberOfRouters(1)
            .replicationFactor(NUMBER_OF_REPLICAS)
            .forkServer(false)
            .serverProperties(serverProperties);
    multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());

    List<VeniceMultiClusterWrapper> childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    List<VeniceControllerWrapper> parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
    String clusterName = "venice-cluster0";
    for (int colo = 0; colo < NUMBER_OF_COLOS; colo++) {
      clusterWrappers.add(colo, childDatacenters.get(colo).getClusters().get(clusterName));
    }

    String parentControllerURLs =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));
    parentControllerClient = new ControllerClient(clusterName, parentControllerURLs);
    TestUtils.assertCommand(
        parentControllerClient.configureActiveActiveReplicationForCluster(
            true,
            VeniceUserStoreType.INCREMENTAL_PUSH.toString(),
            Optional.empty()));
    // create an active-active enabled store
    File inputDir = getTempDataDirectory();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, STORE_NAME);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(500)
        .setHybridOffsetLagThreshold(2)
        .setNativeReplicationEnabled(true)
        .setBackupVersionRetentionMs(1)
        .setIncrementalPushEnabled(true)
        .setPartitionCount(NUMBER_OF_PARTITIONS)
        .setReplicationFactor(NUMBER_OF_REPLICAS);
    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms).close();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    if (parentControllerClient != null) {
      parentControllerClient.disableAndDeleteStore(STORE_NAME);
    }
    Utils.closeQuietlyWithErrorLogged(parentControllerClient);
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
    TestView.resetCounters();
  }

  private Map<byte[], Pair<byte[], byte[]>> generateInputWithMetadata(
      int startIndex,
      int endIndex,
      boolean sorted,
      boolean isIncPush,
      AvroSerializer serializer) {
    Map<byte[], Pair<byte[], byte[]>> records;
    if (sorted) {
      BytewiseComparator comparator = new BytewiseComparator(new ComparatorOptions());
      records = new TreeMap<>((o1, o2) -> {
        ByteBuffer b1 = ByteBuffer.wrap(o1);
        ByteBuffer b2 = ByteBuffer.wrap(o2);
        return comparator.compare(b1, b2);
      });
    } else {
      records = new HashMap<>();
    }
    for (int i = startIndex; i < endIndex; ++i) {
      String value = isIncPush ? VALUE_PREFIX_INC_PUSH + i : VALUE_PREFIX + i;
      String METADATA_PREFIX = "metadata";
      String metadata = METADATA_PREFIX + i;
      records.put(
          serializer.serialize(KEY_PREFIX + i),
          Pair.create(serializer.serialize(value), serializer.serialize(metadata)));
    }
    return records;
  }

  private byte[] getReplicationMetadataWithValueSchemaId(byte[] replicationMetadata, int valueSchemaId) {
    ByteBuffer metadataByteBuffer = ByteBuffer.wrap(replicationMetadata);
    ByteBuffer replicationMetadataWitValueSchemaId =
        ByteUtils.prependIntHeaderToByteBuffer(metadataByteBuffer, valueSchemaId, false);
    replicationMetadataWitValueSchemaId
        .position(replicationMetadataWitValueSchemaId.position() - ByteUtils.SIZE_OF_INT);
    return ByteUtils.extractByteArray(replicationMetadataWitValueSchemaId);
  }

  /** Get partition for a version topic from right venice server out of all available servers */
  private void getPartitionForTopic(
      final String topic,
      Map<Integer, List<ReplicationMetadataRocksDBStoragePartition>> rocksDBStoragePartitions) {
    for (int colo = 0; colo < NUMBER_OF_COLOS; colo++) {
      // reset
      rocksDBStoragePartitions.computeIfAbsent(colo, key -> new ArrayList<>()).clear();
      serverWrappers.computeIfAbsent(colo, key -> new ArrayList<>()).clear();
      int server;
      int numSelectedServers = 0;
      for (server = 0; server < numServers; server++) {
        VeniceServerWrapper serverWrapper = clusterWrappers.get(colo).getVeniceServers().get(server);
        if (serverWrapper.getVeniceServer()
            .getStorageService()
            .getStorageEngineRepository()
            .getLocalStorageEngine(topic) != null) {
          LOGGER.info("selected server is: {} in colo {}", server, colo);
          TestVeniceServer testVeniceServer = serverWrapper.getVeniceServer();
          StorageService storageService = testVeniceServer.getStorageService();
          StorageEngine storageEngine = storageService.getStorageEngineRepository().getLocalStorageEngine(topic);
          assertNotNull(storageEngine);
          assertEquals(storageEngine.getPartitionIds().size(), NUMBER_OF_PARTITIONS);
          rocksDBStoragePartitions.get(colo)
              .add((ReplicationMetadataRocksDBStoragePartition) storageEngine.getPartitionOrThrow(PARTITION_ID));
          serverWrappers.get(colo).add(serverWrapper);

          if (++numSelectedServers == NUMBER_OF_REPLICAS) {
            break;
          }
        }
      }
      assertEquals(numSelectedServers, NUMBER_OF_REPLICAS, "Couldn't find the required number of servers");
    }
  }

  /**
   * This test include below steps:
   * 1. Batch Push data without EOP (100 keys)
   * 2. stop servers, delete SST files, start servers (based on params)
   * 3. Validate whether the data is ingested
   * 4. Incremental push data (10 keys (90-100 of the batch push))
   * 5. Validate whether the data is ingested
   * 6. Validate whether all the data from RT is ingested to the new versions as well.
   */
  @Test(timeOut = TEST_TIMEOUT, dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testActiveActiveStoreWithRMDAndRestartServer(boolean deleteSSTFiles, boolean deleteRMDSSTFiles)
      throws Exception {
    // Create a new version
    VersionCreationResponse versionCreationResponse;
    versionCreationResponse = TestUtils.assertCommand(
        parentControllerClient.requestTopicForWrites(
            STORE_NAME,
            1024 * 1024,
            Version.PushType.BATCH,
            System.currentTimeMillis() + "_test_server_restart_push",
            true,
            true,
            false,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            false,
            -1));

    int versionToBePushed = versionCreationResponse.getVersion();
    assertEquals(newVersion + 1, versionToBePushed);
    newVersion = versionToBePushed;

    final String topic = versionCreationResponse.getKafkaTopic();
    assertNotNull(topic);
    PubSubBrokerWrapper pubSubBrokerWrapper = clusterWrappers.get(SOURCE_COLO).getPubSubBrokerWrapper();
    PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
        pubSubBrokerWrapper.getPubSubClientsFactory().getProducerAdapterFactory();
    VeniceWriterFactory veniceWriterFactory =
        IntegrationTestPushUtils.getVeniceWriterFactory(pubSubBrokerWrapper, pubSubProducerAdapterFactory);

    startKey += NUMBER_OF_KEYS; // to have different version having different set of keys
    int endKey = startKey + NUMBER_OF_KEYS;
    int currKey;
    List<Integer> currNonIncPushKeys = new ArrayList<>();

    try (VeniceWriter<byte[], byte[], byte[]> veniceWriter =
        veniceWriterFactory.createVeniceWriter(new VeniceWriterOptions.Builder(topic).build())) {
      veniceWriter.broadcastStartOfPush(true, Collections.emptyMap());

      // generate and insert data into the new version
      Map<byte[], Pair<byte[], byte[]>> inputRecords =
          generateInputWithMetadata(startKey, endKey, true, false, serializer);

      currKey = startKey;
      for (Map.Entry<byte[], Pair<byte[], byte[]>> entry: inputRecords.entrySet()) {
        currNonIncPushKeys.add(currKey++);
        byte[] replicationMetadataWitValueSchemaIdBytes =
            getReplicationMetadataWithValueSchemaId(entry.getValue().getSecond(), 1);

        PutMetadata putMetadata = (new PutMetadata(1, ByteBuffer.wrap(replicationMetadataWitValueSchemaIdBytes)));
        veniceWriter.put(entry.getKey(), entry.getValue().getFirst(), 1, null, putMetadata).get();
      }

      Map<Integer, List<ReplicationMetadataRocksDBStoragePartition>> rocksDBStoragePartitions = new HashMap<>();
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        getPartitionForTopic(topic, rocksDBStoragePartitions);
        for (int colo = 0; colo < NUMBER_OF_COLOS; colo++) {
          for (int replica = 0; replica < NUMBER_OF_REPLICAS; replica++) {
            assertNotNull(rocksDBStoragePartitions.get(colo).get(replica).getValueRocksDBSstFileWriter());
            assertNotNull(rocksDBStoragePartitions.get(colo).get(replica).getRocksDBSstFileWriter());
          }
        }
      });

      // verify the total number of records ingested
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        for (int colo = 0; colo < NUMBER_OF_COLOS; colo++) {
          AtomicInteger totalIngestedKeys = new AtomicInteger();
          AtomicInteger totalIngestedRMDKeys = new AtomicInteger();
          for (int replica = 0; replica < NUMBER_OF_REPLICAS; replica++) {
            ReplicationMetadataRocksDBStoragePartition partition = rocksDBStoragePartitions.get(colo).get(replica);
            totalIngestedKeys.addAndGet((int) partition.getValueRocksDBSstFileWriter().getRecordNumInAllSSTFiles());
            totalIngestedRMDKeys.addAndGet((int) partition.getRocksDBSstFileWriter().getRecordNumInAllSSTFiles());
          }
          assertEquals(totalIngestedKeys.get(), NUMBER_OF_KEYS * NUMBER_OF_REPLICAS);
          assertEquals(totalIngestedRMDKeys.get(), NUMBER_OF_KEYS * NUMBER_OF_REPLICAS);
        }
      });

      /**
       * Mimic non-graceful shutdown of the servers in the midst of sst files being moved to RocksDB.
       * 1. Stop the server
       * 2. delete the SST files (Deleting before graceful shutdown will not help
       *    mimic this scenario as there will be a sync during the graceful shutdown)
       * 3. start the server
       */
      // TBD: Deleting files and restarting servers from more than one colo and/or n-1 replicas
      // results in flaky tests/failures.
      LOGGER.info("Finished Ingestion of all data to SST Files: Delete the sst files");
      for (int colo = 0; colo < 1; colo++) {
        for (int replica = 0; replica < 1; replica++) {
          VeniceServerWrapper serverWrapper = serverWrappers.get(colo).get(replica);
          clusterWrappers.get(colo).stopVeniceServer(serverWrapper.getPort());

          ReplicationMetadataRocksDBStoragePartition partition = rocksDBStoragePartitions.get(colo).get(replica);
          if (deleteSSTFiles) {
            partition.deleteFilesInDirectory(partition.getValueFullPathForTempSSTFileDir());
          }
          if (deleteRMDSSTFiles) {
            partition.deleteFilesInDirectory(partition.getFullPathForTempSSTFileDir());
          }

          clusterWrappers.get(colo).restartVeniceServer(serverWrapper.getPort());
        }
      }

      veniceWriter.broadcastEndOfPush(Collections.emptyMap());
    }

    // Wait for push to be push completed.
    TestUtils.waitForNonDeterministicAssertion(120, TimeUnit.SECONDS, () -> {
      for (int colo = 0; colo < NUMBER_OF_COLOS; colo++) {
        assertEquals(
            clusterWrappers.get(colo)
                .getLeaderVeniceController()
                .getVeniceAdmin()
                .getOffLinePushStatus(clusterWrappers.get(colo).getClusterName(), topic)
                .getExecutionStatus(),
            ExecutionStatus.COMPLETED);
      }
    });

    // Wait for storage node to finish consuming, and new version to be activated
    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
      for (int colo = 0; colo < NUMBER_OF_COLOS; colo++) {
        int currentVersion =
            ControllerClient
                .getStore(
                    clusterWrappers.get(colo).getLeaderVeniceController().getControllerUrl(),
                    clusterWrappers.get(colo).getClusterName(),
                    STORE_NAME)
                .getStore()
                .getCurrentVersion();
        LOGGER.info("colo {} currentVersion {}, pushVersion {}", colo, currentVersion, newVersion);
        if (currentVersion != newVersion) {
          return false;
        }
      }
      return true;
    });

    // validate the ingested data
    AvroGenericStoreClient<String, Object> storeClient = null;

    try {
      D2Client d2Client = D2TestUtils.getD2Client(clusterWrappers.get(NON_SOURCE_COLO).getZk().getAddress(), false);
      D2ClientUtils.startClient(d2Client);
      storeClient = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(STORE_NAME)
              .setForceClusterDiscoveryAtStartTime(true)
              .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
              .setD2Client(d2Client)
              .setVeniceURL(clusterWrappers.get(NON_SOURCE_COLO).getRandomRouterURL())
              .setSslFactory(SslUtils.getVeniceLocalSslFactory())
              .setRetryOnAllErrors(true));

      // invalid keys: all the keys pushed before this version and not re pushed via incremental push
      AvroGenericStoreClient<String, Object> finalStoreClient = storeClient;
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        for (int key: allNonIncPushKeysUntilLastVersion) {
          assertNull(finalStoreClient.get(KEY_PREFIX + key).get());
        }
      });

      // all valid keys
      currKey = startKey;
      while (currKey < endKey) {
        int finalCurrKey = currKey;
        TestUtils.waitForNonDeterministicAssertion(
            30,
            TimeUnit.SECONDS,
            () -> assertNotNull(finalStoreClient.get(KEY_PREFIX + finalCurrKey).get()));
        assertEquals(storeClient.get(KEY_PREFIX + currKey).get().toString(), VALUE_PREFIX + currKey);
        currKey++;
      }
    } finally {
      if (storeClient != null) {
        storeClient.close();
      }
    }

    String incPushVersion = System.currentTimeMillis() + "_test_inc_push";
    versionCreationResponse = parentControllerClient.requestTopicForWrites(
        STORE_NAME,
        1024 * 1024,
        Version.PushType.INCREMENTAL,
        incPushVersion,
        true,
        true,
        false,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        false,
        -1);
    assertFalse(versionCreationResponse.isError());
    String rtTopic = versionCreationResponse.getKafkaTopic();
    assertNotNull(rtTopic);

    // incremental push: the last 10 keys from the batch push
    int incPushStartKey = startKey + 90;
    try (VeniceWriter<byte[], byte[], byte[]> veniceWriter =
        veniceWriterFactory.createVeniceWriter(new VeniceWriterOptions.Builder(rtTopic).build())) {
      veniceWriter.broadcastStartOfIncrementalPush(incPushVersion, new HashMap<>());

      // generate and insert data into the new version
      Map<byte[], Pair<byte[], byte[]>> inputRecordsForIncPush =
          generateInputWithMetadata(incPushStartKey, endKey, false, true, serializer);

      currKey = incPushStartKey;
      for (Map.Entry<byte[], Pair<byte[], byte[]>> entry: inputRecordsForIncPush.entrySet()) {
        allIncPushKeys.add(currKey++);
        currNonIncPushKeys.remove(currNonIncPushKeys.size() - 1);
        byte[] replicationMetadataWitValueSchemaIdBytes =
            getReplicationMetadataWithValueSchemaId(entry.getValue().getSecond(), 1);

        PutMetadata putMetadata = (new PutMetadata(1, ByteBuffer.wrap(replicationMetadataWitValueSchemaIdBytes)));
        veniceWriter.put(entry.getKey(), entry.getValue().getFirst(), 1, null, putMetadata).get();
      }

      veniceWriter.broadcastEndOfIncrementalPush(incPushVersion, Collections.emptyMap());
    }

    storeClient = null;
    try {
      D2Client d2Client = D2TestUtils.getD2Client(clusterWrappers.get(NON_SOURCE_COLO).getZk().getAddress(), false);
      D2ClientUtils.startClient(d2Client);
      storeClient = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(STORE_NAME)
              .setForceClusterDiscoveryAtStartTime(true)
              .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
              .setD2Client(d2Client)
              .setVeniceURL(clusterWrappers.get(NON_SOURCE_COLO).getRandomRouterURL())
              .setSslFactory(SslUtils.getVeniceLocalSslFactory())
              .setRetryOnAllErrors(true));
      // validate the ingested data
      // first 90 keys which should still have original data pushed via full push
      currKey = startKey;
      while (currKey < incPushStartKey) {
        assertEquals(storeClient.get(KEY_PREFIX + currKey).get().toString(), VALUE_PREFIX + currKey);
        currKey++;
      }

      // last 10 keys should be from incremental push
      AvroGenericStoreClient<String, Object> finalStoreClient = storeClient;
      while (currKey < endKey) {
        int finalCurrKey = currKey;
        TestUtils.waitForNonDeterministicAssertion(
            30,
            TimeUnit.SECONDS,
            () -> assertEquals(
                finalStoreClient.get(KEY_PREFIX + finalCurrKey).get().toString(),
                VALUE_PREFIX_INC_PUSH + finalCurrKey));
        currKey++;
      }

      // also check all the incremental push data so far: New versions should get this from RT
      // check setHybridRewindSeconds() config in setup
      for (int key: allIncPushKeys) {
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          assertNotNull(finalStoreClient.get(KEY_PREFIX + key).get());
          assertEquals(finalStoreClient.get(KEY_PREFIX + key).get().toString(), VALUE_PREFIX_INC_PUSH + key);
        });
      }
    } finally {
      if (storeClient != null) {
        storeClient.close();
      }
    }

    // to be used in the next run
    allNonIncPushKeysUntilLastVersion.addAll(currNonIncPushKeys);
  }
}
