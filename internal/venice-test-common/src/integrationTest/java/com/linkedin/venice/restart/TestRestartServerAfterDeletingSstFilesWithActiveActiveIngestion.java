package com.linkedin.venice.restart;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.CHILD_DATA_CENTER_KAFKA_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithUserSchema;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.rocksdb.ReplicationMetadataRocksDBStoragePartition;
import com.linkedin.davinci.store.rocksdb.RocksDBStorageEngine;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.TestVeniceServer;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.VeniceUserStoreType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
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
  private static final int TEST_TIMEOUT = 120 * Time.MS_PER_SECOND;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private VeniceClusterWrapper clusterWrapper;
  private VeniceServerWrapper serverWrapper;
  private ControllerClient parentControllerClient;
  private AvroSerializer serializer;
  private final int numKeys = 100;
  private int startKey = 0;
  private int newVersion = 0;
  private final String KEY_PREFIX = "key";
  private final String VALUE_PREFIX = "value";
  private final String VALUE_PREFIX_INC_PUSH = "value-inc";
  private final String METADATA_PREFIX = "metadata";
  private String storeName = Utils.getUniqueString("store");
  private final int numServers = 5;
  List<Integer> allIncPushKeys = new ArrayList<>(); // all keys ingested via incremental push
  List<Integer> allNonIncPushKeysUntilLastVersion = new ArrayList<>(); // all keys ingested only via batch push

  @BeforeClass
  public void setUp() throws Exception {
    String stringSchemaStr = "\"string\"";
    serializer = new AvroSerializer(AvroCompatibilityHelper.parse(stringSchemaStr));
    Properties serverProperties = new Properties();
    serverProperties.setProperty(ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1));
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    serverProperties.put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB);
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    serverProperties.put(
        CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + DEFAULT_PARENT_DATA_CENTER_REGION_NAME,
        "localhost:" + TestUtils.getFreePort());
    multiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        1,
        1,
        1,
        1,
        numServers,
        1,
        1,
        Optional.empty(),
        Optional.empty(),
        Optional.of(new VeniceProperties(serverProperties)),
        false);

    List<VeniceMultiClusterWrapper> childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    List<VeniceControllerWrapper> parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
    String clusterName = "venice-cluster0";
    clusterWrapper = childDatacenters.get(0).getClusters().get(clusterName);

    String parentControllerURLs =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));
    parentControllerClient = new ControllerClient(clusterName, parentControllerURLs);
    TestUtils.assertCommand(
        parentControllerClient.configureActiveActiveReplicationForCluster(
            true,
            VeniceUserStoreType.INCREMENTAL_PUSH.toString(),
            Optional.empty()));
    // create a active-active enabled store
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(500)
        .setHybridOffsetLagThreshold(2)
        .setNativeReplicationEnabled(true)
        .setBackupVersionRetentionMs(1)
        .setIncrementalPushEnabled(true);
    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms).close();
  }

  @AfterClass
  public void cleanUp() {
    parentControllerClient.disableAndDeleteStore(storeName);
    multiRegionMultiClusterWrapper.close();
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
      List<ReplicationMetadataRocksDBStoragePartition> rocksDBStoragePartitions) {
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      int i;
      for (i = 0; i < numServers; i++) {
        serverWrapper = clusterWrapper.getVeniceServers().get(i);
        if (serverWrapper.getVeniceServer()
            .getStorageService()
            .getStorageEngineRepository()
            .getLocalStorageEngine(topic) != null) {
          LOGGER.info("selected server is: {}", i);
          break;
        }
      }
      assertFalse(i == numServers);
    });

    TestVeniceServer testVeniceServer = serverWrapper.getVeniceServer();
    StorageService storageService = testVeniceServer.getStorageService();
    RocksDBStorageEngine rocksDBStorageEngine =
        (RocksDBStorageEngine) storageService.getStorageEngineRepository().getLocalStorageEngine(topic);
    assertNotNull(rocksDBStorageEngine);
    assertEquals(rocksDBStorageEngine.getNumberOfPartitions(), 1);

    rocksDBStoragePartitions.clear();
    rocksDBStoragePartitions
        .add((ReplicationMetadataRocksDBStoragePartition) rocksDBStorageEngine.getPartitionOrThrow(0));
  }

  /**
   * This test include below steps:
   * 1. Batch Push data without EOP (100 keys)
   * 2. Delete SST files (based on params)
   * 3. restart servers
   * 4. Validate whether the data is ingested
   * 5. Incremental push data (10 keys (90-100 of the batch push))
   * 6. Validate whether the data is ingested
   * 7. Validate whether all the data from RT is ingested to the new versions as well.
   */
  @Test(timeOut = TEST_TIMEOUT, dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testActiveActiveStoreWithRMDAndRestartServer(boolean deleteSSTFiles, boolean deleteRMDSSTFiles)
      throws Exception {
    // Create a new version
    VersionCreationResponse versionCreationResponse;
    versionCreationResponse = TestUtils.assertCommand(
        parentControllerClient.requestTopicForWrites(
            storeName,
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

    String topic = versionCreationResponse.getKafkaTopic();
    String kafkaUrl = versionCreationResponse.getKafkaBootstrapServers();
    VeniceWriterFactory veniceWriterFactory = TestUtils.getVeniceWriterFactory(kafkaUrl);

    startKey += numKeys; // to have different version having different set of keys
    int endKey = startKey + numKeys;
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

      List<ReplicationMetadataRocksDBStoragePartition> rocksDBStoragePartitions = new ArrayList<>();
      String finalTopic = topic;
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        getPartitionForTopic(finalTopic, rocksDBStoragePartitions);
        assertNotNull(rocksDBStoragePartitions.get(0).getValueRocksDBSstFileWriter());
        assertNotNull(rocksDBStoragePartitions.get(0).getRocksDBSstFileWriter());
      });

      // 2. verify the total number of records ingested
      TestUtils.waitForNonDeterministicAssertion(20, TimeUnit.SECONDS, () -> {
        AtomicInteger totalIngestedKeys = new AtomicInteger();
        AtomicInteger totalIngestedRMDKeys = new AtomicInteger();
        rocksDBStoragePartitions.stream().forEach(partition -> {
          totalIngestedKeys.addAndGet((int) partition.getValueRocksDBSstFileWriter().getRecordNumInAllSSTFiles());
          totalIngestedRMDKeys.addAndGet((int) partition.getRocksDBSstFileWriter().getRecordNumInAllSSTFiles());
        });
        assertEquals(totalIngestedKeys.get(), numKeys);
        assertEquals(totalIngestedRMDKeys.get(), numKeys);
      });

      // Delete the sst files to mimic how ingestExternalFile() moves them to RocksDB.
      LOGGER.info("Finished Ingestion of all data to SST Files: Delete the sst files");
      rocksDBStoragePartitions.stream().forEach(partition -> {
        if (deleteSSTFiles) {
          partition.deleteFilesInDirectory(partition.getValueFullPathForTempSSTFileDir());
        }
        if (deleteRMDSSTFiles) {
          partition.deleteFilesInDirectory(partition.getFullPathForTempSSTFileDir());
        }
      });

      // Restart server
      clusterWrapper.stopVeniceServer(serverWrapper.getPort());
      clusterWrapper.restartVeniceServer(serverWrapper.getPort());

      veniceWriter.broadcastEndOfPush(Collections.emptyMap());
    }

    // Wait for push to be push completed.
    String finalTopic1 = topic;
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
      assertEquals(
          clusterWrapper.getLeaderVeniceController()
              .getVeniceAdmin()
              .getOffLinePushStatus(clusterWrapper.getClusterName(), finalTopic1)
              .getExecutionStatus(),
          ExecutionStatus.COMPLETED);
    });

    // Wait for storage node to finish consuming, and new version to be activated
    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
      int currentVersion =
          ControllerClient
              .getStore(
                  clusterWrapper.getLeaderVeniceController().getControllerUrl(),
                  clusterWrapper.getClusterName(),
                  storeName)
              .getStore()
              .getCurrentVersion();
      LOGGER.info("currentVersion {}, pushVersion {}", currentVersion, newVersion);
      return currentVersion == newVersion;
    });

    // validate the ingested data
    AvroGenericStoreClient<String, Object> storeClient = null;
    try {
      D2Client d2Client = D2TestUtils.getD2Client(clusterWrapper.getZk().getAddress(), false);
      D2ClientUtils.startClient(d2Client);
      storeClient = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName)
              .setForceClusterDiscoveryAtStartTime(true)
              .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
              .setD2Client(d2Client)
              .setVeniceURL(clusterWrapper.getRandomRouterURL())
              .setSslFactory(SslUtils.getVeniceLocalSslFactory())
              .setRetryOnAllErrors(true));

      // 1. invalid keys: all the keys pushed before this version and not repushed via incremental push
      AvroGenericStoreClient<String, Object> finalStoreClient = storeClient;
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        for (int key: allNonIncPushKeysUntilLastVersion) {
          assertNull(finalStoreClient.get(KEY_PREFIX + key).get());
        }
      });

      // 2. all valid keys
      currKey = startKey;
      while (currKey < endKey) {
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
        storeName,
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
    topic = versionCreationResponse.getKafkaTopic();
    assertNotNull(topic);

    // incremental push: the last 10 keys from the batch push
    int incPushStartKey = startKey + 90;
    try (VeniceWriter<byte[], byte[], byte[]> veniceWriter =
        veniceWriterFactory.createVeniceWriter(new VeniceWriterOptions.Builder(topic).build())) {
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
      D2Client d2Client = D2TestUtils.getD2Client(clusterWrapper.getZk().getAddress(), false);
      D2ClientUtils.startClient(d2Client);
      storeClient = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName)
              .setForceClusterDiscoveryAtStartTime(true)
              .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
              .setD2Client(d2Client)
              .setVeniceURL(clusterWrapper.getRandomRouterURL())
              .setSslFactory(SslUtils.getVeniceLocalSslFactory())
              .setRetryOnAllErrors(true));
      // validate the ingested data
      // 1. first 90 keys which should still have original data pushed via full push
      currKey = startKey;
      while (currKey < incPushStartKey) {
        assertEquals(storeClient.get(KEY_PREFIX + currKey).get().toString(), VALUE_PREFIX + currKey);
        currKey++;
      }

      // 2. last 10 keys should be from incremental push
      while (currKey < endKey) {
        int finalCurrKey = currKey;
        AvroGenericStoreClient<String, Object> finalStoreClient1 = storeClient;
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          assertEquals(
              finalStoreClient1.get(KEY_PREFIX + finalCurrKey).get().toString(),
              VALUE_PREFIX_INC_PUSH + finalCurrKey);
        });
        currKey++;
      }

      // also check all the incremental push data so far: New versions should get this from RT
      // check setHybridRewindSeconds() config in setup
      for (int key: allIncPushKeys) {
        AvroGenericStoreClient<String, Object> finalStoreClient2 = storeClient;
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          assertNotNull(finalStoreClient2.get(KEY_PREFIX + key).get());
          assertEquals(finalStoreClient2.get(KEY_PREFIX + key).get().toString(), VALUE_PREFIX_INC_PUSH + key);
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
