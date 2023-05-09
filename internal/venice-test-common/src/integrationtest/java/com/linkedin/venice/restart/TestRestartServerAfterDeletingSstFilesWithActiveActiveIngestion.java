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

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.rocksdb.ReplicationMetadataRocksDBStoragePartition;
import com.linkedin.davinci.store.rocksdb.RocksDBStorageEngine;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.TestVeniceServer;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
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
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.util.BytewiseComparator;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestRestartServerAfterDeletingSstFilesWithActiveActiveIngestion {
  private static final Logger LOGGER =
      LogManager.getLogger(TestRestartServerAfterDeletingSstFilesWithActiveActiveIngestion.class);
  private static final int TEST_TIMEOUT = 360 * Time.MS_PER_SECOND;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, 1).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private String clusterName;
  private VeniceClusterWrapper clusterWrapper;
  private VeniceServerWrapper serverWrapper;
  private AvroSerializer serializer;
  private final int numKeys = 1000;
  private final String KEY_PREFIX = "key";
  private final String VALUE_PREFIX = "value";
  private final String METADATA_PREFIX = "metadata";

  @BeforeClass
  public void setUp() {
    String stringSchemaStr = "\"string\"";
    serializer = new AvroSerializer(AvroCompatibilityHelper.parse(stringSchemaStr));
    Properties serverProperties = new Properties();
    serverProperties.setProperty(ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1));
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    serverProperties.put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB);
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    serverProperties.put(
        CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + DEFAULT_PARENT_DATA_CENTER_REGION_NAME,
        "localhost:" + Utils.getFreePort());
    multiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        Optional.empty(),
        Optional.empty(),
        Optional.of(new VeniceProperties(serverProperties)),
        false);

    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
    clusterName = CLUSTER_NAMES[0];
    clusterWrapper = childDatacenters.get(0).getClusters().get(clusterName);
  }

  @AfterClass
  public void cleanUp() {
    multiRegionMultiClusterWrapper.close();
    TestView.resetCounters();
  }

  private Map<byte[], Pair<byte[], byte[]>> generateInputWithMetadata(
      int startIndex,
      int endIndex,
      boolean sorted,
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
      String value = VALUE_PREFIX + i;
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

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testActiveActiveStoreWithRMDAndRestartServer(boolean deleteSSTFiles, boolean deleteRMDSSTFiles)
      throws Exception {
    // if (deleteSSTFiles == true && deleteRMDSSTFiles == false) return;
    String parentControllerURLs =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));
    ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerURLs);
    TestUtils.assertCommand(
        parentControllerClient.configureActiveActiveReplicationForCluster(
            true,
            VeniceUserStoreType.BATCH_ONLY.toString(),
            Optional.empty()));
    // create a active-active enabled store and run batch push job
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(5)
        .setHybridOffsetLagThreshold(2)
        .setNativeReplicationEnabled(true);
    AvroGenericStoreClient<String, Object> storeClient = null;
    try {
      createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms).close();
      // Create a new version
      VersionCreationResponse versionCreationResponse;
      versionCreationResponse = TestUtils.assertCommand(
          parentControllerClient.requestTopicForWrites(
              storeName,
              1024 * 1024,
              Version.PushType.BATCH,
              Version.guidBasedDummyPushId(),
              true,
              true,
              false,
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              false,
              -1));

      String topic = versionCreationResponse.getKafkaTopic();
      String kafkaUrl = versionCreationResponse.getKafkaBootstrapServers();
      VeniceWriterFactory veniceWriterFactory = TestUtils.getVeniceWriterFactory(kafkaUrl);
      try (VeniceWriter<byte[], byte[], byte[]> veniceWriter =
          veniceWriterFactory.createVeniceWriter(new VeniceWriterOptions.Builder(topic).build())) {
        veniceWriter.broadcastStartOfPush(true, Collections.emptyMap());

        /**
         * Restart storage node during batch ingestion.
         */
        Map<byte[], Pair<byte[], byte[]>> inputRecords = generateInputWithMetadata(0, numKeys, true, serializer);

        for (Map.Entry<byte[], Pair<byte[], byte[]>> entry: inputRecords.entrySet()) {

          byte[] replicationMetadataWitValueSchemaIdBytes =
              getReplicationMetadataWithValueSchemaId(entry.getValue().getSecond(), 1);

          PutMetadata putMetadata = (new PutMetadata(1, ByteBuffer.wrap(replicationMetadataWitValueSchemaIdBytes)));
          veniceWriter.put(entry.getKey(), entry.getValue().getFirst(), 1, null, putMetadata).get();
        }

        serverWrapper = clusterWrapper.getVeniceServers().get(0);
        TestVeniceServer testVeniceServer = serverWrapper.getVeniceServer();
        StorageService storageService = testVeniceServer.getStorageService();

        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
          Assert.assertNotNull(
              storageService.getStorageEngineRepository()
                  .getLocalStorageEngine(versionCreationResponse.getKafkaTopic()));
        });

        RocksDBStorageEngine rocksDBStorageEngine = (RocksDBStorageEngine) storageService.getStorageEngineRepository()
            .getLocalStorageEngine(versionCreationResponse.getKafkaTopic());
        List<ReplicationMetadataRocksDBStoragePartition> rocksDBStoragePartitions = new ArrayList<>();
        rocksDBStoragePartitions
            .add((ReplicationMetadataRocksDBStoragePartition) rocksDBStorageEngine.getPartitionOrThrow(0));

        LOGGER.info("Waiting for the process to Finish ingesting all the data to sst files");
        // 1. wait for rocksDBSstFileWriter to be opened
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
          rocksDBStoragePartitions.stream().forEach(partition -> {
            Assert.assertNotNull(partition.getValueRocksDBSstFileWriter());
            Assert.assertNotNull(partition.getRocksDBSstFileWriter());
          });
        });

        // 2. verify the total number of records ingested
        TestUtils.waitForNonDeterministicAssertion(20, TimeUnit.SECONDS, () -> {
          AtomicInteger totalIngestedKeys = new AtomicInteger();
          AtomicInteger totalIngestedRMDKeys = new AtomicInteger();
          rocksDBStoragePartitions.stream().forEach(partition -> {
            totalIngestedKeys.addAndGet((int) partition.getValueRocksDBSstFileWriter().getRecordNumInAllSSTFiles());
            totalIngestedRMDKeys.addAndGet((int) partition.getRocksDBSstFileWriter().getRecordNumInAllSSTFiles());
          });
          Assert.assertEquals(totalIngestedKeys.get(), numKeys);
          Assert.assertEquals(totalIngestedRMDKeys.get(), numKeys);
        });

        // Delete the sst files to mimic how ingestExternalFile() moves them to RocksDB.
        LOGGER.info("Finished Ingestion of all data to SST Files: Delete the sst files");
        rocksDBStoragePartitions.stream().forEach(partition -> {
          if (deleteSSTFiles) {
            partition.deleteSSTFiles(partition.getValueFullPathForTempSSTFileDir());
          }
          if (deleteRMDSSTFiles) {
            partition.deleteSSTFiles(partition.getFullPathForTempSSTFileDir());
          }
        });

        // Restart server
        clusterWrapper.stopVeniceServer(serverWrapper.getPort());
        clusterWrapper.restartVeniceServer(serverWrapper.getPort());

        veniceWriter.broadcastEndOfPush(Collections.emptyMap());
      }

      // Wait for push to be push completed.
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        Assert.assertTrue(
            clusterWrapper.getLeaderVeniceController()
                .getVeniceAdmin()
                .getOffLinePushStatus(clusterWrapper.getClusterName(), topic)
                .getExecutionStatus()
                .equals(ExecutionStatus.COMPLETED));
      });

      // validate the data that was ingested properly
      storeClient = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName)
              .setVeniceURL(clusterWrapper.getRandomRouterURL())
              .setSslFactory(SslUtils.getVeniceLocalSslFactory()));

      int currkey = 0;
      int endKey = numKeys;

      // 1. invalid key
      Assert.assertNull(storeClient.get(KEY_PREFIX + (currkey - 1)).get());

      // 2. all valid keys
      while (currkey < endKey) {
        Assert.assertEquals(storeClient.get(KEY_PREFIX + currkey).get().toString(), VALUE_PREFIX + currkey);
        currkey++;
      }
    } finally {
      if (storeClient != null) {
        storeClient.close();
      }
      parentControllerClient.disableAndDeleteStore(storeName);
    }
  }
}
