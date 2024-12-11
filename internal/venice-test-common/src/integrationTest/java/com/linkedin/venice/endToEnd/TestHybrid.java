package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOB_FILES_ENABLED;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.SERVER_CONSUMER_POOL_ALLOCATION_STRATEGY;
import static com.linkedin.venice.ConfigKeys.SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_DEDICATED_DRAINER_FOR_SORTED_INPUT_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_SHARED_CONSUMER_ASSIGNMENT_STRATEGY;
import static com.linkedin.venice.ConfigKeys.SSL_TO_KAFKA_LEGACY;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_VALUE_SCHEMA;
import static com.linkedin.venice.meta.BufferReplayPolicy.REWIND_FROM_EOP;
import static com.linkedin.venice.meta.BufferReplayPolicy.REWIND_FROM_SOP;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE;
import static com.linkedin.venice.router.api.VenicePathParser.TYPE_STORAGE;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducerConfig;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.runVPJ;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendCustomSizeStreamingRecord;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFER_VERSION_SWAP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.luben.zstd.Zstd;
import com.linkedin.davinci.kafka.consumer.KafkaConsumerService;
import com.linkedin.davinci.kafka.consumer.KafkaConsumerServiceDelegator;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.RecordTooLargeException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixBaseRoutingRepository;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.LeaderMetadata;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.InstanceStatus;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.StoreStatus;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.producer.VeniceProducer;
import com.linkedin.venice.producer.online.OnlineProducerFactory;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.samza.SamzaExitMode;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.serializer.AvroGenericDeserializer;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.CompletableFutureCallback;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.SystemProducer;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestHybrid {
  private static final Logger LOGGER = LogManager.getLogger(TestHybrid.class);
  public static final int STREAMING_RECORD_SIZE = 1024;

  /**
   * IMPORTANT NOTE: if you use this sharedVenice cluster, please do not close it. The {@link #cleanUp()} function
   *                 will take care of it. Besides, if any backend component of the shared cluster is stopped in
   *                 the middle of the test, please restart them at the end of your test.
   */
  private VeniceClusterWrapper sharedVenice;

  /**
   * This cluster is re-used by some of the tests, in order to speed up the suite. Some other tests require
   * certain specific characteristics which makes it awkward to re-use, though not necessarily impossible.
   * Further reuse of this shared cluster can be attempted later.
   */
  @BeforeClass(alwaysRun = true)
  public void setUp() {
    sharedVenice = setUpCluster(false);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(sharedVenice);
  }

  /**
   * N.B.: Non-L/F does not support chunking, so this permutation is skipped.
   */
  @DataProvider(name = "testPermutations", parallel = false)
  public static Object[][] testPermutations() {
    return new Object[][] { { false, false, REWIND_FROM_EOP }, { false, true, REWIND_FROM_EOP },
        { true, false, REWIND_FROM_EOP }, { true, true, REWIND_FROM_EOP }, { false, false, REWIND_FROM_SOP },
        { false, true, REWIND_FROM_SOP }, { true, false, REWIND_FROM_SOP }, { true, true, REWIND_FROM_SOP } };
  }

  /**
   * This test validates the hybrid batch + streaming semantics and verifies that configured rewind time works as expected.
   *
   * TODO: This test needs to be refactored in order to leverage {@link TestMockTime},
   *       which would allow the test to run faster and more deterministically.
   *
   * @param multiDivStream if false, rewind will happen in the middle of a DIV Segment, which was originally broken.
   *                       if true, two independent DIV Segments will be placed before and after the start of buffer replay.
   *
   *                       If this test succeeds with {@param multiDivStream} set to true, but fails with it set to false,
   *                       then there is a regression in the DIV partial segment tolerance after EOP.
   * @param chunkingEnabled Whether chunking should be enabled.
   */
  @Test(dataProvider = "testPermutations", timeOut = 180 * Time.MS_PER_SECOND, groups = { "flaky" })
  public void testHybridEndToEnd(boolean multiDivStream, boolean chunkingEnabled, BufferReplayPolicy bufferReplayPolicy)
      throws Exception {
    LOGGER.info("About to create VeniceClusterWrapper");
    Properties extraProperties = new Properties();
    extraProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(3L));
    if (chunkingEnabled) {
      // We exercise chunking by setting the servers' max size arbitrarily low. For now, since the RT topic
      // does not support chunking, and write compute is not merged yet, there is no other way to make the
      // store-version data bigger than the RT data and thus have chunked values produced.
      int maxMessageSizeInServer = STREAMING_RECORD_SIZE / 2;
      extraProperties.setProperty(
          VeniceWriter.MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES,
          Integer.toString(maxMessageSizeInServer));
    }

    SystemProducer veniceProducer = null;

    // N.B.: RF 2 with 2 servers is important, in order to test both the leader and follower code paths
    VeniceClusterWrapper venice = sharedVenice;
    try {
      LOGGER.info("Finished creating VeniceClusterWrapper");

      long streamingRewindSeconds = 10L;
      long streamingMessageLag = 2L;

      String storeName = Utils.getUniqueString("hybrid-store");
      File inputDir = getTempDataDirectory();
      String inputDirPath = "file://" + inputDir.getAbsolutePath();
      Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir); // records 1-100
      Properties vpjProperties = defaultVPJProps(venice, inputDirPath, storeName);

      try (ControllerClient controllerClient = createStoreForJob(venice.getClusterName(), recordSchema, vpjProperties);
          AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
              ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(venice.getRandomRouterURL()));
          TopicManager topicManager =
              IntegrationTestPushUtils
                  .getTopicManagerRepo(
                      PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE,
                      100,
                      0l,
                      venice.getPubSubBrokerWrapper(),
                      sharedVenice.getPubSubTopicRepository())
                  .getLocalTopicManager()) {

        Cache cacheNothingCache = Mockito.mock(Cache.class);
        Mockito.when(cacheNothingCache.getIfPresent(Mockito.any())).thenReturn(null);
        topicManager.setTopicConfigCache(cacheNothingCache);

        ControllerResponse response = controllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setHybridRewindSeconds(streamingRewindSeconds)
                .setHybridOffsetLagThreshold(streamingMessageLag)
                .setChunkingEnabled(chunkingEnabled)
                .setHybridBufferReplayPolicy(bufferReplayPolicy));

        Assert.assertFalse(response.isError());

        // Do a VPJ push
        runVPJ(vpjProperties, 1, controllerClient);

        // verify the topic compaction policy
        PubSubTopic topicForStoreVersion1 =
            sharedVenice.getPubSubTopicRepository().getTopic(Version.composeKafkaTopic(storeName, 1));
        Assert.assertTrue(
            topicManager.isTopicCompactionEnabled(topicForStoreVersion1),
            "topic: " + topicForStoreVersion1 + " should have compaction enabled");
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

        // write streaming records
        veniceProducer = getSamzaProducer(venice, storeName, Version.PushType.STREAM);
        for (int i = 1; i <= 10; i++) {
          // The batch values are small, but the streaming records are "big" (i.e.: not that big, but bigger than
          // the server's max configured chunk size). In the scenario where chunking is disabled, the server's
          // max chunk size is not altered, and thus this will be under threshold.
          sendCustomSizeStreamingRecord(veniceProducer, storeName, i, STREAMING_RECORD_SIZE);
        }
        if (multiDivStream) {
          veniceProducer.stop(); // close out the DIV segment
        }

        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
          try {
            checkLargeRecord(client, 2);
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });

        // Update min and max compaction lag
        long expectedMinCompactionLagSeconds = TimeUnit.MINUTES.toSeconds(10); // 10mins
        long expectedMaxCompactionLagSeconds = TimeUnit.MINUTES.toSeconds(20); // 20mins
        controllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setMinCompactionLagSeconds(expectedMinCompactionLagSeconds)
                .setMaxCompactionLagSeconds(expectedMaxCompactionLagSeconds));

        // Run one more VPJ
        runVPJ(vpjProperties, 2, controllerClient);
        // verify the topic compaction policy
        PubSubTopic topicForStoreVersion2 =
            sharedVenice.getPubSubTopicRepository().getTopic(Version.composeKafkaTopic(storeName, 2));
        Assert.assertTrue(
            topicManager.isTopicCompactionEnabled(topicForStoreVersion2),
            "topic: " + topicForStoreVersion2 + " should have compaction enabled");
        long expectedMinCompactionLagMS = TimeUnit.SECONDS.toMillis(expectedMinCompactionLagSeconds);
        Assert.assertEquals(
            topicManager.getTopicMinLogCompactionLagMs(topicForStoreVersion2),
            expectedMinCompactionLagMS,
            "topic:" + topicForStoreVersion2 + " should have min compaction lag config set to "
                + expectedMinCompactionLagMS);
        long expectedMaxCompactionLagMS = TimeUnit.SECONDS.toMillis(expectedMaxCompactionLagSeconds);
        Assert.assertEquals(
            topicManager.getTopicMaxLogCompactionLagMs(topicForStoreVersion2).get().longValue(),
            expectedMaxCompactionLagMS,
            "topic:" + topicForStoreVersion2 + " should have max compaction lag config set to "
                + expectedMaxCompactionLagMS);

        // Verify streaming record in second version
        checkLargeRecord(client, 2);
        assertEquals(client.get("19").get().toString(), "test_name_19");

        // TODO: Would be great to eliminate this wait time...
        LOGGER.info("***** Sleeping to get outside of rewind time: {} seconds", streamingRewindSeconds);
        Utils.sleep(TimeUnit.MILLISECONDS.convert(streamingRewindSeconds, TimeUnit.SECONDS));

        // Write more streaming records
        if (multiDivStream) {
          veniceProducer = getSamzaProducer(venice, storeName, Version.PushType.STREAM); // new producer, new DIV
                                                                                         // segment.
        }
        for (int i = 10; i <= 20; i++) {
          sendCustomSizeStreamingRecord(veniceProducer, storeName, i, STREAMING_RECORD_SIZE);
        }
        TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, () -> {
          try {
            checkLargeRecord(client, 19);
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });

        // Run VPJ a third Time
        runVPJ(vpjProperties, 3, controllerClient);
        // verify the topic compaction policy
        PubSubTopic topicForStoreVersion3 =
            sharedVenice.getPubSubTopicRepository().getTopic(Version.composeKafkaTopic(storeName, 3));
        Assert.assertTrue(
            topicManager.isTopicCompactionEnabled(topicForStoreVersion3),
            "topic: " + topicForStoreVersion3 + " should have compaction enabled");

        // Verify new streaming record in third version
        TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, () -> {
          try {
            checkLargeRecord(client, 19);
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });
        // But not old streaming record (because we waited the rewind time)
        assertEquals(client.get("2").get().toString(), "test_name_2");

        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
          StoreResponse storeResponse = controllerClient.getStore(storeName);
          assertFalse(storeResponse.isError());
          List<Integer> versions =
              storeResponse.getStore().getVersions().stream().map(Version::getNumber).collect(Collectors.toList());
          assertFalse(versions.contains(1), "After version 3 comes online, version 1 should be retired");
          assertTrue(versions.contains(2));
          assertTrue(versions.contains(3));
        });

        controllerClient.listInstancesStatuses(false)
            .getInstancesStatusMap()
            .keySet()
            .forEach(
                s -> LOGGER.info(
                    "Replicas for {}: {}",
                    s,
                    Arrays.toString(controllerClient.listStorageNodeReplicas(s).getReplicas())));

        // TODO will move this test case to a single fail-over integration test.
        // Stop one server
        int port = venice.getVeniceServers().get(0).getPort();
        venice.stopVeniceServer(port);
        TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, true, true, () -> {
          // Make sure Helix knows the instance is shutdown
          Map<String, String> storeStatus = controllerClient.listStoresStatuses().getStoreStatusMap();
          assertEquals(
              StoreStatus.UNDER_REPLICATED.toString(),
              storeStatus.get(storeName),
              "Should be UNDER_REPLICATED");

          Map<String, String> instanceStatus = controllerClient.listInstancesStatuses(false).getInstancesStatusMap();
          Assert.assertTrue(
              instanceStatus.entrySet()
                  .stream()
                  .filter(entry -> entry.getKey().contains(Integer.toString(port)))
                  .map(Map.Entry::getValue)
                  .allMatch(s -> s.equals(InstanceStatus.DISCONNECTED.toString())),
              "Storage Node on port " + port + " should be DISCONNECTED");
        });

        // Restart one server
        venice.restartVeniceServer(port);
        TestUtils.waitForNonDeterministicAssertion(15, TimeUnit.SECONDS, true, true, () -> {
          // Make sure Helix knows the instance has recovered
          Map<String, String> storeStatus = controllerClient.listStoresStatuses().getStoreStatusMap();
          assertEquals(
              StoreStatus.FULLLY_REPLICATED.toString(),
              storeStatus.get(storeName),
              "Should be FULLLY_REPLICATED");
        });
      }
    } finally {
      if (veniceProducer != null) {
        veniceProducer.stop();
      }
    }
  }

  private static VeniceCompressor getVeniceCompressor(
      CompressionStrategy compressionStrategy,
      String storeName,
      int storeVersion,
      VeniceClusterWrapper venice,
      CloseableHttpAsyncClient storageNodeClient) throws IOException, ExecutionException, InterruptedException {
    CompressorFactory compressorFactory = new CompressorFactory();
    if (compressionStrategy.equals(CompressionStrategy.ZSTD_WITH_DICT)) {
      // query the dictionary
      VeniceServerWrapper serverWrapper = venice.getVeniceServers().get(0);
      StringBuilder sb = new StringBuilder().append("http://")
          .append(serverWrapper.getAddress())
          .append("/")
          .append(QueryAction.DICTIONARY.toString().toLowerCase())
          .append("/")
          .append(storeName)
          .append("/")
          .append(storeVersion);
      HttpGet getReq = new HttpGet(sb.toString());
      try (InputStream bodyStream = storageNodeClient.execute(getReq, null).get().getEntity().getContent()) {
        byte[] dictionary = IOUtils.toByteArray(bodyStream);
        return compressorFactory.createCompressorWithDictionary(dictionary, Zstd.maxCompressionLevel());
      } catch (InterruptedException | ExecutionException e) {
        throw e;
      }
    } else {
      return compressorFactory.getCompressor(compressionStrategy);
    }
  }

  private void checkLargeRecord(AvroGenericStoreClient client, int index)
      throws ExecutionException, InterruptedException {
    String key = Integer.toString(index);
    String value = client.get(key).get().toString();
    assertEquals(
        value.length(),
        STREAMING_RECORD_SIZE,
        "Expected a large record for key '" + key + "' but instead got: '" + value + "'.");

    String expectedChar = Integer.toString(index).substring(0, 1);
    for (int i = 0; i < value.length(); i++) {
      assertEquals(value.substring(i, i + 1), expectedChar);
    }
  }

  /**
   * A comprehensive integration test for RP job. We set up RF to be 2 in the cluster and spin up 3 SNs nodes here.
   * 2 RF is required to be the correctness for both leader and follower's behavior. A spare SN is also added for
   * testing whether the flow can work while the original leader dies.
   *
   */
  @Test(timeOut = 180 * Time.MS_PER_SECOND, enabled = false)
  public void testSamzaBatchLoad() throws Exception {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB.name());
    extraProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    extraProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1L));

    SystemProducer veniceBatchProducer = null;
    try (VeniceClusterWrapper veniceClusterWrapper =
        ServiceFactory.getVeniceCluster(1, 3, 1, 2, 1000000, false, false, extraProperties)) {
      try {
        Admin admin = veniceClusterWrapper.getLeaderVeniceController().getVeniceAdmin();
        String clusterName = veniceClusterWrapper.getClusterName();
        String storeName = Utils.getUniqueString("test-store");
        long streamingRewindSeconds = 25L;
        long streamingMessageLag = 2L;

        // Create empty store
        admin.createStore(clusterName, storeName, "tester", "\"string\"", "\"string\"");
        admin.updateStore(
            clusterName,
            storeName,
            new UpdateStoreQueryParams().setPartitionCount(1)
                .setHybridRewindSeconds(streamingRewindSeconds)
                .setHybridOffsetLagThreshold(streamingMessageLag)
                .setChunkingEnabled(true));

        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, true, () -> {
          Assert.assertFalse(admin.getStore(clusterName, storeName).containsVersion(1));
          Assert.assertEquals(admin.getStore(clusterName, storeName).getCurrentVersion(), 0);
        });

        // Batch load from Samza
        VeniceSystemFactory factory = new VeniceSystemFactory();
        Version.PushType pushType = Version.PushType.STREAM_REPROCESSING;
        Map<String, String> samzaConfig = getSamzaProducerConfig(veniceClusterWrapper, storeName, pushType);
        veniceBatchProducer = factory.getProducer("venice", new MapConfig(samzaConfig), null);
        veniceBatchProducer.start();
        if (veniceBatchProducer instanceof VeniceSystemProducer) {
          // The default behavior would exit the process
          ((VeniceSystemProducer) veniceBatchProducer).setExitMode(SamzaExitMode.NO_OP);
        }

        // Purposefully out of order, because Samza batch jobs should be allowed to write out of order
        for (int i = 10; i >= 1; i--) {
          sendStreamingRecord(veniceBatchProducer, storeName, i);
        }

        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, true, () -> {
          Assert.assertTrue(admin.getStore(clusterName, storeName).containsVersion(1));
          Assert.assertEquals(admin.getStore(clusterName, storeName).getCurrentVersion(), 0);
        });

        // while running in L/F model, we try to stop the original SN; let Helix elect a new leader and push some extra
        // data here. This is for testing "pass-through" mode is working properly
        // wait a little time to make sure the leader has re-produced all existing messages
        long waitTime = TimeUnit.SECONDS.toMillis(
            Integer.parseInt(extraProperties.getProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS)) + 2);
        Utils.sleep(waitTime);

        String resourceName = Version.composeKafkaTopic(storeName, 1);
        HelixBaseRoutingRepository routingDataRepo =
            veniceClusterWrapper.getRandomVeniceRouter().getRoutingDataRepository();
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, true, () -> {
          Instance leaderNode = routingDataRepo.getLeaderInstance(resourceName, 0);
          Assert.assertNotNull(leaderNode);
        });
        Instance oldLeaderNode = routingDataRepo.getLeaderInstance(resourceName, 0);

        veniceClusterWrapper.stopVeniceServer(oldLeaderNode.getPort());
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
          Instance newLeaderNode = routingDataRepo.getLeaderInstance(resourceName, 0);
          Assert.assertNotNull(newLeaderNode);
          Assert.assertNotEquals(oldLeaderNode.getPort(), newLeaderNode.getPort());
          Assert.assertTrue(
              routingDataRepo.getPartitionAssignments(resourceName).getPartition(0).getWorkingInstances().size() == 2);
        });

        for (int i = 21; i <= 30; i++) {
          sendCustomSizeStreamingRecord(veniceBatchProducer, storeName, i, ByteUtils.BYTES_PER_MB);
        }

        for (int i = 31; i <= 40; i++) {
          sendStreamingRecord(veniceBatchProducer, storeName, i);
        }

        // Before EOP, the Samza batch producer should still be in active state
        Assert.assertEquals(factory.getNumberOfActiveSystemProducers(), 1);

        /**
         * Use the same VeniceWriter to write END_OF_PUSH message, which will guarantee the message order in topic
         */
        ((VeniceSystemProducer) veniceBatchProducer).getInternalProducer().broadcastEndOfPush(new HashMap<>());

        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
          Assert.assertTrue(admin.getStore(clusterName, storeName).containsVersion(1));
          Assert.assertEquals(admin.getStore(clusterName, storeName).getCurrentVersion(), 1);
          // After EOP, the push monitor inside the system producer would mark the producer as inactive in the factory
          Assert.assertEquals(factory.getNumberOfActiveSystemProducers(), 0);
        });

        SystemProducer veniceStreamProducer =
            getSamzaProducer(veniceClusterWrapper, storeName, Version.PushType.STREAM);
        try (AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName)
                .setVeniceURL(veniceClusterWrapper.getRandomRouterURL()))) {
          TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
            // Verify data, note only 1-10 have been pushed so far
            for (int i = 1; i <= 10; i++) {
              String key = Integer.toString(i);
              Object value = client.get(key).get();
              Assert.assertNotNull(value);
              Assert.assertEquals(value.toString(), "stream_" + key);
            }
          });

          Assert.assertNull(client.get(Integer.toString(11)).get(), "This record should not be found");

          // Should find large values
          for (int i = 21; i <= 30; i++) {
            String key = Integer.toString(i);
            Object value = client.get(key).get();
            assertNotNull(value, "Key " + i + " should not be missing!");
          }

          for (int i = 31; i <= 40; i++) {
            String key = Integer.toString(i);
            Assert.assertEquals(client.get(key).get().toString(), "stream_" + key);
          }

          // Switch to stream mode and push more data
          veniceStreamProducer.start();
          for (int i = 11; i <= 20; i++) {
            sendStreamingRecord(veniceStreamProducer, storeName, i);
          }
          Assert.assertThrows(
              RecordTooLargeException.class,
              () -> sendCustomSizeStreamingRecord(veniceStreamProducer, storeName, 0, ByteUtils.BYTES_PER_MB));

          Assert.assertTrue(admin.getStore(clusterName, storeName).containsVersion(1));
          Assert.assertFalse(admin.getStore(clusterName, storeName).containsVersion(2));
          Assert.assertEquals(admin.getStore(clusterName, storeName).getCurrentVersion(), 1);

          // Verify both batch and stream data
          /**
           * Leader would wait for 5 seconds before switching to real-time topic.
           */
          long extraWaitTime = TimeUnit.SECONDS
              .toMillis(Long.parseLong(extraProperties.getProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS)));
          long normalTimeForConsuming = TimeUnit.SECONDS.toMillis(3);
          LOGGER.info("normalTimeForConsuming: {} ms; extraWaitTime: {} ms", normalTimeForConsuming, extraWaitTime);
          Utils.sleep(normalTimeForConsuming + extraWaitTime);
          TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, false, true, () -> {
            for (int i = 1; i < 20; i++) {
              String key = Integer.toString(i);
              Assert.assertEquals(client.get(key).get().toString(), "stream_" + key);
            }
            Assert.assertNull(client.get(Integer.toString(41)).get(), "This record should not be found");
          });
        } finally {
          veniceStreamProducer.stop();
        }
      } finally {
        if (veniceBatchProducer != null) {
          veniceBatchProducer.stop();
        }
      }
    }
  }

  @Test(timeOut = 180 * Time.MS_PER_SECOND, enabled = false)
  public void testMultiStreamReprocessingSystemProducers() {
    SystemProducer veniceBatchProducer1 = null, veniceBatchProducer2 = null;
    try {
      VeniceClusterWrapper veniceClusterWrapper = sharedVenice;
      Admin admin = veniceClusterWrapper.getLeaderVeniceController().getVeniceAdmin();
      String clusterName = veniceClusterWrapper.getClusterName();
      String storeName1 = Utils.getUniqueString("test-store1");
      String storeName2 = Utils.getUniqueString("test-store2");
      long streamingRewindSeconds = 25L;
      long streamingMessageLag = 2L;

      // create 2 stores
      // Create empty store
      admin.createStore(clusterName, storeName1, "tester", "\"string\"", "\"string\"");
      admin.createStore(clusterName, storeName2, "tester", "\"string\"", "\"string\"");
      UpdateStoreQueryParams storeSettings = new UpdateStoreQueryParams().setHybridRewindSeconds(streamingRewindSeconds)
          .setHybridOffsetLagThreshold(streamingMessageLag);
      admin.updateStore(clusterName, storeName1, storeSettings);
      admin.updateStore(clusterName, storeName2, storeSettings);
      veniceClusterWrapper.getVeniceRouters().get(0).refresh();
      Assert.assertFalse(admin.getStore(clusterName, storeName1).containsVersion(1));
      Assert.assertEquals(admin.getStore(clusterName, storeName1).getCurrentVersion(), 0);
      Assert.assertFalse(admin.getStore(clusterName, storeName2).containsVersion(1));
      Assert.assertEquals(admin.getStore(clusterName, storeName2).getCurrentVersion(), 0);

      // Batch load from Samza to both stores
      VeniceSystemFactory factory = new VeniceSystemFactory();
      Map<String, String> samzaConfig1 =
          getSamzaProducerConfig(veniceClusterWrapper, storeName1, Version.PushType.STREAM_REPROCESSING);
      veniceBatchProducer1 = factory.getProducer("venice", new MapConfig(samzaConfig1), null);
      veniceBatchProducer1.start();
      Map<String, String> samzaConfig2 =
          getSamzaProducerConfig(veniceClusterWrapper, storeName2, Version.PushType.STREAM_REPROCESSING);
      veniceBatchProducer2 = factory.getProducer("venice", new MapConfig(samzaConfig2), null);
      veniceBatchProducer2.start();
      if (veniceBatchProducer1 instanceof VeniceSystemProducer) {
        // The default behavior would exit the process
        ((VeniceSystemProducer) veniceBatchProducer1).setExitMode(SamzaExitMode.NO_OP);
      }
      if (veniceBatchProducer2 instanceof VeniceSystemProducer) {
        // The default behavior would exit the process
        ((VeniceSystemProducer) veniceBatchProducer2).setExitMode(SamzaExitMode.NO_OP);
      }

      for (int i = 10; i >= 1; i--) {
        sendStreamingRecord(veniceBatchProducer1, storeName1, i);
        sendStreamingRecord(veniceBatchProducer2, storeName2, i);
      }

      // Before EOP, there should be 2 active producers
      Assert.assertEquals(factory.getNumberOfActiveSystemProducers(), 2);
      /**
       * Send EOP to the first store, eventually the first SystemProducer will be marked as inactive
       * after push monitor poll the latest push job status from router.
       */
      Utils.sleep(500);
      veniceClusterWrapper.useControllerClient(c -> {
        c.writeEndOfPush(storeName1, 1);
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
          Assert.assertTrue(admin.getStore(clusterName, storeName1).containsVersion(1));
          Assert.assertEquals(admin.getStore(clusterName, storeName1).getCurrentVersion(), 1);
          // The second SystemProducer should still be active
          Assert.assertEquals(factory.getNumberOfActiveSystemProducers(), 1);
        });

        c.writeEndOfPush(storeName2, 1);
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
          Assert.assertTrue(admin.getStore(clusterName, storeName2).containsVersion(1));
          Assert.assertEquals(admin.getStore(clusterName, storeName2).getCurrentVersion(), 1);
          // There should be no active SystemProducer any more.
          Assert.assertEquals(factory.getNumberOfActiveSystemProducers(), 0);
        });
      });
    } finally {
      if (veniceBatchProducer1 != null) {
        veniceBatchProducer1.stop();
      }
      if (veniceBatchProducer2 != null) {
        veniceBatchProducer2.stop();
      }
    }
  }

  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testLeaderHonorLastTopicSwitchMessage() throws Exception {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(10L));
    try (
        VeniceClusterWrapper venice =
            ServiceFactory.getVeniceCluster(1, 2, 1, 2, 1000000, false, false, extraProperties);
        ControllerClient controllerClient =
            new ControllerClient(venice.getClusterName(), venice.getAllControllersURLs())) {
      long streamingRewindSeconds = 25L;
      long streamingMessageLag = 2L;

      String storeName = Utils.getUniqueString("hybrid-store");

      // Create store , make it a hybrid store
      controllerClient.createNewStore(storeName, "owner", STRING_SCHEMA.toString(), STRING_SCHEMA.toString());
      controllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setHybridRewindSeconds(streamingRewindSeconds)
              .setHybridOffsetLagThreshold(streamingMessageLag));

      // Create a new version, and do an empty push for that version
      VersionCreationResponse vcr = TestUtils
          .assertCommand(controllerClient.emptyPush(storeName, Utils.getUniqueString("empty-hybrid-push"), 1L));
      int versionNumber = vcr.getVersion();
      Assert.assertEquals(versionNumber, 1, "Version number should become 1 after an empty-push");
      int partitionCnt = vcr.getPartitions();

      /**
       * Write 2 TopicSwitch messages into version topic:
       * TS1 (new topic: storeName_tmp1, startTime: {@link rewindStartTime})
       * TS2 (new topic: storeName_tmp2, startTime: {@link rewindStartTime})
       *
       * All messages in TS1 should not be replayed into VT and should not be queryable;
       * but messages in TS2 should be replayed and queryable.
       */
      PubSubTopic tmpTopic1 = sharedVenice.getPubSubTopicRepository().getTopic(storeName + "_tmp1_rt");
      PubSubTopic tmpTopic2 = sharedVenice.getPubSubTopicRepository().getTopic(storeName + "_tmp2_rt");
      TopicManager topicManager = venice.getLeaderVeniceController().getVeniceAdmin().getTopicManager();
      topicManager.createTopic(tmpTopic1, partitionCnt, 1, true);
      topicManager.createTopic(tmpTopic2, partitionCnt, 1, true);

      /**
       *  Build a producer that writes to {@link tmpTopic1}
       */
      Properties veniceWriterProperties = new Properties();
      veniceWriterProperties.put(KAFKA_BOOTSTRAP_SERVERS, venice.getPubSubBrokerWrapper().getAddress());
      veniceWriterProperties.putAll(
          PubSubBrokerWrapper.getBrokerDetailsForClients(Collections.singletonList(venice.getPubSubBrokerWrapper())));
      AvroSerializer<String> stringSerializer = new AvroSerializer(STRING_SCHEMA);
      PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
          venice.getPubSubBrokerWrapper().getPubSubClientsFactory().getProducerAdapterFactory();

      try (VeniceWriter<byte[], byte[], byte[]> tmpWriter1 =
          TestUtils.getVeniceWriterFactory(veniceWriterProperties, pubSubProducerAdapterFactory)
              .createVeniceWriter(new VeniceWriterOptions.Builder(tmpTopic1.getName()).build())) {
        // Write 10 records
        for (int i = 0; i < 10; ++i) {
          tmpWriter1.put(stringSerializer.serialize("key_" + i), stringSerializer.serialize("value_" + i), 1);
        }
      }

      /**
       *  Build a producer that writes to {@link tmpTopic2}
       */
      try (VeniceWriter<byte[], byte[], byte[]> tmpWriter2 =
          TestUtils.getVeniceWriterFactory(veniceWriterProperties, pubSubProducerAdapterFactory)
              .createVeniceWriter(new VeniceWriterOptions.Builder(tmpTopic2.getName()).build())) {
        // Write 10 records
        for (int i = 10; i < 20; ++i) {
          tmpWriter2.put(stringSerializer.serialize("key_" + i), stringSerializer.serialize("value_" + i), 1);
        }
      }

      /**
       * Wait for leader to switch over to real-time topic
       */
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
        StoreResponse store = TestUtils.assertCommand(controllerClient.getStore(storeName));
        Assert.assertEquals(store.getStore().getCurrentVersion(), 1);
      });

      StoreInfo storeInfo = TestUtils.assertCommand(controllerClient.getStore(storeName)).getStore();

      /**
       * Verify that all messages from {@link tmpTopic2} are in store and no message from {@link tmpTopic1} is in store.
       */
      try (
          AvroGenericStoreClient<String, Utf8> client = ClientFactory.getAndStartGenericAvroClient(
              ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(venice.getRandomRouterURL()));
          VeniceWriter<byte[], byte[], byte[]> realTimeTopicWriter =
              TestUtils.getVeniceWriterFactory(veniceWriterProperties, pubSubProducerAdapterFactory)
                  .createVeniceWriter(new VeniceWriterOptions.Builder(Utils.getRealTimeTopicName(storeInfo)).build())) {
        // Build a producer to produce 2 TS messages into RT
        realTimeTopicWriter.broadcastTopicSwitch(
            Collections.singletonList(venice.getPubSubBrokerWrapper().getAddress()),
            tmpTopic1.getName(),
            -1L,
            null);
        realTimeTopicWriter.broadcastTopicSwitch(
            Collections.singletonList(venice.getPubSubBrokerWrapper().getAddress()),
            tmpTopic2.getName(),
            -1L,
            null);

        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
          // All messages from tmpTopic2 should exist
          try {
            for (int i = 10; i < 20; i++) {
              String key = "key_" + i;
              Assert.assertEquals(client.get(key).get(), new Utf8("value_" + i));
            }
          } catch (Exception e) {
            LOGGER.error("Caught exception in client.get()", e);
            Assert.fail(e.getMessage());
          }

          // No message from tmpTopic1 should exist
          try {
            for (int i = 0; i < 10; i++) {
              String key = "key_" + i;
              Assert.assertNull(client.get(key).get());
            }
          } catch (Exception e) {
            LOGGER.error("Caught exception in client.get()", e);
            Assert.fail(e.getMessage());
          }
        });
      }
    }
  }

  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testLeaderCanReleaseLatch() {
    VeniceClusterWrapper veniceClusterWrapper = sharedVenice;
    Admin admin = veniceClusterWrapper.getLeaderVeniceController().getVeniceAdmin();
    String clusterName = veniceClusterWrapper.getClusterName();
    String storeName = Utils.getUniqueString("test-store");

    SystemProducer producer = null;
    try (ControllerClient controllerClient =
        new ControllerClient(clusterName, veniceClusterWrapper.getAllControllersURLs())) {
      controllerClient.createNewStore(storeName, "owner", STRING_SCHEMA.toString(), STRING_SCHEMA.toString());
      controllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setHybridRewindSeconds(25L)
              .setHybridOffsetLagThreshold(1L));

      // Create a new version, and do an empty push for that version
      controllerClient.emptyPush(storeName, Utils.getUniqueString("empty-hybrid-push"), 1L);

      // write a few of messages from the Samza
      producer = IntegrationTestPushUtils.getSamzaProducer(veniceClusterWrapper, storeName, Version.PushType.STREAM);
      for (int i = 0; i < 10; i++) {
        sendStreamingRecord(producer, storeName, i);
      }

      // make sure the v1 is online and all the writes have been consumed by the SN
      try (AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceClusterWrapper.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(
            60,
            TimeUnit.SECONDS,
            true,
            true,
            () -> Assert.assertEquals(admin.getStore(clusterName, storeName).getCurrentVersion(), 1));

        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
          try {
            for (int i = 0; i < 10; i++) {
              String key = Integer.toString(i);
              Object value = client.get(key).get();
              Assert.assertNotNull(value, "Did not find key " + i + " in store before restarting SN.");
              Assert.assertEquals(value.toString(), "stream_" + key);
            }
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });

        // stop the SN (leader) and write more messages
        VeniceServerWrapper serverWrapper = veniceClusterWrapper.getVeniceServers().get(0);
        veniceClusterWrapper.stopVeniceServer(serverWrapper.getPort());

        for (int i = 10; i < 20; i++) {
          sendStreamingRecord(producer, storeName, i);
        }

        // restart the SN (leader). The node is supposed to be promoted to leader even with the offset lags.
        veniceClusterWrapper.restartVeniceServer(serverWrapper.getPort());

        String resourceName = Version.composeKafkaTopic(storeName, 1);
        HelixBaseRoutingRepository routingDataRepo = veniceClusterWrapper.getLeaderVeniceController()
            .getVeniceHelixAdmin()
            .getHelixVeniceClusterResources(clusterName)
            .getRoutingDataRepository();
        TestUtils.waitForNonDeterministicAssertion(
            60,
            TimeUnit.SECONDS,
            true,
            true,
            () -> Assert.assertNotNull(routingDataRepo.getLeaderInstance(resourceName, 0)));

        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
          try {
            for (int i = 10; i < 20; i++) {
              String key = Integer.toString(i);
              Object value = client.get(key).get();
              Assert.assertNotNull(value, "Did not find key " + i + " in store after restarting SN.");
              Assert.assertEquals(value.toString(), "stream_" + key);
            }
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        });
      }
    } finally {
      if (producer != null) {
        producer.stop();
      }
    }
  }

  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testHybridMultipleVersions() throws Exception {
    final Properties extraProperties = new Properties();
    extraProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1L));
    final int partitionCount = 2;
    final int keyCount = 10;
    VeniceClusterWrapper cluster = sharedVenice;
    UpdateStoreQueryParams params = new UpdateStoreQueryParams()
        // set hybridRewindSecond to a big number so following versions won't ignore old records in RT
        .setHybridRewindSeconds(2000000)
        .setHybridOffsetLagThreshold(10)
        .setPartitionCount(partitionCount);
    String storeName = Utils.getUniqueString("store");
    cluster.useControllerClient(client -> {
      client.createNewStore(storeName, "owner", DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA);
      client.updateStore(storeName, params);
    });
    cluster.createVersion(
        storeName,
        DEFAULT_KEY_SCHEMA,
        DEFAULT_VALUE_SCHEMA,
        IntStream.range(0, keyCount).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, i)));
    try (AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(20, TimeUnit.SECONDS, true, true, () -> {
        for (Integer i = 0; i < keyCount; i++) {
          assertEquals(client.get(i).get(), i);
        }
      });
      SystemProducer producer = IntegrationTestPushUtils.getSamzaProducer(cluster, storeName, Version.PushType.STREAM);
      for (int i = 0; i < keyCount; i++) {
        IntegrationTestPushUtils.sendStreamingRecord(producer, storeName, i, i + 1);
      }
      producer.stop();

      try (VeniceProducer veniceOnlineProducer = OnlineProducerFactory.createProducer(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL()),
          VeniceProperties.empty(),
          null)) {
        for (int i = keyCount; i < keyCount * 2; i++) {
          veniceOnlineProducer.asyncPut(i, i * 2).get();
        }
      }

      TestUtils.waitForNonDeterministicAssertion(20, TimeUnit.SECONDS, true, true, () -> {
        for (int i = 0; i < keyCount; i++) {
          assertEquals(client.get(i).get(), i + 1);
        }
        for (int i = keyCount; i < keyCount * 2; i++) {
          assertEquals(client.get(i).get(), i * 2);
        }
      });
      cluster.createVersion(
          storeName,
          DEFAULT_KEY_SCHEMA,
          DEFAULT_VALUE_SCHEMA,
          IntStream.range(0, keyCount).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, i + 2)));
      TestUtils.waitForNonDeterministicAssertion(20, TimeUnit.SECONDS, true, true, () -> {
        for (int i = 0; i < keyCount; i++) {
          assertEquals(client.get(i).get(), i + 1);
        }
        for (int i = keyCount; i < keyCount * 2; i++) {
          assertEquals(client.get(i).get(), i * 2);
        }
      });
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testHybridWithZeroLagThreshold() throws Exception {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams()
        // set hybridRewindSecond to a big number so following versions won't ignore old records in RT
        .setHybridRewindSeconds(2000000)
        .setHybridOffsetLagThreshold(0)
        .setPartitionCount(2);
    String storeName = Utils.getUniqueString("store");
    sharedVenice.useControllerClient(client -> {
      client.createNewStore(storeName, "owner", DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA);
      client.updateStore(storeName, params);
    });
    sharedVenice.createVersion(
        storeName,
        DEFAULT_KEY_SCHEMA,
        DEFAULT_VALUE_SCHEMA,
        IntStream.range(0, 10).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, i)));
  }

  @Test(timeOut = 180
      * Time.MS_PER_SECOND, dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testHybridStoreTimeLagThresholdWithEmptyRT(boolean isRealTimeTopicEmpty) throws Exception {
    SystemProducer veniceProducer = null;

    VeniceClusterWrapper venice = sharedVenice;
    try {
      long streamingRewindSeconds = 10L;
      // Disable offset lag threshold
      long streamingMessageLag = -1L;
      // Time lag threshold is 30 seconds for the test case
      long streamingTimeLag = 30L;

      String storeName = Utils.getUniqueString("hybrid-store");
      File inputDir = getTempDataDirectory();
      String inputDirPath = "file://" + inputDir.getAbsolutePath();
      Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir); // records 1-100
      Properties vpjProperties = defaultVPJProps(venice, inputDirPath, storeName);

      try (ControllerClient controllerClient = createStoreForJob(venice.getClusterName(), recordSchema, vpjProperties);
          AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
              ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(venice.getRandomRouterURL()))) {

        ControllerResponse response = controllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setHybridRewindSeconds(streamingRewindSeconds)
                .setHybridOffsetLagThreshold(streamingMessageLag)
                .setHybridTimeLagThreshold(streamingTimeLag));

        Assert.assertFalse(response.isError());

        // Do a VPJ push with an empty RT
        runVPJ(vpjProperties, 1, controllerClient);

        // Verify some records (note, records 1-100 have been pushed)
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
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

        if (!isRealTimeTopicEmpty) {
          // write streaming records
          veniceProducer = getSamzaProducer(venice, storeName, Version.PushType.STREAM);
          for (int i = 1; i <= 10; i++) {
            // The batch values are small, but the streaming records are "big" (i.e.: not that big, but bigger than
            // the server's max configured chunk size). In the scenario where chunking is disabled, the server's
            // max chunk size is not altered, and thus this will be under threshold.
            sendCustomSizeStreamingRecord(veniceProducer, storeName, i, STREAMING_RECORD_SIZE);
          }

          TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
            try {
              checkLargeRecord(client, 2);
            } catch (Exception e) {
              throw new VeniceException(e);
            }
          });
        }

        // bounce servers
        List<VeniceServerWrapper> servers = venice.getVeniceServers();
        for (VeniceServerWrapper server: servers) {
          venice.stopAndRestartVeniceServer(server.getPort());
        }
        // Without waiting after bouncing servers, it may cause this test flaky. It takes for a while for this
        // partition from BOOTSTRAP to ONLINE.
        Utils.sleep(5000);
        if (!isRealTimeTopicEmpty) {
          TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
            try {
              checkLargeRecord(client, 2);
            } catch (Exception e) {
              throw new VeniceException(e);
            }
          });
        } else {
          // Verify some records (note, records 1-100 have been pushed)
          TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
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
    } finally {
      if (veniceProducer != null) {
        veniceProducer.stop();
      }
    }
  }

  @Test(dataProvider = "Compression-Strategies", dataProviderClass = DataProviderUtils.class, timeOut = 60
      * Time.MS_PER_SECOND)
  public void testDuplicatedMessagesWontBePersisted(CompressionStrategy compressionStrategy) throws Exception {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(3L));

    SystemProducer veniceProducer = null;
    // N.B.: RF 2 with 2 servers is important, in order to test both the leader and follower code paths
    VeniceClusterWrapper venice = sharedVenice;
    PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
        venice.getPubSubBrokerWrapper().getPubSubClientsFactory().getProducerAdapterFactory();
    try {
      LOGGER.info("Finished creating VeniceClusterWrapper");

      long streamingRewindSeconds = 10L;
      long streamingMessageLag = 2L;

      String storeName = Utils.getUniqueString("hybrid-store");
      File inputDir = getTempDataDirectory();
      String inputDirPath = "file://" + inputDir.getAbsolutePath();
      Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir); // records 1-100
      Properties vpjProperties = defaultVPJProps(venice, inputDirPath, storeName);

      try (
          ControllerClient controllerClient = createStoreForJob(venice.getClusterName(), recordSchema, vpjProperties)) {
        StoreInfo storeInfo = TestUtils.assertCommand(controllerClient.getStore(storeName)).getStore();
        // Have 1 partition only, so that all keys are produced to the same partition
        ControllerResponse response = controllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setHybridRewindSeconds(streamingRewindSeconds)
                .setHybridOffsetLagThreshold(streamingMessageLag)
                .setCompressionStrategy(compressionStrategy)
                .setPartitionCount(1));

        Assert.assertFalse(response.isError());

        // Do a VPJ push
        runVPJ(vpjProperties, 1, controllerClient);

        /**
         * The following k/v pairs will be sent to RT, with the same producer GUID:
         * <key1, value1, Sequence number: 1>, <key1, value2, seq: 2>, <key1, value1, seq: 1 (Duplicated message)>, <key2, value1, seq: 3>
         * First check key2=value1, which confirms all messages above have been consumed by servers; then check key1=value2 to confirm
         * that duplicated message will not be persisted into disk
         */
        String key1 = "duplicated_message_test_key_1";
        String value1 = "duplicated_message_test_value_1";
        String value2 = "duplicated_message_test_value_2";
        String key2 = "duplicated_message_test_key_2";
        Properties veniceWriterProperties = new Properties();
        veniceWriterProperties.put(KAFKA_BOOTSTRAP_SERVERS, venice.getPubSubBrokerWrapper().getAddress());
        veniceWriterProperties.putAll(
            PubSubBrokerWrapper.getBrokerDetailsForClients(Collections.singletonList(venice.getPubSubBrokerWrapper())));
        AvroSerializer<String> stringSerializer = new AvroSerializer(STRING_SCHEMA);
        AvroGenericDeserializer<String> stringDeserializer =
            new AvroGenericDeserializer<>(STRING_SCHEMA, STRING_SCHEMA);
        try (VeniceWriter<byte[], byte[], byte[]> realTimeTopicWriter =
            TestUtils.getVeniceWriterFactory(veniceWriterProperties, pubSubProducerAdapterFactory)
                .createVeniceWriter(new VeniceWriterOptions.Builder(Utils.getRealTimeTopicName(storeInfo)).build())) {
          // Send <key1, value1, seq: 1>
          Pair<KafkaKey, KafkaMessageEnvelope> record = getKafkaKeyAndValueEnvelope(
              stringSerializer.serialize(key1),
              stringSerializer.serialize(value1),
              1,
              realTimeTopicWriter.getProducerGUID(),
              100,
              1,
              -1);
          realTimeTopicWriter.put(
              record.getFirst(),
              record.getSecond(),
              new CompletableFutureCallback(new CompletableFuture<>()),
              0,
              VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER);

          // Send <key1, value2, seq: 2>
          record = getKafkaKeyAndValueEnvelope(
              stringSerializer.serialize(key1),
              stringSerializer.serialize(value2),
              1,
              realTimeTopicWriter.getProducerGUID(),
              100,
              2,
              -1);
          realTimeTopicWriter.put(
              record.getFirst(),
              record.getSecond(),
              new CompletableFutureCallback(new CompletableFuture<>()),
              0,
              VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER);

          // Send <key1, value1, seq: 1 (Duplicated message)>
          record = getKafkaKeyAndValueEnvelope(
              stringSerializer.serialize(key1),
              stringSerializer.serialize(value1),
              1,
              realTimeTopicWriter.getProducerGUID(),
              100,
              1,
              -1);
          realTimeTopicWriter.put(
              record.getFirst(),
              record.getSecond(),
              new CompletableFutureCallback(new CompletableFuture<>()),
              0,
              VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER);

          // Send <key2, value1, seq: 3>
          record = getKafkaKeyAndValueEnvelope(
              stringSerializer.serialize(key2),
              stringSerializer.serialize(value1),
              1,
              realTimeTopicWriter.getProducerGUID(),
              100,
              3,
              -1);
          realTimeTopicWriter.put(
              record.getFirst(),
              record.getSecond(),
              new CompletableFutureCallback(new CompletableFuture<>()),
              0,
              VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER);
        }

        try (CloseableHttpAsyncClient storageNodeClient = HttpAsyncClients.createDefault()) {
          storageNodeClient.start();
          Base64.Encoder encoder = Base64.getUrlEncoder();

          VeniceCompressor compressor =
              getVeniceCompressor(compressionStrategy, storeName, 1, venice, storageNodeClient);
          // Check both leader and follower hosts
          TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, true, () -> {
            for (VeniceServerWrapper server: venice.getVeniceServers()) {
              /**
               * Check key2=value1 first, it means all messages sent to RT has been consumed already
               */
              StringBuilder sb = new StringBuilder().append("http://")
                  .append(server.getAddress())
                  .append("/")
                  .append(TYPE_STORAGE)
                  .append("/")
                  .append(Version.composeKafkaTopic(storeName, 1))
                  .append("/")
                  .append(0)
                  .append("/")
                  .append(encoder.encodeToString(stringSerializer.serialize(key2)))
                  .append("?f=b64");
              HttpGet getReq = new HttpGet(sb.toString());
              HttpResponse storageNodeResponse = storageNodeClient.execute(getReq, null).get();
              try (InputStream bodyStream = storageNodeClient.execute(getReq, null).get().getEntity().getContent()) {
                byte[] body = IOUtils.toByteArray(bodyStream);
                Assert.assertEquals(
                    storageNodeResponse.getStatusLine().getStatusCode(),
                    HttpStatus.SC_OK,
                    "Response did not return 200: " + new String(body));
                Object value = stringDeserializer.deserialize(compressor.decompress(body, 0, body.length));
                Assert.assertEquals(value.toString(), value1);
              }

              /**
               * If key1=value1, it means duplicated message has been persisted, so key1 must equal to value2
               */
              sb = new StringBuilder().append("http://")
                  .append(server.getAddress())
                  .append("/")
                  .append(TYPE_STORAGE)
                  .append("/")
                  .append(Version.composeKafkaTopic(storeName, 1))
                  .append("/")
                  .append(0)
                  .append("/")
                  .append(encoder.encodeToString(stringSerializer.serialize(key1)))
                  .append("?f=b64");
              getReq = new HttpGet(sb.toString());
              storageNodeResponse = storageNodeClient.execute(getReq, null).get();
              try (InputStream bodyStream = storageNodeClient.execute(getReq, null).get().getEntity().getContent()) {
                byte[] body = IOUtils.toByteArray(bodyStream);
                Assert.assertEquals(
                    storageNodeResponse.getStatusLine().getStatusCode(),
                    HttpStatus.SC_OK,
                    "Response did not return 200: " + new String(body));
                Object value = stringDeserializer.deserialize(compressor.decompress(body, 0, body.length));
                Assert.assertEquals(value.toString(), value2);
              }
            }
          });
        }
      }
    } finally {
      if (veniceProducer != null) {
        veniceProducer.stop();
      }
    }
  }

  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testVersionSwapDeferredWithHybrid() throws Exception {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(3L));

    // N.B.: RF 2 with 2 servers is important, in order to test both the leader and follower code paths
    VeniceClusterWrapper venice = sharedVenice;
    LOGGER.info("Finished creating VeniceClusterWrapper");
    long streamingRewindSeconds = 10L;
    long streamingMessageLag = 2L;
    String storeName = Utils.getUniqueString("hybrid-store");
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir); // records 1-100
    Properties vpjProperties = defaultVPJProps(venice, inputDirPath, storeName);
    try (ControllerClient controllerClient = createStoreForJob(venice.getClusterName(), recordSchema, vpjProperties);
        AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(venice.getRandomRouterURL()))) {
      StoreInfo storeInfo = controllerClient.getStore(storeName).getStore();
      // Have 1 partition only, so that all keys are produced to the same partition
      ControllerResponse response = controllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setHybridRewindSeconds(streamingRewindSeconds)
              .setHybridOffsetLagThreshold(streamingMessageLag)
              .setPartitionCount(1));
      Assert.assertFalse(response.isError());
      // Do a VPJ push normally to make sure everything is working fine.
      runVPJ(vpjProperties, 1, controllerClient);

      // Now do a VPJ push with version swap deferred to make sure we don't swap.
      vpjProperties.put(DEFER_VERSION_SWAP, "true");
      runVPJ(vpjProperties, 1, controllerClient);

      Properties veniceWriterProperties = new Properties();
      veniceWriterProperties.put(KAFKA_BOOTSTRAP_SERVERS, venice.getPubSubBrokerWrapper().getAddress());
      /**
       * Set max segment elapsed time to 0 to enforce creating small segments aggressively
       */
      veniceWriterProperties.put(VeniceWriter.MAX_ELAPSED_TIME_FOR_SEGMENT_IN_MS, "0");
      veniceWriterProperties.putAll(
          PubSubBrokerWrapper.getBrokerDetailsForClients(Collections.singletonList(venice.getPubSubBrokerWrapper())));
      AvroSerializer<String> stringSerializer = new AvroSerializer(STRING_SCHEMA);
      String prefix = "foo_object_";
      PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
          venice.getPubSubBrokerWrapper().getPubSubClientsFactory().getProducerAdapterFactory();
      for (int i = 0; i < 2; i++) {
        try (VeniceWriter<byte[], byte[], byte[]> realTimeTopicWriter =
            TestUtils.getVeniceWriterFactory(veniceWriterProperties, pubSubProducerAdapterFactory)
                .createVeniceWriter(new VeniceWriterOptions.Builder(Utils.getRealTimeTopicName(storeInfo)).build())) {
          for (int j = i * 50 + 1; j <= i * 50 + 50; j++) {
            realTimeTopicWriter
                .put(stringSerializer.serialize(String.valueOf(j)), stringSerializer.serialize(prefix + j), 1);
          }
        }
      }

      // Now mark the deferred version as current and verify it has all the records.
      controllerClient.overrideSetActiveVersion(storeName, 2);

      // Check both leader and follower hosts
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
        try {
          for (int i = 1; i <= 100; i++) {
            String key = Integer.toString(i);
            Object value = client.get(key).get();
            assertNotNull(value, "Key " + i + " should not be missing!");
            assertEquals(value.toString(), prefix + key);
          }
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });
    }
  }

  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testHybridDIVEnhancement() throws Exception {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(3L));

    // N.B.: RF 2 with 2 servers is important, in order to test both the leader and follower code paths
    VeniceClusterWrapper venice = sharedVenice;
    LOGGER.info("Finished creating VeniceClusterWrapper");
    long streamingRewindSeconds = 10L;
    long streamingMessageLag = 2L;
    String storeName = Utils.getUniqueString("hybrid-store");
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir); // records 1-100
    Properties vpjProperties = defaultVPJProps(venice, inputDirPath, storeName);
    try (ControllerClient controllerClient = createStoreForJob(venice.getClusterName(), recordSchema, vpjProperties);
        AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(venice.getRandomRouterURL()))) {
      StoreInfo storeInfo = TestUtils.assertCommand(controllerClient.getStore(storeName)).getStore();
      // Have 1 partition only, so that all keys are produced to the same partition
      ControllerResponse response = controllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setHybridRewindSeconds(streamingRewindSeconds)
              .setHybridOffsetLagThreshold(streamingMessageLag)
              .setPartitionCount(1));
      Assert.assertFalse(response.isError());
      // Do a VPJ push
      runVPJ(vpjProperties, 1, controllerClient);
      Properties veniceWriterProperties = new Properties();
      veniceWriterProperties.put(KAFKA_BOOTSTRAP_SERVERS, venice.getPubSubBrokerWrapper().getAddress());
      /**
       * Set max segment elapsed time to 0 to enforce creating small segments aggressively
       */
      veniceWriterProperties.put(VeniceWriter.MAX_ELAPSED_TIME_FOR_SEGMENT_IN_MS, "0");
      veniceWriterProperties.putAll(
          PubSubBrokerWrapper.getBrokerDetailsForClients(Collections.singletonList(venice.getPubSubBrokerWrapper())));
      AvroSerializer<String> stringSerializer = new AvroSerializer(STRING_SCHEMA);
      String prefix = "hybrid_DIV_enhancement_";
      PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
          venice.getPubSubBrokerWrapper().getPubSubClientsFactory().getProducerAdapterFactory();

      // chunk the data into 2 parts and send each part by different producers. Also, close the producers
      // as soon as it finishes writing. This makes sure that closing or switching producers won't
      // impact the ingestion
      for (int i = 0; i < 2; i++) {
        try (VeniceWriter<byte[], byte[], byte[]> realTimeTopicWriter =
            TestUtils.getVeniceWriterFactory(veniceWriterProperties, pubSubProducerAdapterFactory)
                .createVeniceWriter(new VeniceWriterOptions.Builder(Utils.getRealTimeTopicName(storeInfo)).build())) {
          for (int j = i * 50 + 1; j <= i * 50 + 50; j++) {
            realTimeTopicWriter
                .put(stringSerializer.serialize(String.valueOf(j)), stringSerializer.serialize(prefix + j), 1);
          }
        }
      }

      // Check both leader and follower hosts
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
        try {
          for (int i = 1; i <= 100; i++) {
            String key = Integer.toString(i);
            Object value = client.get(key).get();
            assertNotNull(value, "Key " + i + " should not be missing!");
            assertEquals(value.toString(), prefix + key);
          }
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });
    }
  }

  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testHybridWithPartitionWiseConsumer() throws Exception {
    final Properties extraProperties = new Properties();
    extraProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1L));
    // Using partition count of 4 to trigger realtime topics from different store versions' store ingestion task will
    // share one consumer.
    final int partitionCount = 4;
    final int keyCount = 10;
    try (VeniceClusterWrapper cluster = setUpCluster(true)) {
      UpdateStoreQueryParams params = new UpdateStoreQueryParams()
          // set hybridRewindSecond to a big number so following versions won't ignore old records in RT
          .setHybridRewindSeconds(2000000)
          .setHybridOffsetLagThreshold(10)
          .setPartitionCount(partitionCount);
      String storeName = Utils.getUniqueString("store");
      cluster.useControllerClient(client -> {
        client.createNewStore(storeName, "owner", DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA);
        client.updateStore(storeName, params);
      });
      // Create store version 1 by writing keyCount * 1 records.
      cluster.createVersion(
          storeName,
          DEFAULT_KEY_SCHEMA,
          DEFAULT_VALUE_SCHEMA,
          IntStream.range(0, keyCount).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, i)));
      try (AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(20, TimeUnit.SECONDS, true, true, () -> {
          for (Integer i = 0; i < keyCount; i++) {
            assertEquals(client.get(i).get(), i);
          }
        });
        SystemProducer producer =
            IntegrationTestPushUtils.getSamzaProducer(cluster, storeName, Version.PushType.STREAM);
        for (int i = 0; i < keyCount; i++) {
          IntegrationTestPushUtils.sendStreamingRecord(producer, storeName, i, i);
        }
        producer.stop();
        TestUtils.waitForNonDeterministicAssertion(20, TimeUnit.SECONDS, true, true, () -> {
          for (int i = 0; i < keyCount; i++) {
            assertEquals(client.get(i).get(), i);
          }
        });

        // Create store version 2 by writing keyCount * 2 records.
        cluster.createVersion(
            storeName,
            DEFAULT_KEY_SCHEMA,
            DEFAULT_VALUE_SCHEMA,
            IntStream.range(keyCount, keyCount * 2).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, i)));
        producer = IntegrationTestPushUtils.getSamzaProducer(cluster, storeName, Version.PushType.STREAM);
        for (int i = keyCount; i < keyCount * 2; i++) {
          IntegrationTestPushUtils.sendStreamingRecord(producer, storeName, i, i);
        }
        producer.stop();
        TestUtils.waitForNonDeterministicAssertion(20, TimeUnit.SECONDS, true, true, () -> {
          for (int i = 0; i < keyCount * 2; i++) {
            assertEquals(client.get(i).get(), i);
          }
        });

        // Create store version 3 by writing keyCount * 3 records.
        cluster.createVersion(
            storeName,
            DEFAULT_KEY_SCHEMA,
            DEFAULT_VALUE_SCHEMA,
            IntStream.range(keyCount * 2, keyCount * 3).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, i)));
        producer = IntegrationTestPushUtils.getSamzaProducer(cluster, storeName, Version.PushType.STREAM);
        for (int i = keyCount * 2; i < keyCount * 3; i++) {
          IntegrationTestPushUtils.sendStreamingRecord(producer, storeName, i, i);
        }
        TestUtils.waitForNonDeterministicAssertion(20, TimeUnit.SECONDS, true, true, () -> {
          for (int i = 0; i < keyCount * 3; i++) {
            assertEquals(client.get(i).get(), i);
          }
        });

        // Verify that store version 2 should get keyCount * 3 records, since rt topic has keyCount * 3 records.
        AvroSerializer<Integer> stringSerializer = new AvroSerializer(Schema.parse(DEFAULT_KEY_SCHEMA));
        AvroGenericDeserializer<Integer> stringDeserializer =
            new AvroGenericDeserializer<>(Schema.parse(DEFAULT_KEY_SCHEMA), Schema.parse(DEFAULT_VALUE_SCHEMA));

        try (CloseableHttpAsyncClient storageNodeClient = HttpAsyncClients.createDefault()) {
          storageNodeClient.start();
          Base64.Encoder encoder = Base64.getUrlEncoder();
          TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, true, () -> {
            VeniceServerWrapper server = cluster.getVeniceServers().get(1);
            int foundCount = 0;
            for (int i = 0; i < 3 * keyCount; i++) {
              for (int j = 0; j < partitionCount; j++) {
                StringBuilder sb = new StringBuilder().append("http://")
                    .append(server.getAddress())
                    .append("/")
                    .append(TYPE_STORAGE)
                    .append("/")
                    .append(Version.composeKafkaTopic(storeName, 2))
                    .append("/")
                    .append(j)
                    .append("/")
                    .append(encoder.encodeToString(stringSerializer.serialize(i)))
                    .append("?f=b64");
                HttpGet getReq = new HttpGet(sb.toString());
                HttpResponse storageNodeResponse = storageNodeClient.execute(getReq, null).get();
                try (InputStream bodyStream = storageNodeClient.execute(getReq, null).get().getEntity().getContent()) {
                  byte[] body = IOUtils.toByteArray(bodyStream);
                  if (storageNodeResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    Object value = stringDeserializer.deserialize(null, body);
                    Assert.assertEquals(value, i);
                    foundCount++;
                  }
                }
              }
            }
            Assert.assertEquals(foundCount, keyCount * 3);
          });
        }
      }
    }
  }

  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testLeaderShouldCalculateRewindDuringPromotion() {
    final Properties extraProperties = new Properties();
    extraProperties.setProperty(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(20L));
    final int partitionCount = 1;
    final int keyCount = 10;
    try (VeniceClusterWrapper cluster =
        ServiceFactory.getVeniceCluster(1, 1, 1, 1, 1000000, false, false, extraProperties)) {
      UpdateStoreQueryParams params = new UpdateStoreQueryParams()
          // set hybridRewindSecond to a big number so following versions won't ignore old records in RT
          .setHybridRewindSeconds(10)
          .setHybridOffsetLagThreshold(10)
          .setPartitionCount(partitionCount);
      String storeName = Utils.getUniqueString("store");
      cluster.useControllerClient(client -> {
        client.createNewStore(storeName, "owner", DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA);
        client.updateStore(storeName, params);
      });
      cluster.createVersion(
          storeName,
          DEFAULT_KEY_SCHEMA,
          DEFAULT_VALUE_SCHEMA,
          IntStream.range(0, keyCount).mapToObj(i -> new AbstractMap.SimpleEntry<>(i, i)));
      try (AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL()))) {
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, true, () -> {
          for (Integer i = 0; i < keyCount; i++) {
            assertEquals(client.get(i).get(), i);
          }
        });
        SystemProducer producer =
            IntegrationTestPushUtils.getSamzaProducer(cluster, storeName, Version.PushType.STREAM);
        int badKeyId = 10000;
        IntegrationTestPushUtils.sendStreamingRecord(producer, storeName, badKeyId, badKeyId);
        Utils.sleep(10000);

        // Create store version 2 by writing keyCount * 2 records.
        cluster.useControllerClient(controllerClient -> {
          VersionCreationResponse response = controllerClient.emptyPush(storeName, "test_push_id", 1000);
          assertEquals(response.getVersion(), 2);
          assertFalse(response.isError(), "Empty push to parent colo should succeed");
          TestUtils.waitForNonDeterministicPushCompletion(
              Version.composeKafkaTopic(storeName, 2),
              controllerClient,
              30,
              TimeUnit.SECONDS);
        });
        for (int i = 0; i < keyCount; i++) {
          IntegrationTestPushUtils.sendStreamingRecord(producer, storeName, i, 2 * i);
        }
        producer.stop();

        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, true, () -> {
          for (int i = 0; i < keyCount; i++) {
            assertEquals(client.get(i).get(), 2 * i);
          }
          assertNull(client.get(badKeyId).get());
        });
      }
    }
  }

  private static Pair<KafkaKey, KafkaMessageEnvelope> getKafkaKeyAndValueEnvelope(
      byte[] keyBytes,
      byte[] valueBytes,
      int valueSchemaId,
      GUID producerGUID,
      int segmentNumber,
      int sequenceNumber,
      long upstreamOffset) {
    KafkaKey kafkaKey = new KafkaKey(MessageType.PUT, keyBytes);
    Put putPayload = new Put();
    putPayload.putValue = ByteBuffer.wrap(valueBytes);
    putPayload.schemaId = valueSchemaId;
    putPayload.replicationMetadataVersionId = VeniceWriter.VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID;
    putPayload.replicationMetadataPayload = ByteBuffer.wrap(new byte[0]);

    KafkaMessageEnvelope kafkaValue = new KafkaMessageEnvelope();
    kafkaValue.messageType = MessageType.PUT.getValue();
    kafkaValue.payloadUnion = putPayload;

    ProducerMetadata producerMetadata = new ProducerMetadata();
    producerMetadata.producerGUID = producerGUID;
    producerMetadata.segmentNumber = segmentNumber;
    producerMetadata.messageSequenceNumber = sequenceNumber;
    producerMetadata.messageTimestamp = System.currentTimeMillis();
    kafkaValue.producerMetadata = producerMetadata;
    kafkaValue.leaderMetadataFooter = new LeaderMetadata();
    kafkaValue.leaderMetadataFooter.upstreamOffset = upstreamOffset;
    return Pair.create(kafkaKey, kafkaValue);
  }

  private static VeniceClusterWrapper setUpCluster(boolean enablePartitionWiseSharedConsumer) {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(DEFAULT_MAX_NUMBER_OF_PARTITIONS, "5");
    VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(1, 0, 0, 2, 1000000, false, false, extraProperties);

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
    serverProperties.setProperty(SERVER_CONSUMER_POOL_SIZE_PER_KAFKA_CLUSTER, "3");
    serverProperties.setProperty(SERVER_DEDICATED_DRAINER_FOR_SORTED_INPUT_ENABLED, "true");
    serverProperties.setProperty(
        SERVER_CONSUMER_POOL_ALLOCATION_STRATEGY,
        KafkaConsumerServiceDelegator.ConsumerPoolStrategyType.CURRENT_VERSION_PRIORITIZATION.name());

    if (enablePartitionWiseSharedConsumer) {
      serverProperties.setProperty(DEFAULT_MAX_NUMBER_OF_PARTITIONS, "4");
      serverProperties.setProperty(
          SERVER_SHARED_CONSUMER_ASSIGNMENT_STRATEGY,
          KafkaConsumerService.ConsumerAssignmentStrategy.PARTITION_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY.name());
    }
    cluster.addVeniceServer(new Properties(), serverProperties);
    // Enable blob files in one server
    serverProperties.setProperty(ROCKSDB_BLOB_FILES_ENABLED, "true");
    cluster.addVeniceServer(new Properties(), serverProperties);

    return cluster;
  }
}
