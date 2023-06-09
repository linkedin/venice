package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.*;
import static com.linkedin.venice.CommonConfigKeys.*;
import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.hadoop.VenicePushJob.*;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.*;
import static com.linkedin.venice.samza.VeniceSystemFactory.*;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.*;
import static com.linkedin.venice.utils.TestWriteUtils.*;

import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.ChangelogClientConfig;
import com.linkedin.davinci.consumer.VeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerClientFactory;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreTopicsResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.PubSubBrokerConfigs;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.VeniceUserStoreType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.MockCircularTime;
import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.view.TestView;
import com.linkedin.venice.views.ChangeCaptureView;
import com.linkedin.venice.writer.VeniceWriter;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.samza.config.MapConfig;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;


public class TestChangeCaptureIngestion {
  private static final int TEST_TIMEOUT = 360 * Time.MS_PER_SECOND;
  public static final int LARGE_RECORD_SIZE = 1024;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, 1).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private String clusterName;
  private VeniceClusterWrapper clusterWrapper;

  @BeforeClass
  public void setUp() {
    Properties serverProperties = new Properties();
    // Set a low chunking threshold so that chunking actually happens in the server
    serverProperties
        .setProperty(VeniceWriter.MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES, Integer.toString(LARGE_RECORD_SIZE));
    serverProperties.setProperty(ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1));
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    serverProperties.put(
        CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + DEFAULT_PARENT_DATA_CENTER_REGION_NAME,
        "localhost:" + TestUtils.getFreePort());
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

  public void testAAIngestionWithStoreView(CompressionStrategy compressionStrategy) throws Exception {
    ControllerClient childControllerClient =
        new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    String parentControllerURLs =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));
    ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerURLs);
    TestUtils.assertCommand(
        parentControllerClient.configureActiveActiveReplicationForCluster(
            true,
            VeniceUserStoreType.BATCH_ONLY.toString(),
            Optional.empty()));
    // create a active-active enabled store and run batch push job
    // batch job contains 100 records
    File inputDir = getTempDataDirectory();
    // Write records that are large enough to trigger server side chunking
    // TODO: Something seems to be wrong in the test set up or code that makes it so that the push job
    // will error if we publish records which exceed the chunking threshold (something about getting a cluster
    // lock when making the system stores?)
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir, LARGE_RECORD_SIZE);
    // Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    Properties props =
        TestWriteUtils.defaultVPJProps(parentControllers.get(0).getControllerUrl(), inputDirPath, storeName);
    props.setProperty(VeniceWriter.MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES, Integer.toString(LARGE_RECORD_SIZE));
    String keySchemaStr = recordSchema.getField(DEFAULT_KEY_FIELD_PROP).schema().toString();
    String valueSchemaStr = recordSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema().toString();
    Map<String, String> viewConfig = new HashMap<>();
    viewConfig.put(
        "testView",
        "{\"viewClassName\" : \"" + TestView.class.getCanonicalName() + "\", \"viewParameters\" : {}}");

    viewConfig.put(
        "changeCaptureView",
        "{\"viewClassName\" : \"" + ChangeCaptureView.class.getCanonicalName() + "\", \"viewParameters\" : {}}");
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(500)
        .setHybridOffsetLagThreshold(8)
        .setChunkingEnabled(true)
        .setNativeReplicationEnabled(true)
        .setCompressionStrategy(compressionStrategy)
        .setPartitionCount(1);
    MetricsRepository metricsRepository = new MetricsRepository();
    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms).close();
    storeParms.setStoreViews(viewConfig);
    IntegrationTestPushUtils.updateStore(clusterName, props, storeParms);
    TestWriteUtils.runPushJob("Run push job", props);
    Map<String, String> samzaConfig = getSamzaConfig(storeName);
    VeniceSystemFactory factory = new VeniceSystemFactory();
    // Use a unique key for DELETE with RMD validation
    int deleteWithRmdKeyIndex = 1000;

    TestMockTime testMockTime = new TestMockTime();
    ZkServerWrapper localZkServer = multiRegionMultiClusterWrapper.getChildRegions().get(0).getZkServerWrapper();
    PubSubBrokerWrapper localKafka = ServiceFactory.getPubSubBroker(
        new PubSubBrokerConfigs.Builder().setZkWrapper(localZkServer).setMockTime(testMockTime).build());
    Properties consumerProperties = new Properties();
    String localKafkaUrl = localKafka.getAddress();
    consumerProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, localKafkaUrl);
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class);
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaValueSerializer.class);
    consumerProperties.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
    ChangelogClientConfig globalChangelogClientConfig = new ChangelogClientConfig().setViewName("changeCaptureView")
        .setConsumerProperties(consumerProperties)
        .setControllerD2ServiceName(D2_SERVICE_NAME)
        .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .setLocalD2ZkHosts(localZkServer.getAddress())
        .setControllerRequestRetryCount(3);
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, metricsRepository);

    ChangelogClientConfig globalAfterImageClientConfig =
        ChangelogClientConfig.cloneConfig(globalChangelogClientConfig).setViewName("");
    VeniceChangelogConsumerClientFactory veniceAfterImageConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalAfterImageClientConfig, metricsRepository);

    VeniceChangelogConsumer<Utf8, Utf8> veniceChangelogConsumer =
        veniceChangelogConsumerClientFactory.getChangelogConsumer(storeName);
    veniceChangelogConsumer.subscribeAll().get();
    try (
        VeniceSystemProducer veniceProducer = factory.getClosableProducer("venice", new MapConfig(samzaConfig), null)) {
      veniceProducer.start();
      // Run Samza job to send PUT and DELETE requests.
      runSamzaStreamJob(veniceProducer, storeName, null, 10, 10, 0);
      // Produce a DELETE record with large timestamp
      produceRecordWithLogicalTimestamp(veniceProducer, storeName, deleteWithRmdKeyIndex, 1000, true);
    }

    try (AvroGenericStoreClient<String, Utf8> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(clusterWrapper.getRandomRouterURL())
            .setMetricsRepository(metricsRepository))) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        Assert.assertNull(client.get(Integer.toString(deleteWithRmdKeyIndex)).get());
      });
    }
    // Validate change events for version 1.
    Map<String, ChangeEvent<Utf8>> polledChangeEvents = new HashMap<>();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      Assert.assertEquals(polledChangeEvents.size(), 100);
    });

    polledChangeEvents.clear();

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      // 21 events for nearline events
      Assert.assertEquals(polledChangeEvents.size(), 21);
      for (int i = 0; i < 10; i++) {
        String key = Integer.toString(i);
        ChangeEvent<Utf8> changeEvent = polledChangeEvents.get(key);
        Assert.assertNotNull(changeEvent);
        if (i == 0) {
          Assert.assertNull(changeEvent.getPreviousValue());
        } else {
          Assert.assertTrue(changeEvent.getPreviousValue().toString().contains(key));
        }
        Assert.assertEquals(changeEvent.getCurrentValue().toString(), "stream_" + i);
      }
      for (int i = 10; i < 20; i++) {
        String key = Integer.toString(i);
        ChangeEvent<Utf8> changeEvent = polledChangeEvents.get(key);
        Assert.assertNotNull(changeEvent);
        Assert.assertNull(changeEvent.getPreviousValue()); // schema id is negative, so we did not parse.
        Assert.assertNull(changeEvent.getCurrentValue());
      }
    });
    // run repush
    props.setProperty(SOURCE_KAFKA, "true");
    props.setProperty(KAFKA_INPUT_BROKER_URL, clusterWrapper.getKafka().getAddress());
    props.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
    // intentionally stop re-consuming from RT so stale records don't affect the testing results
    props.put(REWIND_TIME_IN_SECONDS_OVERRIDE, 0);
    TestWriteUtils.runPushJob("Run repush job", props);
    ControllerClient controllerClient =
        new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    TestUtils.waitForNonDeterministicAssertion(
        5,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 2));
    clusterWrapper.refreshAllRouterMetaData();
    // Validate repush from version 2
    try (AvroGenericStoreClient<String, Utf8> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(clusterWrapper.getRandomRouterURL())
            .setMetricsRepository(metricsRepository))) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        // test single get
        for (int i = 0; i < 10; i++) {
          String key = Integer.toString(i);
          Utf8 value = client.get(key).get();
          Assert.assertNotNull(value);
          Assert.assertEquals(value.toString(), "stream_" + i);
        }
        // test deletes
        for (int i = 10; i < 20; i++) {
          String key = Integer.toString(i);
          Utf8 value = client.get(key).get();
          Assert.assertNull(value);
        }
        // test old data
        for (int i = 20; i < 100; i++) {
          String key = Integer.toString(i);
          Utf8 value = client.get(key).get();
          Assert.assertNotNull(value);
          Assert.assertTrue(value.toString().contains(String.valueOf(i).substring(0, 0)));
        }
      });
    }
    try (
        VeniceSystemProducer veniceProducer = factory.getClosableProducer("venice", new MapConfig(samzaConfig), null)) {
      veniceProducer.start();
      // Produce a new PUT with smaller logical timestamp, it is expected to be ignored as there was a DELETE with
      // larger
      // timestamp
      produceRecordWithLogicalTimestamp(veniceProducer, storeName, deleteWithRmdKeyIndex, 2, false);
      // Produce another record to the same partition to make sure the above PUT is processed during validation stage.
      produceRecordWithLogicalTimestamp(veniceProducer, storeName, deleteWithRmdKeyIndex + 1, 1, false);
    }
    try (AvroGenericStoreClient<String, Utf8> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(clusterWrapper.getRandomRouterURL())
            .setMetricsRepository(metricsRepository))) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        Assert.assertNotNull(client.get(Integer.toString(deleteWithRmdKeyIndex + 1)).get());
      });
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        Assert.assertNull(client.get(Integer.toString(deleteWithRmdKeyIndex)).get());
      });
    }
    VeniceChangelogConsumer<Utf8, Utf8> veniceAfterImageConsumer =
        veniceAfterImageConsumerClientFactory.getChangelogConsumer(storeName);
    veniceAfterImageConsumer.subscribeAll().get();
    // Validate changed events for version 2.
    polledChangeEvents.clear();
    // As records keys from VPJ start from 1, real-time produced records' key starts from 0, the message with key as 0
    // is new message.
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      // poll enough to get through the empty push and the topic jump to RT.
      pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      String deleteWithRmdKey = Integer.toString(deleteWithRmdKeyIndex);
      String persistWithRmdKey = Integer.toString(deleteWithRmdKeyIndex + 1);
      Assert.assertNull(polledChangeEvents.get(deleteWithRmdKey));
      Assert.assertNotNull(polledChangeEvents.get(persistWithRmdKey));
      Assert.assertEquals(
          polledChangeEvents.get(persistWithRmdKey).getCurrentValue().toString(),
          "stream_" + persistWithRmdKey);
    });
    /**
     * Test Repush with TTL
     */
    // run empty push to clean up batch data
    parentControllerClient.sendEmptyPushAndWait(storeName, "Run empty push job", 1000, 30 * Time.MS_PER_SECOND);
    // set up mocked time for Samza records so some records can be stale intentionally.
    List<Long> mockTimestampInMs = new LinkedList<>();
    Instant now = Instant.now();
    // always-valid record
    mockTimestampInMs.add(now.toEpochMilli());
    // always-stale records since ttl time is 360 sec
    Instant past = now.minus(1, ChronoUnit.HOURS);
    mockTimestampInMs.add(past.toEpochMilli());
    Time mockTime = new MockCircularTime(mockTimestampInMs);
    try (
        VeniceSystemProducer veniceProducer = factory.getClosableProducer("venice", new MapConfig(samzaConfig), null)) {
      veniceProducer.start();
      // run samza to stream put and delete
      runSamzaStreamJob(veniceProducer, storeName, mockTime, 10, 10, 20);
    }
    // Validate changed events for version 3.
    polledChangeEvents.clear();
    AtomicInteger totalPolledAfterImageMessages = new AtomicInteger();
    Map<String, Utf8> polledAfterImageEvents = new HashMap<>();
    Map<String, Utf8> totalPolledAfterImageEvents = new HashMap<>();
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
      pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      // Filter previous 21 messages.
      Assert.assertEquals(polledChangeEvents.size(), 20);

      // Consume from beginning of the version that was current at time of consumer subscription (version 2) since
      // version 2
      // was a repush of 101 records (0-100) with streaming updates on 1-10 and deletes on 10-19, then we expect
      // a grand total of 91 records in this version. We'll consume up to EOP

      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        totalPolledAfterImageMessages
            .addAndGet(pollAfterImageEventsFromChangeCaptureConsumer(polledAfterImageEvents, veniceAfterImageConsumer));
        Assert.assertEquals(polledAfterImageEvents.size(), 91);
        totalPolledAfterImageEvents.putAll(polledAfterImageEvents);
        polledAfterImageEvents.clear();
      });

      // With this next poll, we'll begin consuming the change capture topic on Version 2 up to the Version swap.
      // There will be one streaming record applied before the version swap message is consumed
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        totalPolledAfterImageMessages
            .addAndGet(pollAfterImageEventsFromChangeCaptureConsumer(polledAfterImageEvents, veniceAfterImageConsumer));
        Assert.assertEquals(polledAfterImageEvents.size(), 1);
        totalPolledAfterImageEvents.putAll(polledAfterImageEvents);
        polledAfterImageEvents.clear();
      });

      // Consume version 3 change capture. This will consist of 20 events that were applied onto version 3
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        totalPolledAfterImageMessages
            .addAndGet(pollAfterImageEventsFromChangeCaptureConsumer(polledAfterImageEvents, veniceAfterImageConsumer));
        Assert.assertEquals(polledAfterImageEvents.size(), 20);
        totalPolledAfterImageEvents.putAll(polledAfterImageEvents);
        polledAfterImageEvents.clear();
      });

      // Make sure nothing else comes out
      totalPolledAfterImageMessages
          .addAndGet(pollAfterImageEventsFromChangeCaptureConsumer(polledAfterImageEvents, veniceAfterImageConsumer));
      Assert.assertEquals(polledAfterImageEvents.size(), 0);
      // After image consumer consumed 3 different topics: v2, v2_cc and v3_cc.
      // The total messages: 102 (v2 repush from v1, key: 0-100, 1000) + 1 (v2_cc, key: 1001) + 42 (v3_cc, key: 0-39,
      // 1000, 1001) - 22 (filtered from v3_cc, key: 0-19, 1000 and 1001 as they were read already.)
      Assert.assertEquals(totalPolledAfterImageMessages.get(), 112);
      for (int i = 20; i < 40; i++) {
        String key = Integer.toString(i);
        ChangeEvent<Utf8> changeEvent = polledChangeEvents.get(key);
        Assert.assertNotNull(changeEvent);
        Assert.assertNull(changeEvent.getPreviousValue());
        if (i >= 20 && i < 30) {
          Assert.assertEquals(changeEvent.getCurrentValue().toString(), "stream_" + i);
        } else {
          Assert.assertNull(changeEvent.getCurrentValue());
        }
      }
      for (int i = 0; i < 100; i++) {
        String key = Integer.toString(i);
        Utf8 afterImageValue = totalPolledAfterImageEvents.get(key);
        if (i < 10 || (i >= 20 && i < 30)) {
          Assert.assertNotNull(afterImageValue);
          Assert.assertEquals(afterImageValue.toString(), "stream_" + i);
        } else if (i < 40) {
          // Deleted
          Assert.assertNull(afterImageValue);
        } else {
          Assert.assertTrue(afterImageValue.toString().contains(String.valueOf(i).substring(0, 0)));
        }
      }
    });
    // enable repush ttl
    props.setProperty(REPUSH_TTL_ENABLE, "true");
    TestWriteUtils.runPushJob("Run repush job with TTL", props);
    TestUtils.waitForNonDeterministicAssertion(
        5,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 4));
    // Validate repush from version 4
    clusterWrapper.refreshAllRouterMetaData();
    try (AvroGenericStoreClient<String, Utf8> client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(clusterWrapper.getRandomRouterURL())
            .setMetricsRepository(metricsRepository))) {
      // test single get
      int validGet = 0, filteredGet = 0;
      for (int i = 20; i < 30; i++) {
        Object result = client.get(Integer.toString(i)).get();
        if (result == null) {
          filteredGet++;
        } else {
          validGet++;
        }
      }
      // Half records are valid, another half is not
      Assert.assertEquals(validGet, 5);
      Assert.assertEquals(filteredGet, 5);
      // test deletes
      for (int i = 30; i < 40; i++) {
        // not matter the DELETE is TTLed or not, the value should always be null
        Assert.assertNull(client.get(Integer.toString(i)).get());
      }
      // test old data - should be empty due to empty push
      for (int i = 40; i < 100; i++) {
        Assert.assertNull(client.get(Integer.toString(i)).get());
      }
    }
    // Since nothing is produced, so changed events generated.
    polledChangeEvents.clear();
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, true, () -> {
      pollChangeEventsFromChangeCaptureConsumer(polledChangeEvents, veniceChangelogConsumer);
      Assert.assertEquals(polledChangeEvents.size(), 0);
    });
    // Verify version swap count matches with version count - 1 (since we don't transmit from version 0 to version 1)
    TestUtils.waitForNonDeterministicAssertion(
        5,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(TestView.getInstance().getVersionSwapCountForStore(storeName), 3));
    // Verify total updates match up (first 20 + next 20 should make 40, And then double it again as rewind updates
    // are
    // applied to a version)
    TestUtils.waitForNonDeterministicAssertion(
        5,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(TestView.getInstance().getRecordCountForStore(storeName), 85));
    parentControllerClient.disableAndDeleteStore(storeName);
    // Verify that topics and store is cleaned up
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      MultiStoreTopicsResponse storeTopicsResponse = childControllerClient.getDeletableStoreTopics();
      Assert.assertFalse(storeTopicsResponse.isError());
      Assert.assertEquals(storeTopicsResponse.getTopics().size(), 0);
    });

  }

  private void pollChangeEventsFromChangeCaptureConsumer(
      Map<String, ChangeEvent<Utf8>> polledChangeEvents,
      VeniceChangelogConsumer veniceChangelogConsumer) {
    Collection<PubSubMessage<Utf8, ChangeEvent<Utf8>, Long>> pubSubMessages = veniceChangelogConsumer.poll(10000);
    for (PubSubMessage<Utf8, ChangeEvent<Utf8>, Long> pubSubMessage: pubSubMessages) {
      ChangeEvent<Utf8> changeEvent = pubSubMessage.getValue();
      String key = pubSubMessage.getKey().toString();
      polledChangeEvents.put(key, changeEvent);
    }
  }

  private int pollAfterImageEventsFromChangeCaptureConsumer(
      Map<String, Utf8> polledChangeEvents,
      VeniceChangelogConsumer veniceChangelogConsumer) {
    int polledMessagesNum = 0;
    Collection<PubSubMessage<Utf8, ChangeEvent<Utf8>, Long>> pubSubMessages = veniceChangelogConsumer.poll(1000);
    for (PubSubMessage<Utf8, ChangeEvent<Utf8>, Long> pubSubMessage: pubSubMessages) {
      Utf8 afterImageEvent = pubSubMessage.getValue().getCurrentValue();
      String key = pubSubMessage.getKey().toString();
      polledChangeEvents.put(key, afterImageEvent);
      polledMessagesNum++;
    }
    return polledMessagesNum;
  }

  private Map<String, String> getSamzaConfig(String storeName) {
    Map<String, String> samzaConfig = new HashMap<>();
    String configPrefix = SYSTEMS_PREFIX + "venice" + DOT;
    samzaConfig.put(configPrefix + VENICE_PUSH_TYPE, Version.PushType.STREAM.toString());
    samzaConfig.put(configPrefix + VENICE_STORE, storeName);
    samzaConfig.put(configPrefix + VENICE_AGGREGATE, "false");
    samzaConfig.put(VENICE_CHILD_D2_ZK_HOSTS, childDatacenters.get(0).getZkServerWrapper().getAddress());
    samzaConfig.put(VENICE_CHILD_CONTROLLER_D2_SERVICE, D2_SERVICE_NAME);
    samzaConfig.put(VENICE_PARENT_D2_ZK_HOSTS, "dfd"); // parentController.getKafkaZkAddress());
    samzaConfig.put(VENICE_PARENT_CONTROLLER_D2_SERVICE, PARENT_D2_SERVICE_NAME);
    samzaConfig.put(DEPLOYMENT_ID, Utils.getUniqueString("venice-push-id"));
    samzaConfig.put(SSL_ENABLED, "false");
    return samzaConfig;
  }

  private void runSamzaStreamJob(
      VeniceSystemProducer veniceProducer,
      String storeName,
      Time mockedTime,
      int numPuts,
      int numDels,
      int startIdx) {
    // Send PUT requests.
    for (int i = startIdx; i < startIdx + numPuts; i++) {
      sendStreamingRecord(
          veniceProducer,
          storeName,
          Integer.toString(i),
          "stream_" + i,
          mockedTime == null ? null : mockedTime.getMilliseconds());
    }
    // Send DELETE requests.
    for (int i = startIdx + numPuts; i < startIdx + numPuts + numDels; i++) {
      sendStreamingDeleteRecord(
          veniceProducer,
          storeName,
          Integer.toString(i),
          mockedTime == null ? null : mockedTime.getMilliseconds());
    }
  }

  private void produceRecordWithLogicalTimestamp(
      VeniceSystemProducer veniceProducer,
      String storeName,
      int index,
      long logicalTimestamp,
      boolean isDeleteOperation) {
    if (isDeleteOperation) {
      sendStreamingDeleteRecord(veniceProducer, storeName, Integer.toString(index), logicalTimestamp);
    } else {
      sendStreamingRecord(veniceProducer, storeName, Integer.toString(index), "stream_" + index, logicalTimestamp);
    }
  }
}
