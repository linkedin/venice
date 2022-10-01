package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
import static com.linkedin.venice.ConfigKeys.CHILD_DATA_CENTER_KAFKA_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.LF_MODEL_DEPENDENCY_CHECK_DISABLED;
import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.hadoop.VenicePushJob.SOURCE_KAFKA;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.samza.VeniceSystemFactory.D2_ZK_HOSTS_PROPERTY;
import static com.linkedin.venice.samza.VeniceSystemFactory.DEPLOYMENT_ID;
import static com.linkedin.venice.samza.VeniceSystemFactory.DOT;
import static com.linkedin.venice.samza.VeniceSystemFactory.SYSTEMS_PREFIX;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_AGGREGATE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_PARENT_D2_ZK_HOSTS;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_PUSH_TYPE;
import static com.linkedin.venice.samza.VeniceSystemFactory.VENICE_STORE;
import static com.linkedin.venice.utils.TestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestPushUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestPushUtils.sendStreamingDeleteRecord;
import static com.linkedin.venice.utils.TestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestPushUtils.writeSimpleAvroFileWithUserSchema;
import static com.linkedin.venice.utils.TestUtils.generateInput;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.meta.VeniceUserStoreType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.samza.config.MapConfig;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestActiveActiveIngestion {
  private static final int TEST_TIMEOUT = 360 * Time.MS_PER_SECOND;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, 1).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiColoMultiClusterWrapper multiColoMultiClusterWrapper;
  private String clusterName;
  private VeniceClusterWrapper clusterWrapper;
  private VeniceServerWrapper serverWrapper;
  private AvroSerializer serializer;

  @BeforeClass
  public void setUp() {
    String stringSchemaStr = "\"string\"";
    serializer = new AvroSerializer(AvroCompatibilityHelper.parse(stringSchemaStr));
    Properties serverProperties = new Properties();
    serverProperties.setProperty(ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1));
    serverProperties.put(LF_MODEL_DEPENDENCY_CHECK_DISABLED, "true");
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    serverProperties.put(
        CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + DEFAULT_PARENT_DATA_CENTER_REGION_NAME,
        "localhost:" + Utils.getFreePort());
    multiColoMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(
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

    childDatacenters = multiColoMultiClusterWrapper.getClusters();
    parentControllers = multiColoMultiClusterWrapper.getParentControllers();
    clusterName = CLUSTER_NAMES[0];
    clusterWrapper = childDatacenters.get(0).getClusters().get(clusterName);
  }

  @AfterClass
  public void cleanUp() {
    multiColoMultiClusterWrapper.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testKIFRepushActiveActiveStore() throws Exception {
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
    Properties props = defaultVPJProps(parentControllers.get(0).getControllerUrl(), inputDirPath, storeName);
    String keySchemaStr = recordSchema.getField(props.getProperty(VenicePushJob.KEY_FIELD_PROP)).schema().toString();
    String valueSchemaStr =
        recordSchema.getField(props.getProperty(VenicePushJob.VALUE_FIELD_PROP)).schema().toString();
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setLeaderFollowerModel(true)
        .setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(5)
        .setHybridOffsetLagThreshold(2)
        .setNativeReplicationEnabled(true);
    createStoreForJob(clusterName, keySchemaStr, valueSchemaStr, props, storeParms).close();

    TestPushUtils.runPushJob("Run push job", props);

    // run samza to stream put and delete
    runSamzaStreamJob(storeName);

    // run repush
    props.setProperty(SOURCE_KAFKA, "true");
    props.setProperty(KAFKA_INPUT_BROKER_URL, clusterWrapper.getKafka().getAddress());
    props.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
    TestPushUtils.runPushJob("Run repush job", props);
    ControllerClient controllerClient =
        new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString());
    TestUtils.waitForNonDeterministicAssertion(
        5,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStore(storeName).getStore().getCurrentVersion(), 2));

    clusterWrapper.refreshAllRouterMetaData();
    // Validate repush from version 2
    MetricsRepository metricsRepository = new MetricsRepository();
    try (AvroGenericStoreClient avroClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(clusterWrapper.getRandomRouterURL())
            .setMetricsRepository(metricsRepository))) {
      // test single get
      for (int i = 0; i < 10; i++) {
        Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "stream_" + i);
      }
      // test deletes
      for (int i = 10; i < 20; i++) {
        Assert.assertNull(avroClient.get(Integer.toString(i)).get());
      }
      // test old data
      for (int i = 20; i < 100; i++) {
        Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "test_name_" + i);
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testActiveActiveStoreRestart() throws Exception {
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
    Properties props = defaultVPJProps(parentControllers.get(0).getControllerUrl(), inputDirPath, storeName);
    String keySchemaStr = recordSchema.getField(props.getProperty(VenicePushJob.KEY_FIELD_PROP)).schema().toString();
    String valueSchemaStr =
        recordSchema.getField(props.getProperty(VenicePushJob.VALUE_FIELD_PROP)).schema().toString();
    UpdateStoreQueryParams storeParms = new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true)
        .setHybridRewindSeconds(5)
        .setHybridOffsetLagThreshold(2)
        .setNativeReplicationEnabled(true);
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
    try (VeniceWriter<byte[], byte[], byte[]> veniceWriter = veniceWriterFactory.createBasicVeniceWriter(topic)) {
      veniceWriter.broadcastStartOfPush(true, Collections.emptyMap());

      /**
       * Restart storage node during batch ingestion.
       */
      Map<byte[], byte[]> sortedInputRecords = generateInput(1000, true, 0, serializer);
      Set<Integer> restartPointSetForSortedInput = new HashSet<>();
      restartPointSetForSortedInput.add(346);
      restartPointSetForSortedInput.add(543);
      serverWrapper = clusterWrapper.getVeniceServers().get(0);
      int cur = 0;
      for (Map.Entry<byte[], byte[]> entry: sortedInputRecords.entrySet()) {
        if (restartPointSetForSortedInput.contains(++cur)) {
          // Restart server
          clusterWrapper.stopVeniceServer(serverWrapper.getPort());
          clusterWrapper.restartVeniceServer(serverWrapper.getPort());
        }
        veniceWriter.put(entry.getKey(), entry.getValue(), 1, null);
      }
      veniceWriter.broadcastEndOfPush(Collections.emptyMap());
    }

    // Wait push completed.
    TestUtils.waitForNonDeterministicCompletion(20, TimeUnit.SECONDS, () -> {
      try {
        return clusterWrapper.getLeaderVeniceController()
            .getVeniceAdmin()
            .getOffLinePushStatus(clusterWrapper.getClusterName(), topic)
            .getExecutionStatus()
            .equals(ExecutionStatus.COMPLETED);
      } catch (Exception e) {
        return false;
      }
    });
  }

  private void runSamzaStreamJob(String storeName) {
    Map<String, String> samzaConfig = new HashMap<>();
    String configPrefix = SYSTEMS_PREFIX + "venice" + DOT;
    samzaConfig.put(configPrefix + VENICE_PUSH_TYPE, Version.PushType.STREAM.toString());
    samzaConfig.put(configPrefix + VENICE_STORE, storeName);
    samzaConfig.put(configPrefix + VENICE_AGGREGATE, "false");
    samzaConfig.put(D2_ZK_HOSTS_PROPERTY, childDatacenters.get(0).getZkServerWrapper().getAddress());
    samzaConfig.put(VENICE_PARENT_D2_ZK_HOSTS, "dfd"); // parentController.getKafkaZkAddress());
    samzaConfig.put(DEPLOYMENT_ID, Utils.getUniqueString("venice-push-id"));
    samzaConfig.put(SSL_ENABLED, "false");
    VeniceSystemFactory factory = new VeniceSystemFactory();

    try (
        VeniceSystemProducer veniceProducer = factory.getClosableProducer("venice", new MapConfig(samzaConfig), null)) {
      veniceProducer.start();
      // send puts
      for (int i = 0; i < 10; i++) {
        sendStreamingRecord(veniceProducer, storeName, i);
      }
      // send deletes
      for (int i = 10; i < 20; i++) {
        sendStreamingDeleteRecord(veniceProducer, storeName, Integer.toString(i));
      }
    }
  }
}
