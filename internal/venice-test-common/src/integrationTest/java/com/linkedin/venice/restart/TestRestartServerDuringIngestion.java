package com.linkedin.venice.restart;

import static com.linkedin.venice.ConfigKeys.MULTI_REGION;
import static com.linkedin.venice.utils.TestUtils.generateInput;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_DISCOVER_URL_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_STORE_NAME_PROP;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.serializer.AvroGenericDeserializer;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public abstract class TestRestartServerDuringIngestion {
  private VeniceClusterWrapper cluster;
  private VeniceServerWrapper serverWrapper;
  private PubSubProducerAdapterFactory pubSubProducerAdapterFactory;

  protected abstract PersistenceType getPersistenceType();

  protected abstract Properties getExtraProperties();

  private Properties getVeniceServerProperties() {
    Properties properties = new Properties();
    properties.put(ConfigKeys.PERSISTENCE_TYPE, getPersistenceType());
    properties.put(ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, 100);
    properties.put(ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_TRANSACTIONAL_MODE, 100);

    properties.putAll(getExtraProperties());
    return properties;
  }

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    int numberOfController = 1;
    int numberOfRouter = 1;
    int replicaFactor = 1;
    int partitionSize = 1000;
    cluster = ServiceFactory
        .getVeniceCluster(numberOfController, 0, numberOfRouter, replicaFactor, partitionSize, false, false);
    pubSubProducerAdapterFactory =
        cluster.getPubSubBrokerWrapper().getPubSubClientsFactory().getProducerAdapterFactory();

    serverWrapper = cluster.addVeniceServer(new Properties(), getVeniceServerProperties());
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    cluster.close();
  }

  @Test(timeOut = 90 * Time.MS_PER_SECOND)
  public void testIngestionRecovery() throws ExecutionException, InterruptedException {
    // Create a store
    String stringSchemaStr = "\"string\"";
    StoreInfo storeInfo;
    AvroSerializer serializer = new AvroSerializer(AvroCompatibilityHelper.parse(stringSchemaStr));
    AvroGenericDeserializer deserializer =
        new AvroGenericDeserializer(Schema.parse(stringSchemaStr), Schema.parse(stringSchemaStr));

    String storeName = Utils.getUniqueString("test_store");
    String veniceUrl = cluster.getLeaderVeniceController().getControllerUrl();
    Properties properties = new Properties();
    properties.put(VENICE_DISCOVER_URL_PROP, veniceUrl);
    properties.put(VENICE_STORE_NAME_PROP, storeName);
    properties.put(MULTI_REGION, false);
    IntegrationTestPushUtils.createStoreForJob(cluster, stringSchemaStr, stringSchemaStr, properties).close();
    IntegrationTestPushUtils.makeStoreHybrid(cluster, storeName, 3600, 10);

    // Create a new version
    VersionCreationResponse versionCreationResponse = null;
    try (ControllerClient controllerClient = new ControllerClient(cluster.getClusterName(), veniceUrl)) {
      versionCreationResponse = TestUtils.assertCommand(
          controllerClient.requestTopicForWrites(
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
      storeInfo = TestUtils.assertCommand(controllerClient.getStore(storeName)).getStore();
    }

    String topic = versionCreationResponse.getKafkaTopic();
    PubSubBrokerWrapper pubSubBrokerWrapper = cluster.getPubSubBrokerWrapper();
    VeniceWriterFactory veniceWriterFactory =
        IntegrationTestPushUtils.getVeniceWriterFactory(pubSubBrokerWrapper, pubSubProducerAdapterFactory);

    try (VeniceWriter<byte[], byte[], byte[]> veniceWriter =
        veniceWriterFactory.createVeniceWriter(new VeniceWriterOptions.Builder(topic).build())) {
      veniceWriter.broadcastStartOfPush(true, Collections.emptyMap());

      /**
       * Restart storage node during batch ingestion.
       */
      Map<byte[], byte[]> sortedInputRecords = generateInput(1000, true, 0, serializer);
      Set<Integer> restartPointSetForSortedInput = new HashSet<>();
      restartPointSetForSortedInput.add(134);
      restartPointSetForSortedInput.add(346);
      restartPointSetForSortedInput.add(678);
      restartPointSetForSortedInput.add(831);
      int cur = 0;
      for (Map.Entry<byte[], byte[]> entry: sortedInputRecords.entrySet()) {
        if (restartPointSetForSortedInput.contains(++cur)) {
          // Restart server
          cluster.stopVeniceServer(serverWrapper.getPort());
          TestUtils.waitForNonDeterministicAssertion(20, TimeUnit.SECONDS, true, true, () -> {
            Assert.assertFalse(cluster.getRandomVeniceRouter().getRoutingDataRepository().containsKafkaTopic(topic));
          });
          cluster.restartVeniceServer(serverWrapper.getPort());
        }
        veniceWriter.put(entry.getKey(), entry.getValue(), 1, null);
      }

      veniceWriter.broadcastEndOfPush(Collections.emptyMap());

      // Wait push completed.
      TestUtils.waitForNonDeterministicCompletion(
          20,
          TimeUnit.SECONDS,
          () -> cluster.getLeaderVeniceController()
              .getVeniceAdmin()
              .getOffLinePushStatus(cluster.getClusterName(), topic)
              .getExecutionStatus()
              .equals(ExecutionStatus.COMPLETED));

      /**
       * There is a delay before router realizes that servers are up, it's possible that the
       * router couldn't find any replica because there is only one server in the cluster and
       * the only server just gets restarted. Restart all routers to get the fresh state of
       * the cluster.
       */
      restartAllRouters();

      try (AvroGenericStoreClient<String, CharSequence> storeClient = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName)
              .setVeniceURL(cluster.getRandomRouterURL())
              .setSslFactory(SslUtils.getVeniceLocalSslFactory()))) {

        for (Map.Entry<byte[], byte[]> entry: sortedInputRecords.entrySet()) {
          String key = deserializer.deserialize(entry.getKey()).toString();
          CharSequence expectedValue = (CharSequence) deserializer.deserialize(entry.getValue());
          CharSequence returnedValue = storeClient.get(key).get();
          Assert.assertEquals(returnedValue, expectedValue);
        }

        /**
         * Restart storage node during streaming ingestion.
         */
        Map<byte[], byte[]> unsortedInputRecords = generateInput(1000, false, 5000, serializer);
        Set<Integer> restartPointSetForUnsortedInput = new HashSet<>();
        restartPointSetForUnsortedInput.add(134);
        restartPointSetForUnsortedInput.add(346);
        restartPointSetForUnsortedInput.add(678);
        restartPointSetForUnsortedInput.add(831);
        cur = 0;

        try (VeniceWriter<byte[], byte[], byte[]> streamingWriter = veniceWriterFactory
            .createVeniceWriter(new VeniceWriterOptions.Builder(Utils.getRealTimeTopicName(storeInfo)).build())) {
          for (Map.Entry<byte[], byte[]> entry: unsortedInputRecords.entrySet()) {
            if (restartPointSetForUnsortedInput.contains(++cur)) {
              // Restart server
              cluster.stopVeniceServer(serverWrapper.getPort());
              // Ensure all of server are shutdown, no partition assigned.
              TestUtils.waitForNonDeterministicAssertion(20, TimeUnit.SECONDS, true, true, () -> {
                Assert
                    .assertFalse(cluster.getRandomVeniceRouter().getRoutingDataRepository().containsKafkaTopic(topic));
              });
              cluster.restartVeniceServer(serverWrapper.getPort());
            }
            streamingWriter.put(entry.getKey(), entry.getValue(), 1, null);
          }
        }

        // Wait until all partitions have ready-to-serve instances
        TestUtils.waitForNonDeterministicAssertion(20, TimeUnit.SECONDS, () -> {
          RoutingDataRepository routingDataRepository = cluster.getRandomVeniceRouter().getRoutingDataRepository();
          Assert.assertTrue(routingDataRepository.containsKafkaTopic(topic));
          int partitionCount = routingDataRepository.getPartitionAssignments(topic).getAllPartitions().size();
          for (int partition = 0; partition < partitionCount; partition++) {
            Assert.assertTrue(routingDataRepository.getReadyToServeInstances(topic, partition).size() > 0);
          }
        });
        restartAllRouters();
        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
          // Verify all the key/value pairs
          for (Map.Entry<byte[], byte[]> entry: unsortedInputRecords.entrySet()) {
            String key = deserializer.deserialize(entry.getKey()).toString();
            CharSequence expectedValue = (CharSequence) deserializer.deserialize(entry.getValue());
            CharSequence returnedValue = storeClient.get(key).get();
            Assert.assertNotNull(returnedValue);
            Assert.assertEquals(returnedValue, expectedValue);
          }
        });
      }
    }
  }

  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testIngestionDrainer() {
    // Create a store
    String stringSchemaStr = "\"string\"";
    AvroSerializer serializer = new AvroSerializer(AvroCompatibilityHelper.parse(stringSchemaStr));

    String storeName = Utils.getUniqueString("test_store");
    String veniceUrl = cluster.getLeaderVeniceController().getControllerUrl();
    Properties properties = new Properties();
    properties.put(VENICE_DISCOVER_URL_PROP, veniceUrl);
    properties.put(VENICE_STORE_NAME_PROP, storeName);
    properties.put(MULTI_REGION, false);
    IntegrationTestPushUtils.createStoreForJob(cluster, stringSchemaStr, stringSchemaStr, properties).close();

    IntegrationTestPushUtils.makeStoreHybrid(cluster, storeName, 3600, 10);

    // Create a new version
    VersionCreationResponse versionCreationResponse;
    try (ControllerClient controllerClient = new ControllerClient(cluster.getClusterName(), veniceUrl)) {
      versionCreationResponse = TestUtils.assertCommand(
          controllerClient.requestTopicForWrites(
              storeName,
              1024 * 1024,
              Version.PushType.BATCH,
              Version.guidBasedDummyPushId(),
              false,
              true,
              false,
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              false,
              -1));
    }
    String topic = versionCreationResponse.getKafkaTopic();
    PubSubBrokerWrapper pubSubBrokerWrapper = cluster.getPubSubBrokerWrapper();
    VeniceWriterFactory veniceWriterFactory =
        IntegrationTestPushUtils.getVeniceWriterFactory(pubSubBrokerWrapper, pubSubProducerAdapterFactory);

    try (VeniceWriter<byte[], byte[], byte[]> veniceWriter =
        veniceWriterFactory.createVeniceWriter(new VeniceWriterOptions.Builder(topic).build())) {
      veniceWriter.broadcastStartOfPush(false, Collections.emptyMap());

      Map<byte[], byte[]> sortedInputRecords = generateInput(1000, false, 0, serializer);
      sortedInputRecords.forEach((key, value) -> veniceWriter.put(key, value, 1, null));

      veniceWriter.broadcastEndOfPush(Collections.emptyMap());

      // Wait push completed.
      TestUtils.waitForNonDeterministicCompletion(
          20,
          TimeUnit.SECONDS,
          () -> cluster.getLeaderVeniceController()
              .getVeniceAdmin()
              .getOffLinePushStatus(cluster.getClusterName(), topic)
              .getExecutionStatus()
              .equals(ExecutionStatus.COMPLETED));
    }
  }

  private void restartAllRouters() {
    for (VeniceRouterWrapper router: cluster.getVeniceRouters()) {
      cluster.stopVeniceRouter(router.getPort());
      cluster.restartVeniceRouter(router.getPort());
    }
  }
}
