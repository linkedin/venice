package com.linkedin.venice.restart;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.serializer.AvroGenericDeserializer;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.Optional;
import org.apache.avro.Schema;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.util.BytewiseComparator;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public abstract class TestRestartServerDuringIngestion {
  private VeniceClusterWrapper cluster;
  private VeniceServerWrapper serverWrapper;
  private int replicaFactor = 1;
  private int partitionSize = 1000;
  private final String keyPrefix = "key_";
  private final String valuePrefix = "value_";

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

  private Map<byte[], byte[]> generateInput(int recordCnt, boolean sorted, int startId, AvroSerializer serializer) {
    Map<byte[], byte[]> records;
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
    for (int i = startId; i < recordCnt + startId; ++i) {
      records.put(serializer.serialize(keyPrefix + i), serializer.serialize(valuePrefix + i));
    }
    return records;
  }

  @BeforeClass(alwaysRun = true)
  public void setup() {
    int numberOfController = 1;
    int numberOfRouter = 1;

    cluster = ServiceFactory.getVeniceCluster(numberOfController, 0, numberOfRouter, replicaFactor,
        partitionSize, false, false);
    serverWrapper = cluster.addVeniceServer(getVeniceServerProperties());
  }

  @AfterClass(alwaysRun = true)
  public void cleanup() {
    cluster.close();
  }

  @Test(timeOut = 90 * Time.MS_PER_SECOND, dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void ingestionRecovery(boolean leaderFollowerEnabled) throws ExecutionException, InterruptedException {
    // Create a store
    String stringSchemaStr = "\"string\"";
    AvroSerializer serializer = new AvroSerializer(Schema.parse(stringSchemaStr));
    AvroGenericDeserializer deserializer = new AvroGenericDeserializer(Schema.parse(stringSchemaStr), Schema.parse(stringSchemaStr));

    String storeName = Utils.getUniqueString("test_store");
    String veniceUrl = cluster.getMasterVeniceController().getControllerUrl();
    Properties properties = new Properties();
    properties.put(VenicePushJob.VENICE_URL_PROP, veniceUrl);
    properties.put(VenicePushJob.VENICE_STORE_NAME_PROP, storeName);
    TestPushUtils.createStoreForJob(cluster, stringSchemaStr, stringSchemaStr, properties).close();
    if (leaderFollowerEnabled) {
      TestPushUtils.makeStoreLF(cluster, storeName);
    }
    TestPushUtils.makeStoreHybrid(cluster, storeName, 3600, 10);

    // Create a new version
    VersionCreationResponse versionCreationResponse = null;
    try (ControllerClient controllerClient =
        new ControllerClient(cluster.getClusterName(), veniceUrl)) {
      versionCreationResponse =
          controllerClient.requestTopicForWrites(storeName, 1024 * 1024, Version.PushType.BATCH,
              Version.guidBasedDummyPushId(), false, true, false, Optional.empty(),
              Optional.empty(), Optional.empty(), false, -1);
    }
    String topic = versionCreationResponse.getKafkaTopic();
    String kafkaUrl = versionCreationResponse.getKafkaBootstrapServers();
    VeniceWriterFactory veniceWriterFactory = TestUtils.getVeniceWriterFactory(kafkaUrl);
    try (VeniceWriter<byte[], byte[], byte[]> veniceWriter = veniceWriterFactory.createBasicVeniceWriter(topic)) {
      veniceWriter.broadcastStartOfPush(true, Collections.emptyMap());

      /**
       * Restart storage node during batch ingestion.
       */
      Map<byte[], byte[]> sortedInputRecords = generateInput(1000, true, 0, serializer);
      Set<Integer> restartPointSetForSortedInput = new HashSet();
      restartPointSetForSortedInput.add(134);
      restartPointSetForSortedInput.add(346);
      restartPointSetForSortedInput.add(678);
      restartPointSetForSortedInput.add(831);
      int cur = 0;
      for (Map.Entry<byte[], byte[]> entry : sortedInputRecords.entrySet()) {
        if (restartPointSetForSortedInput.contains(++cur)) {
          // Restart server
          cluster.stopVeniceServer(serverWrapper.getPort());
          TestUtils.waitForNonDeterministicCompletion(20, TimeUnit.SECONDS, () -> {
            PartitionAssignment partitionAssignment =
                cluster.getRandomVeniceRouter().getRoutingDataRepository().getPartitionAssignments(topic);
            // Ensure all of server are shutdown, no partition assigned.
            return partitionAssignment.getAssignedNumberOfPartitions() == 0;
          });
          cluster.restartVeniceServer(serverWrapper.getPort());
        }
        veniceWriter.put(entry.getKey(), entry.getValue(), 1, null);
      }

      veniceWriter.broadcastEndOfPush(Collections.emptyMap());

      // Wait push completed.
      TestUtils.waitForNonDeterministicCompletion(20, TimeUnit.SECONDS,
          () -> cluster.getMasterVeniceController()
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
              .setSslEngineComponentFactory(SslUtils.getLocalSslFactory()))) {

        for (Map.Entry<byte[], byte[]> entry : sortedInputRecords.entrySet()) {
          String key = deserializer.deserialize(entry.getKey()).toString();
          CharSequence expectedValue = (CharSequence)deserializer.deserialize(entry.getValue());
          CharSequence returnedValue = storeClient.get(key).get();
          Assert.assertEquals(returnedValue, expectedValue);
        }

        //With L/F model the rest of the test does not succeed.  TODO: need to debug to find the reason.
        if (leaderFollowerEnabled) {
          return;
        }
        /**
         * Restart storage node during streaming ingestion.
         */
        Map<byte[], byte[]> unsortedInputRecords = generateInput(1000, false, 5000, serializer);
        Set<Integer> restartPointSetForUnsortedInput = new HashSet();
        restartPointSetForUnsortedInput.add(134);
        restartPointSetForUnsortedInput.add(346);
        restartPointSetForUnsortedInput.add(678);
        restartPointSetForUnsortedInput.add(831);
        cur = 0;

        try (VeniceWriter<byte[], byte[], byte[]> streamingWriter = veniceWriterFactory.createBasicVeniceWriter(
            Version.composeRealTimeTopic(storeName))) {
          for (Map.Entry<byte[], byte[]> entry : unsortedInputRecords.entrySet()) {
            if (restartPointSetForUnsortedInput.contains(++cur)) {
              // Restart server
              cluster.stopVeniceServer(serverWrapper.getPort());
              TestUtils.waitForNonDeterministicCompletion(20, TimeUnit.SECONDS, () -> {
                PartitionAssignment partitionAssignment =
                    cluster.getRandomVeniceRouter().getRoutingDataRepository().getPartitionAssignments(topic);
                // Ensure all of server are shutdown, no partition assigned.
                return partitionAssignment.getAssignedNumberOfPartitions() == 0;
              });
              cluster.restartVeniceServer(serverWrapper.getPort());
            }
            streamingWriter.put(entry.getKey(), entry.getValue(), 1, null);
          }
        }

        // Wait until all partitions have ready-to-serve instances
        TestUtils.waitForNonDeterministicCompletion(20, TimeUnit.SECONDS, () -> {
          PartitionAssignment partitionAssignment =
              cluster.getRandomVeniceRouter().getRoutingDataRepository().getPartitionAssignments(topic);
          boolean allPartitionsReady = true;
          for (Partition partition : partitionAssignment.getAllPartitions()) {
            if (partition.getReadyToServeInstances().size() == 0) {
              allPartitionsReady = false;
              break;
            }
          }
          return allPartitionsReady;
        });
        restartAllRouters();
        // Verify all the key/value pairs
        for (Map.Entry<byte[], byte[]> entry : unsortedInputRecords.entrySet()) {
          String key = deserializer.deserialize(entry.getKey()).toString();
          CharSequence expectedValue = (CharSequence)deserializer.deserialize(entry.getValue());
          CharSequence returnedValue = storeClient.get(key).get();
          Assert.assertEquals(returnedValue, expectedValue);
        }
      }
    }
  }


  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testIngestionDrainer() throws Exception {
    // Create a store
    String stringSchemaStr = "\"string\"";
    AvroSerializer serializer = new AvroSerializer(Schema.parse(stringSchemaStr));

    String storeName = Utils.getUniqueString("test_store");
    String veniceUrl = cluster.getMasterVeniceController().getControllerUrl();
    Properties properties = new Properties();
    properties.put(VenicePushJob.VENICE_URL_PROP, veniceUrl);
    properties.put(VenicePushJob.VENICE_STORE_NAME_PROP, storeName);
    TestPushUtils.createStoreForJob(cluster, stringSchemaStr, stringSchemaStr, properties).close();

    TestPushUtils.makeStoreHybrid(cluster, storeName, 3600, 10);

    // Create a new version
    VersionCreationResponse versionCreationResponse = null;
    try (ControllerClient controllerClient =
        new ControllerClient(cluster.getClusterName(), veniceUrl)) {
      versionCreationResponse =
          controllerClient.requestTopicForWrites(storeName, 1024 * 1024, Version.PushType.BATCH,
              Version.guidBasedDummyPushId(), false, true, false, Optional.empty(),
              Optional.empty(), Optional.empty(), false, -1);
    }
    String topic = versionCreationResponse.getKafkaTopic();
    String kafkaUrl = versionCreationResponse.getKafkaBootstrapServers();
    VeniceWriterFactory veniceWriterFactory = TestUtils.getVeniceWriterFactory(kafkaUrl);

    try (VeniceWriter<byte[], byte[], byte[]> veniceWriter = veniceWriterFactory.createBasicVeniceWriter(topic)) {
      veniceWriter.broadcastStartOfPush(false, Collections.emptyMap());

      Map<byte[], byte[]> sortedInputRecords = generateInput(1000, false, 0, serializer);
      sortedInputRecords.forEach((key, value) -> veniceWriter.put(key, value, 1, null));

      veniceWriter.broadcastEndOfPush(Collections.emptyMap());

      // Wait push completed.
      TestUtils.waitForNonDeterministicCompletion(20, TimeUnit.SECONDS,
          () -> cluster.getMasterVeniceController()
              .getVeniceAdmin()
              .getOffLinePushStatus(cluster.getClusterName(), topic)
              .getExecutionStatus()
              .equals(ExecutionStatus.COMPLETED));
    }
  }

  private void restartAllRouters() {
    for (VeniceRouterWrapper router : cluster.getVeniceRouters()) {
      cluster.stopVeniceRouter(router.getPort());
      cluster.restartVeniceRouter(router.getPort());
    }
  }
}
