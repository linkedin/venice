package com.linkedin.venice.restart;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.hadoop.KafkaPushJob;
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
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
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
import org.rocksdb.Slice;
import org.rocksdb.util.BytewiseComparator;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;


@Test(singleThreaded = true)
public abstract class TestRestartServerDuringIngestion {
  private VeniceClusterWrapper cluster;
  private VeniceServerWrapper serverWrapper;
  private int replicaFactor = 1;
  private int partitionSize = 1000;
  private long testTimeOutMS = 20000;
  private final String keyPrefix = "key_";
  private final String valuePrefix = "value_";

  protected abstract PersistenceType getPersistenceType();

  private Properties getVeniceServerProperties() {
    Properties properties = new Properties();
    properties.put(ConfigKeys.PERSISTENCE_TYPE, getPersistenceType());
    properties.put(ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, 200);
    properties.put(ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_TRANSACTIONAL_MODE, 100);

    return properties;
  }

  private Map<byte[], byte[]> generateInput(int recordCnt, boolean sorted, int startId, AvroSerializer serializer) {
    Map<byte[], byte[]> records;
    if (sorted) {
      BytewiseComparator comparator = new BytewiseComparator(new ComparatorOptions());
      records = new TreeMap<>((o1, o2) -> {
        Slice s1 = new Slice(o1);
        Slice s2 = new Slice(o2);
        return comparator.compare(s1, s2);
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

  @Test(groups = {"flaky"}, timeOut = 60 * Time.MS_PER_SECOND)
  public void ingestionRecovery() throws ExecutionException, InterruptedException {
    // Create a store
    String stringSchemaStr = "\"string\"";
    AvroSerializer serializer = new AvroSerializer(Schema.parse(stringSchemaStr));
    AvroGenericDeserializer deserializer = new AvroGenericDeserializer(Schema.parse(stringSchemaStr), Schema.parse(stringSchemaStr));

    String storeName = TestUtils.getUniqueString("test_store");
    String veniceUrl = cluster.getMasterVeniceController().getControllerUrl();
    Properties properties = new Properties();
    properties.put(KafkaPushJob.VENICE_URL_PROP, veniceUrl);
    properties.put(KafkaPushJob.VENICE_STORE_NAME_PROP, storeName);
    TestPushUtils.createStoreForJob(cluster, stringSchemaStr, stringSchemaStr, properties);
    TestPushUtils.makeStoreHybrid(cluster, storeName, 3600, 10);

    // Create a new version
    ControllerClient controllerClient =
        new ControllerClient(cluster.getClusterName(), veniceUrl);
    VersionCreationResponse versionCreationResponse =
        controllerClient.requestTopicForWrites(storeName, 1024 * 1024, Version.PushType.BATCH,
            Version.guidBasedDummyPushId(), false, true, Optional.empty(), Optional.empty());
    String topic = versionCreationResponse.getKafkaTopic();
    String kafkaUrl = versionCreationResponse.getKafkaBootstrapServers();
    Properties veniceWriterProperties = new Properties();
    veniceWriterProperties.put(KAFKA_BOOTSTRAP_SERVERS, kafkaUrl);
    VeniceWriterFactory veniceWriterFactory = new VeniceWriterFactory(veniceWriterProperties);
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
          TestUtils.waitForNonDeterministicCompletion(testTimeOutMS, TimeUnit.MILLISECONDS, () -> {
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
      TestUtils.waitForNonDeterministicCompletion(testTimeOutMS, TimeUnit.MILLISECONDS,
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

      // Build a venice client to verify all the data
      AvroGenericStoreClient<String, CharSequence> storeClient = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName)
              .setVeniceURL(cluster.getRandomRouterURL())
              .setSslEngineComponentFactory(SslUtils.getLocalSslFactory())
      );

      for (Map.Entry<byte[], byte[]> entry : sortedInputRecords.entrySet()) {
        String key = deserializer.deserialize(entry.getKey()).toString();
        CharSequence expectedValue = (CharSequence)deserializer.deserialize(entry.getValue());
        CharSequence returnedValue = storeClient.get(key).get();
        Assert.assertEquals(returnedValue, expectedValue);
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
            TestUtils.waitForNonDeterministicCompletion(testTimeOutMS, TimeUnit.MILLISECONDS, () -> {
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
      TestUtils.waitForNonDeterministicCompletion(testTimeOutMS, TimeUnit.MILLISECONDS, () -> {
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

  private void restartAllRouters() {
    for (VeniceRouterWrapper router : cluster.getVeniceRouters()) {
      cluster.stopVeniceRouter(router.getPort());
      cluster.restartVeniceRouter(router.getPort());
    }
  }
}
