package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.*;
import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.*;
import static com.linkedin.venice.utils.TestPushUtils.*;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.helix.HelixParticipationService;
import com.linkedin.davinci.notifier.LeaderDoomNotifier;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.helix.HelixBaseRoutingRepository;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.VeniceUserStoreType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.InstanceConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.util.BytewiseComparator;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class TestLeaderReplicaFailover {
  private static final Logger LOGGER = LogManager.getLogger(TestLeaderReplicaFailover.class);

  String stringSchemaStr = "\"string\"";
  String valueSchemaStr =
      "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}";
  AvroSerializer serializer = new AvroSerializer(AvroCompatibilityHelper.parse(stringSchemaStr));
  AvroSerializer valueSerializer = new AvroSerializer(AvroCompatibilityHelper.parse(valueSchemaStr));

  private static final int TEST_TIMEOUT = 360 * Time.MS_PER_SECOND;
  private static String clusterName;
  private VeniceClusterWrapper clusterWrapper;

  @BeforeClass
  public void setUp() {
    String stringSchemaStr = "\"string\"";
    serializer = new AvroSerializer(AvroCompatibilityHelper.parse(stringSchemaStr));
    Properties serverProperties = new Properties();
    serverProperties.setProperty(ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1));
    serverProperties.put(LF_MODEL_DEPENDENCY_CHECK_DISABLED, "true");
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    serverProperties.put(DEFAULT_OFFLINE_PUSH_STRATEGY, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);

    Properties controllerPropery = new Properties();
    Properties parent = new Properties();
    controllerPropery.put(DEFAULT_OFFLINE_PUSH_STRATEGY, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    parent.put(DEFAULT_PARTITION_SIZE, 10000);
    int numberOfController = 1;
    int numberOfServer = 3;
    int numberOfRouter = 1;

    clusterWrapper = ServiceFactory
        .getVeniceCluster(numberOfController, numberOfServer, numberOfRouter, 3, 1, false, false, serverProperties);
    clusterName = clusterWrapper.getClusterName();
  }

  @AfterClass
  public void cleanUp() {
    clusterWrapper.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testLeaderReplicaFailover() throws Exception {
    ControllerClient parentControllerClient =
        new ControllerClient(clusterWrapper.getClusterName(), clusterWrapper.getAllControllersURLs());
    TestUtils.assertCommand(
        parentControllerClient.configureActiveActiveReplicationForCluster(
            true,
            VeniceUserStoreType.BATCH_ONLY.toString(),
            Optional.empty()));
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("store");
    Properties props = defaultVPJProps(clusterWrapper, inputDirPath, storeName);
    props.setProperty(
        DEFAULT_OFFLINE_PUSH_STRATEGY,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION.toString());
    String keySchemaStr = recordSchema.getField(VenicePushJob.DEFAULT_KEY_FIELD_PROP).schema().toString();
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
            10 * 1024,
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
    HelixAdmin admin = new ZKHelixAdmin(clusterWrapper.getZk().getAddress());
    HelixBaseRoutingRepository routingDataRepo = clusterWrapper.getLeaderVeniceController()
        .getVeniceHelixAdmin()
        .getHelixVeniceClusterResources(clusterWrapper.getClusterName())
        .getRoutingDataRepository();
    Instance leader = routingDataRepo.getLeaderInstance(topic, 0);
    for (VeniceServerWrapper serverWrapper: clusterWrapper.getVeniceServers()) {
      // Add a doom notifier which will report leader to be in ERROR instead of COMPLETE
      if (serverWrapper.getPort() == leader.getPort()) {
        HelixParticipationService participationService = serverWrapper.getVeniceServer().getHelixParticipationService();
        LeaderDoomNotifier leaderDoomNotifier = new LeaderDoomNotifier(
            participationService.getVeniceOfflinePushMonitorAccessor(),
            participationService.getInstance().getNodeId());
        participationService.getIngestionBackend().addLeaderFollowerIngestionNotifier(leaderDoomNotifier);
      }
    }
    Instance finalLeader1 = leader;

    try (VeniceWriter<byte[], byte[], byte[]> veniceWriter = veniceWriterFactory.createBasicVeniceWriter(topic)) {
      veniceWriter.broadcastStartOfPush(true, Collections.emptyMap());
      Map<byte[], byte[]> sortedInputRecords = generateData(100, true, 0, serializer);
      for (Map.Entry<byte[], byte[]> entry: sortedInputRecords.entrySet()) {
        veniceWriter.put(entry.getKey(), entry.getValue(), 1, null);
      }
      veniceWriter.broadcastEndOfPush(Collections.emptyMap());
    }

    // Wait push completed.
    TestUtils.waitForNonDeterministicCompletion(60, TimeUnit.SECONDS, () -> {
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

    // Verify the leader is disabled.
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      InstanceConfig instanceConfig = admin.getInstanceConfig(clusterName, finalLeader1.getNodeId());
      Assert.assertEquals(instanceConfig.getDisabledPartitionsMap().size(), 1);
    });
    routingDataRepo = clusterWrapper.getLeaderVeniceController()
        .getVeniceHelixAdmin()
        .getHelixVeniceClusterResources(clusterWrapper.getClusterName())
        .getRoutingDataRepository();
    leader = routingDataRepo.getLeaderInstance(topic, 0);

    // Stop the server
    clusterWrapper.stopVeniceServer(leader.getPort());
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, true, () -> {
      LOGGER.info(
          "Size " + clusterWrapper.getLeaderVeniceController().getVeniceAdmin().getReplicas(clusterName, topic).size());
      Assert.assertTrue(
          clusterWrapper.getLeaderVeniceController().getVeniceAdmin().getReplicas(clusterName, topic).size() == 6);
    });

    // Restart server, the disabled replica should be re-enabled.
    clusterWrapper.restartVeniceServer(leader.getPort());
    HelixBaseRoutingRepository finalRoutingDataRepo1 = routingDataRepo;
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, true, () -> {
      Instance newLeaderNode = finalRoutingDataRepo1.getLeaderInstance(topic, 0);
      Assert.assertNotNull(newLeaderNode);
      Assert.assertTrue(
          clusterWrapper.getLeaderVeniceController().getVeniceAdmin().getReplicas(clusterName, topic).size() == 9);
      InstanceConfig instanceConfig1 = admin.getInstanceConfig(clusterName, newLeaderNode.getNodeId());
      Assert.assertEquals(instanceConfig1.getDisabledPartitionsMap().size(), 0);
    });
  }

  private Map<byte[], byte[]> generateData(int recordCnt, boolean sorted, int startId, AvroSerializer serializer) {
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
      GenericData.Record record = new GenericData.Record(Schema.parse(valueSchemaStr));
      record.put("name", "value" + i);
      records.put(serializer.serialize("key" + i), valueSerializer.serialize(record));
    }
    return records;
  }
}
