package com.linkedin.venice.controller;

import static com.linkedin.venice.controller.VeniceHelixAdmin.OfflinePushStatusInfo;
import static com.linkedin.venice.controller.VeniceHelixAdmin.VERSION_ID_UNSET;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.exception.HelixClusterMaintenanceModeException;
import com.linkedin.venice.controllerapi.UpdateClusterConfigQueryParams;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.helix.HelixReadOnlyLiveClusterConfigRepository;
import com.linkedin.venice.helix.HelixStatusMessageChannel;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.TopicManagerRepository;
import com.linkedin.venice.kafka.VeniceOperationAgainstKafkaTimedOut;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.LiveClusterConfig;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyLiveClusterConfigRepository;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.KillOfflinePushMessage;
import com.linkedin.venice.pushmonitor.PushMonitor;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.utils.MockTestStateModelFactory;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.views.ChangeCaptureView;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.IdealState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Helix Admin test cases which share the same Venice cluster. Please make sure to have the proper
 * clean-up and set cluster back to its default settings after finishing the tests.
 *implements Closeable
 * If it's hard to set cluster back, please move the tests to {@link TestVeniceHelixAdminWithIsolatedEnvironment}
 */
public class TestVeniceHelixAdminWithSharedEnvironment extends AbstractTestVeniceHelixAdmin {
  private final MetricsRepository metricsRepository = new MetricsRepository();

  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private final static Logger LOGGER = LogManager.getLogger(TestVeniceHelixAdminWithSharedEnvironment.class);

  @BeforeClass(alwaysRun = true)
  public void setUp() throws Exception {
    setupCluster(true, metricsRepository);
    verifyParticipantMessageStoreSetup();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    // Controller shutdown needs to complete within 5 minutes
    ExecutorService ex = Executors.newSingleThreadExecutor();
    Future clusterShutdownFuture = ex.submit(this::cleanupCluster);
    TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.MINUTES, () -> clusterShutdownFuture.isDone());
    ex.shutdownNow();
  }

  @Test(timeOut = TOTAL_TIMEOUT_FOR_SHORT_TEST_MS)
  public void testStartClusterAndCreatePush() {
    try {
      String storeName = Utils.getUniqueString("test-store");
      veniceAdmin.createStore(clusterName, storeName, "dev", KEY_SCHEMA, VALUE_SCHEMA);
      String topicName = Version.composeKafkaTopic(storeName, 1);
      Assert.assertEquals(
          veniceAdmin.getOffLinePushStatus(clusterName, topicName).getExecutionStatus(),
          ExecutionStatus.NOT_CREATED,
          "Offline job status should not already exist.");
      veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
      Assert.assertNotEquals(
          veniceAdmin.getOffLinePushStatus(clusterName, topicName).getExecutionStatus(),
          ExecutionStatus.NOT_CREATED,
          "Can not get offline job status correctly.");
    } catch (VeniceException e) {
      Assert.fail("Should be able to create store after starting cluster");
    }
  }

  @Test(timeOut = TOTAL_TIMEOUT_FOR_LONG_TEST_MS)
  public void testIsLeaderController() {
    Assert.assertTrue(
        veniceAdmin.isLeaderControllerFor(clusterName),
        "The default controller should be the leader controller.");

    int newAdminPort = controllerConfig.getAdminPort() + 1; /* Note: dummy port */
    PropertyBuilder builder = new PropertyBuilder().put(controllerProps.toProperties()).put("admin.port", newAdminPort);

    VeniceProperties newControllerProps = builder.build();
    VeniceControllerConfig newConfig = new VeniceControllerConfig(newControllerProps);
    VeniceHelixAdmin newLeaderAdmin = new VeniceHelixAdmin(
        TestUtils.getMultiClusterConfigFromOneCluster(newConfig),
        new MetricsRepository(),
        D2TestUtils.getAndStartD2Client(zkAddress),
        pubSubTopicRepository,
        pubSubBrokerWrapper.getPubSubClientsFactory());
    // Start stand by controller
    newLeaderAdmin.initStorageCluster(clusterName);
    Assert.assertFalse(
        veniceAdmin.isLeaderControllerFor(clusterName) && newLeaderAdmin.isLeaderControllerFor(clusterName),
        "At most one controller can be the leader.");
    veniceAdmin.stop(clusterName);
    // Waiting state transition from standby->leader on new admin
    waitUntilIsLeader(newLeaderAdmin, clusterName, LEADER_CHANGE_TIMEOUT_MS);
    Assert.assertTrue(
        newLeaderAdmin.isLeaderControllerFor(clusterName),
        "The new controller should be the leader controller right now.");
    veniceAdmin.initStorageCluster(clusterName);
    waitForALeader(Arrays.asList(veniceAdmin, newLeaderAdmin), clusterName, LEADER_CHANGE_TIMEOUT_MS);

    /* XOR */
    Assert.assertTrue(
        veniceAdmin.isLeaderControllerFor(clusterName) || newLeaderAdmin.isLeaderControllerFor(clusterName));
    Assert.assertFalse(
        veniceAdmin.isLeaderControllerFor(clusterName) && newLeaderAdmin.isLeaderControllerFor(clusterName));

    // resume to the original venice admin
    veniceAdmin.initStorageCluster(clusterName);
    newLeaderAdmin.close();
    waitUntilIsLeader(veniceAdmin, clusterName, LEADER_CHANGE_TIMEOUT_MS);
  }

  @Test(timeOut = TOTAL_TIMEOUT_FOR_SHORT_TEST_MS)
  public void testMultiCluster() {
    String newClusterName = "new_test_cluster";
    PropertyBuilder builder =
        new PropertyBuilder().put(controllerProps.toProperties()).put("cluster.name", newClusterName);

    VeniceProperties newClusterProps = builder.build();
    VeniceControllerConfig newClusterConfig = new VeniceControllerConfig(newClusterProps);
    veniceAdmin.addConfig(newClusterConfig);
    veniceAdmin.initStorageCluster(newClusterName);
    waitUntilIsLeader(veniceAdmin, newClusterName, LEADER_CHANGE_TIMEOUT_MS);

    Assert.assertTrue(veniceAdmin.isLeaderControllerFor(clusterName));
    Assert.assertTrue(veniceAdmin.isLeaderControllerFor(newClusterName));
  }

  @Test(timeOut = TOTAL_TIMEOUT_FOR_SHORT_TEST_MS)
  public void testGetNumberOfPartition() {
    long partitionSize = controllerConfig.getPartitionSize();
    int maxPartitionNumber = controllerConfig.getMaxNumberOfPartition();
    int minPartitionNumber = controllerConfig.getNumberOfPartition();
    String storeName = Utils.getUniqueString("test");

    veniceAdmin.createStore(clusterName, storeName, "dev", KEY_SCHEMA, VALUE_SCHEMA);

    long storageQuota = partitionSize * (minPartitionNumber + 1);
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(storageQuota));
    int numberOfPartition = veniceAdmin.calculateNumberOfPartitions(clusterName, storeName);
    Assert.assertEquals(
        numberOfPartition,
        storageQuota / partitionSize,
        "Number partition is smaller than max and bigger than min. So use the calculated result.");
    storageQuota = 1;
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(storageQuota));
    numberOfPartition = veniceAdmin.calculateNumberOfPartitions(clusterName, storeName);
    Assert.assertEquals(
        numberOfPartition,
        minPartitionNumber,
        "Store disk quota is too small so should use min number of partitions.");
    storageQuota = partitionSize * (maxPartitionNumber + 1);
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(storageQuota));
    numberOfPartition = veniceAdmin.calculateNumberOfPartitions(clusterName, storeName);
    Assert.assertEquals(
        numberOfPartition,
        maxPartitionNumber,
        "Store disk quota is too big, should use max number of partitions.");

    storageQuota = Long.MAX_VALUE;
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(storageQuota));
    numberOfPartition = veniceAdmin.calculateNumberOfPartitions(clusterName, storeName);
    Assert.assertEquals(numberOfPartition, maxPartitionNumber, "Partition is overflow from Integer, use max one.");

    // invalid storage quota; update store should fail.
    Assert.assertThrows(
        VeniceException.class,
        () -> veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(-2)));
  }

  @Test(timeOut = TOTAL_TIMEOUT_FOR_SHORT_TEST_MS)
  public void testGetNumberOfPartitionsFromStoreLevelConfig() {
    long partitionSize = controllerConfig.getPartitionSize();
    int maxPartitionNumber = controllerConfig.getMaxNumberOfPartition();
    int minPartitionNumber = controllerConfig.getNumberOfPartition();
    String storeName = Utils.getUniqueString("test");

    veniceAdmin.createStore(clusterName, storeName, "dev", KEY_SCHEMA, VALUE_SCHEMA);
    long storageQuota = partitionSize * (minPartitionNumber) + 1;
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(storageQuota));
    int numberOfPartition = veniceAdmin.calculateNumberOfPartitions(clusterName, storeName);
    Store store =
        veniceAdmin.getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository().getStore(storeName);
    store.setPartitionCount(numberOfPartition);
    veniceAdmin.getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository().updateStore(store);
    Version v = veniceAdmin
        .incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), numberOfPartition, 1);
    veniceAdmin.setStoreCurrentVersion(clusterName, storeName, v.getNumber());
    storageQuota = partitionSize * (maxPartitionNumber - 2);
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(storageQuota));
    numberOfPartition = veniceAdmin.calculateNumberOfPartitions(clusterName, storeName);
    Assert.assertEquals(numberOfPartition, minPartitionNumber, "Should use the number of partition from store config");
  }

  @Test
  public void testHandleVersionCreationFailure() {
    String storeName = Utils.getUniqueString("test");
    veniceAdmin.createStore(clusterName, storeName, "owner", KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin
        .updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(DEFAULT_REPLICA_COUNT));
    // Register the handle for kill message. Otherwise, when job manager collect the old version, it would meet error
    // after sending kill job message. Because, participant can not handle message correctly.
    HelixStatusMessageChannel channel =
        new HelixStatusMessageChannel(helixManagerByNodeID.get(NODE_ID), helixMessageChannelStats);
    channel.registerHandler(KillOfflinePushMessage.class, message -> {/*ignore*/});

    delayParticipantJobCompletion(true);

    Version version =
        veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    int versionNumber = version.getNumber();

    Admin.OfflinePushStatusInfo offlinePushStatus =
        veniceAdmin.getOffLinePushStatus(clusterName, Version.composeKafkaTopic(storeName, versionNumber));
    Assert.assertEquals(offlinePushStatus.getExecutionStatus(), ExecutionStatus.STARTED);

    String statusDetails = "synthetic error message";
    veniceAdmin.handleVersionCreationFailure(clusterName, storeName, versionNumber, statusDetails);
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions().size(), 0);
    offlinePushStatus =
        veniceAdmin.getOffLinePushStatus(clusterName, Version.composeKafkaTopic(storeName, versionNumber));
    Assert.assertEquals(offlinePushStatus.getExecutionStatus(), ExecutionStatus.ERROR);
    Assert.assertNotNull(offlinePushStatus.getStatusDetails());
    Assert.assertEquals(offlinePushStatus.getStatusDetails(), statusDetails);

    delayParticipantJobCompletion(false);
    stateModelFactoryByNodeID
        .forEach((nodeId, stateModelFactory) -> stateModelFactory.makeTransitionCompleted(version.kafkaTopicName(), 0));
  }

  @Test
  public void testHandleVersionCreationFailureWithCurrentVersion() {
    String storeName = Utils.getUniqueString("test");
    veniceAdmin.createStore(clusterName, storeName, "owner", KEY_SCHEMA, VALUE_SCHEMA);
    Version version =
        veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    int versionNumber = version.getNumber();
    TestUtils.waitForNonDeterministicCompletion(
        30,
        TimeUnit.SECONDS,
        () -> veniceAdmin.getCurrentVersion(clusterName, storeName) == versionNumber);

    String statusDetails = "synthetic error message";
    try {
      veniceAdmin.handleVersionCreationFailure(clusterName, storeName, versionNumber, statusDetails);
    } catch (VeniceUnsupportedOperationException e) {
      Assert.assertTrue(e.getMessage().contains("The current version could not be deleted from store"));
    }
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getCurrentVersion(), 1);
    Admin.OfflinePushStatusInfo offlinePushStatus =
        veniceAdmin.getOffLinePushStatus(clusterName, Version.composeKafkaTopic(storeName, versionNumber));
    Assert.assertEquals(offlinePushStatus.getExecutionStatus(), ExecutionStatus.COMPLETED);
  }

  @Test
  public void testDeleteOldVersions() {
    String storeName = Utils.getUniqueString("test");
    veniceAdmin.createStore(clusterName, storeName, "owner", KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin
        .updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(DEFAULT_REPLICA_COUNT));
    // Register the handle for kill message. Otherwise, when job manager collect the old version, it would meet error
    // after sending kill job message. Because, participant can not handle message correctly.
    HelixStatusMessageChannel channel =
        new HelixStatusMessageChannel(helixManagerByNodeID.get(NODE_ID), helixMessageChannelStats);
    channel.registerHandler(KillOfflinePushMessage.class, message -> {/*ignore*/});
    Version version = null;
    for (int i = 0; i < 3; i++) {
      version = veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
      int versionNumber = version.getNumber();

      TestUtils.waitForNonDeterministicCompletion(
          30,
          TimeUnit.SECONDS,
          () -> veniceAdmin.getCurrentVersion(clusterName, storeName) == versionNumber);
    }

    // Store-level write lock is shared between VeniceHelixAdmin and AbstractPushMonitor. It's not guaranteed that the
    // new version is online and old version will be deleted during VeniceHelixAdmin#addVersion early backup deletion.
    // Explicitly run early backup deletion again to make the test deterministic.
    veniceAdmin.retireOldStoreVersions(clusterName, storeName, true, VERSION_ID_UNSET);
    TestUtils.waitForNonDeterministicCompletion(
        30,
        TimeUnit.SECONDS,
        () -> veniceAdmin.versionsForStore(clusterName, storeName).size() == 1);
    Assert.assertEquals(veniceAdmin.getCurrentVersion(clusterName, storeName), version.getNumber());
    Assert.assertEquals(veniceAdmin.versionsForStore(clusterName, storeName).get(0).getNumber(), version.getNumber());

    Version deletedVersion = new VersionImpl(storeName, version.getNumber() - 2);
    // Ensure job and topic are deleted
    TestUtils.waitForNonDeterministicCompletion(
        30,
        TimeUnit.SECONDS,
        () -> veniceAdmin.getOffLinePushStatus(clusterName, deletedVersion.kafkaTopicName())
            .getExecutionStatus()
            .equals(ExecutionStatus.NOT_CREATED));
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Assert.assertTrue(veniceAdmin.isTopicTruncated(deletedVersion.kafkaTopicName()));
    });
  }

  @Test
  public void testDeleteResourceThenRestartParticipant() throws Exception {
    delayParticipantJobCompletion(true);
    String storeName = "testDeleteResource";
    veniceAdmin.createStore(clusterName, storeName, "owner", KEY_SCHEMA, VALUE_SCHEMA);
    Version version =
        veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    // Ensure the replica has become BOOTSTRAP
    TestUtils.waitForNonDeterministicCompletion(3, TimeUnit.SECONDS, () -> {
      RoutingDataRepository routingDataRepository =
          veniceAdmin.getHelixVeniceClusterResources(clusterName).getRoutingDataRepository();
      return routingDataRepository.containsKafkaTopic(version.kafkaTopicName())
          && routingDataRepository.getPartitionAssignments(version.kafkaTopicName())
              .getPartition(0)
              .getWorkingInstances()
              .size() == 1;
    });
    // disconnect the participant
    stopParticipant(NODE_ID);
    // ensure it has disappeared from external view.
    TestUtils.waitForNonDeterministicCompletion(3, TimeUnit.SECONDS, () -> {
      RoutingDataRepository routingDataRepository =
          veniceAdmin.getHelixVeniceClusterResources(clusterName).getRoutingDataRepository();
      return routingDataRepository.getPartitionAssignments(version.kafkaTopicName())
          .getAssignedNumberOfPartitions() == 0;
    });
    veniceAdmin.deleteHelixResource(clusterName, version.kafkaTopicName());
    // Ensure idealstate is null which means resource has been deleted.
    TestUtils.waitForNonDeterministicCompletion(3, TimeUnit.SECONDS, () -> {
      PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
      IdealState idealState = veniceAdmin.getHelixVeniceClusterResources(clusterName)
          .getHelixManager()
          .getHelixDataAccessor()
          .getProperty(keyBuilder.idealStates(version.kafkaTopicName()));
      return idealState == null;
    });
    // Start participant again
    startParticipant(true, NODE_ID);
    // Ensure resource has been deleted in external view.
    TestUtils.waitForNonDeterministicCompletion(3, TimeUnit.SECONDS, () -> {
      RoutingDataRepository routingDataRepository =
          veniceAdmin.getHelixVeniceClusterResources(clusterName).getRoutingDataRepository();
      return !routingDataRepository.containsKafkaTopic(version.kafkaTopicName());
    });

    stateModelFactoryByNodeID.forEach(
        (nodeId, stateModelFactory) -> Assert
            .assertEquals(stateModelFactory.getModelList(version.kafkaTopicName(), 0).size(), 1));
    // Replica become OFFLINE state
    stateModelFactoryByNodeID.forEach(
        (nodeId, stateModelFactory) -> Assert.assertEquals(
            stateModelFactory.getModelList(version.kafkaTopicName(), 0).get(0).getCurrentState(),
            "OFFLINE"));
    delayParticipantJobCompletion(false);
  }

  @Test
  public void testUpdateStoreMetadata() throws Exception {
    String storeName = Utils.getUniqueString("test");
    String owner = Utils.getUniqueString("owner");
    int partitionCount = 1;

    // test setting new version

    // The existing participant uses a non-blocking state model which will switch to COMPLETE immediately. We add
    // an additional participant here that uses a blocking state model so it doesn't switch to complete. This way
    // the replicas will not all be COMPLETE, and the new version will not immediately be activated.
    String additionalNode = "localhost_6868";
    startParticipant(true, additionalNode);
    veniceAdmin.createStore(clusterName, storeName, owner, KEY_SCHEMA, VALUE_SCHEMA);

    Version version = veniceAdmin
        .incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), partitionCount, 2); // 2
                                                                                                                // replicas
                                                                                                                // puts
                                                                                                                // a
                                                                                                                // replica
                                                                                                                // on
                                                                                                                // the
                                                                                                                // blocking
                                                                                                                // participant
    Assert.assertEquals(veniceAdmin.getCurrentVersion(clusterName, storeName), 0);
    veniceAdmin.setStoreCurrentVersion(clusterName, storeName, version.getNumber());
    Assert.assertEquals(veniceAdmin.getCurrentVersion(clusterName, storeName), version.getNumber());

    // Version 100 does not exist. Should be failed
    Assert.assertThrows(VeniceException.class, () -> veniceAdmin.setStoreCurrentVersion(clusterName, storeName, 100));

    // test setting owner
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getOwner(), owner);
    String newOwner = Utils.getUniqueString("owner");

    veniceAdmin.setStoreOwner(clusterName, storeName, newOwner);
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getOwner(), newOwner);

    // test setting partition count
    int newPartitionCount = 2;
    Assert.assertEquals(
        veniceAdmin.getStore(clusterName, storeName).getVersion(version.getNumber()).get().getPartitionCount(),
        partitionCount);

    veniceAdmin.setStorePartitionCount(clusterName, storeName, newPartitionCount);
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getPartitionCount(), newPartitionCount);
    Assert.assertThrows(() -> veniceAdmin.setStorePartitionCount(clusterName, storeName, MAX_NUMBER_OF_PARTITION + 1));
    Assert.assertThrows(() -> veniceAdmin.setStorePartitionCount(clusterName, storeName, -1));

    // test setting amplification factor
    Assert.assertEquals(
        veniceAdmin.getStore(clusterName, storeName)
            .getVersion(version.getNumber())
            .get()
            .getPartitionerConfig()
            .getAmplificationFactor(),
        1);
    final int amplificationFactor = 10;
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setAmplificationFactor(amplificationFactor);
    veniceAdmin.setStorePartitionerConfig(clusterName, storeName, partitionerConfig);
    Version newVersion = veniceAdmin
        .incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), partitionCount, 2);
    Assert.assertEquals(
        veniceAdmin.getStore(clusterName, storeName)
            .getVersion(newVersion.getNumber())
            .get()
            .getPartitionerConfig()
            .getAmplificationFactor(),
        amplificationFactor);

    veniceAdmin.setIncrementalPushEnabled(clusterName, storeName, true);
    Assert.assertTrue(veniceAdmin.getStore(clusterName, storeName).isIncrementalPushEnabled());

    veniceAdmin.setBootstrapToOnlineTimeoutInHours(clusterName, storeName, 48);
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getBootstrapToOnlineTimeoutInHours(), 48);

    veniceAdmin.setHybridStoreDiskQuotaEnabled(clusterName, storeName, true);
    Assert.assertTrue(veniceAdmin.getStore(clusterName, storeName).isHybridStoreDiskQuotaEnabled());

    // test setting per-store RMD (replication metadata) version ID
    int rmdVersion = veniceAdmin.getStore(clusterName, storeName).getRmdVersion();
    Assert.assertEquals(rmdVersion, -1);

    veniceAdmin.setReplicationMetadataVersionID(clusterName, storeName, 2);
    rmdVersion = veniceAdmin.getStore(clusterName, storeName).getRmdVersion();
    Assert.assertNotEquals(rmdVersion, -1);
    Assert.assertEquals(rmdVersion, 2);

    // test hybrid config
    // set incrementalPushEnabled to be false as hybrid and incremental are mutex
    veniceAdmin.setIncrementalPushEnabled(clusterName, storeName, false);
    Assert.assertFalse(veniceAdmin.getStore(clusterName, storeName).isHybrid());
    veniceAdmin.updateStore(
        clusterName,
        storeName,
        new UpdateStoreQueryParams().setHybridRewindSeconds(TimeUnit.SECONDS.convert(2, TimeUnit.DAYS))
            .setHybridOffsetLagThreshold(1000L));
    Assert.assertTrue(veniceAdmin.getStore(clusterName, storeName).isHybrid());

    // test updating hybrid data replication policy
    veniceAdmin.updateStore(
        clusterName,
        storeName,
        new UpdateStoreQueryParams().setHybridDataReplicationPolicy(DataReplicationPolicy.AGGREGATE));
    Assert.assertEquals(
        veniceAdmin.getStore(clusterName, storeName).getHybridStoreConfig().getDataReplicationPolicy(),
        DataReplicationPolicy.AGGREGATE);

    // test reverting hybrid store back to batch-only store; negative config value will undo hybrid setting
    veniceAdmin.updateStore(
        clusterName,
        storeName,
        new UpdateStoreQueryParams().setHybridRewindSeconds(-1)
            .setHybridOffsetLagThreshold(-1)
            .setHybridTimeLagThreshold(-1));
    Assert.assertFalse(veniceAdmin.getStore(clusterName, storeName).isHybrid());

    // test setting hybrid config with rewind time and time lag
    veniceAdmin.updateStore(
        clusterName,
        storeName,
        new UpdateStoreQueryParams().setHybridRewindSeconds(TimeUnit.SECONDS.convert(2, TimeUnit.DAYS))
            .setHybridTimeLagThreshold(TimeUnit.MINUTES.toSeconds(1)));
    Assert.assertTrue(veniceAdmin.getStore(clusterName, storeName).isHybrid());

    stopParticipant(additionalNode);
    delayParticipantJobCompletion(false);
    stateModelFactoryByNodeID
        .forEach((nodeId, stateModelFactory) -> stateModelFactory.makeTransitionCompleted(version.kafkaTopicName(), 0));
  }

  @Test
  public void testAddVersionAndStartIngestionTopicCreationTimeout() {
    TopicManagerRepository originalTopicManagerRepository = veniceAdmin.getTopicManagerRepository();

    TopicManager mockedTopicManager = mock(TopicManager.class);
    doThrow(new VeniceOperationAgainstKafkaTimedOut("mock timeout")).when(mockedTopicManager)
        .createTopic(any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any(), eq(true));
    TopicManagerRepository mockedTopicManageRepository = mock(TopicManagerRepository.class);
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager();
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager(any(String.class));
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager(anyString());
    veniceAdmin.setTopicManagerRepository(mockedTopicManageRepository);
    String storeName = "test-store";
    String pushJobId = "test-push-job-id";
    veniceAdmin.createStore(clusterName, storeName, "test-owner", KEY_SCHEMA, VALUE_SCHEMA);
    for (int i = 0; i < 5; i++) {
      // Mimic the retry behavior by the admin consumption task.
      Assert.assertThrows(
          VeniceOperationAgainstKafkaTimedOut.class,
          () -> veniceAdmin.addVersionAndStartIngestion(
              clusterName,
              storeName,
              pushJobId,
              1,
              1,
              Version.PushType.BATCH,
              null,
              -1,
              multiClusterConfig.getCommonConfig().getReplicationMetadataVersion(),
              false));
    }
    Assert.assertFalse(veniceAdmin.getStore(clusterName, storeName).getVersion(1).isPresent());
    veniceAdmin
        .updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(DEFAULT_REPLICA_COUNT));
    reset(mockedTopicManager);
    veniceAdmin.addVersionAndStartIngestion(
        clusterName,
        storeName,
        pushJobId,
        1,
        1,
        Version.PushType.BATCH,
        null,
        -1,
        multiClusterConfig.getCommonConfig().getReplicationMetadataVersion(),
        false);
    Assert.assertTrue(veniceAdmin.getStore(clusterName, storeName).getVersion(1).isPresent());
    Assert.assertEquals(
        veniceAdmin.getStore(clusterName, storeName).getVersions().size(),
        1,
        "There should only be exactly one version added to the test-store");

    // Set topic original topic manager back
    veniceAdmin.setTopicManagerRepository(originalTopicManagerRepository);
  }

  @Test
  public void testAddVersionWhenClusterInMaintenanceMode() {
    String storeName = Utils.getUniqueString("test");

    veniceAdmin.createStore(clusterName, storeName, "owner", KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin
        .updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(DEFAULT_REPLICA_COUNT));
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    Assert.assertEquals(veniceAdmin.versionsForStore(clusterName, storeName).size(), 1);

    // enable maintenance mode
    veniceAdmin.getHelixAdmin().enableMaintenanceMode(clusterName, true);

    // HelixClusterMaintenanceModeException is expected since cluster is in maintenance mode
    Assert.assertThrows(
        HelixClusterMaintenanceModeException.class,
        () -> veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1));

    Admin.OfflinePushStatusInfo statusInfo =
        veniceAdmin.getOffLinePushStatus(clusterName, Version.composeKafkaTopic(storeName, 101));
    Assert.assertEquals(statusInfo.getExecutionStatus(), ExecutionStatus.NOT_CREATED);
    Assert.assertTrue(statusInfo.getStatusDetails().contains("in maintenance mode"));

    // disable maintenance mode
    veniceAdmin.getHelixAdmin().enableMaintenanceMode(clusterName, false);
    // try to add same version again
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    TestUtils.waitForNonDeterministicCompletion(
        30,
        TimeUnit.SECONDS,
        () -> veniceAdmin.getCurrentVersion(clusterName, storeName) == 2);
    veniceAdmin.retireOldStoreVersions(clusterName, storeName, true, VERSION_ID_UNSET);
    Assert.assertEquals(veniceAdmin.versionsForStore(clusterName, storeName).size(), 1);

    veniceAdmin.getHelixAdmin().enableMaintenanceMode(clusterName, false);

  }

  @Test
  public void testGetRealTimeTopic() {
    String storeName = Utils.getUniqueString("store");

    // Must not be able to get a real time topic until the store is created
    Assert.assertThrows(VeniceNoStoreException.class, () -> veniceAdmin.getRealTimeTopic(clusterName, storeName));

    veniceAdmin.createStore(clusterName, storeName, "owner", KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin.updateStore(
        clusterName,
        storeName,
        new UpdateStoreQueryParams().setHybridRewindSeconds(25L).setHybridOffsetLagThreshold(100L)); // make store
                                                                                                     // hybrid

    try {
      veniceAdmin.getRealTimeTopic(clusterName, storeName);
      Assert.fail("Must not be able to get a real time topic until the store is initialized with a version");
    } catch (VeniceException e) {
      Assert.assertTrue(
          e.getMessage().contains("is not initialized with a version"),
          "Got unexpected error message: " + e.getMessage());
    }

    int partitions = 2; // TODO verify partition count for RT topic.
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), partitions, 1);

    String rtTopic = veniceAdmin.getRealTimeTopic(clusterName, storeName);
    Assert.assertEquals(rtTopic, storeName + "_rt");
  }

  @Test
  public void testGetAndCompareStorageNodeStatusForStorageNode() throws Exception {
    String storeName = "testGetStorageNodeStatusForStorageNode";
    int partitionCount = 2;
    int replicaCount = 2;
    // Start a new participant which would hang on bootstrap state.
    String newNodeId = "localhost_9900";
    // Ensure original participant would hang on bootstrap state.
    delayParticipantJobCompletion(true);
    startParticipant(true, newNodeId);
    veniceAdmin.createStore(clusterName, storeName, storeOwner, KEY_SCHEMA, VALUE_SCHEMA);
    Version version = veniceAdmin.incrementVersionIdempotent(
        clusterName,
        storeName,
        Version.guidBasedDummyPushId(),
        partitionCount,
        replicaCount);

    TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS, () -> {
      PartitionAssignment partitionAssignment = veniceAdmin.getHelixVeniceClusterResources(clusterName)
          .getRoutingDataRepository()
          .getPartitionAssignments(version.kafkaTopicName());
      if (partitionAssignment.getAssignedNumberOfPartitions() != partitionCount) {
        return false;
      }
      for (int i = 0; i < partitionCount; i++) {
        if (partitionAssignment.getPartition(i).getWorkingInstances().size() != partitionCount) {
          return false;
        }
      }
      return true;
    });

    // Now all of replica in bootstrap state
    StorageNodeStatus status2 = veniceAdmin.getStorageNodesStatus(clusterName, newNodeId);

    // TODO: Ideally this test should use CUSTOMIZEDVIEW to determine per instance status, but this test mock doesn't
    // actually build a CUSTOMIZEDVIEW for the
    // the cluster. Needs to be refactored.
    OfflinePushStatusInfo pushStatusInfo =
        veniceAdmin.getOffLinePushStatus(clusterName, Version.composeKafkaTopic(storeName, 1));

    Assert.assertEquals(
        pushStatusInfo.getExecutionStatus(),
        ExecutionStatus.STARTED,
        "Replica in server1 should hang on STANDBY");

    // Set replicas to ONLINE.
    for (int i = 0; i < partitionCount; i++) {
      for (Map.Entry<String, MockTestStateModelFactory> entry: stateModelFactoryByNodeID.entrySet()) {
        MockTestStateModelFactory value = entry.getValue();
        value.makeTransitionCompleted(version.kafkaTopicName(), i);
      }
    }

    TestUtils.waitForNonDeterministicCompletion(10, TimeUnit.SECONDS, () -> {
      PartitionAssignment partitionAssignment = veniceAdmin.getHelixVeniceClusterResources(clusterName)
          .getRoutingDataRepository()
          .getPartitionAssignments(version.kafkaTopicName());
      for (int i = 0; i < partitionCount; i++) {
        if (partitionAssignment.getPartition(i).getWorkingInstances().size() != partitionCount) {
          return false;
        }
      }
      return true;
    });

    StorageNodeStatus newStatus2 = veniceAdmin.getStorageNodesStatus(clusterName, newNodeId);
    Assert.assertTrue(newStatus2.isNewerOrEqual(status2), "LEADER replicas should be newer than STANDBY replicas");

    stopParticipant(newNodeId);
    delayParticipantJobCompletion(false);
  }

  @Test
  public void testDisableStoreWrite() {
    String storeName = Utils.getUniqueString("testDisableStoreWriter");
    veniceAdmin.createStore(clusterName, storeName, storeOwner, KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin
        .updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(DEFAULT_REPLICA_COUNT));
    veniceAdmin.setStoreWriteability(clusterName, storeName, false);
    Store store = veniceAdmin.getStore(clusterName, storeName);

    // Store has been disabled, can not accept a new version
    Assert.assertThrows(
        VeniceException.class,
        () -> veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1));

    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions(), store.getVersions());

    // Store has been disabled, can not accept a new version
    Assert.assertThrows(
        VeniceException.class,
        () -> veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1));

    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions(), store.getVersions());

    veniceAdmin.setStoreWriteability(clusterName, storeName, true);

    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    TestUtils.waitForNonDeterministicCompletion(
        30,
        TimeUnit.SECONDS,
        () -> veniceAdmin.getCurrentVersion(clusterName, storeName) == 2);
    veniceAdmin.retireOldStoreVersions(clusterName, storeName, true, VERSION_ID_UNSET);

    store = veniceAdmin.getStore(clusterName, storeName);
    // Version 1 and version 2 are added to this store. Version 1 is deleted by early backup deletion
    Assert.assertTrue(store.isEnableWrites());
    Assert.assertEquals(store.getVersions().size(), 1);
    Assert.assertEquals(store.peekNextVersion().getNumber(), 3);
    PushMonitor monitor = veniceAdmin.getHelixVeniceClusterResources(clusterName).getPushMonitor();
    TestUtils.waitForNonDeterministicCompletion(
        TOTAL_TIMEOUT_FOR_SHORT_TEST_MS,
        TimeUnit.MILLISECONDS,
        () -> monitor.getPushStatusAndDetails(Version.composeKafkaTopic(storeName, 2))
            .getFirst()
            .equals(ExecutionStatus.COMPLETED));
  }

  @Test
  public void testDisableStoreRead() {
    String storeName = Utils.getUniqueString("testDisableStoreRead");
    veniceAdmin.createStore(clusterName, storeName, storeOwner, KEY_SCHEMA, VALUE_SCHEMA);
    Version version =
        veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    veniceAdmin.setStoreCurrentVersion(clusterName, storeName, version.getNumber());

    veniceAdmin.setStoreReadability(clusterName, storeName, false);
    Assert.assertEquals(
        veniceAdmin.getCurrentVersion(clusterName, storeName),
        Store.NON_EXISTING_VERSION,
        "After disabling, store has no version available to serve.");

    veniceAdmin.setStoreReadability(clusterName, storeName, true);
    Assert.assertEquals(
        veniceAdmin.getCurrentVersion(clusterName, storeName),
        version.getNumber(),
        "After enabling, version:" + version.getNumber() + " is ready to serve.");
  }

  @Test
  public void testAccessControl() {
    String storeName = "testAccessControl";
    veniceAdmin.createStore(clusterName, storeName, storeOwner, KEY_SCHEMA, VALUE_SCHEMA);

    veniceAdmin.setAccessControl(clusterName, storeName, false);
    Store store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertFalse(store.isAccessControlled());

    veniceAdmin.setAccessControl(clusterName, storeName, true);
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertTrue(store.isAccessControlled());

    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setAccessControlled(false));
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertFalse(store.isAccessControlled());

    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setAccessControlled(true));
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertTrue(store.isAccessControlled());
  }

  @Test
  public void testAllowlist() {
    int testPort = 5555;
    Assert.assertEquals(veniceAdmin.getAllowlist(clusterName).size(), 0, "Allow list should be empty.");

    veniceAdmin.addInstanceToAllowlist(clusterName, Utils.getHelixNodeIdentifier(Utils.getHostName(), testPort));
    Assert.assertEquals(
        veniceAdmin.getAllowlist(clusterName).size(),
        1,
        "After adding a instance into allowlist, the size of allowlist should be 1");

    Assert.assertEquals(
        veniceAdmin.getAllowlist(clusterName).iterator().next(),
        Utils.getHelixNodeIdentifier(Utils.getHostName(), testPort),
        "Instance in the allowlist is not the one added before.");
    veniceAdmin.removeInstanceFromAllowList(clusterName, Utils.getHelixNodeIdentifier(Utils.getHostName(), testPort));
    Assert.assertEquals(
        veniceAdmin.getAllowlist(clusterName).size(),
        0,
        "After removing the instance, allowlist should be empty.");
  }

  @Test
  public void testKillOfflinePush() throws Exception {
    PubSubTopic participantStoreRTTopic = pubSubTopicRepository
        .getTopic(Version.composeRealTimeTopic(VeniceSystemStoreUtils.getParticipantStoreNameForCluster(clusterName)));
    String newNodeId = Utils.getHelixNodeIdentifier(Utils.getHostName(), 9786);
    // Ensure original participant store would hang on bootstrap state.
    delayParticipantJobCompletion(true);
    startParticipant(true, newNodeId);
    String storeName = "testKillPush";
    int partitionCount = 2;
    int replicaFactor = 1;
    // Start a new version with 2 partition and 1 replica
    veniceAdmin.createStore(clusterName, storeName, "test", KEY_SCHEMA, VALUE_SCHEMA);
    Version version = veniceAdmin.incrementVersionIdempotent(
        clusterName,
        storeName,
        Version.guidBasedDummyPushId(),
        partitionCount,
        replicaFactor);
    TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS, () -> {
      try {
        PartitionAssignment partitionAssignment = veniceAdmin.getHelixVeniceClusterResources(clusterName)
            .getRoutingDataRepository()
            .getPartitionAssignments(version.kafkaTopicName());
        if (partitionAssignment.getAllPartitions().size() < partitionCount) {
          return false;
        }
        if (partitionAssignment.getPartition(0).getWorkingInstances().size() == 1
            && partitionAssignment.getPartition(1).getWorkingInstances().size() == 1) {
          // Get the STARTED replicas
          // veniceAdmin.getHelixVeniceClusterResources(clusterName).getCustomizedViewRepository().getReplicaStates()
          return true;
        }
        return false;
      } catch (VeniceException e) {
        return false;
      }
    });
    // Now we have two participants blocked on ST from BOOTSTRAP to ONLINE.
    Map<Integer, Long> participantTopicOffsets =
        veniceAdmin.getTopicManager().getTopicLatestOffsets(participantStoreRTTopic);
    veniceAdmin.killOfflinePush(clusterName, version.kafkaTopicName(), false);
    // Verify the kill offline push message have been written to the participant message store RT topic.
    TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS, () -> {
      Map<Integer, Long> newPartitionTopicOffsets =
          veniceAdmin.getTopicManager().getTopicLatestOffsets(participantStoreRTTopic);
      for (Map.Entry<Integer, Long> entry: participantTopicOffsets.entrySet()) {
        if (newPartitionTopicOffsets.get(entry.getKey()) > entry.getValue()) {
          return true;
        }
      }
      return false;
    });

    stopParticipant(newNodeId);
    delayParticipantJobCompletion(false);
  }

  @Test
  public void testDeleteAllVersionsInStore() {
    delayParticipantJobCompletion(true);
    String storeName = Utils.getUniqueString("testDeleteAllVersions");
    // register kill message handler for participants.
    for (SafeHelixManager manager: this.helixManagerByNodeID.values()) {
      HelixStatusMessageChannel channel = new HelixStatusMessageChannel(manager, helixMessageChannelStats);
      channel.registerHandler(KillOfflinePushMessage.class, message -> {
        // make state transition failed to simulate kill consumption task.
        stateModelFactoryByNodeID.forEach(
            (nodeId, stateModelFactory) -> stateModelFactory.makeTransitionCompleted(message.getKafkaTopic(), 0));
      });

      // Store has not been created.
      Assert.assertThrows(
          VeniceNoStoreException.class,
          () -> veniceAdmin.deleteAllVersionsInStore(clusterName, storeName));

      // Prepare 3 version. The first two are completed and the last one is still ongoing.
      int versionCount = 3;
      veniceAdmin.createStore(clusterName, storeName, "testOwner", KEY_SCHEMA, VALUE_SCHEMA);
      veniceAdmin.updateStore(
          clusterName,
          storeName,
          new UpdateStoreQueryParams().setReplicationFactor(DEFAULT_REPLICA_COUNT));
      Version lastVersion = null;
      for (int i = 0; i < versionCount; i++) {
        lastVersion =
            veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
        if (i < versionCount - 1) {
          // Hang the state transition of the last version only. Otherwise, retiring would be triggered.
          for (Map.Entry<String, MockTestStateModelFactory> entry: stateModelFactoryByNodeID.entrySet()) {
            MockTestStateModelFactory value = entry.getValue();
            value.makeTransitionCompleted(lastVersion.kafkaTopicName(), 0);
          }
          int versionNumber = lastVersion.getNumber();
          TestUtils.waitForNonDeterministicCompletion(
              30,
              TimeUnit.SECONDS,
              () -> veniceAdmin.getCurrentVersion(clusterName, storeName) == versionNumber);
        }
      }
      Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions().size(), 2);
      // Store has not been disabled.
      Assert.assertThrows(VeniceException.class, () -> veniceAdmin.deleteAllVersionsInStore(clusterName, storeName));

      veniceAdmin.setStoreReadability(clusterName, storeName, false);
      // Store has not been disabled to write
      Assert.assertThrows(VeniceException.class, () -> veniceAdmin.deleteAllVersionsInStore(clusterName, storeName));

      veniceAdmin.setStoreReadability(clusterName, storeName, true);
      veniceAdmin.setStoreWriteability(clusterName, storeName, false);
      // Store has not been disabled to read
      Assert.assertThrows(VeniceException.class, () -> veniceAdmin.deleteAllVersionsInStore(clusterName, storeName));

      // Store has been disabled.
      veniceAdmin.setStoreReadability(clusterName, storeName, false);
      veniceAdmin.deleteAllVersionsInStore(clusterName, storeName);
      Assert.assertEquals(
          veniceAdmin.getStore(clusterName, storeName).getVersions().size(),
          0,
          " Versions should be deleted.");
      Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getCurrentVersion(), Store.NON_EXISTING_VERSION);
      // After enabling store, the serving version is -1 because there is not version available in this store.
      veniceAdmin.setStoreReadability(clusterName, storeName, true);
      Assert.assertEquals(
          veniceAdmin.getStore(clusterName, storeName).getCurrentVersion(),
          Store.NON_EXISTING_VERSION,
          "No version should be available to read");
      String uncompletedTopic = lastVersion.kafkaTopicName();
      Assert.assertTrue(
          veniceAdmin.isTopicTruncated(uncompletedTopic),
          "Kafka topic: " + uncompletedTopic + " should be truncated for the uncompleted version.");
      String completedTopic = Version.composeKafkaTopic(storeName, lastVersion.getNumber() - 1);
      Assert.assertTrue(
          veniceAdmin.isTopicTruncated(completedTopic),
          "Kafka topic: " + completedTopic + " should be truncated for the completed version.");

      delayParticipantJobCompletion(false);
      for (Map.Entry<String, MockTestStateModelFactory> entry: stateModelFactoryByNodeID.entrySet()) {
        MockTestStateModelFactory value = entry.getValue();
        value.makeTransitionCompleted(lastVersion.kafkaTopicName(), 0);
      }
    }
  }

  @Test
  public void testDeleteAllVersionsInStoreWithoutJobAndResource() {
    String storeName = "testDeleteVersionInWithoutJobAndResource";
    Store store = TestUtils.createTestStore(storeName, storeOwner, System.currentTimeMillis());
    Version version = new VersionImpl(store.getName(), store.getLargestUsedVersionNumber() + 1, "pushJobId");
    store.addVersion(version);
    store.updateVersionStatus(version.getNumber(), VersionStatus.ONLINE);
    store.setCurrentVersion(version.getNumber());
    veniceAdmin.getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository().addStore(store);
    veniceAdmin.setStoreReadability(clusterName, storeName, false);
    veniceAdmin.setStoreWriteability(clusterName, storeName, false);

    veniceAdmin.deleteAllVersionsInStore(clusterName, storeName);
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions().size(), 0);
  }

  @Test
  public void testDeleteOldVersionInStore() {
    String storeName = Utils.getUniqueString("testDeleteOldVersion");
    for (SafeHelixManager manager: this.helixManagerByNodeID.values()) {
      HelixStatusMessageChannel channel = new HelixStatusMessageChannel(manager, helixMessageChannelStats);
      channel.registerHandler(KillOfflinePushMessage.class, message -> {/*ignore*/ });
    }
    veniceAdmin.createStore(clusterName, storeName, "testOwner", KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin
        .updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(DEFAULT_REPLICA_COUNT));
    // Add two versions.
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    TestUtils.waitForNonDeterministicCompletion(
        TOTAL_TIMEOUT_FOR_SHORT_TEST_MS,
        TimeUnit.MILLISECONDS,
        () -> veniceAdmin.getStore(clusterName, storeName).getCurrentVersion() == 2);
    veniceAdmin.setStoreReadability(clusterName, storeName, false);
    veniceAdmin.deleteOldVersionInStore(clusterName, storeName, 1);
    Assert.assertEquals(
        veniceAdmin.getStore(clusterName, storeName).getVersions().size(),
        1,
        " Version 1 should be deleted.");

    // Current version should not be deleted
    Assert.assertThrows(VeniceException.class, () -> veniceAdmin.deleteOldVersionInStore(clusterName, storeName, 2));

    try {
      veniceAdmin.deleteOldVersionInStore(clusterName, storeName, 3);
    } catch (VeniceException e) {
      Assert.fail("Version 3 does not exist, so deletion request should be skipped without throwing any exception.");
    }
  }

  @Test
  public void testRetireOldStoreVersionsKillOfflineFails() {
    String storeName = Utils.getUniqueString("testDeleteOldVersion");
    HelixStatusMessageChannel channel = new HelixStatusMessageChannel(helixManager, helixMessageChannelStats);
    channel.registerHandler(KillOfflinePushMessage.class, message -> {
      if (message.getKafkaTopic().equals(Version.composeKafkaTopic(storeName, 1))) {
        throw new VeniceException("offline job failed!!");
      }
    });

    veniceAdmin.createStore(clusterName, storeName, "testOwner", KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin
        .updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(DEFAULT_REPLICA_COUNT));
    // Add three versions.
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);

    TestUtils.waitForNonDeterministicCompletion(
        TOTAL_TIMEOUT_FOR_LONG_TEST_MS,
        TimeUnit.MILLISECONDS,
        () -> veniceAdmin.getStore(clusterName, storeName).getCurrentVersion() == 3);
    veniceAdmin.retireOldStoreVersions(clusterName, storeName, true, VERSION_ID_UNSET);
    veniceAdmin.setStoreReadability(clusterName, storeName, false);
    veniceAdmin.retireOldStoreVersions(clusterName, storeName, false, VERSION_ID_UNSET);
    Assert.assertEquals(
        veniceAdmin.getStore(clusterName, storeName).getVersions().size(),
        1,
        " Versions should be deleted.");
  }

  @Test
  public void testDeleteStore() {
    String storeName = Utils.getUniqueString("testDeleteStore");
    TestUtils.createTestStore(storeName, storeOwner, System.currentTimeMillis());
    for (SafeHelixManager manager: this.helixManagerByNodeID.values()) {
      HelixStatusMessageChannel channel = new HelixStatusMessageChannel(manager, helixMessageChannelStats);
      channel.registerHandler(
          KillOfflinePushMessage.class,
          message -> stateModelFactoryByNodeID.forEach(
              (nodeId, stateModelFactory) -> stateModelFactory.makeTransitionCompleted(message.getKafkaTopic(), 0)));
    }
    veniceAdmin.createStore(clusterName, storeName, storeOwner, "\"string\"", "\"string\"");
    veniceAdmin
        .updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(DEFAULT_REPLICA_COUNT));
    Version version =
        veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    TestUtils.waitForNonDeterministicCompletion(
        TOTAL_TIMEOUT_FOR_SHORT_TEST_MS,
        TimeUnit.MILLISECONDS,
        () -> veniceAdmin.getCurrentVersion(clusterName, storeName) == version.getNumber());
    PubSubTopic storeVersionTopic =
        pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, version.getNumber()));
    Assert.assertTrue(
        veniceAdmin.getTopicManager().containsTopicAndAllPartitionsAreOnline(storeVersionTopic),
        "Kafka topic should be created.");
    Assert.assertNotNull(metricsRepository.getMetric("." + storeName + "--successful_push_duration_sec_gauge.Gauge"));

    // Store has not been disabled.
    Assert.assertThrows(
        VeniceException.class,
        () -> veniceAdmin.deleteStore(clusterName, storeName, Store.IGNORE_VERSION, true));

    veniceAdmin.setStoreReadability(clusterName, storeName, false);
    veniceAdmin.setStoreWriteability(clusterName, storeName, false);
    veniceAdmin.deleteStore(clusterName, storeName, Store.IGNORE_VERSION, true);
    Assert.assertNull(veniceAdmin.getStore(clusterName, storeName), "Store should be deleted before.");
    Assert.assertEquals(
        veniceAdmin.getStoreGraveyard().getLargestUsedVersionNumber(storeName),
        version.getNumber(),
        "LargestUsedVersionNumber should be kept in graveyard.");
    TestUtils.waitForNonDeterministicCompletion(
        TOTAL_TIMEOUT_FOR_LONG_TEST_MS,
        TimeUnit.MILLISECONDS,
        () -> veniceAdmin.isTopicTruncated(Version.composeKafkaTopic(storeName, version.getNumber())));
    Assert.assertNull(metricsRepository.getMetric("." + storeName + "--successful_push_duration_sec_gauge.Gauge"));
  }

  @Test
  public void testRemoveStoreFromGraveyard() {
    String storeName = Utils.getUniqueString("testRemoveStoreFromGraveyard");
    veniceAdmin.createStore(clusterName, storeName, storeOwner, "\"string\"", "\"string\"");
    Version version =
        veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    PubSubTopic versionTopic =
        pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, version.getNumber()));
    Assert.assertTrue(
        veniceAdmin.getTopicManager().containsTopicAndAllPartitionsAreOnline(versionTopic),
        "Kafka topic should be created.");

    veniceAdmin.setStoreReadability(clusterName, storeName, false);
    veniceAdmin.setStoreWriteability(clusterName, storeName, false);
    veniceAdmin.deleteStore(clusterName, storeName, Store.IGNORE_VERSION, true);
    // Topic has not been deleted. Store graveyard could not be removed.
    Assert.assertThrows(VeniceException.class, () -> veniceAdmin.removeStoreFromGraveyard(clusterName, storeName));

    TestUtils
        .waitForNonDeterministicAssertion(TOTAL_TIMEOUT_FOR_LONG_TEST_MS, TimeUnit.MILLISECONDS, false, true, () -> {
          veniceAdmin.getTopicManager().ensureTopicIsDeletedAndBlockWithRetry(versionTopic);
          veniceAdmin.removeStoreFromGraveyard(clusterName, storeName);
          Assert.assertNull(veniceAdmin.getStoreGraveyard().getStoreFromGraveyard(clusterName, storeName, null));
        });
  }

  @Test
  public void testDeleteStoreWithLargestUsedVersionNumberOverwritten() {
    String storeName = Utils.getUniqueString("testDeleteStore");
    int largestUsedVersionNumber = 1000;

    TestUtils.createTestStore(storeName, storeOwner, System.currentTimeMillis());
    for (SafeHelixManager manager: this.helixManagerByNodeID.values()) {
      HelixStatusMessageChannel channel = new HelixStatusMessageChannel(manager, helixMessageChannelStats);
      channel.registerHandler(
          KillOfflinePushMessage.class,
          message -> stateModelFactoryByNodeID.forEach(
              (nodeId, stateModelFactory) -> stateModelFactory.makeTransitionCompleted(message.getKafkaTopic(), 0)));

      veniceAdmin.createStore(clusterName, storeName, storeOwner, "\"string\"", "\"string\"");
      veniceAdmin.updateStore(
          clusterName,
          storeName,
          new UpdateStoreQueryParams().setReplicationFactor(DEFAULT_REPLICA_COUNT));
      Version version =
          veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
      TestUtils.waitForNonDeterministicCompletion(
          TOTAL_TIMEOUT_FOR_SHORT_TEST_MS,
          TimeUnit.MILLISECONDS,
          () -> veniceAdmin.getCurrentVersion(clusterName, storeName) == version.getNumber());
      PubSubTopic storeVersionTopic =
          pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, version.getNumber()));
      Assert.assertTrue(
          veniceAdmin.getTopicManager().containsTopicAndAllPartitionsAreOnline(storeVersionTopic),
          "Kafka topic should be created.");

      veniceAdmin.setStoreReadability(clusterName, storeName, false);
      veniceAdmin.setStoreWriteability(clusterName, storeName, false);
      veniceAdmin.deleteStore(clusterName, storeName, largestUsedVersionNumber, true);
      Assert.assertNull(veniceAdmin.getStore(clusterName, storeName), "Store should be deleted before.");
      Assert.assertEquals(
          veniceAdmin.getStoreGraveyard().getLargestUsedVersionNumber(storeName),
          largestUsedVersionNumber,
          "LargestUsedVersionNumber should be overwritten and kept in graveyard.");
    }
  }

  @Test
  public void testReCreateStore() {
    String storeName = Utils.getUniqueString("testReCreateStore");
    int largestUsedVersionNumber = 100;
    veniceAdmin.createStore(clusterName, storeName, storeOwner, "\"string\"", "\"string\"");

    Store store = veniceAdmin.getStore(clusterName, storeName);
    store.setLargestUsedVersionNumber(largestUsedVersionNumber);
    store.setEnableReads(false);
    store.setEnableWrites(false);
    veniceAdmin.getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository().updateStore(store);
    veniceAdmin.deleteStore(clusterName, storeName, Store.IGNORE_VERSION, true);

    // Re-create store with incompatible schema
    veniceAdmin.createStore(clusterName, storeName, storeOwner, "\"long\"", "\"long\"");
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    Assert.assertEquals(veniceAdmin.getKeySchema(clusterName, storeName).getSchema().toString(), "\"long\"");
    Assert.assertEquals(veniceAdmin.getValueSchema(clusterName, storeName, 1).getSchema().toString(), "\"long\"");
    Assert.assertEquals(
        veniceAdmin.getStore(clusterName, storeName).getLargestUsedVersionNumber(),
        largestUsedVersionNumber + 1);
  }

  @Test
  public void testReCreateStoreWithLegacyStore() {
    String storeName = Utils.getUniqueString("testReCreateStore");
    int largestUsedVersionNumber = 100;
    veniceAdmin.createStore(clusterName, storeName, storeOwner, "\"string\"", "\"string\"");

    Store store = veniceAdmin.getStore(clusterName, storeName);
    store.setLargestUsedVersionNumber(largestUsedVersionNumber);
    store.setEnableWrites(false);
    store.setEnableReads(false);
    // Legacy store
    ZkStoreConfigAccessor storeConfigAccessor =
        veniceAdmin.getHelixVeniceClusterResources(clusterName).getStoreConfigAccessor();
    StoreConfig storeConfig = storeConfigAccessor.getStoreConfig(storeName);
    storeConfig.setDeleting(true);
    storeConfigAccessor.updateConfig(storeConfig, false);
    veniceAdmin.getHelixVeniceClusterResources(clusterName).getStoreMetadataRepository().updateStore(store);
    // Re-create store with incompatible schema
    veniceAdmin.createStore(clusterName, storeName, storeOwner, "\"long\"", "\"long\"");
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    Assert.assertEquals(veniceAdmin.getKeySchema(clusterName, storeName).getSchema().toString(), "\"long\"");
    Assert.assertEquals(veniceAdmin.getValueSchema(clusterName, storeName, 1).getSchema().toString(), "\"long\"");
    Assert.assertEquals(
        veniceAdmin.getStore(clusterName, storeName).getLargestUsedVersionNumber(),
        largestUsedVersionNumber + 1);
  }

  @Test
  public void testChunkingEnabled() {
    String storeName = Utils.getUniqueString("test_store");
    veniceAdmin.createStore(clusterName, storeName, storeOwner, "\"string\"", "\"string\"");

    Store store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertFalse(store.isChunkingEnabled());

    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setChunkingEnabled(true));
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertTrue(store.isChunkingEnabled());
  }

  // @Test
  // TODO: This test 'possibly' obsolete long term, and definitely not working now. There is no more BOOTSTRAP state in
  // L/F
  // BUT the concept of a partition which is catching up and not ready to serve is valid. We need to refactor the API to
  // work correctly for L/F stores.
  public void testFindAllBootstrappingVersions() throws Exception {
    delayParticipantJobCompletion(true);
    String storeName = Utils.getUniqueString("test_store");
    veniceAdmin.createStore(clusterName, storeName, storeOwner, "\"string\"", "\"string\"");
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    veniceAdmin
        .updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(DEFAULT_REPLICA_COUNT));
    stateModelFactoryByNodeID.forEach(
        (nodeId, stateModelFactory) -> stateModelFactory
            .makeTransitionCompleted(Version.composeKafkaTopic(storeName, 1), 0));
    // Wait version 1 become online.
    TestUtils.waitForNonDeterministicCompletion(
        TOTAL_TIMEOUT_FOR_SHORT_TEST_MS,
        TimeUnit.MILLISECONDS,
        () -> veniceAdmin.getCurrentVersion(clusterName, storeName) == 1);
    // Restart participant
    stopAllParticipants();
    // This will make all participant store versions on bootstrap state.
    startParticipant(true, NODE_ID);
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    Thread.sleep(1000l);

    RoutingDataRepository routingDataRepository =
        veniceAdmin.getHelixVeniceClusterResources(clusterName).getRoutingDataRepository();
    veniceAdmin.findAllBootstrappingVersions(clusterName);
    // After participant restart, the original participant store version will hang on bootstrap state. Instead of
    // checking #
    // of versions having bootstrapping replicas, we directly checking bootstrapping replicas the test store's versions.
    Assert.assertTrue(routingDataRepository.containsKafkaTopic(Version.composeKafkaTopic(storeName, 1)));
    Assert.assertTrue(routingDataRepository.containsKafkaTopic(Version.composeKafkaTopic(storeName, 2)));
    Assert.assertFalse(
        routingDataRepository.getResourceAssignment()
            .getPartitionAssignment(Version.composeKafkaTopic(storeName, 1))
            .isMissingAssignedPartitions());

    // Verify that version 2 isn't the current version yet (as it's still not in COMPLETED state)
    Assert.assertEquals(veniceAdmin.getCurrentVersion(clusterName, storeName), 1);

    // TODO: use veniceAdmin.getBootstrappingVersions api to check bootstrapping (non COMPLETED) replicas and check
    // results.
    // today it doesn't do the right thing.

    delayParticipantJobCompletion(false);
    stateModelFactoryByNodeID.forEach(
        (nodeId, stateModelFactory) -> stateModelFactory
            .makeTransitionCompleted(Version.composeKafkaTopic(storeName, 2), 0));
  }

  @Test
  public void testGetFutureVersions() throws Exception {
    delayParticipantJobCompletion(true);
    String storeName = Utils.getUniqueString("test_store");
    veniceAdmin.createStore(clusterName, storeName, storeOwner, "\"string\"", "\"string\"");
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);

    int futureVersion = veniceAdmin.getFutureVersion(clusterName, storeName);
    Assert.assertEquals(futureVersion, 1, "Expected future version number of 1!!");
    stateModelFactoryByNodeID.forEach(
        (nodeId, stateModelFactory) -> stateModelFactory
            .makeTransitionCompleted(Version.composeKafkaTopic(storeName, 1), 0));
    // Wait version 1 become online.
    // TOTAL_TIMEOUT_FOR_SHORT_TEST
    Long initialPromotionTimestamp =
        veniceAdmin.getStore(clusterName, storeName).getLatestVersionPromoteToCurrentTimestamp();

    TestUtils.waitForNonDeterministicCompletion(
        TOTAL_TIMEOUT_FOR_LONG_TEST_MS,
        TimeUnit.MILLISECONDS,
        () -> veniceAdmin.getCurrentVersion(clusterName, storeName) == 1);

    Long version1PromotionTimestamp =
        veniceAdmin.getStore(clusterName, storeName).getLatestVersionPromoteToCurrentTimestamp();
    Assert.assertNotEquals(initialPromotionTimestamp, version1PromotionTimestamp);

    futureVersion = veniceAdmin.getFutureVersion(clusterName, storeName);
    Assert.assertEquals(futureVersion, Store.NON_EXISTING_VERSION, "Expected future version number of 0!!");
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    futureVersion = veniceAdmin.getFutureVersion(clusterName, storeName);
    Assert.assertEquals(futureVersion, 2, "Expected future version number of 2!!");
    stateModelFactoryByNodeID.forEach(
        (nodeId, stateModelFactory) -> stateModelFactory
            .makeTransitionCompleted(Version.composeKafkaTopic(storeName, 2), 0));
    TestUtils.waitForNonDeterministicCompletion(
        TOTAL_TIMEOUT_FOR_LONG_TEST_MS,
        TimeUnit.MILLISECONDS,
        () -> veniceAdmin.getCurrentVersion(clusterName, storeName) == 2);

    Long version2PromotionTimestamp =
        veniceAdmin.getStore(clusterName, storeName).getLatestVersionPromoteToCurrentTimestamp();
    Assert.assertNotEquals(version1PromotionTimestamp, version2PromotionTimestamp);

    futureVersion = veniceAdmin.getFutureVersion(clusterName, storeName);
    Assert.assertEquals(futureVersion, Store.NON_EXISTING_VERSION, "Expected future version number of 0!!");
    delayParticipantJobCompletion(false);
  }

  @Test
  public void testBatchGetLimit() {
    String storeName = Utils.getUniqueString("test_store");
    veniceAdmin.createStore(clusterName, storeName, storeOwner, "\"string\"", "\"string\"");

    Store store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertEquals(store.getBatchGetLimit(), -1);

    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setBatchGetLimit(100));
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertEquals(store.getBatchGetLimit(), 100);
  }

  @Test
  public void testNumVersionsToPreserve() {
    String storeName = Utils.getUniqueString("test_store");
    veniceAdmin.createStore(clusterName, storeName, storeOwner, "\"string\"", "\"string\"");

    Store store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertEquals(store.getNumVersionsToPreserve(), store.NUM_VERSION_PRESERVE_NOT_SET);
    int numVersionsToPreserve = 100;

    veniceAdmin.updateStore(
        clusterName,
        storeName,
        new UpdateStoreQueryParams().setNumVersionsToPreserve(numVersionsToPreserve));
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertEquals(store.getNumVersionsToPreserve(), numVersionsToPreserve);
  }

  @Test
  public void leakyTopicTruncation() {
    TopicManager topicManager = veniceAdmin.getTopicManager();
    // 5 stores, 10 topics and 2 active versions each.
    final int NUMBER_OF_VERSIONS = 10;
    final int NUMBER_OF_STORES = 5;
    List<Store> stores = new ArrayList<>();
    for (int storeNumber = 1; storeNumber <= NUMBER_OF_STORES; storeNumber++) {
      String storeName = Utils.getUniqueString("store-" + storeNumber);
      Store store = new ZKStore(
          storeName,
          storeOwner,
          System.currentTimeMillis(),
          PersistenceType.ROCKS_DB,
          RoutingStrategy.CONSISTENT_HASH,
          ReadStrategy.ANY_OF_ONLINE,
          OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
          1);

      // Two active versions, selected at random
      List<Integer> activeVersions = new ArrayList<>();
      int firstActiveVersion = (int) Math.ceil(Math.random() * NUMBER_OF_VERSIONS);
      activeVersions.add(firstActiveVersion);
      int secondActiveVersion = 0;
      while (secondActiveVersion == 0 || firstActiveVersion == secondActiveVersion) {
        secondActiveVersion = (int) Math.ceil(Math.random() * NUMBER_OF_VERSIONS);
      }
      activeVersions.add(secondActiveVersion);

      LOGGER.info("Active versions for '{}': {}", storeName, activeVersions);

      // Create ten topics and keep track of the active versions in the Store instance
      for (int versionNumber = 1; versionNumber <= NUMBER_OF_VERSIONS; versionNumber++) {
        Version version = new VersionImpl(storeName, versionNumber, Utils.getUniqueString(storeName));
        PubSubTopic versionTopic = pubSubTopicRepository.getTopic(version.kafkaTopicName());
        topicManager.createTopic(versionTopic, 1, 1, true);
        if (activeVersions.contains(versionNumber)) {
          store.addVersion(version);
        }
      }
      stores.add(store);
    }

    // Sanity check...
    for (Store store: stores) {
      for (int versionNumber = 1; versionNumber <= NUMBER_OF_VERSIONS; versionNumber++) {
        PubSubTopic versionTopic =
            pubSubTopicRepository.getTopic(Version.composeKafkaTopic(store.getName(), versionNumber));
        Assert.assertTrue(
            topicManager.containsTopicAndAllPartitionsAreOnline(versionTopic),
            "Topic '" + versionTopic + "' should exist.");
      }
    }

    Store storeToCleanUp = stores.get(0);
    veniceAdmin.truncateOldTopics(clusterName, storeToCleanUp, false);

    // verify that the storeToCleanUp has its topics cleaned up, and the others don't
    // verify all the topics of 'storeToCleanup' without corresponding active versions have been cleaned up
    for (Store store: stores) {
      for (int versionNumber = 1; versionNumber <= NUMBER_OF_VERSIONS; versionNumber++) {
        String topicName = Version.composeKafkaTopic(store.getName(), versionNumber);
        if (store.equals(storeToCleanUp) && !store.containsVersion(versionNumber)
            && versionNumber <= store.getLargestUsedVersionNumber()) {
          Assert.assertTrue(veniceAdmin.isTopicTruncated(topicName), "Topic '" + topicName + "' should be truncated.");
        } else {
          Assert.assertTrue(
              !veniceAdmin.isTopicTruncated(topicName),
              "Topic '" + topicName + "' should exist when active versions are: "
                  + store.getVersions()
                      .stream()
                      .map(version -> Integer.toString(version.getNumber()))
                      .collect(Collectors.joining(", "))
                  + ", and largest used version: " + store.getLargestUsedVersionNumber() + ".");
        }
      }
    }
  }

  @Test
  public void testSetLargestUsedVersion() {
    String storeName = "testSetLargestUsedVersion";
    veniceAdmin.createStore(clusterName, storeName, storeOwner, KEY_SCHEMA, VALUE_SCHEMA);
    Store store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertEquals(store.getLargestUsedVersionNumber(), 0);

    Version version =
        veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertTrue(version.getNumber() > 0);
    Assert.assertEquals(store.getLargestUsedVersionNumber(), version.getNumber());

    veniceAdmin.setStoreLargestUsedVersion(clusterName, storeName, 0);
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertEquals(store.getLargestUsedVersionNumber(), 0);
  }

  @Test
  public void testWriteComputationEnabled() {
    String storeName = Utils.getUniqueString("test_store");
    veniceAdmin.createStore(clusterName, storeName, storeOwner, "\"string\"", "\"string\"");

    Store store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertFalse(store.isWriteComputationEnabled());

    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setWriteComputationEnabled(true));
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertTrue(store.isWriteComputationEnabled());
  }

  @Test
  public void testComputationEnabled() {
    String storeName = Utils.getUniqueString("test_store");
    veniceAdmin.createStore(clusterName, storeName, storeOwner, KEY_SCHEMA, VALUE_SCHEMA);

    Store store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertFalse(store.isReadComputationEnabled());

    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReadComputationEnabled(true));
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertTrue(store.isReadComputationEnabled());
  }

  @Test
  public void testAddAndRemoveDerivedSchema() {
    String storeName = Utils.getUniqueString("write_compute_store");
    String recordSchemaStr = TestWriteUtils.USER_SCHEMA_STRING_WITH_DEFAULT;
    Schema derivedSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchemaStr(recordSchemaStr);

    veniceAdmin.createStore(clusterName, storeName, storeOwner, KEY_SCHEMA, recordSchemaStr);
    veniceAdmin.addDerivedSchema(clusterName, storeName, 1, derivedSchema.toString());
    Assert.assertEquals(veniceAdmin.getDerivedSchemas(clusterName, storeName).size(), 1);

    veniceAdmin.removeDerivedSchema(clusterName, storeName, 1, 1);
    Assert.assertEquals(veniceAdmin.getDerivedSchemas(clusterName, storeName).size(), 0);
  }

  @Test
  public void testStoreLevelConfigUpdateShouldNotModifyExistingVersionLevelConfig() {
    String storeName = Utils.getUniqueString("test_store");
    veniceAdmin.createStore(clusterName, storeName, storeOwner, "\"string\"", "\"string\"");

    /**
     * Create a version with default version level setting:
     * chunkingEnabled = false
     * leaderFollowerModelEnabled = false
     * compressionStrategy = CompressionStrategy.NO_OP
     */
    Version existingVersion =
        veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);

    Store store = veniceAdmin.getStore(clusterName, storeName);
    // Check all default setting in store level config
    Assert.assertFalse(store.isChunkingEnabled());
    Assert.assertEquals(store.getCompressionStrategy(), CompressionStrategy.NO_OP);
    // Check all setting in the existing version
    Assert.assertFalse(store.getVersion(existingVersion.getNumber()).get().isChunkingEnabled());
    Assert.assertEquals(
        store.getVersion(existingVersion.getNumber()).get().getCompressionStrategy(),
        CompressionStrategy.NO_OP);

    /**
     * Enable chunking for the store; it should only modify the store level config; the existing version metadata
     * should remain the same!
     */
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setChunkingEnabled(true));
    store = veniceAdmin.getStore(clusterName, storeName);
    // Store level config should be updated
    Assert.assertTrue(store.isChunkingEnabled());
    // Existing version config should not be updated!
    Assert.assertFalse(store.getVersion(existingVersion.getNumber()).get().isChunkingEnabled());

    /**
     * Enable compression.
     */
    veniceAdmin.updateStore(
        clusterName,
        storeName,
        new UpdateStoreQueryParams().setCompressionStrategy(CompressionStrategy.GZIP));
    store = veniceAdmin.getStore(clusterName, storeName);
    // Store level config should be updated
    Assert.assertEquals(store.getCompressionStrategy(), CompressionStrategy.GZIP);
    // Existing version config should not be updated!
    Assert.assertEquals(
        store.getVersion(existingVersion.getNumber()).get().getCompressionStrategy(),
        CompressionStrategy.NO_OP);
  }

  @Test
  public void testAddVersionWithRemoteKafkaBootstrapServers() {
    TopicManagerRepository originalTopicManagerRepository = veniceAdmin.getTopicManagerRepository();

    TopicManager mockedTopicManager = mock(TopicManager.class);
    TopicManagerRepository mockedTopicManageRepository = mock(TopicManagerRepository.class);
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager();
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager(any(String.class));
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager(anyString());
    veniceAdmin.setTopicManagerRepository(mockedTopicManageRepository);
    String storeName = Utils.getUniqueString("test-store");
    String pushJobId1 = "test-push-job-id-1";
    veniceAdmin.createStore(clusterName, storeName, "test-owner", KEY_SCHEMA, VALUE_SCHEMA);
    /**
     * Enable native replication.
     */
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setNativeReplicationEnabled(true));

    /**
     * Add version 1 without remote Kafka bootstrap servers.
     */
    veniceAdmin.addVersionAndTopicOnly(
        clusterName,
        storeName,
        pushJobId1,
        VERSION_ID_UNSET,
        1,
        1,
        false,
        true,
        Version.PushType.BATCH,
        null,
        null,
        Optional.empty(),
        -1,
        1,
        Optional.empty(),
        false);
    // Version 1 should exist.
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions().size(), 1);

    /**
     * Add version 2 with remote kafka bootstrap servers.
     */
    String remoteKafkaBootstrapServers = "localhost:9092";
    String pushJobId2 = "test-push-job-id-2";
    veniceAdmin.addVersionAndTopicOnly(
        clusterName,
        storeName,
        pushJobId2,
        VERSION_ID_UNSET,
        2,
        1,
        false,
        true,
        Version.PushType.BATCH,
        null,
        remoteKafkaBootstrapServers,
        Optional.empty(),
        -1,
        1,
        Optional.empty(),
        false);
    // Version 2 should exist and remote Kafka bootstrap servers info should exist in version 2.
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions().size(), 2);
    Assert.assertEquals(
        veniceAdmin.getStore(clusterName, storeName).getVersion(2).get().getPushStreamSourceAddress(),
        remoteKafkaBootstrapServers);

    // Set topic original topic manager back
    veniceAdmin.setTopicManagerRepository(originalTopicManagerRepository);
  }

  @Test
  public void testNativeReplicationSourceFabric() {
    String storeName = Utils.getUniqueString("test_store_nr");
    veniceAdmin.createStore(clusterName, storeName, storeOwner, KEY_SCHEMA, VALUE_SCHEMA);

    Store store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertTrue(store.getNativeReplicationSourceFabric().isEmpty());

    veniceAdmin
        .updateStore(clusterName, storeName, new UpdateStoreQueryParams().setNativeReplicationSourceFabric("dc1"));
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertEquals(store.getNativeReplicationSourceFabric(), "dc1");
  }

  @Test(description = "VT truncation should not affect inc push; however, RT truncation should fail inc-push")
  public void testGetIncrementalPushVersion() {
    String incrementalAndHybridEnabledStoreName = Utils.getUniqueString("testHybridStore");
    veniceAdmin.createStore(clusterName, incrementalAndHybridEnabledStoreName, storeOwner, "\"string\"", "\"string\"");
    veniceAdmin.updateStore(
        clusterName,
        incrementalAndHybridEnabledStoreName,
        new UpdateStoreQueryParams().setHybridOffsetLagThreshold(1)
            .setHybridRewindSeconds(0)
            .setIncrementalPushEnabled(true));
    veniceAdmin.incrementVersionIdempotent(
        clusterName,
        incrementalAndHybridEnabledStoreName,
        Version.guidBasedDummyPushId(),
        1,
        1);
    String rtTopic = veniceAdmin.getRealTimeTopic(clusterName, incrementalAndHybridEnabledStoreName);
    TestUtils.waitForNonDeterministicCompletion(
        TOTAL_TIMEOUT_FOR_SHORT_TEST_MS,
        TimeUnit.MILLISECONDS,
        () -> veniceAdmin.getCurrentVersion(clusterName, incrementalAndHybridEnabledStoreName) == 1);

    // For incremental push policy INCREMENTAL_PUSH_SAME_AS_REAL_TIME, incremental push should succeed even if version
    // topic is truncated
    veniceAdmin.truncateKafkaTopic(Version.composeKafkaTopic(incrementalAndHybridEnabledStoreName, 1));
    veniceAdmin.getIncrementalPushVersion(clusterName, incrementalAndHybridEnabledStoreName);

    // For incremental push policy INCREMENTAL_PUSH_SAME_AS_REAL_TIME, incremental push should fail if rt topic is
    // truncated
    veniceAdmin.truncateKafkaTopic(rtTopic);
    Assert.assertThrows(
        VeniceException.class,
        () -> veniceAdmin.getIncrementalPushVersion(clusterName, incrementalAndHybridEnabledStoreName));
  }

  @Test
  public void testEarlyDeleteBackup() {
    String testDeleteStore = Utils.getUniqueString("testDeleteStore");
    veniceAdmin.createStore(clusterName, testDeleteStore, storeOwner, "\"string\"", "\"string\"");
    veniceAdmin.updateStore(clusterName, testDeleteStore, new UpdateStoreQueryParams().setIncrementalPushEnabled(true));
    veniceAdmin.incrementVersionIdempotent(clusterName, testDeleteStore, Version.guidBasedDummyPushId(), 1, 1);
    TestUtils.waitForNonDeterministicCompletion(
        TOTAL_TIMEOUT_FOR_SHORT_TEST_MS,
        TimeUnit.MILLISECONDS,
        () -> veniceAdmin.getCurrentVersion(clusterName, testDeleteStore) == 1);

    veniceAdmin.incrementVersionIdempotent(clusterName, testDeleteStore, Version.guidBasedDummyPushId(), 1, 1);

    TestUtils.waitForNonDeterministicCompletion(
        TOTAL_TIMEOUT_FOR_SHORT_TEST_MS,
        TimeUnit.MILLISECONDS,
        () -> veniceAdmin.getCurrentVersion(clusterName, testDeleteStore) == 2);

    Store store = veniceAdmin.getStore(clusterName, testDeleteStore);

    Assert.assertEquals(store.getVersions().size(), 2);

    veniceAdmin.incrementVersionIdempotent(clusterName, testDeleteStore, Version.guidBasedDummyPushId(), 1, 1);

    TestUtils.waitForNonDeterministicCompletion(
        TOTAL_TIMEOUT_FOR_SHORT_TEST_MS,
        TimeUnit.MILLISECONDS,
        () -> veniceAdmin.getCurrentVersion(clusterName, testDeleteStore) == 3);
    store = veniceAdmin.getStore(clusterName, testDeleteStore);
    Assert.assertEquals(store.getVersions().size(), 2);
  }

  @Test
  public void testVersionLevelActiveActiveReplicationConfig() {
    TopicManagerRepository originalTopicManagerRepository = veniceAdmin.getTopicManagerRepository();

    TopicManager mockedTopicManager = mock(TopicManager.class);
    TopicManagerRepository mockedTopicManageRepository = mock(TopicManagerRepository.class);
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager();
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager(any(String.class));
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager(anyString());
    veniceAdmin.setTopicManagerRepository(mockedTopicManageRepository);
    String storeName = Utils.getUniqueString("test-store");
    String pushJobId1 = "test-push-job-id-1";
    veniceAdmin.createStore(clusterName, storeName, "test-owner", KEY_SCHEMA, VALUE_SCHEMA);
    /**
     * Enable L/F and Active/Active replication
     */
    veniceAdmin
        .updateStore(clusterName, storeName, new UpdateStoreQueryParams().setActiveActiveReplicationEnabled(true));

    /**
     * Add version 1
     */
    veniceAdmin.addVersionAndTopicOnly(
        clusterName,
        storeName,
        pushJobId1,
        VERSION_ID_UNSET,
        1,
        1,
        false,
        true,
        Version.PushType.BATCH,
        null,
        null,
        Optional.empty(),
        -1,
        1,
        Optional.empty(),
        false);
    // Version 1 should exist.
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions().size(), 1);
    // A/A version level config should be true
    Assert.assertTrue(
        veniceAdmin.getStore(clusterName, storeName).getVersion(1).get().isActiveActiveReplicationEnabled());

    // Set topic original topic manager back
    veniceAdmin.setTopicManagerRepository(originalTopicManagerRepository);
  }

  @Test
  public void testAddMetadataSchema() {
    String storeName = Utils.getUniqueString("aa_store");
    String recordSchemaStr = TestWriteUtils.USER_SCHEMA_STRING_WITH_DEFAULT;
    int replicationMetadataVersionId = multiClusterConfig.getCommonConfig().getReplicationMetadataVersion();
    Schema metadataSchema = RmdSchemaGenerator.generateMetadataSchema(recordSchemaStr, replicationMetadataVersionId);

    veniceAdmin.createStore(clusterName, storeName, storeOwner, KEY_SCHEMA, recordSchemaStr);
    veniceAdmin.addReplicationMetadataSchema(
        clusterName,
        storeName,
        1,
        replicationMetadataVersionId,
        metadataSchema.toString());
    Collection<RmdSchemaEntry> metadataSchemas = veniceAdmin.getReplicationMetadataSchemas(clusterName, storeName);
    Assert.assertEquals(metadataSchemas.size(), 1);
    Assert.assertEquals(metadataSchemas.iterator().next().getSchema(), metadataSchema);
  }

  @Test
  public void testRepairStoreReplicationFactor() {
    String storeName = Utils.getUniqueString("test");
    veniceAdmin.createStore(clusterName, storeName, storeOwner, "\"string\"", "\"string\"");
    Store store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertTrue(store.getReplicationFactor() > 0, "The replication factor for a new store should be positive.");
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(0));
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertEquals(store.getReplicationFactor(), 0, "The replication factor should be 0 after the update.");
    veniceAdmin.getHelixVeniceClusterResources(clusterName).refresh();
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertTrue(
        store.getReplicationFactor() > 0,
        "The replication factor should be positive after the one-time repair.");
  }

  @Test
  public void testUpdateClusterConfig() {
    String region0 = "region0";
    String region1 = "region1";

    int region0Quota = 10000;
    int region1Quota = 20000;
    UpdateClusterConfigQueryParams updateClusterConfigQueryParams =
        new UpdateClusterConfigQueryParams().setServerKafkaFetchQuotaRecordsPerSecondForRegion(region0, region0Quota)
            .setServerKafkaFetchQuotaRecordsPerSecondForRegion(region1, region1Quota);
    veniceAdmin.updateClusterConfig(clusterName, updateClusterConfigQueryParams);

    ReadOnlyLiveClusterConfigRepository liveClusterConfigRepository = new HelixReadOnlyLiveClusterConfigRepository(
        veniceAdmin.getZkClient(),
        veniceAdmin.getAdapterSerializer(),
        clusterName);
    liveClusterConfigRepository.refresh();

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      LiveClusterConfig clusterConfig = liveClusterConfigRepository.getConfigs();
      Assert.assertEquals(clusterConfig.getServerKafkaFetchQuotaRecordsPerSecondForRegion(region0), region0Quota);
      Assert.assertEquals(clusterConfig.getServerKafkaFetchQuotaRecordsPerSecondForRegion(region1), region1Quota);
    });
  }

  @Test
  public void testHybridStoreToBatchOnly() {
    String storeName = Utils.getUniqueString("test_hybrid_to_batch");
    veniceAdmin.createStore(clusterName, storeName, storeOwner, "\"string\"", "\"string\"");
    veniceAdmin.updateStore(
        clusterName,
        storeName,
        new UpdateStoreQueryParams().setHybridOffsetLagThreshold(1).setHybridRewindSeconds(1));
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    TestUtils.waitForNonDeterministicCompletion(
        TOTAL_TIMEOUT_FOR_SHORT_TEST_MS,
        TimeUnit.MILLISECONDS,
        () -> veniceAdmin.getCurrentVersion(clusterName, storeName) == 1);
    String rtTopic = veniceAdmin.getRealTimeTopic(clusterName, storeName);
    Assert.assertFalse(veniceAdmin.isTopicTruncated(rtTopic));
    veniceAdmin.updateStore(
        clusterName,
        storeName,
        new UpdateStoreQueryParams().setHybridOffsetLagThreshold(-1)
            .setHybridRewindSeconds(-1)
            .setHybridTimeLagThreshold(-1));
    Assert.assertFalse(veniceAdmin.isTopicTruncated(rtTopic));
    // Perform two new pushes and the RT should be deleted upon the completion of the new pushes.
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    TestUtils.waitForNonDeterministicCompletion(
        TOTAL_TIMEOUT_FOR_SHORT_TEST_MS,
        TimeUnit.MILLISECONDS,
        () -> veniceAdmin.getCurrentVersion(clusterName, storeName) == 2);
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    TestUtils.waitForNonDeterministicCompletion(
        TOTAL_TIMEOUT_FOR_SHORT_TEST_MS,
        TimeUnit.MILLISECONDS,
        () -> veniceAdmin.getCurrentVersion(clusterName, storeName) == 3);
    Assert.assertTrue(veniceAdmin.isTopicTruncated(rtTopic));
  }

  @Test
  public void testUpdateStoreWithVersionInheritedConfigs() {
    // This test is meant to test those configurations applied to a store that get applied to specific versions as they
    // are created.
    String storeName = Utils.getUniqueString("test_param_inheritance");
    veniceAdmin.createStore(clusterName, storeName, storeOwner, "\"string\"", "\"string\"");
    Map<String, String> viewConfig = new HashMap<>();
    viewConfig.put(
        "changeCapture",
        "{\"viewClassName\" : \"" + ChangeCaptureView.class.getCanonicalName() + "\", \"viewParameters\" : {}}");
    veniceAdmin.updateStore(
        clusterName,
        storeName,
        new UpdateStoreQueryParams().setHybridOffsetLagThreshold(1)
            .setHybridRewindSeconds(1)
            .setStoreViews(viewConfig));
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    TestUtils.waitForNonDeterministicCompletion(
        TOTAL_TIMEOUT_FOR_SHORT_TEST_MS,
        TimeUnit.MILLISECONDS,
        () -> veniceAdmin.getCurrentVersion(clusterName, storeName) == 1);
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setStoreViews(new HashMap<>()));
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    TestUtils.waitForNonDeterministicCompletion(
        TOTAL_TIMEOUT_FOR_SHORT_TEST_MS,
        TimeUnit.MILLISECONDS,
        () -> veniceAdmin.getCurrentVersion(clusterName, storeName) == 2);
    Store store = veniceAdmin.getStore(clusterName, storeName);
    // Verify that version 1 has the config
    Assert.assertTrue(store.getVersion(1).get().getViewConfigs().containsKey("changeCapture"));
    // Verify that version 2 does NOT have the config
    Assert.assertFalse(store.getVersion(2).get().getViewConfigs().containsKey("changeCapture"));

    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setStoreViews(viewConfig));
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertTrue(store.getVersion(3).get().getViewConfigs().containsKey("changeCapture"));

  }
}
