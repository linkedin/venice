package com.linkedin.venice.controller;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.exception.HelixClusterMaintenanceModeException;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.helix.HelixStatusMessageChannel;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.TopicManagerRepository;
import com.linkedin.venice.kafka.VeniceOperationAgainstKafkaTimedOut;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.IncrementalPushPolicy;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.KillOfflinePushMessage;
import com.linkedin.venice.pushmonitor.PushMonitor;
import com.linkedin.venice.schema.MetadataSchemaAdapter;
import com.linkedin.venice.schema.MetadataSchemaEntry;
import com.linkedin.venice.schema.WriteComputeSchemaAdapter;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.MockTestStateModelFactory;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

/**
 * Helix Admin test cases which share the same Venice cluster. Please make sure to have the proper
 * clean-up and set cluster back to its default settings after finishing the tests.
 *implements Closeable
 * If it's hard to set cluster back, please move the tests to {@link TestVeniceHelixAdminWithIsolatedEnvironment}
 */
public class TestVeniceHelixAdminWithSharedEnvironment extends AbstractTestVeniceHelixAdmin {

  @BeforeClass(alwaysRun = true)
  public void setup() throws Exception {
    setupCluster();
    participantMessageStoreSetup();
  }

  @AfterClass(alwaysRun = true)
  public void cleanup() {
    cleanupCluster();
  }

  @Test(timeOut = TOTAL_TIMEOUT_FOR_SHORT_TEST)
  public void testStartClusterAndCreatePush() {
    try {
      String storeName = TestUtils.getUniqueString("test-store");
      veniceAdmin.addStore(clusterName, storeName, "dev", KEY_SCHEMA, VALUE_SCHEMA);
      String topicName = Version.composeKafkaTopic(storeName, 1);
      Assert.assertEquals(veniceAdmin.getOffLinePushStatus(clusterName, topicName).getExecutionStatus(),
          ExecutionStatus.NOT_CREATED, "Offline job status should not already exist.");
      veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
      Assert.assertNotEquals(veniceAdmin.getOffLinePushStatus(clusterName, topicName).getExecutionStatus(),
          ExecutionStatus.NOT_CREATED, "Can not get offline job status correctly.");
    } catch (VeniceException e) {
      Assert.fail("Should be able to create store after starting cluster");
    }
  }

  @Test(timeOut = TOTAL_TIMEOUT_FOR_LONG_TEST)
  public void testIsMasterController() {
    Assert.assertTrue(veniceAdmin.isMasterController(clusterName),
        "The default controller should be the master controller.");

    int newAdminPort = config.getAdminPort() + 1; /* Note: dummy port */
    PropertyBuilder builder = new PropertyBuilder().put(controllerProps.toProperties()).put("admin.port", newAdminPort);

    VeniceProperties newControllerProps = builder.build();
    VeniceControllerConfig newConfig = new VeniceControllerConfig(newControllerProps);
    VeniceHelixAdmin newMasterAdmin = new VeniceHelixAdmin(TestUtils.getMultiClusterConfigFromOneCluster(newConfig), new MetricsRepository());
    //Start stand by controller
    newMasterAdmin.start(clusterName);
    Assert.assertFalse(veniceAdmin.isMasterController(clusterName) && newMasterAdmin.isMasterController(clusterName),
        "At most one controller can be the master.");
    veniceAdmin.stop(clusterName);
    // Waiting state transition from standby->leader on new admin
    waitUntilIsMaster(newMasterAdmin, clusterName, MASTER_CHANGE_TIMEOUT);
    Assert.assertTrue(newMasterAdmin.isMasterController(clusterName),
        "The new controller should be the master controller right now.");
    veniceAdmin.start(clusterName);
    waitForAMaster(Arrays.asList(veniceAdmin, newMasterAdmin), clusterName, MASTER_CHANGE_TIMEOUT);

    /* XOR */
    Assert.assertTrue(veniceAdmin.isMasterController(clusterName) || newMasterAdmin.isMasterController(clusterName));
    Assert.assertFalse(veniceAdmin.isMasterController(clusterName) && newMasterAdmin.isMasterController(clusterName));

    //resume to the original venice admin
    veniceAdmin.start(clusterName);
    newMasterAdmin.close();
    waitUntilIsMaster(veniceAdmin, clusterName, MASTER_CHANGE_TIMEOUT);
  }

  @Test(timeOut = TOTAL_TIMEOUT_FOR_SHORT_TEST)
  public void testMultiCluster() {
    String newClusterName = "new_test_cluster";
    PropertyBuilder builder =
        new PropertyBuilder().put(controllerProps.toProperties()).put("cluster.name", newClusterName);

    VeniceProperties newClusterProps = builder.build();
    VeniceControllerConfig newClusterConfig = new VeniceControllerConfig(newClusterProps);
    veniceAdmin.addConfig(newClusterConfig);
    veniceAdmin.start(newClusterName);
    waitUntilIsMaster(veniceAdmin, newClusterName, MASTER_CHANGE_TIMEOUT);

    Assert.assertTrue(veniceAdmin.isMasterController(clusterName));
    Assert.assertTrue(veniceAdmin.isMasterController(newClusterName));
  }

  @Test(timeOut = TOTAL_TIMEOUT_FOR_SHORT_TEST)
  public void testGetNumberOfPartition() {
    long partitionSize = config.getPartitionSize();
    int maxPartitionNumber = config.getMaxNumberOfPartition();
    int minPartitionNumber = config.getNumberOfPartition();
    String storeName = TestUtils.getUniqueString("test");

    veniceAdmin.addStore(clusterName, storeName, "dev", KEY_SCHEMA, VALUE_SCHEMA);

    long storeSize = partitionSize * (minPartitionNumber + 1);
    int numberOfPartition = veniceAdmin.calculateNumberOfPartitions(clusterName, storeName, storeSize);
    Assert.assertEquals(numberOfPartition, storeSize / partitionSize,
        "Number partition is smaller than max and bigger than min. So use the calculated result.");
    storeSize = 1;
    numberOfPartition = veniceAdmin.calculateNumberOfPartitions(clusterName, storeName, storeSize);
    Assert.assertEquals(numberOfPartition, minPartitionNumber,
        "Store size is too small so should use min number of partitions.");
    storeSize = partitionSize * (maxPartitionNumber + 1);
    numberOfPartition = veniceAdmin.calculateNumberOfPartitions(clusterName, storeName, storeSize);
    Assert.assertEquals(numberOfPartition, maxPartitionNumber,
        "Store size is too big, should use max number of partitions.");

    storeSize = Long.MAX_VALUE;
    numberOfPartition = veniceAdmin.calculateNumberOfPartitions(clusterName, storeName, storeSize);
    Assert.assertEquals(numberOfPartition, maxPartitionNumber, "Partition is overflow from Integer, use max one.");

    //invalid store; should fail.
    Assert.assertThrows(VeniceException.class,
        () -> veniceAdmin.calculateNumberOfPartitions(clusterName, storeName, -1));
  }

  @Test(timeOut = TOTAL_TIMEOUT_FOR_SHORT_TEST)
  public void testGetNumberOfPartitionsFromPreviousVersion() {
    long partitionSize = config.getPartitionSize();
    int maxPartitionNumber = config.getMaxNumberOfPartition();
    int minPartitionNumber = config.getNumberOfPartition();
    String storeName = TestUtils.getUniqueString("test");

    veniceAdmin.addStore(clusterName, storeName, "dev", KEY_SCHEMA, VALUE_SCHEMA);
    long storeSize = partitionSize * (minPartitionNumber) + 1;
    int numberOfParition = veniceAdmin.calculateNumberOfPartitions(clusterName, storeName, storeSize);
    Store store = veniceAdmin.getVeniceHelixResource(clusterName).getMetadataRepository().getStore(storeName);
    store.setPartitionCount(numberOfParition);
    veniceAdmin.getVeniceHelixResource(clusterName).getMetadataRepository().updateStore(store);
    Version v = veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(),
        numberOfParition, 1);
    veniceAdmin.setStoreCurrentVersion(clusterName, storeName, v.getNumber());
    veniceAdmin.setStoreCurrentVersion(clusterName, storeName, v.getNumber());
    storeSize = partitionSize * (maxPartitionNumber - 2);
    numberOfParition = veniceAdmin.calculateNumberOfPartitions(clusterName, storeName, storeSize);
    Assert.assertEquals(numberOfParition, minPartitionNumber,
        "Should use the number of partition from previous version");
  }

  @Test
  public void testHandleVersionCreationFailure() {
    String storeName = TestUtils.getUniqueString("test");
    veniceAdmin.addStore(clusterName, storeName, "owner", KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(DEFAULT_REPLICA_COUNT));
    // Register the handle for kill message. Otherwise, when job manager collect the old version, it would meet error
    // after sending kill job message. Because, participant can not handle message correctly.
    HelixStatusMessageChannel channel = new HelixStatusMessageChannel(participants.get(NODE_ID), helixMessageChannelStats);
    channel.registerHandler(KillOfflinePushMessage.class, message -> {/*ignore*/});

    delayParticipantJobCompletion(true);

    Version version =
        veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(),1, 1);
    int versionNumber = version.getNumber();

    Admin.OfflinePushStatusInfo offlinePushStatus = veniceAdmin.getOffLinePushStatus(clusterName, Version.composeKafkaTopic(storeName, versionNumber));
    Assert.assertEquals(offlinePushStatus.getExecutionStatus(), ExecutionStatus.STARTED);

    String statusDetails = "synthetic error message";
    veniceAdmin.handleVersionCreationFailure(clusterName, storeName, versionNumber, statusDetails);
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions().size(), 0);
    offlinePushStatus = veniceAdmin.getOffLinePushStatus(clusterName, Version.composeKafkaTopic(storeName, versionNumber));
    Assert.assertEquals(offlinePushStatus.getExecutionStatus(), ExecutionStatus.ERROR);
    Assert.assertTrue(offlinePushStatus.getStatusDetails().isPresent());
    Assert.assertEquals(offlinePushStatus.getStatusDetails().get(), statusDetails);

    delayParticipantJobCompletion(false);
    stateModelFactories.forEach((nodeId, stateModelFactory) ->
        stateModelFactory.makeTransitionCompleted(version.kafkaTopicName(), 0));
  }

  @Test
  public void testDeleteOldVersions() {
    String storeName = TestUtils.getUniqueString("test");
    veniceAdmin.addStore(clusterName, storeName, "owner", KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(DEFAULT_REPLICA_COUNT));
    // Register the handle for kill message. Otherwise, when job manager collect the old version, it would meet error
    // after sending kill job message. Because, participant can not handle message correctly.
    HelixStatusMessageChannel channel = new HelixStatusMessageChannel(participants.get(NODE_ID), helixMessageChannelStats);
    channel.registerHandler(KillOfflinePushMessage.class, message -> {/*ignore*/});
    Version version = null;
    for (int i = 0; i < 3; i++) {
      version =
          veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
      int versionNumber = version.getNumber();

      TestUtils.waitForNonDeterministicCompletion(30000, TimeUnit.MILLISECONDS,
          () -> veniceAdmin.getCurrentVersion(clusterName, storeName) == versionNumber);
    }

    // Store-level write lock is shared between VeniceHelixAdmin and AbstractPushMonitor. It's not guaranteed that the
    // new version is online and old version will be deleted during VeniceHelixAdmin#addVersion early backup deletion.
    // Explicitly run early backup deletion again to make the test deterministic.
    veniceAdmin.retireOldStoreVersions(clusterName, storeName, true);
    TestUtils.waitForNonDeterministicCompletion(30000, TimeUnit.MILLISECONDS, () -> veniceAdmin.versionsForStore(clusterName, storeName).size() == 1);
    Assert.assertEquals(veniceAdmin.getCurrentVersion(clusterName, storeName), version.getNumber());
    Assert.assertEquals(veniceAdmin.versionsForStore(clusterName, storeName).get(0).getNumber(), version.getNumber());

    Version deletedVersion = new VersionImpl(storeName, version.getNumber() - 2);
    // Ensure job and topic are deleted
    TestUtils.waitForNonDeterministicCompletion(30000, TimeUnit.MILLISECONDS,
        () -> veniceAdmin.getOffLinePushStatus(clusterName, deletedVersion.kafkaTopicName()).getExecutionStatus()
            .equals(ExecutionStatus.NOT_CREATED));
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Assert.assertTrue(veniceAdmin.isTopicTruncated(deletedVersion.kafkaTopicName()));
    });
  }

  @Test
  public void testDeleteResourceThenRestartParticipant() throws Exception {
    delayParticipantJobCompletion(true);
    String storeName = "testDeleteResource";
    veniceAdmin.addStore(clusterName, storeName, "owner", KEY_SCHEMA, VALUE_SCHEMA);
    Version version =
        veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    // Ensure the the replica has became BOOTSTRAP
    TestUtils.waitForNonDeterministicCompletion(3000, TimeUnit.MILLISECONDS, () -> {
      RoutingDataRepository routingDataRepository =
          veniceAdmin.getVeniceHelixResource(clusterName).getRoutingDataRepository();
      return routingDataRepository.containsKafkaTopic(version.kafkaTopicName()) &&
          routingDataRepository.getPartitionAssignments(version.kafkaTopicName())
              .getPartition(0)
              .getBootstrapInstances()
              .size() == 1;
    });
    // disconnect the participant
    stopParticipant(NODE_ID);
    // ensure it has disappeared from external view.
    TestUtils.waitForNonDeterministicCompletion(3000, TimeUnit.MILLISECONDS, () -> {
      RoutingDataRepository routingDataRepository =
          veniceAdmin.getVeniceHelixResource(clusterName).getRoutingDataRepository();
      return routingDataRepository.getPartitionAssignments(version.kafkaTopicName()).getAssignedNumberOfPartitions()
          == 0;
    });
    veniceAdmin.deleteHelixResource(clusterName, version.kafkaTopicName());
    // Ensure idealstate is null which means resource has been deleted.
    TestUtils.waitForNonDeterministicCompletion(3000, TimeUnit.MILLISECONDS, () -> {
      PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
      IdealState idealState = veniceAdmin.getVeniceHelixResource(clusterName)
          .getController()
          .getHelixDataAccessor()
          .getProperty(keyBuilder.idealStates(version.kafkaTopicName()));
      return idealState == null;
    });
    // Start participant again
    startParticipant(true, NODE_ID);
    // Ensure resource has been deleted in external view.
    TestUtils.waitForNonDeterministicCompletion(3000, TimeUnit.MILLISECONDS, () -> {
      RoutingDataRepository routingDataRepository =
          veniceAdmin.getVeniceHelixResource(clusterName).getRoutingDataRepository();
      return !routingDataRepository.containsKafkaTopic(version.kafkaTopicName());
    });

    stateModelFactories.forEach((nodeId, stateModelFactory)
        -> Assert.assertEquals(stateModelFactory.getModelList(version.kafkaTopicName(), 0).size(), 1)
    );
    // Replica become OFFLINE state
    stateModelFactories.forEach((nodeId, stateModelFactory)
        -> Assert.assertEquals(stateModelFactory.getModelList(version.kafkaTopicName(), 0).get(0).getCurrentState(), "OFFLINE")
    );
    delayParticipantJobCompletion(false);
  }

  @Test
  public void testUpdateStoreMetadata() throws Exception {
    String storeName = TestUtils.getUniqueString("test");
    String owner = TestUtils.getUniqueString("owner");
    int partitionCount = 1;

    //test setting new version

    // The existing participant uses a non-blocking state model which will switch to COMPLETE immediately.  We add
    // an additional participant here that uses a blocking state model so it doesn't switch to complete.  This way
    // the replicas will not all be COMPLETE, and the new version will not immediately be activated.
    String additionalNode = "localhost_6868";
    startParticipant(true, additionalNode);
    veniceAdmin.addStore(clusterName, storeName, owner, KEY_SCHEMA, VALUE_SCHEMA);
    Version version = veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(),
        partitionCount, 2); // 2 replicas puts a replica on the blocking participant
    Assert.assertEquals(veniceAdmin.getCurrentVersion(clusterName, storeName), 0);
    veniceAdmin.setStoreCurrentVersion(clusterName, storeName, version.getNumber());
    Assert.assertEquals(veniceAdmin.getCurrentVersion(clusterName, storeName), version.getNumber());

    //Version 100 does not exist. Should be failed
    Assert.assertThrows(VeniceException.class,
        () -> veniceAdmin.setStoreCurrentVersion(clusterName, storeName, 100));

    //test setting owner
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getOwner(), owner);
    String newOwner = TestUtils.getUniqueString("owner");

    veniceAdmin.setStoreOwner(clusterName, storeName, newOwner);
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getOwner(), newOwner);

    //test setting partition count
    int newPartitionCount = 2;
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersion(version.getNumber()).get().getPartitionCount(), partitionCount);

    veniceAdmin.setStorePartitionCount(clusterName, storeName, newPartitionCount);
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getPartitionCount(), newPartitionCount);
    Assert.assertThrows(VeniceHttpException.class,
        () -> veniceAdmin.setStorePartitionCount(clusterName, storeName, MAX_NUMBER_OF_PARTITION + 1));
    Assert.assertThrows(VeniceHttpException.class,
        () -> veniceAdmin.setStorePartitionCount(clusterName, storeName, -1));

    // test setting amplification factor
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersion(version.getNumber()).get().getPartitionerConfig().getAmplificationFactor(), 1);
    final int amplificationFactor = 10;
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setAmplificationFactor(amplificationFactor);
    veniceAdmin.setStorePartitionerConfig(clusterName, storeName, partitionerConfig);
    Version newVersion = veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(),
        partitionCount, 2);
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersion(newVersion.getNumber()).get().getPartitionerConfig().getAmplificationFactor(), amplificationFactor);

    veniceAdmin.setIncrementalPushEnabled(clusterName, storeName, true);
    Assert.assertTrue(veniceAdmin.getStore(clusterName, storeName).isIncrementalPushEnabled());

    veniceAdmin.setBootstrapToOnlineTimeoutInHours(clusterName, storeName, 48);
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getBootstrapToOnlineTimeoutInHours(), 48);
    veniceAdmin.setLeaderFollowerModelEnabled(clusterName, storeName, true);
    Assert.assertTrue(veniceAdmin.getStore(clusterName, storeName).isLeaderFollowerModelEnabled());

    veniceAdmin.setHybridStoreDiskQuotaEnabled(clusterName, storeName, true);
    Assert.assertTrue(veniceAdmin.getStore(clusterName, storeName).isHybridStoreDiskQuotaEnabled());

    // test hybrid config
    //set incrementalPushEnabled to be false as hybrid and incremental are mutex
    veniceAdmin.setIncrementalPushEnabled(clusterName, storeName, false);
    Assert.assertFalse(veniceAdmin.getStore(clusterName, storeName).isHybrid());
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams()
            .setHybridRewindSeconds(TimeUnit.SECONDS.convert(2, TimeUnit.DAYS))
            .setHybridOffsetLagThreshold(1000L));
    Assert.assertTrue(veniceAdmin.getStore(clusterName, storeName).isHybrid());

    // test updating hybrid data replication policy
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams()
        .setHybridDataReplicationPolicy(DataReplicationPolicy.AGGREGATE));
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getHybridStoreConfig().getDataReplicationPolicy(), DataReplicationPolicy.AGGREGATE);

    // test reverting hybrid store back to batch-only store; negative config value will undo hybrid setting
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams()
        .setHybridRewindSeconds(-1)
        .setHybridOffsetLagThreshold(-1)
        .setHybridTimeLagThreshold(-1));
    Assert.assertFalse(veniceAdmin.getStore(clusterName, storeName).isHybrid());

    // test setting hybrid config with rewind time and time lag
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams()
        .setHybridRewindSeconds(TimeUnit.SECONDS.convert(2, TimeUnit.DAYS))
        .setHybridTimeLagThreshold(TimeUnit.MINUTES.toSeconds(1)));
    Assert.assertTrue(veniceAdmin.getStore(clusterName, storeName).isHybrid());

    stopParticipant(additionalNode);
    delayParticipantJobCompletion(false);
    stateModelFactories.forEach((nodeId, stateModelFactory)
        -> stateModelFactory.makeTransitionCompleted(version.kafkaTopicName(), 0));
  }

  @Test
  public void testAddVersionAndStartIngestionTopicCreationTimeout() {
    TopicManagerRepository originalTopicManagerRepository = veniceAdmin.getTopicManagerRepository();

    TopicManager mockedTopicManager = mock(TopicManager.class);
    doThrow(new VeniceOperationAgainstKafkaTimedOut("mock timeout"))
        .when(mockedTopicManager)
        .createTopic(anyString(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any(), eq(true));
    TopicManagerRepository mockedTopicManageRepository = mock(TopicManagerRepository.class);
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager();
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager(any(String.class));
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager(any(Pair.class));
    veniceAdmin.setTopicManagerRepository(mockedTopicManageRepository);
    String storeName = "test-store";
    String pushJobId = "test-push-job-id";
    veniceAdmin.addStore(clusterName, storeName, "test-owner", KEY_SCHEMA, VALUE_SCHEMA);
    for (int i = 0; i < 5; i ++) {
      // Mimic the retry behavior by the admin consumption task.
      Assert.assertThrows(VeniceOperationAgainstKafkaTimedOut.class,
          () -> veniceAdmin.addVersionAndStartIngestion(clusterName, storeName, pushJobId, 1, 1, Version.PushType.BATCH, null, -1, multiClusterConfig.getCommonConfig().getMetadataVersionId()));
    }
    Assert.assertFalse(veniceAdmin.getStore(clusterName, storeName).getVersion(1).isPresent());
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(DEFAULT_REPLICA_COUNT));
    reset(mockedTopicManager);
    veniceAdmin.addVersionAndStartIngestion(clusterName, storeName, pushJobId, 1, 1, Version.PushType.BATCH, null, -1, multiClusterConfig.getCommonConfig().getMetadataVersionId());
    Assert.assertTrue(veniceAdmin.getStore(clusterName, storeName).getVersion(1).isPresent());
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions().size(), 1,
        "There should only be exactly one version added to the test-store");

    //set topic original topic manager back
    veniceAdmin.setTopicManagerRepository(originalTopicManagerRepository);
  }

  @Test
  public void testAddVersionWhenClusterInMaintenanceMode() {
    String storeName = TestUtils.getUniqueString("test");

    veniceAdmin.addStore(clusterName, storeName, "owner", KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(DEFAULT_REPLICA_COUNT));
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    Assert.assertEquals(veniceAdmin.versionsForStore(clusterName, storeName).size(), 1);

    // enable maintenance mode
    veniceAdmin.getHelixAdmin().enableMaintenanceMode(clusterName, true);

    //HelixClusterMaintenanceModeException is expected since cluster is in maintenance mode
    Assert.assertThrows(HelixClusterMaintenanceModeException.class, () ->
        veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1));

    Admin.OfflinePushStatusInfo
        statusInfo = veniceAdmin.getOffLinePushStatus(clusterName, Version.composeKafkaTopic(storeName, 101));
    Assert.assertEquals(statusInfo.getExecutionStatus(), ExecutionStatus.NOT_CREATED);
    Assert.assertTrue(statusInfo.getStatusDetails().get().contains("in maintenance mode"));

    // disable maintenance mode
    veniceAdmin.getHelixAdmin().enableMaintenanceMode(clusterName, false);
    // try to add same version again
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    TestUtils.waitForNonDeterministicCompletion(30000, TimeUnit.MILLISECONDS, () -> veniceAdmin.getCurrentVersion(clusterName, storeName) == 2);
    veniceAdmin.retireOldStoreVersions(clusterName, storeName, true);
    Assert.assertEquals(veniceAdmin.versionsForStore(clusterName, storeName).size(), 1);

    veniceAdmin.getHelixAdmin().enableMaintenanceMode(clusterName, false);

  }

  @Test
  public void testGetRealTimeTopic(){
    String storeName = TestUtils.getUniqueString("store");

    //Must not be able to get a real time topic until the store is created
    Assert.assertThrows(VeniceNoStoreException.class, () -> veniceAdmin.getRealTimeTopic(clusterName, storeName));

    veniceAdmin.addStore(clusterName, storeName, "owner", KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams()
        .setHybridRewindSeconds(25L)
        .setHybridOffsetLagThreshold(100L)); //make store hybrid

    try {
      veniceAdmin.getRealTimeTopic(clusterName, storeName);
      Assert.fail("Must not be able to get a real time topic until the store is initialized with a version");
    } catch (VeniceException e){
      Assert.assertTrue(e.getMessage().contains("is not initialized with a version"), "Got unexpected error message: " + e.getMessage());
    }

    int partitions = 2; //TODO verify partition count for RT topic.
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), partitions, 1);

    String rtTopic = veniceAdmin.getRealTimeTopic(clusterName, storeName);
    Assert.assertEquals(rtTopic, storeName + "_rt");
  }

  @Test
  public void testGetAndCompareStorageNodeStatusForStorageNode() throws Exception {
    String storeName = "testGetStorageNodeStatusForStorageNode";
    int partitionCount = 2;
    int replicaCount = 2;
    //Start a new participant which would hang on bootstrap state.
    String newNodeId = "localhost_9900";
    //Ensure original participant would hang on bootstrap state.
    delayParticipantJobCompletion(true);
    startParticipant(true, newNodeId);
    veniceAdmin.addStore(clusterName, storeName, storeOwner, KEY_SCHEMA, VALUE_SCHEMA);
    Version version = veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(),
        partitionCount, replicaCount);

    TestUtils.waitForNonDeterministicCompletion(5000, TimeUnit.MILLISECONDS, () -> {
      PartitionAssignment partitionAssignment =
          veniceAdmin.getVeniceHelixResource(clusterName).getRoutingDataRepository().getPartitionAssignments(version.kafkaTopicName());
      if (partitionAssignment.getAssignedNumberOfPartitions() != partitionCount) {
        return false;
      }
      for (int i = 0; i < partitionCount; i++) {
        if (partitionAssignment.getPartition(i).getBootstrapInstances().size() != partitionCount) {
          return false;
        }
      }
      return true;
    });

    //Now all of replica in bootstrap state
    StorageNodeStatus status1 = veniceAdmin.getStorageNodesStatus(clusterName, NODE_ID);
    StorageNodeStatus status2 = veniceAdmin.getStorageNodesStatus(clusterName, newNodeId);
    for (int i = 0; i < partitionCount; i++) {
      Assert.assertEquals(status1.getStatusValueForReplica(HelixUtils.getPartitionName(version.kafkaTopicName(), i)),
          HelixState.BOOTSTRAP.getStateValue(), "Replica in server1 should hang on BOOTSTRAP");
    }
    for (int i = 0; i < partitionCount; i++) {
      Assert.assertEquals(status2.getStatusValueForReplica(HelixUtils.getPartitionName(version.kafkaTopicName(), i)),
          HelixState.BOOTSTRAP.getStateValue(), "Replica in server2 should hang on BOOTSTRAP");
    }

    //Set replicas to ONLINE.
    for (int i = 0; i < partitionCount; i++) {
      for (Map.Entry<String, MockTestStateModelFactory> entry : stateModelFactories.entrySet()) {
        MockTestStateModelFactory value = entry.getValue();
        value.makeTransitionCompleted(version.kafkaTopicName(), i);
      }
    }

    TestUtils.waitForNonDeterministicCompletion(10000, TimeUnit.MILLISECONDS, () -> {
      PartitionAssignment partitionAssignment =
          veniceAdmin.getVeniceHelixResource(clusterName).getRoutingDataRepository().getPartitionAssignments(version.kafkaTopicName());
      for (int i = 0; i < partitionCount; i++) {
        if (partitionAssignment.getPartition(i).getReadyToServeInstances().size() != partitionCount) {
          return false;
        }
      }
      return true;
    });

    StorageNodeStatus newStatus2 = veniceAdmin.getStorageNodesStatus(clusterName, newNodeId);
    Assert.assertTrue(newStatus2.isNewerOrEqual(status2), "ONLINE replicas should be newer than BOOTSTRAP replicas");

    stopParticipant(newNodeId);
    delayParticipantJobCompletion(false);
  }

  @Test
  public void testDisableStoreWrite() {
    String storeName = TestUtils.getUniqueString("testDisableStoreWriter");
    veniceAdmin.addStore(clusterName, storeName, storeOwner, KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(DEFAULT_REPLICA_COUNT));
    veniceAdmin.setStoreWriteability(clusterName, storeName, false);
    Store store = veniceAdmin.getStore(clusterName, storeName);

    //Store has been disabled, can not accept a new version
    Assert.assertThrows(VeniceException.class, () -> veniceAdmin.incrementVersionIdempotent(clusterName, storeName,
        Version.guidBasedDummyPushId(), 1, 1));

    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions(), store.getVersions());

    //Store has been disabled, can not accept a new version
    Assert.assertThrows(VeniceException.class, () ->
        veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1));

    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions(), store.getVersions());

    veniceAdmin.setStoreWriteability(clusterName, storeName, true);

    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    TestUtils.waitForNonDeterministicCompletion(30000, TimeUnit.MILLISECONDS, () -> veniceAdmin.getCurrentVersion(clusterName, storeName) == 2);
    veniceAdmin.retireOldStoreVersions(clusterName, storeName, true);

    store = veniceAdmin.getStore(clusterName, storeName);
    //Version 1 and version 2 are added to this store. Version 1 is deleted by early backup deletion
    Assert.assertTrue(store.isEnableWrites());
    Assert.assertEquals(store.getVersions().size(), 1);
    Assert.assertEquals(store.peekNextVersion().getNumber(), 3);
    PushMonitor monitor = veniceAdmin.getVeniceHelixResource(clusterName).getPushMonitor();
    TestUtils.waitForNonDeterministicCompletion(TOTAL_TIMEOUT_FOR_SHORT_TEST, TimeUnit.MILLISECONDS,
        () -> monitor.getPushStatusAndDetails(Version.composeKafkaTopic(storeName, 2), Optional.empty()).getFirst().equals(ExecutionStatus.COMPLETED)
    );
  }

  @Test
  public void testDisableStoreRead() {
    String storeName = TestUtils.getUniqueString("testDisableStoreRead");
    veniceAdmin.addStore(clusterName, storeName, storeOwner, KEY_SCHEMA, VALUE_SCHEMA);
    Version version =
        veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    veniceAdmin.setStoreCurrentVersion(clusterName, storeName, version.getNumber());

    veniceAdmin.setStoreReadability(clusterName, storeName, false);
    Assert.assertEquals(veniceAdmin.getCurrentVersion(clusterName, storeName), Store.NON_EXISTING_VERSION,
        "After disabling, store has no version available to serve.");

    veniceAdmin.setStoreReadability(clusterName, storeName, true);
    Assert.assertEquals(veniceAdmin.getCurrentVersion(clusterName, storeName), version.getNumber(),
        "After enabling, version:" + version.getNumber() + " is ready to serve.");
  }

  @Test
  public void testAccessControl() {
    String storeName = "testAccessControl";
    veniceAdmin.addStore(clusterName, storeName, storeOwner, KEY_SCHEMA, VALUE_SCHEMA);

    veniceAdmin.setAccessControl(clusterName, storeName, false);
    Store store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertFalse(store.isAccessControlled());

    veniceAdmin.setAccessControl(clusterName, storeName, true);
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertTrue(store.isAccessControlled());

    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams()
        .setAccessControlled(false));
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertFalse(store.isAccessControlled());

    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams()
        .setAccessControlled(true));
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertTrue(store.isAccessControlled());
  }

  @Test
  public void testWhitelist() {
    int testPort = 5555;
    Assert.assertEquals(veniceAdmin.getWhitelist(clusterName).size(), 0, "White list should be empty.");

    veniceAdmin.addInstanceToWhitelist(clusterName, Utils.getHelixNodeIdentifier(testPort));
    Assert.assertEquals(veniceAdmin.getWhitelist(clusterName).size(), 1,
        "After adding a instance into white list, the size of white list should be 1");

    Assert.assertEquals(veniceAdmin.getWhitelist(clusterName).iterator().next(), Utils.getHelixNodeIdentifier(testPort),
        "Instance in the white list is not the one added before.");
    veniceAdmin.removeInstanceFromWhiteList(clusterName, Utils.getHelixNodeIdentifier(testPort));
    Assert.assertEquals(veniceAdmin.getWhitelist(clusterName).size(), 0,
        "After removing the instance, white list should be empty.");
  }

  @Test
  public void testKillOfflinePush() throws Exception {
    String participantStoreRTTopic =
        Version.composeRealTimeTopic(VeniceSystemStoreUtils.getParticipantStoreNameForCluster(clusterName));
    String newNodeId = Utils.getHelixNodeIdentifier(9786);
    //Ensure original participant store would hang on bootstrap state.
    delayParticipantJobCompletion(true);
    startParticipant(true, newNodeId);
    String storeName = "testKillPush";
    int partitionCount = 2;
    int replicaFactor = 1;
    // Start a new version with 2 partition and 1 replica
    veniceAdmin.addStore(clusterName, storeName, "test", KEY_SCHEMA, VALUE_SCHEMA);
    Version version = veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(),
        partitionCount, replicaFactor);
    Map<String, Integer> nodesToPartitionMap = new HashMap<>();
    TestUtils.waitForNonDeterministicCompletion(5000, TimeUnit.MILLISECONDS, () -> {
      try {
        PartitionAssignment partitionAssignment = veniceAdmin.getVeniceHelixResource(clusterName)
            .getRoutingDataRepository()
            .getPartitionAssignments(version.kafkaTopicName());
        if(partitionAssignment.getAllPartitions().size() < partitionCount){
          return false;
        }
        if (partitionAssignment.getPartition(0).getBootstrapInstances().size() == 1
            && partitionAssignment.getPartition(1).getBootstrapInstances().size() == 1) {
          nodesToPartitionMap.put(partitionAssignment.getPartition(0).getBootstrapInstances().get(0).getNodeId(), 0);
          nodesToPartitionMap.put(partitionAssignment.getPartition(1).getBootstrapInstances().get(0).getNodeId(), 1);
          return true;
        }
        return false;
      }catch(VeniceException e) {
        return false;
      }
    });
    // Now we have two participants blocked on ST from BOOTSTRAP to ONLINE.
    Map<Integer, Long> participantTopicOffsets = veniceAdmin.getTopicManager().getLatestOffsets(participantStoreRTTopic);
    veniceAdmin.killOfflinePush(clusterName, version.kafkaTopicName(), false);
    // Verify the kill offline push message have been written to the participant message store RT topic.
    TestUtils.waitForNonDeterministicCompletion(5000, TimeUnit.MILLISECONDS, () -> {
      Map<Integer, Long> newPartitionTopicOffsets =
          veniceAdmin.getTopicManager().getLatestOffsets(participantStoreRTTopic);
      for (Map.Entry<Integer, Long> entry : participantTopicOffsets.entrySet()) {
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
    String storeName = TestUtils.getUniqueString("testDeleteAllVersions");
    // register kill message handler for participants.
    for (SafeHelixManager manager : this.participants.values()) {
      HelixStatusMessageChannel channel = new HelixStatusMessageChannel(manager, helixMessageChannelStats);
      channel.registerHandler(KillOfflinePushMessage.class, message -> {
        //make state transition failed to simulate kill consumption task.
        stateModelFactories.forEach((nodeId, stateModelFactory) ->
            stateModelFactory.makeTransitionCompleted(message.getKafkaTopic(), 0));
      });

      // Store has not been created.
      Assert.assertThrows(VeniceNoStoreException.class, () -> veniceAdmin.deleteAllVersionsInStore(clusterName, storeName));

      // Prepare 3 version. The first two are completed and the last one is still ongoing.
      int versionCount = 3;
      veniceAdmin.addStore(clusterName, storeName, "testOwner", KEY_SCHEMA, VALUE_SCHEMA);
      veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(DEFAULT_REPLICA_COUNT));
      Version lastVersion = null;
      for (int i = 0; i < versionCount; i++) {
        lastVersion =
            veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
        if (i < versionCount - 1) {
          // Hang the state transition of the last version only. Otherwise, retiring would be triggered.
          for (Map.Entry<String, MockTestStateModelFactory> entry : stateModelFactories.entrySet()) {
            MockTestStateModelFactory value = entry.getValue();
            value.makeTransitionCompleted(lastVersion.kafkaTopicName(), 0);
          }
          int versionNumber = lastVersion.getNumber();
          TestUtils.waitForNonDeterministicCompletion(30000, TimeUnit.MILLISECONDS,
              () -> veniceAdmin.getCurrentVersion(clusterName, storeName) == versionNumber);
        }
      }
      Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions().size(), 2);
      // Store has not been disabled.
      Assert.assertThrows(VeniceException.class, () -> veniceAdmin.deleteAllVersionsInStore(clusterName, storeName));

      veniceAdmin.setStoreReadability(clusterName, storeName, false);
      //Store has not been disabled to write
      Assert.assertThrows(VeniceException.class, () -> veniceAdmin.deleteAllVersionsInStore(clusterName, storeName));

      veniceAdmin.setStoreReadability(clusterName, storeName, true);
      veniceAdmin.setStoreWriteability(clusterName, storeName, false);
      //Store has not been disabled to read
      Assert.assertThrows(VeniceException.class, () -> veniceAdmin.deleteAllVersionsInStore(clusterName, storeName));

      // Store has been disabled.
      veniceAdmin.setStoreReadability(clusterName, storeName, false);
      veniceAdmin.deleteAllVersionsInStore(clusterName, storeName);
      Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions().size(), 0,
          " Versions should be deleted.");
      Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getCurrentVersion(), Store.NON_EXISTING_VERSION);
      // After enabling store, the serving version is -1 because there is not version available in this store.
      veniceAdmin.setStoreReadability(clusterName, storeName, true);
      Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getCurrentVersion(), Store.NON_EXISTING_VERSION,
          "No version should be available to read");
      String uncompletedTopic = lastVersion.kafkaTopicName();
      Assert.assertTrue(veniceAdmin.isTopicTruncated(uncompletedTopic), "Kafka topic: " + uncompletedTopic + " should be truncated for the uncompleted version.");
      String completedTopic = Version.composeKafkaTopic(storeName, lastVersion.getNumber() - 1);
      Assert.assertTrue(veniceAdmin.isTopicTruncated(completedTopic), "Kafka topic: " + completedTopic + " should be truncated for the completed version.");

      delayParticipantJobCompletion(false);
      for (Map.Entry<String, MockTestStateModelFactory> entry : stateModelFactories.entrySet()) {
        MockTestStateModelFactory value = entry.getValue();
        value.makeTransitionCompleted(lastVersion.kafkaTopicName(), 0);
      }
    }
  }

  @Test
  public void testDeleteAllVersionsInStoreWithoutJobAndResource() {
    String storeName = "testDeleteVersionInWithoutJobAndResource";
    Store store = TestUtils.createTestStore(storeName, storeOwner, System.currentTimeMillis());
    Version version = store.increaseVersion();
    store.updateVersionStatus(version.getNumber(), VersionStatus.ONLINE);
    store.setCurrentVersion(version.getNumber());
    veniceAdmin.getVeniceHelixResource(clusterName).getMetadataRepository().addStore(store);
    veniceAdmin.setStoreReadability(clusterName, storeName, false);
    veniceAdmin.setStoreWriteability(clusterName, storeName, false);

    veniceAdmin.deleteAllVersionsInStore(clusterName, storeName);
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions().size(), 0);
  }

  @Test
  public void testDeleteOldVersionInStore() {
    String storeName = TestUtils.getUniqueString("testDeleteOldVersion");
    for (SafeHelixManager manager : this.participants.values()) {
      HelixStatusMessageChannel channel = new HelixStatusMessageChannel(manager, helixMessageChannelStats);
      channel.registerHandler(KillOfflinePushMessage.class, message -> {/*ignore*/ });
    }
    veniceAdmin.addStore(clusterName, storeName, "testOwner", KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(DEFAULT_REPLICA_COUNT));
    // Add two versions.
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    TestUtils.waitForNonDeterministicCompletion(TOTAL_TIMEOUT_FOR_SHORT_TEST, TimeUnit.MILLISECONDS,
        () -> veniceAdmin.getStore(clusterName, storeName).getCurrentVersion() == 2);
    veniceAdmin.setStoreReadability(clusterName, storeName, false);
    veniceAdmin.deleteOldVersionInStore(clusterName, storeName, 1);
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions().size(), 1,
        " Version 1 should be deleted.");

    //Current version should not be deleted
    Assert.assertThrows(VeniceException.class, () -> veniceAdmin.deleteOldVersionInStore(clusterName, storeName, 2));

    try{
      veniceAdmin.deleteOldVersionInStore(clusterName,storeName, 3);
    }catch (VeniceException e){
      Assert.fail("Version 3 does not exist, so deletion request should be skipped without throwing any exception.");
    }
  }

  @Test
  public void testRetireOldStoreVersionsKillOfflineFails() {
    String storeName = TestUtils.getUniqueString("testDeleteOldVersion");
    HelixStatusMessageChannel channel = new HelixStatusMessageChannel(manager, helixMessageChannelStats);
    channel.registerHandler(KillOfflinePushMessage.class, message -> {
      if (message.getKafkaTopic().equals(Version.composeKafkaTopic(storeName, 1))) {
        throw new VeniceException("offline job failed!!");
      }
    });

    veniceAdmin.addStore(clusterName, storeName, "testOwner", KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(DEFAULT_REPLICA_COUNT));
    // Add three versions.
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);

    TestUtils.waitForNonDeterministicCompletion(TOTAL_TIMEOUT_FOR_LONG_TEST, TimeUnit.MILLISECONDS,
        () -> {
          System.out.println("sidian's log: current version is : " + veniceAdmin.getStore(clusterName, storeName).getCurrentVersion());
          return veniceAdmin.getStore(clusterName, storeName).getCurrentVersion() == 3;
        });
    veniceAdmin.retireOldStoreVersions(clusterName, storeName, true);
    veniceAdmin.setStoreReadability(clusterName, storeName, false);
    veniceAdmin.retireOldStoreVersions(clusterName, storeName, false);
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions().size(), 1,
        " Versions should be deleted.");
  }

  @Test
  public void testDeleteStore() {
    String storeName = TestUtils.getUniqueString("testDeleteStore");
    TestUtils.createTestStore(storeName, storeOwner, System.currentTimeMillis());
    for (SafeHelixManager manager : this.participants.values()) {
      HelixStatusMessageChannel channel = new HelixStatusMessageChannel(manager, helixMessageChannelStats);
      channel.registerHandler(KillOfflinePushMessage.class,
          message ->  stateModelFactories.forEach((nodeId, stateModelFactory)
              -> stateModelFactory.makeTransitionCompleted(message.getKafkaTopic(), 0))
      );
    }
    veniceAdmin.addStore(clusterName, storeName, storeOwner, "\"string\"", "\"string\"");
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(DEFAULT_REPLICA_COUNT));
    Version version =
        veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1,1);
    TestUtils.waitForNonDeterministicCompletion(TOTAL_TIMEOUT_FOR_SHORT_TEST, TimeUnit.MILLISECONDS,
        () -> veniceAdmin.getCurrentVersion(clusterName, storeName) == version.getNumber());
    Assert.assertTrue(
        veniceAdmin.getTopicManager().containsTopicAndAllPartitionsAreOnline(Version.composeKafkaTopic(storeName, version.getNumber())),
        "Kafka topic should be created.");

    // Store has not been disabled.
    Assert.assertThrows(VeniceException.class, () -> veniceAdmin.deleteStore(clusterName, storeName, Store.IGNORE_VERSION,
        true));

    veniceAdmin.setStoreReadability(clusterName, storeName, false);
    veniceAdmin.setStoreWriteability(clusterName, storeName, false);
    veniceAdmin.deleteStore(clusterName, storeName, Store.IGNORE_VERSION, true);
    Assert.assertNull(veniceAdmin.getStore(clusterName, storeName), "Store should be deleted before.");
    Assert.assertEquals(
        veniceAdmin.getStoreGraveyard().getLargestUsedVersionNumber(storeName),
        version.getNumber(), "LargestUsedVersionNumber should be kept in graveyard.");
    TestUtils.waitForNonDeterministicCompletion(TOTAL_TIMEOUT_FOR_LONG_TEST, TimeUnit.MILLISECONDS,
        () -> veniceAdmin.isTopicTruncated(Version.composeKafkaTopic(storeName, version.getNumber())));
  }

  @Test
  public void testDeleteStoreWithLargestUsedVersionNumberOverwritten() {
    String storeName = TestUtils.getUniqueString("testDeleteStore");
    int largestUsedVersionNumber = 1000;

    TestUtils.createTestStore(storeName, storeOwner, System.currentTimeMillis());
    for (SafeHelixManager manager : this.participants.values()) {
      HelixStatusMessageChannel channel = new HelixStatusMessageChannel(manager, helixMessageChannelStats);
      channel.registerHandler(KillOfflinePushMessage.class,
          message ->  stateModelFactories.forEach((nodeId, stateModelFactory)
              -> stateModelFactory.makeTransitionCompleted(message.getKafkaTopic(), 0))
      );

      veniceAdmin.addStore(clusterName, storeName, storeOwner, "\"string\"", "\"string\"");
      veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(DEFAULT_REPLICA_COUNT));
      Version version =
          veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
      TestUtils.waitForNonDeterministicCompletion(TOTAL_TIMEOUT_FOR_SHORT_TEST, TimeUnit.MILLISECONDS,
          () -> veniceAdmin.getCurrentVersion(clusterName, storeName) == version.getNumber());
      Assert.assertTrue(veniceAdmin.getTopicManager().containsTopicAndAllPartitionsAreOnline(Version.composeKafkaTopic(storeName, version.getNumber())),
          "Kafka topic should be created.");

      veniceAdmin.setStoreReadability(clusterName, storeName, false);
      veniceAdmin.setStoreWriteability(clusterName, storeName, false);
      veniceAdmin.deleteStore(clusterName, storeName, largestUsedVersionNumber, true);
      Assert.assertNull(veniceAdmin.getStore(clusterName, storeName), "Store should be deleted before.");
      Assert.assertEquals(veniceAdmin.getStoreGraveyard().getLargestUsedVersionNumber(storeName),
          largestUsedVersionNumber, "LargestUsedVersionNumber should be overwritten and kept in graveyard.");
    }
  }

  @Test
  public void testReCreateStore() {
    String storeName = TestUtils.getUniqueString("testReCreateStore");
    int largestUsedVersionNumber = 100;
    veniceAdmin.addStore(clusterName, storeName, storeOwner, "\"string\"", "\"string\"");

    Store store = veniceAdmin.getStore(clusterName, storeName);
    store.setLargestUsedVersionNumber(largestUsedVersionNumber);
    store.setEnableReads(false);
    store.setEnableWrites(false);
    veniceAdmin.getVeniceHelixResource(clusterName).getMetadataRepository().updateStore(store);
    veniceAdmin.deleteStore(clusterName, storeName, Store.IGNORE_VERSION, true);

    //Re-create store with incompatible schema
    veniceAdmin.addStore(clusterName, storeName, storeOwner, "\"long\"", "\"long\"");
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    Assert.assertEquals(veniceAdmin.getKeySchema(clusterName, storeName).getSchema().toString(), "\"long\"");
    Assert.assertEquals(veniceAdmin.getValueSchema(clusterName, storeName, 1).getSchema().toString(), "\"long\"");
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getLargestUsedVersionNumber(),
        largestUsedVersionNumber + 1);
  }

  @Test
  public void testReCreateStoreWithLegacyStore(){
    String storeName = TestUtils.getUniqueString("testReCreateStore");
    int largestUsedVersionNumber = 100;
    veniceAdmin.addStore(clusterName, storeName, storeOwner, "\"string\"", "\"string\"");

    Store store = veniceAdmin.getStore(clusterName, storeName);
    store.setLargestUsedVersionNumber(largestUsedVersionNumber);
    store.setEnableWrites(false);
    store.setEnableReads(false);
    // Legacy store
    ZkStoreConfigAccessor storeConfigAccessor = veniceAdmin.getVeniceHelixResource(clusterName).getStoreConfigAccessor();
    StoreConfig storeConfig = storeConfigAccessor.getStoreConfig(storeName);
    storeConfig.setDeleting(true);
    storeConfigAccessor.updateConfig(storeConfig, false);
    veniceAdmin.getVeniceHelixResource(clusterName).getMetadataRepository().updateStore(store);
    //Re-create store with incompatible schema
    veniceAdmin.addStore(clusterName, storeName, storeOwner, "\"long\"", "\"long\"");
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    Assert.assertEquals(veniceAdmin.getKeySchema(clusterName, storeName).getSchema().toString(), "\"long\"");
    Assert.assertEquals(veniceAdmin.getValueSchema(clusterName, storeName, 1).getSchema().toString(), "\"long\"");
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getLargestUsedVersionNumber(),
        largestUsedVersionNumber + 1);
  }

  @Test
  public void testChunkingEnabled() {
    String storeName = TestUtils.getUniqueString("test_store");
    veniceAdmin.addStore(clusterName, storeName, storeOwner, "\"string\"", "\"string\"");

    Store store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertFalse(store.isChunkingEnabled());

    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams()
        .setChunkingEnabled(true));
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertTrue(store.isChunkingEnabled());
  }

  @Test
  public void testFindAllBootstrappingVersions() throws Exception {
    delayParticipantJobCompletion(true);
    String storeName = TestUtils.getUniqueString("test_store");
    veniceAdmin.addStore(clusterName, storeName, storeOwner, "\"string\"", "\"string\"");
        veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(DEFAULT_REPLICA_COUNT));
    stateModelFactories.forEach((nodeId, stateModelFactory) ->
        stateModelFactory.makeTransitionCompleted(Version.composeKafkaTopic(storeName, 1), 0));
    // Wait version 1 become online.
    TestUtils.waitForNonDeterministicCompletion(TOTAL_TIMEOUT_FOR_SHORT_TEST, TimeUnit.MILLISECONDS,
        () -> veniceAdmin.getCurrentVersion(clusterName, storeName) == 1);
    // Restart participant
    stopParticipants();
    // This will make all participant store versions on bootstrap state.
    startParticipant(true, NODE_ID);
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    Thread.sleep(1000l);
    Map<String, String> result = veniceAdmin.findAllBootstrappingVersions(clusterName);
    // After participant restart, the original participant store version will hang on bootstrap state. Instead of checking #
    // of versions having bootstrapping replicas, we directly checking bootstrapping replicas the test store's versions.
    Assert.assertTrue(result.containsKey(Version.composeKafkaTopic(storeName, 1)));
    Assert.assertTrue(result.containsKey(Version.composeKafkaTopic(storeName, 2)));
    Assert.assertEquals(result.get(Version.composeKafkaTopic(storeName, 1)), VersionStatus.ONLINE.toString(),
        "version 1 has been ONLINE, but we stopped participant which will ask replica to bootstrap again.");
    Assert.assertEquals(result.get(Version.composeKafkaTopic(storeName, 2)), VersionStatus.STARTED.toString(),
        "version 2 has been started, replica is bootstrapping.");

    delayParticipantJobCompletion(false);
  }

  @Test
  public void testGetFutureVersions() throws Exception {
    delayParticipantJobCompletion(true);
    String storeName = TestUtils.getUniqueString("test_store");
    veniceAdmin.addStore(clusterName, storeName, storeOwner, "\"string\"", "\"string\"");
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);

    int futureVersion = veniceAdmin.getFutureVersion(clusterName, storeName);
    Assert.assertEquals(futureVersion, 1, "Expected future version number of 1!!");
    stateModelFactories.forEach((nodeId, stateModelFactory) ->
        stateModelFactory.makeTransitionCompleted(Version.composeKafkaTopic(storeName, 1), 0));
    // Wait version 1 become online.
    // TOTAL_TIMEOUT_FOR_SHORT_TEST
    TestUtils.waitForNonDeterministicCompletion(TOTAL_TIMEOUT_FOR_LONG_TEST*100, TimeUnit.MILLISECONDS,
        () -> veniceAdmin.getCurrentVersion(clusterName, storeName) == 1);
    futureVersion = veniceAdmin.getFutureVersion(clusterName, storeName);
    Assert.assertEquals(futureVersion, Store.NON_EXISTING_VERSION, "Expected future version number of 0!!");
    veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);
    futureVersion = veniceAdmin.getFutureVersion(clusterName, storeName);
    Assert.assertEquals(futureVersion, 2, "Expected future version number of 2!!");
    stateModelFactories.forEach((nodeId, stateModelFactory) ->
        stateModelFactory.makeTransitionCompleted(Version.composeKafkaTopic(storeName, 2), 0));
    TestUtils.waitForNonDeterministicCompletion(TOTAL_TIMEOUT_FOR_LONG_TEST*100, TimeUnit.MILLISECONDS,
        () -> veniceAdmin.getCurrentVersion(clusterName, storeName) == 2);
    futureVersion = veniceAdmin.getFutureVersion(clusterName, storeName);
    Assert.assertEquals(futureVersion, Store.NON_EXISTING_VERSION, "Expected future version number of 0!!");
    delayParticipantJobCompletion(false);
  }

  @Test
  public void testBatchGetLimit() {
    String storeName = TestUtils.getUniqueString("test_store");
    veniceAdmin.addStore(clusterName, storeName, storeOwner, "\"string\"", "\"string\"");

    Store store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertEquals(store.getBatchGetLimit(), -1);

    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams()
        .setBatchGetLimit(100));
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertEquals(store.getBatchGetLimit(), 100);
  }

  @Test
  public void testNumVersionsToPreserve() {
    String storeName = TestUtils.getUniqueString("test_store");
    veniceAdmin.addStore(clusterName, storeName, storeOwner, "\"string\"", "\"string\"");

    Store store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertEquals(store.getNumVersionsToPreserve(), store.NUM_VERSION_PRESERVE_NOT_SET);
    int numVersionsToPreserve = 100;

    veniceAdmin.updateStore(clusterName, storeName,
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
      String storeName = TestUtils.getUniqueString("store-" + storeNumber);
      Store store = new ZKStore(storeName, storeOwner, System.currentTimeMillis(), PersistenceType.ROCKS_DB,
          RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE,
          OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION, 1);

      // Two active versions, selected at random
      List<Integer> activeVersions = new ArrayList<>();
      int firstActiveVersion = (int) Math.ceil(Math.random() * NUMBER_OF_VERSIONS);
      activeVersions.add(firstActiveVersion);
      int secondActiveVersion = 0;
      while (secondActiveVersion == 0 || firstActiveVersion == secondActiveVersion) {
        secondActiveVersion = (int) Math.ceil(Math.random() * NUMBER_OF_VERSIONS);
      }
      activeVersions.add(secondActiveVersion);

      logger.info("Active versions for '" + storeName + "': " + activeVersions);

      // Create ten topics and keep track of the active versions in the Store instance
      for (int versionNumber = 1; versionNumber <= NUMBER_OF_VERSIONS; versionNumber++) {
        Version version = new VersionImpl(storeName, versionNumber, TestUtils.getUniqueString(storeName));
        String topicName = version.kafkaTopicName();
        topicManager.createTopic(topicName, 1, 1, true);
        if (activeVersions.contains(versionNumber)) {
          store.addVersion(version);
        }
      }
      stores.add(store);
    }

    // Sanity check...
    for (Store store: stores) {
      for (int versionNumber = 1; versionNumber <= NUMBER_OF_VERSIONS; versionNumber++) {
        String topicName = Version.composeKafkaTopic(store.getName(), versionNumber);
        Assert.assertTrue(topicManager.containsTopicAndAllPartitionsAreOnline(topicName), "Topic '" + topicName + "' should exist.");
      }
    }

    Store storeToCleanUp = stores.get(0);
    veniceAdmin.truncateOldTopics(clusterName, storeToCleanUp, false);

    // verify that the storeToCleanUp has its topics cleaned up, and the others don't
    // verify all the topics of 'storeToCleanup' without corresponding active versions have been cleaned up
    for (Store store: stores) {
      for (int versionNumber = 1; versionNumber <= NUMBER_OF_VERSIONS; versionNumber++) {
        String topicName = Version.composeKafkaTopic(store.getName(), versionNumber);
        if (store.equals(storeToCleanUp) && !store.containsVersion(versionNumber) && versionNumber <= store.getLargestUsedVersionNumber()) {
          Assert.assertTrue(veniceAdmin.isTopicTruncated(topicName), "Topic '" + topicName + "' should be truncated.");
        } else {
          Assert.assertTrue(!veniceAdmin.isTopicTruncated(topicName),
              "Topic '" + topicName + "' should exist when active versions are: " +
                  store.getVersions().stream()
                      .map(version -> Integer.toString(version.getNumber()))
                      .collect(Collectors.joining(", ")) +
                  ", and largest used version: " + store.getLargestUsedVersionNumber() + ".");
        }
      }
    }
  }

  @Test
  public void testSetLargestUsedVersion() {
    String storeName = "testSetLargestUsedVersion";
    veniceAdmin.addStore(clusterName, storeName, storeOwner, KEY_SCHEMA, VALUE_SCHEMA);
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
    String storeName = TestUtils.getUniqueString("test_store");
    veniceAdmin.addStore(clusterName, storeName, storeOwner, "\"string\"", "\"string\"");

    Store store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertFalse(store.isWriteComputationEnabled());

    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams()
        .setWriteComputationEnabled(true));
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertTrue(store.isWriteComputationEnabled());
  }

  @Test
  public void testComputationEnabled() {
    String storeName = TestUtils.getUniqueString("test_store");
    veniceAdmin.addStore(clusterName, storeName, storeOwner, KEY_SCHEMA, VALUE_SCHEMA);

    Store store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertFalse(store.isReadComputationEnabled());

    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams()
        .setReadComputationEnabled(true));
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertTrue(store.isReadComputationEnabled());
  }

  @Test
  public void testAddAndRemoveDerivedSchema() {
    String storeName = TestUtils.getUniqueString("write_compute_store");
    String recordSchemaStr = TestPushUtils.USER_SCHEMA_STRING_WITH_DEFAULT;
    Schema derivedSchema = WriteComputeSchemaAdapter.parse(recordSchemaStr);

    veniceAdmin.addStore(clusterName, storeName, storeOwner, KEY_SCHEMA, recordSchemaStr);
    veniceAdmin.addDerivedSchema(clusterName, storeName, 1, derivedSchema.toString());
    Assert.assertEquals(veniceAdmin.getDerivedSchemas(clusterName, storeName).size(), 1);

    veniceAdmin.removeDerivedSchema(clusterName, storeName, 1, 1);
    Assert.assertEquals(veniceAdmin.getDerivedSchemas(clusterName, storeName).size(), 0);
  }

  @Test
  public void testStoreLevelConfigUpdateShouldNotModifyExistingVersionLevelConfig() {
    String storeName = TestUtils.getUniqueString("test_store");
    veniceAdmin.addStore(clusterName, storeName, storeOwner, "\"string\"", "\"string\"");

    /**
     * Create a version with default version level setting:
     * chunkingEnabled = false
     * leaderFollowerModelEnabled = false
     * compressionStrategy = CompressionStrategy.NO_OP
     */
    Version existingVersion = veniceAdmin.incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);

    Store store = veniceAdmin.getStore(clusterName, storeName);
    // Check all default setting in store level config
    Assert.assertFalse(store.isChunkingEnabled());
    Assert.assertFalse(store.isLeaderFollowerModelEnabled());
    Assert.assertEquals(store.getCompressionStrategy(), CompressionStrategy.NO_OP);
    // Check all setting in the existing version
    Assert.assertFalse(store.getVersion(existingVersion.getNumber()).get().isChunkingEnabled());
    Assert.assertFalse(store.getVersion(existingVersion.getNumber()).get().isLeaderFollowerModelEnabled());
    Assert.assertEquals(store.getVersion(existingVersion.getNumber()).get().getCompressionStrategy(), CompressionStrategy.NO_OP);

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
     * Enable leader/follower for the store.
     */
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setLeaderFollowerModel(true));
    store = veniceAdmin.getStore(clusterName, storeName);
    // Store level config should be updated
    Assert.assertTrue(store.isLeaderFollowerModelEnabled());
    // Existing version config should not be updated!
    Assert.assertFalse(store.getVersion(existingVersion.getNumber()).get().isLeaderFollowerModelEnabled());

    /**
     * Enable compression.
     */
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setCompressionStrategy(CompressionStrategy.GZIP));
    store = veniceAdmin.getStore(clusterName, storeName);
    // Store level config should be updated
    Assert.assertEquals(store.getCompressionStrategy(), CompressionStrategy.GZIP);
    // Existing version config should not be updated!
    Assert.assertEquals(store.getVersion(existingVersion.getNumber()).get().getCompressionStrategy(), CompressionStrategy.NO_OP);
  }

  @Test
  public void testZkSharedStore() {
    String metadataStorePrefix = VeniceSystemStoreType.METADATA_STORE.getPrefix();
    String systemStoreOne = metadataStorePrefix + "_store_one";
    String systemStoreTwo = metadataStorePrefix + "_store_two";
    veniceAdmin.addStore(clusterName, metadataStorePrefix,"Venice", KEY_SCHEMA, VALUE_SCHEMA, true);
    Store storeOne = veniceAdmin.getStore(clusterName, systemStoreOne);
    Store storeTwo = veniceAdmin.getStore(clusterName, systemStoreTwo);
    Assert.assertEquals(storeOne, storeTwo, "The two metadata system store should share the same Store in Zk");
    Schema storeOneValueSchema = veniceAdmin.getLatestValueSchema(clusterName, storeOne);
    Assert.assertNotNull(storeOneValueSchema);
    Assert.assertEquals(storeOneValueSchema, veniceAdmin.getLatestValueSchema(clusterName, storeTwo),
        "The two metadata system store should share the same value schema in Zk");
    long quotaInBytes = 12345;
    veniceAdmin.updateStore(clusterName, metadataStorePrefix,
        new UpdateStoreQueryParams().setStorageQuotaInByte(quotaInBytes));
    storeOne = veniceAdmin.getStore(clusterName, systemStoreOne);
    storeTwo = veniceAdmin.getStore(clusterName, systemStoreTwo);
    Assert.assertEquals(storeOne.getStorageQuotaInByte(), quotaInBytes, "The metadata system store quota should be updated");
    Assert.assertEquals(storeOne, storeTwo, "The two metadata system store should get the same update");
    veniceAdmin.newZkSharedStoreVersion(clusterName, metadataStorePrefix);
    storeOne = veniceAdmin.getStore(clusterName, systemStoreOne);
    Assert.assertEquals(storeOne.getCurrentVersion(), 1);
    Assert.assertTrue(storeOne.getVersion(1).isPresent());
    Assert.assertEquals(storeOne.getVersion(1).get().getPartitionCount(), storeOne.getPartitionCount());
  }

  @Test
  public void testAddVersionWithRemoteKafkaBootstrapServers() {
    TopicManagerRepository originalTopicManagerRepository = veniceAdmin.getTopicManagerRepository();

    TopicManager mockedTopicManager = mock(TopicManager.class);
    TopicManagerRepository mockedTopicManageRepository = mock(TopicManagerRepository.class);
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager();
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager(any(String.class));
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager(any(Pair.class));
    veniceAdmin.setTopicManagerRepository(mockedTopicManageRepository);
    String storeName = TestUtils.getUniqueString("test-store");
    String pushJobId1 = "test-push-job-id-1";
    veniceAdmin.addStore(clusterName, storeName, "test-owner", KEY_SCHEMA, VALUE_SCHEMA);
    /**
     * Enable L/F and native replication.
     */
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setLeaderFollowerModel(true).setNativeReplicationEnabled(true));

    /**
     * Add version 1 without remote Kafka bootstrap servers.
     */
    veniceAdmin.addVersionAndTopicOnly(clusterName, storeName, pushJobId1, 1, 1,
        false, true, Version.PushType.BATCH, null, null, Optional.empty(), -1, 1);
    // Version 1 should exist.
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions().size(), 1);

    /**
     * Add version 2 with remote kafka bootstrap servers.
     */
    String remoteKafkaBootstrapServers = "localhost:9092";
    String pushJobId2 = "test-push-job-id-2";
    veniceAdmin.addVersionAndTopicOnly(clusterName, storeName, pushJobId2, 2, 1,
        false, true, Version.PushType.BATCH, null, remoteKafkaBootstrapServers, Optional.empty(), -1, 1);
    // Version 2 should exist and remote Kafka bootstrap servers info should exist in version 2.
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions().size(), 2);
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersion(2).get().getPushStreamSourceAddress(), remoteKafkaBootstrapServers);

    //set topic original topic manager back
    veniceAdmin.setTopicManagerRepository(originalTopicManagerRepository);
  }

  @Test
  public void testUpdateStoreFailsWhenIncrementalAndHybridAreEnabledWithoutCorrectPolicy() {
    String incrementalEnabledStoreName = TestUtils.getUniqueString("testIncrementalStore");
    veniceAdmin.addStore(clusterName, incrementalEnabledStoreName, storeOwner, "\"string\"", "\"string\"");

    String hybridEnabledStoreName = TestUtils.getUniqueString("testHybridStore");
    veniceAdmin.addStore(clusterName, hybridEnabledStoreName, storeOwner, "\"string\"", "\"string\"");

    veniceAdmin.updateStore(clusterName, incrementalEnabledStoreName, new UpdateStoreQueryParams().setIncrementalPushEnabled(true));
    veniceAdmin.updateStore(clusterName, hybridEnabledStoreName, new UpdateStoreQueryParams().setHybridOffsetLagThreshold(1).setHybridRewindSeconds(0));

    // Incremental push enabled store can not be enabled as hybrid store without setting incremental push policy
    Assert.assertThrows(() -> veniceAdmin.updateStore(clusterName, incrementalEnabledStoreName, new UpdateStoreQueryParams().setHybridOffsetLagThreshold(0).setHybridRewindSeconds(0)));
    // Incremental push enabled store can not set push policy to anything other than PUSH_TO_VERSION_TOPIC if it is not enabled for hybrid
    Assert.assertThrows(() -> veniceAdmin.updateStore(clusterName, incrementalEnabledStoreName, new UpdateStoreQueryParams().setIncrementalPushPolicy(
        IncrementalPushPolicy.INCREMENTAL_PUSH_SAME_AS_REAL_TIME)));
    // Incremental push enabled store has no effect if hybrid store is being made batch only
    veniceAdmin.updateStore(clusterName, incrementalEnabledStoreName, new UpdateStoreQueryParams().setHybridOffsetLagThreshold(-1).setHybridRewindSeconds(-1));
    // Incremental push enabled store can be made a hybrid store if the incremental push policy is also updated
    veniceAdmin.updateStore(clusterName, incrementalEnabledStoreName, new UpdateStoreQueryParams()
        .setIncrementalPushPolicy(IncrementalPushPolicy.INCREMENTAL_PUSH_SAME_AS_REAL_TIME)
        .setHybridOffsetLagThreshold(0)
        .setHybridTimeLagThreshold(0)
        .setHybridRewindSeconds(0)
    );

    // Hybrid store can not be enabled for incremental push without setting incremental push policy
    Assert.assertThrows(() -> veniceAdmin.updateStore(clusterName, hybridEnabledStoreName, new UpdateStoreQueryParams().setIncrementalPushEnabled(true)));
    // Hybrid store can be enabled for incremental push if the incremental push policy is also updated
    veniceAdmin.updateStore(clusterName, hybridEnabledStoreName, new UpdateStoreQueryParams()
        .setIncrementalPushEnabled(true)
        .setIncrementalPushPolicy(IncrementalPushPolicy.INCREMENTAL_PUSH_SAME_AS_REAL_TIME)
    );

    // A hybrid and incremental push enabled store can not change the incremental push policy to PUSH_TO_VERSION_TOPIC
    Assert.assertThrows(() -> veniceAdmin.updateStore(clusterName, hybridEnabledStoreName, new UpdateStoreQueryParams().setIncrementalPushPolicy(IncrementalPushPolicy.PUSH_TO_VERSION_TOPIC)));
  }

  @Test
  public void testCheckWhetherStoreWillHaveConflictConfigForIncrementalAndHybrid() {
    String incrementalEnabledStoreName = TestUtils.getUniqueString("testIncrementalStore");
    Store incrementalEnabledStore = mock(Store.class);

    String hybridEnabledStoreName = TestUtils.getUniqueString("testHybridStore");
    Store hybridEnabledStore = mock(Store.class);

    String incrementalAndHybridEnabledStoreName = TestUtils.getUniqueString("testHybridIncrementalStore");
    Store incrementalAndHybridEnabledStore = mock(Store.class);

    HybridStoreConfig mockHybridConfig = mock(HybridStoreConfig.class);
    HybridStoreConfig mockHybridToBatchConfig = mock(HybridStoreConfig.class);

    doReturn(incrementalEnabledStoreName).when(incrementalEnabledStore).getName();
    doReturn(false).when(incrementalEnabledStore).isHybrid();
    doReturn(true).when(incrementalEnabledStore).isIncrementalPushEnabled();
    doReturn(IncrementalPushPolicy.PUSH_TO_VERSION_TOPIC).when(incrementalEnabledStore).getIncrementalPushPolicy();

    doReturn(hybridEnabledStoreName).when(hybridEnabledStore).getName();
    doReturn(true).when(hybridEnabledStore).isHybrid();
    doReturn(false).when(hybridEnabledStore).isIncrementalPushEnabled();
    doReturn(mockHybridConfig).when(hybridEnabledStore).getHybridStoreConfig();
    doReturn(IncrementalPushPolicy.PUSH_TO_VERSION_TOPIC).when(hybridEnabledStore).getIncrementalPushPolicy();

    doReturn(incrementalAndHybridEnabledStoreName).when(incrementalAndHybridEnabledStore).getName();
    doReturn(true).when(incrementalAndHybridEnabledStore).isHybrid();
    doReturn(true).when(incrementalAndHybridEnabledStore).isIncrementalPushEnabled();
    doReturn(mockHybridConfig).when(incrementalAndHybridEnabledStore).getHybridStoreConfig();
    doReturn(IncrementalPushPolicy.PUSH_TO_VERSION_TOPIC).when(incrementalAndHybridEnabledStore).getIncrementalPushPolicy();

    doReturn(0L).when(mockHybridConfig).getRewindTimeInSeconds();
    doReturn(0L).when(mockHybridConfig).getOffsetLagThresholdToGoOnline();
    doReturn(0L).when(mockHybridConfig).getProducerTimestampLagThresholdToGoOnlineInSeconds();

    doReturn(-1L).when(mockHybridToBatchConfig).getRewindTimeInSeconds();
    doReturn(-1L).when(mockHybridToBatchConfig).getOffsetLagThresholdToGoOnline();
    doReturn(-1L).when(mockHybridToBatchConfig).getProducerTimestampLagThresholdToGoOnlineInSeconds();

    Optional<Boolean> newIncrementalPushEnabled = Optional.of(true);
    Optional<Boolean> newIncrementalPushDisabled = Optional.of(false);
    Optional<Boolean> newIncrementalPushNotDefined = Optional.empty();

    Optional<IncrementalPushPolicy> newIncrementalPushPolicyIncompatibleWithHybrid = Optional.of(IncrementalPushPolicy.PUSH_TO_VERSION_TOPIC);
    Optional<IncrementalPushPolicy> newIncrementalPushPolicyCompatibleWithHybrid = Optional.of(IncrementalPushPolicy.INCREMENTAL_PUSH_SAME_AS_REAL_TIME);
    Optional<IncrementalPushPolicy> newIncrementalPushPolicyIncompatibleWithBatchOnly = Optional.of(IncrementalPushPolicy.INCREMENTAL_PUSH_SAME_AS_REAL_TIME);
    Optional<IncrementalPushPolicy> newIncrementalPushPolicyCompatibleWithBatchOnly = Optional.of(IncrementalPushPolicy.PUSH_TO_VERSION_TOPIC);
    Optional<IncrementalPushPolicy> newIncrementalPushPolicyNotDefined = Optional.empty();

    // Incremental push enabled store can be made a hybrid store if the incremental push policy is also updated
    veniceAdmin.checkWhetherStoreWillHaveConflictConfigForIncrementalAndHybrid(incrementalEnabledStore, newIncrementalPushNotDefined, newIncrementalPushPolicyCompatibleWithHybrid, Optional.of(mockHybridConfig));

    // Incremental push enabled store can not be enabled as hybrid store without setting a compatible incremental push policy
    Assert.assertThrows(() -> veniceAdmin.checkWhetherStoreWillHaveConflictConfigForIncrementalAndHybrid(incrementalEnabledStore, newIncrementalPushNotDefined, newIncrementalPushPolicyIncompatibleWithHybrid, Optional.of(mockHybridConfig)));

    // Incremental push enabled store can not set push policy to hybrid store incompatible policy if it is not enabled for hybrid
    Assert.assertThrows(() -> veniceAdmin.checkWhetherStoreWillHaveConflictConfigForIncrementalAndHybrid(incrementalEnabledStore, newIncrementalPushNotDefined, newIncrementalPushPolicyIncompatibleWithBatchOnly, Optional.of(mockHybridToBatchConfig)));

    // Incremental push enabled store has no effect if hybrid store is being made batch only
    veniceAdmin.checkWhetherStoreWillHaveConflictConfigForIncrementalAndHybrid(incrementalEnabledStore, newIncrementalPushNotDefined, newIncrementalPushPolicyCompatibleWithBatchOnly, Optional.of(mockHybridToBatchConfig));

    // Incremental push enabled store can be made a hybrid store if the incremental push policy is also updated
    veniceAdmin.checkWhetherStoreWillHaveConflictConfigForIncrementalAndHybrid(incrementalEnabledStore, newIncrementalPushNotDefined, newIncrementalPushPolicyCompatibleWithHybrid, Optional.of(mockHybridConfig));

    // Hybrid store can not be enabled for incremental push without setting incremental push policy
    Assert.assertThrows(() -> veniceAdmin.checkWhetherStoreWillHaveConflictConfigForIncrementalAndHybrid(hybridEnabledStore, newIncrementalPushEnabled, newIncrementalPushPolicyNotDefined, Optional.empty()));

    // Hybrid store can be enabled for incremental push if the incremental push policy is also updated
    veniceAdmin.checkWhetherStoreWillHaveConflictConfigForIncrementalAndHybrid(hybridEnabledStore, newIncrementalPushEnabled, newIncrementalPushPolicyCompatibleWithHybrid, Optional.empty());

    // A hybrid and incremental push enabled store can not change the incremental push policy to PUSH_TO_VERSION_TOPIC
    Assert.assertThrows(() -> veniceAdmin.checkWhetherStoreWillHaveConflictConfigForIncrementalAndHybrid(incrementalAndHybridEnabledStore, newIncrementalPushNotDefined, newIncrementalPushPolicyIncompatibleWithHybrid, Optional.empty()));

    // A hybrid and incremental push enabled store can disable incremental push without changing the policy since there'll be no more incremental pushes
    veniceAdmin.checkWhetherStoreWillHaveConflictConfigForIncrementalAndHybrid(incrementalAndHybridEnabledStore, newIncrementalPushDisabled, newIncrementalPushPolicyNotDefined, Optional.empty());
  }

  @Test
  public void testNativeReplicationSourceFabric() {
    String storeName = TestUtils.getUniqueString("test_store_nr");
    veniceAdmin.addStore(clusterName, storeName, storeOwner, KEY_SCHEMA, VALUE_SCHEMA);

    Store store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertTrue(store.getNativeReplicationSourceFabric().isEmpty());

    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams()
        .setNativeReplicationSourceFabric("dc1"));
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertEquals(store.getNativeReplicationSourceFabric(), "dc1");
  }

  @Test
  public void testGetIncrementalPushVersion() {
    String incrementalEnabledStoreName = TestUtils.getUniqueString("testIncrementalStore");
    veniceAdmin.addStore(clusterName, incrementalEnabledStoreName, storeOwner, "\"string\"", "\"string\"");
    veniceAdmin.updateStore(clusterName, incrementalEnabledStoreName, new UpdateStoreQueryParams().setIncrementalPushEnabled(true));
    veniceAdmin.incrementVersionIdempotent(clusterName, incrementalEnabledStoreName, Version.guidBasedDummyPushId(), 1, 1);
    TestUtils.waitForNonDeterministicCompletion(TOTAL_TIMEOUT_FOR_SHORT_TEST, TimeUnit.MILLISECONDS,
        () -> veniceAdmin.getCurrentVersion(clusterName, incrementalEnabledStoreName) == 1);
    veniceAdmin.getIncrementalPushVersion(clusterName, incrementalEnabledStoreName);

    // For incremental push policy PUSH_TO_VERSION_TOPIC, incremental push should fail if version topic is truncated
    veniceAdmin.truncateKafkaTopic(Version.composeKafkaTopic(incrementalEnabledStoreName, 1));
    Assert.assertThrows(VeniceException.class, () -> veniceAdmin.getIncrementalPushVersion(clusterName, incrementalEnabledStoreName));

    String incrementalAndHybridEnabledStoreName = TestUtils.getUniqueString("testHybridStore");
    veniceAdmin.addStore(clusterName, incrementalAndHybridEnabledStoreName, storeOwner, "\"string\"", "\"string\"");
    veniceAdmin.updateStore(clusterName, incrementalAndHybridEnabledStoreName,
        new UpdateStoreQueryParams()
            .setHybridOffsetLagThreshold(1)
            .setHybridRewindSeconds(0)
            .setIncrementalPushEnabled(true)
            .setIncrementalPushPolicy(IncrementalPushPolicy.INCREMENTAL_PUSH_SAME_AS_REAL_TIME));
    veniceAdmin.incrementVersionIdempotent(clusterName, incrementalAndHybridEnabledStoreName, Version.guidBasedDummyPushId(), 1, 1);
    String rtTopic = veniceAdmin.getRealTimeTopic(clusterName, incrementalAndHybridEnabledStoreName);
    TestUtils.waitForNonDeterministicCompletion(TOTAL_TIMEOUT_FOR_SHORT_TEST, TimeUnit.MILLISECONDS,
        () -> veniceAdmin.getCurrentVersion(clusterName, incrementalAndHybridEnabledStoreName) == 1);

    // For incremental push policy INCREMENTAL_PUSH_SAME_AS_REAL_TIME, incremental push should succeed even if version topic is truncated
    veniceAdmin.truncateKafkaTopic(Version.composeKafkaTopic(incrementalAndHybridEnabledStoreName, 1));
    veniceAdmin.getIncrementalPushVersion(clusterName, incrementalAndHybridEnabledStoreName);

    // For incremental push policy INCREMENTAL_PUSH_SAME_AS_REAL_TIME, incremental push should fail if rt topic is truncated
    veniceAdmin.truncateKafkaTopic(rtTopic);
    Assert.assertThrows(VeniceException.class, () -> veniceAdmin.getIncrementalPushVersion(clusterName, incrementalAndHybridEnabledStoreName));
  }

  @Test
  public void testVersionLevelActiveActiveReplicationConfig() {
    TopicManagerRepository originalTopicManagerRepository = veniceAdmin.getTopicManagerRepository();

    TopicManager mockedTopicManager = mock(TopicManager.class);
    TopicManagerRepository mockedTopicManageRepository = mock(TopicManagerRepository.class);
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager();
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager(any(String.class));
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager(any(Pair.class));
    veniceAdmin.setTopicManagerRepository(mockedTopicManageRepository);
    String storeName = TestUtils.getUniqueString("test-store");
    String pushJobId1 = "test-push-job-id-1";
    veniceAdmin.addStore(clusterName, storeName, "test-owner", KEY_SCHEMA, VALUE_SCHEMA);
    /**
     * Enable L/F and Active/Active replication
     */
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams()
        .setLeaderFollowerModel(true)
        .setActiveActiveReplicationEnabled(true));

    /**
     * Add version 1
     */
    veniceAdmin.addVersionAndTopicOnly(clusterName, storeName, pushJobId1, 1, 1,
        false, true, Version.PushType.BATCH, null, null, Optional.empty(), -1, 1);
    // Version 1 should exist.
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions().size(), 1);
    // A/A version level config should be true
    Assert.assertTrue(veniceAdmin.getStore(clusterName, storeName).getVersion(1).get().isActiveActiveReplicationEnabled());

    //set topic original topic manager back
    veniceAdmin.setTopicManagerRepository(originalTopicManagerRepository);
  }

  @Test
  public void testAddMetadataSchema() {
    String storeName = TestUtils.getUniqueString("aa_store");
    String recordSchemaStr = TestPushUtils.USER_SCHEMA_STRING_WITH_DEFAULT;
    int timestampMetadataVersionId = multiClusterConfig.getCommonConfig().getMetadataVersionId();
    Schema metadataSchema = MetadataSchemaAdapter.parse(recordSchemaStr, timestampMetadataVersionId);

    veniceAdmin.addStore(clusterName, storeName, storeOwner, KEY_SCHEMA, recordSchemaStr);
    veniceAdmin.addMetadataSchema(clusterName, storeName, 1,  timestampMetadataVersionId, metadataSchema.toString());
    Collection<MetadataSchemaEntry> metadataSchemas = veniceAdmin.getMetadataSchemas(clusterName, storeName);
    Assert.assertEquals(metadataSchemas.size(), 1);
    Assert.assertEquals(metadataSchemas.iterator().next().getSchema(), metadataSchema);
  }

  @Test
  public void testRepairStoreReplicationFactor() {
    String storeName = TestUtils.getUniqueString("test");
    veniceAdmin.addStore(clusterName, storeName, storeOwner, "\"string\"", "\"string\"");
    Store store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertTrue(store.getReplicationFactor() > 0, "The replication factor for a new store should be positive.");
    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setReplicationFactor(0));
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertEquals(store.getReplicationFactor(), 0, "The replication factor should be 0 after the update.");
    veniceAdmin.getVeniceHelixResource(clusterName).refresh();
    store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertTrue(store.getReplicationFactor() > 0, "The replication factor should be positive after the one-time repair.");
  }
}
