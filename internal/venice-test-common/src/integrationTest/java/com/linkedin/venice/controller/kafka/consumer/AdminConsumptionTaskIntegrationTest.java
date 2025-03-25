package com.linkedin.venice.controller.kafka.consumer;

import static com.linkedin.venice.ConfigKeys.ADMIN_CONSUMPTION_CYCLE_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.ADMIN_CONSUMPTION_MAX_WORKER_THREAD_POOL_SIZE;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteStore;
import com.linkedin.venice.controller.kafka.protocol.admin.DisableStoreRead;
import com.linkedin.venice.controller.kafka.protocol.admin.PauseStore;
import com.linkedin.venice.controller.kafka.protocol.admin.SchemaMeta;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.enums.SchemaType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Slow test class, given fast priority
 */
@Test(priority = -5)
public class AdminConsumptionTaskIntegrationTest {
  private static final int TIMEOUT = 1 * Time.MS_PER_MINUTE;

  private final AdminOperationSerializer adminOperationSerializer = new AdminOperationSerializer();

  private static final String owner = "test_owner";
  private static final String keySchema = "\"string\"";
  private static final String valueSchema = "\"string\"";
  private static final int adminConsumptionMaxWorkerPoolSize = 3;

  private VeniceTwoLayerMultiRegionMultiClusterWrapper venice;
  private ControllerClient parentControllerClient;
  private VeniceWriter<byte[], byte[], byte[]> writer;
  private String clusterName;
  private int executionId = 0;

  @BeforeClass
  public void setUp() {
    Properties serverProperties = new Properties();
    serverProperties.setProperty(ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1));
    Properties parentControllerProps = new Properties();
    parentControllerProps.put(ADMIN_CONSUMPTION_MAX_WORKER_THREAD_POOL_SIZE, adminConsumptionMaxWorkerPoolSize);
    parentControllerProps.put(ADMIN_CONSUMPTION_CYCLE_TIMEOUT_MS, 3000);
    venice = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfClusters(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(1)
            .parentControllerProperties(parentControllerProps)
            .serverProperties(serverProperties)
            .build());

    parentControllerClient =
        new ControllerClient(venice.getClusterNames()[0], venice.getParentControllers().get(0).getControllerUrl());

    clusterName = venice.getClusterNames()[0];
    Admin admin = venice.getParentControllers().get(0).getVeniceAdmin();
    PubSubTopicRepository pubSubTopicRepository = admin.getPubSubTopicRepository();
    TopicManager topicManager = admin.getTopicManager();
    PubSubTopic adminTopic = pubSubTopicRepository.getTopic(AdminTopicUtils.getTopicNameFromClusterName(clusterName));
    topicManager.createTopic(adminTopic, 1, 1, true);
    PubSubBrokerWrapper pubSubBrokerWrapper = venice.getParentKafkaBrokerWrapper();

    PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
        pubSubBrokerWrapper.getPubSubClientsFactory().getProducerAdapterFactory();
    writer = IntegrationTestPushUtils.getVeniceWriterFactory(pubSubBrokerWrapper, pubSubProducerAdapterFactory)
        .createVeniceWriter(new VeniceWriterOptions.Builder(adminTopic.getName()).build());
  }

  @AfterClass
  public void cleanUp() {
    venice.close();
  }

  /**
   * This test is flaky on slower hardware, with a short timeout ):
   */
  @Test(timeOut = TIMEOUT)
  public void testSkipMessageEndToEnd() throws ExecutionException, InterruptedException, IOException {
    String storeName = Utils.getUniqueString("test-store");

    byte[] message = getStoreCreationMessage(
        clusterName,
        storeName,
        owner,
        "invalid_key_schema",
        valueSchema,
        nextExecutionId(),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    long badOffset = writer.put(new byte[0], message, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)
        .get()
        .getOffset();

    byte[] goodMessage = getStoreCreationMessage(
        clusterName,
        storeName,
        owner,
        keySchema,
        valueSchema,
        nextExecutionId(),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    writer.put(new byte[0], goodMessage, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertTrue(parentControllerClient.getStore(storeName).isError());
    });

    parentControllerClient.skipAdminMessage(Long.toString(badOffset), false);
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT * 3, TimeUnit.MILLISECONDS, () -> {
      Assert.assertFalse(parentControllerClient.getStore(storeName).isError());
    });
  }

  @Test(timeOut = 2 * TIMEOUT)
  public void testParallelAdminExecutionTasks() throws InterruptedException {
    String storeName = Utils.getUniqueString("test-store");
    byte[] goodMessage = getStoreCreationMessage(
        clusterName,
        storeName,
        owner,
        keySchema,
        valueSchema,
        executionId++,
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    writer.put(new byte[0], goodMessage, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertFalse(parentControllerClient.getStore(storeName).isError());
    });

    // Spin up a thread to occupy the store write lock to simulate the blocking admin execution task thread.
    CountDownLatch lockOccupyThreadStartedSignal = new CountDownLatch(1);
    Runnable infiniteLockOccupy = getRunnable(venice, storeName, lockOccupyThreadStartedSignal);
    Thread infiniteLockThread = new Thread(infiniteLockOccupy, "infiniteLockOccupy: " + storeName);
    infiniteLockThread.start();
    Assert.assertTrue(lockOccupyThreadStartedSignal.await(5, TimeUnit.SECONDS));

    // Here we wait here to send every operation to let each consumer pool has at most one admin operation from
    // this store, as the waiting time of 5 seconds > ADMIN_CONSUMPTION_CYCLE_TIMEOUT_MS setting.
    for (int i = 0; i < adminConsumptionMaxWorkerPoolSize; i++) {
      Utils.sleep(5000);
      byte[] valueSchemaMessage = getDisableWrite(clusterName, storeName, nextExecutionId());
      writer.put(new byte[0], valueSchemaMessage, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    }

    // Store deletion need to disable read.
    Utils.sleep(5000);
    byte[] valueSchemaMessage = getDisableRead(clusterName, storeName, nextExecutionId());
    writer.put(new byte[0], valueSchemaMessage, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    // Create a new store to see if it is blocked by previous messages.
    String otherStoreName = Utils.getUniqueString("test-store");
    byte[] otherStoreMessage = getStoreCreationMessage(
        clusterName,
        otherStoreName,
        owner,
        keySchema,
        valueSchema,
        nextExecutionId(),
        AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    writer.put(new byte[0], otherStoreMessage, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertFalse(parentControllerClient.getStore(storeName).isError());
    });

    infiniteLockThread.interrupt(); // This will release the lock
    // Check this store is unblocked or not.
    byte[] storeDeletionMessage = getStoreDeletionMessage(clusterName, storeName, nextExecutionId());
    writer.put(new byte[0], storeDeletionMessage, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertTrue(parentControllerClient.getStore(storeName).isError());
    });
  }

  @Test(timeOut = 2 * TIMEOUT)
  public void testUpdateAdminOperationVersion() {
    Long currentVersion = -1L;
    Long newVersion = 18L;

    String clusterName = venice.getClusterNames()[0];

    // Get the parent controller
    VeniceControllerWrapper controller = venice.getParentControllers().get(0);
    Admin admin = controller.getVeniceAdmin();

    AdminConsumerService adminConsumerService = controller.getAdminConsumerServiceByCluster(clusterName);

    // Setup the original metadata
    adminConsumerService.updateAdminOperationProtocolVersion(clusterName, currentVersion);

    // Verify that the original metadata is correct
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Map<String, Long> adminTopicMetadata = admin.getAdminTopicMetadata(clusterName, Optional.empty());
      Assert.assertTrue(adminTopicMetadata.containsKey("adminOperationProtocolVersion"));
      Assert.assertEquals(adminTopicMetadata.get("adminOperationProtocolVersion"), currentVersion);
    });

    // Update the admin operation version
    admin.updateAdminOperationProtocolVersion(clusterName, newVersion);

    // Verify the admin operation metadata version is updated
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Map<String, Long> adminTopicMetadata = admin.getAdminTopicMetadata(clusterName, Optional.empty());
      Assert.assertTrue(adminTopicMetadata.containsKey("adminOperationProtocolVersion"));
      Assert.assertEquals(adminTopicMetadata.get("adminOperationProtocolVersion"), newVersion);
    });
  }

  @Test(timeOut = 2 * TIMEOUT)
  public void testAdminConsumptionTaskWithSpecificWriterId() {
    for (int i = 1; i <= 5; i++) {
      // Use a specific version to test the serialization and deserialization of admin operation.
      int writerSchemaId = AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION - i;
      String storeName = Utils.getUniqueString("test-store");

      // Create store
      byte[] storeCreationMessage = getStoreCreationMessage(
          clusterName,
          storeName,
          owner,
          keySchema,
          valueSchema,
          nextExecutionId(),
          writerSchemaId);
      writer.put(new byte[0], storeCreationMessage, writerSchemaId);

      TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        Assert.assertFalse(parentControllerClient.getStore(storeName).isError());
      });

      // Update store
      byte[] updateStoreMessage =
          getStoreUpdateMessage(clusterName, storeName, owner, nextExecutionId(), writerSchemaId);
      writer.put(new byte[0], updateStoreMessage, writerSchemaId);

      TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        Assert.assertFalse(parentControllerClient.getStore(storeName).isError());
        StoreInfo storeInfo = parentControllerClient.getStore(storeName).getStore();
        Assert.assertTrue(storeInfo.isEnableStoreWrites());
        Assert.assertTrue(storeInfo.isEnableStoreReads());
      });
    }
  }

  private Runnable getRunnable(
      VeniceTwoLayerMultiRegionMultiClusterWrapper venice,
      String storeName,
      CountDownLatch latch) {
    String clusterName = venice.getClusterNames()[0];
    VeniceControllerWrapper parentController = venice.getParentControllers().get(0);
    Admin admin = parentController.getVeniceAdmin();
    return () -> {
      try (AutoCloseableLock ignore =
          admin.getHelixVeniceClusterResources(clusterName).getClusterLockManager().createStoreWriteLock(storeName)) {
        latch.countDown();
        while (true) {
          Thread.sleep(10000);
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    };
  }

  private byte[] getDisableRead(String clusterName, String storeName, long executionId) {
    DisableStoreRead disableStoreRead = (DisableStoreRead) AdminMessageType.DISABLE_STORE_READ.getNewInstance();
    disableStoreRead.clusterName = clusterName;
    disableStoreRead.storeName = storeName;
    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.DISABLE_STORE_READ.getValue();
    adminMessage.payloadUnion = disableStoreRead;
    adminMessage.executionId = executionId;
    return adminOperationSerializer
        .serialize(adminMessage, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
  }

  private byte[] getDisableWrite(String clusterName, String storeName, long executionId) {
    PauseStore pauseStore = (PauseStore) AdminMessageType.DISABLE_STORE_WRITE.getNewInstance();
    pauseStore.clusterName = clusterName;
    pauseStore.storeName = storeName;
    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.DISABLE_STORE_WRITE.getValue();
    adminMessage.payloadUnion = pauseStore;
    adminMessage.executionId = executionId;
    return adminOperationSerializer
        .serialize(adminMessage, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
  }

  private byte[] getStoreCreationMessage(
      String clusterName,
      String storeName,
      String owner,
      String keySchema,
      String valueSchema,
      long executionId,
      int writerSchemaId) {
    StoreCreation storeCreation = (StoreCreation) AdminMessageType.STORE_CREATION.getNewInstance();
    storeCreation.clusterName = clusterName;
    storeCreation.storeName = storeName;
    storeCreation.owner = owner;
    storeCreation.keySchema = new SchemaMeta();
    storeCreation.keySchema.definition = keySchema;
    storeCreation.keySchema.schemaType = SchemaType.AVRO_1_4.getValue();
    storeCreation.valueSchema = new SchemaMeta();
    storeCreation.valueSchema.definition = valueSchema;
    storeCreation.valueSchema.schemaType = SchemaType.AVRO_1_4.getValue();
    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.STORE_CREATION.getValue();
    adminMessage.payloadUnion = storeCreation;
    adminMessage.executionId = executionId;
    return adminOperationSerializer.serialize(adminMessage, writerSchemaId);
  }

  private byte[] getStoreDeletionMessage(String clusterName, String storeName, long executionId) {
    DeleteStore deleteStore = (DeleteStore) AdminMessageType.DELETE_STORE.getNewInstance();
    deleteStore.clusterName = clusterName;
    deleteStore.storeName = storeName;
    // Tell each prod colo the largest used version number in corp to make it consistent.
    deleteStore.largestUsedVersionNumber = 0;
    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.DELETE_STORE.getValue();
    adminMessage.payloadUnion = deleteStore;
    adminMessage.executionId = executionId;
    return adminOperationSerializer
        .serialize(adminMessage, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
  }

  private byte[] getStoreUpdateMessage(
      String clusterName,
      String storeName,
      String owner,
      long executionId,
      int writerSchemaId) {
    UpdateStore updateStore = (UpdateStore) AdminMessageType.UPDATE_STORE.getNewInstance();
    updateStore.clusterName = clusterName;
    updateStore.storeName = storeName;
    updateStore.owner = owner;
    updateStore.partitionNum = 3;
    updateStore.currentVersion = AdminConsumptionTask.IGNORED_CURRENT_VERSION;
    updateStore.enableReads = true;
    updateStore.enableWrites = true;
    updateStore.replicateAllConfigs = true;
    updateStore.updatedConfigsList = Collections.emptyList();
    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.UPDATE_STORE.getValue();
    adminMessage.payloadUnion = updateStore;
    adminMessage.executionId = executionId;
    return adminOperationSerializer.serialize(adminMessage, writerSchemaId);
  }

  private int nextExecutionId() {
    return executionId++;
  }
}
