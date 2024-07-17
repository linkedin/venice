package com.linkedin.venice.controller.kafka.consumer;

import static com.linkedin.venice.ConfigKeys.ADMIN_CONSUMPTION_CYCLE_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.ADMIN_CONSUMPTION_MAX_WORKER_THREAD_POOL_SIZE;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteStore;
import com.linkedin.venice.controller.kafka.protocol.admin.DisableStoreRead;
import com.linkedin.venice.controller.kafka.protocol.admin.PauseStore;
import com.linkedin.venice.controller.kafka.protocol.admin.SchemaMeta;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.enums.SchemaType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
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
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
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

  /**
   * This test is flaky on slower hardware, with a short timeout ):
   */
  @Test(timeOut = TIMEOUT)
  public void testSkipMessageEndToEnd() throws ExecutionException, InterruptedException, IOException {
    try (
        VeniceTwoLayerMultiRegionMultiClusterWrapper venice =
            ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
                new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
                    .numberOfClusters(1)
                    .numberOfParentControllers(1)
                    .numberOfChildControllers(1)
                    .numberOfServers(1)
                    .numberOfRouters(1)
                    .replicationFactor(1)
                    .build());
        ControllerClient parentControllerClient = new ControllerClient(
            venice.getClusterNames()[0],
            venice.getParentControllers().get(0).getControllerUrl())) {
      String clusterName = venice.getClusterNames()[0];
      Admin admin = venice.getParentControllers().get(0).getVeniceAdmin();
      PubSubTopicRepository pubSubTopicRepository = admin.getPubSubTopicRepository();
      TopicManager topicManager = admin.getTopicManager();
      PubSubTopic adminTopic = pubSubTopicRepository.getTopic(AdminTopicUtils.getTopicNameFromClusterName(clusterName));
      topicManager.createTopic(adminTopic, 1, 1, true);
      String storeName = "test-store";
      PubSubBrokerWrapper pubSubBrokerWrapper = venice.getParentKafkaBrokerWrapper();
      try (
          PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
              pubSubBrokerWrapper.getPubSubClientsFactory().getProducerAdapterFactory();
          VeniceWriter<byte[], byte[], byte[]> writer =
              IntegrationTestPushUtils.getVeniceWriterFactory(pubSubBrokerWrapper, pubSubProducerAdapterFactory)
                  .createVeniceWriter(new VeniceWriterOptions.Builder(adminTopic.getName()).build())) {
        byte[] message = getStoreCreationMessage(clusterName, storeName, owner, "invalid_key_schema", valueSchema, 1);
        long badOffset = writer.put(new byte[0], message, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)
            .get()
            .getOffset();

        byte[] goodMessage = getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 2);
        writer.put(new byte[0], goodMessage, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

        Thread.sleep(5000); // Non-deterministic, but whatever. This should never fail.
        Assert.assertTrue(parentControllerClient.getStore(storeName).isError());

        parentControllerClient.skipAdminMessage(Long.toString(badOffset), false);
        TestUtils.waitForNonDeterministicAssertion(TIMEOUT * 3, TimeUnit.MILLISECONDS, () -> {
          Assert.assertFalse(parentControllerClient.getStore(storeName).isError());
        });
      }
    }
  }

  @Test(timeOut = 2 * TIMEOUT)
  public void testParallelAdminExecutionTasks() throws IOException, InterruptedException {
    int adminConsumptionMaxWorkerPoolSize = 3;

    Properties parentControllerProps = new Properties();
    parentControllerProps.put(ADMIN_CONSUMPTION_MAX_WORKER_THREAD_POOL_SIZE, adminConsumptionMaxWorkerPoolSize);
    parentControllerProps.put(ADMIN_CONSUMPTION_CYCLE_TIMEOUT_MS, 3000);

    try (
        VeniceTwoLayerMultiRegionMultiClusterWrapper venice =
            ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
                new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
                    .numberOfClusters(1)
                    .numberOfParentControllers(1)
                    .numberOfChildControllers(1)
                    .numberOfServers(1)
                    .numberOfRouters(1)
                    .replicationFactor(1)
                    .parentControllerProperties(parentControllerProps)
                    .build());
        ControllerClient parentControllerClient = new ControllerClient(
            venice.getClusterNames()[0],
            venice.getParentControllers().get(0).getControllerUrl())) {
      String clusterName = venice.getClusterNames()[0];
      Admin admin = venice.getParentControllers().get(0).getVeniceAdmin();
      PubSubTopicRepository pubSubTopicRepository = admin.getPubSubTopicRepository();
      TopicManager topicManager = admin.getTopicManager();
      PubSubTopic adminTopic = pubSubTopicRepository.getTopic(AdminTopicUtils.getTopicNameFromClusterName(clusterName));
      topicManager.createTopic(adminTopic, 1, 1, true);
      String storeName = "test-store";
      PubSubBrokerWrapper pubSubBrokerWrapper = venice.getParentKafkaBrokerWrapper();
      try (
          PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
              pubSubBrokerWrapper.getPubSubClientsFactory().getProducerAdapterFactory();
          VeniceWriter<byte[], byte[], byte[]> writer =
              IntegrationTestPushUtils.getVeniceWriterFactory(pubSubBrokerWrapper, pubSubProducerAdapterFactory)
                  .createVeniceWriter(new VeniceWriterOptions.Builder(adminTopic.getName()).build())) {
        int executionId = 1;
        byte[] goodMessage =
            getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, executionId);
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
          executionId++;
          byte[] valueSchemaMessage = getDisableWrite(clusterName, storeName, executionId);
          writer.put(new byte[0], valueSchemaMessage, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
        }

        // Store deletion need to disable read.
        Utils.sleep(5000);
        executionId++;
        byte[] valueSchemaMessage = getDisableRead(clusterName, storeName, executionId);
        writer.put(new byte[0], valueSchemaMessage, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

        // Create a new store to see if it is blocked by previous messages.
        String otherStoreName = "other-test-store";
        executionId++;
        byte[] otherStoreMessage =
            getStoreCreationMessage(clusterName, otherStoreName, owner, keySchema, valueSchema, executionId);
        writer.put(new byte[0], otherStoreMessage, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

        TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
          Assert.assertFalse(parentControllerClient.getStore(storeName).isError());
        });

        infiniteLockThread.interrupt(); // This will release the lock
        // Check this store is unblocked or not.
        executionId++;
        byte[] storeDeletionMessage = getStoreDeletionMessage(clusterName, storeName, executionId);
        writer.put(new byte[0], storeDeletionMessage, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
        TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
          Assert.assertTrue(parentControllerClient.getStore(storeName).isError());
        });
      }
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
    return adminOperationSerializer.serialize(adminMessage);
  }

  private byte[] getDisableWrite(String clusterName, String storeName, long executionId) {
    PauseStore pauseStore = (PauseStore) AdminMessageType.DISABLE_STORE_WRITE.getNewInstance();
    pauseStore.clusterName = clusterName;
    pauseStore.storeName = storeName;
    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.DISABLE_STORE_WRITE.getValue();
    adminMessage.payloadUnion = pauseStore;
    adminMessage.executionId = executionId;
    return adminOperationSerializer.serialize(adminMessage);
  }

  private byte[] getStoreCreationMessage(
      String clusterName,
      String storeName,
      String owner,
      String keySchema,
      String valueSchema,
      long executionId) {
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
    return adminOperationSerializer.serialize(adminMessage);
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
    return adminOperationSerializer.serialize(adminMessage);
  }
}
