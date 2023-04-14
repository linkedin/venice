package com.linkedin.venice.controller.kafka.consumer;

import static com.linkedin.venice.kafka.TopicManager.DEFAULT_KAFKA_OPERATION_TIMEOUT_MS;

import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.SchemaMeta;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.enums.SchemaType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.integration.utils.PubSubBrokerConfigs;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerCreateOptions;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.io.IOException;
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

  private String clusterName = Utils.getUniqueString("test-cluster");
  private final AdminOperationSerializer adminOperationSerializer = new AdminOperationSerializer();

  private static final String owner = "test_owner";
  private static final String keySchema = "\"string\"";
  private static final String valueSchema = "\"string\"";

  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  /**
   * This test is flaky on slower hardware, with a short timeout ):
   */
  @Test(timeOut = TIMEOUT)
  public void testSkipMessageEndToEnd() throws ExecutionException, InterruptedException, IOException {
    try (ZkServerWrapper zkServer = ServiceFactory.getZkServer();
        PubSubBrokerWrapper kafka =
            ServiceFactory.getPubSubBroker(new PubSubBrokerConfigs.Builder().setZkWrapper(zkServer).build());
        TopicManager topicManager = IntegrationTestPushUtils
            .getTopicManagerRepo(DEFAULT_KAFKA_OPERATION_TIMEOUT_MS, 100, 0l, kafka.getAddress(), pubSubTopicRepository)
            .getTopicManager()) {
      PubSubTopic adminTopic = pubSubTopicRepository.getTopic(AdminTopicUtils.getTopicNameFromClusterName(clusterName));
      topicManager.createTopic(adminTopic, 1, 1, true);
      String storeName = "test-store";
      try (
          VeniceControllerWrapper controller = ServiceFactory
              .getVeniceController(new VeniceControllerCreateOptions.Builder(clusterName, zkServer, kafka).build());
          VeniceWriter<byte[], byte[], byte[]> writer = TestUtils.getVeniceWriterFactory(kafka.getAddress())
              .createVeniceWriter(new VeniceWriterOptions.Builder(adminTopic.getName()).build())) {
        byte[] message = getStoreCreationMessage(clusterName, storeName, owner, "invalid_key_schema", valueSchema, 1);
        long badOffset = writer.put(new byte[0], message, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION)
            .get()
            .getOffset();

        byte[] goodMessage = getStoreCreationMessage(clusterName, storeName, owner, keySchema, valueSchema, 2);
        writer.put(new byte[0], goodMessage, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

        Thread.sleep(5000); // Non-deterministic, but whatever. This should never fail.
        Assert.assertFalse(controller.getVeniceAdmin().hasStore(clusterName, storeName));

        try (ControllerClient controllerClient = new ControllerClient(clusterName, controller.getControllerUrl())) {
          controllerClient.skipAdminMessage(Long.toString(badOffset), false);
        }
        TestUtils.waitForNonDeterministicAssertion(TIMEOUT * 3, TimeUnit.MILLISECONDS, () -> {
          Assert.assertTrue(controller.getVeniceAdmin().hasStore(clusterName, storeName));
        });
      }
    }
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
}
