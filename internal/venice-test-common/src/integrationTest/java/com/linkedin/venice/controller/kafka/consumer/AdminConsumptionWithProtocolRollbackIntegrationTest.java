package com.linkedin.venice.controller.kafka.consumer;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.utils.AvroSchemaUtils.*;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.FieldBuilder;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.SchemaMeta;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.enums.SchemaType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
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
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.ConfigCommonUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test for AdminConsumptionTask to verify that it can handle protocol rollback scenario.
 * Scenario:
 * 1. Parent has a new schema and register it to schema system store. We roll-out this new schema to all controllers.
 *    ProtocolAutoDetectionService will mark good protocol version to use the new schema.
 * 2. An AdminOperation message (UpdateStore) is serialized with the new schema and sent to admin topic.
 * 3. Oops, we need to rollback to previous version with old schema :(.
 * 4. Perform rollback in parent and child controllers.
 * 5. However, the message in admin topic is having the new schema id, which will cause deserialization failure if
 *    the AdminConsumptionTask only relies on local schema cache, which then blocks further admin consumption.
 * 6. To fix this, AdminConsumptionTask should be able to fetch the new schema from schema system store and proceed to consume the message.
 */
@Test
public class AdminConsumptionWithProtocolRollbackIntegrationTest {
  private static final int TIMEOUT = 1 * Time.MS_PER_MINUTE;

  private AdminOperationSerializer adminOperationSerializer;

  private static final String owner = "test_owner";
  private static final String keySchema = "\"string\"";
  private static final String valueSchema = "\"string\"";
  private static final int adminConsumptionMaxWorkerPoolSize = 3;

  private VeniceTwoLayerMultiRegionMultiClusterWrapper venice;
  private ControllerClient parentControllerClient;
  private VeniceParentHelixAdmin parentHelixAdmin;
  private VeniceWriter<byte[], byte[], byte[]> writer;
  private String clusterName;
  private int executionId = 0;

  @BeforeClass
  public void setUp() {
    Properties serverProperties = new Properties();
    Properties parentControllerProps = new Properties();
    parentControllerProps.put(ADMIN_CONSUMPTION_MAX_WORKER_THREAD_POOL_SIZE, adminConsumptionMaxWorkerPoolSize);
    parentControllerProps.put(ADMIN_CONSUMPTION_CYCLE_TIMEOUT_MS, 3000);
    parentControllerProps.put(CONTROLLER_ADMIN_OPERATION_SYSTEM_STORE_ENABLED, true);

    Properties childControllerProps = new Properties();
    childControllerProps.put(CONTROLLER_ADMIN_OPERATION_SYSTEM_STORE_ENABLED, true);
    venice = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfClusters(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(1)
            .parentControllerProperties(parentControllerProps)
            .childControllerProperties(childControllerProps)
            .serverProperties(serverProperties)
            .build());

    VeniceControllerWrapper parentController = venice.getParentControllers().get(0);
    parentControllerClient = new ControllerClient(venice.getClusterNames()[0], parentController.getControllerUrl());
    clusterName = venice.getClusterNames()[0];
    parentHelixAdmin = (VeniceParentHelixAdmin) parentController.getVeniceAdmin();
    PubSubTopicRepository pubSubTopicRepository = parentHelixAdmin.getPubSubTopicRepository();
    TopicManager topicManager = parentHelixAdmin.getTopicManager();
    PubSubTopic adminTopic = pubSubTopicRepository.getTopic(AdminTopicUtils.getTopicNameFromClusterName(clusterName));
    topicManager.createTopic(adminTopic, 1, 1, true);
    PubSubBrokerWrapper pubSubBrokerWrapper = venice.getParentKafkaBrokerWrapper();

    PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
        pubSubBrokerWrapper.getPubSubClientsFactory().getProducerAdapterFactory();
    writer = IntegrationTestPushUtils.getVeniceWriterFactory(pubSubBrokerWrapper, pubSubProducerAdapterFactory)
        .createVeniceWriter(new VeniceWriterOptions.Builder(adminTopic.getName()).build());
    adminOperationSerializer = parentHelixAdmin.getAdminOperationSerializer();
  }

  @AfterClass
  public void cleanUp() {
    venice.close();
  }

  @Test
  public void testAdminConsumptionTaskWithProtocolRollback() {
    // Create a new schema and register it
    int newSchemaId = createNewSchemaAndRegister(AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    // Create unique store name
    String storeName = Utils.getUniqueString("test-store");

    // Create store
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
      Assert.assertFalse(parentControllerClient.getStore(storeName).isError());
    });

    // Update store command, which is serialized with new schema
    byte[] updateStoreMessage = getStoreUpdateMessage(clusterName, storeName, owner, nextExecutionId(), newSchemaId);

    // Rollback to old schema
    adminOperationSerializer.removeSchema(newSchemaId);

    // Send in the serialized message with new schema id
    // This message should be consumed successfully since the AdminConsumptionTask should be able to
    // fetch the new schema from schema system store
    writer.put(new byte[0], updateStoreMessage, newSchemaId);

    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      Assert.assertFalse(parentControllerClient.getStore(storeName).isError());
      StoreInfo storeInfo = parentControllerClient.getStore(storeName).getStore();
      Assert.assertTrue(storeInfo.isEnableStoreWrites());
      Assert.assertTrue(storeInfo.isEnableStoreReads());
    });

  }

  private void addSchemaToSchemaSystemStore(int schemaId, Schema schema) {
    String adminOperationSchemaStoreName = AvroProtocolDefinition.ADMIN_OPERATION.getSystemStoreName();
    TestUtils.waitForNonDeterministicAssertion(TIMEOUT * 5, TimeUnit.MILLISECONDS, () -> {
      MultiSchemaResponse multiSchemaResponse = parentControllerClient.getAllValueSchema(adminOperationSchemaStoreName);
      Assert.assertFalse(multiSchemaResponse.isError(), "Failed to get schemas: " + multiSchemaResponse.getError());
      Assert.assertNotNull(multiSchemaResponse.getSchemas(), "Schemas array is null");
      Assert.assertEquals(multiSchemaResponse.getSchemas().length, schemaId - 1);
    });

    String clusterName = parentControllerClient.getStore(adminOperationSchemaStoreName).getCluster();

    parentHelixAdmin.getVeniceHelixAdmin()
        .addValueSchema(
            clusterName,
            adminOperationSchemaStoreName,
            schema.toString(),
            schemaId,
            DirectionalSchemaCompatibilityType.NONE);

    TestUtils.waitForNonDeterministicAssertion(TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      String fetchedSchema =
          parentControllerClient.getValueSchema(adminOperationSchemaStoreName, schemaId).getSchemaStr();
      assert fetchedSchema.equals(schema.toString());
    });
  }

  /**
   * Create a new schema by adding a new field to the original schema
   * and register it to schema system store with new schema id
   */
  private int createNewSchemaAndRegister(int originalSchemaId) {
    Schema schema = adminOperationSerializer.getSchema(originalSchemaId);

    int newSchemaId = originalSchemaId + 1;
    Schema newSchema = createNewSchema(schema);
    // We use the same schema and by-pass compatibility check for testing purpose
    addSchemaToSchemaSystemStore(newSchemaId, newSchema);

    adminOperationSerializer.addSchema(newSchemaId, newSchema);
    return newSchemaId;
  }

  /**
   * Create a new schema by adding a new field to the original schema
   */
  private Schema createNewSchema(final Schema originalSchema) {
    if (originalSchema == null) {
      throw new IllegalArgumentException("Original schema cannot be null");
    }

    // Create a new record schema with defensive copies
    final Schema newSchema = Schema.createRecord(
        originalSchema.getName(),
        originalSchema.getDoc(),
        originalSchema.getNamespace(),
        originalSchema.isError());

    // Deep copy fields
    final List<Schema.Field> newFields = new ArrayList<>();
    for (Schema.Field field: originalSchema.getFields()) {
      // Defensive copy of schema (Avro schemas are immutable, so this is safe)
      FieldBuilder newField = deepCopySchemaFieldWithoutFieldProps(field);
      copyFieldProperties(newField, field);
      newFields.add(newField.build());
    }

    // Add extra field
    final Schema.Field testField = AvroCompatibilityHelper
        .createSchemaField("testField", Schema.create(Schema.Type.INT), "Documentation for testField", 0);
    newFields.add(testField);

    // Set fields using defensive copy
    newSchema.setFields(Collections.unmodifiableList(new ArrayList<>(newFields)));

    return newSchema;
  }

  /**
   * Create a StoreCreation admin message
   */
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

  /**
   * Create an UpdateStore admin message
   */
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
    updateStore.storeLifecycleHooks = Collections.emptyList();
    updateStore.blobTransferInServerEnabled = ConfigCommonUtils.ActivationState.NOT_SPECIFIED.name();
    updateStore.keyUrnFields = Collections.emptyList();
    return adminOperationSerializer.serialize(adminMessage, writerSchemaId);
  }

  private int nextExecutionId() {
    return executionId++;
  }

  private static FieldBuilder deepCopySchemaFieldWithoutFieldProps(Schema.Field field) {
    FieldBuilder fieldBuilder = AvroCompatibilityHelper.newField(null)
        .setName(field.name())
        .setSchema(field.schema())
        .setDoc(field.doc())
        .setOrder(field.order());
    // set default as AvroCompatibilityHelper builder might drop defaults if there is type mismatch
    if (field.hasDefaultValue()) {
      fieldBuilder.setDefault(getFieldDefault(field));
    }
    return fieldBuilder;
  }

  private static void copyFieldProperties(FieldBuilder fieldBuilder, Schema.Field field) {
    AvroCompatibilityHelper.getAllPropNames(field).forEach(k -> {
      String propValue = AvroCompatibilityHelper.getFieldPropAsJsonString(field, k);
      if (propValue != null) {
        fieldBuilder.addProp(k, propValue);
      }
    });
  }
}
