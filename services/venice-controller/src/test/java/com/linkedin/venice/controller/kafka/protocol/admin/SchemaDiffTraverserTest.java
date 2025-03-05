package com.linkedin.venice.controller.kafka.protocol.admin;

import static org.testng.Assert.*;

import com.linkedin.avroutil1.compatibility.shaded.org.apache.commons.lang3.function.TriFunction;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controller.kafka.protocol.serializer.SchemaDiffTraverser;
import com.linkedin.venice.utils.Pair;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class SchemaDiffTraverserTest {
  @Test
  public void testTraverse() {
    // Create an AdminOperation object with latest version
    UpdateStore updateStore = (UpdateStore) AdminMessageType.UPDATE_STORE.getNewInstance();
    updateStore.clusterName = "clusterName";
    updateStore.storeName = "storeName";
    updateStore.owner = "owner";
    updateStore.partitionNum = 20;
    updateStore.currentVersion = 1;
    updateStore.enableReads = true;
    updateStore.enableWrites = true;
    updateStore.replicateAllConfigs = true;
    updateStore.updatedConfigsList = Collections.emptyList();
    // Purposely set to true. This field doesn't exist in v74, will be dropped during serialization
    // Default value of this field is False.
    updateStore.separateRealTimeTopicEnabled = true;

    HybridStoreConfigRecord hybridStoreConfig = new HybridStoreConfigRecord();
    hybridStoreConfig.rewindTimeInSeconds = 123L;
    hybridStoreConfig.offsetLagThresholdToGoOnline = 1000L;
    hybridStoreConfig.producerTimestampLagThresholdToGoOnlineInSeconds = 300L;
    // Default value is empty string
    hybridStoreConfig.realTimeTopicName = "AAAA";
    updateStore.hybridStoreConfig = hybridStoreConfig;

    // Default value of this field is 60
    updateStore.targetSwapRegionWaitTime = 10;
    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.UPDATE_STORE.getValue();
    adminMessage.payloadUnion = updateStore;
    adminMessage.executionId = 1;
    AdminOperationSerializer adminOperationSerializer = new AdminOperationSerializer();
    Schema targetSchema = adminOperationSerializer.getSchema(74);
    Schema currentSchema = adminOperationSerializer.getSchema(84);
    SchemaDiffTraverser schemaDiffTraverser = new SchemaDiffTraverser();
    AtomicBoolean flag = new AtomicBoolean(false);
    ArrayList<String> fieldName = new ArrayList<>();

    TriFunction<Object, String, Pair<Schema.Field, Schema.Field>, Object> filter =
        schemaDiffTraverser.usingNewSemanticCheck(flag, fieldName);

    schemaDiffTraverser.traverse(adminMessage, currentSchema, targetSchema, "", filter);

    // Check if the flag is set to true
    assertTrue(flag.get(), "The flag should be set to true");
    // Check if the field name is as expected
    ArrayList<String> expectedFieldName = new ArrayList<>();
    expectedFieldName.add("realTimeTopicName");
    expectedFieldName.add("separateRealTimeTopicEnabled");
    expectedFieldName.add("targetSwapRegionWaitTime");
    assertEquals(fieldName, expectedFieldName, "The field name should be as expected");
  }
}
