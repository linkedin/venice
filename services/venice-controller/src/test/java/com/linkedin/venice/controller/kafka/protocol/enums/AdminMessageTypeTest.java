package com.linkedin.venice.controller.kafka.protocol.enums;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;


public class AdminMessageTypeTest {
  @Test
  public void testVeniceDimensionInterface() {
    Map<AdminMessageType, String> expectedDimensionValues = new HashMap<>();
    expectedDimensionValues.put(AdminMessageType.STORE_CREATION, "store_creation");
    expectedDimensionValues.put(AdminMessageType.VALUE_SCHEMA_CREATION, "value_schema_creation");
    expectedDimensionValues.put(AdminMessageType.DISABLE_STORE_WRITE, "disable_store_write");
    expectedDimensionValues.put(AdminMessageType.ENABLE_STORE_WRITE, "enable_store_write");
    expectedDimensionValues.put(AdminMessageType.KILL_OFFLINE_PUSH_JOB, "kill_offline_push_job");
    expectedDimensionValues.put(AdminMessageType.DISABLE_STORE_READ, "disable_store_read");
    expectedDimensionValues.put(AdminMessageType.ENABLE_STORE_READ, "enable_store_read");
    expectedDimensionValues.put(AdminMessageType.DELETE_ALL_VERSIONS, "delete_all_versions");
    expectedDimensionValues.put(AdminMessageType.SET_STORE_OWNER, "set_store_owner");
    expectedDimensionValues.put(AdminMessageType.SET_STORE_PARTITION, "set_store_partition");
    expectedDimensionValues.put(AdminMessageType.SET_STORE_CURRENT_VERSION, "set_store_current_version");
    expectedDimensionValues.put(AdminMessageType.UPDATE_STORE, "update_store");
    expectedDimensionValues.put(AdminMessageType.DELETE_STORE, "delete_store");
    expectedDimensionValues.put(AdminMessageType.DELETE_OLD_VERSION, "delete_old_version");
    expectedDimensionValues.put(AdminMessageType.MIGRATE_STORE, "migrate_store");
    expectedDimensionValues.put(AdminMessageType.ABORT_MIGRATION, "abort_migration");
    expectedDimensionValues.put(AdminMessageType.ADD_VERSION, "add_version");
    expectedDimensionValues.put(AdminMessageType.DERIVED_SCHEMA_CREATION, "derived_schema_creation");
    expectedDimensionValues.put(AdminMessageType.SUPERSET_SCHEMA_CREATION, "superset_schema_creation");
    expectedDimensionValues
        .put(AdminMessageType.CONFIGURE_NATIVE_REPLICATION_FOR_CLUSTER, "configure_native_replication_for_cluster");
    expectedDimensionValues
        .put(AdminMessageType.REPLICATION_METADATA_SCHEMA_CREATION, "replication_metadata_schema_creation");
    expectedDimensionValues.put(
        AdminMessageType.CONFIGURE_ACTIVE_ACTIVE_REPLICATION_FOR_CLUSTER,
        "configure_active_active_replication_for_cluster");
    expectedDimensionValues
        .put(AdminMessageType.CONFIGURE_INCREMENTAL_PUSH_FOR_CLUSTER, "configure_incremental_push_for_cluster");
    expectedDimensionValues
        .put(AdminMessageType.META_SYSTEM_STORE_AUTO_CREATION_VALIDATION, "meta_system_store_auto_creation_validation");
    expectedDimensionValues.put(
        AdminMessageType.PUSH_STATUS_SYSTEM_STORE_AUTO_CREATION_VALIDATION,
        "push_status_system_store_auto_creation_validation");
    expectedDimensionValues.put(AdminMessageType.CREATE_STORAGE_PERSONA, "create_storage_persona");
    expectedDimensionValues.put(AdminMessageType.DELETE_STORAGE_PERSONA, "delete_storage_persona");
    expectedDimensionValues.put(AdminMessageType.UPDATE_STORAGE_PERSONA, "update_storage_persona");
    expectedDimensionValues.put(AdminMessageType.DELETE_UNUSED_VALUE_SCHEMA, "delete_unused_value_schema");
    expectedDimensionValues.put(AdminMessageType.ROLLBACK_CURRENT_VERSION, "rollback_current_version");
    expectedDimensionValues.put(AdminMessageType.ROLLFORWARD_CURRENT_VERSION, "rollforward_current_version");

    assertEquals(
        AdminMessageType.values().length,
        expectedDimensionValues.size(),
        "New AdminMessageType values were added but not included in this test");

    for (AdminMessageType type: AdminMessageType.values()) {
      assertEquals(
          type.getDimensionName(),
          VeniceMetricsDimensions.VENICE_ADMIN_MESSAGE_TYPE,
          "Unexpected dimension name for " + type.name());
      String expectedValue = expectedDimensionValues.get(type);
      assertNotNull(expectedValue, "No expected dimension value for " + type.name());
      assertNotNull(type.getDimensionValue(), "Dimension value should not be null for " + type.name());
      assertEquals(type.getDimensionValue(), expectedValue, "Unexpected dimension value for " + type.name());
    }
  }
}
