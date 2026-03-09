package com.linkedin.venice.controller.kafka.protocol.enums;

import com.linkedin.venice.stats.dimensions.VeniceDimensionTestFixture;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;
import org.testng.annotations.Test;


public class AdminMessageTypeTest {
  @Test
  public void testDimensionInterface() {
    Map<AdminMessageType, String> expectedValues = CollectionUtils.<AdminMessageType, String>mapBuilder()
        .put(AdminMessageType.STORE_CREATION, "store_creation")
        .put(AdminMessageType.VALUE_SCHEMA_CREATION, "value_schema_creation")
        .put(AdminMessageType.DISABLE_STORE_WRITE, "disable_store_write")
        .put(AdminMessageType.ENABLE_STORE_WRITE, "enable_store_write")
        .put(AdminMessageType.KILL_OFFLINE_PUSH_JOB, "kill_offline_push_job")
        .put(AdminMessageType.DISABLE_STORE_READ, "disable_store_read")
        .put(AdminMessageType.ENABLE_STORE_READ, "enable_store_read")
        .put(AdminMessageType.DELETE_ALL_VERSIONS, "delete_all_versions")
        .put(AdminMessageType.SET_STORE_OWNER, "set_store_owner")
        .put(AdminMessageType.SET_STORE_PARTITION, "set_store_partition")
        .put(AdminMessageType.SET_STORE_CURRENT_VERSION, "set_store_current_version")
        .put(AdminMessageType.UPDATE_STORE, "update_store")
        .put(AdminMessageType.DELETE_STORE, "delete_store")
        .put(AdminMessageType.DELETE_OLD_VERSION, "delete_old_version")
        .put(AdminMessageType.MIGRATE_STORE, "migrate_store")
        .put(AdminMessageType.ABORT_MIGRATION, "abort_migration")
        .put(AdminMessageType.ADD_VERSION, "add_version")
        .put(AdminMessageType.DERIVED_SCHEMA_CREATION, "derived_schema_creation")
        .put(AdminMessageType.SUPERSET_SCHEMA_CREATION, "superset_schema_creation")
        .put(AdminMessageType.CONFIGURE_NATIVE_REPLICATION_FOR_CLUSTER, "configure_native_replication_for_cluster")
        .put(AdminMessageType.REPLICATION_METADATA_SCHEMA_CREATION, "replication_metadata_schema_creation")
        .put(
            AdminMessageType.CONFIGURE_ACTIVE_ACTIVE_REPLICATION_FOR_CLUSTER,
            "configure_active_active_replication_for_cluster")
        .put(AdminMessageType.CONFIGURE_INCREMENTAL_PUSH_FOR_CLUSTER, "configure_incremental_push_for_cluster")
        .put(AdminMessageType.META_SYSTEM_STORE_AUTO_CREATION_VALIDATION, "meta_system_store_auto_creation_validation")
        .put(
            AdminMessageType.PUSH_STATUS_SYSTEM_STORE_AUTO_CREATION_VALIDATION,
            "push_status_system_store_auto_creation_validation")
        .put(AdminMessageType.CREATE_STORAGE_PERSONA, "create_storage_persona")
        .put(AdminMessageType.DELETE_STORAGE_PERSONA, "delete_storage_persona")
        .put(AdminMessageType.UPDATE_STORAGE_PERSONA, "update_storage_persona")
        .put(AdminMessageType.DELETE_UNUSED_VALUE_SCHEMA, "delete_unused_value_schema")
        .put(AdminMessageType.ROLLBACK_CURRENT_VERSION, "rollback_current_version")
        .put(AdminMessageType.ROLLFORWARD_CURRENT_VERSION, "rollforward_current_version")
        .build();
    new VeniceDimensionTestFixture<>(
        AdminMessageType.class,
        VeniceMetricsDimensions.VENICE_ADMIN_MESSAGE_TYPE,
        expectedValues).assertAll();
  }
}
