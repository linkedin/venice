package com.linkedin.venice.controller.init;

import static com.linkedin.venice.controller.init.SystemStoreInitializationHelper.DEFAULT_KEY_SCHEMA_STR;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.VeniceConstants;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.Utils;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class SystemStoreInitializationHelperTest {
  @BeforeClass
  public void setUp() {
    SystemStoreInitializationHelper.setDelayBetweenStoreUpdateRetries(Duration.ofSeconds(1));
  }

  /**
   * Tests the case where this is the first time creating the system store
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testInitialSystemStoreSetup(boolean explicitlyProvidedKeySchema) {
    String clusterName = "testCluster";
    String expectedKeySchema = explicitlyProvidedKeySchema ? "\"string\"" : DEFAULT_KEY_SCHEMA_STR;
    Schema keySchema = explicitlyProvidedKeySchema ? AvroCompatibilityHelper.parse(expectedKeySchema) : null;
    AvroProtocolDefinition protocolDefinition = AvroProtocolDefinition.PUSH_JOB_DETAILS;
    String systemStoreName = VeniceSystemStoreUtils.getPushJobDetailsStoreName();
    Admin admin = mock(VeniceHelixAdmin.class);
    Function<Store, Boolean> updateStoreCheckSupplier = store -> !store.isHybrid();
    UpdateStoreQueryParams updateStoreQueryParams =
        new UpdateStoreQueryParams().setHybridOffsetLagThreshold(1000).setHybridRewindSeconds(100);
    VeniceControllerMultiClusterConfig multiClusterConfigs = mock(VeniceControllerMultiClusterConfig.class);

    Map<Integer, Schema> protocolSchemaMap = Utils.getAllSchemasFromResources(protocolDefinition);

    Version firstVersion = mock(Version.class);
    int versionNumber = 1;
    int partitionCount = 10;
    int replicationFactor = 3;
    doReturn(1).when(firstVersion).getNumber();

    Store storeForTest = mock(Store.class);

    Store storeForTestAfterUpdateStore = mock(Store.class);
    doReturn(true).when(storeForTestAfterUpdateStore).isHybrid();

    Store storeForTestAfterCreatingVersion = mock(Store.class);
    doReturn(true).when(storeForTestAfterCreatingVersion).isHybrid();
    doReturn(versionNumber).when(storeForTestAfterCreatingVersion).getCurrentVersion();
    doReturn(firstVersion).when(storeForTestAfterCreatingVersion).getVersion(versionNumber);
    doReturn(Collections.singletonList(firstVersion)).when(storeForTestAfterCreatingVersion).getVersions();

    doReturn(replicationFactor).when(admin).getReplicationFactor(clusterName, systemStoreName);
    doReturn(Collections.singleton(new SchemaEntry(1, protocolSchemaMap.get(1)))).when(admin)
        .getValueSchemas(clusterName, systemStoreName);
    doReturn(firstVersion).when(admin)
        .incrementVersionIdempotent(
            eq(clusterName),
            eq(systemStoreName),
            any(),
            eq(partitionCount),
            eq(replicationFactor));

    doReturn(null) // First time
        .doReturn(storeForTest) // After store is created
        .doReturn(storeForTestAfterUpdateStore) // After store is updated to hybrid
        .doReturn(storeForTestAfterCreatingVersion) // After version is created
        .when(admin)
        .getStore(clusterName, systemStoreName);

    VeniceControllerClusterConfig controllerConfig = mock(VeniceControllerClusterConfig.class);
    doReturn(controllerConfig).when(multiClusterConfigs).getControllerConfig(clusterName);
    doReturn(partitionCount).when(controllerConfig).getMinNumberOfPartitions();

    SystemStoreInitializationHelper.setupSystemStore(
        clusterName,
        systemStoreName,
        protocolDefinition,
        keySchema,
        updateStoreCheckSupplier,
        updateStoreQueryParams,
        admin,
        multiClusterConfigs);

    // getStore should be called at the beginning, after store creation, after store update, after version creation
    verify(admin, times(4)).getStore(clusterName, systemStoreName);

    verify(admin, times(1)).createStore(
        clusterName,
        systemStoreName,
        VeniceConstants.SYSTEM_STORE_OWNER,
        expectedKeySchema,
        protocolSchemaMap.get(1).toString(),
        true);

    verify(admin, times(1)).updateStore(clusterName, systemStoreName, updateStoreQueryParams);

    // First value schema should always get added during store creation
    verify(admin, never()).addValueSchema(
        clusterName,
        systemStoreName,
        protocolSchemaMap.get(1).toString(),
        1,
        DirectionalSchemaCompatibilityType.NONE);
    for (int i = 2; i <= protocolDefinition.getCurrentProtocolVersion(); i++) {
      verify(admin, times(1)).addValueSchema(
          clusterName,
          systemStoreName,
          protocolSchemaMap.get(i).toString(),
          i,
          DirectionalSchemaCompatibilityType.NONE);
    }

    verify(admin, times(1)).incrementVersionIdempotent(
        eq(clusterName),
        eq(systemStoreName),
        any(),
        eq(partitionCount),
        eq(replicationFactor));
  }

  /**
   * Test the case where this is the system store has been previously created, and we are evolving the value schemas
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testSystemStoreEvolveValueSchema(boolean explicitlyProvidedKeySchema) {
    String clusterName = "testCluster";
    String expectedKeySchema = explicitlyProvidedKeySchema ? "\"string\"" : DEFAULT_KEY_SCHEMA_STR;
    Schema keySchema = explicitlyProvidedKeySchema ? AvroCompatibilityHelper.parse(expectedKeySchema) : null;
    AvroProtocolDefinition protocolDefinition = AvroProtocolDefinition.PUSH_JOB_DETAILS;
    String systemStoreName = VeniceSystemStoreUtils.getPushJobDetailsStoreName();
    Admin admin = mock(VeniceHelixAdmin.class);
    Function<Store, Boolean> updateStoreCheckSupplier = store -> !store.isHybrid();
    UpdateStoreQueryParams updateStoreQueryParams =
        new UpdateStoreQueryParams().setHybridOffsetLagThreshold(1000).setHybridRewindSeconds(100);
    VeniceControllerMultiClusterConfig multiClusterConfigs = mock(VeniceControllerMultiClusterConfig.class);

    Map<Integer, Schema> protocolSchemaMap = Utils.getAllSchemasFromResources(protocolDefinition);

    Version firstVersion = mock(Version.class);
    int versionNumber = 1;
    doReturn(1).when(firstVersion).getNumber();

    Store storeToTest = mock(Store.class);
    doReturn(true).when(storeToTest).isHybrid();
    doReturn(versionNumber).when(storeToTest).getCurrentVersion();
    doReturn(firstVersion).when(storeToTest).getVersion(versionNumber);
    doReturn(Collections.singletonList(firstVersion)).when(storeToTest).getVersions();

    doReturn(storeToTest).when(admin).getStore(clusterName, systemStoreName);

    doReturn(new SchemaEntry(1, expectedKeySchema)).when(admin).getKeySchema(clusterName, systemStoreName);
    doReturn(Arrays.asList(new SchemaEntry(1, protocolSchemaMap.get(1)), new SchemaEntry(2, protocolSchemaMap.get(2))))
        .when(admin)
        .getValueSchemas(clusterName, systemStoreName);

    VeniceControllerClusterConfig controllerConfig = mock(VeniceControllerClusterConfig.class);
    doReturn(controllerConfig).when(multiClusterConfigs).getControllerConfig(clusterName);

    SystemStoreInitializationHelper.setupSystemStore(
        clusterName,
        systemStoreName,
        protocolDefinition,
        keySchema,
        updateStoreCheckSupplier,
        updateStoreQueryParams,
        admin,
        multiClusterConfigs);

    // getStore should be called only at the beginning
    verify(admin, times(1)).getStore(clusterName, systemStoreName);

    verify(admin, never()).createStore(
        clusterName,
        systemStoreName,
        VeniceConstants.SYSTEM_STORE_OWNER,
        expectedKeySchema,
        protocolSchemaMap.get(1).toString(),
        true);
    verify(admin, never()).updateStore(clusterName, systemStoreName, updateStoreQueryParams);

    // First two value schema should already be registered
    verify(admin, never()).addValueSchema(
        clusterName,
        systemStoreName,
        protocolSchemaMap.get(1).toString(),
        1,
        DirectionalSchemaCompatibilityType.NONE);
    verify(admin, never()).addValueSchema(
        clusterName,
        systemStoreName,
        protocolSchemaMap.get(2).toString(),
        2,
        DirectionalSchemaCompatibilityType.NONE);
    for (int i = 3; i <= protocolDefinition.getCurrentProtocolVersion(); i++) {
      verify(admin, times(1)).addValueSchema(
          clusterName,
          systemStoreName,
          protocolSchemaMap.get(i).toString(),
          i,
          DirectionalSchemaCompatibilityType.NONE);
    }

    verify(admin, never()).incrementVersionIdempotent(eq(clusterName), eq(systemStoreName), any(), anyInt(), anyInt());
  }
}
