package com.linkedin.venice.controller.util;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.ACTIVE_ACTIVE_REPLICATION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.INCREMENTAL_PUSH_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NATIVE_REPLICATION_SOURCE_FABRIC;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PARTITION_COUNT;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.WRITE_COMPUTATION_ENABLED;
import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_GB;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.HelixVeniceClusterResources;
import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controller.VeniceControllerConfig;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.PartitionerSchemaMismatchException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.helix.StoragePersonaRepository;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.ETLStoreConfigImpl;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.persona.StoragePersona;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;
import org.apache.avro.Schema;
import org.apache.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.Test;


public class UpdateStoreUtilsTest {
  private static final String VALUE_FIELD_NAME = "int_field";
  private static final String SECOND_VALUE_FIELD_NAME = "opt_int_field";
  private static final String VALUE_SCHEMA_V1_STR = "{\n" + "\"type\": \"record\",\n"
      + "\"name\": \"TestValueSchema\",\n" + "\"namespace\": \"com.linkedin.venice.fastclient.schema\",\n"
      + "\"fields\": [\n" + "  {\"name\": \"" + VALUE_FIELD_NAME + "\", \"type\": \"int\", \"default\": 10}]\n" + "}";
  private static final String VALUE_SCHEMA_V2_STR =
      "{\n" + "\"type\": \"record\",\n" + "\"name\": \"TestValueSchema\",\n"
          + "\"namespace\": \"com.linkedin.venice.fastclient.schema\",\n" + "\"fields\": [\n" + "{\"name\": \""
          + SECOND_VALUE_FIELD_NAME + "\", \"type\": [\"null\", \"int\"], \"default\": null}]\n" + "}";

  @Test
  public void testMergeNewHybridConfigValuesToOldStore() {
    String storeName = Utils.getUniqueString("storeName");
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    assertFalse(store.isHybrid());

    Optional<Long> rewind = Optional.of(123L);
    Optional<Long> lagOffset = Optional.of(1500L);
    Optional<Long> timeLag = Optional.of(300L);
    Optional<DataReplicationPolicy> dataReplicationPolicy = Optional.of(DataReplicationPolicy.AGGREGATE);
    Optional<BufferReplayPolicy> bufferReplayPolicy = Optional.of(BufferReplayPolicy.REWIND_FROM_EOP);
    HybridStoreConfig hybridStoreConfig = UpdateStoreUtils.mergeNewSettingsIntoOldHybridStoreConfig(
        store,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
    Assert.assertNull(
        hybridStoreConfig,
        "passing empty optionals and a non-hybrid store should generate a null hybrid config");

    hybridStoreConfig = UpdateStoreUtils.mergeNewSettingsIntoOldHybridStoreConfig(
        store,
        rewind,
        lagOffset,
        timeLag,
        dataReplicationPolicy,
        bufferReplayPolicy);
    Assert.assertNotNull(hybridStoreConfig, "specifying rewind and lagOffset should generate a valid hybrid config");
    Assert.assertEquals(hybridStoreConfig.getRewindTimeInSeconds(), 123L);
    Assert.assertEquals(hybridStoreConfig.getOffsetLagThresholdToGoOnline(), 1500L);
    Assert.assertEquals(hybridStoreConfig.getProducerTimestampLagThresholdToGoOnlineInSeconds(), 300L);
    Assert.assertEquals(hybridStoreConfig.getDataReplicationPolicy(), DataReplicationPolicy.AGGREGATE);

    // It's okay that time lag threshold or data replication policy is not specified
    hybridStoreConfig = UpdateStoreUtils.mergeNewSettingsIntoOldHybridStoreConfig(
        store,
        rewind,
        lagOffset,
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
    Assert.assertNotNull(hybridStoreConfig, "specifying rewind and lagOffset should generate a valid hybrid config");
    Assert.assertEquals(hybridStoreConfig.getRewindTimeInSeconds(), 123L);
    Assert.assertEquals(hybridStoreConfig.getOffsetLagThresholdToGoOnline(), 1500L);
    Assert.assertEquals(
        hybridStoreConfig.getProducerTimestampLagThresholdToGoOnlineInSeconds(),
        HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD);
    Assert.assertEquals(hybridStoreConfig.getDataReplicationPolicy(), DataReplicationPolicy.NON_AGGREGATE);
  }

  @Test
  public void testIsInferredStoreUpdateAllowed() {
    String clusterName = "clusterName";
    String storeName = "storeName";
    Admin mockAdmin = mock(Admin.class);

    assertFalse(
        UpdateStoreUtils.isInferredStoreUpdateAllowed(mockAdmin, VeniceSystemStoreUtils.getMetaStoreName(storeName)));
    assertFalse(
        UpdateStoreUtils
            .isInferredStoreUpdateAllowed(mockAdmin, VeniceSystemStoreUtils.getDaVinciPushStatusStoreName(storeName)));
    assertFalse(
        UpdateStoreUtils.isInferredStoreUpdateAllowed(
            mockAdmin,
            VeniceSystemStoreUtils.getParticipantStoreNameForCluster(clusterName)));
    assertFalse(
        UpdateStoreUtils.isInferredStoreUpdateAllowed(
            mockAdmin,
            VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE.getZkSharedStoreName()));

    doReturn(false).when(mockAdmin).isPrimary();
    assertFalse(UpdateStoreUtils.isInferredStoreUpdateAllowed(mockAdmin, storeName));

    Admin mockChildAdmin = mock(VeniceHelixAdmin.class);
    doReturn(true).when(mockChildAdmin).isPrimary();
    doReturn(false).when(mockChildAdmin).isParent();
    assertTrue(UpdateStoreUtils.isInferredStoreUpdateAllowed(mockChildAdmin, storeName));

    Admin mockParentAdmin = mock(VeniceParentHelixAdmin.class);
    doReturn(true).when(mockParentAdmin).isPrimary();
    doReturn(true).when(mockParentAdmin).isParent();
    assertTrue(UpdateStoreUtils.isInferredStoreUpdateAllowed(mockParentAdmin, storeName));
  }

  @Test
  public void testUpdateInferredConfig() {
    String storeName = "storeName";
    Admin admin = mock(Admin.class);
    Store store = mock(Store.class);
    Set<CharSequence> updatedConfigSet = new HashSet<>();
    final AtomicBoolean updaterInvoked = new AtomicBoolean(false);

    doReturn(storeName).when(store).getName();
    doReturn(true).when(admin).isPrimary();
    doReturn(false).when(admin).isParent();

    // Config previously updated. Will not update again.
    updatedConfigSet.add("key1");
    updaterInvoked.set(false);
    UpdateStoreUtils.updateInferredConfig(admin, store, "key1", updatedConfigSet, () -> updaterInvoked.set(true));
    assertFalse(updaterInvoked.get());
    assertTrue(updatedConfigSet.contains("key1"));
    assertEquals(updatedConfigSet.size(), 1);

    // Config not updated previously. Will update it.
    updatedConfigSet.clear();
    updaterInvoked.set(false);
    UpdateStoreUtils.updateInferredConfig(admin, store, "key1", updatedConfigSet, () -> updaterInvoked.set(true));
    assertTrue(updaterInvoked.get());
    assertTrue(updatedConfigSet.contains("key1"));
    assertEquals(updatedConfigSet.size(), 1);

    // Config not updated previously. Will not update it for system stores.
    updatedConfigSet.clear();
    updaterInvoked.set(false);
    doReturn(VeniceSystemStoreUtils.getParticipantStoreNameForCluster(storeName)).when(store).getName();
    UpdateStoreUtils.updateInferredConfig(admin, store, "key1", updatedConfigSet, () -> updaterInvoked.set(true));
    assertFalse(updaterInvoked.get());
    assertTrue(updatedConfigSet.isEmpty());
  }

  @Test
  public void testUpdateInferredConfigsForHybridToBatch() {
    String storeName = "storeName";
    Admin admin = mock(Admin.class);
    Store store = mock(Store.class);
    VeniceControllerClusterConfig clusterConfig = mock(VeniceControllerClusterConfig.class);

    doReturn(true).when(admin).isPrimary();
    doReturn(false).when(admin).isParent();

    Set<CharSequence> updatedConfigSet = new HashSet<>();
    doReturn(storeName).when(store).getName();
    doReturn("dc-batch").when(clusterConfig).getNativeReplicationSourceFabricAsDefaultForBatchOnly();
    doReturn("dc-hybrid").when(clusterConfig).getNativeReplicationSourceFabricAsDefaultForHybrid();

    UpdateStoreUtils.updateInferredConfigsForHybridToBatch(admin, clusterConfig, store, updatedConfigSet);

    verify(store).setIncrementalPushEnabled(false);
    verify(store).setNativeReplicationSourceFabric("dc-batch");
    verify(store).setActiveActiveReplicationEnabled(false);

    assertEquals(updatedConfigSet.size(), 3);
    assertTrue(updatedConfigSet.contains(INCREMENTAL_PUSH_ENABLED));
    assertTrue(updatedConfigSet.contains(NATIVE_REPLICATION_SOURCE_FABRIC));
    assertTrue(updatedConfigSet.contains(ACTIVE_ACTIVE_REPLICATION_ENABLED));
  }

  @Test
  public void testUpdateInferredConfigsForBatchToHybrid() {
    String clusterName = "clusterName";
    String storeName = "storeName";
    Admin admin = mock(Admin.class);
    Store store = mock(Store.class);
    VeniceControllerClusterConfig clusterConfig = mock(VeniceControllerClusterConfig.class);

    doReturn(true).when(admin).isPrimary();
    doReturn(false).when(admin).isParent();

    doReturn(storeName).when(store).getName();
    doReturn(false).when(store).isSystemStore();
    doReturn(0).when(store).getPartitionCount();
    doReturn(10 * BYTES_PER_GB).when(store).getStorageQuotaInByte();

    Set<CharSequence> updatedConfigSet = new HashSet<>();
    doReturn(clusterName).when(clusterConfig).getClusterName();
    doReturn("dc-batch").when(clusterConfig).getNativeReplicationSourceFabricAsDefaultForBatchOnly();
    doReturn("dc-hybrid").when(clusterConfig).getNativeReplicationSourceFabricAsDefaultForHybrid();
    doReturn(false).when(clusterConfig).isActiveActiveReplicationEnabledAsDefaultForHybrid();
    doReturn(1 * BYTES_PER_GB).when(clusterConfig).getPartitionSize();
    doReturn(3).when(clusterConfig).getMinNumberOfPartitionsForHybrid();
    doReturn(100).when(clusterConfig).getMaxNumberOfPartitions();
    doReturn(1).when(clusterConfig).getPartitionCountRoundUpSize();
    doReturn(true).when(clusterConfig).isEnablePartialUpdateForHybridNonActiveActiveUserStores();

    doReturn(Arrays.asList(new SchemaEntry(1, VALUE_SCHEMA_V1_STR), new SchemaEntry(2, VALUE_SCHEMA_V2_STR)))
        .when(admin)
        .getValueSchemas(clusterName, storeName);

    UpdateStoreUtils.updateInferredConfigsForBatchToHybrid(admin, clusterConfig, store, updatedConfigSet);

    verify(store).setNativeReplicationSourceFabric("dc-hybrid");
    verify(store).setActiveActiveReplicationEnabled(false);
    verify(store).setPartitionCount(10);
    verify(store).setWriteComputationEnabled(true);

    assertEquals(updatedConfigSet.size(), 4);
    assertTrue(updatedConfigSet.contains(NATIVE_REPLICATION_SOURCE_FABRIC));
    assertTrue(updatedConfigSet.contains(ACTIVE_ACTIVE_REPLICATION_ENABLED));
    assertTrue(updatedConfigSet.contains(PARTITION_COUNT));
    assertTrue(updatedConfigSet.contains(WRITE_COMPUTATION_ENABLED));

    // Update schemas should only be generated in dry-run mode and not registered yet.
    verify(admin, never()).addDerivedSchema(eq(clusterName), eq(storeName), anyInt(), anyString());
  }

  @Test
  public void testIsIncrementalPushEnabled() {
    HybridStoreConfig nonHybridConfig =
        new HybridStoreConfigImpl(-1, -1, -1, DataReplicationPolicy.AGGREGATE, BufferReplayPolicy.REWIND_FROM_EOP);
    HybridStoreConfig hybridConfigWithNonAggregateDRP = new HybridStoreConfigImpl(
        100,
        1000,
        -1,
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP);
    HybridStoreConfig hybridConfigWithAggregateDRP =
        new HybridStoreConfigImpl(100, 1000, -1, DataReplicationPolicy.AGGREGATE, BufferReplayPolicy.REWIND_FROM_EOP);
    HybridStoreConfig hybridConfigWithActiveActiveDRP = new HybridStoreConfigImpl(
        100,
        1000,
        -1,
        DataReplicationPolicy.ACTIVE_ACTIVE,
        BufferReplayPolicy.REWIND_FROM_EOP);
    HybridStoreConfig hybridConfigWithNoneDRP =
        new HybridStoreConfigImpl(100, 1000, -1, DataReplicationPolicy.NONE, BufferReplayPolicy.REWIND_FROM_EOP);

    // In single-region mode, any hybrid store should have incremental push enabled.
    assertFalse(UpdateStoreUtils.isIncrementalPushEnabled(false, null));
    assertFalse(UpdateStoreUtils.isIncrementalPushEnabled(false, nonHybridConfig));
    assertTrue(UpdateStoreUtils.isIncrementalPushEnabled(false, hybridConfigWithNonAggregateDRP));
    assertTrue(UpdateStoreUtils.isIncrementalPushEnabled(false, hybridConfigWithAggregateDRP));
    assertTrue(UpdateStoreUtils.isIncrementalPushEnabled(false, hybridConfigWithActiveActiveDRP));
    assertTrue(UpdateStoreUtils.isIncrementalPushEnabled(false, hybridConfigWithNoneDRP));

    // In multi-region mode, hybrid stores with NON_AGGREGATE DataReplicationPolicy should not have incremental push
    // enabled.
    assertFalse(UpdateStoreUtils.isIncrementalPushEnabled(true, null));
    assertFalse(UpdateStoreUtils.isIncrementalPushEnabled(true, nonHybridConfig));
    assertFalse(UpdateStoreUtils.isIncrementalPushEnabled(true, hybridConfigWithNonAggregateDRP));
    assertTrue(UpdateStoreUtils.isIncrementalPushEnabled(true, hybridConfigWithAggregateDRP));
    assertTrue(UpdateStoreUtils.isIncrementalPushEnabled(true, hybridConfigWithActiveActiveDRP));
    assertTrue(UpdateStoreUtils.isIncrementalPushEnabled(true, hybridConfigWithNoneDRP));
  }

  @Test
  public void testValidateStoreConfigs() {
    String clusterName = "clusterName";
    String storeName = "storeName";
    Admin admin = mock(Admin.class);
    Store store = mock(Store.class);
    VeniceControllerMultiClusterConfig multiClusterConfigs = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerConfig controllerConfig = mock(VeniceControllerConfig.class);

    doReturn(multiClusterConfigs).when(admin).getMultiClusterConfigs();
    doReturn(controllerConfig).when(multiClusterConfigs).getControllerConfig(clusterName);

    // Batch-only + incremental push is not allowed
    doReturn(storeName).when(store).getName();
    doReturn(false).when(store).isHybrid();
    doReturn(true).when(store).isIncrementalPushEnabled();
    VeniceHttpException e1 =
        expectThrows(VeniceHttpException.class, () -> UpdateStoreUtils.validateStoreConfigs(admin, clusterName, store));
    assertEquals(e1.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
    assertEquals(e1.getErrorType(), ErrorType.INVALID_CONFIG);
    assertTrue(e1.getMessage().contains("Incremental push is only supported for hybrid stores"));

    reset(store);

    // Batch-only + write compute is not allowed
    doReturn(storeName).when(store).getName();
    doReturn(false).when(store).isHybrid();
    doReturn(true).when(store).isWriteComputationEnabled();
    VeniceHttpException e2 =
        expectThrows(VeniceHttpException.class, () -> UpdateStoreUtils.validateStoreConfigs(admin, clusterName, store));
    assertEquals(e2.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
    assertEquals(e2.getErrorType(), ErrorType.INVALID_CONFIG);
    assertTrue(e2.getMessage().contains("Write computation is only supported for hybrid stores"));

    reset(store);

    // Hybrid store cannot have negative rewind time config
    doReturn(storeName).when(store).getName();
    doReturn(true).when(store).isHybrid();
    doReturn(
        new HybridStoreConfigImpl(-1, 100, -1, DataReplicationPolicy.NON_AGGREGATE, BufferReplayPolicy.REWIND_FROM_EOP))
            .when(store)
            .getHybridStoreConfig();
    VeniceHttpException e3 =
        expectThrows(VeniceHttpException.class, () -> UpdateStoreUtils.validateStoreConfigs(admin, clusterName, store));
    assertEquals(e3.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
    assertEquals(e3.getErrorType(), ErrorType.INVALID_CONFIG);
    assertTrue(e3.getMessage().contains("Rewind time cannot be negative for a hybrid store"));

    reset(store);

    // Hybrid store cannot have negative offset lag and negative producer time lag thresholds
    doReturn(storeName).when(store).getName();
    doReturn(true).when(store).isHybrid();
    doReturn(
        new HybridStoreConfigImpl(100, -1, -1, DataReplicationPolicy.NON_AGGREGATE, BufferReplayPolicy.REWIND_FROM_EOP))
            .when(store)
            .getHybridStoreConfig();
    VeniceHttpException e4 =
        expectThrows(VeniceHttpException.class, () -> UpdateStoreUtils.validateStoreConfigs(admin, clusterName, store));
    assertEquals(e4.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
    assertEquals(e4.getErrorType(), ErrorType.INVALID_CONFIG);
    assertTrue(
        e4.getMessage()
            .contains(
                "Both offset lag threshold and producer timestamp lag threshold cannot be negative for a hybrid store"));

    reset(store);

    // Incremental push + NON_AGGREGATE DRP is not supported in multi-region mode
    doReturn(true).when(controllerConfig).isMultiRegion();
    doReturn(storeName).when(store).getName();
    doReturn(true).when(store).isHybrid();
    doReturn(true).when(store).isIncrementalPushEnabled();
    doReturn(
        new HybridStoreConfigImpl(
            100,
            100,
            -1,
            DataReplicationPolicy.NON_AGGREGATE,
            BufferReplayPolicy.REWIND_FROM_EOP)).when(store).getHybridStoreConfig();
    VeniceHttpException e5 =
        expectThrows(VeniceHttpException.class, () -> UpdateStoreUtils.validateStoreConfigs(admin, clusterName, store));
    assertEquals(e5.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
    assertEquals(e5.getErrorType(), ErrorType.INVALID_CONFIG);
    assertTrue(
        e5.getMessage()
            .contains(
                "Incremental push is not supported for hybrid stores with non-aggregate data replication policy"));

    reset(controllerConfig);
    reset(store);

    // Incremental push + NON_AGGREGATE DRP is supported in single-region mode
    doReturn(false).when(controllerConfig).isMultiRegion();
    doReturn(storeName).when(store).getName();
    doReturn(true).when(store).isHybrid();
    doReturn(true).when(store).isIncrementalPushEnabled();
    doReturn(
        new HybridStoreConfigImpl(
            100,
            100,
            -1,
            DataReplicationPolicy.NON_AGGREGATE,
            BufferReplayPolicy.REWIND_FROM_EOP)).when(store).getHybridStoreConfig();
    doReturn(new PartitionerConfigImpl()).when(store).getPartitionerConfig();
    doReturn(new SchemaEntry(1, VALUE_SCHEMA_V1_STR)).when(admin).getKeySchema(clusterName, storeName);
    doReturn(SchemaData.INVALID_VALUE_SCHEMA_ID).when(store).getLatestSuperSetValueSchemaId();
    UpdateStoreUtils.validateStoreConfigs(admin, clusterName, store);

    reset(controllerConfig);
    reset(store);

    // ACTIVE_ACTIVE DRP is only supported when activeActiveReplicationEnabled = true
    doReturn(storeName).when(store).getName();
    doReturn(true).when(store).isHybrid();
    doReturn(false).when(store).isActiveActiveReplicationEnabled();
    doReturn(
        new HybridStoreConfigImpl(
            100,
            -1,
            100,
            DataReplicationPolicy.ACTIVE_ACTIVE,
            BufferReplayPolicy.REWIND_FROM_EOP)).when(store).getHybridStoreConfig();
    VeniceHttpException e6 =
        expectThrows(VeniceHttpException.class, () -> UpdateStoreUtils.validateStoreConfigs(admin, clusterName, store));
    assertEquals(e6.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
    assertEquals(e6.getErrorType(), ErrorType.INVALID_CONFIG);
    assertTrue(
        e6.getMessage()
            .contains(
                "Data replication policy ACTIVE_ACTIVE is only supported for hybrid stores with active-active replication enabled"));

    reset(controllerConfig);
    reset(store);

    // Storage quota can not be less than 0
    doReturn(storeName).when(store).getName();
    doReturn(-5L).when(store).getStorageQuotaInByte();
    VeniceHttpException e7 =
        expectThrows(VeniceHttpException.class, () -> UpdateStoreUtils.validateStoreConfigs(admin, clusterName, store));
    assertEquals(e7.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
    assertEquals(e7.getErrorType(), ErrorType.INVALID_CONFIG);
    assertTrue(e7.getMessage().contains("Storage quota can not be less than 0"));

    reset(controllerConfig);
    reset(store);

    // Storage quota can be -1. Special value for unlimited quota
    doReturn(storeName).when(store).getName();
    doReturn(-1L).when(store).getStorageQuotaInByte();
    doReturn(new PartitionerConfigImpl()).when(store).getPartitionerConfig();
    doReturn(new SchemaEntry(1, VALUE_SCHEMA_V1_STR)).when(admin).getKeySchema(clusterName, storeName);
    doReturn(SchemaData.INVALID_VALUE_SCHEMA_ID).when(store).getLatestSuperSetValueSchemaId();
    UpdateStoreUtils.validateStoreConfigs(admin, clusterName, store);

    reset(controllerConfig);
    reset(store);

    // Read quota can not be less than 0
    doReturn(storeName).when(store).getName();
    doReturn(-5L).when(store).getReadQuotaInCU();
    VeniceHttpException e8 =
        expectThrows(VeniceHttpException.class, () -> UpdateStoreUtils.validateStoreConfigs(admin, clusterName, store));
    assertEquals(e8.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
    assertEquals(e8.getErrorType(), ErrorType.INVALID_CONFIG);
    assertTrue(e8.getMessage().contains("Read quota can not be less than 0"));

    reset(controllerConfig);
    reset(store);

    // Active-active replication is only supported for stores that also have native replication
    doReturn(storeName).when(store).getName();
    doReturn(false).when(store).isNativeReplicationEnabled();
    doReturn(true).when(store).isActiveActiveReplicationEnabled();
    VeniceHttpException e9 =
        expectThrows(VeniceHttpException.class, () -> UpdateStoreUtils.validateStoreConfigs(admin, clusterName, store));
    assertEquals(e9.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
    assertEquals(e9.getErrorType(), ErrorType.INVALID_CONFIG);
    assertTrue(
        e9.getMessage()
            .contains(
                "Active/Active Replication cannot be enabled for store " + store.getName()
                    + " since Native Replication is not enabled on it."));

    reset(controllerConfig);
    reset(store);

    // Partitioner Config cannot be null
    doReturn(storeName).when(store).getName();
    doReturn(null).when(store).getPartitionerConfig();
    VeniceHttpException e10 =
        expectThrows(VeniceHttpException.class, () -> UpdateStoreUtils.validateStoreConfigs(admin, clusterName, store));
    assertEquals(e10.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
    assertEquals(e10.getErrorType(), ErrorType.INVALID_CONFIG);
    assertTrue(e10.getMessage().contains("Partitioner Config cannot be null"));

    reset(controllerConfig);
    reset(store);

    // Active-Active is not supported when amplification factor is more than 1
    doReturn(storeName).when(store).getName();
    doReturn(new PartitionerConfigImpl(DefaultVenicePartitioner.class.getName(), new HashMap<>(), 10)).when(store)
        .getPartitionerConfig();
    doReturn(true).when(store).isNativeReplicationEnabled();
    doReturn(true).when(store).isActiveActiveReplicationEnabled();
    VeniceHttpException e11 =
        expectThrows(VeniceHttpException.class, () -> UpdateStoreUtils.validateStoreConfigs(admin, clusterName, store));
    assertEquals(e11.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
    assertEquals(e11.getErrorType(), ErrorType.INVALID_CONFIG);
    assertTrue(
        e11.getMessage()
            .contains(
                "Active-active replication or write computation is not supported for stores with amplification factor > 1"));

    reset(controllerConfig);
    reset(store);

    // Write-compute is not supported when amplification factor is more than 1
    doReturn(storeName).when(store).getName();
    doReturn(true).when(store).isHybrid();
    doReturn(
        new HybridStoreConfigImpl(
            100,
            100,
            -1,
            DataReplicationPolicy.NON_AGGREGATE,
            BufferReplayPolicy.REWIND_FROM_EOP)).when(store).getHybridStoreConfig();
    doReturn(new PartitionerConfigImpl(DefaultVenicePartitioner.class.getName(), new HashMap<>(), 10)).when(store)
        .getPartitionerConfig();
    doReturn(true).when(store).isWriteComputationEnabled();
    VeniceHttpException e12 =
        expectThrows(VeniceHttpException.class, () -> UpdateStoreUtils.validateStoreConfigs(admin, clusterName, store));
    assertEquals(e12.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
    assertEquals(e12.getErrorType(), ErrorType.INVALID_CONFIG);
    assertTrue(
        e12.getMessage()
            .contains(
                "Active-active replication or write computation is not supported for stores with amplification factor > 1"));

    reset(controllerConfig);
    reset(store);

    // Verify the updated partitionerConfig can be built - partitioner doesn't exist
    doReturn(storeName).when(store).getName();
    doReturn(new PartitionerConfigImpl("com.linkedin.venice.InvalidPartitioner", new HashMap<>(), 10)).when(store)
        .getPartitionerConfig();
    VeniceHttpException e13 =
        expectThrows(VeniceHttpException.class, () -> UpdateStoreUtils.validateStoreConfigs(admin, clusterName, store));
    assertEquals(e13.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
    assertEquals(e13.getErrorType(), ErrorType.INVALID_CONFIG);
    assertTrue(
        e13.getMessage()
            .contains(
                "Partitioner Configs invalid, please verify that partitioner configs like classpath and parameters are correct!"));

    reset(controllerConfig);
    reset(store);

    // Verify the updated partitionerConfig can be built - schema is not supported by partitioner
    doReturn(storeName).when(store).getName();
    doReturn(
        new PartitionerConfigImpl(
            PickyVenicePartitioner.class.getName(),
            Collections.singletonMap(PickyVenicePartitioner.SCHEMA_VALID, "false"),
            10)).when(store).getPartitionerConfig();
    VeniceHttpException e14 =
        expectThrows(VeniceHttpException.class, () -> UpdateStoreUtils.validateStoreConfigs(admin, clusterName, store));
    assertEquals(e14.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
    assertEquals(e14.getErrorType(), ErrorType.INVALID_SCHEMA);
    assertTrue(e14.getMessage().contains("Schema is not valid"));

    reset(controllerConfig);
    reset(store);

    // Validate if the latest superset schema id is an existing value schema
    doReturn(storeName).when(store).getName();
    doReturn(new PartitionerConfigImpl()).when(store).getPartitionerConfig();
    doReturn(new SchemaEntry(1, VALUE_SCHEMA_V1_STR)).when(admin).getKeySchema(clusterName, storeName);
    doReturn(null).when(admin).getValueSchema(clusterName, storeName, 10);
    doReturn(10).when(store).getLatestSuperSetValueSchemaId();
    VeniceHttpException e15 =
        expectThrows(VeniceHttpException.class, () -> UpdateStoreUtils.validateStoreConfigs(admin, clusterName, store));
    assertEquals(e15.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
    assertEquals(e15.getErrorType(), ErrorType.INVALID_CONFIG);
    assertTrue(e15.getMessage().contains("Unknown value schema id: 10 in store: storeName"));

    reset(controllerConfig);
    reset(store);

    // Max compaction lag >= Min compaction lag
    doReturn(storeName).when(store).getName();
    doReturn(new PartitionerConfigImpl()).when(store).getPartitionerConfig();
    doReturn(SchemaData.INVALID_VALUE_SCHEMA_ID).when(store).getLatestSuperSetValueSchemaId();
    doReturn(10L).when(store).getMaxCompactionLagSeconds();
    doReturn(100L).when(store).getMinCompactionLagSeconds();
    VeniceHttpException e16 =
        expectThrows(VeniceHttpException.class, () -> UpdateStoreUtils.validateStoreConfigs(admin, clusterName, store));
    assertEquals(e16.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
    assertEquals(e16.getErrorType(), ErrorType.INVALID_CONFIG);
    assertTrue(
        e16.getMessage()
            .contains(
                "Store's max compaction lag seconds: 10 shouldn't be smaller than store's min compaction lag seconds: 100"));

    reset(controllerConfig);
    reset(store);

    // ETL Proxy user must be set if ETL is enabled for current or future version
    doReturn(storeName).when(store).getName();
    doReturn(new PartitionerConfigImpl()).when(store).getPartitionerConfig();
    doReturn(SchemaData.INVALID_VALUE_SCHEMA_ID).when(store).getLatestSuperSetValueSchemaId();
    doReturn(new ETLStoreConfigImpl("", true, false)).when(store).getEtlStoreConfig();
    VeniceHttpException e17 =
        expectThrows(VeniceHttpException.class, () -> UpdateStoreUtils.validateStoreConfigs(admin, clusterName, store));
    assertEquals(e17.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
    assertEquals(e17.getErrorType(), ErrorType.INVALID_CONFIG);
    assertTrue(
        e17.getMessage().contains("Cannot enable ETL for this store because etled user proxy account is not set"));

    reset(controllerConfig);
    reset(store);
  }

  @Test
  public void testValidateStorePartitionCountUpdate() {
    String clusterName = "clusterName";
    String storeName = "storeName";

    Admin admin = mock(Admin.class);
    VeniceControllerMultiClusterConfig multiClusterConfigs = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerClusterConfig clusterConfig = mock(VeniceControllerClusterConfig.class);
    HelixVeniceClusterResources clusterResources = mock(HelixVeniceClusterResources.class);
    TopicManager topicManager = mock(TopicManager.class);
    PubSubTopicRepository topicRepository = mock(PubSubTopicRepository.class);
    PubSubTopic rtTopic = mock(PubSubTopic.class);

    doReturn(false).when(admin).isParent();
    doReturn(topicManager).when(admin).getTopicManager();
    doReturn(topicRepository).when(admin).getPubSubTopicRepository();
    doReturn(rtTopic).when(topicRepository).getTopic(Version.composeRealTimeTopic(storeName));

    Store originalStore = mock(Store.class);
    Store updatedStore = mock(Store.class);

    doReturn(3).when(clusterConfig).getMinNumberOfPartitions();
    doReturn(100).when(clusterConfig).getMaxNumberOfPartitions();

    doReturn(clusterResources).when(admin).getHelixVeniceClusterResources(clusterName);
    doReturn(clusterConfig).when(clusterResources).getConfig();

    doReturn(storeName).when(originalStore).getName();
    doReturn(storeName).when(updatedStore).getName();

    // Negative partition count is not allowed
    doReturn(-1).when(updatedStore).getPartitionCount();
    VeniceHttpException e1 = expectThrows(
        VeniceHttpException.class,
        () -> UpdateStoreUtils
            .validateStorePartitionCountUpdate(admin, multiClusterConfigs, clusterName, originalStore, updatedStore));
    assertEquals(e1.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
    assertEquals(e1.getErrorType(), ErrorType.INVALID_CONFIG);

    // Hybrid store with partition count = 0
    doReturn(true).when(updatedStore).isHybrid();
    doReturn(0).when(updatedStore).getPartitionCount();
    VeniceHttpException e2 = expectThrows(
        VeniceHttpException.class,
        () -> UpdateStoreUtils
            .validateStorePartitionCountUpdate(admin, multiClusterConfigs, clusterName, originalStore, updatedStore));
    assertEquals(e2.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
    assertEquals(e2.getErrorType(), ErrorType.INVALID_CONFIG);

    // Partition count cannot be less than min partition count
    doReturn(true).when(updatedStore).isHybrid();
    doReturn(1).when(updatedStore).getPartitionCount();
    VeniceHttpException e3 = expectThrows(
        VeniceHttpException.class,
        () -> UpdateStoreUtils
            .validateStorePartitionCountUpdate(admin, multiClusterConfigs, clusterName, originalStore, updatedStore));
    assertEquals(e3.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
    assertEquals(e3.getErrorType(), ErrorType.INVALID_CONFIG);

    // Partition count cannot be greater than max partition count
    doReturn(false).when(updatedStore).isHybrid();
    doReturn(1000).when(updatedStore).getPartitionCount();
    VeniceHttpException e4 = expectThrows(
        VeniceHttpException.class,
        () -> UpdateStoreUtils
            .validateStorePartitionCountUpdate(admin, multiClusterConfigs, clusterName, originalStore, updatedStore));
    assertEquals(e4.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
    assertEquals(e4.getErrorType(), ErrorType.INVALID_CONFIG);

    // Partition count change for hybrid stores is not allowed
    doReturn(true).when(originalStore).isHybrid();
    doReturn(true).when(updatedStore).isHybrid();
    doReturn(10).when(originalStore).getPartitionCount();
    doReturn(20).when(updatedStore).getPartitionCount();
    VeniceHttpException e5 = expectThrows(
        VeniceHttpException.class,
        () -> UpdateStoreUtils
            .validateStorePartitionCountUpdate(admin, multiClusterConfigs, clusterName, originalStore, updatedStore));
    assertEquals(e5.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
    assertEquals(e5.getErrorType(), ErrorType.INVALID_CONFIG);

    // Partition count update is allowed if RT topic doesn't exist
    doReturn(true).when(originalStore).isHybrid();
    doReturn(true).when(updatedStore).isHybrid();
    doReturn(10).when(originalStore).getPartitionCount();
    doReturn(10).when(updatedStore).getPartitionCount();
    doReturn(false).when(topicManager).containsTopic(rtTopic);
    UpdateStoreUtils
        .validateStorePartitionCountUpdate(admin, multiClusterConfigs, clusterName, originalStore, updatedStore);

    // Partition count update is allowed if RT topic exists and partition count matches the store's partition count
    doReturn(true).when(originalStore).isHybrid();
    doReturn(true).when(updatedStore).isHybrid();
    doReturn(10).when(originalStore).getPartitionCount();
    doReturn(10).when(updatedStore).getPartitionCount();
    doReturn(true).when(topicManager).containsTopic(rtTopic);
    doReturn(10).when(topicManager).getPartitionCount(rtTopic);
    UpdateStoreUtils
        .validateStorePartitionCountUpdate(admin, multiClusterConfigs, clusterName, originalStore, updatedStore);

    // Partition count update is not allowed if RT topic exists and partition count is different from the store's
    // partition count
    doReturn(true).when(originalStore).isHybrid();
    doReturn(true).when(updatedStore).isHybrid();
    doReturn(10).when(originalStore).getPartitionCount();
    doReturn(10).when(updatedStore).getPartitionCount();
    doReturn(true).when(topicManager).containsTopic(rtTopic);
    doReturn(20).when(topicManager).getPartitionCount(rtTopic);
    VeniceHttpException e6 = expectThrows(
        VeniceHttpException.class,
        () -> UpdateStoreUtils
            .validateStorePartitionCountUpdate(admin, multiClusterConfigs, clusterName, originalStore, updatedStore));
    assertEquals(e6.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
    assertEquals(e6.getErrorType(), ErrorType.INVALID_CONFIG);

    // Partition count change for batch stores is allowed
    doReturn(true).when(originalStore).isHybrid();
    doReturn(false).when(updatedStore).isHybrid();
    doReturn(10).when(originalStore).getPartitionCount();
    doReturn(20).when(updatedStore).getPartitionCount();
    // No exception is thrown
    UpdateStoreUtils
        .validateStorePartitionCountUpdate(admin, multiClusterConfigs, clusterName, originalStore, updatedStore);
  }

  @Test
  public void testValidateStorePartitionerUpdate() {
    String clusterName = "clusterName";
    String storeName = "storeName";

    Store originalStore = mock(Store.class);
    Store updatedStore = mock(Store.class);

    doReturn(storeName).when(originalStore).getName();
    doReturn(storeName).when(updatedStore).getName();

    // Partitioner param update is allowed for batch-only stores
    doReturn(false).when(originalStore).isHybrid();
    doReturn(false).when(updatedStore).isHybrid();
    UpdateStoreUtils.validateStorePartitionerUpdate(clusterName, originalStore, updatedStore);

    // Partitioner param update is allowed during hybrid to batch conversion
    doReturn(true).when(originalStore).isHybrid();
    doReturn(false).when(updatedStore).isHybrid();
    UpdateStoreUtils.validateStorePartitionerUpdate(clusterName, originalStore, updatedStore);

    // Partitioner param update is allowed during batch to hybrid conversion
    doReturn(false).when(originalStore).isHybrid();
    doReturn(true).when(updatedStore).isHybrid();
    UpdateStoreUtils.validateStorePartitionerUpdate(clusterName, originalStore, updatedStore);

    PartitionerConfig originalPartitionerConfig;
    PartitionerConfig updatedPartitionerConfig;

    // Partitioner class update is not allowed for hybrid stores
    doReturn(true).when(originalStore).isHybrid();
    doReturn(true).when(updatedStore).isHybrid();
    originalPartitionerConfig = new PartitionerConfigImpl("ClassA", Collections.singletonMap("key1", "value1"), 1);
    updatedPartitionerConfig = new PartitionerConfigImpl("ClassB", Collections.singletonMap("key1", "value1"), 1);
    doReturn(originalPartitionerConfig).when(originalStore).getPartitionerConfig();
    doReturn(updatedPartitionerConfig).when(updatedStore).getPartitionerConfig();
    VeniceHttpException e1 = expectThrows(
        VeniceHttpException.class,
        () -> UpdateStoreUtils.validateStorePartitionerUpdate(clusterName, originalStore, updatedStore));
    assertEquals(e1.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
    assertEquals(e1.getErrorType(), ErrorType.INVALID_CONFIG);
    assertTrue(e1.getMessage().contains("Partitioner class cannot be changed for hybrid store"));

    // Partitioner param update is not allowed for hybrid stores
    doReturn(true).when(originalStore).isHybrid();
    doReturn(true).when(updatedStore).isHybrid();
    originalPartitionerConfig = new PartitionerConfigImpl("ClassA", Collections.singletonMap("key1", "value1"), 1);
    updatedPartitionerConfig = new PartitionerConfigImpl("ClassA", Collections.singletonMap("key2", "value2"), 1);
    doReturn(originalPartitionerConfig).when(originalStore).getPartitionerConfig();
    doReturn(updatedPartitionerConfig).when(updatedStore).getPartitionerConfig();
    VeniceHttpException e2 = expectThrows(
        VeniceHttpException.class,
        () -> UpdateStoreUtils.validateStorePartitionerUpdate(clusterName, originalStore, updatedStore));
    assertEquals(e2.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
    assertEquals(e2.getErrorType(), ErrorType.INVALID_CONFIG);
    assertTrue(e2.getMessage().contains("Partitioner params cannot be changed for hybrid store"));

    // Amplification factor changes are allowed for hybrid stores
    doReturn(true).when(originalStore).isHybrid();
    doReturn(true).when(updatedStore).isHybrid();
    originalPartitionerConfig = new PartitionerConfigImpl("ClassA", Collections.singletonMap("key1", "value1"), 1);
    updatedPartitionerConfig = new PartitionerConfigImpl("ClassA", Collections.singletonMap("key1", "value1"), 10);
    doReturn(originalPartitionerConfig).when(originalStore).getPartitionerConfig();
    doReturn(updatedPartitionerConfig).when(updatedStore).getPartitionerConfig();
    UpdateStoreUtils.validateStorePartitionerUpdate(clusterName, originalStore, updatedStore);
  }

  @Test
  public void testValidatePersona() {
    String clusterName = "clusterName";
    String storeName = "storeName";

    Store store = mock(Store.class);
    Admin admin = mock(Admin.class);
    HelixVeniceClusterResources clusterResources = mock(HelixVeniceClusterResources.class);
    StoragePersonaRepository personaRepository = mock(StoragePersonaRepository.class);
    StoragePersona persona = mock(StoragePersona.class);

    doReturn(storeName).when(store).getName();

    doReturn(clusterResources).when(admin).getHelixVeniceClusterResources(clusterName);
    doReturn(personaRepository).when(clusterResources).getStoragePersonaRepository();

    // Persona not updated. Store doesn't have an existing persona. Update is allowed.
    doReturn(null).when(personaRepository).getPersonaContainingStore(storeName);
    UpdateStoreUtils.validatePersona(admin, clusterName, store, Optional.empty());

    // Persona not updated. Store has an existing persona. Update is allowed if persona repo allows.
    doReturn(persona).when(personaRepository).getPersonaContainingStore(storeName);
    // Validation doesn't throw exception -> update is allowed
    doNothing().when(personaRepository).validateAddUpdatedStore(any(), any());
    UpdateStoreUtils.validatePersona(admin, clusterName, store, Optional.empty());
    // Validation throws exception -> update is not allowed
    doThrow(new VeniceException()).when(personaRepository).validateAddUpdatedStore(any(), any());
    assertThrows(
        VeniceException.class,
        () -> UpdateStoreUtils.validatePersona(admin, clusterName, store, Optional.empty()));

    String updatedPersona = "persona2";
    // Persona updated. New persona doesn't exist. Update is not allowed.
    doReturn(null).when(personaRepository).getPersonaContainingStore(storeName);
    doReturn(null).when(admin).getStoragePersona(clusterName, updatedPersona);
    assertThrows(
        VeniceException.class,
        () -> UpdateStoreUtils.validatePersona(admin, clusterName, store, Optional.of(updatedPersona)));

    // Persona updated. New persona exists. Update is allowed if persona repo allows.
    doReturn(null).when(personaRepository).getPersonaContainingStore(storeName);
    doReturn(persona).when(admin).getStoragePersona(clusterName, updatedPersona);
    // Validation doesn't throw exception -> update is allowed
    doNothing().when(personaRepository).validateAddUpdatedStore(any(), any());
    UpdateStoreUtils.validatePersona(admin, clusterName, store, Optional.of(updatedPersona));
    // Validation throws exception -> update is not allowed
    doThrow(new VeniceException()).when(personaRepository).validateAddUpdatedStore(any(), any());
    assertThrows(
        VeniceException.class,
        () -> UpdateStoreUtils.validatePersona(admin, clusterName, store, Optional.of(updatedPersona)));
  }

  @Test
  public void testMergeNewSettingsIntoOldPartitionerConfig() {
    String storeName = "storeName";
    Store store = mock(Store.class);

    PartitionerConfig oldPartitionerConfig = new PartitionerConfigImpl();

    doReturn(storeName).when(store).getName();
    doReturn(oldPartitionerConfig).when(store).getPartitionerConfig();

    // No updates to the store partitioner configs should return the same partitioner configs
    assertSame(
        UpdateStoreUtils
            .mergeNewSettingsIntoOldPartitionerConfig(store, Optional.empty(), Optional.empty(), Optional.empty()),
        oldPartitionerConfig);

    String updatedPartitionerClass = "Class B";
    Map<String, String> updatedPartitionerParams = Collections.singletonMap("key1", "value1");
    int updatedAmpFactor = 10;

    PartitionerConfig newPartitionerConfig = UpdateStoreUtils.mergeNewSettingsIntoOldPartitionerConfig(
        store,
        Optional.of(updatedPartitionerClass),
        Optional.of(updatedPartitionerParams),
        Optional.of(updatedAmpFactor));
    assertNotSame(newPartitionerConfig, oldPartitionerConfig); // Should be a new object
    assertEquals(newPartitionerConfig.getPartitionerClass(), updatedPartitionerClass);
    assertEquals(newPartitionerConfig.getPartitionerParams(), updatedPartitionerParams);
    assertEquals(newPartitionerConfig.getAmplificationFactor(), updatedAmpFactor);

    // Even if the store doesn't have a partitioner config, the new partitioner config should be returned
    doReturn(null).when(store).getPartitionerConfig();
    PartitionerConfig newPartitionerConfig2 = UpdateStoreUtils.mergeNewSettingsIntoOldPartitionerConfig(
        store,
        Optional.of(updatedPartitionerClass),
        Optional.of(updatedPartitionerParams),
        Optional.of(updatedAmpFactor));
    assertNotSame(newPartitionerConfig2, oldPartitionerConfig); // Should be a new object
    assertEquals(newPartitionerConfig2.getPartitionerClass(), updatedPartitionerClass);
    assertEquals(newPartitionerConfig2.getPartitionerParams(), updatedPartitionerParams);
    assertEquals(newPartitionerConfig2.getAmplificationFactor(), updatedAmpFactor);
  }

  @Test
  public void testAddNewViewConfigsIntoOldConfigs() {
    String storeName = "storeName";
    Store store = mock(Store.class);
    String classA = "ClassA";
    String classB = "ClassB";
    String classC = "ClassC";

    ViewConfig viewConfigA = mock(ViewConfig.class);
    ViewConfig viewConfigB = mock(ViewConfig.class);
    ViewConfig viewConfigC = mock(ViewConfig.class);

    Map<String, ViewConfig> viewConfigMap = new HashMap<String, ViewConfig>() {
      {
        put(classA, viewConfigA);
        put(classB, viewConfigB);
      }
    };

    doReturn(storeName).when(store).getName();
    doReturn(viewConfigMap).when(store).getViewConfigs();

    Map<String, ViewConfig> mergedViewConfig1 =
        UpdateStoreUtils.addNewViewConfigsIntoOldConfigs(store, classC, viewConfigC);
    assertEquals(mergedViewConfig1.size(), 3);
    assertEquals(mergedViewConfig1.get(classA), viewConfigA);
    assertEquals(mergedViewConfig1.get(classB), viewConfigB);
    assertEquals(mergedViewConfig1.get(classC), viewConfigC);

    Map<String, ViewConfig> mergedViewConfig2 =
        UpdateStoreUtils.addNewViewConfigsIntoOldConfigs(store, classB, viewConfigC);
    assertEquals(mergedViewConfig2.size(), 2);
    assertEquals(mergedViewConfig2.get(classA), viewConfigA);
    assertEquals(mergedViewConfig2.get(classB), viewConfigC);

    doReturn(null).when(store).getViewConfigs();
    Map<String, ViewConfig> mergedViewConfig3 =
        UpdateStoreUtils.addNewViewConfigsIntoOldConfigs(store, classA, viewConfigA);
    assertEquals(mergedViewConfig3.size(), 1);
    assertEquals(mergedViewConfig3.get(classA), viewConfigA);
  }

  @Test
  public void testRemoveViewConfigFromStoreViewConfigMap() {
    String storeName = "storeName";
    Store store = mock(Store.class);
    String classA = "ClassA";
    String classB = "ClassB";

    ViewConfig viewConfigA = mock(ViewConfig.class);
    ViewConfig viewConfigB = mock(ViewConfig.class);

    Map<String, ViewConfig> viewConfigMap = new HashMap<String, ViewConfig>() {
      {
        put(classA, viewConfigA);
        put(classB, viewConfigB);
      }
    };

    doReturn(storeName).when(store).getName();
    doReturn(viewConfigMap).when(store).getViewConfigs();

    Map<String, ViewConfig> newViewConfig1 = UpdateStoreUtils.removeViewConfigFromStoreViewConfigMap(store, classB);
    assertEquals(newViewConfig1.size(), 1);
    assertEquals(newViewConfig1.get(classA), viewConfigA);

    doReturn(null).when(store).getViewConfigs();
    Map<String, ViewConfig> newViewConfig2 = UpdateStoreUtils.removeViewConfigFromStoreViewConfigMap(store, classA);
    assertTrue(newViewConfig2.isEmpty());
  }

  public static class PickyVenicePartitioner extends VenicePartitioner {
    private static final String SCHEMA_VALID = "SCHEMA_VALID";

    public PickyVenicePartitioner() {
      this(VeniceProperties.empty(), null);
    }

    public PickyVenicePartitioner(VeniceProperties props) {
      this(props, null);
    }

    public PickyVenicePartitioner(VeniceProperties props, Schema schema) {
      super(props, schema);
    }

    @Override
    public int getPartitionId(byte[] keyBytes, int numPartitions) {
      return 0;
    }

    @Override
    public int getPartitionId(ByteBuffer keyByteBuffer, int numPartitions) {
      return 0;
    }

    @Override
    protected void checkSchema(@Nonnull Schema keySchema) throws PartitionerSchemaMismatchException {
      if (!props.getBoolean(SCHEMA_VALID)) {
        throw new PartitionerSchemaMismatchException("Schema is not valid");
      }
    }
  }
}
