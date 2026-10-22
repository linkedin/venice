package com.linkedin.venice.controller.init;

import static com.linkedin.venice.ConfigKeys.CLUSTER_PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.common.VeniceSystemStoreUtils.CLUSTER_PUSH_STATUS_STORE_PARTITION_COUNT;
import static com.linkedin.venice.controller.init.SystemStoreInitializationHelper.DEFAULT_KEY_SCHEMA_STR;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.VeniceConstants;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pushstatus.PushStatusKey;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.mockito.InOrder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class SystemStoreInitializationHelperTest {
  private static final String CLUSTER_NAME = "cluster";
  private static final String STORE_NAME = VeniceSystemStoreUtils.getDaVinciPushStatusStoreNameForCluster(CLUSTER_NAME);

  @BeforeClass
  public void setUp() {
    SystemStoreInitializationHelper.setDelayBetweenStoreUpdateRetries(Duration.ofSeconds(1));
  }

  /**
   * Tests the case where this is the first time creating the system store
   */
  @Test(dataProvider = "Three-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testInitialSystemStoreSetup(
      boolean explicitlyProvidedKeySchema,
      boolean singleColoMode,
      boolean explicitPartitionCount) {
    String clusterName = "testCluster";
    String expectedKeySchemaStr = explicitlyProvidedKeySchema ? "\"string\"" : DEFAULT_KEY_SCHEMA_STR;
    Schema expectedKeySchema = AvroCompatibilityHelper.parse(expectedKeySchemaStr);
    Schema keySchema = explicitlyProvidedKeySchema ? expectedKeySchema : null;
    AvroProtocolDefinition protocolDefinition = AvroProtocolDefinition.PUSH_JOB_DETAILS;
    Map<Integer, Schema> protocolSchemaMap = Utils.getAllSchemasFromResources(protocolDefinition);
    ArrayList<SchemaEntry> allValueSchemaEntries = new ArrayList<>(protocolSchemaMap.size());
    for (Map.Entry<Integer, Schema> entry: protocolSchemaMap.entrySet()) {
      allValueSchemaEntries.add(new SchemaEntry(entry.getKey(), entry.getValue()));
    }

    String systemStoreName = VeniceSystemStoreUtils.getPushJobDetailsStoreName();
    Admin admin = mock(VeniceHelixAdmin.class);
    Function<Store, Boolean> updateStoreCheckSupplier = store -> !store.isHybrid();
    UpdateStoreQueryParams updateStoreQueryParams =
        new UpdateStoreQueryParams().setHybridOffsetLagThreshold(1000).setHybridRewindSeconds(100);
    VeniceControllerMultiClusterConfig multiClusterConfigs = mock(VeniceControllerMultiClusterConfig.class);

    Version firstVersion = mock(Version.class);
    int versionNumber = 1;
    int partitionCount = explicitPartitionCount ? 200 : 10;
    if (explicitPartitionCount) {
      updateStoreQueryParams.setPartitionCount(partitionCount);
    }
    int replicationFactor = 3;
    doReturn(1).when(firstVersion).getNumber();
    doReturn(VersionStatus.ONLINE).when(firstVersion).getStatus();

    Store storeForTest = mock(Store.class);

    Store storeForTestAfterUpdateStore = mock(Store.class);
    doReturn(true).when(storeForTestAfterUpdateStore).isHybrid();

    Store storeForTestAfterCreatingVersion = mock(Store.class);
    doReturn(true).when(storeForTestAfterCreatingVersion).isHybrid();
    if (singleColoMode) {
      // The parent controller always has a current version of zero, so this info is not available there.
      doReturn(versionNumber).when(storeForTestAfterCreatingVersion).getCurrentVersion();
    }
    doReturn(firstVersion).when(storeForTestAfterCreatingVersion).getVersion(versionNumber);
    doReturn(Collections.singletonList(firstVersion)).when(storeForTestAfterCreatingVersion).getVersions();

    doReturn(replicationFactor).when(admin).getReplicationFactor(clusterName, systemStoreName);
    doReturn(Collections.singleton(new SchemaEntry(1, protocolSchemaMap.get(1)))) // Right after store creation
        .doReturn(allValueSchemaEntries) // After back-filling other schemas as well...
        .when(admin)
        .getValueSchemas(clusterName, systemStoreName);
    doReturn(new SchemaEntry(1, expectedKeySchema)).when(admin).getKeySchema(clusterName, systemStoreName);
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
    doReturn(10).when(controllerConfig).getMinNumberOfPartitions();

    // Do it multiple times, to verify idempotency.
    int totalInvocations = 2;
    for (int i = 0; i < totalInvocations; i++) {
      SystemStoreInitializationHelper.setupSystemStore(
          clusterName,
          systemStoreName,
          protocolDefinition,
          keySchema,
          updateStoreCheckSupplier,
          updateStoreQueryParams,
          admin,
          multiClusterConfigs);

      /**
       * getStore should be called either 4 or 5 times:
       *
       * 1. at the beginning of each attempt
       * 2. after store creation of the 1st attempt only
       * 3. after store update of the 1st attempt only
       * 4. after version creation of the 1st attempt only
       */
      verify(admin, times(4 + i)).getStore(clusterName, systemStoreName);

      // The rest of the APIs below should be called only once (during the first attempt).

      verify(admin, times(1)).createStore(
          clusterName,
          systemStoreName,
          VeniceConstants.SYSTEM_STORE_OWNER,
          expectedKeySchemaStr,
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
      for (int j = 2; j <= protocolDefinition.getCurrentProtocolVersion(); j++) {
        verify(admin, times(1)).addValueSchema(
            clusterName,
            systemStoreName,
            protocolSchemaMap.get(j).toString(),
            j,
            DirectionalSchemaCompatibilityType.NONE);
      }

      verify(admin, times(1)).incrementVersionIdempotent(
          eq(clusterName),
          eq(systemStoreName),
          any(),
          eq(partitionCount),
          eq(replicationFactor));
    }
  }

  @Test
  public void testClusterPushStatusProvisioningIsOptIn() {
    Admin admin = mock(Admin.class);
    VeniceControllerMultiClusterConfig configs = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerClusterConfig config = mock(VeniceControllerClusterConfig.class);
    doReturn(config).when(configs).getControllerConfig("cluster");
    doReturn(VeniceProperties.empty()).when(config).getProps();
    new ClusterPushStatusStoreInitializationRoutine(configs, admin).execute("cluster");
    verifyNoInteractions(admin);
  }

  @DataProvider
  public Object[][] clusterPushStatusStoreStates() {
    return new Object[][] { { true, false, false }, { false, false, false }, { false, true, true } };
  }

  @Test(dataProvider = "clusterPushStatusStoreStates")
  public void testClusterPushStatusProvisioning(boolean newStore, boolean onlineVersion, boolean writeCompute) {
    ClusterPushStatusFixture fixture = new ClusterPushStatusFixture();
    Admin admin = fixture.admin;
    Store initialStore = fixture.newStore();
    doReturn(onlineVersion).when(initialStore).isHybrid();
    doReturn(writeCompute).when(initialStore).isWriteComputationEnabled();
    doReturn(onlineVersion ? CLUSTER_PUSH_STATUS_STORE_PARTITION_COUNT : 1).when(initialStore).getPartitionCount();
    Store configuredStore = fixture.newStore();
    Version version = newPushStatusVersion(1, true);
    Store servingStore = fixture.newStore();
    doReturn(1).when(servingStore).getCurrentVersion();
    doReturn(version).when(servingStore).getVersion(1);
    doReturn(Collections.singletonList(version)).when(servingStore).getVersions();
    if (onlineVersion) {
      doReturn(1).when(initialStore).getCurrentVersion();
      doReturn(version).when(initialStore).getVersion(1);
      doReturn(Collections.singletonList(version)).when(initialStore).getVersions();
      doReturn(Collections.singletonList(version)).when(configuredStore).getVersions();
      doReturn(initialStore, initialStore, servingStore).when(admin).getStore(CLUSTER_NAME, STORE_NAME);
    } else if (newStore) {
      doReturn(null, null, initialStore, configuredStore, servingStore).when(admin).getStore(CLUSTER_NAME, STORE_NAME);
    } else {
      doReturn(initialStore, initialStore, configuredStore, servingStore).when(admin)
          .getStore(CLUSTER_NAME, STORE_NAME);
    }
    doReturn(onlineVersion).doReturn(true).when(fixture.topicManager).containsTopic(fixture.realTimeTopic);
    doReturn(CLUSTER_PUSH_STATUS_STORE_PARTITION_COUNT).when(fixture.topicManager)
        .getPartitionCount(fixture.realTimeTopic);
    doReturn(3).when(admin).getReplicationFactor(CLUSTER_NAME, STORE_NAME);
    doReturn(version).when(admin)
        .incrementVersionIdempotent(
            eq(CLUSTER_NAME),
            eq(STORE_NAME),
            any(),
            eq(CLUSTER_PUSH_STATUS_STORE_PARTITION_COUNT),
            eq(3));
    doReturn(new SchemaEntry(1, PushStatusKey.SCHEMA$)).when(admin).getKeySchema(CLUSTER_NAME, STORE_NAME);
    ArrayList<SchemaEntry> valueSchemas = new ArrayList<>();
    Utils.getAllSchemasFromResources(AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE)
        .forEach((id, schema) -> valueSchemas.add(new SchemaEntry(id, schema)));
    doReturn(valueSchemas).when(admin).getValueSchemas(CLUSTER_NAME, STORE_NAME);
    doReturn(Collections.emptyList()).when(admin).getDerivedSchemas(CLUSTER_NAME, STORE_NAME);

    ClusterPushStatusStoreInitializationRoutine routine = fixture.routine;
    routine.execute(CLUSTER_NAME);

    UpdateStoreQueryParams expectedParams =
        new UpdateStoreQueryParams().setPartitionCount(CLUSTER_PUSH_STATUS_STORE_PARTITION_COUNT)
            .setHybridOffsetLagThreshold(1)
            .setHybridRewindSeconds(TimeUnit.DAYS.toSeconds(1))
            .setHybridTimeLagThreshold(-1)
            .setWriteComputationEnabled(true)
            .setNativeReplicationSourceFabric("region");
    boolean needsUpdate = !onlineVersion || !writeCompute;
    verify(admin, times(needsUpdate ? 1 : 0)).updateStore(
        eq(CLUSTER_NAME),
        eq(STORE_NAME),
        argThat(params -> params.getNameValuePairs().equals(expectedParams.getNameValuePairs())));
    verify(admin, times(newStore ? 1 : 0)).createStore(any(), any(), any(), any(), any(), eq(true));
    verify(admin, times(onlineVersion ? 0 : 1))
        .incrementVersionIdempotent(any(), any(), any(), eq(CLUSTER_PUSH_STATUS_STORE_PARTITION_COUNT), eq(3));
    if (!onlineVersion) {
      InOrder provisioningOrder = inOrder(admin);
      provisioningOrder.verify(admin).updateStore(eq(CLUSTER_NAME), eq(STORE_NAME), any());
      provisioningOrder.verify(admin)
          .incrementVersionIdempotent(any(), any(), any(), eq(CLUSTER_PUSH_STATUS_STORE_PARTITION_COUNT), eq(3));
    }

    ArrayList<DerivedSchemaEntry> derivedSchemas = new ArrayList<>();
    derivedSchemas.add(new DerivedSchemaEntry(-1, 1, "\"string\""));
    for (SchemaEntry valueSchema: valueSchemas) {
      Schema updateSchema =
          WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema.getSchema());
      verify(admin).addDerivedSchema(CLUSTER_NAME, STORE_NAME, valueSchema.getId(), 1, updateSchema.toString());
      derivedSchemas.add(new DerivedSchemaEntry(valueSchema.getId(), 2, updateSchema));
      derivedSchemas.add(new DerivedSchemaEntry(valueSchema.getId(), 1, updateSchema));
    }
    doReturn(derivedSchemas).when(admin).getDerivedSchemas(CLUSTER_NAME, STORE_NAME);
    routine.execute(CLUSTER_NAME);
    for (SchemaEntry valueSchema: valueSchemas) {
      verify(admin, times(1)).addDerivedSchema(eq(CLUSTER_NAME), eq(STORE_NAME), eq(valueSchema.getId()), eq(1), any());
    }

    doReturn(Collections.singletonList(new DerivedSchemaEntry(valueSchemas.get(0).getId(), 1, "\"string\"")))
        .when(admin)
        .getDerivedSchemas(CLUSTER_NAME, STORE_NAME);
    VeniceException exception = Assert.expectThrows(VeniceException.class, () -> routine.execute(CLUSTER_NAME));
    Assert.assertTrue(exception.getMessage().contains("Inconsistent push status update schema"));
  }

  @Test
  public void testClusterPushStatusServingVersionsShareUnversionedRealTimeTopic() {
    Store store = mock(Store.class);
    doReturn(STORE_NAME).when(store).getName();
    doReturn(2).when(store).getCurrentVersion();
    HybridStoreConfigImpl storeConfig = new HybridStoreConfigImpl(86400, 1, -1, null);
    storeConfig.setRealTimeTopicName(Utils.composeRealTimeTopic(STORE_NAME, 3));
    doReturn(storeConfig).when(store).getHybridStoreConfig();
    ArrayList<Version> versions = new ArrayList<>();
    for (int number = 1; number <= 2; number++) {
      Version version = newPushStatusVersion(number, true);
      version.getHybridStoreConfig().setRealTimeTopicName(Utils.composeRealTimeTopic(STORE_NAME, number));
      versions.add(version);
    }
    doReturn(versions).when(store).getVersions();

    Assert.assertFalse(Utils.isRTVersioningApplicable(STORE_NAME));
    String realTimeTopic = Utils.composeRealTimeTopic(STORE_NAME);
    Assert.assertEquals(Utils.getRealTimeTopicName(store), realTimeTopic);
    for (Version version: versions) {
      Assert.assertNotEquals(version.getHybridStoreConfig().getRealTimeTopicName(), realTimeTopic);
      Assert.assertEquals(Utils.getRealTimeTopicName(version), realTimeTopic);
    }
  }

  @DataProvider
  public Object[][] incompatibleClusterPushStatusStores() {
    return new Object[][] { { "hybrid-metadata", "hybrid store must have 200 partitions" },
        { "online-partitions", "serving version 1 must be hybrid with 200 partitions" },
        { "online-batch", "serving version 1 must be hybrid with 200 partitions" },
        { "current-batch", "serving version 1 must be hybrid with 200 partitions" },
        { "online-write-compute", "requires write computation to be enabled before ingestion starts" },
        { "current-write-compute", "requires write computation to be enabled before ingestion starts" },
        { "missing-current", "missing current version 2" },
        { "old-online", "serving version 1 must be hybrid with 200 partitions" },
        { "missing-rt", "Missing push status RT topic" }, { "rt-partitions", "must have 200 partitions" },
        { "versionless-rt", "must have 200 partitions" }, { "orphan-rt", "must have 200 partitions" } };
  }

  @Test(dataProvider = "incompatibleClusterPushStatusStores")
  public void testRejectIncompatibleClusterPushStatusStoreBeforeMutation(String state, String expectedError) {
    ClusterPushStatusFixture fixture = new ClusterPushStatusFixture();
    Admin admin = fixture.admin;
    Store store = fixture.newStore();
    doReturn(!state.endsWith("-write-compute")).when(store).isWriteComputationEnabled();
    doReturn(!state.equals("online-batch")).when(store).isHybrid();
    doReturn(state.equals("hybrid-metadata") ? 1 : CLUSTER_PUSH_STATUS_STORE_PARTITION_COUNT).when(store)
        .getPartitionCount();
    Version version = newPushStatusVersion(1, !state.equals("online-batch") && !state.equals("current-batch"));
    if (state.equals("online-partitions")) {
      version.setPartitionCount(1);
    }
    boolean currentVersion = state.equals("current-batch") || state.equals("current-write-compute");
    version.setStatus(currentVersion ? VersionStatus.STARTED : VersionStatus.ONLINE);
    doReturn(Collections.singletonList(version)).when(store).getVersions();
    doReturn(version).when(store).getVersion(1);
    if (currentVersion) {
      doReturn(1).when(store).getCurrentVersion();
    } else if (state.equals("missing-current")) {
      doReturn(2).when(store).getCurrentVersion();
    } else if (state.equals("old-online")) {
      version.setPartitionCount(1);
      Version current = newPushStatusVersion(2, true);
      doReturn(2).when(store).getCurrentVersion();
      doReturn(current).when(store).getVersion(2);
      doReturn(Arrays.asList(current, version)).when(store).getVersions();
    } else if (state.equals("versionless-rt")) {
      doReturn(Collections.emptyList()).when(store).getVersions();
    }
    doReturn(state.equals("orphan-rt") ? null : store).when(admin).getStore(CLUSTER_NAME, STORE_NAME);
    TopicManager topicManager = fixture.topicManager;
    PubSubTopic realTimeTopic = fixture.realTimeTopic;
    doReturn(!state.equals("missing-rt")).when(topicManager).containsTopic(realTimeTopic);
    doReturn(1).when(topicManager).getPartitionCount(realTimeTopic);

    VeniceException exception = Assert.expectThrows(VeniceException.class, () -> fixture.routine.execute(CLUSTER_NAME));
    Assert.assertTrue(exception.getMessage().contains(expectedError), exception.getMessage());
    verify(admin).getStore(CLUSTER_NAME, STORE_NAME);
    if (state.endsWith("-rt") || state.equals("rt-partitions")) {
      verify(admin).getPubSubTopicRepository();
      verify(admin).getTopicManager();
      verify(topicManager).containsTopic(realTimeTopic);
      if (!state.equals("missing-rt")) {
        verify(topicManager).getPartitionCount(realTimeTopic);
      }
    } else {
      verifyNoInteractions(topicManager);
    }
    verifyNoMoreInteractions(admin, topicManager);
  }

  private static final class ClusterPushStatusFixture {
    private final Admin admin = mock(Admin.class);
    private final TopicManager topicManager = mock(TopicManager.class);
    private final PubSubTopic realTimeTopic;
    private final ClusterPushStatusStoreInitializationRoutine routine;

    private ClusterPushStatusFixture() {
      VeniceControllerMultiClusterConfig configs = mock(VeniceControllerMultiClusterConfig.class);
      VeniceControllerClusterConfig config = mock(VeniceControllerClusterConfig.class);
      doReturn(config).when(configs).getControllerConfig(CLUSTER_NAME);
      doReturn(new VeniceProperties(Collections.singletonMap(CLUSTER_PUSH_STATUS_STORE_ENABLED, "true"))).when(config)
          .getProps();
      doReturn("region").when(configs).getRegionName();
      PubSubTopicRepository topicRepository = new PubSubTopicRepository();
      realTimeTopic = topicRepository.getTopic(Utils.composeRealTimeTopic(STORE_NAME));
      doReturn(topicRepository).when(admin).getPubSubTopicRepository();
      doReturn(topicManager).when(admin).getTopicManager();
      routine = new ClusterPushStatusStoreInitializationRoutine(configs, admin);
    }

    private Store newStore() {
      Store store = mock(Store.class);
      doReturn(STORE_NAME).when(store).getName();
      doReturn(true).when(store).isHybrid();
      doReturn(true).when(store).isWriteComputationEnabled();
      doReturn(CLUSTER_PUSH_STATUS_STORE_PARTITION_COUNT).when(store).getPartitionCount();
      return store;
    }
  }

  private static Version newPushStatusVersion(int number, boolean hybrid) {
    Version version = new VersionImpl(STORE_NAME, number, "push-" + number, CLUSTER_PUSH_STATUS_STORE_PARTITION_COUNT);
    if (hybrid) {
      version.setHybridStoreConfig(new HybridStoreConfigImpl(86400, 1, -1, null));
    }
    version.setStatus(VersionStatus.ONLINE);
    return version;
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
    doReturn(VersionStatus.ONLINE).when(firstVersion).getStatus();

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
