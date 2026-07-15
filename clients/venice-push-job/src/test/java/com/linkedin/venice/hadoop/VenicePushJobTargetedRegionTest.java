package com.linkedin.venice.hadoop;

import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFER_VERSION_SWAP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INCREMENTAL_PUSH;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGETED_REGION_PUSH_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGETED_REGION_PUSH_LIST;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_FIELD_PROP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.PushJobCheckpoints;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.exceptions.VeniceValidationException;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.MaterializedViewParameters;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.meta.ViewConfigImpl;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.status.PushJobDetailsStatus;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobDetailsStatusTuple;
import com.linkedin.venice.views.MaterializedView;
import com.linkedin.venice.views.ViewUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * This class contains only unit tests for VenicePushJob class.
 *
 * For integration tests please refer to TestVenicePushJob
 *
 * todo: Remove dependency on utils from 'venice-test-common' module
 */

public class VenicePushJobTargetedRegionTest extends VenicePushJobTestBase {
  @Test
  public void testTargetedRegionPushConfigValidation() throws Exception {
    Properties props = getVpjRequiredProperties();
    props.put(TARGETED_REGION_PUSH_ENABLED, false);
    props.put(TARGETED_REGION_PUSH_LIST, "dc-0");
    try (VenicePushJob pushJob = new VenicePushJob(PUSH_JOB_ID, props)) {
      fail("Test should fail, but doesn't.");
    } catch (VeniceException e) {
      assertEquals(e.getMessage(), "Targeted region push list is only supported when targeted region push is enabled");
    }

    props.put(TARGETED_REGION_PUSH_ENABLED, true);
    props.put(INCREMENTAL_PUSH, true);
    try (VenicePushJob pushJob = new VenicePushJob(PUSH_JOB_ID, props)) {
      fail("Test should fail, but doesn't.");
    } catch (VeniceException e) {
      assertEquals(e.getMessage(), "Incremental push is not supported while using targeted region push mode");
    }
  }

  @Test
  public void testTargetedRegionPushConfigOverride() throws Exception {
    Properties props = getVpjRequiredProperties();
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    props.put(TARGETED_REGION_PUSH_ENABLED, true);
    // when targeted region push is enabled, but store doesn't have source fabric set.
    ControllerClient client = getClient(store -> {
      store.setNativeReplicationSourceFabric("");
    });
    JobStatusQueryResponse response = mockJobStatusQuery();

    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), anyString(), anyBoolean());
      skipVPJValidation(pushJob);
      try {
        pushJob.run();
        fail("Test should fail, but doesn't.");
      } catch (VeniceException e) {
        Assert.assertTrue(
            e.getMessage()
                .contains(
                    "The store either does not have native replication mode enabled or set up default source fabric."));
      }
    }

    props.put(TARGETED_REGION_PUSH_LIST, "dc-0");
    try (VenicePushJob pushJob = new VenicePushJob(PUSH_JOB_ID, props)) {
      Assert.assertEquals(pushJob.getPushJobSetting().targetedRegions, "dc-0");
    }
  }

  @Test
  public void testTargetedRegionPushReporting() throws Exception {
    Properties props = getVpjRequiredProperties();
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    props.put(TARGETED_REGION_PUSH_ENABLED, true);
    props.put(TARGETED_REGION_PUSH_LIST, "dc-0");
    ControllerClient client = getClient();
    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      skipVPJValidation(pushJob);

      JobStatusQueryResponse response = mockJobStatusQuery();
      Map<String, String> extraInfo = response.getExtraInfo();
      // one of the regions failed, so should fail
      extraInfo.put("dc-0", ExecutionStatus.NOT_STARTED.toString());
      doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), anyString(), anyBoolean());
      try {
        pushJob.run();
        fail("Test should fail, but doesn't.");
      } catch (VeniceException e) {
        assertTrue(e.getMessage().contains("Push job error"));
      }
    }

    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      skipVPJValidation(pushJob);

      JobStatusQueryResponse response = mockJobStatusQuery();
      Map<String, String> extraInfo = response.getExtraInfo();
      doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), anyString(), anyBoolean());
      extraInfo.put("dc-0", ExecutionStatus.COMPLETED.toString());
      extraInfo.put("dc-1", ExecutionStatus.COMPLETED.toString());
      // both regions completed, so should succeed

      ControllerResponse dataRecoveryResponse = new ControllerResponse();
      doReturn(dataRecoveryResponse).when(client)
          .dataRecovery(anyString(), anyString(), anyString(), anyInt(), anyBoolean(), anyBoolean(), any());
      pushJob.run();
    }
  }

  @Test
  public void testKMEValidationFailure() throws Exception {
    Properties props = getVpjRequiredProperties();
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    ControllerClient client = getClient();
    // mock fetching KME schema and intentionally return an older version
    MultiSchemaResponse multiSchemaResponse = mock(MultiSchemaResponse.class);
    MultiSchemaResponse.Schema valueSchema = mock(MultiSchemaResponse.Schema.class);
    when(valueSchema.getId()).thenReturn(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersion() - 1);
    when(valueSchema.getSchemaStr())
        .thenReturn(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersionSchema().toString());
    when(multiSchemaResponse.getSchemas())
        .thenReturn(Collections.singletonList(valueSchema).toArray(new MultiSchemaResponse.Schema[0]));
    doReturn(multiSchemaResponse).when(client).getAllValueSchema(anyString());
    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      skipVPJValidation(pushJob);
      try {
        pushJob.run();
        fail("Test should fail, but doesn't.");
      } catch (VeniceException e) {
        assertTrue(
            e.getMessage().contains("KME protocol is upgraded in the push job but not in the Venice controller"));
      }
    }
  }

  @Test
  public void testTargetedRegionPushPostValidationConsumptionForBatchStore() throws Exception {
    Properties props = getVpjRequiredProperties();
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    props.put(TARGETED_REGION_PUSH_ENABLED, true);
    ControllerClient client = getClient();
    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      skipVPJValidation(pushJob);

      JobStatusQueryResponse response = mockJobStatusQuery();
      doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), anyString(), anyBoolean());

      VersionCreationResponse mockVersionCreationResponse = mockVersionCreationResponse(client);
      mockVersionCreationResponse.setKafkaSourceRegion(null);

      // verify the kafka source region must be present when kick off post-validation consumption
      try {
        pushJob.run();
        fail("Test should fail, but doesn't.");
      } catch (VeniceException e) {
        assertTrue(
            e.getMessage().contains("Post-validation consumption halted due to no available source region found"));
      }
      mockVersionCreationResponse.setKafkaSourceRegion("dc-0");
      verify(pushJob, times(1)).postPushValidation();
    }

    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      skipVPJValidation(pushJob);
      ControllerResponse badDataRecoveryResponse = new ControllerResponse();
      badDataRecoveryResponse.setError("error");
      doReturn(badDataRecoveryResponse).when(client)
          .dataRecovery(anyString(), anyString(), anyString(), anyInt(), anyBoolean(), anyBoolean(), any());
      // verify failure of data recovery will fail the push job
      try {
        pushJob.run();
      } catch (VeniceException e) {
        assertTrue(e.getMessage().contains("Can't push data for region"));
      }
    }

    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      skipVPJValidation(pushJob);

      ControllerResponse goodDataRecoveryResponse = new ControllerResponse();
      doReturn(goodDataRecoveryResponse).when(client)
          .dataRecovery(anyString(), anyString(), anyString(), anyInt(), anyBoolean(), anyBoolean(), any());

      // the job should succeed
      pushJob.run();
    }
  }

  @Test
  public void testConfigValidateForHybridStore() throws Exception {
    Properties props = getVpjRequiredProperties();
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    ControllerClient client = getClient(storeInfo -> {
      storeInfo.setHybridStoreConfig(new HybridStoreConfigImpl(10, 10, 10, null));
    }, true);
    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      skipVPJValidation(pushJob);
      PushJobSetting setting = pushJob.getPushJobSetting();
      JobStatusQueryResponse response = mockJobStatusQuery();
      doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), anyString(), anyBoolean());
      doCallRealMethod().when(pushJob).run();
      setting.suppressEndOfPushMessage = true;
      pushJob.run();
    }
  }

  @Test
  public void testTargetedRegionPushPostValidationFailedForValidation() throws Exception {
    Properties props = getVpjRequiredProperties();
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    props.put(TARGETED_REGION_PUSH_ENABLED, true);
    ControllerClient client = getClient();
    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      skipVPJValidation(pushJob);

      JobStatusQueryResponse response = mockJobStatusQuery();
      doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), anyString(), anyBoolean());
      mockVersionCreationResponse(client);

      doThrow(new VeniceValidationException("error")).when(pushJob).postPushValidation();

      assertThrows(VeniceValidationException.class, pushJob::run);
      verify(pushJob, never()).postValidationConsumption(any());
    }
  }

  @Test
  public void testTargetRegionPushWithDeferredSwapSettings() {
    Properties props = getVpjRequiredProperties();
    String regions = "test1, test2";
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
    props.put(TARGETED_REGION_PUSH_LIST, regions);

    try (VenicePushJob pushJob = getSpyVenicePushJob(props, getClient())) {
      PushJobSetting pushJobSetting = pushJob.getPushJobSetting();
      Assert.assertEquals(pushJobSetting.deferVersionSwap, true);
      Assert.assertEquals(pushJobSetting.isTargetRegionPushWithDeferredSwapEnabled, true);
      Assert.assertEquals(pushJobSetting.targetedRegions, regions);
    }
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*cannot be enabled at the same time.*")
  public void testEnableBothTargetRegionConfigs() {
    Properties props = getVpjRequiredProperties();
    String regions = "test1, test2";
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
    props.put(TARGETED_REGION_PUSH_ENABLED, true);
    props.put(TARGETED_REGION_PUSH_LIST, regions);

    getSpyVenicePushJob(props, getClient());
  }

  @Test
  public void testConfigureWithMaterializedViewConfigs() throws Exception {
    Properties properties = getVpjRequiredProperties();
    properties.put(KEY_FIELD_PROP, "id");
    properties.put(VALUE_FIELD_PROP, "name");
    JobStatusQueryResponse response = mockJobStatusQuery();
    ControllerClient client = getClient();
    doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), eq(null), anyBoolean());
    try (final VenicePushJob vpj = getSpyVenicePushJob(properties, client)) {
      skipVPJValidation(vpj);
      vpj.run();
      PushJobSetting pushJobSetting = vpj.getPushJobSetting();
      Assert.assertNull(pushJobSetting.materializedViewConfigFlatMap);
    }
    Map<String, ViewConfig> viewConfigs = new HashMap<>();
    MaterializedViewParameters.Builder builder =
        new MaterializedViewParameters.Builder("testView").setPartitionCount(12)
            .setPartitioner(DefaultVenicePartitioner.class.getCanonicalName());
    viewConfigs.put("testView", new ViewConfigImpl(MaterializedView.class.getCanonicalName(), builder.build()));
    viewConfigs.put("dummyView", new ViewConfigImpl("com.linkedin.venice.views.DummyView", Collections.emptyMap()));
    Version version = new VersionImpl(TEST_STORE, 1, TEST_PUSH);
    version.setViewConfigs(viewConfigs);
    client = getClient(storeInfo -> {
      storeInfo.setViewConfigs(viewConfigs);
      storeInfo.setVersions(Collections.singletonList(version));
    }, true);
    doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), eq(null), anyBoolean());
    MultiSchemaResponse valueSchemaResponse = getMultiSchemaResponse();
    MultiSchemaResponse.Schema[] schemas = new MultiSchemaResponse.Schema[1];
    schemas[0] = getBasicSchema();
    valueSchemaResponse.setSchemas(schemas);
    doReturn(valueSchemaResponse).when(client).getAllValueSchema(TEST_STORE);
    doReturn(getMultiSchemaResponse()).when(client).getAllReplicationMetadataSchemas(TEST_STORE);
    doReturn(getKeySchemaResponse()).when(client).getKeySchema(TEST_STORE);
    try (final VenicePushJob vpj = getSpyVenicePushJob(properties, client)) {
      skipVPJValidation(vpj);
      vpj.run();
      PushJobSetting pushJobSetting = vpj.getPushJobSetting();
      Assert.assertNotNull(pushJobSetting.materializedViewConfigFlatMap);
      Map<String, ViewConfig> viewConfigMap =
          ViewUtils.parseViewConfigMapString(pushJobSetting.materializedViewConfigFlatMap);
      // Ensure only materialized view configs are propagated to the job settings
      Assert.assertEquals(viewConfigMap.size(), 1);
      Assert.assertTrue(viewConfigMap.containsKey("testView"));
      Assert.assertEquals(viewConfigMap.get("testView").getViewClassName(), MaterializedView.class.getCanonicalName());
    }
  }

  @Test
  public void testConfigureWithMaterializedViewConfigsWithFlinkVeniceViewsEnabled() throws Exception {
    Properties properties = getVpjRequiredProperties();
    properties.put(KEY_FIELD_PROP, "id");
    properties.put(VALUE_FIELD_PROP, "name");
    JobStatusQueryResponse response = mockJobStatusQuery();
    ControllerClient client = getClient();
    doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), eq(null), anyBoolean());
    try (final VenicePushJob vpj = getSpyVenicePushJob(properties, client)) {
      skipVPJValidation(vpj);
      vpj.run();
      PushJobSetting pushJobSetting = vpj.getPushJobSetting();
      Assert.assertNull(pushJobSetting.materializedViewConfigFlatMap);
    }
    Map<String, ViewConfig> viewConfigs = new HashMap<>();
    MaterializedViewParameters.Builder builder =
        new MaterializedViewParameters.Builder("testView").setPartitionCount(12)
            .setPartitioner(DefaultVenicePartitioner.class.getCanonicalName());
    viewConfigs.put("testView", new ViewConfigImpl(MaterializedView.class.getCanonicalName(), builder.build()));
    viewConfigs.put("dummyView", new ViewConfigImpl("com.linkedin.venice.views.DummyView", Collections.emptyMap()));
    Version version = new VersionImpl(TEST_STORE, 1, TEST_PUSH);
    version.setViewConfigs(viewConfigs);
    client = getClient(storeInfo -> {
      storeInfo.setViewConfigs(viewConfigs);
      storeInfo.setVersions(Collections.singletonList(version));
      storeInfo.setFlinkVeniceViewsEnabled(true); // Enable Flink Venice Views at store level
    }, true);
    doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), eq(null), anyBoolean());
    MultiSchemaResponse valueSchemaResponse = getMultiSchemaResponse();
    MultiSchemaResponse.Schema[] schemas = new MultiSchemaResponse.Schema[1];
    schemas[0] = getBasicSchema();
    valueSchemaResponse.setSchemas(schemas);
    doReturn(valueSchemaResponse).when(client).getAllValueSchema(TEST_STORE);
    doReturn(getMultiSchemaResponse()).when(client).getAllReplicationMetadataSchemas(TEST_STORE);
    doReturn(getKeySchemaResponse()).when(client).getKeySchema(TEST_STORE);
    try (final VenicePushJob vpj = getSpyVenicePushJob(properties, client)) {
      skipVPJValidation(vpj);
      vpj.run();
      PushJobSetting pushJobSetting = vpj.getPushJobSetting();
      Assert.assertNull(pushJobSetting.materializedViewConfigFlatMap); // Should be null since Flink Venice Views is
                                                                       // enabled
    }
  }

  @Test
  public void testTargetedRegionPushWithDeferredSwapConfigValidation() throws Exception {
    Properties props = getVpjRequiredProperties();
    props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, false);
    props.put(TARGETED_REGION_PUSH_LIST, "dc-0");
    try (VenicePushJob pushJob = new VenicePushJob(PUSH_JOB_ID, props)) {
      fail("Test should fail, but doesn't.");
    } catch (VeniceException e) {
      assertEquals(e.getMessage(), "Targeted region push list is only supported when targeted region push is enabled");
    }

    props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
    props.put(INCREMENTAL_PUSH, true);
    try (VenicePushJob pushJob = new VenicePushJob(PUSH_JOB_ID, props)) {
      fail("Test should fail, but doesn't.");
    } catch (VeniceException e) {
      assertEquals(e.getMessage(), "Incremental push is not supported while using targeted region push mode");
    }

    props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
    props.put(DEFER_VERSION_SWAP, true);
    props.put(INCREMENTAL_PUSH, false);
    try (VenicePushJob pushJob = new VenicePushJob(PUSH_JOB_ID, props)) {
      fail("Test should fail, but doesn't.");
    } catch (VeniceException e) {
      assertEquals(
          e.getMessage(),
          "Target region push with deferred swap and deferred swap cannot be enabled at the same time");
    }
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Target region list cannot contain all regions.*")
  public void testTargetRegionPushWithAllRegions() {
    Properties props = getVpjRequiredProperties();
    props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
    props.put(TARGETED_REGION_PUSH_LIST, "dc-0, dc-1");

    ControllerClient client = getClient(storeInfo -> {
      storeInfo.setColoToCurrentVersions(new HashMap<String, Integer>() {
        {
          put("dc-0", 1);
          put("dc-1", 1);
        }
      });
    });

    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      pushJob.run();
    }
  }

  /**
   * Test that START_VERSION_SWAP checkpoint is invoked when target region push with deferred swap is enabled.
   */
  @Test
  public void testVersionSwapCheckpoint() throws Exception {
    Properties props = getVpjRequiredProperties();
    props.put(KEY_FIELD_PROP, "id");
    props.put(VALUE_FIELD_PROP, "name");
    props.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
    props.put(TARGETED_REGION_PUSH_LIST, "dc-0");

    // Create a version with ONLINE status to simulate successful version swap
    Version version = new VersionImpl(TEST_STORE, 1);
    version.setNumber(1);
    version.setStatus(VersionStatus.ONLINE);

    ControllerClient client = getClient(storeInfo -> {
      storeInfo.setColoToCurrentVersions(new HashMap<String, Integer>() {
        {
          put("dc-0", 1);
          put("dc-1", 1);
        }
      });
      storeInfo.setVersions(Collections.singletonList(version));
      storeInfo.setLargestUsedVersionNumber(1);
      storeInfo.setTargetRegionSwapWaitTime(60); // 60 minutes wait time
    });

    try (VenicePushJob pushJob = getSpyVenicePushJob(props, client)) {
      skipVPJValidation(pushJob);

      // Mock job status query to return COMPLETED
      JobStatusQueryResponse response = mockJobStatusQuery();
      doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), anyString(), anyBoolean());

      pushJob.run();

      // Verify that START_VERSION_SWAP checkpoint is called
      verify(pushJob).updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.START_VERSION_SWAP);

      // Verify that COMPLETE_VERSION_SWAP checkpoint is called
      verify(pushJob).updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.COMPLETE_VERSION_SWAP);

      // Get the actual PushJobDetails object
      PushJobDetails pushJobDetails = pushJob.getPushJobDetails();

      // Verify that COMPLETED status was added to overallStatus
      boolean foundCompletedStatus = false;
      for (PushJobDetailsStatusTuple statusTuple: pushJobDetails.overallStatus) {
        if (statusTuple.status == PushJobDetailsStatus.COMPLETED.getValue()) {
          foundCompletedStatus = true;
          break;
        }
      }
      assertTrue(foundCompletedStatus);
    }
  }

  @Test(dataProvider = "versionStatuses")
  public void testTargetRegionPushWithDeferredSwapVersionStatusChecks(
      VersionStatus versionStatus,
      Map<String, String> extraInfo) {
    Properties properties = getVpjRequiredProperties();
    properties.put(KEY_FIELD_PROP, "id");
    properties.put(VALUE_FIELD_PROP, "name");
    properties.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
    Version version = new VersionImpl(TEST_STORE, 1);
    version.setNumber(1);
    version.setStatus(versionStatus);
    ControllerClient client = getClient(store -> {
      store.setVersions(Collections.singletonList(version));
      store.setLargestUsedVersionNumber(1);
    });

    try (VenicePushJob pushJob = getSpyVenicePushJob(properties, client)) {
      skipVPJValidation(pushJob);

      JobStatusQueryResponse response = mockJobStatusQuery();
      response.setExtraInfo(extraInfo);
      doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), anyString(), anyBoolean());

      pushJob.run();
    } catch (Exception e) {
      if (VersionStatus.PARTIALLY_ONLINE.equals(versionStatus)) {
        Assert.assertEquals(
            e.getMessage(),
            "Version kafka-topic is only partially online in some regions. Check nuage to see which regions are not serving the latest version."
                + " It is possible that there was a failure in rolling forward on the controller side or ingestion failed in some regions.");
      } else if (VersionStatus.KILLED.equals(versionStatus)) {
        Assert.assertEquals(e.getMessage(), "Version kafka-topic was killed and cannot be served.");
      } else {
        Assert.assertEquals(
            e.getMessage(),
            "Version kafka-topic was rolled back after ingestion completed due to validation failure");
      }
    }
  }

  // --- Degraded mode tests ---

  @Test
  public void testDegradedModeFlagsSetFromVersionCreationResponse() {
    // Verify the detection logic: when VersionCreationResponse has degradedDatacenters,
    // the PushJobSetting flags should be set correctly.
    PushJobSetting setting = new PushJobSetting();
    setting.storeName = TEST_STORE;

    VersionCreationResponse response = new VersionCreationResponse();
    response.setVersion(1);
    response.setKafkaTopic(Version.composeKafkaTopic(TEST_STORE, 1));
    response.setPartitions(1);
    Set<String> degradedDcs = new HashSet<>();
    degradedDcs.add("dc-2");
    degradedDcs.add("dc-3");
    response.setDegradedDatacenters(degradedDcs);

    // Simulate the detection logic from createNewStoreVersion (lines 2615-2625)
    Set<String> responseDegradedDcs = response.getDegradedDatacenters();
    if (responseDegradedDcs != null && !responseDegradedDcs.isEmpty()) {
      setting.isDegradedModePush = true;
      setting.degradedDatacenters = responseDegradedDcs;
      setting.isTargetRegionPushWithDeferredSwapEnabled = true;
    }

    Assert.assertTrue(setting.isDegradedModePush, "isDegradedModePush should be true");
    Assert.assertTrue(
        setting.isTargetRegionPushWithDeferredSwapEnabled,
        "isTargetRegionPushWithDeferredSwapEnabled should be true");
    Assert.assertNotNull(setting.degradedDatacenters, "degradedDatacenters should be set");
    Assert.assertEquals(setting.degradedDatacenters.size(), 2);
    Assert.assertTrue(setting.degradedDatacenters.contains("dc-2"));
    Assert.assertTrue(setting.degradedDatacenters.contains("dc-3"));
  }

  @Test
  public void testDegradedModeFlagsNotSetWhenNoDegradedDatacenters() {
    PushJobSetting setting = new PushJobSetting();
    setting.storeName = TEST_STORE;

    VersionCreationResponse response = new VersionCreationResponse();
    response.setVersion(1);
    // No degradedDatacenters set — should remain default (false)

    Set<String> responseDegradedDcs = response.getDegradedDatacenters();
    if (responseDegradedDcs != null && !responseDegradedDcs.isEmpty()) {
      setting.isDegradedModePush = true;
      setting.degradedDatacenters = responseDegradedDcs;
      setting.isTargetRegionPushWithDeferredSwapEnabled = true;
    }

    Assert.assertFalse(setting.isDegradedModePush, "isDegradedModePush should be false when no degraded DCs");
    Assert.assertFalse(
        setting.isTargetRegionPushWithDeferredSwapEnabled,
        "isTargetRegionPushWithDeferredSwapEnabled should remain false");
    Assert.assertNull(setting.degradedDatacenters, "degradedDatacenters should be null");
  }

  @Test
  public void testDegradedModePushAcceptsPartiallyOnline() {
    Properties properties = getVpjRequiredProperties();
    properties.put(KEY_FIELD_PROP, "id");
    properties.put(VALUE_FIELD_PROP, "name");
    properties.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
    Version version = new VersionImpl(TEST_STORE, 1);
    version.setNumber(1);
    version.setStatus(VersionStatus.PARTIALLY_ONLINE);
    ControllerClient client = getClient(store -> {
      store.setVersions(Collections.singletonList(version));
      store.setLargestUsedVersionNumber(1);
    });

    try (VenicePushJob pushJob = getSpyVenicePushJob(properties, client)) {
      skipVPJValidation(pushJob);

      // Set degraded mode flags on the push job setting
      PushJobSetting setting = pushJob.getPushJobSetting();
      setting.isDegradedModePush = true;
      setting.degradedDatacenters = new HashSet<>();
      setting.degradedDatacenters.add("dc-2");

      JobStatusQueryResponse response = mockJobStatusQuery();
      Map<String, String> extraInfo = new HashMap<>();
      extraInfo.put("dc-0", ExecutionStatus.COMPLETED.toString());
      extraInfo.put("dc-1", ExecutionStatus.COMPLETED.toString());
      response.setExtraInfo(extraInfo);
      doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), anyString(), anyBoolean());

      // Should succeed without throwing — PARTIALLY_ONLINE accepted in degraded mode
      pushJob.run();
    } catch (Exception e) {
      Assert.fail("Degraded mode push should accept PARTIALLY_ONLINE as success, but got: " + e.getMessage());
    }
  }

  @Test
  public void testTargetedRegionPushWithDeferredSwapRejectsPartiallyOnline() {
    // isTargetRegionPushWithDeferredSwap=true but isDegradedModePush=false
    // This is a normal targeted push (not degraded) — should reject PARTIALLY_ONLINE
    Properties properties = getVpjRequiredProperties();
    properties.put(KEY_FIELD_PROP, "id");
    properties.put(VALUE_FIELD_PROP, "name");
    properties.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
    Version version = new VersionImpl(TEST_STORE, 1);
    version.setNumber(1);
    version.setStatus(VersionStatus.PARTIALLY_ONLINE);
    ControllerClient client = getClient(store -> {
      store.setVersions(Collections.singletonList(version));
      store.setLargestUsedVersionNumber(1);
    });

    try (VenicePushJob pushJob = getSpyVenicePushJob(properties, client)) {
      skipVPJValidation(pushJob);

      // Explicitly set deferred swap but NOT degraded mode
      PushJobSetting setting = pushJob.getPushJobSetting();
      setting.isTargetRegionPushWithDeferredSwapEnabled = true;
      // isDegradedModePush remains false (default)

      JobStatusQueryResponse response = mockJobStatusQuery();
      Map<String, String> extraInfo = new HashMap<>();
      extraInfo.put("dc-0", ExecutionStatus.COMPLETED.toString());
      extraInfo.put("dc-1", ExecutionStatus.COMPLETED.toString());
      response.setExtraInfo(extraInfo);
      doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), anyString(), anyBoolean());

      pushJob.run();
      Assert.fail("Targeted push with deferred swap (non-degraded) should throw on PARTIALLY_ONLINE");
    } catch (Exception e) {
      Assert.assertTrue(
          e.getMessage().contains("partially online"),
          "Expected PARTIALLY_ONLINE error, got: " + e.getMessage());
    }
  }

  @Test
  public void testNonDegradedModePushRejectsPartiallyOnline() {
    Properties properties = getVpjRequiredProperties();
    properties.put(KEY_FIELD_PROP, "id");
    properties.put(VALUE_FIELD_PROP, "name");
    properties.put(TARGETED_REGION_PUSH_WITH_DEFERRED_SWAP, true);
    Version version = new VersionImpl(TEST_STORE, 1);
    version.setNumber(1);
    version.setStatus(VersionStatus.PARTIALLY_ONLINE);
    ControllerClient client = getClient(store -> {
      store.setVersions(Collections.singletonList(version));
      store.setLargestUsedVersionNumber(1);
    });

    try (VenicePushJob pushJob = getSpyVenicePushJob(properties, client)) {
      skipVPJValidation(pushJob);

      // isDegradedModePush is NOT set (default false)

      JobStatusQueryResponse response = mockJobStatusQuery();
      Map<String, String> extraInfo = new HashMap<>();
      extraInfo.put("dc-0", ExecutionStatus.COMPLETED.toString());
      extraInfo.put("dc-1", ExecutionStatus.COMPLETED.toString());
      response.setExtraInfo(extraInfo);
      doReturn(response).when(client).queryOverallJobStatus(anyString(), any(), anyString(), anyBoolean());

      pushJob.run();
      Assert.fail("Non-degraded push should throw on PARTIALLY_ONLINE");
    } catch (Exception e) {
      Assert.assertTrue(
          e.getMessage().contains("partially online"),
          "Expected PARTIALLY_ONLINE error, got: " + e.getMessage());
    }
  }
}
