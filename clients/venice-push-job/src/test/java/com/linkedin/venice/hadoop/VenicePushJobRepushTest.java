package com.linkedin.venice.hadoop;

import static com.linkedin.venice.vpj.VenicePushJobConstants.ALLOW_REGULAR_PUSH_WITH_TTL_REPUSH;
import static com.linkedin.venice.vpj.VenicePushJobConstants.COMPLIANCE_PUSH;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_ENABLE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_SECONDS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_START_TIMESTAMP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_USE_FALLBACK_VALUE_SCHEMA_ID;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_KAFKA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_REPUSH_SOURCE_PUBSUB_BROKER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Time;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * This class contains only unit tests for VenicePushJob class.
 *
 * For integration tests please refer to TestVenicePushJob
 *
 * todo: Remove dependency on utils from 'venice-test-common' module
 */

public class VenicePushJobRepushTest extends VenicePushJobTestBase {
  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Repush with TTL is only supported while using Kafka Input Format.*")
  public void testRepushTTLJobWithNonKafkaInput() {
    Properties repushProps = new Properties();
    repushProps.setProperty(REPUSH_TTL_ENABLE, "true");
    try (VenicePushJob pushJob = getSpyVenicePushJob(repushProps, null)) {
      pushJob.run();
    }
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Repush TTL is only supported for real-time only store.*")
  public void testRepushTTLJobWithBatchStore() {
    Properties repushProps = getRepushWithTTLProps();

    ControllerClient client = getClient(storeInfo -> {
      storeInfo.setColoToCurrentVersions(new HashMap<String, Integer>() {
        {
          // the initial version for all regions is 1, otherwise the topic name would mismatch
          put("dc-0", 1);
          put("dc-1", 1);
        }
      });
    });
    try (VenicePushJob pushJob = getSpyVenicePushJob(repushProps, client)) {
      pushJob.run();
    }
  }

  @Test
  public void testRepushTTLJobConfig() {
    // Test with default configs
    Properties repushProps1 = getRepushWithTTLProps();
    try (VenicePushJob pushJob = getSpyVenicePushJob(repushProps1, null)) {
      PushJobSetting pushJobSetting = pushJob.getPushJobSetting();
      Assert.assertTrue(pushJobSetting.repushTTLEnabled);
      Assert.assertEquals(pushJobSetting.repushTTLStartTimeMs, -1);
      Assert.assertTrue(Version.isPushIdTTLRePush(pushJob.getPushJobDetails().getPushId().toString()));
    }

    // Test with explicit TTL start timestamp
    Properties repushProps2 = getRepushWithTTLProps();
    repushProps2.setProperty(REPUSH_TTL_ENABLE, "true");
    repushProps2.setProperty(REPUSH_TTL_START_TIMESTAMP, "100");
    try (VenicePushJob pushJob = getSpyVenicePushJob(repushProps2, null)) {
      PushJobSetting pushJobSetting = pushJob.getPushJobSetting();
      Assert.assertTrue(pushJobSetting.repushTTLEnabled);
      Assert.assertEquals(pushJobSetting.repushTTLStartTimeMs, 100);
      Assert.assertTrue(Version.isPushIdTTLRePush(pushJob.getPushJobDetails().getPushId().toString()));
    }

    // Test with explicit TTL age
    Properties repushProps3 = getRepushWithTTLProps();
    repushProps3.setProperty(REPUSH_TTL_ENABLE, "true");
    repushProps3.setProperty(REPUSH_TTL_SECONDS, "100");
    try (VenicePushJob pushJob = getSpyVenicePushJob(repushProps3, null)) {
      PushJobSetting pushJobSetting = pushJob.getPushJobSetting();
      Assert.assertTrue(pushJobSetting.repushTTLEnabled);
      Assert.assertTrue(
          pushJobSetting.repushTTLStartTimeMs > 0
              && pushJobSetting.repushTTLStartTimeMs <= System.currentTimeMillis() - 100 * Time.MS_PER_SECOND);
    }

    // Test with both explicit TTL age and TTL start timestamp - Not allowed
    Properties repushProps4 = getRepushWithTTLProps();
    repushProps4.setProperty(REPUSH_TTL_ENABLE, "true");
    repushProps4.setProperty(REPUSH_TTL_START_TIMESTAMP, "100");
    repushProps4.setProperty(REPUSH_TTL_SECONDS, "100");
    VeniceException exception =
        Assert.expectThrows(VeniceException.class, () -> getSpyVenicePushJob(repushProps4, null));
    Assert.assertTrue(exception.getMessage().endsWith("Please set only one."));

    // Test with TTL disabled.
    Properties repushProps5 = getRepushWithTTLProps();
    repushProps5.setProperty(REPUSH_TTL_ENABLE, "false");
    // Doesn't matter if these are set, they should be ignored.
    repushProps5.setProperty(REPUSH_TTL_START_TIMESTAMP, "100");
    repushProps5.setProperty(REPUSH_TTL_SECONDS, "100");
    try (VenicePushJob pushJob = getSpyVenicePushJob(repushProps5, null)) {
      PushJobSetting pushJobSetting = pushJob.getPushJobSetting();
      Assert.assertFalse(pushJobSetting.repushTTLEnabled);
      Assert.assertEquals(pushJobSetting.repushTTLStartTimeMs, -1);
      Assert.assertTrue(Version.isPushIdRePush(pushJob.getPushJobDetails().getPushId().toString()));
    }
  }

  /**
   * When repush.use.fallback.value.schema.id is enabled, the latest value schema ID should be
   * retrieved from the controller as a global fallback for records missing per-record schema IDs.
   */
  @Test
  public void testKifRepushRetrievesValueSchemaIdWhenFallbackEnabled() throws Exception {
    Properties repushProps = new Properties();
    repushProps.setProperty(SOURCE_KAFKA, "true");
    repushProps.setProperty(KAFKA_INPUT_TOPIC, Version.composeKafkaTopic(TEST_STORE, REPUSH_VERSION));
    repushProps.setProperty(VENICE_REPUSH_SOURCE_PUBSUB_BROKER, "localhost");
    repushProps.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
    repushProps.setProperty(REPUSH_USE_FALLBACK_VALUE_SCHEMA_ID, "true");

    ControllerClient client = getClient(storeInfo -> {
      Map<String, Integer> coloVersions = new HashMap<>();
      coloVersions.put("dc-0", REPUSH_VERSION);
      coloVersions.put("dc-1", REPUSH_VERSION);
      storeInfo.setColoToCurrentVersions(coloVersions);
    });

    MultiSchemaResponse valueSchemaResponse = new MultiSchemaResponse();
    MultiSchemaResponse.Schema schema1 = new MultiSchemaResponse.Schema();
    schema1.setId(1);
    schema1.setSchemaStr(VALUE_SCHEMA_STR);
    MultiSchemaResponse.Schema schema2 = new MultiSchemaResponse.Schema();
    schema2.setId(2);
    schema2.setSchemaStr(VALUE_SCHEMA_STR);
    valueSchemaResponse.setSchemas(new MultiSchemaResponse.Schema[] { schema1, schema2 });
    doReturn(valueSchemaResponse).when(client).getAllValueSchema(eq(TEST_STORE));

    try (VenicePushJob pushJob = getSpyVenicePushJob(repushProps, client)) {
      skipVPJValidation(pushJob);
      doNothing().when(pushJob).pollStatusUntilComplete(any(), any(), any(), any(), anyBoolean(), anyBoolean());
      pushJob.run();

      assertEquals(
          pushJob.getPushJobSetting().valueSchemaId,
          2,
          "KIF repush should retrieve the latest value schema ID from the controller when fallback is enabled");
    }
  }

  /**
   * By default (repush.use.fallback.value.schema.id=false), the value schema ID should NOT be
   * retrieved from the controller. The job will fail later if per-record schema IDs are missing.
   */
  @Test
  public void testKifRepushDoesNotRetrieveValueSchemaIdByDefault() throws Exception {
    Properties repushProps = new Properties();
    repushProps.setProperty(SOURCE_KAFKA, "true");
    repushProps.setProperty(KAFKA_INPUT_TOPIC, Version.composeKafkaTopic(TEST_STORE, REPUSH_VERSION));
    repushProps.setProperty(VENICE_REPUSH_SOURCE_PUBSUB_BROKER, "localhost");
    repushProps.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
    // No REPUSH_USE_FALLBACK_VALUE_SCHEMA_ID set — defaults to false

    ControllerClient client = getClient(storeInfo -> {
      Map<String, Integer> coloVersions = new HashMap<>();
      coloVersions.put("dc-0", REPUSH_VERSION);
      coloVersions.put("dc-1", REPUSH_VERSION);
      storeInfo.setColoToCurrentVersions(coloVersions);
    });

    try (VenicePushJob pushJob = getSpyVenicePushJob(repushProps, client)) {
      skipVPJValidation(pushJob);
      doNothing().when(pushJob).pollStatusUntilComplete(any(), any(), any(), any(), anyBoolean(), anyBoolean());
      pushJob.run();

      assertEquals(
          pushJob.getPushJobSetting().valueSchemaId,
          0,
          "KIF repush should NOT retrieve value schema ID when fallback is disabled (default)");
    }
  }

  @Test
  public void testCompliancePushJobConfig() {
    // Test with compliance push enabled
    Properties complianceProps = new Properties();
    complianceProps.setProperty(COMPLIANCE_PUSH, "true");
    try (VenicePushJob pushJob = getSpyVenicePushJob(complianceProps, null)) {
      PushJobSetting pushJobSetting = pushJob.getPushJobSetting();
      Assert.assertTrue(pushJobSetting.isCompliancePush);
      Assert.assertTrue(Version.isPushIdCompliancePush(pushJob.getPushJobDetails().getPushId().toString()));
    }

    // Test with compliance push disabled (default)
    Properties regularProps = new Properties();
    try (VenicePushJob pushJob = getSpyVenicePushJob(regularProps, null)) {
      PushJobSetting pushJobSetting = pushJob.getPushJobSetting();
      Assert.assertFalse(pushJobSetting.isCompliancePush);
      Assert.assertFalse(Version.isPushIdCompliancePush(pushJob.getPushJobDetails().getPushId().toString()));
    }

    // Compliance push cannot be combined with TTL repush
    Properties complianceRepushProps = getRepushWithTTLProps();
    complianceRepushProps.setProperty(COMPLIANCE_PUSH, "true");
    VeniceException e =
        Assert.expectThrows(VeniceException.class, () -> getSpyVenicePushJob(complianceRepushProps, null));
    Assert.assertTrue(e.getMessage().contains("Compliance push cannot be combined with TTL repush settings"));

    // Compliance push cannot be combined with regular push with TTL repush
    Properties complianceRegularTTLProps = new Properties();
    complianceRegularTTLProps.setProperty(COMPLIANCE_PUSH, "true");
    complianceRegularTTLProps.setProperty(ALLOW_REGULAR_PUSH_WITH_TTL_REPUSH, "true");
    VeniceException e2 =
        Assert.expectThrows(VeniceException.class, () -> getSpyVenicePushJob(complianceRegularTTLProps, null));
    Assert.assertTrue(e2.getMessage().contains("Compliance push cannot be combined with TTL repush settings"));
  }

  @Test
  public void testValidateRegularPushWithTTLRepush() {
    ControllerClient mockControllerClient = mock(ControllerClient.class);
    StoreResponse mockStoreResponse = mock(StoreResponse.class);
    doReturn(mockStoreResponse).when(mockControllerClient).getStore(anyString());
    doReturn(false).when(mockStoreResponse).isError();
    StoreInfo mockStoreInfo = mock(StoreInfo.class);
    doReturn(mockStoreInfo).when(mockStoreResponse).getStore();
    doReturn(true).when(mockStoreInfo).isTTLRepushEnabled();
    // Re-push, incremental and empty pushes should be allowed
    Properties props = getVpjRequiredProperties();
    VenicePushJob venicePushJob = new VenicePushJob(PUSH_JOB_ID, props);
    PushJobSetting pushJobSetting = venicePushJob.getPushJobSetting();
    pushJobSetting.inputHasRecords = true;
    pushJobSetting.isIncrementalPush = true;
    venicePushJob.checkRegularPushWithTTLRepush(mockControllerClient, venicePushJob.getPushJobSetting());
    venicePushJob = new VenicePushJob(PUSH_JOB_ID, props);
    pushJobSetting = venicePushJob.getPushJobSetting();
    pushJobSetting.inputHasRecords = true;
    pushJobSetting.isSourceKafka = true;
    venicePushJob.checkRegularPushWithTTLRepush(mockControllerClient, venicePushJob.getPushJobSetting());
    venicePushJob = new VenicePushJob(PUSH_JOB_ID, props);
    pushJobSetting = venicePushJob.getPushJobSetting();
    pushJobSetting.inputHasRecords = false;
    venicePushJob.checkRegularPushWithTTLRepush(mockControllerClient, venicePushJob.getPushJobSetting());
    // Regular batch push should be rejected
    final VenicePushJob failPushJob = new VenicePushJob(PUSH_JOB_ID, props);
    pushJobSetting = failPushJob.getPushJobSetting();
    pushJobSetting.inputHasRecords = true;
    Assert.assertThrows(
        VeniceException.class,
        () -> failPushJob.checkRegularPushWithTTLRepush(mockControllerClient, failPushJob.getPushJobSetting()));
  }
}
