package com.linkedin.venice.hadoop;

import static com.linkedin.venice.status.BatchJobHeartbeatConfigs.HEARTBEAT_ENABLED_CONFIG;
import static com.linkedin.venice.vpj.VenicePushJobConstants.CONTROLLER_REQUEST_RETRY_ATTEMPTS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ENABLE_WRITE_COMPUTE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INCREMENTAL_PUSH;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.LEGACY_AVRO_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.LEGACY_AVRO_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_ETL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_KAFKA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGET_WRITER_VALUE_SCHEMA_ID_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_DISCOVER_URL_PROP;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Collections;
import java.util.Optional;
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

public class VenicePushJobTest extends VenicePushJobTestBase {
  @Test(expectedExceptions = NullPointerException.class)
  public void testVenicePushJobThrowsNpeIfVpjPropertiesIsNull() {
    new VenicePushJob(PUSH_JOB_ID, null);
  }

  @Test
  public void testGetPushJobSettingThrowsUndefinedPropertyException() {
    Properties props = getVpjRequiredProperties();
    Set<Object> reqPropKeys = props.keySet();
    for (Object prop: reqPropKeys) {
      Properties propsCopy = (Properties) props.clone();
      propsCopy.remove(prop);
      try {
        new VenicePushJob(PUSH_JOB_ID, propsCopy);
        fail("Should throw UndefinedPropertyException for missing property: " + prop);
      } catch (UndefinedPropertyException expected) {
      }
    }
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Duplicate key field config found.*")
  public void testVenicePushJobCanHandleLegacyFieldsThrowsExceptionIfDuplicateKeysButValuesDiffer() {
    Properties props = getVpjRequiredProperties();
    props.put(LEGACY_AVRO_KEY_FIELD_PROP, "id");
    props.put(KEY_FIELD_PROP, "name");
    new VenicePushJob(PUSH_JOB_ID, props);
  }

  @Test
  public void testVenicePushJobCanHandleLegacyFields() {
    Properties props = getVpjRequiredProperties();
    props.put(LEGACY_AVRO_KEY_FIELD_PROP, "id");
    props.put(LEGACY_AVRO_VALUE_FIELD_PROP, "name");
    try (VenicePushJob vpj = new VenicePushJob(PUSH_JOB_ID, props)) {
      VeniceProperties veniceProperties = vpj.getJobProperties();
      assertNotNull(veniceProperties);
      assertEquals(veniceProperties.getString(KEY_FIELD_PROP), "id");
      assertEquals(veniceProperties.getString(VALUE_FIELD_PROP), "name");
    }
  }

  @Test
  public void testGetPushJobSetting() {
    Properties props = getVpjRequiredProperties();
    try (VenicePushJob vpj = new VenicePushJob(PUSH_JOB_ID, props)) {
      PushJobSetting pushJobSetting = vpj.getPushJobSetting();
      assertNotNull(pushJobSetting);
      assertTrue(pushJobSetting.d2Routing);
    }
  }

  @Test
  public void testGetPushJobSettingShouldNotUseD2RoutingIfControllerUrlDoesNotStartWithD2() {
    Properties props = getVpjRequiredProperties();
    props.put(VENICE_DISCOVER_URL_PROP, "http://venice.db:9898");
    try (VenicePushJob vpj = new VenicePushJob(PUSH_JOB_ID, props)) {
      PushJobSetting pushJobSetting = vpj.getPushJobSetting();
      assertNotNull(pushJobSetting);
      assertFalse(pushJobSetting.d2Routing);
    }
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Incremental push is not supported while using Kafka Input Format")
  public void testGetPushJobSettingShouldThrowExceptionIfSourceIsKafkaAndJobIsIncPush() {
    Properties props = getVpjRequiredProperties();
    props.put(SOURCE_KAFKA, true);
    props.put(INCREMENTAL_PUSH, true);
    new VenicePushJob(PUSH_JOB_ID, props);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Source ETL is not supported while using Kafka Input Format")
  public void testGetPushJobSettingShouldThrowExceptionWhenBothSourceKafkaAndEtlAreSet() {
    Properties props = getVpjRequiredProperties();
    props.put(SOURCE_KAFKA, true);
    props.put(SOURCE_ETL, true);
    new VenicePushJob(PUSH_JOB_ID, props);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*not supported together with write compute.*")
  public void testGetPushJobSettingThrowsWhenProjectionEnabledWithWriteCompute() {
    Properties props = getVpjRequiredProperties();
    props.put(TARGET_WRITER_VALUE_SCHEMA_ID_PROP, 7);
    props.put(ENABLE_WRITE_COMPUTE, true);
    new VenicePushJob(PUSH_JOB_ID, props);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*not supported together with incremental push.*")
  public void testGetPushJobSettingThrowsWhenProjectionEnabledWithIncrementalPush() {
    Properties props = getVpjRequiredProperties();
    props.put(TARGET_WRITER_VALUE_SCHEMA_ID_PROP, 7);
    props.put(INCREMENTAL_PUSH, true);
    new VenicePushJob(PUSH_JOB_ID, props);
  }

  @Test
  public void testGetPushJobSettingDoesNotThrowWhenProjectionEnabledForBatchPush() {
    Properties props = getVpjRequiredProperties();
    props.put(TARGET_WRITER_VALUE_SCHEMA_ID_PROP, 7);
    try (VenicePushJob vpj = new VenicePushJob(PUSH_JOB_ID, props)) {
      PushJobSetting setting = vpj.getPushJobSetting();
      assertEquals(setting.targetWriterValueSchemaId, 7);
    }
  }

  @Test
  public void testPushJobSettingWithD2Routing() {
    ControllerClient client = getClient(storeInfo -> {
      Version version = new VersionImpl(TEST_STORE, REPUSH_VERSION, TEST_PUSH);
      storeInfo.setWriteComputationEnabled(true);
      storeInfo.setVersions(Collections.singletonList(version));
      storeInfo.setHybridStoreConfig(new HybridStoreConfigImpl(0, 0, 0, null));
    });
    try (VenicePushJob pushJob = getSpyVenicePushJobWithD2Routing(new Properties(), client)) {
      PushJobSetting pushJobSetting = pushJob.getPushJobSetting();
      Assert.assertTrue(pushJobSetting.d2Routing);
      Assert.assertEquals(pushJobSetting.controllerD2ServiceName, TEST_CHILD_CONTROLLER_D2_SERVICE);
      Assert.assertEquals(pushJobSetting.childControllerRegionD2ZkHosts, TEST_ZK_ADDRESS);
    }

    try (VenicePushJob multiRegionPushJob = getSpyVenicePushJobWithMultiRegionD2Routing(new Properties(), client)) {
      PushJobSetting multiRegionPushJobSetting = multiRegionPushJob.getPushJobSetting();
      Assert.assertTrue(multiRegionPushJobSetting.d2Routing);
      Assert.assertEquals(multiRegionPushJobSetting.controllerD2ServiceName, TEST_PARENT_CONTROLLER_D2_SERVICE);
      Assert.assertEquals(multiRegionPushJobSetting.parentControllerRegionD2ZkHosts, TEST_PARENT_ZK_ADDRESS);
    }
  }

  @Test
  public void testPushJobSettingWithLivenessHeartbeat() {
    Properties vpjProps = new Properties();
    vpjProps.setProperty(HEARTBEAT_ENABLED_CONFIG.getConfigName(), "true");
    ControllerClient client = getClient(storeInfo -> {
      Version version = new VersionImpl(TEST_STORE, REPUSH_VERSION, TEST_PUSH);
      storeInfo.setWriteComputationEnabled(true);
      storeInfo.setVersions(Collections.singletonList(version));
      storeInfo.setHybridStoreConfig(new HybridStoreConfigImpl(0, 0, 0, null));
    });
    try (VenicePushJob pushJob = getSpyVenicePushJob(vpjProps, client)) {
      PushJobSetting pushJobSetting = pushJob.getPushJobSetting();
      Assert.assertTrue(pushJobSetting.livenessHeartbeatEnabled);
    }
  }

  @Test
  public void testResolveD2ClientWithExternalD2Client() {
    D2Client mockD2Client = mock(D2Client.class);

    Properties baseProps = TestWriteUtils.defaultVPJProps(TEST_URL, TEST_PATH, TEST_STORE, Collections.emptyMap());
    baseProps.put(CONTROLLER_REQUEST_RETRY_ATTEMPTS, 1);

    // When an external D2Client is provided, resolveD2Client should return it directly
    VenicePushJob pushJob = new VenicePushJob(TEST_PUSH, baseProps, mockD2Client);
    D2Client resolved = pushJob.resolveD2Client("someZkHost", Optional.empty());
    assertEquals(resolved, mockD2Client);
  }
}
