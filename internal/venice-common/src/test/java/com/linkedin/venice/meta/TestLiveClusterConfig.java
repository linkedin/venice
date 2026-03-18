package com.linkedin.venice.meta;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestLiveClusterConfig {
  private static final String CONFIGURED_REGION = "ConfiguredRegion";
  private static final String NON_CONFIGURED_REGION = "NonConfiguredRegion";

  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
  private static final String SERIALIZED_CONFIG = String.format(
      "{\"%s\":{\"%s\": 1500},\"%s\":true,\"%s\":true,\"%s\":false}",
      ConfigKeys.SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND,
      CONFIGURED_REGION,
      ConfigKeys.ALLOW_STORE_MIGRATION,
      ConfigKeys.CHILD_CONTROLLER_ADMIN_TOPIC_CONSUMPTION_ENABLED,
      ConfigKeys.DEGRADED_MODE_ENABLED);

  @Test
  public void deserializesAsJson() throws IOException {
    LiveClusterConfig config = OBJECT_MAPPER.readValue(SERIALIZED_CONFIG, LiveClusterConfig.class);
    Assert.assertEquals(config.getServerKafkaFetchQuotaRecordsPerSecond().size(), 1);
    Assert.assertEquals(config.getServerKafkaFetchQuotaRecordsPerSecondForRegion(CONFIGURED_REGION), 1500);
    Assert.assertEquals(
        config.getServerKafkaFetchQuotaRecordsPerSecondForRegion(NON_CONFIGURED_REGION),
        LiveClusterConfig.DEFAULT_SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND);
    Assert.assertTrue(config.isStoreMigrationAllowed());
    Assert.assertFalse(config.isDegradedModeEnabled());
  }

  @Test
  public void serializesAsJson() throws IOException {
    LiveClusterConfig config = new LiveClusterConfig();
    config.setServerKafkaFetchQuotaRecordsPerSecondForRegion(CONFIGURED_REGION, 1500);

    String serializedTestObj = OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(config);
    Assert.assertEquals(OBJECT_MAPPER.readTree(serializedTestObj), OBJECT_MAPPER.readTree(SERIALIZED_CONFIG));
  }

  @Test
  public void oldJsonWithoutDegradedModeEnabledDeserializesCleanly() throws IOException {
    String oldJson = String.format(
        "{\"%s\":{\"%s\": 1500},\"%s\":true,\"%s\":true}",
        ConfigKeys.SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND,
        CONFIGURED_REGION,
        ConfigKeys.ALLOW_STORE_MIGRATION,
        ConfigKeys.CHILD_CONTROLLER_ADMIN_TOPIC_CONSUMPTION_ENABLED);

    LiveClusterConfig config = OBJECT_MAPPER.readValue(oldJson, LiveClusterConfig.class);
    Assert.assertFalse(config.isDegradedModeEnabled());
    Assert.assertTrue(config.isStoreMigrationAllowed());
  }

  @Test
  public void copyConstructorCopiesDegradedModeEnabled() {
    LiveClusterConfig original = new LiveClusterConfig();
    original.setDegradedModeEnabled(true);

    LiveClusterConfig copy = new LiveClusterConfig(original);
    Assert.assertTrue(copy.isDegradedModeEnabled());
  }
}
