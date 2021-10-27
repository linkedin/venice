package com.linkedin.venice.meta;

import com.linkedin.venice.ConfigKeys;
import java.io.IOException;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestLiveClusterConfig {
  private static final String CONFIGURED_REGION = "ConfiguredRegion";
  private static final String NON_CONFIGURED_REGION = "NonConfiguredRegion";

  private static final ObjectMapper objectMapper = new ObjectMapper();
  static final String serialized = String.format("{\"%s\":{\"%s\": 1500},\"%s\":true}", ConfigKeys.SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND,
      CONFIGURED_REGION, ConfigKeys.ALLOW_STORE_MIGRATION);

  @Test
  public void deserializesAsJson() throws IOException {
    LiveClusterConfig config = objectMapper.readValue(serialized, LiveClusterConfig.class);
    Assert.assertEquals(config.getServerKafkaFetchQuotaRecordsPerSecond().size(), 1);
    Assert.assertEquals(config.getServerKafkaFetchQuotaRecordsPerSecondForRegion(CONFIGURED_REGION), 1500);
    Assert.assertEquals(config.getServerKafkaFetchQuotaRecordsPerSecondForRegion(NON_CONFIGURED_REGION), LiveClusterConfig.DEFAULT_SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND);
    Assert.assertTrue(config.isStoreMigrationAllowed());
  }

  @Test
  public void serializesAsJson() throws IOException {
    LiveClusterConfig config = new LiveClusterConfig();
    config.setServerKafkaFetchQuotaRecordsPerSecondForRegion(CONFIGURED_REGION, 1500);

    String serializedTestObj = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(config);
    Assert.assertEquals(objectMapper.readTree(serializedTestObj), objectMapper.readTree(serialized));
  }
}
