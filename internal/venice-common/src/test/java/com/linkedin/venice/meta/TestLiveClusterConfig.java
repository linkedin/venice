package com.linkedin.venice.meta;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestLiveClusterConfig {
  private static final String CONFIGURED_REGION = "ConfiguredRegion";
  private static final String NON_CONFIGURED_REGION = "NonConfiguredRegion";

  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
  private static final String SERIALIZED_CONFIG = String.format(
      "{\"%s\":{\"%s\": 1500},\"%s\":true,\"%s\":true,\"%s\":false,\"%s\":false,\"%s\":[],\"degraded.datacenters\":null}",
      ConfigKeys.SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND,
      CONFIGURED_REGION,
      ConfigKeys.ALLOW_STORE_MIGRATION,
      ConfigKeys.CHILD_CONTROLLER_ADMIN_TOPIC_CONSUMPTION_ENABLED,
      ConfigKeys.DEGRADED_MODE_ENABLED,
      ConfigKeys.SERVER_STALLED_DRAINER_DIAGNOSTICS_ENABLED,
      ConfigKeys.SERVER_STALLED_DRAINER_DIAGNOSTICS_HOST_ALLOWLIST);

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
    Assert.assertFalse(config.isStalledDrainerDiagnosticsEnabled());
    Assert.assertTrue(config.getStalledDrainerDiagnosticsHostAllowlist().isEmpty());
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
    Assert.assertFalse(config.isStalledDrainerDiagnosticsEnabled());
    Assert.assertTrue(config.getStalledDrainerDiagnosticsHostAllowlist().isEmpty());
  }

  @Test
  public void stalledDrainerDiagnosticsRoundTripsAndCopies() throws IOException {
    LiveClusterConfig config = new LiveClusterConfig();
    Assert.assertFalse(config.isStalledDrainerDiagnosticsEnabled());
    Set<String> hosts = new HashSet<>(Arrays.asList("server-a.example.com", "server-b.example.com"));
    config.setStalledDrainerDiagnosticsHostAllowlist(hosts);
    hosts.clear();
    for (boolean enabled: new boolean[] { true, false }) {
      config.setStalledDrainerDiagnosticsEnabled(enabled);
      LiveClusterConfig copy = new LiveClusterConfig(config);
      Assert.assertEquals(copy.isStalledDrainerDiagnosticsEnabled(), enabled);
      String json = OBJECT_MAPPER.writeValueAsString(copy);
      Assert.assertEquals(
          OBJECT_MAPPER.readTree(json).get(ConfigKeys.SERVER_STALLED_DRAINER_DIAGNOSTICS_ENABLED).booleanValue(),
          enabled);
      LiveClusterConfig decoded = OBJECT_MAPPER.readValue(json, LiveClusterConfig.class);
      Assert.assertEquals(decoded.isStalledDrainerDiagnosticsEnabled(), enabled);
      Assert.assertEquals(
          decoded.getStalledDrainerDiagnosticsHostAllowlist(),
          config.getStalledDrainerDiagnosticsHostAllowlist());
      Assert.assertEquals(decoded.isStalledDrainerDiagnosticsEnabledForHost("server-a.example.com"), enabled);
      Assert.assertEquals(copy.isStalledDrainerDiagnosticsEnabledForHost("server-b.example.com"), enabled);
      Assert.expectThrows(
          UnsupportedOperationException.class,
          () -> decoded.getStalledDrainerDiagnosticsHostAllowlist().clear());
      Assert.expectThrows(
          UnsupportedOperationException.class,
          () -> copy.getStalledDrainerDiagnosticsHostAllowlist().clear());
      Assert
          .assertTrue(copy.toString().contains(ConfigKeys.SERVER_STALLED_DRAINER_DIAGNOSTICS_ENABLED + "=" + enabled));
      Assert.assertTrue(copy.toString().contains(ConfigKeys.SERVER_STALLED_DRAINER_DIAGNOSTICS_HOST_ALLOWLIST + "="));
      copy.setStalledDrainerDiagnosticsHostAllowlist(Collections.emptySet());
      Assert.assertEquals(config.getStalledDrainerDiagnosticsHostAllowlist().size(), 2);
    }
    Assert.expectThrows(
        UnsupportedOperationException.class,
        () -> config.getStalledDrainerDiagnosticsHostAllowlist().clear());
  }

  @Test
  public void stalledDrainerDiagnosticsRequireExactAllowlistedHostnameAndMasterFlag() throws IOException {
    LiveClusterConfig config = OBJECT_MAPPER
        .readValue("{\"" + ConfigKeys.SERVER_STALLED_DRAINER_DIAGNOSTICS_ENABLED + "\":true}", LiveClusterConfig.class);
    Assert.assertFalse(config.isStalledDrainerDiagnosticsEnabledForHost("server-a.example.com"));
    config.setStalledDrainerDiagnosticsHostAllowlist(Collections.singleton("server-a.example.com"));
    Assert.assertTrue(config.isStalledDrainerDiagnosticsEnabledForHost("server-a.example.com"));
    for (String hostname: new String[] { "server-a", "SERVER-A.EXAMPLE.COM", "server-a.example.com:1234",
        "server-a.example.com_1234", "server-b.example.com", "server-a.example.com.", "*.example.com", "", null }) {
      Assert.assertFalse(config.isStalledDrainerDiagnosticsEnabledForHost(hostname));
    }
    config.setStalledDrainerDiagnosticsEnabled(false);
    Assert.assertFalse(config.isStalledDrainerDiagnosticsEnabledForHost("server-a.example.com"));
    config.setStalledDrainerDiagnosticsEnabled(true);
    config.setStalledDrainerDiagnosticsHostAllowlist(Collections.emptySet());
    Assert.assertFalse(config.isStalledDrainerDiagnosticsEnabledForHost("server-a.example.com"));
    config.setStalledDrainerDiagnosticsHostAllowlist(null);
    Assert.assertTrue(config.getStalledDrainerDiagnosticsHostAllowlist().isEmpty());
    Assert.assertFalse(config.isStalledDrainerDiagnosticsEnabledForHost("server-a.example.com"));
    LiveClusterConfig nullConfig = OBJECT_MAPPER.readValue(
        "{\"" + ConfigKeys.SERVER_STALLED_DRAINER_DIAGNOSTICS_ENABLED + "\":true,\""
            + ConfigKeys.SERVER_STALLED_DRAINER_DIAGNOSTICS_HOST_ALLOWLIST + "\":null}",
        LiveClusterConfig.class);
    Assert.assertFalse(nullConfig.isStalledDrainerDiagnosticsEnabledForHost("server-a.example.com"));
    Assert.assertTrue(new LiveClusterConfig(nullConfig).getStalledDrainerDiagnosticsHostAllowlist().isEmpty());
  }

  @Test
  public void stalledDrainerDiagnosticsDoNotExpandAliasesOrWildcards() throws IOException {
    LiveClusterConfig config = OBJECT_MAPPER.readValue(
        "{\"" + ConfigKeys.SERVER_STALLED_DRAINER_DIAGNOSTICS_ENABLED + "\":true,\""
            + ConfigKeys.SERVER_STALLED_DRAINER_DIAGNOSTICS_HOST_ALLOWLIST
            + "\":[\"server-a\",\"*.example.com\",\"\",null]}",
        LiveClusterConfig.class);
    Assert.assertEquals(
        config.getStalledDrainerDiagnosticsHostAllowlist(),
        new HashSet<>(Arrays.asList("server-a", "*.example.com", "", null)));
    Assert.assertTrue(config.isStalledDrainerDiagnosticsEnabledForHost("server-a"));
    Assert.assertFalse(config.isStalledDrainerDiagnosticsEnabledForHost("server-a.example.com"));
    Assert.assertFalse(config.isStalledDrainerDiagnosticsEnabledForHost("server-b.example.com"));
    Assert.assertFalse(config.isStalledDrainerDiagnosticsEnabledForHost(""));
    Assert.assertFalse(config.isStalledDrainerDiagnosticsEnabledForHost(null));
  }

  @Test
  public void copyConstructorCopiesDegradedModeEnabled() {
    LiveClusterConfig original = new LiveClusterConfig();
    original.setDegradedModeEnabled(true);

    LiveClusterConfig copy = new LiveClusterConfig(original);
    Assert.assertTrue(copy.isDegradedModeEnabled());
  }

  @Test
  public void oldJsonWithoutDegradedDatacentersDeserializesCleanly() throws IOException {
    String oldJson = String.format(
        "{\"%s\":{\"%s\": 1500},\"%s\":true,\"%s\":true,\"%s\":true}",
        ConfigKeys.SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND,
        CONFIGURED_REGION,
        ConfigKeys.ALLOW_STORE_MIGRATION,
        ConfigKeys.CHILD_CONTROLLER_ADMIN_TOPIC_CONSUMPTION_ENABLED,
        ConfigKeys.DEGRADED_MODE_ENABLED);

    LiveClusterConfig config = OBJECT_MAPPER.readValue(oldJson, LiveClusterConfig.class);
    Assert.assertTrue(config.isDegradedModeEnabled());
    Assert.assertNull(config.getDegradedDatacenters());
    Assert.assertFalse(config.isDatacenterDegraded("dc-1"));
  }

  @Test
  public void addRemoveDegradedDatacenter() {
    LiveClusterConfig config = new LiveClusterConfig();
    Assert.assertFalse(config.isDatacenterDegraded("dc-1"));

    config.addDegradedDatacenter("dc-1", new DegradedDcInfo(123L, 60, "operator"));
    Assert.assertTrue(config.isDatacenterDegraded("dc-1"));
    Assert.assertEquals(config.getDegradedDatacenters().size(), 1);
    Assert.assertEquals(config.getDegradedDatacenters().get("dc-1").getOperatorId(), "operator");

    config.removeDegradedDatacenter("dc-1");
    Assert.assertFalse(config.isDatacenterDegraded("dc-1"));
    Assert.assertEquals(config.getDegradedDatacenters().size(), 0);
  }

  @Test
  public void copyConstructorDeepCopiesDegradedDatacenters() {
    LiveClusterConfig original = new LiveClusterConfig();
    original.addDegradedDatacenter("dc-1", new DegradedDcInfo(123L, 60, "operator"));

    LiveClusterConfig copy = new LiveClusterConfig(original);
    Assert.assertTrue(copy.isDatacenterDegraded("dc-1"));

    // Mutating the copy should not affect the original (deep copy).
    copy.removeDegradedDatacenter("dc-1");
    Assert.assertFalse(copy.isDatacenterDegraded("dc-1"));
    Assert.assertTrue(original.isDatacenterDegraded("dc-1"));
  }
}
