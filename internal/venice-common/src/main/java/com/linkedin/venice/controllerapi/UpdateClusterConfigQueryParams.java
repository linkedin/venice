package com.linkedin.venice.controllerapi;

import static com.linkedin.venice.ConfigKeys.DEGRADED_MODE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_STALLED_DRAINER_DIAGNOSTICS_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_STALLED_DRAINER_DIAGNOSTICS_HOST_ALLOWLIST;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ALLOW_STORE_MIGRATION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.CHILD_CONTROLLER_ADMIN_TOPIC_CONSUMPTION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;


public class UpdateClusterConfigQueryParams extends QueryParams {
  public UpdateClusterConfigQueryParams(Map<String, String> initialParams) {
    super(initialParams);
  }

  public UpdateClusterConfigQueryParams() {
    super();
  }

  public UpdateClusterConfigQueryParams setServerKafkaFetchQuotaRecordsPerSecondForRegion(
      String region,
      long kafkaFetchQuotaRecordsPerSecond) {
    Map<String, String> serverKafkaFetchQuotaRecordsPerSecond =
        getStringMap(SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND).orElse(new HashMap<>());
    serverKafkaFetchQuotaRecordsPerSecond.put(region, String.valueOf(kafkaFetchQuotaRecordsPerSecond));
    return (UpdateClusterConfigQueryParams) putStringMap(
        SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND,
        serverKafkaFetchQuotaRecordsPerSecond);
  }

  public Optional<Map<String, Integer>> getServerKafkaFetchQuotaRecordsPerSecond() {
    return getStringMap(SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND).map(serverKafkaFetchQuotaRecordsPerSecondStr -> {
      Map<String, Integer> serverKafkaFetchQuotaRecordsPerSecond = new HashMap<>();
      for (Map.Entry<String, String> regionToQuota: serverKafkaFetchQuotaRecordsPerSecondStr.entrySet()) {
        serverKafkaFetchQuotaRecordsPerSecond.put(regionToQuota.getKey(), Integer.parseInt(regionToQuota.getValue()));
      }

      return serverKafkaFetchQuotaRecordsPerSecond;
    });
  }

  public UpdateClusterConfigQueryParams setStoreMigrationAllowed(boolean storeMigrationAllowed) {
    return putBoolean(ALLOW_STORE_MIGRATION, storeMigrationAllowed);
  }

  public Optional<Boolean> getStoreMigrationAllowed() {
    return getBoolean(ALLOW_STORE_MIGRATION);
  }

  public UpdateClusterConfigQueryParams setChildControllerAdminTopicConsumptionEnabled(boolean consumptionEnabled) {
    return putBoolean(CHILD_CONTROLLER_ADMIN_TOPIC_CONSUMPTION_ENABLED, consumptionEnabled);
  }

  public Optional<Boolean> getChildControllerAdminTopicConsumptionEnabled() {
    return getBoolean(CHILD_CONTROLLER_ADMIN_TOPIC_CONSUMPTION_ENABLED);
  }

  public UpdateClusterConfigQueryParams setDegradedModeEnabled(boolean degradedModeEnabled) {
    return putBoolean(DEGRADED_MODE_ENABLED, degradedModeEnabled);
  }

  public Optional<Boolean> getDegradedModeEnabled() {
    return getBoolean(DEGRADED_MODE_ENABLED);
  }

  public UpdateClusterConfigQueryParams setStalledDrainerDiagnosticsEnabled(boolean enabled) {
    return putBoolean(SERVER_STALLED_DRAINER_DIAGNOSTICS_ENABLED, enabled);
  }

  public Optional<Boolean> getStalledDrainerDiagnosticsEnabled() {
    return getBoolean(SERVER_STALLED_DRAINER_DIAGNOSTICS_ENABLED);
  }

  public UpdateClusterConfigQueryParams setStalledDrainerDiagnosticsHostAllowlist(Set<String> hostnames) {
    return (UpdateClusterConfigQueryParams) putStringSet(SERVER_STALLED_DRAINER_DIAGNOSTICS_HOST_ALLOWLIST, hostnames);
  }

  public Optional<Set<String>> getStalledDrainerDiagnosticsHostAllowlist() {
    return getStringSet(SERVER_STALLED_DRAINER_DIAGNOSTICS_HOST_ALLOWLIST);
  }

  // ***************** above this line are getters and setters *****************

  private UpdateClusterConfigQueryParams putBoolean(String name, boolean value) {
    return (UpdateClusterConfigQueryParams) add(name, value);
  }

  private Optional<Boolean> getBoolean(String name) {
    return Optional.ofNullable(params.get(name)).map(Boolean::valueOf);
  }
}
