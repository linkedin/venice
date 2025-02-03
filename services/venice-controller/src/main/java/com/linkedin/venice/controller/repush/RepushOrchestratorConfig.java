package com.linkedin.venice.controller.repush;

import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.utils.VeniceProperties;


public class RepushOrchestratorConfig {
  private final String airflowUrl;
  private final String airflowUsername;
  private final String airflowPassword;
  private final String dagId;
  private final Client client;

  public RepushOrchestratorConfig(VeniceProperties repushClusterConfigs, Client client) {
    this.airflowUrl = repushClusterConfigs.getString("airflowUrl");
    this.airflowUsername = repushClusterConfigs.getString("airflowUsername");
    this.airflowPassword = repushClusterConfigs.getString("airflowPassword");
    this.dagId = repushClusterConfigs.getString("dagId");
    this.client = client;
  }

  public String getAirflowUrl() {
    return airflowUrl;
  }

  public String getAirflowUsername() {
    return airflowUsername;
  }

  public String getAirflowPassword() {
    return airflowPassword;
  }

  public String getDagId() {
    return dagId;
  }

  public Client getClient() {
    return client;
  }
}
