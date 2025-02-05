package com.linkedin.venice.controller.repush;

import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.utils.VeniceProperties;


public class RepushOrchestratorConfig {
  public static final String AIRFLOW_URL_CFG_KEY = "airflowUrl";
  public static final String DAG_ID_CFG_KEY = "dagId";
  public static final String AIRFLOW_USERNAME_CFG_KEY = "airflowUsername";
  public static final String AIRFLOW_PASSWORD_CFG_KEY = "airflowPassword";

  private final String airflowUrl;
  private final String airflowUsername;
  private final String airflowPassword;
  private final String dagId;
  private final Client client;

  // TODO: exception here if any of the configs are empty
  public RepushOrchestratorConfig(VeniceProperties repushClusterConfigs, Client client) {
    this.airflowUrl = repushClusterConfigs.getString(AIRFLOW_URL_CFG_KEY);
    this.airflowUsername = repushClusterConfigs.getString(AIRFLOW_USERNAME_CFG_KEY);
    this.airflowPassword = repushClusterConfigs.getString(AIRFLOW_PASSWORD_CFG_KEY);
    this.dagId = repushClusterConfigs.getString(DAG_ID_CFG_KEY);
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
