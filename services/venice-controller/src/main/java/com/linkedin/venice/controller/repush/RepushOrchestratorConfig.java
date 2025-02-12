package com.linkedin.venice.controller.repush;

import com.linkedin.venice.utils.VeniceProperties;


public class RepushOrchestratorConfig {
  public static final String AIRFLOW_URL_CFG_KEY = "airflow.url";
  public static final String DAG_ID_CFG_KEY = "dagId";
  public static final String AIRFLOW_USERNAME_CFG_KEY = "airflow.username";
  public static final String AIRFLOW_PASSWORD_CFG_KEY = "airflow.password";

  private final String airflowUrl;
  private final String airflowUsername;
  private final String airflowPassword;
  private final String dagId;

  // TODO: exception here if any of the configs are empty
  public RepushOrchestratorConfig(VeniceProperties repushClusterConfigs) {
    this.airflowUrl = repushClusterConfigs.getString(AIRFLOW_URL_CFG_KEY);
    this.airflowUsername = repushClusterConfigs.getString(AIRFLOW_USERNAME_CFG_KEY);
    this.airflowPassword = repushClusterConfigs.getString(AIRFLOW_PASSWORD_CFG_KEY);
    this.dagId = repushClusterConfigs.getString(DAG_ID_CFG_KEY);
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
}
