package com.linkedin.venice.controller.repush;

/**
 * Data model of response from a repush job trigger request for a store
 */
public class RepushJobResponse {
  private final String storeName;
  private final String execId;
  private final String execUrl;
  private final String execDate;

  // TODO LC: airflow: storeName from conf, jobName from dag_id, jobExecId from dag_run_id

  public RepushJobResponse(String storeName, String jobName, String jobId, String jobExecUrl, String execDate) {
    this.storeName = storeName;
    this.execId = jobId;
    this.execUrl = jobExecUrl;
    this.execDate = execDate;
  }

  public String getStoreName() {
    return storeName;
  }

  public String getExecId() {
    return execId;
  }

  public String getExecUrl() {
    return execUrl;
  }

  public String getExecDate() {
    return execDate;
  }
}
