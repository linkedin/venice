package com.linkedin.venice.controller.repush;

/**
 * Data model of response from a repush job trigger request
 */
public class RepushJobResponse {
  private final String storeName;
  private final String jobName;
  private final String jobExecId;

  // TODO: airflow: storeName from conf, jobName from dag_id, jobExecId from dag_run_id

  public RepushJobResponse(String storeName, String jobName, String jobId) {
    this.storeName = storeName;
    this.jobName = jobName;
    this.jobExecId = jobId;
  }

  public String getStoreName() {
    return storeName;
  }

  public String getJobName() {
    return jobName;
  }

  public String getJobExecId() {
    return jobExecId;
  }
}
