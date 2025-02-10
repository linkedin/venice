package com.linkedin.venice.controller.repush;

/**
 * Data model of response from a repush job trigger request for a store
 */
public class RepushJobResponse {
  private final String storeName;
  private final String execId;
  private final String execUrl;

  public RepushJobResponse(String storeName, String execId, String execUrl) {
    this.storeName = storeName;
    this.execId = execId;
    this.execUrl = execUrl;
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

}
