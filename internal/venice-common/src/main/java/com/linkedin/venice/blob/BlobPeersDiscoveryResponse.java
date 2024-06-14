package com.linkedin.venice.blob;

import java.util.List;


public class BlobPeersDiscoveryResponse {
  private boolean isError;

  private String message;

  private List<String> hostNameList;

  public void setError(boolean error) {
    this.isError = error;
  }

  public boolean isError() {
    return this.isError;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public String getMessage() {
    return this.message;
  }

  public void setDiscoveryResult(List<String> hostNames) {
    this.hostNameList = hostNames;
  }

  public List<String> getDiscoveryResult() {
    return this.hostNameList;
  }

}
