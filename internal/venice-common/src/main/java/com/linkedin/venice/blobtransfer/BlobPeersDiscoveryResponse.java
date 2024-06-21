package com.linkedin.venice.blobtransfer;

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

  public void setErrorMessage(String message) {
    this.message = message;
  }

  public String getErrorMessage() {
    return this.message;
  }

  public void setDiscoveryResult(List<String> hostNames) {
    this.hostNameList = hostNames;
  }

  public List<String> getDiscoveryResult() {
    return this.hostNameList;
  }

}
