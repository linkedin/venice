package com.linkedin.davinci.listener.response;

import java.util.List;
import java.util.Optional;


public class BlobDiscoveryResponse {
  private boolean isError;
  private List<String> liveNodeHostNames;

  private String message;

  public BlobDiscoveryResponse() {
  }

  public void setLiveNodeNames(List<String> liveNodeHostNames) {
    this.liveNodeHostNames = liveNodeHostNames;
  }

  public List<String> getLiveNodeHostNames() {
    return liveNodeHostNames;
  }

  public void setError(boolean error) {
    this.isError = error;
  }

  public boolean isError() {
    return this.isError;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public Optional<String> getMessage() {
    return Optional.ofNullable(message);
  }
}
