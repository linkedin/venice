package com.linkedin.venice.blobtransfer;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class BlobPeersDiscoveryResponse {
  private boolean isError;

  private String message;

  private List<String> hostNameList;

  private Set<String> serverHostNames = Collections.emptySet();

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

  public void setServerHostNames(Set<String> serverHostNames) {
    this.serverHostNames =
        serverHostNames == null || serverHostNames.isEmpty() ? Collections.emptySet() : new HashSet<>(serverHostNames);
  }

  public Set<String> getServerHostNames() {
    return this.serverHostNames;
  }

}
