package com.linkedin.venice.blobtransfer;

import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import java.util.ArrayList;
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

  public void addPartition(Partition partition) {
    List<String> hostNames = new ArrayList<>();
    for (Instance instance: partition.getReadyToServeInstances()) {
      String host = instance.getHost();
      hostNames.add(host);
    }
    setDiscoveryResult(hostNames);
  }

}
