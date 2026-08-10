package com.linkedin.venice.blobtransfer;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class BlobPeersDiscoveryResponse {
  private boolean isError;

  private String message;

  private List<String> hostNameList;

  /**
   * Normalized hosts known to be Venice servers. Hosts not present in this set are treated as Da Vinci peer senders
   * when {@link #sourceAware} is true.
   */
  private Set<String> serverHostNames = Collections.emptySet();

  /**
   * True when discovery can distinguish Da Vinci peer senders from Venice server senders for source-attributed
   * blob-transfer metrics.
   */
  private boolean sourceAware;

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
    this.serverHostNames = serverHostNames == null || serverHostNames.isEmpty()
        ? Collections.emptySet()
        : Collections.unmodifiableSet(new HashSet<>(serverHostNames));
  }

  public Set<String> getServerHostNames() {
    return this.serverHostNames;
  }

  public void setSourceAware(boolean sourceAware) {
    this.sourceAware = sourceAware;
  }

  public boolean isSourceAware() {
    return sourceAware;
  }

}
