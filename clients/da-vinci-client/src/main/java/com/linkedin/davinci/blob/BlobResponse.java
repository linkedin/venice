package com.linkedin.davinci.blob;

import java.util.List;
import java.util.Objects;


public class BlobResponse {
  private boolean isError;

  private String message;

  private List<String> partitionURLs;

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

  public void setPartitionValues(List<String> partitionURLs) {
    this.partitionURLs = partitionURLs;
  }

  public List<String> getPartitionValues() {
    return this.partitionURLs;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof BlobResponse)) {
      return false;
    }
    BlobResponse other = (BlobResponse) obj;
    return other.isError == this.isError && Objects.equals(other.message, this.message) && other.partitionURLs != null
        && other.partitionURLs.equals(this.partitionURLs);
  }

}
