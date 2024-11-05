package com.linkedin.davinci.listener.response;

public class ReplicaIngestionResponse {
  private boolean isError;
  private byte[] replicaIngestionInfoByteArray;
  private String message;

  public ReplicaIngestionResponse() {
  }

  public void setReplicaIngestionInfoByteArray(byte[] replicaIngestionInfoByteArray) {
    this.replicaIngestionInfoByteArray = replicaIngestionInfoByteArray;
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

  public String getMessage() {
    return this.message;
  }

  public byte[] getReplicaIngestionInfoByteArray() {
    return replicaIngestionInfoByteArray;
  }
}
