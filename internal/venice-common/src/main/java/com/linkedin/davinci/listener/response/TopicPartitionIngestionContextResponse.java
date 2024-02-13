package com.linkedin.davinci.listener.response;

public class TopicPartitionIngestionContextResponse {
  private boolean isError;
  private byte[] topicPartitionIngestionContext;
  private String message;

  public TopicPartitionIngestionContextResponse() {
  }

  public void setTopicPartitionIngestionContext(byte[] topicPartitionIngestionContext) {
    this.topicPartitionIngestionContext = topicPartitionIngestionContext;
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

  public byte[] getTopicPartitionIngestionContext() {
    return topicPartitionIngestionContext;
  }
}
