package com.linkedin.davinci.listener.response;

public class TopicPartitionIngestionContextResponse {
  private boolean isError;
  private String topicPartitionIngestionContext;
  private String message;

  public TopicPartitionIngestionContextResponse() {
  }

  public void setTopicPartitionIngestionContext(String topicPartitionIngestionContext) {
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

  public String getTopicPartitionIngestionContext() {
    return topicPartitionIngestionContext;
  }
}
