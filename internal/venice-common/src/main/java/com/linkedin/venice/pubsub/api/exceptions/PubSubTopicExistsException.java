package com.linkedin.venice.pubsub.api.exceptions;

public class PubSubTopicExistsException extends PubSubClientException {
  public PubSubTopicExistsException(String topicName) {
    super("Topic " + topicName + " already exists");
  }

  public PubSubTopicExistsException(String topicName, Throwable cause) {
    super("Topic " + topicName + " already exists", cause);
  }
}
