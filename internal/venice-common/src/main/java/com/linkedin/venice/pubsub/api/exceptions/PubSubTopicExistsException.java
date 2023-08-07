package com.linkedin.venice.pubsub.api.exceptions;

import com.linkedin.venice.pubsub.api.PubSubTopic;


public class PubSubTopicExistsException extends PubSubClientException {
  public PubSubTopicExistsException(String customMessage) {
    super(customMessage);
  }

  public PubSubTopicExistsException(PubSubTopic pubSubTopic, Throwable cause) {
    super("Topic " + pubSubTopic.getName() + " already exists", cause);
  }
}
