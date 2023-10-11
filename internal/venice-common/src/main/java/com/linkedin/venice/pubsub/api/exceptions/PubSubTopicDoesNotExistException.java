package com.linkedin.venice.pubsub.api.exceptions;

import com.linkedin.venice.pubsub.api.PubSubTopic;


/**
 * The source or destination topic for the replication request does not exit
 */
public class PubSubTopicDoesNotExistException extends PubSubClientRetriableException {
  public PubSubTopicDoesNotExistException(String message) {
    super(message);
  }

  public PubSubTopicDoesNotExistException(String message, Throwable cause) {
    super(message, cause);
  }

  public PubSubTopicDoesNotExistException(PubSubTopic topic) {
    super(String.format("Topic %s does not exist", topic.getName()));
  }

  public PubSubTopicDoesNotExistException(PubSubTopic topic, Throwable cause) {
    super(String.format("Topic %s does not exist", topic.getName()), cause);
  }
}
