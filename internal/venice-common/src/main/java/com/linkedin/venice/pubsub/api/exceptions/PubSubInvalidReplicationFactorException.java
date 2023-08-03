package com.linkedin.venice.pubsub.api.exceptions;

/**
 * PubSub clients should throw this exception when the replication factor is invalid as determined by the PubSub service.
 *
 * Sometimes this could be a transient error (number of brokers are less than the RF, for example),
 * so the client should consider retrying the operation.
 */
public class PubSubInvalidReplicationFactorException extends PubSubClientRetriableException {
  public PubSubInvalidReplicationFactorException(String topicName, int replicationFactor, Throwable cause) {
    super("Replication factor specified is invalid: " + topicName + " replicationFactor: " + replicationFactor, cause);
  }

  public PubSubInvalidReplicationFactorException(String message) {
    super(message);
  }

  public PubSubInvalidReplicationFactorException(String message, Throwable cause) {
    super(message, cause);
  }
}
