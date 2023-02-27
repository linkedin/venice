package com.linkedin.davinci.ingestion.consumption;

import com.linkedin.venice.annotation.NotThreadsafe;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;


/**
 * An abstraction of a receiver of data consumed from a message queue. In other words, its an abstraction that accepts data
 * consumed from a message queue. Note that it is NOT thread-safe.
 * This abstraction may be converted to a more explicit partition buffer interface when we introduce the partition buffer
 * interface to avoid potential confusion.
 *
 * @param <MESSAGE> Type of consumed data. For example, in the Kafka case, this type could be {@link org.apache.kafka.clients.consumer.ConsumerRecords}
 */
@NotThreadsafe
public interface ConsumedDataReceiver<MESSAGE> {
  /**
   * This method accepts data consumed from a queue and it should be non-blocking. This method may throw an exception
   * if write is not successful. No exception being thrown means write is successful.
   *
   * @param consumedData Consumed data.
   */
  void write(MESSAGE consumedData) throws Exception;

  /**
   * N.B.: Used for defensive coding. Today, this is exclusively used to return the version-topic name. If this is to
   * be expanded to other usages in the future, we should consider carefully if it needs refactoring.
   *
   * @return an identifier of where the data is going.
   */
  PubSubTopic destinationIdentifier();

  void notifyOfTopicDeletion(String topicName);

  PubSubTopicPartition getPubSubTopicPartition();
}
