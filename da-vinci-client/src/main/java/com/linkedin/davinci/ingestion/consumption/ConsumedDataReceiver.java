package com.linkedin.davinci.ingestion.consumption;

import com.linkedin.venice.annotation.NotThreadsafe;


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
}
