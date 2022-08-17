package com.linkedin.davinci.ingestion.consumption;

import java.util.Map;
import java.util.concurrent.CompletableFuture;


/**
 * This interface is an abstraction provided to StoreIngestionTask and it allows its users to start/stop consuming a
 * given message queue. It should be a singleton object, meaning one per Venice server (storage node).
 *
 * All methods this interface provides are non-blocking async methods for below reasons:
 *
 *  1. In most (if not all) cases, the only two ways to handle failures when calling any API on interface is
 *     to retry or let the caller throw an exception to fail out loud. Because, for example, when this interface is used
 *     in our Venice server storage node to consume from Kafka topic partitions, say, {@link this#startConsuming} fails. What
 *     could be done here to handle its failure? Either retry or fail the whole ingestion task. Retry can happen inside
 *     the implementation of this interface and reported as metrics that eventually trigger alerts. On the other hand, caller
 *     can store the returned {@link CompletableFuture} and revisit them later, if any one of them is completed with an
 *     exception, the caller thread can look at the exception type and decide whether to fail out loud.
 *     Hence this non-blocking async design allows API users to handle both situations (retry and fail out loud).
 *
 *  2. Flexibility. Specifically, users can easily convert a non-blocking async method to a blocking one by waiting on the
 *     {@link CompletableFuture} but not the other way around.
 *
 * @param <MESSAGE_QUEUE> Type of the message queue where data is consumed. For example, if data is consuming from Kafka,
 *                       this type could be {@link org.apache.kafka.common.TopicPartition}.
 * @param <MESSAGE> Type of consumed data. For example, in the Kafka case, this type could be {@link org.apache.kafka.clients.consumer.ConsumerRecords}
 */
public interface ConsumptionService<MESSAGE_QUEUE, MESSAGE> {
  /**
   * Start consuming a message queue at an given offset. This method must be non-blocking. In other words, it must return immediately.
   *
   * If consumption on this queue has already been started, this method moves the consuming offset to {@param startingOffset}
   * and start consuming at that offset. IOW, it changes the consumption offset.
   *
   * @param queue Queue to start consuming from.
   * @param consumedDataReceiver Where all consumed data is written to.
   * @param startingOffset Consumption starting offset.
   * @return A callback that is invoked when the consumption actually starts. If consumption fails to start, users waiting
   *         on this callback will get an exception which contains failure context.
   *         Note that the {@link CompletableFuture} should guarantee to be completed by a fixed amount of time. This
   *         guaranteed upper bound must be provided by its implementations.
   */
  CompletableFuture<Exception> startConsuming(
      MESSAGE_QUEUE queue,
      ConsumedDataReceiver<MESSAGE> consumedDataReceiver,
      long startingOffset);

  /**
   * A batch version of {@link #startConsuming}. This method could be useful when there are a large number of message queues
   * (e.g. topic partitions) that need to be consumed in a very short period of time. This situation occurs, for example,
   * when a storage node starts up.
   *
   * @param queueOffsetConsumedDataReceiverMap Self-explanatory.
   * @return Same as the return param in {@link #startConsuming(Object, ConsumedDataReceiver, long)}.
   *         Note that the {@link CompletableFuture} should guarantee to be completed by a fixed amount of time. This
   *         guaranteed upper bound must be provided by its implementations.
   */
  CompletableFuture<Exception> startBatchConsuming(
      Map<MessageQueueOffset<MESSAGE_QUEUE>, ConsumedDataReceiver<MESSAGE>> queueOffsetConsumedDataReceiverMap);

  /**
   * Stop consuming a message queue. This method must be non-blocking. In other words, it must return immediately.
   * Calling this method makes the {@link ConsumptionService} object forget about this queue completely. Calling this
   * method before calling {@link #startConsuming} is a no-op.
   *
   * @param queue Queue to stop consuming from.
   * @return A callback that is invoked when the consumption actually stopped. If consumption fails to stop, users waiting
   *         on this callback will get an exception which contains failure context.
   *         Note that the {@link CompletableFuture} should guarantee to be completed by a fixed amount of time. This
   *         guaranteed upper bound must be provided by its implementations.
   */
  CompletableFuture<Exception> stopConsuming(MESSAGE_QUEUE queue);
}
