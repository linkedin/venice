package com.linkedin.davinci.kafka.consumer;

import com.linkedin.avroutil1.compatibility.shaded.org.apache.commons.lang3.Validate;
import com.linkedin.davinci.ingestion.consumption.ConsumedDataReceiver;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.Utils;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class StorePartitionDataReceiver implements ConsumedDataReceiver<List<DefaultPubSubMessage>> {
  private final StoreIngestionTask storeIngestionTask;
  private final PubSubTopicPartition topicPartition;
  private final String kafkaUrl;
  private final String kafkaUrlForLogger;
  private final int kafkaClusterId;
  private final Logger LOGGER;

  private long receivedRecordsCount;

  public StorePartitionDataReceiver(
      StoreIngestionTask storeIngestionTask,
      PubSubTopicPartition topicPartition,
      String kafkaUrl,
      int kafkaClusterId) {
    this.storeIngestionTask = Validate.notNull(storeIngestionTask);
    this.topicPartition = Validate.notNull(topicPartition);
    this.kafkaUrl = Validate.notNull(kafkaUrl);
    this.kafkaUrlForLogger = Utils.getSanitizedStringForLogger(kafkaUrl);
    this.kafkaClusterId = kafkaClusterId;
    this.LOGGER = LogManager.getLogger(this.getClass().getSimpleName() + " [" + kafkaUrlForLogger + "]");
    this.receivedRecordsCount = 0L;
  }

  @Override
  public void write(List<DefaultPubSubMessage> consumedData) throws Exception {
    receivedRecordsCount += consumedData.size();
    try {
      /**
       * This function could be blocked by the following reasons:
       * 1. The pre-condition is not satisfied before producing to the shared StoreBufferService, such as value schema is not available;
       * 2. The producing is blocked by the throttling of the shared StoreBufferService;
       *
       * For #1, it is acceptable since there is a timeout for the blocking logic, and it doesn't happen very often
       * based on the operational experience;
       * For #2, the blocking caused by throttling is expected since all the ingestion tasks are sharing the
       * same StoreBufferService;
       *
       * If there are changes with the above assumptions or new blocking behaviors, we need to evaluate whether
       * we need to do some kind of isolation here, otherwise the consumptions for other store versions with the
       * same shared consumer will be affected.
       * The potential isolation strategy is:
       * 1. When detecting such kind of prolonged or infinite blocking, the following function should expose a
       * param to decide whether it should return early in those conditions;
       * 2. Once this function realizes this behavior, it could choose to temporarily {@link PubSubConsumerAdapter#pause}
       * the blocked consumptions;
       * 3. This runnable could {@link PubSubConsumerAdapter#resume} the subscriptions after some delays or
       * condition change, and there are at least two ways to make the subscription resumption without missing messages:
       * a. Keep the previous message leftover in this class and retry, and once the messages can be processed
       * without blocking, then resume the paused subscriptions;
       * b. Don't keep the message leftover in this class, but every time, rewind the offset to the checkpointed offset
       * of the corresponding {@link StoreIngestionTask} and resume subscriptions;
       *
       * For option #a, the logic is simpler and but the concern is that
       * the buffered messages inside the shared consumer and the message leftover could potentially cause
       * some GC issue, and option #b won't have this problem since {@link PubSubConsumerAdapter#pause} will drop
       * all the buffered messages for the paused partitions, but just slightly more complicate.
       *
       */
      storeIngestionTask.produceToStoreBufferServiceOrKafka(consumedData, topicPartition, kafkaUrl, kafkaClusterId);
    } catch (Exception e) {
      handleDataReceiverException(e);
    }
  }

  @Override
  public PubSubTopic destinationIdentifier() {
    return storeIngestionTask.getVersionTopic();
  }

  @Override
  public void notifyOfTopicDeletion(String topicName) {
    storeIngestionTask.setLastConsumerException(new VeniceException("Topic " + topicName + " got deleted."));
  }

  private void handleDataReceiverException(Exception e) throws Exception {
    if (ExceptionUtils.recursiveClassEquals(e, InterruptedException.class)) {
      // We sometimes wrap InterruptedExceptions, so not taking any chances...
      if (storeIngestionTask.isRunning()) {
        /**
         * Based on the order of operations in {@link KafkaStoreIngestionService#stopInner()} the ingestion
         * tasks should all be closed (and therefore not running) prior to this service here being stopped.
         * Hence, the state detected here where we get interrupted while the ingestion task is still running
         * should never happen. It's unknown whether this happens or not, and if it does, whether it carries
         * any significant consequences. For now we will only log it if it does happen, but will not take
         * any special action. Some action which we might consider taking in the future would be to call
         * {@link StoreIngestionTask#close()} here, but in the interest of keeping the shutdown flow
         * simpler, we will avoid doing this for now.
         */
        LOGGER.warn(
            "Unexpected: got interrupted prior to the {} getting closed.",
            storeIngestionTask.getClass().getSimpleName());
      }
      /**
       * We want to rethrow the interrupted exception in order to skip the quota-related code below and
       * break the run loop. We avoid calling {@link StoreIngestionTask#setLastConsumerException(Exception)}
       * as we do for other exceptions as this carries side-effects that may be undesirable.
       */
      throw e;
    }
    LOGGER.error(
        "Received {} while StoreIngestionTask is processing the polled consumer record for topic: {}. Will propagate via setLastConsumerException(e).",
        e.getClass().getSimpleName(),
        topicPartition);
    storeIngestionTask.setLastConsumerException(e);
  }

  /**
   * @return Number of data records put in the receiver, for testing purpose.
   */
  public long receivedRecordsCount() {
    return receivedRecordsCount;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "{" + "VT=" + storeIngestionTask.getVersionTopic() + ", topicPartition="
        + topicPartition + '}';
  }

  // for testing purpose only
  int getKafkaClusterId() {
    return this.kafkaClusterId;
  }
}
