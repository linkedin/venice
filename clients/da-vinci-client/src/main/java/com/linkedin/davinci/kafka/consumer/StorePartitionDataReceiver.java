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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
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
  private final boolean timeCheckpointsEnabled;
  private final Object checkpointLifecycleLock;
  private boolean checkpointFailed;
  private final String checkpointSource;
  private final long intervalNs;
  private final long spreadNs;
  private PeriodicCheckpoint periodicCheckpoint;
  private final LongSupplier nanoTime;
  private volatile boolean subscribed = true;
  private PartitionConsumptionState checkpointState;

  public StorePartitionDataReceiver(
      StoreIngestionTask storeIngestionTask,
      PubSubTopicPartition topicPartition,
      String kafkaUrl,
      int kafkaClusterId) {
    this(storeIngestionTask, topicPartition, kafkaUrl, kafkaClusterId, System::nanoTime);
  }

  StorePartitionDataReceiver(
      StoreIngestionTask storeIngestionTask,
      PubSubTopicPartition topicPartition,
      String kafkaUrl,
      int kafkaClusterId,
      LongSupplier nanoTime) {
    this.storeIngestionTask = Validate.notNull(storeIngestionTask);
    this.topicPartition = Validate.notNull(topicPartition);
    this.kafkaUrl = Validate.notNull(kafkaUrl);
    this.kafkaUrlForLogger = Utils.getSanitizedStringForLogger(kafkaUrl);
    this.kafkaClusterId = kafkaClusterId;
    this.LOGGER = LogManager.getLogger(this.getClass().getSimpleName() + " [" + kafkaUrlForLogger + "]");
    this.receivedRecordsCount = 0L;
    this.nanoTime = nanoTime;
    this.timeCheckpointsEnabled = storeIngestionTask.isTimeBasedCheckpointEnabled();
    this.checkpointLifecycleLock = timeCheckpointsEnabled ? new Object() : null;
    this.checkpointSource =
        timeCheckpointsEnabled ? StoreIngestionTask.checkpointSource(topicPartition, kafkaUrl) : null;
    this.intervalNs = timeCheckpointsEnabled
        ? TimeUnit.SECONDS.toNanos(storeIngestionTask.getServerConfig().getDatabaseSyncIntervalSeconds())
        : 0;
    this.spreadNs = timeCheckpointsEnabled
        ? TimeUnit.SECONDS.toNanos(storeIngestionTask.getServerConfig().getDatabaseSyncSpreadSeconds())
        : 0;
  }

  @Override
  public boolean requiresPollCallback() {
    return timeCheckpointsEnabled;
  }

  @Override
  public synchronized void onPoll() throws Exception {
    if (!timeCheckpointsEnabled || !subscribed || !storeIngestionTask.isRunning()) {
      return;
    }
    PartitionConsumptionState pcs = bindCheckpointState();
    if (pcs == null || checkpointFailed) {
      return;
    }
    try {
      synchronized (pcs.getCheckpointProgress().admissionLock) {
        PeriodicCheckpoint checkpoint = periodicCheckpoint;
        BooleanSupplier valid = () -> subscribed && storeIngestionTask.isRunning()
            && storeIngestionTask.getPartitionConsumptionState(topicPartition.getPartitionNumber()) == pcs
            && pcs.getCheckpointProgress().isValid()
            && pcs.getCheckpointProgress().source(checkpointSource) == checkpoint
            && checkpoint.isDue(nanoTime.getAsLong());
        checkpoint.maybeCheckpoint(nanoTime.getAsLong(), () -> {
          CompletableFuture<Void> tracked = new CompletableFuture<>();
          synchronized (checkpointLifecycleLock) {
            if (!subscribed) {
              return null;
            }
            // Reserve before any potentially blocking enqueue/produce. Unsubscribe need only invalidate under
            // this short lock; teardown's barrier then includes work not yet present in the drainer queue.
            pcs.getCheckpointProgress().trackCheckpoint(tracked);
          }
          try {
            CompletableFuture<Void> future =
                storeIngestionTask.checkpointOnTime(topicPartition, kafkaUrl, kafkaClusterId, pcs, valid);
            if (future == null) {
              tracked.complete(null);
            } else {
              future.whenComplete((ignored, failure) -> {
                if (failure == null) {
                  tracked.complete(null);
                } else {
                  tracked.completeExceptionally(failure);
                }
              });
            }
            return future;
          } catch (InterruptedException | RuntimeException e) {
            tracked.completeExceptionally(e);
            throw e;
          }
        });
      }
    } catch (InterruptedException e) {
      // An actual shared-consumer interruption must retain its normal shutdown semantics.
      Thread.currentThread().interrupt();
      throw e;
    } catch (Exception e) {
      // A periodic checkpoint failure belongs to this replica, not every subscription on this shared consumer.
      LOGGER.error("Time checkpoint failed for {}", topicPartition, e);
      checkpointFailed = true;
      storeIngestionTask.setIngestionException(pcs.getPartition(), e);
    }
  }

  @Override
  public void onUnsubscription() {
    if (timeCheckpointsEnabled) {
      synchronized (checkpointLifecycleLock) {
        subscribed = false;
      }
    } else {
      subscribed = false;
    }
  }

  @Override
  public void write(List<DefaultPubSubMessage> consumedData) throws Exception {
    if (timeCheckpointsEnabled) {
      synchronized (this) {
        writeRecords(consumedData);
      }
    } else {
      writeRecords(consumedData);
    }
  }

  private void writeRecords(List<DefaultPubSubMessage> consumedData) throws Exception {
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
      if (timeCheckpointsEnabled) {
        PartitionConsumptionState pcs = bindCheckpointState();
        if (subscribed && pcs != null && pcs.getCheckpointProgress().isValid()) {
          // Serialize source admission with poll snapshots, including AA sources with different per-key locks.
          synchronized (pcs.getCheckpointProgress().admissionLock) {
            storeIngestionTask
                .produceToStoreBufferServiceOrKafka(consumedData, topicPartition, kafkaUrl, kafkaClusterId);
          }
        }
      } else {
        storeIngestionTask.produceToStoreBufferServiceOrKafka(consumedData, topicPartition, kafkaUrl, kafkaClusterId);
      }
    } catch (Exception e) {
      handleDataReceiverException(e);
    }
  }

  private PartitionConsumptionState bindCheckpointState() {
    PartitionConsumptionState current =
        storeIngestionTask.getPartitionConsumptionState(topicPartition.getPartitionNumber());
    if (current != checkpointState) {
      checkpointState = current;
      periodicCheckpoint = null;
      checkpointFailed = false;
    }
    if (current != null && periodicCheckpoint == null) {
      periodicCheckpoint = current.initializeCheckpointProgress()
          .source(
              checkpointSource,
              new PeriodicCheckpoint(
                  intervalNs,
                  spreadNs == 0 ? 0 : ThreadLocalRandom.current().nextLong(spreadNs),
                  nanoTime.getAsLong()));
    }
    return current;
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
    subscribed = false;
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
