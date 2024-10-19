package com.linkedin.davinci.kafka.consumer;

import com.linkedin.avroutil1.compatibility.shaded.org.apache.commons.lang3.Validate;
import com.linkedin.davinci.ingestion.consumption.ConsumedDataReceiver;
import com.linkedin.davinci.stats.HostLevelIngestionStats;
import com.linkedin.davinci.utils.ByteArrayKey;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.Update;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.ValueHolder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class StorePartitionDataReceiver
    implements ConsumedDataReceiver<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> {
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
  public void write(List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> consumedData) throws Exception {
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
      produceToStoreBufferServiceOrKafka(consumedData, topicPartition, kafkaUrl, kafkaClusterId);
    } catch (Exception e) {
      handleDataReceiverException(e);
    }
  }

  /**
   * This function is in charge of producing the consumer records to the writer buffers maintained by {@link StoreBufferService}.
   *
   * This function may modify the original record in KME and it is unsafe to use the payload from KME directly after this call.
   *
   * @param records : received consumer records
   */
  protected void produceToStoreBufferServiceOrKafka(
      Iterable<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> records,
      PubSubTopicPartition topicPartition,
      String kafkaUrl,
      int kafkaClusterId) throws InterruptedException {
    final int partition = topicPartition.getPartitionNumber();
    PartitionConsumptionState partitionConsumptionState = storeIngestionTask.getPartitionConsumptionState(partition);
    if (partitionConsumptionState == null) {
      throw new VeniceException(
          "PartitionConsumptionState should present for store version: " + storeIngestionTask.getKafkaVersionTopic()
              + ", partition: " + partition);
    }

    /**
     * Validate and filter out duplicate messages from the real-time topic as early as possible, so that
     * the following batch processing logic won't spend useless efforts on duplicate messages.
     */
    records =
        storeIngestionTask.validateAndFilterOutDuplicateMessagesFromLeaderTopic(records, kafkaUrl, topicPartition);

    if (storeIngestionTask.shouldProduceInBatch(records)) {
      produceToStoreBufferServiceOrKafkaInBatch(
          records,
          topicPartition,
          partitionConsumptionState,
          kafkaUrl,
          kafkaClusterId);
      return;
    }

    long totalBytesRead = 0;
    ValueHolder<Double> elapsedTimeForPuttingIntoQueue = new ValueHolder<>(0d);
    boolean metricsEnabled = storeIngestionTask.isMetricsEmissionEnabled();
    long beforeProcessingBatchRecordsTimestampMs = System.currentTimeMillis();

    for (PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record: records) {
      long beforeProcessingPerRecordTimestampNs = System.nanoTime();
      partitionConsumptionState.setLatestPolledMessageTimestampInMs(beforeProcessingBatchRecordsTimestampMs);
      if (!storeIngestionTask.shouldProcessRecord(record)) {
        partitionConsumptionState.updateLatestIgnoredUpstreamRTOffset(kafkaUrl, record.getOffset());
        continue;
      }

      // Check schema id availability before putting consumer record to drainer queue
      waitReadyToProcessRecord(record);

      totalBytesRead += handleSingleMessage(
          new PubSubMessageProcessedResultWrapper<>(record),
          topicPartition,
          partitionConsumptionState,
          kafkaUrl,
          kafkaClusterId,
          beforeProcessingPerRecordTimestampNs,
          beforeProcessingBatchRecordsTimestampMs,
          metricsEnabled,
          elapsedTimeForPuttingIntoQueue);
    }

    updateMetricsAndEnforceQuota(
        totalBytesRead,
        partition,
        elapsedTimeForPuttingIntoQueue,
        beforeProcessingBatchRecordsTimestampMs);
  }

  public void produceToStoreBufferServiceOrKafkaInBatch(
      Iterable<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> records,
      PubSubTopicPartition topicPartition,
      PartitionConsumptionState partitionConsumptionState,
      String kafkaUrl,
      int kafkaClusterId) throws InterruptedException {
    long totalBytesRead = 0;
    ValueHolder<Double> elapsedTimeForPuttingIntoQueue = new ValueHolder<>(0d);
    boolean metricsEnabled = storeIngestionTask.isMetricsEmissionEnabled();
    long beforeProcessingBatchRecordsTimestampMs = System.currentTimeMillis();
    /**
     * Split the records into mini batches.
     */
    int batchSize = storeIngestionTask.getServerConfig().getAAWCWorkloadParallelProcessingThreadPoolSize();
    List<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> batches = new ArrayList<>();
    List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> ongoingBatch = new ArrayList<>(batchSize);
    Iterator<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> iter = records.iterator();
    while (iter.hasNext()) {
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record = iter.next();
      if (partitionConsumptionState != null) {
        partitionConsumptionState.setLatestPolledMessageTimestampInMs(beforeProcessingBatchRecordsTimestampMs);
      }
      if (!storeIngestionTask.shouldProcessRecord(record)) {
        if (partitionConsumptionState != null) {
          partitionConsumptionState.updateLatestIgnoredUpstreamRTOffset(kafkaUrl, record.getOffset());
        }
        continue;
      }
      waitReadyToProcessRecord(record);
      ongoingBatch.add(record);
      if (ongoingBatch.size() == batchSize) {
        batches.add(ongoingBatch);
        ongoingBatch = new ArrayList<>(batchSize);
      }
    }
    if (!ongoingBatch.isEmpty()) {
      batches.add(ongoingBatch);
    }
    if (batches.isEmpty()) {
      return;
    }
    IngestionBatchProcessor ingestionBatchProcessor = storeIngestionTask.getIngestionBatchProcessor();
    if (ingestionBatchProcessor == null) {
      throw new VeniceException(
          "IngestionBatchProcessor object should present for store version: "
              + storeIngestionTask.getKafkaVersionTopic());
    }
    /**
     * Process records batch by batch.
     */
    for (List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> batch: batches) {
      NavigableMap<ByteArrayKey, ReentrantLock> keyLockMap = ingestionBatchProcessor.lockKeys(batch);
      try {
        long beforeProcessingPerRecordTimestampNs = System.nanoTime();
        List<PubSubMessageProcessedResultWrapper<KafkaKey, KafkaMessageEnvelope, Long>> processedResults =
            ingestionBatchProcessor.process(
                batch,
                partitionConsumptionState,
                topicPartition.getPartitionNumber(),
                kafkaUrl,
                kafkaClusterId,
                beforeProcessingPerRecordTimestampNs,
                beforeProcessingBatchRecordsTimestampMs);

        for (PubSubMessageProcessedResultWrapper<KafkaKey, KafkaMessageEnvelope, Long> processedRecord: processedResults) {
          totalBytesRead += handleSingleMessage(
              processedRecord,
              topicPartition,
              partitionConsumptionState,
              kafkaUrl,
              kafkaClusterId,
              beforeProcessingPerRecordTimestampNs,
              beforeProcessingBatchRecordsTimestampMs,
              metricsEnabled,
              elapsedTimeForPuttingIntoQueue);
        }
      } finally {
        ingestionBatchProcessor.unlockKeys(keyLockMap);
      }
    }

    updateMetricsAndEnforceQuota(
        totalBytesRead,
        topicPartition.getPartitionNumber(),
        elapsedTimeForPuttingIntoQueue,
        beforeProcessingBatchRecordsTimestampMs);
  }

  private void updateMetricsAndEnforceQuota(
      long totalBytesRead,
      int partition,
      ValueHolder<Double> elapsedTimeForPuttingIntoQueue,
      long beforeProcessingBatchRecordsTimestampMs) {
    /**
     * Even if the records list is empty, we still need to check quota to potentially resume partition
     */
    final StorageUtilizationManager storageUtilizationManager = storeIngestionTask.getStorageUtilizationManager();
    storageUtilizationManager.enforcePartitionQuota(partition, totalBytesRead);

    if (storeIngestionTask.isMetricsEmissionEnabled()) {
      HostLevelIngestionStats hostLevelIngestionStats = storeIngestionTask.getHostLevelIngestionStats();
      if (totalBytesRead > 0) {
        hostLevelIngestionStats.recordTotalBytesReadFromKafkaAsUncompressedSize(totalBytesRead);
      }
      if (elapsedTimeForPuttingIntoQueue.getValue() > 0) {
        hostLevelIngestionStats.recordConsumerRecordsQueuePutLatency(
            elapsedTimeForPuttingIntoQueue.getValue(),
            beforeProcessingBatchRecordsTimestampMs);
      }

      hostLevelIngestionStats.recordStorageQuotaUsed(storageUtilizationManager.getDiskQuotaUsage());
    }
  }

  /**
   * This method checks whether the given record needs to be checked schema availability. Only PUT and UPDATE message
   * needs to #checkValueSchemaAvail
   * @param record
   */
  private void waitReadyToProcessRecord(PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record)
      throws InterruptedException {
    KafkaMessageEnvelope kafkaValue = record.getValue();
    if (record.getKey().isControlMessage() || kafkaValue == null) {
      return;
    }

    switch (MessageType.valueOf(kafkaValue)) {
      case PUT:
        Put put = (Put) kafkaValue.payloadUnion;
        waitReadyToProcessDataRecord(put.schemaId);
        try {
          storeIngestionTask.deserializeValue(put.schemaId, put.putValue, record);
        } catch (Exception e) {
          PartitionConsumptionState pcs =
              storeIngestionTask.getPartitionConsumptionState(record.getTopicPartition().getPartitionNumber());
          LeaderFollowerStateType state = pcs == null ? null : pcs.getLeaderFollowerState();
          throw new VeniceException(
              "Failed to deserialize PUT for: " + record.getTopicPartition() + ", offset: " + record.getOffset()
                  + ", schema id: " + put.schemaId + ", LF state: " + state,
              e);
        }
        break;
      case UPDATE:
        Update update = (Update) kafkaValue.payloadUnion;
        waitReadyToProcessDataRecord(update.schemaId);
        break;
      case DELETE:
        /* we don't need to check schema availability for DELETE */
        break;
      default:
        throw new VeniceMessageException(
            storeIngestionTask.getIngestionTaskName() + " : Invalid/Unrecognized operation type submitted: "
                + kafkaValue.messageType);
    }
  }

  /**
   * Check whether the given schema id is available for current store.
   * The function will bypass the check if schema id is -1 (VPJ job is still using it before we finishes the integration with schema registry).
   * Right now, this function is maintaining a local cache for schema id of current store considering that the value schema is immutable;
   * If the schema id is not available, this function will polling until the schema appears or timeout: {@link StoreIngestionTask#SCHEMA_POLLING_TIMEOUT_MS};
   *
   * @param schemaId
   */
  private void waitReadyToProcessDataRecord(int schemaId) throws InterruptedException {
    if (schemaId == -1) {
      // TODO: Once Venice Client (VeniceShellClient) finish the integration with schema registry,
      // we need to remove this check here.
      return;
    }

    if (schemaId == AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion()
        || schemaId == AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion()) {
      StoreVersionState storeVersionState = waitVersionStateAvailable(storeIngestionTask.getKafkaVersionTopic());
      if (!storeVersionState.chunked) {
        throw new VeniceException(
            "Detected chunking in a store-version where chunking is NOT enabled. Will abort ingestion.");
      }
      return;
    }

    storeIngestionTask.waitUntilValueSchemaAvailable(schemaId);
  }

  private StoreVersionState waitVersionStateAvailable(String kafkaTopic) throws InterruptedException {
    long startTime = System.currentTimeMillis();
    long elapsedTime;
    StoreVersionState state;
    for (;;) {
      state = storeIngestionTask.getStorageEngine().getStoreVersionState();
      elapsedTime = System.currentTimeMillis() - startTime;

      if (state != null) {
        return state;
      }

      if (elapsedTime > StoreIngestionTask.SCHEMA_POLLING_TIMEOUT_MS || !storeIngestionTask.isRunning()) {
        LOGGER.warn("Version state is not available for {} after {}", kafkaTopic, elapsedTime);
        throw new VeniceException("Store version state is not available for " + kafkaTopic);
      }

      Thread.sleep(StoreIngestionTask.SCHEMA_POLLING_DELAY_MS);
    }
  }

  private int handleSingleMessage(
      PubSubMessageProcessedResultWrapper<KafkaKey, KafkaMessageEnvelope, Long> consumerRecordWrapper,
      PubSubTopicPartition topicPartition,
      PartitionConsumptionState partitionConsumptionState,
      String kafkaUrl,
      int kafkaClusterId,
      long beforeProcessingPerRecordTimestampNs,
      long beforeProcessingBatchRecordsTimestampMs,
      boolean metricsEnabled,
      ValueHolder<Double> elapsedTimeForPuttingIntoQueue) throws InterruptedException {
    final int partition = topicPartition.getPartitionNumber();
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record = consumerRecordWrapper.getMessage();
    if (record.getKey().isControlMessage()) {
      ControlMessage controlMessage = (ControlMessage) record.getValue().payloadUnion;
      if (ControlMessageType.valueOf(controlMessage.controlMessageType) == ControlMessageType.START_OF_PUSH) {
        /**
         * N.B.: The rest of the {@link ControlMessage} types are handled by: {@link StoreIngestionTask#processControlMessage}
         *
         * But for the SOP in particular, we want to process it here, at the start of the pipeline, to ensure that the
         * {@link StoreVersionState} is properly primed, as other functions below this point, but prior to being
         * enqueued into the {@link StoreBufferService} rely on this state to be there.
         */
        storeIngestionTask.processStartOfPush(
            record.getValue(),
            controlMessage,
            record.getTopicPartition().getPartitionNumber(),
            storeIngestionTask.getPartitionConsumptionState(partition));
      }
    }

    // This function may modify the original record in KME and it is unsafe to use the payload from KME directly after
    // this call.
    StoreIngestionTask.DelegateConsumerRecordResult delegateConsumerRecordResult =
        storeIngestionTask.delegateConsumerRecord(
            consumerRecordWrapper,
            partition,
            kafkaUrl,
            kafkaClusterId,
            beforeProcessingPerRecordTimestampNs,
            beforeProcessingBatchRecordsTimestampMs);

    switch (delegateConsumerRecordResult) {
      case QUEUED_TO_DRAINER:
        long queuePutStartTimeInNS = metricsEnabled ? System.nanoTime() : 0;

        // blocking call
        storeIngestionTask.putConsumerRecord(record, partition, kafkaUrl, beforeProcessingPerRecordTimestampNs);

        if (metricsEnabled) {
          elapsedTimeForPuttingIntoQueue.setValue(
              elapsedTimeForPuttingIntoQueue.getValue() + LatencyUtils.getElapsedTimeFromNSToMS(queuePutStartTimeInNS));
        }
        break;
      case PRODUCED_TO_KAFKA:
      case SKIPPED_MESSAGE:
        break;
      default:
        throw new VeniceException(
            storeIngestionTask.getIngestionTaskName() + " received unknown DelegateConsumerRecordResult enum for "
                + record.getTopicPartition());
    }
    // Update the latest message consumed time
    partitionConsumptionState.setLatestMessageConsumedTimestampInMs(beforeProcessingBatchRecordsTimestampMs);

    return record.getPayloadSize();
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
        "Received exception when StoreIngestionTask is processing the polled consumer record for topic: {}",
        topicPartition,
        e);
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
