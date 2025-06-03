package com.linkedin.venice.spark.input.table.rawPubSub;

import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.jetbrains.annotations.NotNull;


public class VeniceBasicPubsubInputPartitionReader implements PartitionReader<InternalRow> {
  private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[0]);
  private static final int CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES = 12;
  private static final long EMPTY_POLL_SLEEP_TIME_MS = TimeUnit.SECONDS.toMillis(5);
  private static final Long CONSUMER_POLL_TIMEOUT = TimeUnit.SECONDS.toMillis(1); // 1 second

  private static final Logger LOGGER = LogManager.getLogger(VeniceBasicPubsubInputPartitionReader.class);

  private final boolean filterControlMessages = true;

  // this is the buffer that holds the messages that are consumed from the pubsub
  private final PubSubTopicPartition targetPubSubTopicPartition;
  private final PubSubTopic targetPubSubTopic;
  private final PubSubConsumerAdapter pubSubConsumer;
  // inputPartitionReader local buffer, that gets filled from partitionMessagesBuffer

  private final ArrayDeque<PubSubMessage<KafkaKey, KafkaMessageEnvelope, PubSubPosition>> messageBuffer =
      new ArrayDeque<>();
  private final PubSubPosition endingPosition;
  private final long endingOffset;
  private final PubSubPosition startingPosition;
  private final long startingOffset;
  private final long offsetLength;
  private final int targetPartitionNumber;
  private final String topicName;
  private PubSubPosition currentPosition;
  private InternalRow currentRow = null;
  private long recordsServed = 0;
  private long recordsSkipped = 0;
  private long recordsDeliveredByGet = 0;
  private long lastKnownProgressPercent = 0;

  // the buffer that holds the relevant messages for the current partition

  public VeniceBasicPubsubInputPartitionReader(
      VeniceProperties jobConfig,
      VeniceBasicPubsubInputPartition inputPartition) {
    this(
        jobConfig,
        inputPartition,
        PubSubClientsFactory.createConsumerFactory(jobConfig)
            .create(
                new PubSubConsumerAdapterContext.Builder().setVeniceProperties(jobConfig)
                    .setPubSubTopicRepository(new PubSubTopicRepository())
                    .setPubSubMessageDeserializer(PubSubMessageDeserializer.createOptimizedDeserializer())
                    .setPubSubPositionTypeRegistry(PubSubPositionTypeRegistry.fromPropertiesOrDefault(jobConfig))
                    .setConsumerName("raw_kif_" + inputPartition.getTopic() + "_" + inputPartition.getPartitionNumber())
                    .build()), // this is hideous
        new PubSubTopicRepository());
  }

  // testing constructor
  public VeniceBasicPubsubInputPartitionReader(
      VeniceProperties jobConfig,
      VeniceBasicPubsubInputPartition inputPartition,
      PubSubConsumerAdapter consumer,
      PubSubTopicRepository pubSubTopicRepository) {

    pubSubConsumer = consumer;

    targetPubSubTopic = inputPartition.getTopic();
    topicName = targetPubSubTopic.getName();
    targetPartitionNumber = inputPartition.getPartitionNumber();
    startingPosition = inputPartition.getBeginningPosition();
    startingOffset = inputPartition.getBeginningOffset();
    endingPosition = inputPartition.getEndPosition();
    endingOffset = inputPartition.getEndOffset();
    offsetLength = inputPartition.getOffsetLength();
    targetPubSubTopicPartition = inputPartition.getTopicPartition();
    currentPosition = startingPosition;

    pubSubConsumer.subscribe(targetPubSubTopicPartition, startingPosition);
    LOGGER.info("Subscribed to  Topic: {} Partition {}.", topicName, targetPartitionNumber);
    initialize();
  }

  private void initialize() {
    next();// get the first record ready to go.
    recordsServed = 0; // reset the counter
  }

  /**
   *  Assuming that Current row has meaningful data, this method will return that and counts the number of invocations.
   * @return The current row as an {@link InternalRow}.
   */
  @Override
  public InternalRow get() {
    recordsDeliveredByGet++;
    // should return the same row if called multiple times
    return currentRow;
  }

  private boolean offsetRemains() {
    return currentPosition.getNumericOffset() < endingOffset;
  }

  @Override
  public boolean next() {
    if (offsetRemains()) {
      return false;
    }

    if (ableToPrepNextRow()) {
      // there is a fresh row to serve
      return true;
    }
    // at this point, buffer is empty.

    pollAndFillMessageBufferWithNewPubsubRecords(); // try to poll for some records and allow the exception to bubble up
    return ableToPrepNextRow();
  }

  @Override
  public void close() {
    pubSubConsumer.close();
    // double progressPercent = (currentPosition.getNumericOffset() - startingOffset) * 100.0 / offsetLength;
    LOGGER.info("Consumer closed for Topic: {} , consumed {} records,", topicName, recordsServed);
    LOGGER.info("Skipped {} records, delivered rows {} times .", recordsSkipped, recordsDeliveredByGet);
  }

  private void pollAndFillMessageBufferWithNewPubsubRecords() {
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> consumerBuffer;
    int retries = 0;

    while (retries < CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES) {
      consumerBuffer = pubSubConsumer.poll(CONSUMER_POLL_TIMEOUT);
      List<DefaultPubSubMessage> partitionMessagesBuffer =
          new ArrayList<>(consumerBuffer.get(targetPubSubTopicPartition));
      consumerBuffer.clear();

      if (!partitionMessagesBuffer.isEmpty()) {
        messageBuffer.addAll(partitionMessagesBuffer); // we are done.
        return;
      }

      try {
        Thread.sleep(EMPTY_POLL_SLEEP_TIME_MS);
      } catch (InterruptedException e) {
        logProgress();
        LOGGER.error(
            "Interrupted while waiting for records to be consumed from topic {} partition {} to be available",
            topicName,
            targetPartitionNumber,
            e);
        // should we re-throw here to break the consumption task ?
        // Thread.currentThread().interrupt(); // very questionable genAI suggestion,
        // what's the intended way to terminate this task?
      }
      retries++;
    }

    // tried really hard, but nothing came out of consumer, giving up.
    throw new RuntimeException(
        "Empty poll after " + retries + " retries for topic: " + topicName + " partition: " + targetPartitionNumber
            + ". No messages were consumed.");

  }

  private InternalRow processPubSubMessageToRow(
      @NotNull PubSubMessage<KafkaKey, KafkaMessageEnvelope, PubSubPosition> pubSubMessage) {
    // after deliberation, I think we are better off isolating further processing of the messages after they are dumped
    // into the dataframe, Spark job can handle the rest of the processing.

    // should we detect chunking on the topic ?

    KafkaKey kafkaKey = pubSubMessage.getKey();
    KafkaMessageEnvelope kafkaMessageEnvelope = pubSubMessage.getValue();
    MessageType pubSubMessageType = MessageType.valueOf(kafkaMessageEnvelope);

    /*
    List of fields we need in the row:  @see KAFKA_INPUT_TABLE_SCHEMA
    1. offset ( currently a long , maybe some other complicated thing in the Northguard world)
    2. key ( serialized key Byte[])
    3. value ( serialized value Byte[])
    4. partition ( int )
    5. messageType ( put vs delete ) .getValue is the int value and gives us that. value type is also of this kind
    6. schemaId ( for put and delete ) int
    7. replicationMetadataPayload ByteBuffer
    8. replicationMetadataVersionId int
    */

    // Spark row setup :
    long offset = pubSubMessage.getOffset().getNumericOffset();
    ByteBuffer key = ByteBuffer.wrap(kafkaKey.getKey(), 0, kafkaKey.getKeyLength());
    ByteBuffer value;
    int messageType;
    int schemaId;
    ByteBuffer replicationMetadataPayload;
    int replicationMetadataVersionId;

    switch (pubSubMessageType) {
      case PUT:
        Put put = (Put) kafkaMessageEnvelope.payloadUnion;
        messageType = MessageType.PUT.getValue();
        value = put.putValue;
        schemaId = put.schemaId; // chunking will be handled down the road in spark job.
        replicationMetadataPayload = put.replicationMetadataPayload;
        replicationMetadataVersionId = put.replicationMetadataVersionId;
        break;
      case DELETE:
        messageType = MessageType.DELETE.getValue();
        Delete delete = (Delete) kafkaMessageEnvelope.payloadUnion;
        schemaId = delete.schemaId;
        value = EMPTY_BYTE_BUFFER;
        replicationMetadataPayload = delete.replicationMetadataPayload;
        replicationMetadataVersionId = delete.replicationMetadataVersionId;
        break;
      default:
        messageType = -1; // this is an error condition
        schemaId = Integer.MAX_VALUE;
        value = EMPTY_BYTE_BUFFER;
        replicationMetadataPayload = EMPTY_BYTE_BUFFER;
        replicationMetadataVersionId = Integer.MAX_VALUE;
        // we don't care about messages other than PUT and DELETE
    }

    /*
    Dealing with chunking :
    @link https://github.com/linkedin/venice/blob/main/clients/da-vinci-client/src/main/java/com/linkedin/davinci/storage/chunking/ChunkingUtils.java#L53
       * 1. The top-level key is queried.
       * 2. The top-level key's value's schema ID is checked.
       *    a) If it is positive, then it's a full value, and is returned immediately.
       *    b) If it is negative, then it's a {@link ChunkedValueManifest}, and we continue to the next steps.
       * 3. The {@link ChunkedValueManifest} is deserialized, and its chunk keys are extracted.
       * 4. Each chunk key is queried.
       * 5. The chunks are stitched back together using the various adapter interfaces of this package,
       *    depending on whether it is the single get or batch get/compute path that needs to re-assemble
       *    a chunked value.
    
    For dumping application, we can treat this as pass-through .
    
           chunking code:
        RawKeyBytesAndChunkedKeySuffix rawKeyAndChunkedKeySuffix =
            splitCompositeKey(kafkaKey.getKey(), messageType, getSchemaIdFromValue(kafkaMessageEnvelope));
        key.key = rawKeyAndChunkedKeySuffix.getRawKeyBytes();
    
        value.chunkedKeySuffix = rawKeyAndChunkedKeySuffix.getChunkedKeySuffixBytes();
    */

    // need to figure out task tracking in Spark Land.
    // pack pieces of information into the Spark intermediate row, this will populate the dataframe to be read by the
    // spark job
    // The weirdest use of verb "GET" in heapBuffer !!!!!
    byte[] keyBytes = new byte[key.remaining()];
    key.get(keyBytes);
    byte[] valueBytes = new byte[value.remaining()];
    value.get(valueBytes);
    byte[] replicationMetadataPayloadBytes = new byte[replicationMetadataPayload.remaining()];
    replicationMetadataPayload.get(replicationMetadataPayloadBytes);

    return new GenericInternalRow(
        new Object[] { offset, keyBytes, valueBytes, targetPartitionNumber, messageType, schemaId,
            replicationMetadataPayloadBytes, replicationMetadataVersionId });
  }

  private void maybeLogProgress() {
    double progressPercent = (currentPosition.getNumericOffset() - startingOffset) * 100.0 / offsetLength;
    if (progressPercent > 10 + lastKnownProgressPercent) {
      logProgress();
      lastKnownProgressPercent = (long) progressPercent;
    }
  }

  private void logProgress() {
    double progressPercent = (currentPosition.getNumericOffset() - startingOffset) * 100.0 / offsetLength;
    LOGGER.info(
        "Consuming progress for"
            + " Topic: {}, partition {} , consumed {}% of {} records. actual records delivered: {}, records skipped: {}",
        targetPubSubTopic,
        targetPartitionNumber,
        String.format("%.1f", (float) progressPercent),
        offsetLength,
        recordsServed,
        recordsSkipped);
  }

  public List<Long> getStats() {
    return Arrays.asList(recordsServed, recordsSkipped, recordsDeliveredByGet);
  }

  // go through the current buffer and find the next usable message
  private boolean ableToPrepNextRow() {

    PubSubMessage<KafkaKey, KafkaMessageEnvelope, PubSubPosition> message;
    boolean found;
    // buffer is already empty.
    if (messageBuffer.isEmpty()) {
      return false;
    }

    // look for the next viable message
    found = false;
    while (!found) {
      try {
        message = messageBuffer.pop();
      } catch (NoSuchElementException e) {
        // ran out of messages in the buffer
        return false;
      }

      currentPosition = message.getOffset();

      if (filterControlMessages && message.getKey().isControlMessage()) {
        recordsSkipped++;
      } else {
        currentRow = processPubSubMessageToRow(message);
        recordsServed++;
        found = true;
      }
    }
    maybeLogProgress();
    return true;
  }
}
