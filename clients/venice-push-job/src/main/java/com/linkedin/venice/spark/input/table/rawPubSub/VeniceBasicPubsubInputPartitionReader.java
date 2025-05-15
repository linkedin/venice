package com.linkedin.venice.spark.input.table.rawPubSub;

import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
  private final String targetPubSubTopicName;
  private final int targetPartitionNumber;
  private final PubSubConsumerAdapter pubSubConsumer;
  // inputPartitionReader local buffer, that gets filled from partitionMessagesBuffer

  private final ArrayDeque<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> messageBuffer = new ArrayDeque<>();
  private long currentOffset;
  private InternalRow currentRow = null;
  private long recordsServed = 0;
  private long recordsSkipped = 0;
  private long lastKnownProgressPercent = 0;
  private long recordsDeliveredByGet = 0;

  private Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> consumerBuffer =
      new HashMap<>();
  // the buffer that holds the relevant messages for the current partition

  public VeniceBasicPubsubInputPartitionReader(
      VeniceProperties jobConfig,
      VeniceBasicPubsubInputPartitionReader inputPartition) {
    this(
        jobConfig,
        inputPartition,
        new PubSubClientsFactory(jobConfig).getConsumerAdapterFactory()
            .create(
                jobConfig,
                false,
                PubSubMessageDeserializer.getInstance(),
                // PubSubPassThroughDeserializer.getInstance(),
                "Spark_raw_pubsub_input_partition_consumer"),
        new PubSubTopicRepository());
  }

  // testing constructor
  public VeniceBasicPubsubInputPartitionReader(
      VeniceProperties jobConfig,
      VeniceBasicPubsubInputPartition inputPartition,
      PubSubConsumerAdapter consumer,
      PubSubTopicRepository pubSubTopicRepository) {

    targetPubSubTopicName = inputPartition.getTopicName();
    targetPartitionNumber = inputPartition.getPartitionNumber();
    long startOffset = pubSubConsumer.beginningOffset(pubSubTopicPartition, Duration.ofSeconds(60));
    long endOffset = pubSubConsumer.endOffset(pubSubTopicPartition);

    this.pubSubConsumer = consumer;

    LOGGER.info("Consumption started for Topic: {} Partition {}.", targetPubSubTopicName, targetPartitionNumber);

    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(targetPubSubTopicName);

    // List<PubSubTopicPartitionInfo> listOfPartitions = pubSubConsumer.partitionsFor(pubSubTopic);
    // at this point, we hope that driver has given us good information about
    // the partition and offsets and the fact that topic exists.

    targetPubSubTopicPartition = new PubSubTopicPartitionImpl(pubSubTopic, targetPartitionNumber);

    pubSubConsumer.subscribe(targetPubSubTopicPartition, startingOffset - 1);
    // pubSubConsumer.seek(startingOffset); // do we need this? or should we rely on the starting offset passed to
    // subscribe ?

    initialize(); // see, MET05-J asked for this !
  }

  private void initialize() {
    next();// get the first record ready to go.
    recordsServed = 0; // reset the counter
  }

  // if it returns a row, it's going to be key and value and offset in the row in that order
  @Override
  public InternalRow get() {
    recordsDeliveredByGet++;
    // should return the same row if called multiple times
    return currentRow;
  }

  @Override
  public boolean next() {
    // Are we past the finish line ?
    if (currentOffset >= endingOffset) {
      return false;
    }

    if (ableToPrepNextRow()) {
      // there is a fresh row to serve
      return true;
    }
    // at this point, buffer is empty.

    loadRecords(); // try to poll for some records and allow the exception to bubble up
    return ableToPrepNextRow();
  }

  @Override
  public void close() {
    pubSubConsumer.close();
    double progressPercent = (currentOffset - startingOffset) * 100.0 / offsetLength;
    LOGGER.info(
        "Consuming ended for Topic: {} , consumed {}% of {} records,",
        targetPubSubTopicName,
        progressPercent,
        recordsServed);
    LOGGER.info("Skipped {} records, delivered rows {} times .", recordsSkipped, recordsDeliveredByGet);
  }

  // borrowing Gaojie's code for dealing with empty polls.
  private void loadRecords() {
    List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> partitionMessagesBuffer = new ArrayList<>();

    int retry = 0;
    while (retry++ < CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES) {
      consumerBuffer = pubSubConsumer.poll(CONSUMER_POLL_TIMEOUT);

      partitionMessagesBuffer.addAll(consumerBuffer.get(targetPubSubTopicPartition));
      if (!partitionMessagesBuffer.isEmpty()) {
        // we got some records back for the desired partition.
        break;
      }

      try {
        Thread.sleep(EMPTY_POLL_SLEEP_TIME_MS);
      } catch (InterruptedException e) {
        logProgress();
        LOGGER.error(
            "Interrupted while waiting for records to be consumed from topic {} partition {} to be available",
            targetPubSubTopicName,
            targetPartitionNumber,
            e);
        // should we re-throw here to break the consumption task ?
      }
    }
    if (partitionMessagesBuffer.isEmpty()) {
      // this is a valid place to throw exception and kill the consumer task
      // as there is no more records to consume.
      throw new RuntimeException("Empty poll after " + retry + " retries");
    }
    messageBuffer.addAll(partitionMessagesBuffer);
  }

  private InternalRow processPubSubMessageToRow(
      @NotNull PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> pubSubMessage) {
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
    long offset = pubSubMessage.getOffset();
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
    // pack pieces of information into the spart intermediate row, this will populate the dataframe to be read by the
    // spark job
    // weirdest use of verb "GET" in heabBuffer !!!!!
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
    double progressPercent = (currentOffset - startingOffset) * 100.0 / offsetLength;
    if (progressPercent > 10 + lastKnownProgressPercent) {
      logProgress();
      lastKnownProgressPercent = (long) progressPercent;
    }
  }

  private void logProgress() {
    double progressPercent = (currentOffset - startingOffset) * 100.0 / offsetLength;
    LOGGER.info(
        "Consuming progress for"
            + " Topic: {}, partition {} , consumed {}% of {} records. actual records delivered: {}, records skipped: {}",
        targetPubSubTopicName,
        targetPartitionNumber,
        String.format("%.1f", (float) progressPercent),
        offsetLength,
        recordsServed,
        recordsSkipped);
  }

  public List<Long> getStats() {
    return Arrays.asList(currentOffset, recordsServed, recordsSkipped, recordsDeliveredByGet);
  }

  // go through the current buffer and find the next usable message
  private boolean ableToPrepNextRow() {

    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> message;
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

      currentOffset = message.getOffset();

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
