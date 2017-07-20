package com.linkedin.venice.writer;

import com.linkedin.venice.annotation.NotThreadsafe;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.protocol.*;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.validation.Segment;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import java.util.function.Supplier;
import org.apache.avro.specific.FixedSize;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;


/**
 * Class which acts as the primary writer API.
 *
 * N.B.: Not intended for multi-threaded usage. If that becomes a use case, we need to revisit.
 */
@NotThreadsafe
public class VeniceWriter<K, V> extends AbstractVeniceWriter<K, V> {

  // log4j logger
  private final Logger logger;

  // Config names
  private static final String VENICE_WRITER_CONFIG_PREFIX = "venice.writer.";
  public static final String CLOSE_TIMEOUT_MS = VENICE_WRITER_CONFIG_PREFIX + "close.timeout.ms";
  public static final String CHECK_SUM_TYPE = VENICE_WRITER_CONFIG_PREFIX + "checksum.type";

  // Config value defaults
  public static final int DEFAULT_CLOSE_TIMEOUT_MS = 30 * Time.MS_PER_SECOND;
  public static final String DEFAULT_CHECK_SUM_TYPE = CheckSumType.MD5.name();

  // Immutable state
  private final VeniceKafkaSerializer<K> keySerializer;
  private final VeniceKafkaSerializer<V> valueSerializer;
  private final KafkaProducerWrapper producer;
  private final GUID producerGUID;
  private final Time time;
  private final VenicePartitioner partitioner;
  private final int numberOfPartitions;
  /** Map of partition to {@link Segment}, which keeps track of all segment-related state. */
  private final Map<Integer, Segment> segmentsMap = new HashMap<>();
  private final int closeTimeOut;
  private final CheckSumType checkSumType;

  public VeniceWriter(
      VeniceProperties props,
      String topicName,
      VeniceKafkaSerializer<K> keySerializer,
      VeniceKafkaSerializer<V> valueSerializer,
      VenicePartitioner partitioner,
      Time time,
      Supplier<KafkaProducerWrapper> producerWrapperSupplier) {
    super(topicName);
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.time = time;
    this.partitioner = partitioner;
    this.closeTimeOut = props.getInt(CLOSE_TIMEOUT_MS, DEFAULT_CLOSE_TIMEOUT_MS);
    this.checkSumType = CheckSumType.valueOf(props.getString(CHECK_SUM_TYPE, DEFAULT_CHECK_SUM_TYPE));

    try {
      this.producer = producerWrapperSupplier.get();
      // We cache the number of partitions, as it is expected to be immutable, and the call to Kafka is expensive.
      this.numberOfPartitions = producer.getNumberOfPartitions(topicName);
      this.producerGUID = GuidUtils.getGUID(props);
      this.logger = Logger.getLogger(VeniceWriter.class.getSimpleName() + " [" + GuidUtils.getHexFromGuid(producerGUID) + "]");
    } catch (Exception e) {
      throw new VeniceException("Error while constructing VeniceWriter for store name: " + topicName + ", props: " + props.toString(), e);
    }
  }

  public VeniceWriter(
      VeniceProperties props,
      String topicName,
      VeniceKafkaSerializer<K> keySerializer,
      VeniceKafkaSerializer<V> valueSerializer,
      Time time) {
    this(
        props,
        topicName,
        keySerializer,
        valueSerializer,
        new DefaultVenicePartitioner(props),
        time,
        () -> new ApacheKafkaProducer(props));
  }


  public VeniceWriter(
      VeniceProperties props,
      String topicName,
      VeniceKafkaSerializer<K> keySerializer,
      VeniceKafkaSerializer<V> valueSerializer) {
    this(props, topicName, keySerializer, valueSerializer, SystemTime.INSTANCE);
  }

  public void close() {
    /**
     * If {@link #broadcastEndOfPush(Map)} was already called, the {@link #endAllSegments(boolean)}
     * will not do anything (it's idempotent).
     */
    endAllSegments(true);
    producer.close(closeTimeOut);
  }

  public String toString() {
    return this.getClass().getSimpleName()
        + "{topicName: " + topicName
        + ", producerGUID: " + producerGUID
        + ", numberOfPartitions: " + numberOfPartitions + "}";
  }

  public GUID getProducerGUID() {
    return producerGUID;
  }

  /**
   * @return the Kafka topic name that this {@link VeniceWriter} instance writes into.
   */
  public String getTopicName() {
    return topicName;
  }

  /**
   * Execute a standard "delete" on the key.
   *
   * @param key - The key to delete in storage.
   * @return a java.util.concurrent.Future Future for the RecordMetadata that will be assigned to this
   * record. Invoking java.util.concurrent.Future's get() on this future will block until the associated request
   * completes and then return the metadata for the record or throw any exception that occurred while sending the record.
   */
  public Future<RecordMetadata> delete(K key) {
    KafkaKey kafkaKey = new KafkaKey(MessageType.DELETE, keySerializer.serialize(topicName, key));
    int partition = getPartition(kafkaKey);

    KafkaMessageEnvelope kafkaValue = getKafkaMessageEnvelope(MessageType.DELETE, partition);

    return sendMessage(kafkaKey, kafkaValue, partition);
  }

  /**
   * Execute a standard "put" on the key.
   *
   * @param key   - The key to put in storage.
   * @param value - The value to be associated with the given key
   * @param valueSchemaId - value schema id for the given value
   * @param callback - Callback function invoked by Kafka producer after sending the message
   * @return a java.util.concurrent.Future Future for the RecordMetadata that will be assigned to this
   * record. Invoking java.util.concurrent.Future's get() on this future will block until the associated request
   * completes and then return the metadata for the record or throw any exception that occurred while sending the record.
   */
  @Override
  public Future<RecordMetadata> put(K key, V value, int valueSchemaId, Callback callback) {
    KafkaKey kafkaKey = new KafkaKey(MessageType.PUT, keySerializer.serialize(topicName, key));

    int partition = getPartition(kafkaKey);

    // Initialize the SpecificRecord instances used by the Avro-based Kafka protocol
    KafkaMessageEnvelope kafkaValue = getKafkaMessageEnvelope(MessageType.PUT, partition);
    Put putPayload = new Put();
    putPayload.putValue = ByteBuffer.wrap(valueSerializer.serialize(topicName, value));
    putPayload.schemaId = valueSchemaId;
    kafkaValue.payloadUnion = putPayload;

    return sendMessage(kafkaKey, kafkaValue, partition, callback);
  }

  /**
   * @param debugInfo arbitrary key/value pairs of information that will be propagated alongside the control message.
   */
  public void broadcastStartOfPush(Map<String, String> debugInfo) {
    broadcastStartOfPush(false, debugInfo);
  }

  /**
   * @param sorted whether the messages between 'StartOfPush' control messages and 'EndOfPush' control
   *               message in current topic partition is lexicographically sorted by key bytes
   * @param debugInfo arbitrary key/value pairs of information that will be propagated alongside the control message.
   */
  public void broadcastStartOfPush(boolean sorted, Map<String, String> debugInfo) {
    ControlMessage controlMessage = getEmptyControlMessage(ControlMessageType.START_OF_PUSH);
    StartOfPush startOfPush = new StartOfPush();
    startOfPush.sorted = sorted;
    controlMessage.controlMessageUnion = startOfPush;
    broadcastControlMessage(controlMessage, debugInfo);
    // Flush start of push message to avoid data message arrives before it.
    producer.flush();
  }

  /**
   * @param debugInfo arbitrary key/value pairs of information that will be propagated alongside the control message.
   */
  public void broadcastEndOfPush(Map<String, String> debugInfo) {
    broadcastControlMessage(getEmptyControlMessage(ControlMessageType.END_OF_PUSH), debugInfo);
    endAllSegments(true);
  }

  /**
   * @param debugInfo arbitrary key/value pairs of information that will be propagated alongside the control message.
   */
  public void broadcastStartOfBufferReplay(List<Long> sourceOffsets, String sourceKafkaCluster, String sourceTopicName, Map<String, String> debugInfo) {
    ControlMessage controlMessage = getEmptyControlMessage(ControlMessageType.START_OF_BUFFER_REPLAY);
    StartOfBufferReplay startOfBufferReplay = new StartOfBufferReplay();
    startOfBufferReplay.sourceOffsets = Utils.notNull(sourceOffsets);
    startOfBufferReplay.sourceKafkaCluster = Utils.notNull(sourceKafkaCluster);
    startOfBufferReplay.sourceTopicName = Utils.notNull(sourceTopicName);
    controlMessage.controlMessageUnion = startOfBufferReplay;
    broadcastControlMessage(controlMessage, debugInfo);
    // Flush start of push message to avoid data message arrives before it.
    producer.flush();
  }

  private Future<RecordMetadata> sendMessage(KafkaKey key, KafkaMessageEnvelope value, int partition) {
    return sendMessage(key, value, partition, new KafkaMessageCallback(key, value, logger));
  }


  private Future<RecordMetadata> sendMessage(KafkaKey key, KafkaMessageEnvelope value, int partition, Callback callback) {
    segmentsMap.get(partition).addToCheckSum(key, value);
    Callback messageCallback = (null != callback ? callback : new KafkaMessageCallback(key, value, logger));
    return producer.sendMessage(topicName, key, value, partition, messageCallback);
  }

  /**
   * Send a {@link ControlMessageType#START_OF_SEGMENT} control message into the designated partition.
   *
   * @param partition the Kafka partition to write to.
   * @param debugInfo arbitrary key/value pairs of information that will be propagated alongside the control message.
   */
  private void sendStartOfSegment(int partition, Map<String, String> debugInfo) {
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageType = ControlMessageType.START_OF_SEGMENT.getValue();
    StartOfSegment startOfSegment = new StartOfSegment();
    startOfSegment.checksumType = checkSumType.getValue();
    startOfSegment.upcomingAggregates = new ArrayList<>(); // TODO Add extra aggregates
    controlMessage.controlMessageUnion = startOfSegment;
    sendControlMessage(controlMessage, partition, debugInfo);
  }

  /**
   * Send a {@link ControlMessageType#END_OF_SEGMENT} control message into the designated partition.
   *
   * @param partition the Kafka partition to write to.
   * @param debugInfo arbitrary key/value pairs of information that will be propagated alongside the control message.
   * @param finalSegment a boolean indicating if this is the final segment that this producer will send
   *                     into this partition.
   */
  private void sendEndOfSegment(int partition, Map<String, String> debugInfo, boolean finalSegment) {
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageType = ControlMessageType.END_OF_SEGMENT.getValue();
    EndOfSegment endOfSegment = new EndOfSegment();
    endOfSegment.checksumValue = ByteBuffer.wrap(segmentsMap.get(partition).getFinalCheckSum());
    endOfSegment.computedAggregates = new ArrayList<>(); // TODO Add extra aggregates
    endOfSegment.finalSegment = finalSegment;
    controlMessage.controlMessageUnion = endOfSegment;
    sendControlMessage(controlMessage, partition, debugInfo);
  }

  /**
   * Utility function to minimize code duplication for the simple {@link ControlMessage} which have no
   * extra metadata associated with them.
   *
   * @param controlMessageType the {@link ControlMessageType} for which to return an instance.
   * @return an instance of {@link ControlMessage} which corresponds to the requested {@param controlMessageType}.
   */
  private ControlMessage getEmptyControlMessage(ControlMessageType controlMessageType) {
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageType = controlMessageType.getValue();
    controlMessage.controlMessageUnion = controlMessageType.getNewInstance();
    return controlMessage;
  }

  /**
   * @param controlMessage a {@link ControlMessage} instance to persist into all Kafka partitions.
   * @param debugInfo arbitrary key/value pairs of information that will be propagated alongside the control message.
   */
  private void broadcastControlMessage(ControlMessage controlMessage, Map<String, String> debugInfo) {
    for(int partition = 0; partition < numberOfPartitions ; partition ++) {
      sendControlMessage(controlMessage, partition, debugInfo);
    }
    logger.info("Successfully broadcasted " + ControlMessageType.valueOf(controlMessage)
        + " Control Message for topic '" + topicName + "'.");
  }

  /**
   * @param controlMessage a {@link ControlMessage} instance to persist into Kafka.
   * @param partition the Kafka partition to write to.
   * @param debugInfo arbitrary key/value pairs of information that will be propagated alongside the control message.
   */
  private void sendControlMessage(ControlMessage controlMessage, int partition, Map<String, String> debugInfo) {
    // Work around until we upgrade to a more modern Avro version which supports overriding the
    // String implementation.
    controlMessage.debugInfo = new HashMap<>(debugInfo);

    // Initialize the SpecificRecord instances used by the Avro-based Kafka protocol
    KafkaMessageEnvelope kafkaValue = getKafkaMessageEnvelope(MessageType.CONTROL_MESSAGE, partition);
    kafkaValue.payloadUnion = controlMessage;
    try {
      sendMessage(getControlMessageKey(kafkaValue), kafkaValue, partition).get();
    } catch (InterruptedException|ExecutionException e) {
      throw new VeniceException("Got an exception while trying to send a control message (" +
          ControlMessageType.valueOf(controlMessage).name() + ")", e);
    }
  }

  /**
   * For control messages, the Key part of the {@link KafkaKey} includes the producer GUID, segment and sequence number.
   *
   * N.B.: This could be optimized further by defining an Avro record to hold this data, since Avro would use
   * variable length encoding for the two integers, which would be smaller than their regular size.
   */
  private static final int CONTROL_MESSAGE_KAFKA_KEY_LENGTH = GUID.class.getAnnotation(FixedSize.class).value() + Integer.SIZE * 2;

  /**
   * The Key part of the {@link KafkaKey} needs to be unique in order to avoid clobbering each other during
   * Kafka's Log Compaction. Since there is no key per say associated with control messages, we generate one
   * from the producer metadata, including: GUID, segment and sequence number.

   * @param kafkaValue from which to derive the producer metadata necessary to generate the key.
   * @return a {@link KafkaKey} guaranteed to be unique within its target partition.
   */
  private KafkaKey getControlMessageKey(KafkaMessageEnvelope kafkaValue) {
    return new KafkaKey(
        MessageType.CONTROL_MESSAGE,
        ByteBuffer.allocate(CONTROL_MESSAGE_KAFKA_KEY_LENGTH)
            .put(kafkaValue.producerMetadata.producerGUID.bytes())
            .putInt(kafkaValue.producerMetadata.segmentNumber)
            .putInt(kafkaValue.producerMetadata.messageSequenceNumber)
            .array());
  }

  /**
   * A utility function to centralize some boiler plate code for the instantiation of
   * {@link org.apache.avro.specific.SpecificRecord} classes holding the content of our
   * Kafka values.
   *
   * @param messageType an instance of the {@link MessageType} enum.
   * @return A {@link KafkaMessageEnvelope} suitable for producing into Kafka
   */
  private KafkaMessageEnvelope getKafkaMessageEnvelope(MessageType messageType, int partition) {
    // If single-threaded, the kafkaValue could be re-used (and clobbered). TODO: explore GC tuning later.
    KafkaMessageEnvelope kafkaValue = new KafkaMessageEnvelope();
    kafkaValue.messageType = messageType.getValue();

    ProducerMetadata producerMetadata = new ProducerMetadata();
    producerMetadata.producerGUID = producerGUID;
    Segment currentSegment = getSegment(partition);
    producerMetadata.segmentNumber = currentSegment.getSegmentNumber();
    producerMetadata.messageSequenceNumber = currentSegment.getAndIncrementSequenceNumber();
    producerMetadata.messageTimestamp = time.getMilliseconds();
    kafkaValue.producerMetadata = producerMetadata;

    return kafkaValue;
  }

  /**
   * @param key the {@link KafkaKey} for which we want to get the partition.
   * @return the partition number that the provided key belongs to.
   */
  private int getPartition(KafkaKey key) {
    return partitioner.getPartitionId(key.getKey(), numberOfPartitions);
  }

  /**
   * @param partition for which we want to get the current {@link Segment}
   * @return the existing {@link Segment} associated with the requested partition, or
   *         a new one if none existed previously.
   */
  private Segment getSegment(int partition) {
    Segment currentSegment = segmentsMap.get(partition);
    if (null == currentSegment || currentSegment.isEnded()) {
      currentSegment = startSegment(partition);
    }
    return currentSegment;
  }

  /**
   * @param partition in which to start a new segment
   * @return the number of the new segment which was just begun
   */
  private synchronized Segment startSegment(int partition) {
    Segment currentSegment = segmentsMap.get(partition);

    if (null == currentSegment) {
      currentSegment = new Segment(partition, 0, checkSumType);
      segmentsMap.put(partition, currentSegment);
    } else if (currentSegment.isEnded()) {
      int newSegmentNumber = currentSegment.getSegmentNumber() + 1;
      currentSegment = new Segment(partition, newSegmentNumber, checkSumType);
      segmentsMap.put(partition, currentSegment);
    }

    if (!currentSegment.isStarted()) {
      sendStartOfSegment(
          partition,
          new HashMap<>() // TODO: Add extra debugging info
      );

      currentSegment.start();
    }

    return currentSegment;
  }

  private void endAllSegments(boolean finalSegment) {
    segmentsMap.keySet().stream().forEach(partition -> endSegment(partition, finalSegment));
  }

  /**
   * @param partition in which to end the current segment
   */
  private synchronized void endSegment(int partition, boolean finalSegment) {
    Segment currentSegment = segmentsMap.get(partition);
    if (null == currentSegment) {
      logger.warn("endSegment(partition " + partition + ") called but currentSegment == null. Ignoring.");
    } else if (!currentSegment.isStarted()) {
      logger.warn("endSegment(partition " + partition + ") called but currentSegment.begun == false. Ignoring.");
    } else if (currentSegment.isEnded()) {
      logger.warn("endSegment(partition " + partition + ") called but currentSegment.ended == true. Ignoring.");
    } else {
      logger.info("endSegment(partition " + partition + ") called. Proceeding.");
      sendEndOfSegment(
          partition,
          new HashMap<>(), // TODO: Add extra debugging info
          finalSegment // TODO: This will not always be true, once we support streaming, or more than one segment per mapper in batch
      );
      currentSegment.end(finalSegment);
    }
  }

  private static class KafkaMessageCallback implements Callback {
    private final KafkaKey key;
    private final KafkaMessageEnvelope value;
    private final Logger logger;
    public KafkaMessageCallback(KafkaKey key, KafkaMessageEnvelope value, Logger logger) {
      this.key = key;
      this.value = value;
      this.logger = logger;
    }
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
      if (e != null) {
        logger.error("Failed to send out message to Kafka producer: [key: " + key + ", value: " + value + "]", e);
      }
    }
  }
}
