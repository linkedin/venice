package com.linkedin.venice.writer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.protocol.*;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.serialization.VeniceSerializer;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.avro.specific.FixedSize;
import org.apache.http.annotation.NotThreadSafe;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;


/**
 * Class which acts as the primary writer API.
 *
 * N.B.: Not intended for multi-threaded usage. If that becomes a use case, we need to revisit.
 */
@NotThreadSafe
public class VeniceWriter<K, V> extends AbstractVeniceWriter<K, V> {

  // log4j logger
  private static final Logger LOGGER = Logger.getLogger(VeniceWriter.class);

  public static final String CLOSE_TIMEOUT_MS = "venice.writer.close.timeout.ms";
  public static final int DEFAULT_CLOSE_TIMEOUT_MS = 30 * Time.MS_PER_SECOND;

  // Immutable state
  private final VeniceSerializer<K> keySerializer;
  private final VeniceSerializer<V> valueSerializer;
  private final KafkaProducerWrapper producer;
  private final GUID producerGUID;
  private final Time time;
  private final VenicePartitioner partitioner;
  private final int numberOfPartitions;
  /** Map of partition to {@link Segment}, which keeps track of all segment-related state. */
  private final Map<Integer, Segment> segmentsMap = Maps.newHashMap();
  private final int checksumType = 0; // TODO: Make configurable
  private final int closeTimeOut;

  public VeniceWriter(
      VeniceProperties props,
      String topicName,
      VeniceSerializer<K> keySerializer,
      VeniceSerializer<V> valueSerializer,
      Time time) {
    super(topicName);
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.time = time;
    this.partitioner = new DefaultVenicePartitioner(props);
    this.closeTimeOut = props.getInt(CLOSE_TIMEOUT_MS, DEFAULT_CLOSE_TIMEOUT_MS);

    try {
      this.producer = new KafkaProducerWrapper(props);
      // We cache the number of partitions, as it is expected to be immutable, and the call to Kafka is expensive.
      this.numberOfPartitions = producer.getNumberOfPartitions(topicName);
      this.producerGUID = GuidUtils.getGUID(props);
    } catch (Exception e) {
      throw new VeniceException("Error while constructing VeniceWriter for store name: " + topicName, e);
    }
  }

  public VeniceWriter(
      VeniceProperties props,
      String topicName,
      VeniceSerializer<K> keySerializer,
      VeniceSerializer<V> valueSerializer) {
    this(props, topicName, keySerializer, valueSerializer, SystemTime.INSTANCE);
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

    return producer.sendMessage(topicName, kafkaKey, kafkaValue, partition);
  }

  // TODO: Once we finishes venice client/schema registry integration, we need to remove this interface
  @Deprecated
  public Future<RecordMetadata> put(K key, V value) {
    return put(key, value, -1);
  }

  /**
   * Execute a standard "put" on the key.
   *
   * @param key   - The key to put in storage.
   * @param value - The value to be associated with the given key
   * @param valueSchemaId - value schema id for the given value
   * @return a java.util.concurrent.Future Future for the RecordMetadata that will be assigned to this
   * record. Invoking java.util.concurrent.Future's get() on this future will block until the associated request
   * completes and then return the metadata for the record or throw any exception that occurred while sending the record.
   */
  public Future<RecordMetadata> put(K key, V value, int valueSchemaId) {
    KafkaKey kafkaKey = new KafkaKey(MessageType.PUT, keySerializer.serialize(topicName, key));

    int partition = getPartition(kafkaKey);

    // Initialize the SpecificRecord instances used by the Avro-based Kafka protocol
    KafkaMessageEnvelope kafkaValue = getKafkaMessageEnvelope(MessageType.PUT, partition);
    Put putPayload = new Put();
    putPayload.putValue = ByteBuffer.wrap(valueSerializer.serialize(topicName, value));
    putPayload.schemaId = valueSchemaId;
    kafkaValue.payloadUnion = putPayload;

    return producer.sendMessage(topicName, kafkaKey, kafkaValue, partition);
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
    startOfSegment.checksumType = checksumType;
    startOfSegment.upcomingAggregates = Lists.newArrayList(); // TODO Add extra aggregates
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
    endOfSegment.checksumValue = ByteBuffer.allocate(16); // TODO: Populate checksum
    endOfSegment.computedAggregates = Lists.newArrayList(); // TODO Add extra aggregates
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
   * @param debugInfo arbitrary key/value pairs of information that will be propagated alongside the control message.
   */
  public void broadcastStartOfPush(Map<String, String> debugInfo) {
    broadcastControlMessage(getEmptyControlMessage(ControlMessageType.START_OF_PUSH), debugInfo);
  }

  /**
   * @param debugInfo arbitrary key/value pairs of information that will be propagated alongside the control message.
   */
  public void broadcastEndOfPush(Map<String, String> debugInfo) {
    broadcastControlMessage(getEmptyControlMessage(ControlMessageType.END_OF_PUSH), debugInfo);
  }

  /**
   * @param controlMessage a {@link ControlMessage} instance to persist into all Kafka partitions.
   * @param debugInfo arbitrary key/value pairs of information that will be propagated alongside the control message.
   */
  private void broadcastControlMessage(ControlMessage controlMessage, Map<String, String> debugInfo) {
    for(int partition = 0; partition < numberOfPartitions ; partition ++) {
      sendControlMessage(controlMessage, partition, debugInfo);
    }
    LOGGER.info("Successfully broadcasted" + ControlMessageType.valueOf(controlMessage)
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
    controlMessage.debugInfo = Maps.newHashMap(debugInfo);

    // Initialize the SpecificRecord instances used by the Avro-based Kafka protocol
    KafkaMessageEnvelope kafkaValue = getKafkaMessageEnvelope(MessageType.CONTROL_MESSAGE, partition);
    kafkaValue.payloadUnion = controlMessage;
    producer.sendMessage(topicName, getControlMessageKey(kafkaValue), kafkaValue, partition);
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

  public void close() {
    segmentsMap.values().stream()
        .filter(segment -> segment.isStarted() && !segment.isEnded())
        .forEach(segment -> endSegment(segment.getPartition()));
    producer.close(closeTimeOut);
  }

  public String toString() {
    return this.getClass().getSimpleName()
        + "{topicName: " + topicName
        + ", producerGUID: " + producerGUID
        + ", numberOfPartitions: " + numberOfPartitions + "}";
  }

  /**
   * @return the Kafka topic name that this {@link VeniceWriter} instance writes into.
   */
  public String getTopicName() {
    return topicName;
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
      currentSegment = new Segment(partition, 0);
      segmentsMap.put(partition, currentSegment);
    } else if (currentSegment.isEnded()) {
      int newSegmentNumber = currentSegment.getSegmentNumber() + 1;
      currentSegment = new Segment(partition, newSegmentNumber);
      segmentsMap.put(partition, currentSegment);
    }

    if (!currentSegment.isStarted()) {
      sendStartOfSegment(
          partition,
          Maps.newHashMap() // TODO: Add extra debugging info
      );

      currentSegment.start();
    }

    return currentSegment;
  }

  /**
   * @param partition in which to end the current segment
   */
  private synchronized void endSegment(int partition) {
    Segment currentSegment = segmentsMap.get(partition);
    if (null == currentSegment) {
      LOGGER.warn("endSegment(partition " + partition + ") called but currentSegment == null. Ignoring.");
    } else if (!currentSegment.isStarted()) {
      LOGGER.warn("endSegment(partition " + partition + ") called but currentSegment.begun == false. Ignoring.");
    } else if (currentSegment.isEnded()) {
      LOGGER.warn("endSegment(partition " + partition + ") called but currentSegment.ended == true. Ignoring.");
    } else {
      LOGGER.info("endSegment(partition " + partition + ") called. Proceeding.");
      sendEndOfSegment(
          partition,
          Maps.newHashMap(), // TODO: Add extra debugging info
          true // TODO: This will not always be true, once we support streaming.
      );
      currentSegment.end();
    }
  }
}
