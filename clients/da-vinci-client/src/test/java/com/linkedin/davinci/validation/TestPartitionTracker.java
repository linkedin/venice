package com.linkedin.davinci.validation;

import static org.mockito.Mockito.mock;

import com.linkedin.venice.exceptions.validation.DuplicateDataException;
import com.linkedin.venice.exceptions.validation.MissingDataException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.EndOfSegment;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.StartOfSegment;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.ProducerPartitionState;
import com.linkedin.venice.kafka.validation.Segment;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.writer.VeniceWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Optional;
import org.apache.avro.specific.FixedSize;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestPartitionTracker {
  private PartitionTracker partitionTracker;
  private GUID guid;
  private String topic;
  int partitionId = 0;
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @BeforeMethod(alwaysRun = true)
  public void methodSetUp() {
    String testGuid = "test_guid_" + System.currentTimeMillis();
    this.guid = new GUID();
    guid.bytes(testGuid.getBytes());
    this.topic = "test_topic_" + System.currentTimeMillis() + "_v1";
    this.partitionTracker = new PartitionTracker(topic, partitionId);
  }

  private KafkaMessageEnvelope getKafkaMessageEnvelope(
      MessageType messageType,
      GUID producerGUID,
      Segment currentSegment,
      Optional<Integer> sequenceNum,
      Object payload) {
    KafkaMessageEnvelope kafkaValue = new KafkaMessageEnvelope();
    kafkaValue.messageType = messageType.getValue();

    ProducerMetadata producerMetadata = new ProducerMetadata();
    producerMetadata.producerGUID = producerGUID;
    producerMetadata.segmentNumber = currentSegment.getSegmentNumber();
    if (sequenceNum.isPresent()) {
      producerMetadata.messageSequenceNumber = sequenceNum.get();
    } else {
      producerMetadata.messageSequenceNumber = currentSegment.getAndIncrementSequenceNumber();
    }
    producerMetadata.messageTimestamp = System.currentTimeMillis();
    producerMetadata.logicalTimestamp = VeniceWriter.VENICE_DEFAULT_LOGICAL_TS;
    kafkaValue.producerMetadata = producerMetadata;
    kafkaValue.payloadUnion = payload;

    return kafkaValue;
  }

  private static final int CONTROL_MESSAGE_KAFKA_KEY_LENGTH =
      GUID.class.getAnnotation(FixedSize.class).value() + Integer.SIZE * 2;

  private KafkaKey getControlMessageKey(KafkaMessageEnvelope kafkaValue) {
    return new KafkaKey(
        MessageType.CONTROL_MESSAGE,
        ByteBuffer.allocate(CONTROL_MESSAGE_KAFKA_KEY_LENGTH)
            .put(kafkaValue.producerMetadata.producerGUID.bytes())
            .putInt(kafkaValue.producerMetadata.segmentNumber)
            .putInt(kafkaValue.producerMetadata.messageSequenceNumber)
            .array());
  }

  private KafkaKey getPutMessageKey(byte[] bytes) {
    return new KafkaKey(MessageType.PUT, bytes);
  }

  private Put getPutMessage(byte[] bytes) {
    Put putPayload = new Put();
    putPayload.putValue = ByteBuffer.wrap(bytes);
    putPayload.schemaId = -1;

    return putPayload;
  }

  private ControlMessage getStartOfSegment() {
    return getStartOfSegment(CheckSumType.NONE);
  }

  private ControlMessage getStartOfSegment(CheckSumType checkSumType) {
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageType = ControlMessageType.START_OF_SEGMENT.getValue();
    StartOfSegment startOfSegment = new StartOfSegment();
    startOfSegment.upcomingAggregates = new ArrayList<>();
    startOfSegment.upcomingAggregates.add("baz");
    startOfSegment.checksumType = checkSumType.getValue();
    controlMessage.controlMessageUnion = startOfSegment;
    controlMessage.debugInfo = new HashMap<>();
    controlMessage.debugInfo.put("foo", "bar");

    return controlMessage;
  }

  private ControlMessage getEndOfSegment() {
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageType = ControlMessageType.END_OF_SEGMENT.getValue();
    EndOfSegment endOfSegment = new EndOfSegment();
    controlMessage.controlMessageUnion = endOfSegment;
    return controlMessage;
  }

  @Test
  public void testSequenceNumber() {
    Segment currentSegment = new Segment(partitionId, 0, CheckSumType.NONE);
    PubSubTopicPartition pubSubTopicPartition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), partitionId);

    // Send Start_Of_Segment control msg.
    ControlMessage startOfSegment = getStartOfSegment();
    KafkaMessageEnvelope startOfSegmentMessage =
        getKafkaMessageEnvelope(MessageType.CONTROL_MESSAGE, guid, currentSegment, Optional.empty(), startOfSegment);
    long offset = 10;
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> controlMessageConsumerRecord = new ImmutablePubSubMessage<>(
        getControlMessageKey(startOfSegmentMessage),
        startOfSegmentMessage,
        pubSubTopicPartition,
        offset++,
        System.currentTimeMillis() + 1000,
        0);
    partitionTracker.validateMessage(controlMessageConsumerRecord, false, Lazy.FALSE);

    Put firstPut = getPutMessage("first_message".getBytes());
    KafkaMessageEnvelope firstMessage =
        getKafkaMessageEnvelope(MessageType.PUT, guid, currentSegment, Optional.empty(), firstPut); // sequence number
                                                                                                    // is 1
    KafkaKey firstMessageKey = getPutMessageKey("first_key".getBytes());
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> firstConsumerRecord = new ImmutablePubSubMessage<>(
        firstMessageKey,
        firstMessage,
        pubSubTopicPartition,
        offset++,
        System.currentTimeMillis() + 1000,
        0);
    partitionTracker.validateMessage(firstConsumerRecord, false, Lazy.FALSE);

    // Message with gap
    Put secondPut = getPutMessage("second_message".getBytes());
    KafkaMessageEnvelope secondMessage =
        getKafkaMessageEnvelope(MessageType.PUT, guid, currentSegment, Optional.of(100), secondPut); // sequence number
                                                                                                     // is 100
    KafkaKey secondMessageKey = getPutMessageKey("second_key".getBytes());
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> secondConsumerRecord = new ImmutablePubSubMessage<>(
        secondMessageKey,
        secondMessage,
        pubSubTopicPartition,
        offset++,
        System.currentTimeMillis() + 1000,
        0);
    Assert.assertThrows(
        MissingDataException.class,
        () -> partitionTracker.validateMessage(secondConsumerRecord, false, Lazy.FALSE));

    // Message without gap
    Put thirdPut = getPutMessage("third_message".getBytes());
    KafkaMessageEnvelope thirdMessage =
        getKafkaMessageEnvelope(MessageType.PUT, guid, currentSegment, Optional.of(2), thirdPut); // sequence number is
                                                                                                  // 2
    KafkaKey thirdMessageKey = getPutMessageKey("third_key".getBytes());
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> thirdConsumerRecord = new ImmutablePubSubMessage<>(
        thirdMessageKey,
        thirdMessage,
        pubSubTopicPartition,
        offset++,
        System.currentTimeMillis() + 1000,
        0);
    // It doesn't matter whether EOP is true/false. The result is same.
    partitionTracker.validateMessage(thirdConsumerRecord, false, Lazy.FALSE);

    // Message with gap but tolerate messages is allowed
    Put fourthPut = getPutMessage("fourth_message".getBytes());
    KafkaMessageEnvelope fourthMessage =
        getKafkaMessageEnvelope(MessageType.PUT, guid, currentSegment, Optional.of(100), fourthPut); // sequence number
                                                                                                     // is 100
    KafkaKey fourthMessageKey = getPutMessageKey("fourth_key".getBytes());
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> fourthConsumerRecord = new ImmutablePubSubMessage<>(
        fourthMessageKey,
        fourthMessage,
        pubSubTopicPartition,
        offset,
        System.currentTimeMillis() + 1000,
        0);
    partitionTracker.validateMessage(fourthConsumerRecord, false, Lazy.TRUE);
  }

  // This test is verity that no fault is thrown for receiving a new message from an unknown guid for the first time.
  @Test(dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = 10
      * Time.MS_PER_SECOND)
  public void testNewSegmentWithSeqNumberAfterEOP(boolean zeroSegmentNum, boolean zeroSequenceNum) {
    int segmentNum = zeroSegmentNum ? 0 : 5;
    int sequenceNum = zeroSequenceNum ? 0 : 10;
    int offset = 10;
    boolean endOfPushReceived = true;

    Segment segment = new Segment(partitionId, segmentNum, CheckSumType.NONE);
    PubSubTopicPartition pubSubTopicPartition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), partitionId);

    Put firstPut = getPutMessage("first_message".getBytes());
    KafkaMessageEnvelope firstMessage =
        getKafkaMessageEnvelope(MessageType.PUT, guid, segment, Optional.of(sequenceNum), firstPut);
    KafkaKey firstMessageKey = getPutMessageKey("first_key".getBytes());
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> firstConsumerRecord = new ImmutablePubSubMessage<>(
        firstMessageKey,
        firstMessage,
        pubSubTopicPartition,
        offset,
        System.currentTimeMillis(),
        0);
    partitionTracker.validateMessage(firstConsumerRecord, endOfPushReceived, Lazy.FALSE);
  }

  @Test(timeOut = 5 * Time.MS_PER_SECOND)
  public void testHandleFirstMessageAfterLoadingFromCheckPoint() {
    PubSubTopicPartition pubSubTopicPartition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), partitionId);
    int segmentNum = 1;
    int sequenceNum = 1;
    int firstNewSegmentNumber = 5;
    int firstNewSequenceNum = 2;
    int secondNewSegmentNumber = 6;
    int secondNewSequenceNum = 10;
    long offset = 10;

    // Create an InternalAvroSpecificSerializer instance
    InternalAvroSpecificSerializer<PartitionState> serializer = mock(InternalAvroSpecificSerializer.class);

    // Create an OffsetRecord instance with your specific constructor
    OffsetRecord offsetRecord = new OffsetRecord(serializer);

    // Create a ProducerPartitionState instance
    ProducerPartitionState state = new ProducerPartitionState();
    state.segmentNumber = segmentNum;
    state.setMessageSequenceNumber(sequenceNum);
    state.checksumType = 0;
    state.checksumState = ByteBuffer.wrap(new byte[0]);
    state.isRegistered = true;
    state.messageTimestamp = System.currentTimeMillis();

    // Set the ProducerPartitionState in the OffsetRecord
    offsetRecord.setProducerPartitionState(guid, state);

    // Set the OffsetRecord in the PartitionTracker to mimic the behavior of loading from checkpoint.
    partitionTracker.setPartitionState(offsetRecord, 0);

    // Send the first message after loading from checkpoint.
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> firstMsgAfterLoadingFromCheckPoint = buildPutMessage(
        firstNewSegmentNumber,
        firstNewSequenceNum,
        offset,
        pubSubTopicPartition,
        "first_key",
        "first_message");

    /**
     * After loading from checkpoint, the new message with segment number gap should be accepted by ProducerTracker
     * when it is the 1st encountered message even if tolerate message flag is false.
     */
    partitionTracker.validateMessage(firstMsgAfterLoadingFromCheckPoint, true, Lazy.FALSE);

    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> secondMsgAfterLoadingFromCheckPoint = buildPutMessage(
        secondNewSegmentNumber,
        secondNewSequenceNum,
        offset + 1,
        pubSubTopicPartition,
        "second_key",
        "second_message");

    // After the 1st message is handled correctly, if DIV still sees segment gaps, it should report
    // MissingDataException.
    Assert.assertThrows(
        MissingDataException.class,
        () -> partitionTracker.validateMessage(secondMsgAfterLoadingFromCheckPoint, true, Lazy.FALSE));
  }

  private PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> buildPutMessage(
      int segNum,
      int seqNum,
      long offset,
      PubSubTopicPartition partition,
      String key,
      String value) {
    Segment segment = new Segment(partitionId, segNum, CheckSumType.NONE);
    Put putMsg = getPutMessage(value.getBytes());
    KafkaMessageEnvelope putMsgEnv =
        getKafkaMessageEnvelope(MessageType.PUT, guid, segment, Optional.of(seqNum), putMsg);
    KafkaKey putKey = getPutMessageKey(key.getBytes());

    return new ImmutablePubSubMessage<>(putKey, putMsgEnv, partition, offset, System.currentTimeMillis(), 0);
  }

  @Test
  public void testSegmentNumber() {
    PubSubTopicPartition pubSubTopicPartition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), partitionId);

    int firstSegmentNumber = 0;
    int skipSegmentNumber = 2;
    Segment firstSegment = new Segment(partitionId, firstSegmentNumber, CheckSumType.NONE);
    Segment secondSegment = new Segment(partitionId, skipSegmentNumber, CheckSumType.NONE);
    long offset = 10;

    // Send the first segment
    ControlMessage startOfSegment = getStartOfSegment();
    KafkaMessageEnvelope startOfSegmentMessage =
        getKafkaMessageEnvelope(MessageType.CONTROL_MESSAGE, guid, firstSegment, Optional.empty(), startOfSegment); // sequence
                                                                                                                    // number
                                                                                                                    // is
                                                                                                                    // zero
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> controlMessageConsumerRecord = new ImmutablePubSubMessage<>(
        getControlMessageKey(startOfSegmentMessage),
        startOfSegmentMessage,
        pubSubTopicPartition,
        offset++,
        System.currentTimeMillis() + 1000,
        0);
    partitionTracker.validateMessage(controlMessageConsumerRecord, true, Lazy.FALSE);

    // Send the second segment. Notice this segment number has a gap than previous one
    Put firstPut = getPutMessage("message".getBytes());
    int skipSequenceNumber = 5;
    KafkaMessageEnvelope firstMessage =
        getKafkaMessageEnvelope(MessageType.PUT, guid, secondSegment, Optional.of(skipSequenceNumber), firstPut);
    KafkaKey firstMessageKey = getPutMessageKey("key".getBytes());
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> firstConsumerRecord = new ImmutablePubSubMessage<>(
        firstMessageKey,
        firstMessage,
        pubSubTopicPartition,
        offset,
        System.currentTimeMillis() + 1000,
        0);
    /**
     * The new message with segment number gap will not be accepted by ProducerTracker if tolerate message flag is false
     */
    Assert.assertThrows(
        MissingDataException.class,
        () -> partitionTracker.validateMessage(firstConsumerRecord, true, Lazy.FALSE));
    /**
     * The new message with segment number gap will be accepted and tracked if tolerate message flag is true
     */
    partitionTracker.validateMessage(firstConsumerRecord, true, Lazy.TRUE);
    /**
     * Adding the same message again will be treated as duplicated
     */
    Assert.assertThrows(
        DuplicateDataException.class,
        () -> partitionTracker.validateMessage(firstConsumerRecord, true, Lazy.TRUE));
    Assert.assertEquals(partitionTracker.getSegment(guid).getSegmentNumber(), skipSegmentNumber);
    Assert.assertEquals(partitionTracker.getSegment(guid).getSequenceNumber(), skipSequenceNumber);
  }

  @Test
  public void testDuplicateMsgsDetected() {
    PubSubTopicPartition pubSubTopicPartition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), partitionId);
    Segment firstSegment = new Segment(partitionId, 0, CheckSumType.MD5);
    long offset = 10;

    // Send SOS
    ControlMessage startOfSegment = getStartOfSegment();
    KafkaMessageEnvelope startOfSegmentMessage =
        getKafkaMessageEnvelope(MessageType.CONTROL_MESSAGE, guid, firstSegment, Optional.empty(), startOfSegment); // sequence
                                                                                                                    // number
                                                                                                                    // is
                                                                                                                    // 0
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> controlMessageConsumerRecord = new ImmutablePubSubMessage<>(
        getControlMessageKey(startOfSegmentMessage),
        startOfSegmentMessage,
        pubSubTopicPartition,
        offset++,
        System.currentTimeMillis() + 1000,
        0);
    partitionTracker.validateMessage(controlMessageConsumerRecord, true, Lazy.FALSE);
    Assert.assertEquals(partitionTracker.getSegment(guid).getSequenceNumber(), 0);

    // send EOS
    ControlMessage endOfSegment = getEndOfSegment();
    KafkaMessageEnvelope endOfSegmentMessage =
        getKafkaMessageEnvelope(MessageType.CONTROL_MESSAGE, guid, firstSegment, Optional.of(5), endOfSegment); // sequence
                                                                                                                // number
                                                                                                                // is 5
    controlMessageConsumerRecord = new ImmutablePubSubMessage<>(
        getControlMessageKey(endOfSegmentMessage),
        endOfSegmentMessage,
        pubSubTopicPartition,
        offset++,
        System.currentTimeMillis() + 1000,
        0);
    partitionTracker.validateMessage(controlMessageConsumerRecord, true, Lazy.TRUE);
    Assert.assertEquals(partitionTracker.getSegment(guid).getSequenceNumber(), 5);

    // Send a put msg following EOS
    Put firstPut = getPutMessage("first_message".getBytes());
    KafkaMessageEnvelope firstMessage =
        getKafkaMessageEnvelope(MessageType.PUT, guid, firstSegment, Optional.of(1), firstPut); // sequence number is 1
    KafkaKey firstMessageKey = getPutMessageKey("first_key".getBytes());
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> firstConsumerRecord = new ImmutablePubSubMessage<>(
        firstMessageKey,
        firstMessage,
        pubSubTopicPartition,
        offset,
        System.currentTimeMillis() + 1000,
        0);
    Assert.assertThrows(
        DuplicateDataException.class,
        () -> partitionTracker.validateMessage(firstConsumerRecord, true, Lazy.TRUE));
    // The sequence number should not change
    Assert.assertEquals(partitionTracker.getSegment(guid).getSequenceNumber(), 5);
  }

  /**
   * This test is to ensure when meeting a mid segment, i.e. segment which doesn't start with SOS, the check sum
   * type and check sum state should be aligned with each other.
   */
  @Test
  public void testMidSegmentCheckSumStates() {
    PubSubTopicPartition pubSubTopicPartition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), partitionId);
    Segment firstSegment = new Segment(partitionId, 0, CheckSumType.MD5);
    Segment secondSegment = new Segment(partitionId, 1, CheckSumType.MD5);
    long offset = 10;
    OffsetRecord record = TestUtils.getOffsetRecord(offset);

    // Send SOS with check sum type set to MD5
    ControlMessage startOfSegment = getStartOfSegment(CheckSumType.MD5);
    KafkaMessageEnvelope startOfSegmentMessage =
        getKafkaMessageEnvelope(MessageType.CONTROL_MESSAGE, guid, firstSegment, Optional.empty(), startOfSegment);
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> controlMessageConsumerRecord = new ImmutablePubSubMessage<>(
        getControlMessageKey(startOfSegmentMessage),
        startOfSegmentMessage,
        pubSubTopicPartition,
        offset++,
        System.currentTimeMillis() + 1000,
        0);
    partitionTracker.validateMessage(controlMessageConsumerRecord, true, Lazy.FALSE);
    partitionTracker.updateOffsetRecord(record);
    Assert.assertEquals(record.getProducerPartitionState(guid).checksumType, CheckSumType.MD5.getValue());

    // The msg is a put msg without check sum type
    Put firstPut = getPutMessage("first_message".getBytes());
    KafkaMessageEnvelope firstMessage =
        getKafkaMessageEnvelope(MessageType.PUT, guid, secondSegment, Optional.empty(), firstPut);
    KafkaKey firstMessageKey = getPutMessageKey("first_key".getBytes());
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> firstConsumerRecord = new ImmutablePubSubMessage<>(
        firstMessageKey,
        firstMessage,
        pubSubTopicPartition,
        offset,
        System.currentTimeMillis() + 1000,
        0);
    partitionTracker.validateMessage(firstConsumerRecord, true, Lazy.TRUE);
    partitionTracker.updateOffsetRecord(record);
    Assert.assertEquals(record.getProducerPartitionState(guid).checksumType, CheckSumType.NONE.getValue());
    Assert.assertEquals(record.getProducerPartitionState(guid).checksumState, ByteBuffer.wrap(new byte[0]));
  }
}
