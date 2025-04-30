package com.linkedin.davinci.validation;

import static com.linkedin.davinci.validation.PartitionTracker.TopicType.REALTIME_TOPIC_TYPE;
import static com.linkedin.davinci.validation.PartitionTracker.TopicType.VERSION_TOPIC_TYPE;
import static com.linkedin.venice.kafka.validation.checksum.CheckSumType.ADHASH;
import static com.linkedin.venice.kafka.validation.checksum.CheckSumType.MD5;
import static com.linkedin.venice.kafka.validation.checksum.CheckSumType.NONE;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.davinci.validation.PartitionTracker.TopicType;
import com.linkedin.venice.exceptions.validation.CorruptDataException;
import com.linkedin.venice.exceptions.validation.DuplicateDataException;
import com.linkedin.venice.exceptions.validation.ImproperlyStartedSegmentException;
import com.linkedin.venice.exceptions.validation.MissingDataException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.EndOfSegment;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.StartOfSegment;
import com.linkedin.venice.kafka.protocol.Update;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.validation.Segment;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.writer.VeniceWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import org.apache.avro.specific.FixedSize;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestPartitionTracker {
  private static final Logger LOGGER = LogManager.getLogger(TestPartitionTracker.class);
  private static TopicType rt = TopicType.of(TopicType.REALTIME_TOPIC_TYPE, "testUrl");
  private static TopicType vt = TopicType.of(TopicType.VERSION_TOPIC_TYPE);
  private PartitionTracker partitionTracker;
  private GUID guid;
  private PubSubTopic realTimeTopic;
  private PubSubTopic versionTopic;
  int partitionId = 0;
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @BeforeMethod(alwaysRun = true)
  public void methodSetUp() {
    this.guid = GuidUtils.getGUID(VeniceProperties.empty());
    String topicNamePrefix = "test_topic_" + System.currentTimeMillis();
    this.realTimeTopic = this.pubSubTopicRepository.getTopic(topicNamePrefix + "_rt");
    this.versionTopic = this.pubSubTopicRepository.getTopic(topicNamePrefix + "_v1");

    /** N.B.: {@link PartitionTracker} instances are always constructed with version topics. */
    this.partitionTracker = new PartitionTracker(versionTopic.getName(), partitionId);
  }

  @DataProvider(name = "TopicType-Checksum")
  public static Object[][] topicTypeChecksumType() {
    return new Object[][] { { rt, MD5 }, { rt, ADHASH }, { rt, NONE }, { vt, MD5 }, { vt, ADHASH }, { vt, NONE } };
  }

  @DataProvider(name = "Two-Booleans-And-TopicType")
  public static Object[][] twoBooleansAndTopicType() {
    return new Object[][] { { true, true, rt }, { false, true, rt }, { true, false, rt }, { false, false, rt },
        { true, true, vt }, { false, true, vt }, { true, false, vt }, { false, false, vt } };
  }

  @DataProvider(name = "TopicType")
  public static Object[][] topicType() {
    return new Object[][] { { rt }, { vt } };
  }

  private PubSubTopic getTopic(TopicType topicType) {
    switch (topicType.getValue()) {
      case VERSION_TOPIC_TYPE:
        return this.versionTopic;
      case REALTIME_TOPIC_TYPE:
        return this.realTimeTopic;
      default:
        throw new IllegalArgumentException("Invalid TopicType: " + topicType);
    }
  }

  private PubSubTopicPartition getPubSubTopicPartition(TopicType topicType) {
    return new PubSubTopicPartitionImpl(getTopic(topicType), this.partitionId);
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

  private KafkaKey getUpdateMessageKey(byte[] bytes) {
    return new KafkaKey(MessageType.UPDATE, bytes);
  }

  private Update getUpdateMessage(byte[] bytes) {
    Update updatePayload = new Update();
    updatePayload.schemaId = 1;
    updatePayload.updateSchemaId = 1;
    updatePayload.updateValue = ByteBuffer.wrap(bytes);

    return updatePayload;
  }

  private KafkaKey getDeleteMessageKey(byte[] bytes) {
    return new KafkaKey(MessageType.DELETE, bytes);
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

  private ControlMessage getEndOfSegment(ByteBuffer checksumValue) {
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageType = ControlMessageType.END_OF_SEGMENT.getValue();
    EndOfSegment endOfSegment = new EndOfSegment();
    endOfSegment.checksumValue = checksumValue;
    controlMessage.controlMessageUnion = endOfSegment;
    return controlMessage;
  }

  @Test(dataProvider = "TopicType", timeOut = 10 * Time.MS_PER_SECOND)
  public void testSequenceNumber(TopicType type) {
    Segment currentSegment = new Segment(partitionId, 0, CheckSumType.NONE);
    PubSubTopicPartition pubSubTopicPartition = getPubSubTopicPartition(type);

    // Send Start_Of_Segment control msg.
    ControlMessage startOfSegment = getStartOfSegment();
    KafkaMessageEnvelope startOfSegmentMessage =
        getKafkaMessageEnvelope(MessageType.CONTROL_MESSAGE, guid, currentSegment, Optional.empty(), startOfSegment);
    long offset = 10;
    DefaultPubSubMessage controlMessageConsumerRecord = new ImmutablePubSubMessage(
        getControlMessageKey(startOfSegmentMessage),
        startOfSegmentMessage,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(offset++),
        System.currentTimeMillis() + 1000,
        0);
    partitionTracker.validateMessage(type, controlMessageConsumerRecord, false, Lazy.FALSE);

    Put firstPut = getPutMessage("first_message".getBytes());
    KafkaMessageEnvelope firstMessage =
        getKafkaMessageEnvelope(MessageType.PUT, guid, currentSegment, Optional.empty(), firstPut); // sequence number
                                                                                                    // is 1
    KafkaKey firstMessageKey = getPutMessageKey("first_key".getBytes());
    DefaultPubSubMessage firstConsumerRecord = new ImmutablePubSubMessage(
        firstMessageKey,
        firstMessage,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(offset++),
        System.currentTimeMillis() + 1000,
        0);
    partitionTracker.validateMessage(type, firstConsumerRecord, false, Lazy.FALSE);

    // Message with gap
    Put secondPut = getPutMessage("second_message".getBytes());
    KafkaMessageEnvelope secondMessage =
        getKafkaMessageEnvelope(MessageType.PUT, guid, currentSegment, Optional.of(100), secondPut); // sequence number
                                                                                                     // is 100
    KafkaKey secondMessageKey = getPutMessageKey("second_key".getBytes());
    DefaultPubSubMessage secondConsumerRecord = new ImmutablePubSubMessage(
        secondMessageKey,
        secondMessage,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(offset++),
        System.currentTimeMillis() + 1000,
        0);
    Assert.assertThrows(
        MissingDataException.class,
        () -> partitionTracker.validateMessage(type, secondConsumerRecord, false, Lazy.FALSE));

    // Message without gap
    Put thirdPut = getPutMessage("third_message".getBytes());
    KafkaMessageEnvelope thirdMessage =
        getKafkaMessageEnvelope(MessageType.PUT, guid, currentSegment, Optional.of(2), thirdPut); // sequence number is
                                                                                                  // 2
    KafkaKey thirdMessageKey = getPutMessageKey("third_key".getBytes());
    DefaultPubSubMessage thirdConsumerRecord = new ImmutablePubSubMessage(
        thirdMessageKey,
        thirdMessage,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(offset++),
        System.currentTimeMillis() + 1000,
        0);
    // It doesn't matter whether EOP is true/false. The result is same.
    partitionTracker.validateMessage(type, thirdConsumerRecord, false, Lazy.FALSE);

    // Message with gap but tolerate messages is allowed
    Put fourthPut = getPutMessage("fourth_message".getBytes());
    KafkaMessageEnvelope fourthMessage =
        getKafkaMessageEnvelope(MessageType.PUT, guid, currentSegment, Optional.of(100), fourthPut); // sequence number
                                                                                                     // is 100
    KafkaKey fourthMessageKey = getPutMessageKey("fourth_key".getBytes());
    DefaultPubSubMessage fourthConsumerRecord = new ImmutablePubSubMessage(
        fourthMessageKey,
        fourthMessage,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(offset),
        System.currentTimeMillis() + 1000,
        0);
    partitionTracker.validateMessage(type, fourthConsumerRecord, false, Lazy.TRUE);
  }

  // This test is verity that no fault is thrown for receiving a new message from an unknown guid for the first time.
  @Test(dataProvider = "Two-Booleans-And-TopicType", timeOut = 10 * Time.MS_PER_SECOND)
  public void testNewSegmentWithSeqNumberAfterEOP(boolean zeroSegmentNum, boolean zeroSequenceNum, TopicType type) {
    int segmentNum = zeroSegmentNum ? 0 : 5;
    int sequenceNum = zeroSequenceNum ? 0 : 10;
    int offset = 10;
    boolean endOfPushReceived = true;

    Segment segment = new Segment(partitionId, segmentNum, CheckSumType.NONE);
    PubSubTopicPartition pubSubTopicPartition = getPubSubTopicPartition(type);

    Put firstPut = getPutMessage("first_message".getBytes());
    KafkaMessageEnvelope firstMessage =
        getKafkaMessageEnvelope(MessageType.PUT, guid, segment, Optional.of(sequenceNum), firstPut);
    KafkaKey firstMessageKey = getPutMessageKey("first_key".getBytes());
    DefaultPubSubMessage firstConsumerRecord = new ImmutablePubSubMessage(
        firstMessageKey,
        firstMessage,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(offset),
        System.currentTimeMillis(),
        0);
    partitionTracker.validateMessage(type, firstConsumerRecord, endOfPushReceived, Lazy.FALSE);
  }

  @Test(dataProvider = "TopicType", timeOut = 10 * Time.MS_PER_SECOND)
  public void testSegmentNumber(TopicType type) {
    PubSubTopicPartition pubSubTopicPartition = getPubSubTopicPartition(type);

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
    DefaultPubSubMessage controlMessageConsumerRecord = new ImmutablePubSubMessage(
        getControlMessageKey(startOfSegmentMessage),
        startOfSegmentMessage,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(offset++),
        System.currentTimeMillis() + 1000,
        0);
    partitionTracker.validateMessage(type, controlMessageConsumerRecord, true, Lazy.FALSE);

    // Send the second segment. Notice this segment number has a gap than previous one
    Put firstPut = getPutMessage("message".getBytes());
    int skipSequenceNumber = 5;
    KafkaMessageEnvelope firstMessage =
        getKafkaMessageEnvelope(MessageType.PUT, guid, secondSegment, Optional.of(skipSequenceNumber), firstPut);
    KafkaKey firstMessageKey = getPutMessageKey("key".getBytes());
    DefaultPubSubMessage firstConsumerRecord = new ImmutablePubSubMessage(
        firstMessageKey,
        firstMessage,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(offset),
        System.currentTimeMillis() + 1000,
        0);
    /**
     * The new message with segment number gap will not be accepted by ProducerTracker if tolerate message flag is false
     */
    Assert.assertThrows(
        MissingDataException.class,
        () -> partitionTracker.validateMessage(type, firstConsumerRecord, true, Lazy.FALSE));
    /**
     * The new message with segment number gap will be accepted and tracked if tolerate message flag is true
     */
    partitionTracker.validateMessage(type, firstConsumerRecord, true, Lazy.TRUE);
    /**
     * Adding the same message again will be treated as duplicated
     */
    Assert.assertThrows(
        DuplicateDataException.class,
        () -> partitionTracker.validateMessage(type, firstConsumerRecord, true, Lazy.TRUE));
    Assert.assertEquals(partitionTracker.getSegment(type, guid).getSegmentNumber(), skipSegmentNumber);
    Assert.assertEquals(partitionTracker.getSegment(type, guid).getSequenceNumber(), skipSequenceNumber);
  }

  @Test(dataProvider = "TopicType-Checksum", timeOut = 10 * Time.MS_PER_SECOND)
  public void testDuplicateMsgsDetected(TopicType type, CheckSumType checkSumType) {
    PubSubTopicPartition pubSubTopicPartition = getPubSubTopicPartition(type);
    Segment firstSegment = new Segment(partitionId, 0, checkSumType);
    long offset = 10;

    // Send SOS
    ControlMessage startOfSegment = getStartOfSegment();
    KafkaMessageEnvelope startOfSegmentMessage =
        getKafkaMessageEnvelope(MessageType.CONTROL_MESSAGE, guid, firstSegment, Optional.empty(), startOfSegment); // sequence
                                                                                                                    // number
                                                                                                                    // is
                                                                                                                    // 0
    DefaultPubSubMessage controlMessageConsumerRecord = new ImmutablePubSubMessage(
        getControlMessageKey(startOfSegmentMessage),
        startOfSegmentMessage,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(offset++),
        System.currentTimeMillis() + 1000,
        0);
    partitionTracker.validateMessage(type, controlMessageConsumerRecord, true, Lazy.FALSE);
    Assert.assertEquals(partitionTracker.getSegment(type, guid).getSequenceNumber(), 0);

    // send EOS
    ControlMessage endOfSegment = getEndOfSegment(ByteBuffer.allocate(0));
    KafkaMessageEnvelope endOfSegmentMessage =
        getKafkaMessageEnvelope(MessageType.CONTROL_MESSAGE, guid, firstSegment, Optional.of(5), endOfSegment); // sequence
                                                                                                                // number
                                                                                                                // is 5
    controlMessageConsumerRecord = new ImmutablePubSubMessage(
        getControlMessageKey(endOfSegmentMessage),
        endOfSegmentMessage,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(offset++),
        System.currentTimeMillis() + 1000,
        0);
    partitionTracker.validateMessage(type, controlMessageConsumerRecord, true, Lazy.TRUE);
    Assert.assertEquals(partitionTracker.getSegment(type, guid).getSequenceNumber(), 5);

    // Send a put msg following EOS
    Put firstPut = getPutMessage("first_message".getBytes());
    KafkaMessageEnvelope firstMessage =
        getKafkaMessageEnvelope(MessageType.PUT, guid, firstSegment, Optional.of(1), firstPut); // sequence number is 1
    KafkaKey firstMessageKey = getPutMessageKey("first_key".getBytes());
    DefaultPubSubMessage firstConsumerRecord = new ImmutablePubSubMessage(
        firstMessageKey,
        firstMessage,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(offset),
        System.currentTimeMillis() + 1000,
        0);

    DuplicateDataException caught = null;
    try {
      partitionTracker.validateMessage(type, firstConsumerRecord, true, Lazy.TRUE);
      fail("Should have thrown a DuplicateDataException!");
    } catch (DuplicateDataException e) {
      caught = e;
    }
    try {
      partitionTracker.validateMessage(type, firstConsumerRecord, true, Lazy.TRUE);
      fail("Should have thrown a DuplicateDataException!");
    } catch (DuplicateDataException e) {
      assertSame(caught, e, "Duplicate data exceptions should be a singleton!");
    }

    // The sequence number should not change
    Assert.assertEquals(partitionTracker.getSegment(type, guid).getSequenceNumber(), 5);
  }

  /**
   * This test is to ensure when meeting a mid segment, i.e. segment which doesn't start with SOS, the check sum
   * type and check sum state should be aligned with each other.
   */
  @Test(dataProvider = "TopicType-Checksum", timeOut = 10 * Time.MS_PER_SECOND)
  public void testMidSegmentCheckSumStates(TopicType type, CheckSumType checkSumType) {
    boolean isVersionTopic = TopicType.isVersionTopic(type);
    PubSubTopicPartition pubSubTopicPartition = getPubSubTopicPartition(type);
    Segment firstSegment = new Segment(partitionId, 0, checkSumType);
    Segment secondSegment = new Segment(partitionId, 1, checkSumType);
    long offset = 10;
    OffsetRecord record = TestUtils.getOffsetRecord(offset);

    // Send SOS with check sum type set to checkpoint-able checkSumType.
    ControlMessage startOfSegment = getStartOfSegment(checkSumType);
    KafkaMessageEnvelope startOfSegmentMessage =
        getKafkaMessageEnvelope(MessageType.CONTROL_MESSAGE, guid, firstSegment, Optional.empty(), startOfSegment);
    DefaultPubSubMessage controlMessageConsumerRecord = new ImmutablePubSubMessage(
        getControlMessageKey(startOfSegmentMessage),
        startOfSegmentMessage,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(offset++),
        System.currentTimeMillis() + 1000,
        0);
    partitionTracker.validateMessage(type, controlMessageConsumerRecord, true, Lazy.FALSE);
    partitionTracker.updateOffsetRecord(type, record);
    if (isVersionTopic) {
      Assert.assertEquals(record.getProducerPartitionState(guid).checksumType, checkSumType.getValue());
    } else {
      Assert.assertEquals(
          record.getRealTimeProducerState(type.getKafkaUrl(), guid).checksumType,
          checkSumType.getValue());
    }

    // The msg is a put msg without check sum type
    Put firstPut = getPutMessage("first_message".getBytes());
    KafkaMessageEnvelope firstMessage =
        getKafkaMessageEnvelope(MessageType.PUT, guid, secondSegment, Optional.empty(), firstPut);
    KafkaKey firstMessageKey = getPutMessageKey("first_key".getBytes());
    DefaultPubSubMessage firstConsumerRecord = new ImmutablePubSubMessage(
        firstMessageKey,
        firstMessage,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(offset),
        System.currentTimeMillis() + 1000,
        0);
    partitionTracker.validateMessage(type, firstConsumerRecord, true, Lazy.TRUE);
    partitionTracker.updateOffsetRecord(type, record);
    if (isVersionTopic) {
      Assert.assertEquals(record.getProducerPartitionState(guid).checksumType, CheckSumType.NONE.getValue());
      Assert.assertEquals(record.getProducerPartitionState(guid).checksumState, ByteBuffer.wrap(new byte[0]));
    } else {
      Assert.assertEquals(
          record.getRealTimeProducerState(type.getKafkaUrl(), guid).checksumType,
          CheckSumType.NONE.getValue());
      Assert.assertEquals(
          record.getRealTimeProducerState(type.getKafkaUrl(), guid).checksumState,
          ByteBuffer.wrap(new byte[0]));
    }
  }

  @Test(dataProvider = "TopicType-Checksum", timeOut = 10 * Time.MS_PER_SECOND)
  public void endToEndDIV(TopicType type, CheckSumType checkSumType) {
    int segmentId = 0;
    long offset = 10;

    /** SCENARIO 1: Consume full segment with an empty checksum at the end (valid only if checksum type is NONE). */
    List<DefaultPubSubMessage> messageBatch1 =
        getSegmentOfMessages(type, checkSumType, segmentId++, offset, ByteBuffer.allocate(0));

    /** We stop shy of the last message, because we'll handle it separately after... */
    int indexPriorToLastMessage = messageBatch1.size() - 1;
    for (int i = 0; i < indexPriorToLastMessage; i++) {
      DefaultPubSubMessage message = messageBatch1.get(i);
      this.partitionTracker.validateMessage(type, message, true, Lazy.FALSE);
    }

    DefaultPubSubMessage lastMessage = messageBatch1.get(messageBatch1.size() - 1);
    offset = lastMessage.getOffset().getNumericOffset() + 1;

    if (checkSumType == NONE) {
      this.partitionTracker.validateMessage(type, lastMessage, true, Lazy.FALSE);
    } else {
      try {
        this.partitionTracker.validateMessage(type, lastMessage, true, Lazy.FALSE);
        fail("Should have thrown!");
      } catch (CorruptDataException e) {
        LOGGER.info("Caught CorruptDataEx!", e);
      }
    }

    /** SCENARIO 2: Consume full segment with a proper checksum at the end. */
    ByteBuffer computedChecksumState = ByteBuffer.wrap(this.partitionTracker.getSegment(type, guid).getFinalCheckSum());

    List<DefaultPubSubMessage> messageBatch2 =
        getSegmentOfMessages(type, checkSumType, segmentId++, offset, computedChecksumState);

    /** This time, we should be able to process all messages, including the EOS with checksum. */
    for (int i = 0; i < messageBatch2.size(); i++) {
      DefaultPubSubMessage message = messageBatch2.get(i);
      this.partitionTracker.validateMessage(type, message, true, Lazy.FALSE);
    }

    /** SCENARIO 2.1: Consume full segment with a present but wrong checksum at the end. */
    ByteBuffer corruptedChecksumState = ByteBuffer.allocate(computedChecksumState.remaining());
    for (byte i = 0; i < computedChecksumState.remaining(); i++) {
      corruptedChecksumState.put(i);
    }
    corruptedChecksumState.position(0);

    List<DefaultPubSubMessage> messageBatch2point1 =
        getSegmentOfMessages(type, checkSumType, segmentId++, offset, corruptedChecksumState);

    /** We stop shy of the last message, because we'll handle it separately after... */
    indexPriorToLastMessage = messageBatch2point1.size() - 1;
    for (int i = 0; i < indexPriorToLastMessage; i++) {
      DefaultPubSubMessage message = messageBatch2point1.get(i);
      this.partitionTracker.validateMessage(type, message, true, Lazy.FALSE);
    }

    lastMessage = messageBatch2point1.get(messageBatch2point1.size() - 1);

    if (checkSumType == NONE) {
      this.partitionTracker.validateMessage(type, lastMessage, true, Lazy.FALSE);
    } else {
      try {
        this.partitionTracker.validateMessage(type, lastMessage, true, Lazy.FALSE);
        fail("Should have thrown!");
      } catch (CorruptDataException e) {
        LOGGER.info("Caught CorruptDataEx!", e);
      }
    }

    /**
     * Now we want to start testing the behavior in various data loss scenarios.
     *
     * SCENARIO 3: Skip the SOS.
     */

    List<DefaultPubSubMessage> messageBatch3 =
        getSegmentOfMessages(type, checkSumType, segmentId++, offset, computedChecksumState);

    /** Starting at message index 1 (i.e. skipping the SOS). */
    assertThrows(
        ImproperlyStartedSegmentException.class,
        () -> this.partitionTracker.validateMessage(type, messageBatch3.get(1), true, Lazy.FALSE));

    /**
     * Then we can keep going until the end of the segment, and will not trigger a checksum error, since the segment is
     * unregistered and therefore the checksum is unusable.
     */
    for (int i = 2; i < messageBatch3.size(); i++) {
      DefaultPubSubMessage message = messageBatch3.get(i);
      this.partitionTracker.validateMessage(type, message, true, Lazy.FALSE);
    }

    /** SCENARIO 4: Skip a data message. */
    List<DefaultPubSubMessage> messageBatch4 =
        getSegmentOfMessages(type, checkSumType, segmentId++, offset, computedChecksumState);

    /** Starting with the SOS. */
    this.partitionTracker.validateMessage(type, messageBatch4.get(0), true, Lazy.FALSE);

    for (int i = 1; i < messageBatch4.size() - 1; i++) {
      DefaultPubSubMessage currentMessage = messageBatch4.get(i);
      DefaultPubSubMessage nextMessage = messageBatch4.get(i + 1);

      try {
        /** Skip some data message, and go straight to the next one. */
        this.partitionTracker.validateMessage(type, nextMessage, true, Lazy.FALSE);
        fail("Should have thrown!");
      } catch (MissingDataException e) {
        /**
         * N.B.: {@link ImproperlyStartedSegmentException} is a subclass of {@link MissingDataException}, but not the one
         *       we're looking for.
         */
        assertFalse(e instanceof ImproperlyStartedSegmentException);
      }

      /** But if we do consume the correct message (monotonically speaking), then it's fine. */
      this.partitionTracker.validateMessage(type, currentMessage, true, Lazy.FALSE);
    }

    assertTrue(segmentId > 0);
  }

  private List<DefaultPubSubMessage> getSegmentOfMessages(
      TopicType type,
      CheckSumType checkSumType,
      int segmentId,
      long startingOffset,
      ByteBuffer eosChecksumValue) {
    PubSubTopicPartition pubSubTopicPartition = getPubSubTopicPartition(type);
    Segment segment = new Segment(partitionId, segmentId, checkSumType);
    long offset = startingOffset;

    List<DefaultPubSubMessage> messages = new ArrayList<>();

    // 1st message: SOS
    ControlMessage startOfSegment = getStartOfSegment(checkSumType);
    KafkaMessageEnvelope sosMessage =
        getKafkaMessageEnvelope(MessageType.CONTROL_MESSAGE, guid, segment, Optional.empty(), startOfSegment);
    DefaultPubSubMessage sosConsumerRecord = new ImmutablePubSubMessage(
        getControlMessageKey(sosMessage),
        sosMessage,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(offset++),
        System.currentTimeMillis() + 1000,
        0);
    messages.add(sosConsumerRecord);

    // 2nd message: PUT
    Put firstPut = getPutMessage("first_message".getBytes());
    KafkaMessageEnvelope putMessage =
        getKafkaMessageEnvelope(MessageType.PUT, guid, segment, Optional.empty(), firstPut);
    DefaultPubSubMessage putConsumerRecord = new ImmutablePubSubMessage(
        getPutMessageKey("first_key".getBytes()),
        putMessage,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(offset++),
        System.currentTimeMillis() + 1000,
        0);
    messages.add(putConsumerRecord);

    // 3rd message: UPDATE
    Update firstUpdate = getUpdateMessage("second_message".getBytes());
    KafkaMessageEnvelope updateMessage =
        getKafkaMessageEnvelope(MessageType.UPDATE, guid, segment, Optional.empty(), firstUpdate);
    DefaultPubSubMessage updateConsumerRecord = new ImmutablePubSubMessage(
        getUpdateMessageKey("second_key".getBytes()),
        updateMessage,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(offset++),
        System.currentTimeMillis() + 1000,
        0);
    messages.add(updateConsumerRecord);

    // 4th message: DELETE
    Delete firstDelete = new Delete();
    KafkaMessageEnvelope deleteMessage =
        getKafkaMessageEnvelope(MessageType.DELETE, guid, segment, Optional.empty(), firstDelete);
    DefaultPubSubMessage deleteConsumerRecord = new ImmutablePubSubMessage(
        getDeleteMessageKey("third_key".getBytes()),
        deleteMessage,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(offset++),
        System.currentTimeMillis() + 1000,
        0);
    messages.add(deleteConsumerRecord);

    // Final message: EOS
    ControlMessage endOfSegment = getEndOfSegment(eosChecksumValue);
    KafkaMessageEnvelope eosMessage =
        getKafkaMessageEnvelope(MessageType.CONTROL_MESSAGE, guid, segment, Optional.empty(), endOfSegment);
    DefaultPubSubMessage eosConsumerRecord = new ImmutablePubSubMessage(
        getControlMessageKey(eosMessage),
        eosMessage,
        pubSubTopicPartition,
        ApacheKafkaOffsetPosition.of(offset++),
        System.currentTimeMillis() + 1000,
        0);
    messages.add(eosConsumerRecord);

    assertTrue(offset > startingOffset);

    return messages;
  }
}
