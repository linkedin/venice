package com.linkedin.venice.kafka.validation;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.Update;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.message.KafkaKey;
import java.nio.ByteBuffer;
import org.testng.annotations.Test;


public class SegmentTest {
  @Test
  public void test() {
    Segment segmentWithoutChecksum = new Segment(0, 0, CheckSumType.NONE);
    assertEquals(segmentWithoutChecksum.getCheckSumType(), CheckSumType.NONE);
    assertEquals(segmentWithoutChecksum.getCheckSumState(), new byte[0]);
    assertEquals(segmentWithoutChecksum.getFinalCheckSum(), new byte[0]);

    Segment segmentWithMD5Checksum1 = new Segment(0, 0, CheckSumType.MD5);
    assertEquals(segmentWithMD5Checksum1.getCheckSumType(), CheckSumType.MD5);
    assertTrue(segmentWithMD5Checksum1.getCheckSumState().length > 0);

    Segment segmentWithMD5Checksum2 = new Segment(0, 0, CheckSumType.MD5);
    assertEquals(segmentWithMD5Checksum2.getCheckSumType(), CheckSumType.MD5);
    assertTrue(segmentWithMD5Checksum2.getCheckSumState().length > 0);

    // Verify checksum determinism
    assertEquals(segmentWithMD5Checksum1.getCheckSumState(), segmentWithMD5Checksum2.getCheckSumState());
    KafkaMessageEnvelope messageEnvelope1 = new KafkaMessageEnvelope();
    messageEnvelope1.setMessageType(MessageType.CONTROL_MESSAGE.getValue());
    ControlMessage controlMessage1 = new ControlMessage();
    controlMessage1.setControlMessageType(ControlMessageType.START_OF_SEGMENT.getValue());
    messageEnvelope1.setPayloadUnion(controlMessage1);
    segmentWithMD5Checksum1.addToCheckSum(null, messageEnvelope1);
    assertNotEquals(
        segmentWithMD5Checksum1.getCheckSumState(),
        segmentWithMD5Checksum2.getCheckSumState(),
        "Checksum state should have changed!");
    segmentWithMD5Checksum2.addToCheckSum(null, messageEnvelope1);
    assertEquals(
        segmentWithMD5Checksum1.getCheckSumState(),
        segmentWithMD5Checksum2.getCheckSumState(),
        "Identical operations should result in identical checksums!");

    KafkaKey putKey = new KafkaKey(MessageType.PUT, new byte[] { 1, 2, 3 });
    KafkaMessageEnvelope messageEnvelope2 = new KafkaMessageEnvelope();
    messageEnvelope2.setMessageType(MessageType.PUT.getValue());
    Put put = new Put();
    put.setSchemaId(1);
    put.setPutValue(ByteBuffer.wrap(new byte[] { 1, 2, 3 }));
    messageEnvelope2.setPayloadUnion(put);
    segmentWithMD5Checksum1.addToCheckSum(putKey, messageEnvelope2);
    assertNotEquals(
        segmentWithMD5Checksum1.getCheckSumState(),
        segmentWithMD5Checksum2.getCheckSumState(),
        "Checksum state should have changed!");
    segmentWithMD5Checksum2.addToCheckSum(putKey, messageEnvelope2);
    assertEquals(
        segmentWithMD5Checksum1.getCheckSumState(),
        segmentWithMD5Checksum2.getCheckSumState(),
        "Identical operations should result in identical checksums!");

    KafkaMessageEnvelope messageEnvelope3 = new KafkaMessageEnvelope();
    messageEnvelope3.setMessageType(MessageType.UPDATE.getValue());
    Update update = new Update();
    update.setSchemaId(1);
    update.setUpdateSchemaId(1);
    update.setUpdateValue(ByteBuffer.wrap(new byte[] { 1, 2, 3 }));
    messageEnvelope3.setPayloadUnion(update);
    segmentWithMD5Checksum1.addToCheckSum(putKey, messageEnvelope3);
    assertNotEquals(
        segmentWithMD5Checksum1.getCheckSumState(),
        segmentWithMD5Checksum2.getCheckSumState(),
        "Checksum state should have changed!");
    segmentWithMD5Checksum2.addToCheckSum(putKey, messageEnvelope3);
    assertEquals(
        segmentWithMD5Checksum1.getCheckSumState(),
        segmentWithMD5Checksum2.getCheckSumState(),
        "Identical operations should result in identical checksums!");

    KafkaMessageEnvelope messageEnvelope4 = new KafkaMessageEnvelope();
    messageEnvelope4.setMessageType(MessageType.DELETE.getValue());
    Delete delete = new Delete();
    messageEnvelope4.setPayloadUnion(delete);
    segmentWithMD5Checksum1.addToCheckSum(putKey, messageEnvelope4);
    assertNotEquals(
        segmentWithMD5Checksum1.getCheckSumState(),
        segmentWithMD5Checksum2.getCheckSumState(),
        "Checksum state should have changed!");
    segmentWithMD5Checksum2.addToCheckSum(putKey, messageEnvelope4);
    assertEquals(
        segmentWithMD5Checksum1.getCheckSumState(),
        segmentWithMD5Checksum2.getCheckSumState(),
        "Identical operations should result in identical checksums!");

    KafkaMessageEnvelope messageEnvelope5 = new KafkaMessageEnvelope();
    messageEnvelope5.setMessageType(MessageType.CONTROL_MESSAGE.getValue());
    ControlMessage controlMessage2 = new ControlMessage();
    controlMessage2.setControlMessageType(ControlMessageType.END_OF_SEGMENT.getValue());
    messageEnvelope5.setPayloadUnion(controlMessage2);
    segmentWithMD5Checksum1.addToCheckSum(null, messageEnvelope5);
    assertEquals(
        segmentWithMD5Checksum1.getCheckSumState(),
        segmentWithMD5Checksum2.getCheckSumState(),
        "EOS should have been a no-op for the checksum state!");
    segmentWithMD5Checksum2.addToCheckSum(null, messageEnvelope5);
    assertEquals(
        segmentWithMD5Checksum1.getCheckSumState(),
        segmentWithMD5Checksum2.getCheckSumState(),
        "Identical operations should result in identical checksums!");

    KafkaMessageEnvelope messageEnvelope6 = new KafkaMessageEnvelope();
    messageEnvelope6.setMessageType(-1);
    assertThrows(VeniceMessageException.class, () -> segmentWithMD5Checksum1.addToCheckSum(null, messageEnvelope6));

    assertEquals(segmentWithMD5Checksum1.getFinalCheckSum(), segmentWithMD5Checksum2.getFinalCheckSum());
  }
}
