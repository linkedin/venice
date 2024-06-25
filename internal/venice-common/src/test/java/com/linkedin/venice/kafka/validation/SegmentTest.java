package com.linkedin.venice.kafka.validation;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
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
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.Utils;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;


public class SegmentTest {
  @Test(dataProvider = "CheckpointingSupported-CheckSum-Types", dataProviderClass = DataProviderUtils.class)
  public void test(CheckSumType checkSumType) {
    Segment segmentWithoutChecksum = new Segment(0, 0, CheckSumType.NONE);
    assertEquals(segmentWithoutChecksum.getCheckSumType(), CheckSumType.NONE);
    assertEquals(segmentWithoutChecksum.getCheckSumState(), new byte[0]);
    assertEquals(segmentWithoutChecksum.getFinalCheckSum(), new byte[0]);

    Segment segmentChecksum1 = new Segment(0, 0, checkSumType);
    assertEquals(segmentChecksum1.getCheckSumType(), checkSumType);
    assertTrue(segmentChecksum1.getCheckSumState().length > 0);

    Segment segmentChecksum2 = new Segment(0, 0, checkSumType);
    assertEquals(segmentChecksum2.getCheckSumType(), checkSumType);
    assertTrue(segmentChecksum2.getCheckSumState().length > 0);

    // Verify checksum determinism
    assertEquals(segmentChecksum1.getCheckSumState(), segmentChecksum2.getCheckSumState());
    KafkaMessageEnvelope messageEnvelope1 = new KafkaMessageEnvelope();
    messageEnvelope1.setMessageType(MessageType.CONTROL_MESSAGE.getValue());
    ControlMessage controlMessage1 = new ControlMessage();
    controlMessage1.setControlMessageType(ControlMessageType.START_OF_SEGMENT.getValue());
    messageEnvelope1.setPayloadUnion(controlMessage1);
    segmentChecksum1.addToCheckSum(null, messageEnvelope1);
    assertNotEquals(
        segmentChecksum1.getCheckSumState(),
        segmentChecksum2.getCheckSumState(),
        "Checksum state should have changed!");
    segmentChecksum2.addToCheckSum(null, messageEnvelope1);
    assertEquals(
        segmentChecksum1.getCheckSumState(),
        segmentChecksum2.getCheckSumState(),
        "Identical operations should result in identical checksums!");

    KafkaKey putKey = new KafkaKey(MessageType.PUT, new byte[] { 1, 2, 3 });
    KafkaMessageEnvelope messageEnvelope2 = new KafkaMessageEnvelope();
    messageEnvelope2.setMessageType(MessageType.PUT.getValue());
    Put put = new Put();
    put.setSchemaId(1);
    put.setPutValue(ByteBuffer.wrap(new byte[] { 1, 2, 3 }));
    messageEnvelope2.setPayloadUnion(put);
    segmentChecksum1.addToCheckSum(putKey, messageEnvelope2);
    assertNotEquals(
        segmentChecksum1.getCheckSumState(),
        segmentChecksum2.getCheckSumState(),
        "Checksum state should have changed!");
    segmentChecksum2.addToCheckSum(putKey, messageEnvelope2);
    assertEquals(
        segmentChecksum1.getCheckSumState(),
        segmentChecksum2.getCheckSumState(),
        "Identical operations should result in identical checksums!");

    KafkaMessageEnvelope messageEnvelope3 = new KafkaMessageEnvelope();
    messageEnvelope3.setMessageType(MessageType.UPDATE.getValue());
    Update update = new Update();
    update.setSchemaId(1);
    update.setUpdateSchemaId(1);
    update.setUpdateValue(ByteBuffer.wrap(new byte[] { 1, 2, 3 }));
    messageEnvelope3.setPayloadUnion(update);
    segmentChecksum1.addToCheckSum(putKey, messageEnvelope3);
    assertNotEquals(
        segmentChecksum1.getCheckSumState(),
        segmentChecksum2.getCheckSumState(),
        "Checksum state should have changed!");
    segmentChecksum2.addToCheckSum(putKey, messageEnvelope3);
    assertEquals(
        segmentChecksum1.getCheckSumState(),
        segmentChecksum2.getCheckSumState(),
        "Identical operations should result in identical checksums!");

    KafkaMessageEnvelope messageEnvelope4 = new KafkaMessageEnvelope();
    messageEnvelope4.setMessageType(MessageType.DELETE.getValue());
    Delete delete = new Delete();
    messageEnvelope4.setPayloadUnion(delete);
    segmentChecksum1.addToCheckSum(putKey, messageEnvelope4);
    assertNotEquals(
        segmentChecksum1.getCheckSumState(),
        segmentChecksum2.getCheckSumState(),
        "Checksum state should have changed!");
    segmentChecksum2.addToCheckSum(putKey, messageEnvelope4);
    assertEquals(
        segmentChecksum1.getCheckSumState(),
        segmentChecksum2.getCheckSumState(),
        "Identical operations should result in identical checksums!");

    KafkaMessageEnvelope messageEnvelope5 = new KafkaMessageEnvelope();
    messageEnvelope5.setMessageType(MessageType.CONTROL_MESSAGE.getValue());
    ControlMessage controlMessage2 = new ControlMessage();
    controlMessage2.setControlMessageType(ControlMessageType.END_OF_SEGMENT.getValue());
    messageEnvelope5.setPayloadUnion(controlMessage2);
    segmentChecksum1.addToCheckSum(null, messageEnvelope5);
    assertEquals(
        segmentChecksum1.getCheckSumState(),
        segmentChecksum2.getCheckSumState(),
        "EOS should have been a no-op for the checksum state!");
    segmentChecksum2.addToCheckSum(null, messageEnvelope5);
    assertEquals(
        segmentChecksum1.getCheckSumState(),
        segmentChecksum2.getCheckSumState(),
        "Identical operations should result in identical checksums!");

    KafkaMessageEnvelope messageEnvelope6 = new KafkaMessageEnvelope();
    messageEnvelope6.setMessageType(-1);
    assertThrows(VeniceMessageException.class, () -> segmentChecksum1.addToCheckSum(null, messageEnvelope6));

    assertEquals(segmentChecksum1.getFinalCheckSum(), segmentChecksum2.getFinalCheckSum());
  }

  @Test(dataProvider = "CheckpointingSupported-CheckSum-Types", dataProviderClass = DataProviderUtils.class)
  public void testDebugInfoDeduping(CheckSumType checkSumType) {
    Map<CharSequence, CharSequence> debugInfo1 = Utils.getDebugInfo();
    Map<CharSequence, CharSequence> debugInfo2 = Utils.getDebugInfo();
    assertEquals(
        debugInfo1,
        debugInfo2,
        "We should get equal debug info when calling Utils.getDebugInfo() multiple times.");

    Segment segment1 = new Segment(0, 0, 0, checkSumType, debugInfo1, Collections.emptyMap());
    Segment segment2 = new Segment(1, 0, 0, checkSumType, debugInfo2, Collections.emptyMap());
    assertEquals(
        segment1.getDebugInfo(),
        segment2.getDebugInfo(),
        "The debug info of the two segments should still be equal.");

    List<String> allProperties = Arrays.asList("host", "JDK major version", "path", "pid", "user", "version");
    Set<String> nonSingletonProperties = new HashSet<>(Arrays.asList("JDK major version", "path", "pid"));
    for (String property: allProperties) {
      CharSequence rawValue1 = debugInfo1.get(property);
      CharSequence rawValue2 = debugInfo2.get(property);
      if (nonSingletonProperties.contains(property)) {
        assertFalse(
            rawValue1 == rawValue2,
            "The identity of the elements inside the debug info map are not expected to be the same; property: "
                + property + ", rawValue1: " + rawValue1 + ", rawValue2: " + rawValue2);
      }
      assertEquals(
          rawValue1,
          rawValue2,
          "The content of the elements inside the debug info map are expected to be equal; property: " + property
              + ", rawValue1: " + rawValue1 + ", rawValue2: " + rawValue2);

      CharSequence dedupedValue1 = segment1.getDebugInfo().get(property);
      CharSequence dedupedValue2 = segment2.getDebugInfo().get(property);
      assertTrue(
          dedupedValue1 == dedupedValue2,
          "The identity of the elements inside the debug info maps should be deduped; property: " + property
              + ", dedupedValue1: " + dedupedValue1 + ", dedupedValue2: " + dedupedValue2);
    }
  }
}
