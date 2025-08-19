package com.linkedin.davinci.consumer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVeniceChangeCoordinate {
  static final long TEST_OFFSET = 1000L;
  static final String TEST_STORE_NAME = "datastax_test_store";
  static final String TEST_STORE_TOPIC = TEST_STORE_NAME + "_v1_cc";

  static final Integer TEST_PARTITION = 1337;

  @Test
  public void testReadAndWriteExternal() throws IOException, ClassNotFoundException {
    PubSubPosition position = ApacheKafkaOffsetPosition.of(TEST_OFFSET);
    long sequenceId = 123L;
    VeniceChangeCoordinate veniceChangeCoordinate =
        new VeniceChangeCoordinate(TEST_STORE_TOPIC, position, TEST_PARTITION, sequenceId);

    ByteArrayOutputStream inMemoryOutputStream = new ByteArrayOutputStream();
    ObjectOutputStream objectOutputStream = new ObjectOutputStream(inMemoryOutputStream);
    veniceChangeCoordinate.writeExternal(objectOutputStream);

    objectOutputStream.flush();
    objectOutputStream.close();

    byte[] data = inMemoryOutputStream.toByteArray();
    ByteArrayInputStream inMemoryInputStream = new ByteArrayInputStream(data);
    ObjectInputStream objectInputStream = new ObjectInputStream(inMemoryInputStream);
    VeniceChangeCoordinate restoredCoordinate = new VeniceChangeCoordinate();
    restoredCoordinate.readExternal(objectInputStream);

    Assert.assertEquals(restoredCoordinate.getStoreName(), TEST_STORE_NAME);
    Assert.assertEquals(restoredCoordinate.getPartition(), TEST_PARTITION);
    Assert.assertEquals(restoredCoordinate.getPosition(), position);
    Assert.assertEquals(restoredCoordinate.getConsumerSequenceId(), sequenceId);
    Assert.assertEquals(
        restoredCoordinate,
        veniceChangeCoordinate,
        "Restored VeniceChangeCoordinate should be equal to the original one");
    Assert.assertEquals(restoredCoordinate.hashCode(), veniceChangeCoordinate.hashCode());
  }

  @Test
  public void testReadExternalBackwardsCompatible() throws IOException, ClassNotFoundException {
    PubSubPosition position = ApacheKafkaOffsetPosition.of(TEST_OFFSET);
    long sequenceId = 123L;
    VeniceChangeCoordinate veniceChangeCoordinate =
        new VeniceChangeCoordinate(TEST_STORE_TOPIC, position, TEST_PARTITION, sequenceId);
    ByteArrayOutputStream inMemoryOutputStream = new ByteArrayOutputStream();
    ObjectOutputStream objectOutputStream = new ObjectOutputStream(inMemoryOutputStream);
    // Manually write an output stream without the consumer id field
    objectOutputStream.writeUTF(veniceChangeCoordinate.getTopic());
    objectOutputStream.writeInt(veniceChangeCoordinate.getPartition());
    objectOutputStream.writeObject(veniceChangeCoordinate.getPosition().getPositionWireFormat());
    objectOutputStream.flush();
    objectOutputStream.close();

    byte[] data = inMemoryOutputStream.toByteArray();
    ByteArrayInputStream inMemoryInputStream = new ByteArrayInputStream(data);
    ObjectInputStream objectInputStream = new ObjectInputStream(inMemoryInputStream);
    VeniceChangeCoordinate restoredCoordinate = new VeniceChangeCoordinate();
    restoredCoordinate.readExternal(objectInputStream);

    Assert.assertEquals(restoredCoordinate.getStoreName(), TEST_STORE_NAME);
    Assert.assertEquals(restoredCoordinate.getPartition(), TEST_PARTITION);
    Assert.assertEquals(restoredCoordinate.getPosition(), position);
    Assert.assertEquals(
        restoredCoordinate.getConsumerSequenceId(),
        VeniceChangeCoordinate.UNDEFINED_CONSUMER_SEQUENCE_ID);
  }

  @Test
  public void testComparePosition() {
    VeniceChangeCoordinate veniceChangeCoordinate =
        new VeniceChangeCoordinate(TEST_STORE_TOPIC, ApacheKafkaOffsetPosition.of(TEST_OFFSET), TEST_PARTITION);
    VeniceChangeCoordinate veniceChangeCoordinate_1 =
        new VeniceChangeCoordinate(TEST_STORE_TOPIC, ApacheKafkaOffsetPosition.of(TEST_OFFSET), TEST_PARTITION);
    Assert.assertEquals(veniceChangeCoordinate.comparePosition(veniceChangeCoordinate_1), 0);

    VeniceChangeCoordinate veniceChangeCoordinate_2 =
        new VeniceChangeCoordinate(TEST_STORE_TOPIC, ApacheKafkaOffsetPosition.of(TEST_OFFSET), TEST_PARTITION + 1);
    Assert.assertThrows(VeniceException.class, () -> veniceChangeCoordinate.comparePosition(veniceChangeCoordinate_2));

    VeniceChangeCoordinate veniceChangeCoordinate_3 =
        new VeniceChangeCoordinate(TEST_STORE_TOPIC, ApacheKafkaOffsetPosition.of(TEST_OFFSET - 1), TEST_PARTITION);
    Assert.assertTrue(veniceChangeCoordinate.comparePosition(veniceChangeCoordinate_3) > 0);

    VeniceChangeCoordinate veniceChangeCoordinate_4 =
        new VeniceChangeCoordinate(TEST_STORE_TOPIC, ApacheKafkaOffsetPosition.of(TEST_OFFSET + 1), TEST_PARTITION);
    Assert.assertTrue(veniceChangeCoordinate.comparePosition(veniceChangeCoordinate_4) < 0);

    VeniceChangeCoordinate veniceChangeCoordinate_5 =
        new VeniceChangeCoordinate(TEST_STORE_TOPIC + "v2", ApacheKafkaOffsetPosition.of(TEST_OFFSET), TEST_PARTITION);
    Assert.assertTrue(veniceChangeCoordinate.comparePosition(veniceChangeCoordinate_5) < 0);
  }

  @Test
  public void testComparePositionWithDifferentConsumerSequenceId() {
    VeniceChangeCoordinate veniceChangeCoordinate =
        new VeniceChangeCoordinate(TEST_STORE_TOPIC, ApacheKafkaOffsetPosition.of(TEST_OFFSET), TEST_PARTITION, 1L);
    VeniceChangeCoordinate veniceChangeCoordinate_1 =
        new VeniceChangeCoordinate(TEST_STORE_TOPIC, ApacheKafkaOffsetPosition.of(TEST_OFFSET), TEST_PARTITION, 2L);
    Assert.assertTrue(veniceChangeCoordinate.comparePosition(veniceChangeCoordinate_1) < 0);
    // Should fall back to use topic name and position if one of the change coordinate doesn't have consumer sequence id
    VeniceChangeCoordinate veniceChangeCoordinate_2 =
        new VeniceChangeCoordinate(TEST_STORE_TOPIC, ApacheKafkaOffsetPosition.of(TEST_OFFSET), TEST_PARTITION);
    Assert.assertEquals(veniceChangeCoordinate.comparePosition(veniceChangeCoordinate_2), 0);
  }
}
