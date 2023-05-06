package com.linkedin.davinci.consumer;

import com.linkedin.venice.pubsub.adapter.kafka.ApacheKafkaOffsetPosition;
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
    PubSubPosition position = new ApacheKafkaOffsetPosition(TEST_OFFSET);
    VeniceChangeCoordinate veniceChangeCoordinate =
        new VeniceChangeCoordinate(TEST_STORE_TOPIC, position, TEST_PARTITION);

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

  }
}
