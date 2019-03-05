package com.linkedin.venice.client.store.streaming;

import com.linkedin.venice.compute.protocol.response.ComputeResponseRecordV1;
import com.linkedin.venice.serializer.AvroSerializer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ComputeResponseRecordV1ChunkedDeserializerTest {
  @Test
  public void testDeserialize() {
    ComputeResponseRecordV1 record1 = new ComputeResponseRecordV1();
    record1.keyIndex = 100000;
    record1.value = ByteBuffer.wrap("0123456789".getBytes());
    ComputeResponseRecordV1 record2 = new ComputeResponseRecordV1();
    record2.keyIndex = -100000;
    record2.value = ByteBuffer.wrap("abcdefghijklmn0123456789".getBytes());

    AvroSerializer<ComputeResponseRecordV1> serializer = new AvroSerializer<>(ComputeResponseRecordV1.SCHEMA$);
    List<ComputeResponseRecordV1> records = new ArrayList<>();
    records.add(record1);
    records.add(record2);
    byte[] serializedBytes = serializer.serializeObjects(records);

    ComputeResponseRecordV1ChunkedDeserializer chunkedDeserializer = new ComputeResponseRecordV1ChunkedDeserializer();
    chunkedDeserializer.write(ByteBuffer.wrap(serializedBytes, 0, 2));
    Assert.assertEquals(0, chunkedDeserializer.consume().size(), "No record should be available at this moment");
    chunkedDeserializer.write(ByteBuffer.wrap(serializedBytes, 2, 10));
    Assert.assertEquals(0, chunkedDeserializer.consume().size(), "No record should be available at this moment");
    chunkedDeserializer.write(ByteBuffer.wrap(serializedBytes, 12, 7));
    List<ComputeResponseRecordV1> availableRecords = chunkedDeserializer.consume();
    Assert.assertEquals(1, availableRecords.size());
    Assert.assertEquals(record1, availableRecords.get(0));
    Assert.assertEquals(0, chunkedDeserializer.consume().size(), "record could only be consumed once");
    chunkedDeserializer.write(ByteBuffer.wrap(serializedBytes, 19, 2));
    Assert.assertEquals(0, chunkedDeserializer.consume().size(), "No record should be available at this moment");
    chunkedDeserializer.write(ByteBuffer.wrap(serializedBytes, 21, 4));
    Assert.assertEquals(0, chunkedDeserializer.consume().size(), "No record should be available at this moment");
    chunkedDeserializer.write(ByteBuffer.wrap(serializedBytes, 25, 10));
    Assert.assertEquals(0, chunkedDeserializer.consume().size(), "No record should be available at this moment");
    chunkedDeserializer.write(ByteBuffer.wrap(serializedBytes, 35, 7));
    availableRecords = chunkedDeserializer.consume();
    Assert.assertEquals(1, availableRecords.size());
    Assert.assertEquals(record2, availableRecords.get(0));
  }
}
