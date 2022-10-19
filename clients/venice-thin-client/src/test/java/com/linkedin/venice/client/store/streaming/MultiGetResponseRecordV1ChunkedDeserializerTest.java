package com.linkedin.venice.client.store.streaming;

import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.serializer.AvroSerializer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MultiGetResponseRecordV1ChunkedDeserializerTest {
  @Test
  public void testDeserialize() {
    MultiGetResponseRecordV1 record1 = new MultiGetResponseRecordV1();
    record1.keyIndex = 100000;
    record1.value = ByteBuffer.wrap("0123456789".getBytes());
    record1.schemaId = 900232210;
    MultiGetResponseRecordV1 record2 = new MultiGetResponseRecordV1();
    record2.keyIndex = -100000;
    record2.value = ByteBuffer.wrap("abcdefghijklmn0123456789".getBytes());
    record2.schemaId = -900232210;

    AvroSerializer<MultiGetResponseRecordV1> serializer = new AvroSerializer<>(MultiGetResponseRecordV1.SCHEMA$);
    List<MultiGetResponseRecordV1> records = new ArrayList<>();
    records.add(record1);
    records.add(record2);
    byte[] serializedBytes = serializer.serializeObjects(records);

    MultiGetResponseRecordV1ChunkedDeserializer chunkedDeserializer = new MultiGetResponseRecordV1ChunkedDeserializer();
    chunkedDeserializer.write(ByteBuffer.wrap(serializedBytes, 0, 2));
    Assert.assertEquals(0, chunkedDeserializer.consume().size(), "No record should be available at this moment");
    chunkedDeserializer.write(ByteBuffer.wrap(serializedBytes, 2, 10));
    Assert.assertEquals(0, chunkedDeserializer.consume().size(), "No record should be available at this moment");
    chunkedDeserializer.write(ByteBuffer.wrap(serializedBytes, 12, 7));
    List<MultiGetResponseRecordV1> availableRecords = chunkedDeserializer.consume();
    Assert.assertEquals(1, availableRecords.size());
    Assert.assertEquals(record1, availableRecords.get(0));
    Assert.assertEquals(0, chunkedDeserializer.consume().size(), "record could only be consumed once");
    chunkedDeserializer.write(ByteBuffer.wrap(serializedBytes, 19, 2));
    Assert.assertEquals(0, chunkedDeserializer.consume().size(), "No record should be available at this moment");
    chunkedDeserializer.write(ByteBuffer.wrap(serializedBytes, 21, 4));
    Assert.assertEquals(0, chunkedDeserializer.consume().size(), "No record should be available at this moment");
    chunkedDeserializer.write(ByteBuffer.wrap(serializedBytes, 25, 16));
    Assert.assertEquals(0, chunkedDeserializer.consume().size(), "No record should be available at this moment");
    chunkedDeserializer.write(ByteBuffer.wrap(serializedBytes, 41, 11));
    availableRecords = chunkedDeserializer.consume();
    Assert.assertEquals(1, availableRecords.size());
    Assert.assertEquals(record2, availableRecords.get(0));
  }
}
