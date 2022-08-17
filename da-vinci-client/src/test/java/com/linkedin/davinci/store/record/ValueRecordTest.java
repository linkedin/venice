package com.linkedin.davinci.store.record;

import com.linkedin.venice.utils.ByteUtils;
import java.nio.ByteBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ValueRecordTest {
  @Test
  public void testSerialize() {
    int schemaId = 1;
    String testStr = "abc";
    byte[] data = testStr.getBytes();
    ValueRecord record = ValueRecord.create(schemaId, data);
    Assert.assertEquals(record.getSchemaId(), 1);
    Assert.assertEquals(record.getData().array(), data);
    byte[] serialized = record.serialize();
    Assert.assertEquals(serialized.length, 7);
    Assert.assertEquals(ByteUtils.readInt(serialized, 0), schemaId);
    Assert.assertEquals(new String(serialized, 4, 3), testStr);
    Assert.assertEquals(new String(record.getDataInBytes()), testStr);
  }

  @Test
  public void testDeserialize() {
    int schemaId = 1000;
    String testStr = "sdfja;sld203920201-pfse";
    byte[] data = testStr.getBytes();
    ByteBuffer bb = ByteBuffer.allocate(4 + data.length);
    bb.putInt(schemaId);
    bb.put(testStr.getBytes());
    ValueRecord record = ValueRecord.parseAndCreate(bb.array());

    Assert.assertEquals(record.getSchemaId(), schemaId);
    Assert.assertEquals(record.getData().capacity(), data.length);
    // compare byte by byte
    for (int cur = 0; cur < data.length; ++cur) {
      Assert.assertEquals(record.getData().getByte(cur), data[cur]);
    }

    Assert.assertEquals(new String(record.getDataInBytes()), testStr);
  }
}
