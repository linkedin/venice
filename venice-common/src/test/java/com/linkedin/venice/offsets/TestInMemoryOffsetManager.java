package com.linkedin.venice.offsets;

import com.linkedin.venice.utils.TestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestInMemoryOffsetManager {

  @Test
  public void canSaveOffsets(){
    String topic = TestUtils.getUniqueString("topic");

    OffsetManager om = new InMemoryOffsetManager();
    OffsetRecord record = new OffsetRecord();
    record.setOffset(1234);
    OffsetRecord oldRecord = new OffsetRecord();
    oldRecord.setOffset(234);
    OffsetRecord newRecord = new OffsetRecord();
    newRecord.setOffset(11234);

    om.put(topic, 0, record);
    Assert.assertEquals(om.getLastOffset(topic, 0), record);

    om.put(topic, 0, oldRecord);
    Assert.assertEquals(om.getLastOffset(topic, 0), record);

    om.put(topic, 0, newRecord);
    Assert.assertEquals(om.getLastOffset(topic, 0), newRecord);

    om.clearOffset(topic, 1);
    Assert.assertEquals(om.getLastOffset(topic, 0), newRecord);

    om.clearOffset(topic, 0);
    Assert.assertNotEquals(om.getLastOffset(topic, 0), newRecord);
    Assert.assertNotNull(om.getLastOffset(topic, 0));

  }
}
