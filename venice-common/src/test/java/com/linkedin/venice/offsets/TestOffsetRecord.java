package com.linkedin.venice.offsets;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestOffsetRecord {
  @Test
  public void testToBytes() {
    OffsetRecord offsetRecord1 = new OffsetRecord(100);
    OffsetRecord offsetRecord2 = new OffsetRecord(offsetRecord1.toBytes());
    Assert.assertEquals(offsetRecord2, offsetRecord1);

    offsetRecord1 = new OffsetRecord(100);
    offsetRecord1.complete();
    offsetRecord2 = new OffsetRecord(offsetRecord1.toBytes());
    Assert.assertEquals(offsetRecord2, offsetRecord1);
  }
}
