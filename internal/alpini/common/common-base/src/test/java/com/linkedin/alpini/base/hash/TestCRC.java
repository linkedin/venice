package com.linkedin.alpini.base.hash;

import java.nio.charset.StandardCharsets;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class TestCRC {
  @Test(groups = "unit")
  public void testSimple() {

    Crc32 crc = new Crc32();
    Assert.assertEquals(crc.getValue(), 0);
    crc.update("The quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.US_ASCII));
    Assert.assertEquals(crc.getValue(), 0x414fa339L);

    crc.reset();
    crc.update('T');
    crc.update("he quick brown fox jumps over the lazy do".getBytes(StandardCharsets.US_ASCII));
    crc.update('g');
    Assert.assertEquals(crc.getValue(), 0x414fa339L);

    crc.reset();
    crc.update("Test vector from febooti.com".getBytes(StandardCharsets.US_ASCII));
    Assert.assertEquals(crc.getValue(), 0x0c877f61L);
  }
}
