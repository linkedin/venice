package com.linkedin.alpini.base.hash;

import com.linkedin.alpini.base.misc.CollectionUtil;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class TestCRC32C {
  @DataProvider
  public Object[][] rfc3720() {
    return new Object[][] {
        new Object[] { "32 bytes of zeros",
            CollectionUtil.listOf(
                new byte[] { 0, 0, 0, 0 },
                new byte[] { 0, 0, 0, 0 },
                new byte[] { 0, 0, 0, 0 },
                new byte[] { 0, 0, 0, 0 },
                new byte[] { 0, 0, 0, 0 },
                new byte[] { 0, 0, 0, 0 },
                new byte[] { 0, 0, 0, 0 },
                new byte[] { 0, 0, 0, 0 }),
            (int) 0x8a9136aaL },

        new Object[] { "32 bytes of ones",
            CollectionUtil.listOf(
                new byte[] { -1, -1, -1, -1 },
                new byte[] { -1, -1, -1, -1 },
                new byte[] { -1, -1, -1, -1 },
                new byte[] { -1, -1, -1, -1 },
                new byte[] { -1, -1, -1, -1 },
                new byte[] { -1, -1, -1, -1 },
                new byte[] { -1, -1, -1, -1 },
                new byte[] { -1, -1, -1, -1 }),
            (int) 0x62a8ab43L },

        new Object[] { "32 bytes of incrementing 00..1f",
            CollectionUtil.listOf(
                new byte[] { 0, 1, 2, 3 },
                new byte[] { 4, 5, 6, 7 },
                new byte[] { 8, 9, 10, 11 },
                new byte[] { 12, 13, 14, 15 },
                new byte[] { 16, 17, 18, 19 },
                new byte[] { 20, 21, 22, 23 },
                new byte[] { 24, 25, 26, 27 },
                new byte[] { 28, 29, 30, 31 }),
            (int) 0x46dd794eL },

        new Object[] { "32 bytes of decrementing 1f..00",
            CollectionUtil.listOf(
                new byte[] { 31, 30, 29, 28 },
                new byte[] { 27, 26, 25, 24 },
                new byte[] { 23, 22, 21, 20 },
                new byte[] { 19, 18, 17, 16 },
                new byte[] { 15, 14, 13, 12 },
                new byte[] { 11, 10, 9, 8 },
                new byte[] { 7, 6, 5, 4 },
                new byte[] { 3, 2, 1, 0 }),
            (int) 0x113fdb5cL },

        new Object[] { "An iSCSI - SCSI Read (10) Command PDU",
            CollectionUtil.listOf(
                new byte[] { 1, (byte) 0xc0, 0, 0, 0, 0, 0, 0 },
                new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 },
                new byte[] { 0x14, 0, 0, 0, 0, 0, 4, 0 },
                new byte[] { 0, 0, 0, 0x14, 0, 0, 0, 0x18 },
                new byte[] { 0x28, 0, 0, 0, 0, 0, 0, 0 },
                new byte[] { 2, 0, 0, 0, 0, 0, 0, 0 }),
            (int) 0xd9963a56L }, };
  }

  @Test(groups = "unit", dataProvider = "rfc3720")
  public void testRFC3720Vectors(String name, List<byte[]> data, int expected) {
    Crc32C crc = new Crc32C();
    data.stream().forEach(crc::update);
    Assert.assertEquals(crc.getIntValue(), expected);
  }
}
