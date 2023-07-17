package com.linkedin.venice.kafka.validation.checksum;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestCheckSum {
  @Test
  public void testCheckSum() {
    CheckSum checkSum = CheckSum.getInstance(CheckSumType.MD5);

    checkSum.update(1);

    // checksum will return the cached value even if "getCheckSum"
    // is called multiple times
    byte[] curCheckSumVal = checkSum.getCheckSum();
    Assert.assertTrue(Arrays.equals(checkSum.getCheckSum(), curCheckSumVal));

    // checksum will flip to the "read mode" and cannot accept any updates
    // when "getCheckSum" is called
    Assert.assertThrows(VeniceException.class, () -> checkSum.update(2));

    // checksum will flip back to the "write mode" when reset() is called.
    checkSum.reset();
    checkSum.update(3);
    Assert.assertFalse(Arrays.equals(curCheckSumVal, checkSum.getCheckSum()));
  }
}
