package com.linkedin.venice.meta;

import static org.testng.Assert.assertSame;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class CurrentVersionTypeTest {
  @DataProvider
  public Object[][] wireCodes() {
    return new Object[][] { { 0, CurrentVersionType.UNKNOWN }, { 1, CurrentVersionType.BATCH_ONLY },
        { 2, CurrentVersionType.HYBRID }, { -1, CurrentVersionType.UNKNOWN }, { 3, CurrentVersionType.UNKNOWN },
        { Integer.MIN_VALUE, CurrentVersionType.UNKNOWN }, { Integer.MAX_VALUE, CurrentVersionType.UNKNOWN } };
  }

  @Test(dataProvider = "wireCodes")
  public void testUnknownWireCodesBypassCaching(int wireCode, CurrentVersionType expected) {
    assertSame(CurrentVersionType.valueOf(wireCode), expected);
  }
}
