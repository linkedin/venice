package com.linkedin.venice.guid;

import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.utils.ByteArray;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 */
public class GuidUtilsTest {
  private static final Logger LOGGER = LogManager.getLogger(GuidUtilsTest.class);

  private static ByteArray[] getByteArray(Function<Integer, Integer> valueGenerator) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(GuidUtils.GUID_SIZE_IN_BYTES);
    for (int i = 0; i < GuidUtils.GUID_SIZE_IN_BYTES; i++) {
      byteBuffer.put((byte) valueGenerator.apply(i).intValue());
    }
    return new ByteArray[] { new ByteArray(byteBuffer.array()) };
  }

  @DataProvider(name = "byteArrays")
  public static Object[][] byteArrays() {
    List<ByteArray[]> returnList = new ArrayList<>();

    returnList.add(getByteArray(index -> index));
    returnList.add(getByteArray(index -> 255 - index));
    returnList.add(getByteArray(index -> Byte.MAX_VALUE + index));
    returnList.add(getByteArray(index -> Byte.MAX_VALUE - index));
    returnList.add(getByteArray(index -> Byte.MIN_VALUE + index));
    returnList.add(getByteArray(index -> -255 + index));

    ByteArray[][] byteArraysToReturn = new ByteArray[returnList.size()][1];

    return returnList.toArray(byteArraysToReturn);
  }

  @Test(dataProvider = "byteArrays")
  public void testCharSeqToGuidConversionAndBack(ByteArray arrayUnderTest) {
    LOGGER.info("array: {}", arrayUnderTest);
    CharSequence originalCharSequence = new String(arrayUnderTest.get(), GuidUtils.CHARSET);
    GUID convertedGuid = GuidUtils.getGuidFromCharSequence(originalCharSequence);
    CharSequence resultingCharSequence = GuidUtils.getCharSequenceFromGuid(convertedGuid);
    Assert.assertEquals(
        resultingCharSequence,
        originalCharSequence,
        "A CharSequence converted into GUID and back should be the same as previously!");
  }

  @Test(dataProvider = "byteArrays")
  public void testGuidToCharSeqConversionAndBack(ByteArray arrayUnderTest) {
    LOGGER.info("ByteArray: {}", arrayUnderTest);
    GUID originalGuid = new GUID();
    originalGuid.bytes(arrayUnderTest.get());
    CharSequence convertedCharSequence = GuidUtils.getCharSequenceFromGuid(originalGuid);
    GUID resultingGuid = GuidUtils.getGuidFromCharSequence(convertedCharSequence);
    Assert.assertEquals(
        resultingGuid,
        originalGuid,
        "A GUID converted into a CharSequence and back should be the same as previously!");
  }

}
