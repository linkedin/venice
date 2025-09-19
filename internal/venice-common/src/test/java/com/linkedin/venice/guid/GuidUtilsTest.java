package com.linkedin.venice.guid;

import static com.linkedin.venice.ConfigKeys.PUSH_JOB_GUID_LEAST_SIGNIFICANT_BITS;
import static com.linkedin.venice.ConfigKeys.PUSH_JOB_GUID_MOST_SIGNIFICANT_BITS;
import static com.linkedin.venice.guid.GuidUtils.GUID_GENERATOR_IMPLEMENTATION;

import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.utils.ByteArray;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import org.apache.avro.util.Utf8;
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
  public void testStringToGuidConversionAndBack(ByteArray arrayUnderTest) {
    testCharSeqToGuidConversionAndBack(
        new String(arrayUnderTest.get(), GuidUtils.CHARSET),
        GuidUtils::getCharSequenceFromGuid);
  }

  @Test(dataProvider = "byteArrays")
  public void testGuidToStringConversionAndBack(ByteArray arrayUnderTest) {
    testGuidToCharSeqConversionAndBack(arrayUnderTest, GuidUtils::getCharSequenceFromGuid);
  }

  @Test(dataProvider = "byteArrays")
  public void testUtf8ToGuidConversionAndBack(ByteArray arrayUnderTest) {
    testCharSeqToGuidConversionAndBack(
        new Utf8(new String(arrayUnderTest.get(), GuidUtils.CHARSET)),
        GuidUtils::getUtf8FromGuid);
  }

  @Test(dataProvider = "byteArrays")
  public void testGuidToUtf8ConversionAndBack(ByteArray arrayUnderTest) {
    testGuidToCharSeqConversionAndBack(arrayUnderTest, GuidUtils::getUtf8FromGuid);
  }

  public void testCharSeqToGuidConversionAndBack(
      CharSequence originalCharSequence,
      Function<GUID, CharSequence> guidToCharSeq) {
    LOGGER.info("input: {}", originalCharSequence);
    GUID convertedGuid = GuidUtils.getGuidFromCharSequence(originalCharSequence);
    CharSequence resultingCharSequence = guidToCharSeq.apply(convertedGuid);
    Assert.assertEquals(
        resultingCharSequence,
        originalCharSequence,
        "A CharSequence converted into GUID and back should be the same as previously!");
  }

  public void testGuidToCharSeqConversionAndBack(ByteArray arrayUnderTest, Function<GUID, CharSequence> guidToCharSeq) {
    LOGGER.info("ByteArray: {}", arrayUnderTest);
    GUID originalGuid = new GUID();
    originalGuid.bytes(arrayUnderTest.get());
    CharSequence convertedCharSequence = guidToCharSeq.apply(originalGuid);
    GUID resultingGuid = GuidUtils.getGuidFromCharSequence(convertedCharSequence);
    Assert.assertEquals(
        resultingGuid,
        originalGuid,
        "A GUID converted into a CharSequence and back should be the same as previously!");
  }

  @Test
  public void testDeterministicGuidGenerator() {
    // When no job ID and compute task id is provided, a default value will be used
    Properties properties1 = new Properties();
    properties1.put(GUID_GENERATOR_IMPLEMENTATION, DeterministicGuidGenerator.class.getName());
    properties1.put(PUSH_JOB_GUID_MOST_SIGNIFICANT_BITS, "10");
    properties1.put(PUSH_JOB_GUID_LEAST_SIGNIFICANT_BITS, "1200");
    VeniceProperties props1 = new VeniceProperties(properties1);
    GUID guid1 = GuidUtils.getGUID(props1);
    Assert.assertEquals(
        guid1,
        GuidUtils.getGUID(props1),
        "Two different GUIDs generated with the same properties should be equal when using DeterministicGuidGenerator!");

    // When a job id is provided but no compute task id is provided, a default compute task id will be used
    Properties properties2 = new Properties();
    properties2.put(GUID_GENERATOR_IMPLEMENTATION, DeterministicGuidGenerator.class.getName());
    properties2.put(PUSH_JOB_GUID_MOST_SIGNIFICANT_BITS, "120");
    properties2.put(PUSH_JOB_GUID_LEAST_SIGNIFICANT_BITS, "170");
    VeniceProperties props2 = new VeniceProperties(properties2);
    GUID guid2 = GuidUtils.getGUID(props2);
    Assert.assertEquals(
        guid2,
        GuidUtils.getGUID(props2),
        "Two different GUIDs generated with the same properties should be equal when using DeterministicGuidGenerator!");

    // When job id and compute task id are provided, they will be used to generate the guid
    Properties properties3 = new Properties();
    properties3.put(GUID_GENERATOR_IMPLEMENTATION, DeterministicGuidGenerator.class.getName());
    properties3.put(PUSH_JOB_GUID_MOST_SIGNIFICANT_BITS, "3");
    properties3.put(PUSH_JOB_GUID_LEAST_SIGNIFICANT_BITS, "100");
    VeniceProperties props3 = new VeniceProperties(properties3);
    GUID guid3 = GuidUtils.getGUID(props3);
    Assert.assertEquals(
        guid3,
        GuidUtils.getGUID(props3),
        "Two different GUIDs generated with the same properties should be equal when using DeterministicGuidGenerator!");

    // Ensure GUIDs using different job id and task ids are different
    Assert.assertNotEquals(
        guid1,
        guid2,
        "GUIDs generated with different job ids should be different when using DeterministicGuidGenerator!");
    Assert.assertNotEquals(
        guid2,
        guid3,
        "GUIDs generated with different job ids should be different when using DeterministicGuidGenerator!");
    Assert.assertNotEquals(
        guid3,
        guid1,
        "GUIDs generated with different job ids should be different when using DeterministicGuidGenerator!");
  }

  @Test
  public void testRandomGuidGenerator() {
    // When no job ID and compute task id is provided, a default value will be used
    Properties properties1 = new Properties();
    VeniceProperties props1 = new VeniceProperties(properties1);
    GUID guid1 = GuidUtils.getGUID(props1);
    Assert.assertNotEquals(
        guid1,
        GuidUtils.getGUID(props1),
        "Two different GUIDs generated with the same properties should be equal when using DeterministicGuidGenerator!");

    // When a job id is provided but no compute task id is provided, a default compute task id will be used
    Properties properties2 = new Properties();
    VeniceProperties props2 = new VeniceProperties(properties2);
    GUID guid2 = GuidUtils.getGUID(props2);
    Assert.assertNotEquals(
        guid2,
        GuidUtils.getGUID(props2),
        "Two different GUIDs generated with the same properties should be equal when using DeterministicGuidGenerator!");

    // When job id and compute task id are provided, they will be used to generate the guid
    Properties properties3 = new Properties();
    VeniceProperties props3 = new VeniceProperties(properties3);
    GUID guid3 = GuidUtils.getGUID(props3);
    Assert.assertNotEquals(
        guid3,
        GuidUtils.getGUID(props3),
        "Two different GUIDs generated with the same properties should be equal when using DeterministicGuidGenerator!");

  }

}
