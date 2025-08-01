package com.linkedin.venice.meta;

import static com.linkedin.venice.meta.Version.VENICE_TTL_RE_PUSH_PUSH_ID_PREFIX;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version.PushType;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by mwise on 5/9/16.
 */
public class TestVersion {
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

  private static final String OLD_SERIALIZED =
      "{\"storeName\":\"store-1492637190910-78714331\",\"number\":17,\"createdTime\":1492637190912,\"status\":\"STARTED\"}";
  private static final String EXTRA_FIELD_SERIALIZED =
      "{\"storeName\":\"store-1492637190910-12345678\",\"number\":17,\"createdTime\":1492637190912,\"status\":\"STARTED\",\"extraField\":\"12345\"}";
  private static final String MISSING_FIELD_SERIALIZED =
      "{\"storeName\":\"store-missing\",\"number\":17,\"createdTime\":1492637190912}"; // no status

  @Test
  public void identifiesValidTopicNames() {
    String goodTopic = "my_very_good_store_v4";
    assertTrue(
        Version.isVersionTopicOrStreamReprocessingTopic(goodTopic),
        goodTopic + " should parse as a valid store-version topic");

    String badTopic = "__consumer_offsets";
    assertFalse(
        Version.isVersionTopicOrStreamReprocessingTopic(badTopic),
        badTopic + " must not parse as a valid store-version topic");

    String badTopic2 = "myStore_v1_rt";
    assertFalse(
        Version.isVersionTopicOrStreamReprocessingTopic(badTopic2),
        badTopic2 + " must not parse as a valid store-version topic");

    String badTopic3 = "myStore_v1_rt_sep";
    assertFalse(
        Version.isVersionTopicOrStreamReprocessingTopic(badTopic3),
        badTopic3 + " must not parse as a valid store-version topic");
  }

  @Test
  public void serializes() throws IOException {
    String storeName = Utils.getUniqueString("store");
    int versionNumber = 17;
    Version version = new VersionImpl(storeName, versionNumber);
    String serialized = OBJECT_MAPPER.writeValueAsString(version);
    assertTrue(serialized.contains(storeName));
  }

  /**
   * This tests that the deserialization works with extra fields and with missing fields.  In other words it tests
   * that we can add fields to this object and still maintain cross version compatibility in our components
   * @throws IOException
   */
  @Test
  public void deserializeWithWrongFields() throws IOException {
    Version oldParsedVersion = OBJECT_MAPPER.readValue(OLD_SERIALIZED, Version.class);
    assertEquals(oldParsedVersion.getStoreName(), "store-1492637190910-78714331");

    Version newParsedVersion = OBJECT_MAPPER.readValue(EXTRA_FIELD_SERIALIZED, Version.class);
    assertEquals(newParsedVersion.getStoreName(), "store-1492637190910-12345678");

    Version legacyParsedVersion = OBJECT_MAPPER.readValue(MISSING_FIELD_SERIALIZED, Version.class);
    assertEquals(legacyParsedVersion.getStoreName(), "store-missing");
    assertNotNull(legacyParsedVersion.getPushJobId()); // missing final field can still deserialize, just gets
                                                       // arbitrary value from constructor
  }

  @Test
  public void testParseStoreFromRealTimeTopic() {
    String validRealTimeTopic = "abc_rt";
    String validSeparateRealTimeTopic = Utils.getSeparateRealTimeTopicName(validRealTimeTopic);
    assertEquals(Version.parseStoreFromRealTimeTopic(validRealTimeTopic), "abc");
    assertEquals(Version.parseStoreFromRealTimeTopic(validSeparateRealTimeTopic), "abc");

    String validRealTimeTopic2 = Utils.composeRealTimeTopic("abc", 1);
    String validSeparateRealTimeTopic2 = Utils.getSeparateRealTimeTopicName(validRealTimeTopic2);
    assertEquals(Version.parseStoreFromRealTimeTopic(validRealTimeTopic2), "abc");
    assertEquals(Version.parseStoreFromRealTimeTopic(validSeparateRealTimeTopic2), "abc");

    String validRealTimeTopic3 = Utils.composeRealTimeTopic("abc_v", 1);
    String validSeparateRealTimeTopic3 = Utils.getSeparateRealTimeTopicName(validRealTimeTopic3);
    assertEquals(Version.parseStoreFromRealTimeTopic(validRealTimeTopic3), "abc_v");
    assertEquals(Version.parseStoreFromRealTimeTopic(validSeparateRealTimeTopic3), "abc_v");

    String validRealTimeTopic4 = Utils.composeRealTimeTopic("abc_v1", 1);
    String validSeparateRealTimeTopic4 = Utils.getSeparateRealTimeTopicName(validRealTimeTopic4);
    assertEquals(Version.parseStoreFromRealTimeTopic(validRealTimeTopic4), "abc_v1");
    assertEquals(Version.parseStoreFromRealTimeTopic(validSeparateRealTimeTopic4), "abc_v1");

    String invalidRealTimeTopic = "abc";
    try {
      Version.parseStoreFromRealTimeTopic(invalidRealTimeTopic);
      Assert.fail("VeniceException should be thrown for invalid real-time topic");
    } catch (VeniceException e) {

    }
    String invalidSeparateRealTimeTopic = Utils.getSeparateRealTimeTopicName(invalidRealTimeTopic);
    try {
      Version.parseStoreFromRealTimeTopic(invalidSeparateRealTimeTopic);
      Assert.fail("VeniceException should be thrown for invalid real-time topic");
    } catch (VeniceException e) {

    }

    String invalidRealTimeTopic2 = "_v1_rt";
    try {
      Version.parseStoreFromRealTimeTopic(invalidRealTimeTopic2);
      Assert.fail("VeniceException should be thrown for invalid real-time topic");
    } catch (VeniceException e) {

    }

    String invalidSeparateRealTimeTopic2 = Utils.getSeparateRealTimeTopicName("_v1_rt");
    try {
      Version.parseStoreFromRealTimeTopic(invalidSeparateRealTimeTopic2);
      Assert.fail("VeniceException should be thrown for invalid real-time topic");
    } catch (VeniceException e) {

    }
  }

  private void verifyTopic(
      String topic,
      boolean isVT,
      boolean isRT,
      boolean isSR,
      boolean isVTorSR,
      boolean isVersioned,
      boolean isSeperateTopic) {
    assert (Version.isVersionTopic(topic) == isVT);
    assert (Version.isRealTimeTopic(topic) == isRT);
    assert (Version.isStreamReprocessingTopic(topic) == isSR);
    assert (Version.isVersionTopicOrStreamReprocessingTopic(topic) == isVTorSR);
    assert (Version.isATopicThatIsVersioned(topic) == isVersioned);
    assert (Version.isIncrementalPushTopic(topic) == isSeperateTopic);
  }

  @Test
  public void testIsTopic() {
    verifyTopic("abc_rt", false, true, false, false, false, false);
    verifyTopic("abc", false, false, false, false, false, false);
    verifyTopic("abc_v12df", false, false, false, false, false, false);
    verifyTopic("abc_v123", true, false, false, true, true, false);
    verifyTopic("abc_v123_sr", false, false, true, true, true, false);
    verifyTopic("abc_v12ab3_sr", false, false, false, false, false, false);
    verifyTopic("abc_v_sr", false, false, false, false, false, false);
    verifyTopic("abc_v1", true, false, false, true, true, false);
    verifyTopic("abc_v1_sr", false, false, true, true, true, false);
    verifyTopic("abc_v1_cc", false, false, false, false, true, false);
    verifyTopic("abc_mv", false, false, false, false, true, false);
    verifyTopic("abc_rt_v1", false, true, false, false, false, false);
    verifyTopic("abc_rt_v1_sep", false, true, false, false, false, true);
  }

  @Test
  public void testPushId() {
    String pushId = VENICE_TTL_RE_PUSH_PUSH_ID_PREFIX + System.currentTimeMillis();
    assertTrue(Version.isPushIdTTLRePush(pushId));
    String regularPushWithRePushId =
        Version.generateRegularPushWithTTLRePushId(Long.toString(System.currentTimeMillis()));
    assertTrue(Version.isPushIdRegularPushWithTTLRePush(regularPushWithRePushId));
    assertFalse(Version.isPushIdRegularPushWithTTLRePush(pushId));
  }

  @Test
  public void testRemoveRTVersionSuffix() {
    String topic = "store_rt_v1";
    Assert.assertEquals(Version.removeRTVersionSuffix(topic), "store_rt");

    topic = "store_rt";
    Assert.assertEquals(Version.removeRTVersionSuffix(topic), "store_rt");
  }

  @Test
  public void testParseStoreFromKafkaTopicName() {
    String storeName = "abc";
    String topic = "abc_rt";
    assertEquals(Version.parseStoreFromKafkaTopicName(topic), storeName);
    topic = "abc_v1";
    assertEquals(Version.parseStoreFromKafkaTopicName(topic), storeName);
    topic = "abc_v1_cc";
    assertEquals(Version.parseStoreFromKafkaTopicName(topic), storeName);
  }

  @Test
  public void testParseVersionFromKafkaTopicName() {
    int version = 1;
    String topic = "abc_v1";
    assertEquals(Version.parseVersionFromVersionTopicName(topic), version);
    topic = "abc_v1_cc";
    assertEquals(Version.parseVersionFromKafkaTopicName(topic), version);
  }

  @Test
  public void testParseVersionFromVersionTopicPartition() {
    int version = 1;
    String topic = "abc_v1-0";
    assertEquals(Version.parseVersionFromVersionTopicPartition(topic), version);
    topic = "abc_v1-0_cc";
    assertEquals(Version.parseVersionFromVersionTopicPartition(topic), version);
  }

  @Test
  void testVersionStatus() {
    for (VersionStatus status: VersionStatus.values()) {
      if (status == VersionStatus.KILLED) {
        assertTrue(VersionStatus.isVersionKilled(status));
      } else {
        assertFalse(VersionStatus.isVersionKilled(status));
      }
    }
  }

  @Test
  public void testExtractPushType() {
    // Case 1: Valid push types
    assertEquals(PushType.extractPushType("BATCH"), PushType.BATCH);
    assertEquals(PushType.extractPushType("STREAM_REPROCESSING"), PushType.STREAM_REPROCESSING);
    assertEquals(PushType.extractPushType("STREAM"), PushType.STREAM);
    assertEquals(PushType.extractPushType("INCREMENTAL"), PushType.INCREMENTAL);

    // Case 2: Invalid push type
    String invalidType = "INVALID_TYPE";
    IllegalArgumentException invalidException =
        expectThrows(IllegalArgumentException.class, () -> PushType.extractPushType(invalidType));
    assertTrue(invalidException.getMessage().contains(invalidType));
    assertTrue(invalidException.getMessage().contains("Valid push types are"));

    // Case 3: Case sensitivity
    String lowerCaseType = "batch";
    IllegalArgumentException caseException =
        expectThrows(IllegalArgumentException.class, () -> PushType.extractPushType(lowerCaseType));
    assertTrue(caseException.getMessage().contains(lowerCaseType));

    // Case 4: Empty string
    String emptyInput = "";
    IllegalArgumentException emptyException =
        expectThrows(IllegalArgumentException.class, () -> PushType.extractPushType(emptyInput));
    assertTrue(emptyException.getMessage().contains(emptyInput));

    // Case 5: Null input
    IllegalArgumentException exception =
        expectThrows(IllegalArgumentException.class, () -> PushType.extractPushType(null));
    assertNotNull(exception);
  }

  @Test
  public void testValueOfIntReturnsPushType() {
    // Case 1: Valid integer values
    assertEquals(PushType.valueOf(0), PushType.BATCH);
    assertEquals(PushType.valueOf(1), PushType.STREAM_REPROCESSING);
    assertEquals(PushType.valueOf(2), PushType.STREAM);
    assertEquals(PushType.valueOf(3), PushType.INCREMENTAL);

    // Case 2: Invalid integer value (negative)
    int invalidNegative = -1;
    VeniceException negativeException = expectThrows(VeniceException.class, () -> PushType.valueOf(invalidNegative));
    assertTrue(negativeException.getMessage().contains("Invalid push type with int value: " + invalidNegative));

    // Case 3: Invalid integer value (positive out of range)
    int invalidPositive = 999;
    VeniceException positiveException = expectThrows(VeniceException.class, () -> PushType.valueOf(invalidPositive));
    assertTrue(positiveException.getMessage().contains("Invalid push type with int value: " + invalidPositive));

    // Case 4: Edge case - Valid minimum value
    assertEquals(PushType.valueOf(0), PushType.BATCH);

    // Case 5: Edge case - Valid maximum value
    assertEquals(PushType.valueOf(3), PushType.INCREMENTAL);
  }
}
