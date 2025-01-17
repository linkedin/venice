package com.linkedin.venice.meta;

import static com.linkedin.venice.meta.Version.VENICE_TTL_RE_PUSH_PUSH_ID_PREFIX;
import static org.testng.Assert.assertEquals;
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
    Assert.assertFalse(
        Version.isVersionTopicOrStreamReprocessingTopic(badTopic),
        badTopic + " must not parse as a valid store-version topic");
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
    assertEquals(Version.parseStoreFromRealTimeTopic(validRealTimeTopic), "abc");
    String invalidRealTimeTopic = "abc";
    try {
      Version.parseStoreFromRealTimeTopic(invalidRealTimeTopic);
      Assert.fail("VeniceException should be thrown for invalid real-time topic");
    } catch (VeniceException e) {

    }
  }

  @Test
  public void testIsTopic() {
    String topic = "abc_rt";
    Assert.assertFalse(Version.isVersionTopic(topic));
    assertTrue(Version.isRealTimeTopic(topic));
    topic = "abc";
    Assert.assertFalse(Version.isVersionTopic(topic));
    topic = "abc_v12df";
    Assert.assertFalse(Version.isVersionTopic(topic));
    topic = "abc_v123";
    assertTrue(Version.isVersionTopic(topic));
    Assert.assertFalse(Version.isRealTimeTopic(topic));
    assertTrue(Version.isVersionTopicOrStreamReprocessingTopic(topic));
    topic = "abc_v123_sr";
    Assert.assertFalse(Version.isVersionTopic(topic));
    assertTrue(Version.isStreamReprocessingTopic(topic));
    assertTrue(Version.isVersionTopicOrStreamReprocessingTopic(topic));
    topic = "abc_v12ab3_sr";
    Assert.assertFalse(Version.isVersionTopic(topic));
    Assert.assertFalse(Version.isStreamReprocessingTopic(topic));
    Assert.assertFalse(Version.isVersionTopicOrStreamReprocessingTopic(topic));
    topic = "abc_v_sr";
    Assert.assertFalse(Version.isVersionTopic(topic));
    Assert.assertFalse(Version.isStreamReprocessingTopic(topic));
    Assert.assertFalse(Version.isVersionTopicOrStreamReprocessingTopic(topic));
  }

  @Test
  public void testIsATopicThatIsVersioned() {
    String topic = "abc_rt";
    Assert.assertFalse(Version.isATopicThatIsVersioned(topic));
    topic = "abc_v1_sr";
    assertTrue(Version.isATopicThatIsVersioned(topic));
    topic = "abc_v1";
    assertTrue(Version.isATopicThatIsVersioned(topic));
    topic = "abc_v1_cc";
    assertTrue(Version.isATopicThatIsVersioned(topic));
    String pushId = VENICE_TTL_RE_PUSH_PUSH_ID_PREFIX + System.currentTimeMillis();
    assertTrue(Version.isPushIdTTLRePush(pushId));
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
  void testVersionStatus() {
    for (VersionStatus status: VersionStatus.values()) {
      if (status == VersionStatus.KILLED) {
        assertTrue(VersionStatus.isVersionKilled(status));
      } else {
        Assert.assertFalse(VersionStatus.isVersionKilled(status));
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
