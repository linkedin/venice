package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.Utils;

import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by mwise on 5/9/16.
 */
public class TestVersion {
  //TODO, converge on fasterxml or codehouse
  static com.fasterxml.jackson.databind.ObjectMapper fasterXmlMapper = new com.fasterxml.jackson.databind.ObjectMapper();
  static org.codehaus.jackson.map.ObjectMapper codehouseMapper = new org.codehaus.jackson.map.ObjectMapper();

  static final String oldSerialized = "{\"storeName\":\"store-1492637190910-78714331\",\"number\":17,\"createdTime\":1492637190912,\"status\":\"STARTED\"}";
  static final String extraFieldSerialized = "{\"storeName\":\"store-1492637190910-12345678\",\"number\":17,\"createdTime\":1492637190912,\"status\":\"STARTED\",\"extraField\":\"12345\"}";
  static final String missingFieldSerialized = "{\"storeName\":\"store-missing\",\"number\":17,\"createdTime\":1492637190912}";  //no status


  @Test
  public void identifiesValidTopicNames(){
    String goodTopic = "my_very_good_store_v4";
    Assert.assertTrue(Version.isVersionTopicOrStreamReprocessingTopic(goodTopic),
        goodTopic + " should parse as a valid store-version topic");

    String badTopic = "__consumer_offsets";
    Assert.assertFalse(Version.isVersionTopicOrStreamReprocessingTopic(badTopic),
        badTopic + " must not parse as a valid store-version topic");
  }

  @Test
  public void serializes() throws IOException {
    String storeName = Utils.getUniqueString("store");
    int versionNumber = 17;
    Version version = new VersionImpl(storeName, versionNumber);
    String serialized = codehouseMapper.writeValueAsString(version);
    Assert.assertTrue(serialized.contains(storeName));
  }

  /**
   * This tests that the deserialization works with extra fields and with missing fields.  In other words it tests
   * that we can add fields to this object and still maintain cross version compatibility in our components
   * @throws IOException
   */
  @Test
  public void deserializeWithWrongFieldsAndCodehouse() throws IOException {
    Version oldParsedVersion = codehouseMapper.readValue(oldSerialized, Version.class);
    Assert.assertEquals(oldParsedVersion.getStoreName(), "store-1492637190910-78714331");

    Version newParsedVersion = codehouseMapper.readValue(extraFieldSerialized, Version.class);
    Assert.assertEquals(newParsedVersion.getStoreName(), "store-1492637190910-12345678");

    Version legacyParsedVersion = codehouseMapper.readValue(missingFieldSerialized, Version.class);
    Assert.assertEquals(legacyParsedVersion.getStoreName(), "store-missing");
    Assert.assertNotNull(legacyParsedVersion.getPushJobId()); // missing final field can still deserialize, just gets arbitrary value from constructor
  }

  @Test
  public void deserializeWithWrongFieldsAndFasterXml() throws IOException {
    Version oldParsedVersion = fasterXmlMapper.readValue(oldSerialized, Version.class);
    Assert.assertEquals(oldParsedVersion.getStoreName(), "store-1492637190910-78714331");

    Version newParsedVersion = fasterXmlMapper.readValue(extraFieldSerialized, Version.class);
    Assert.assertEquals(newParsedVersion.getStoreName(), "store-1492637190910-12345678");

    Version legacyParsedVersion = fasterXmlMapper.readValue(missingFieldSerialized, Version.class);
    Assert.assertEquals(legacyParsedVersion.getStoreName(), "store-missing");
    Assert.assertNotNull(legacyParsedVersion.getPushJobId()); // missing final field can still deserialize, just gets arbitrary value from constructor
  }

  @Test
  public void testParseStoreFromRealTimeTopic() {
    String validRealTimeTopic = "abc_rt";
    Assert.assertEquals(Version.parseStoreFromRealTimeTopic(validRealTimeTopic), "abc");
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
    Assert.assertTrue(Version.isRealTimeTopic(topic));
    topic = "abc";
    Assert.assertFalse(Version.isVersionTopic(topic));
    topic = "abc_v12df";
    Assert.assertFalse(Version.isVersionTopic(topic));
    topic = "abc_v123";
    Assert.assertTrue(Version.isVersionTopic(topic));
    Assert.assertFalse(Version.isRealTimeTopic(topic));
    Assert.assertTrue(Version.isVersionTopicOrStreamReprocessingTopic(topic));
    topic = "abc_v123_sr";
    Assert.assertFalse(Version.isVersionTopic(topic));
    Assert.assertTrue(Version.isStreamReprocessingTopic(topic));
    Assert.assertTrue(Version.isVersionTopicOrStreamReprocessingTopic(topic));
    topic = "abc_v12ab3_sr";
    Assert.assertFalse(Version.isVersionTopic(topic));
    Assert.assertFalse(Version.isStreamReprocessingTopic(topic));
    Assert.assertFalse(Version.isVersionTopicOrStreamReprocessingTopic(topic));
    topic = "abc_v_sr";
    Assert.assertFalse(Version.isVersionTopic(topic));
    Assert.assertFalse(Version.isStreamReprocessingTopic(topic));
    Assert.assertFalse(Version.isVersionTopicOrStreamReprocessingTopic(topic));
  }
}
