package com.linkedin.venice.meta;

import com.linkedin.venice.utils.TestUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by mwise on 5/9/16.
 */
public class TestVersion {
  private static ObjectMapper mapper = new ObjectMapper();
  @Test
  public void identifiesValidTopicNames(){
    String goodTopic = "my_very_good_store_v4";
    Assert.assertTrue(Version.topicIsValidStoreVersion(goodTopic),
        goodTopic + " should parse as a valid store-version topic");

    String badTopic = "__consumer_offsets";
    Assert.assertFalse(Version.topicIsValidStoreVersion(badTopic),
        badTopic + " must not parse as a valid store-version topic");
  }

  @Test
  public void serializes() throws IOException {
    String storeName = TestUtils.getUniqueString("store");
    int versionNumber = 17;
    Version version = new Version(storeName, versionNumber);
    String serialized = mapper.writeValueAsString(version);
    Assert.assertTrue(serialized.contains(storeName));
  }

  /**
   * This tests that the deserialization works with extra fields and with missing fields.  In other words it tests
   * that we can add fields to this object and still maintain cross version compatibility in our components
   * @throws IOException
   */
  @Test
  public void deserializeWithWrongFields() throws IOException {
    String oldSerialized = "{\"storeName\":\"store-1492637190910-78714331\",\"number\":17,\"createdTime\":1492637190912,\"status\":\"STARTED\"}";
    String extraFieldSerialized = "{\"storeName\":\"store-1492637190910-12345678\",\"number\":17,\"createdTime\":1492637190912,\"status\":\"STARTED\",\"extraField\":\"12345\"}";
    String missingFieldSerialized = "{\"storeName\":\"store-missing\",\"number\":17,\"createdTime\":1492637190912}";  //no status

    Version oldParsedVersion = mapper.readValue(oldSerialized, Version.class);
    Assert.assertEquals(oldParsedVersion.getStoreName(), "store-1492637190910-78714331");

    Version newParsedVersion = mapper.readValue(extraFieldSerialized, Version.class);
    Assert.assertEquals(newParsedVersion.getStoreName(), "store-1492637190910-12345678");

    Version legacyParsedVersion = mapper.readValue(missingFieldSerialized, Version.class);
    Assert.assertEquals(legacyParsedVersion.getStoreName(), "store-missing");

  }

}
