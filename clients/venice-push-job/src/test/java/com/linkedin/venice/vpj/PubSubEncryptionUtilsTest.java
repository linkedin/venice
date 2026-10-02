package com.linkedin.venice.vpj;

import static com.linkedin.venice.vpj.VenicePushJobConstants.PUB_SUB_ENCRYPTION_KEY_URN;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.Properties;
import java.util.function.Function;
import org.testng.annotations.Test;


public class PubSubEncryptionUtilsTest {
  private static final String KEY_URN = "urn:li:kmsKeyLineage:test-key";

  @Test
  public void testLookupIgnoresStoreNameArgument() {
    Function<String, String> lookup = PubSubEncryptionUtils.getKeyUrnLookup(KEY_URN);

    assertEquals(lookup.apply("store-a"), KEY_URN);
    assertEquals(lookup.apply("store-b"), KEY_URN);
    assertEquals(lookup.apply(null), KEY_URN);
  }

  @Test
  public void testBlankUrnYieldsNullLookup() {
    assertNull(PubSubEncryptionUtils.getKeyUrnLookup(""));
    assertNull(PubSubEncryptionUtils.getKeyUrnLookup("   "));
  }

  @Test
  public void testUrnIsTrimmed() {
    assertEquals(PubSubEncryptionUtils.getKeyUrnLookup("  " + KEY_URN + "  ").apply("my-store"), KEY_URN);
  }

  @Test
  public void testPropertiesOverloadReadsThreadedUrn() {
    Properties properties = new Properties();
    properties.setProperty(PUB_SUB_ENCRYPTION_KEY_URN, KEY_URN);

    assertEquals(PubSubEncryptionUtils.getKeyUrnLookup(properties).apply("my-store"), KEY_URN);
  }

  @Test
  public void testPropertiesOverloadWithoutUrnYieldsNullLookup() {
    assertNull(PubSubEncryptionUtils.getKeyUrnLookup(new Properties()));
  }
}
