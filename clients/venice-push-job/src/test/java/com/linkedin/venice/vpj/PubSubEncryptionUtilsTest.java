package com.linkedin.venice.vpj;

import static com.linkedin.venice.vpj.VenicePushJobConstants.PUB_SUB_ENCRYPTION_KEY_URN;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import java.util.function.Function;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
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

  @DataProvider(name = "encryptionConfigurations")
  public static Object[][] encryptionConfigurations() {
    return new Object[][] { { null, null, false }, { null, KEY_URN, false }, { false, null, false },
        { false, KEY_URN, false }, { true, "  " + KEY_URN + "  ", false }, { true, null, true }, { true, "", true },
        { true, "   ", true } };
  }

  @DataProvider(name = "encryptionEnabled")
  public static Object[][] encryptionEnabled() {
    return new Object[][] { { false }, { true } };
  }

  @DataProvider(name = "requiredKeyUrns")
  public Object[][] requiredKeyUrns() {
    return new Object[][] { { null, true }, { "", true }, { "   ", true }, { "\u2003", true },
        { "  " + KEY_URN + "  ", false } };
  }

  @Test(dataProvider = "requiredKeyUrns")
  public void testRequiredLookupRejectsMissingOrBlankUrn(String keyUrn, boolean invalid) {
    Properties props = new Properties();
    if (keyUrn != null) {
      props.setProperty(PUB_SUB_ENCRYPTION_KEY_URN, keyUrn);
    }
    if (invalid) {
      VeniceException error = Assert.expectThrows(
          VeniceException.class,
          () -> PubSubEncryptionUtils.getRequiredKeyUrnLookup(new VeniceProperties(props)));
      Assert.assertTrue(error.getMessage().contains(PUB_SUB_ENCRYPTION_KEY_URN));
    } else {
      assertEquals(
          PubSubEncryptionUtils.getRequiredKeyUrnLookup(new VeniceProperties(props)).apply("store"),
          keyUrn.trim());
    }
  }
}
