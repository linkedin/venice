package com.linkedin.alpini.netty4.misc;

import io.netty.util.AsciiString;
import java.nio.charset.StandardCharsets;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class TestAsciiStringURLCodec {
  static final int SWISS_GERMAN_STUFF_UNICODE[] = { 0x47, 0x72, 0xFC, 0x65, 0x7A, 0x69, 0x5F, 0x7A, 0xE4, 0x6D, 0xE4 };

  static final int RUSSIAN_STUFF_UNICODE[] =
      { 0x412, 0x441, 0x435, 0x43C, 0x5F, 0x43F, 0x440, 0x438, 0x432, 0x435, 0x442 };

  private static String constructString(int[] unicodeChars) {
    StringBuilder buffer = new StringBuilder();
    if (unicodeChars != null) {
      for (int unicodeChar: unicodeChars) {
        buffer.append((char) unicodeChar);
      }
    }
    return buffer.toString();
  }

  @Test(groups = "unit")
  public void testUTF8RoundTrip() throws Exception {
    String ru_msg = constructString(RUSSIAN_STUFF_UNICODE);
    String ch_msg = constructString(SWISS_GERMAN_STUFF_UNICODE);

    Assert.assertEquals(
        AsciiStringURLCodec.encode(ru_msg, StandardCharsets.UTF_8),
        new AsciiString("%D0%92%D1%81%D0%B5%D0%BC_%D0%BF%D1%80%D0%B8%D0%B2%D0%B5%D1%82"));

    Assert.assertEquals(
        AsciiStringURLCodec.encode(ch_msg, StandardCharsets.UTF_8),
        new AsciiString("Gr%C3%BCezi_z%C3%A4m%C3%A4"));

    Assert.assertEquals(
        AsciiStringURLCodec.decode(AsciiStringURLCodec.encode(ru_msg, StandardCharsets.UTF_8), StandardCharsets.UTF_8)
            .toString(),
        ru_msg);

    Assert.assertEquals(
        AsciiStringURLCodec.decode(AsciiStringURLCodec.encode(ch_msg, StandardCharsets.UTF_8), StandardCharsets.UTF_8)
            .toString(),
        ch_msg);
  }

  @Test(groups = "unit")
  public void testBasicEncodeDecode() throws Exception {
    String plain = "Hello there!";
    AsciiString encoded = AsciiStringURLCodec.encode(plain, StandardCharsets.US_ASCII);
    Assert.assertTrue(AsciiString.contentEquals(encoded, "Hello+there%21"));
    CharSequence decoded = AsciiStringURLCodec.decode(encoded, StandardCharsets.US_ASCII);
    Assert.assertTrue(AsciiString.contentEquals(decoded, plain));
  }

  @Test(groups = "unit")
  public void testSafeCharEncodeDecode() throws Exception {
    String plain = "abc123_-.*";
    AsciiString encoded = AsciiStringURLCodec.encode(plain, StandardCharsets.US_ASCII);
    Assert.assertTrue(encoded.contentEquals(plain));
    CharSequence decoded = AsciiStringURLCodec.decode(encoded, StandardCharsets.US_ASCII);
    Assert.assertTrue(AsciiString.contentEquals(decoded, plain));
  }

  @Test(groups = "unit")
  public void testUnsafeEncodeDecode() throws Exception {
    String plain = "~!@#$%^&()+{}\"\\;:`,/[]";
    AsciiString encoded = AsciiStringURLCodec.encode(plain, StandardCharsets.US_ASCII);
    Assert.assertTrue(encoded.contentEquals("%7E%21%40%23%24%25%5E%26%28%29%2B%7B%7D%22%5C%3B%3A%60%2C%2F%5B%5D"));
    CharSequence decoded = AsciiStringURLCodec.decode(encoded, StandardCharsets.US_ASCII);
    Assert.assertTrue(AsciiString.contentEquals(decoded, plain));
  }

  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
  public void testDecodeInvalid1() {
    AsciiStringURLCodec.decode(new AsciiString("%"), StandardCharsets.US_ASCII);
  }

  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
  public void testDecodeInvalid2() {
    AsciiStringURLCodec.decode(new AsciiString("%A"), StandardCharsets.US_ASCII);
  }

  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
  public void testDecodeInvalid3() {
    AsciiStringURLCodec.decode(new AsciiString("%WW"), StandardCharsets.US_ASCII);
  }

  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
  public void testDecodeInvalid4() {
    AsciiStringURLCodec.decode(new AsciiString("%0W"), StandardCharsets.US_ASCII);
  }

}
