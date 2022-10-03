package com.linkedin.alpini.base.misc;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Some test vectors originate from Apache Commons URLCodecTest
 *
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class TestURLCodec {
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
        URLCodec.encode(ru_msg, StandardCharsets.UTF_8),
        "%D0%92%D1%81%D0%B5%D0%BC_%D0%BF%D1%80%D0%B8%D0%B2%D0%B5%D1%82");

    Assert.assertEquals(URLCodec.encode(ch_msg, StandardCharsets.UTF_8), "Gr%C3%BCezi_z%C3%A4m%C3%A4");

    Assert
        .assertEquals(URLCodec.decode(URLCodec.encode(ru_msg, StandardCharsets.UTF_8), StandardCharsets.UTF_8), ru_msg);

    Assert
        .assertEquals(URLCodec.decode(URLCodec.encode(ch_msg, StandardCharsets.UTF_8), StandardCharsets.UTF_8), ch_msg);
  }

  @Test(groups = "unit")
  public void testBasicEncodeDecode() throws Exception {
    String plain = "Hello there!";
    String encoded = URLCodec.encode(plain, StandardCharsets.US_ASCII);
    Assert.assertEquals(encoded, "Hello+there%21");
    Assert.assertEquals(URLCodec.decode(encoded, StandardCharsets.US_ASCII), plain);
  }

  @Test(groups = "unit")
  public void testSafeCharEncodeDecode() throws Exception {
    String plain = "abc123_-.*";
    String encoded = URLCodec.encode(plain, StandardCharsets.US_ASCII);
    Assert.assertSame(encoded, plain);
    String decoded = URLCodec.decode(encoded, StandardCharsets.US_ASCII);
    Assert.assertSame(decoded, plain);
  }

  @Test(groups = "unit")
  public void testUnsafeEncodeDecode() throws Exception {
    String plain = "~!@#$%^&()+{}\"\\;:`,/[]";
    String encoded = URLCodec.encode(plain, StandardCharsets.US_ASCII);
    Assert.assertEquals(encoded, "%7E%21%40%23%24%25%5E%26%28%29%2B%7B%7D%22%5C%3B%3A%60%2C%2F%5B%5D");
    String decoded = URLCodec.decode(encoded, StandardCharsets.US_ASCII);
    Assert.assertEquals(decoded, plain);
  }

  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
  public void testDecodeInvalid1() {
    URLCodec.decode("%", StandardCharsets.US_ASCII);
  }

  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
  public void testDecodeInvalid2() {
    URLCodec.decode("%A", StandardCharsets.US_ASCII);
  }

  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
  public void testDecodeInvalid3() {
    URLCodec.decode("%WW", StandardCharsets.US_ASCII);
  }

  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
  public void testDecodeInvalid4() {
    URLCodec.decode("%0W", StandardCharsets.US_ASCII);
  }

  @Test(groups = "unit")
  public void testESPENG32089() throws UnsupportedEncodingException {
    String encoded = "%F0%9D%91%A5.png";
    String target = URLDecoder.decode(encoded, "UTF-8");
    Assert.assertEquals(URLCodec.decode(encoded, StandardCharsets.UTF_8), target);
  }

  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "URLCodec: Malformed input")
  public void testESPENG32089Bad() {
    String encoded = "%F0%9D||%91%A5.png";
    URLCodec.decode(encoded, StandardCharsets.UTF_8);
    Assert.fail();
  }
}
