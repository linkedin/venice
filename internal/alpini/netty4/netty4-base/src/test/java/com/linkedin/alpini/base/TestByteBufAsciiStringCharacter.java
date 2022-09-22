package com.linkedin.alpini.base;

import com.linkedin.alpini.base.misc.ByteBufAsciiString;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.AsciiString;
import io.netty.util.CharsetUtil;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.regex.Pattern;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestByteBufAsciiStringCharacter {
  private static final Random r = new Random();

  @Test(groups = "unit")
  public void testGetBytesStringBuilder() {
    final StringBuilder b = new StringBuilder();
    for (int i = 0; i < 1 << 16; ++i) {
      b.append("eéaà");
    }
    final String bString = b.toString();
    final Charset[] charsets = CharsetUtil.values();
    for (int i = 0; i < charsets.length; ++i) {
      final Charset charset = charsets[i];
      byte[] expected = bString.getBytes(charset);
      ByteBufAsciiString byteBufAsciiString = new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, b, charset);
      byte[] actual = byteBufAsciiString.toByteArray();
      Assert.assertEquals(actual, expected, "failure for " + charset);
      Assert.assertTrue(ByteBufAsciiString.contentEquals(byteBufAsciiString, new AsciiString(b, charset)));
      Assert.assertTrue(ByteBufAsciiString.contentEquals(new AsciiString(b, charset), byteBufAsciiString));
      Assert
          .assertTrue(ByteBufAsciiString.contentEquals(new AsciiString(b, charset), new AsciiString(bString, charset)));
      Assert.assertTrue(ByteBufAsciiString.contentEquals(byteBufAsciiString, byteBufAsciiString));
      Assert.assertTrue(
          ByteBufAsciiString
              .contentEquals(byteBufAsciiString, new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, b, charset)));
    }
  }

  @Test(groups = "unit")
  public void testGetBytesString() {
    final StringBuilder b = new StringBuilder();
    for (int i = 0; i < 1 << 16; ++i) {
      b.append("eéaà");
    }
    final String bString = b.toString();
    final Charset[] charsets = CharsetUtil.values();
    for (int i = 0; i < charsets.length; ++i) {
      final Charset charset = charsets[i];
      byte[] expected = bString.getBytes(charset);
      ByteBufAsciiString byteBufAsciiString =
          new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, bString, charset);
      byte[] actual = byteBufAsciiString.toByteArray();
      Assert.assertEquals(actual, expected, "failure for " + charset);
      Assert.assertTrue(ByteBufAsciiString.contentEquals(byteBufAsciiString, new AsciiString(bString, charset)));
      Assert.assertTrue(ByteBufAsciiString.contentEquals(new AsciiString(bString, charset), byteBufAsciiString));
    }
  }

  @Test(groups = "unit")
  public void testGetBytesAsciiString() {
    final StringBuilder b = new StringBuilder();
    for (int i = 0; i < 1 << 16; ++i) {
      b.append("eéaà");
    }
    final String bString = b.toString();
    // The AsciiString class actually limits the Charset to ISO_8859_1
    byte[] expected = bString.getBytes(CharsetUtil.ISO_8859_1);
    byte[] actual = new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, bString).toByteArray();
    Assert.assertEquals(actual, expected);
  }

  @Test(groups = "unit")
  public void testComparisonWithString() {
    String string = "shouldn't fail";
    ByteBufAsciiString ascii = new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, string.toCharArray());
    Assert.assertEquals(ascii.toString(), string);
  }

  @Test(groups = "unit")
  public void subSequenceTest() {
    byte[] init = { 't', 'h', 'i', 's', ' ', 'i', 's', ' ', 'a', ' ', 't', 'e', 's', 't' };
    ByteBufAsciiString ascii = new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, init);
    final int start = 2;
    final int end = init.length;
    ByteBufAsciiString sub1 = ascii.subSequence(start, end, false);
    ByteBufAsciiString sub2 = ascii.subSequence(start, end);
    Assert.assertEquals(sub1.hashCode(), sub2.hashCode());
    Assert.assertEquals(sub1, sub2);
    for (int i = start; i < end; ++i) {
      Assert.assertEquals(init[i], sub1.byteAt(i - start));
    }
    Assert.assertThrows(IndexOutOfBoundsException.class, () -> sub1.byteAt(20));
    Assert.assertEquals(ascii.subSequence(10).toString(), "test");
    Assert.assertSame(ascii.subSequence(0, ascii.length()), ascii);
    Assert.assertThrows(IndexOutOfBoundsException.class, () -> ascii.subSequence(0, ascii.length() + 1));
    char[] chars = ascii.toString().toCharArray();
    Assert.assertThrows(
        IndexOutOfBoundsException.class,
        () -> new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, chars, 0, 20));
    Assert.assertThrows(
        IndexOutOfBoundsException.class,
        () -> new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, ascii, 0, 20));
  }

  @Test(groups = "unit")
  public void testContains() {
    String[] falseLhs = { null, "a", "aa", "aaa" };
    String[] falseRhs = { null, "b", "ba", "baa" };
    for (int i = 0; i < falseLhs.length; ++i) {
      for (int j = 0; j < falseRhs.length; ++j) {
        assertContains(falseLhs[i], falseRhs[i], false, false);
      }
    }

    assertContains("", "", true, true);
    assertContains("AsfdsF", "", true, true);
    assertContains("", "b", false, false);
    assertContains("a", "a", true, true);
    assertContains("a", "b", false, false);
    assertContains("a", "A", false, true);
    String b = "xyz";
    String a = b;
    assertContains(a, b, true, true);

    a = "a" + b;
    assertContains(a, b, true, true);

    a = b + "a";
    assertContains(a, b, true, true);

    a = "a" + b + "a";
    assertContains(a, b, true, true);

    b = "xYz";
    a = "xyz";
    assertContains(a, b, false, true);

    b = "xYz";
    a = "xyzxxxXyZ" + b + "aaa";
    assertContains(a, b, true, true);

    b = "foOo";
    a = "fooofoO";
    assertContains(a, b, false, true);

    b = "Content-Equals: 10000";
    a = "content-equals: 1000";
    assertContains(a, b, false, false);
    a += "0";
    assertContains(a, b, false, true);
  }

  private static void assertContains(String a, String b, boolean caseSensitiveEquals, boolean caseInsenstaiveEquals) {
    Assert.assertEquals(ByteBufAsciiString.contains(a, b), caseSensitiveEquals);
    Assert.assertEquals(ByteBufAsciiString.containsIgnoreCase(a, b), caseInsenstaiveEquals);

    ByteBufAsciiString aString = a != null ? new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, a) : null;
    ByteBufAsciiString bString = a != null ? new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, b) : null;
    Assert.assertEquals(ByteBufAsciiString.contains(aString, bString), caseSensitiveEquals);
    Assert.assertEquals(ByteBufAsciiString.containsIgnoreCase(aString, bString), caseInsenstaiveEquals);
  }

  @Test(groups = "unit")
  public void testCaseSensitivity() {
    int i = 0;
    for (; i < 32; i++) {
      doCaseSensitivity(i);
    }
    final int min = i;
    final int max = 4000;
    final int len = r.nextInt((max - min) + 1) + min;
    doCaseSensitivity(len);
  }

  private static void doCaseSensitivity(int len) {
    // Build an upper case and lower case string
    final int upperA = 'A';
    final int upperZ = 'Z';
    final int upperToLower = (int) 'a' - upperA;
    byte[] lowerCaseBytes = new byte[len];
    StringBuilder upperCaseBuilder = new StringBuilder(len);
    for (int i = 0; i < len; ++i) {
      char upper = (char) (r.nextInt((upperZ - upperA) + 1) + upperA);
      upperCaseBuilder.append(upper);
      lowerCaseBytes[i] = (byte) (upper + upperToLower);
    }
    String upperCaseString = upperCaseBuilder.toString();
    String lowerCaseString = new String(lowerCaseBytes);
    ByteBufAsciiString lowerCaseAscii = new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, lowerCaseBytes);
    ByteBufAsciiString upperCaseAscii = new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, upperCaseString);

    Assert.assertSame(lowerCaseAscii.toLowerCase(), lowerCaseAscii);
    Assert.assertSame(upperCaseAscii.toUpperCase(), upperCaseAscii);

    final String errorString = "len: " + len;
    // Test upper case hash codes are equal
    final int upperCaseExpected = upperCaseAscii.hashCode();
    Assert.assertEquals(ByteBufAsciiString.hashCode(upperCaseBuilder), upperCaseExpected, errorString);
    Assert.assertEquals(ByteBufAsciiString.hashCode(upperCaseString), upperCaseExpected, errorString);
    Assert.assertEquals(upperCaseAscii.hashCode(), upperCaseExpected, errorString);

    // Test lower case hash codes are equal
    final int lowerCaseExpected = lowerCaseAscii.hashCode();
    Assert.assertEquals(ByteBufAsciiString.hashCode(lowerCaseAscii), lowerCaseExpected, errorString);
    Assert.assertEquals(ByteBufAsciiString.hashCode(lowerCaseString), lowerCaseExpected, errorString);
    Assert.assertEquals(lowerCaseAscii.hashCode(), lowerCaseExpected, errorString);

    // Test case insensitive hash codes are equal
    final int expectedCaseInsensitive = lowerCaseAscii.hashCode();
    Assert.assertEquals(ByteBufAsciiString.hashCode(upperCaseBuilder), expectedCaseInsensitive, errorString);
    Assert.assertEquals(ByteBufAsciiString.hashCode(upperCaseString), expectedCaseInsensitive, errorString);
    Assert.assertEquals(ByteBufAsciiString.hashCode(lowerCaseString), expectedCaseInsensitive, errorString);
    Assert.assertEquals(ByteBufAsciiString.hashCode(lowerCaseAscii), expectedCaseInsensitive, errorString);
    Assert.assertEquals(ByteBufAsciiString.hashCode(upperCaseAscii), expectedCaseInsensitive, errorString);
    Assert.assertEquals(lowerCaseAscii.hashCode(), expectedCaseInsensitive);
    Assert.assertEquals(upperCaseAscii.hashCode(), expectedCaseInsensitive);

    // Test that opposite cases are equal
    Assert.assertEquals(ByteBufAsciiString.hashCode(upperCaseString), lowerCaseAscii.hashCode(), errorString);
    Assert.assertEquals(ByteBufAsciiString.hashCode(lowerCaseString), upperCaseAscii.hashCode(), errorString);

    Assert.assertEquals(upperCaseAscii.toLowerCase(), lowerCaseAscii);
    Assert.assertEquals(lowerCaseAscii.toUpperCase(), upperCaseAscii);
    Assert.assertEquals(
        upperCaseAscii,
        new ByteBufAsciiString(
            UnpooledByteBufAllocator.DEFAULT,
            upperCaseAscii.toString().toCharArray(),
            StandardCharsets.UTF_8));
  }

  @Test(groups = "unit")
  public void caseInsensitiveHasherCharBuffer() {
    String s1 = new String("TRANSFER-ENCODING");
    char[] array = new char[128];
    final int offset = 100;
    for (int i = 0; i < s1.length(); ++i) {
      array[offset + i] = s1.charAt(i);
    }
    CharBuffer buffer = CharBuffer.wrap(array, offset, s1.length());
    Assert.assertEquals(ByteBufAsciiString.hashCode(s1), ByteBufAsciiString.hashCode(buffer));
  }

  /*@Test(groups = "unit")
  public void testBooleanUtilityMethods() {
    assertTrue(new AsciiString(new byte[] { 1 }).parseBoolean());
    assertFalse(AsciiString.EMPTY_STRING.parseBoolean());
    assertFalse(new AsciiString(new byte[] { 0 }).parseBoolean());
    assertTrue(new AsciiString(new byte[] { 5 }).parseBoolean());
    assertTrue(new AsciiString(new byte[] { 2, 0 }).parseBoolean());
  }*/

  @Test(groups = "unit")
  public void testEqualsIgnoreCase() {
    Assert.assertTrue(ByteBufAsciiString.contentEqualsIgnoreCase(null, null));
    Assert.assertFalse(ByteBufAsciiString.contentEqualsIgnoreCase(null, "foo"));
    Assert.assertFalse(ByteBufAsciiString.contentEqualsIgnoreCase("bar", null));
    Assert.assertTrue(ByteBufAsciiString.contentEqualsIgnoreCase("FoO", "fOo"));

    // Test variations (Ascii + String, Ascii + Ascii, String + Ascii)
    ByteBufAsciiString stringFoO = new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "FoO");
    Assert.assertFalse(stringFoO.contentEqualsIgnoreCase(null));
    Assert.assertTrue(
        ByteBufAsciiString
            .contentEqualsIgnoreCase(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, stringFoO), "fOo"));
    Assert.assertTrue(
        ByteBufAsciiString
            .contentEqualsIgnoreCase(stringFoO, new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "fOo")));
    Assert.assertTrue(
        ByteBufAsciiString
            .contentEqualsIgnoreCase("FoO", new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "fOo")));

    // Test variations (Ascii + String, Ascii + Ascii, String + Ascii)
    Assert.assertFalse(ByteBufAsciiString.contentEqualsIgnoreCase(stringFoO, "bAr"));
    Assert.assertFalse(
        ByteBufAsciiString
            .contentEqualsIgnoreCase(stringFoO, new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "bAr")));
    Assert.assertFalse(
        ByteBufAsciiString
            .contentEqualsIgnoreCase("FoO", new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "bAr")));
  }

  @Test(groups = "unit")
  public void testIndexOfIgnoreCase() {
    Assert.assertEquals(ByteBufAsciiString.indexOfIgnoreCase(null, "abc", 1), -1);
    Assert.assertEquals(ByteBufAsciiString.indexOfIgnoreCase("abc", null, 1), -1);
    Assert.assertEquals(ByteBufAsciiString.indexOfIgnoreCase("", "", 0), 0);
    Assert.assertEquals(ByteBufAsciiString.indexOfIgnoreCase("aabaabaa", "A", 0), 0);
    Assert.assertEquals(ByteBufAsciiString.indexOfIgnoreCase("aabaabaa", "B", 0), 2);
    Assert.assertEquals(ByteBufAsciiString.indexOfIgnoreCase("aabaabaa", "AB", 0), 1);
    Assert.assertEquals(ByteBufAsciiString.indexOfIgnoreCase("aabaabaa", "B", 3), 5);
    Assert.assertEquals(ByteBufAsciiString.indexOfIgnoreCase("aabaabaa", "B", 9), -1);
    Assert.assertEquals(ByteBufAsciiString.indexOfIgnoreCase("aabaabaa", "B", -1), 2);
    Assert.assertEquals(ByteBufAsciiString.indexOfIgnoreCase("aabaabaa", "", 2), 2);
    Assert.assertEquals(ByteBufAsciiString.indexOfIgnoreCase("abc", "", 9), -1);
    Assert.assertEquals(ByteBufAsciiString.indexOfIgnoreCase("ãabaabaa", "Ã", 0), 0);

    Assert.assertEquals(
        ByteBufAsciiString.indexOfIgnoreCase(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abc"), null, 1),
        -1);
    Assert.assertEquals(ByteBufAsciiString.indexOfIgnoreCase(ByteBufAsciiString.EMPTY_STRING, "", 0), 0);
    Assert.assertEquals(
        ByteBufAsciiString
            .indexOfIgnoreCase(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "aabaabaa"), "A", 0),
        0);
    Assert.assertEquals(
        ByteBufAsciiString
            .indexOfIgnoreCase(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "aabaabaa"), "B", 0),
        2);
    Assert.assertEquals(
        ByteBufAsciiString
            .indexOfIgnoreCase(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "aabaabaa"), "AB", 0),
        1);
    Assert.assertEquals(
        ByteBufAsciiString
            .indexOfIgnoreCase(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "aabaabaa"), "B", 3),
        5);
    Assert.assertEquals(
        ByteBufAsciiString
            .indexOfIgnoreCase(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "aabaabaa"), "B", 9),
        -1);
    Assert.assertEquals(
        ByteBufAsciiString
            .indexOfIgnoreCase(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "aabaabaa"), "B", -1),
        2);
    Assert.assertEquals(
        ByteBufAsciiString
            .indexOfIgnoreCase(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "aabaabaa"), "", 2),
        2);
    Assert.assertEquals(
        ByteBufAsciiString.indexOfIgnoreCase(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abc"), "", 9),
        -1);
    Assert.assertEquals(
        ByteBufAsciiString
            .indexOfIgnoreCase(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "ãabaabaa"), "Ã", 0),
        -1);
  }

  @Test(groups = "unit")
  public void testIndexOfIgnoreCaseAscii() {
    Assert.assertEquals(ByteBufAsciiString.indexOfIgnoreCaseAscii(null, "abc", 1), -1);
    Assert.assertEquals(ByteBufAsciiString.indexOfIgnoreCaseAscii("abc", null, 1), -1);
    Assert.assertEquals(ByteBufAsciiString.indexOfIgnoreCaseAscii("", "", 0), 0);
    Assert.assertEquals(ByteBufAsciiString.indexOfIgnoreCaseAscii("aabaabaa", "A", 0), 0);
    Assert.assertEquals(ByteBufAsciiString.indexOfIgnoreCaseAscii("aabaabaa", "B", 0), 2);
    Assert.assertEquals(ByteBufAsciiString.indexOfIgnoreCaseAscii("aabaabaa", "AB", 0), 1);
    Assert.assertEquals(ByteBufAsciiString.indexOfIgnoreCaseAscii("aabaabaa", "B", 3), 5);
    Assert.assertEquals(ByteBufAsciiString.indexOfIgnoreCaseAscii("aabaabaa", "B", 9), -1);
    Assert.assertEquals(ByteBufAsciiString.indexOfIgnoreCaseAscii("aabaabaa", "B", -1), 2);
    Assert.assertEquals(ByteBufAsciiString.indexOfIgnoreCaseAscii("aabaabaa", "", 2), 2);
    Assert.assertEquals(ByteBufAsciiString.indexOfIgnoreCaseAscii("abc", "", 9), -1);

    Assert.assertEquals(
        ByteBufAsciiString
            .indexOfIgnoreCaseAscii(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abc"), null, 1),
        -1);
    Assert.assertEquals(ByteBufAsciiString.indexOfIgnoreCaseAscii(ByteBufAsciiString.EMPTY_STRING, "", 0), 0);
    Assert.assertEquals(
        ByteBufAsciiString
            .indexOfIgnoreCaseAscii(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "aabaabaa"), "A", 0),
        0);
    Assert.assertEquals(
        ByteBufAsciiString
            .indexOfIgnoreCaseAscii(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "aabaabaa"), "B", 0),
        2);
    Assert.assertEquals(
        ByteBufAsciiString
            .indexOfIgnoreCaseAscii(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "aabaabaa"), "AB", 0),
        1);
    Assert.assertEquals(
        ByteBufAsciiString
            .indexOfIgnoreCaseAscii(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "aabaabaa"), "B", 3),
        5);
    Assert.assertEquals(
        ByteBufAsciiString
            .indexOfIgnoreCaseAscii(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "aabaabaa"), "B", 9),
        -1);
    Assert.assertEquals(
        ByteBufAsciiString
            .indexOfIgnoreCaseAscii(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "aabaabaa"), "B", -1),
        2);
    Assert.assertEquals(
        ByteBufAsciiString
            .indexOfIgnoreCaseAscii(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "aabaabaa"), "", 2),
        2);
    Assert.assertEquals(
        ByteBufAsciiString
            .indexOfIgnoreCaseAscii(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abc"), "", 9),
        -1);
  }

  @Test(groups = "unit")
  public void testTrim() {
    Assert.assertEquals(ByteBufAsciiString.EMPTY_STRING.trim().toString(), "");
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "  abc").trim().toString(), "abc");
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abc  ").trim().toString(), "abc");
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "  abc  ").trim().toString(), "abc");

    Assert.assertEquals(
        ByteBufAsciiString.trim(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "  abc")).toString(),
        "abc");
    Assert.assertEquals(
        ByteBufAsciiString.trim(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abc  ")).toString(),
        "abc");
    Assert.assertEquals(
        ByteBufAsciiString.trim(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "  abc  ")).toString(),
        "abc");

    Assert.assertEquals(ByteBufAsciiString.trim(AsciiString.of("  abc")).toString(), "abc");
    Assert.assertEquals(ByteBufAsciiString.trim(AsciiString.of("abc  ")).toString(), "abc");
    Assert.assertEquals(ByteBufAsciiString.trim(AsciiString.of("  abc  ")).toString(), "abc");
  }

  @Test(groups = "unit")
  public void testIndexOfChar() {
    Assert.assertEquals(ByteBufAsciiString.indexOf(null, 'a', 0), -1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "").indexOf('a', 0), -1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abc").indexOf('d', 0), -1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "aabaabaa").indexOf('A', 0), -1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "aabaabaa").indexOf('a', 0), 0);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "aabaabaa").indexOf('a', 1), 1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "aabaabaa").indexOf('a', 2), 3);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "aabdabaa").indexOf('d', 1), 3);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd", 1, 2).indexOf('c', 0), 1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd", 1, 2).indexOf('c', -1), 1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd", 1, 3).indexOf('d', 2), 2);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd", 1, 2).indexOf('b', 0), 0);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd", 0, 2).indexOf('c', 0), -1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd", 1, 3).indexOf('a', 0), -1);
    Assert
        .assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd", 1, 3).indexOf('\u1077', 0), -1);
  }

  @Test(groups = "unit")
  public void testIndexOfCharSequence() {
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd").indexOf("abcd", 0), 0);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd").indexOf("abc", 0), 0);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd").indexOf("bcd", 0), 1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd").indexOf("bc", 0), 1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcdabcd").indexOf("bcd", 0), 1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd", 1, 2).indexOf("bc", 0), 0);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd", 1, 3).indexOf("bcd", 0), 0);
    Assert
        .assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcdabcd", 4, 4).indexOf("bcd", 0), 1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "012345").indexOf("345", 3), 3);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "012345").indexOf("345", 0), 3);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "012345").indexOf("345", -1), 3);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "012345").indexOf("\u1077345", 0), -1);

    // Test with empty string
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd").indexOf("", 0), 0);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd").indexOf("", 1), 1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd", 1, 3).indexOf("", 4), 3);

    // Test not found
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd").indexOf("abcde", 0), -1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcdbc").indexOf("bce", 0), -1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd", 1, 3).indexOf("abc", 0), -1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd", 1, 2).indexOf("bd", 0), -1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "012345").indexOf("345", 4), -1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "012345").indexOf("abc", 3), -1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "012345").indexOf("abc", 0), -1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "012345").indexOf("abcdefghi", 0), -1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "012345").indexOf("abcdefghi", 4), -1);
  }

  @Test(groups = "unit")
  public void testStaticIndexOfChar() {
    Assert.assertEquals(ByteBufAsciiString.indexOf(null, 'a', 0), -1);
    Assert.assertEquals(ByteBufAsciiString.indexOf("", 'a', 0), -1);
    Assert.assertEquals(ByteBufAsciiString.indexOf("abc", 'd', 0), -1);
    Assert.assertEquals(ByteBufAsciiString.indexOf("aabaabaa", 'A', 0), -1);
    Assert.assertEquals(ByteBufAsciiString.indexOf("aabaabaa", 'a', 0), 0);
    Assert.assertEquals(ByteBufAsciiString.indexOf("aabaabaa", 'a', 1), 1);
    Assert.assertEquals(ByteBufAsciiString.indexOf("aabaabaa", 'a', 2), 3);
    Assert.assertEquals(ByteBufAsciiString.indexOf("aabdabaa", 'd', 1), 3);
    Assert.assertEquals(ByteBufAsciiString.indexOf("aabdabaa", '\u203d', 1), -1);

    Assert.assertEquals(ByteBufAsciiString.indexOf(ByteBufAsciiString.EMPTY_STRING, 'a', 0), -1);
    Assert.assertEquals(
        ByteBufAsciiString.indexOf(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abc"), 'd', 0),
        -1);
    Assert.assertEquals(
        ByteBufAsciiString.indexOf(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "aabaabaa"), 'A', 0),
        -1);
    Assert.assertEquals(
        ByteBufAsciiString.indexOf(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "aabaabaa"), 'a', 0),
        0);
    Assert.assertEquals(
        ByteBufAsciiString.indexOf(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "aabaabaa"), 'a', 1),
        1);
    Assert.assertEquals(
        ByteBufAsciiString.indexOf(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "aabaabaa"), 'a', 2),
        3);
    Assert.assertEquals(
        ByteBufAsciiString.indexOf(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "aabdabaa"), 'd', 1),
        3);

    Assert.assertEquals(ByteBufAsciiString.indexOf(AsciiString.EMPTY_STRING, 'a', 0), -1);
    Assert.assertEquals(ByteBufAsciiString.indexOf(AsciiString.of("abc"), 'd', 0), -1);
    Assert.assertEquals(ByteBufAsciiString.indexOf(AsciiString.of("aabaabaa"), 'A', 0), -1);
    Assert.assertEquals(ByteBufAsciiString.indexOf(AsciiString.of("aabaabaa"), 'a', 0), 0);
    Assert.assertEquals(ByteBufAsciiString.indexOf(AsciiString.of("aabaabaa"), 'a', 1), 1);
    Assert.assertEquals(ByteBufAsciiString.indexOf(AsciiString.of("aabaabaa"), 'a', 2), 3);
    Assert.assertEquals(ByteBufAsciiString.indexOf(AsciiString.of("aabdabaa"), 'd', 1), 3);
  }

  @Test(groups = "unit")
  public void testLastIndexOfCharSequence() {
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd").lastIndexOf("abcd", 0), 0);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd").lastIndexOf("abc", 4), 0);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd").lastIndexOf("bcd", 4), 1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd").lastIndexOf("bc", 4), 1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcdabcd").lastIndexOf("bcd", 10), 5);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcdabcd").lastIndexOf("bcd"), 5);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd", 1, 2).lastIndexOf("bc", 0), 0);
    Assert
        .assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd", 1, 3).lastIndexOf("bcd", 0), 0);
    Assert.assertEquals(
        new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcdabcd", 4, 4).lastIndexOf("bcd", 4),
        1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "012345").lastIndexOf("345", 3), 3);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "012345").lastIndexOf("345", 6), 3);
    Assert.assertEquals(
        new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "012345").lastIndexOf("\u1077345", 0),
        -1);

    // Test with empty string
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd").lastIndexOf("", 0), 0);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd").lastIndexOf("", 1), 1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd", 1, 3).lastIndexOf("", 4), 3);

    // Test not found
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd").lastIndexOf("abcde", 0), -1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcdbc").lastIndexOf("bce", 0), -1);
    Assert
        .assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd", 1, 3).lastIndexOf("abc", 0), -1);
    Assert
        .assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd", 1, 2).lastIndexOf("bd", 0), -1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "012345").lastIndexOf("345", 2), -1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "012345").lastIndexOf("abc", 3), -1);
    Assert.assertEquals(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "012345").lastIndexOf("abc", 0), -1);
    Assert.assertEquals(
        new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "012345").lastIndexOf("abcdefghi", 0),
        -1);
    Assert.assertEquals(
        new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "012345").lastIndexOf("abcdefghi", 4),
        -1);
  }

  @Test(groups = "unit")
  public void testReplace() {
    ByteBufAsciiString abcd = new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd");
    Assert.assertEquals(abcd.replace('b', 'd'), new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "adcd"));
    Assert.assertEquals(abcd.replace('a', 'd'), new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "dbcd"));
    Assert.assertEquals(abcd.replace('d', 'a'), new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abca"));
    Assert.assertSame(abcd.replace('x', 'a'), abcd);
    Assert.assertSame(abcd.replace('\u203d', 'a'), abcd);
    Assert.assertEquals(
        new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd", 1, 2).replace('b', 'c'),
        new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "cc"));
    Assert.assertEquals(
        new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcd", 1, 2).replace('c', 'b'),
        new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "bb"));
    Assert.assertEquals(
        new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcdc", 1, 4).replace('c', 'd'),
        new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "bddd"));
    Assert.assertEquals(
        new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abcada", 0, 5).replace('a', 'x'),
        new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "xbcxd"));
  }

  @Test(groups = "unit")
  public void testSubStringHashCode() {
    // two "123"s
    Assert.assertEquals(ByteBufAsciiString.hashCode("123"), ByteBufAsciiString.hashCode("a123".substring(1)));
  }

  @Test(groups = "unit")
  public void testIndexOf() {
    ByteBufAsciiString foo = new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "This is a test");
    int i1 = foo.indexOf(' ', 0);
    Assert.assertEquals(i1, 4);
    int i2 = foo.indexOf(' ', i1 + 1);
    Assert.assertEquals(i2, 7);
    int i3 = foo.indexOf(' ', i2 + 1);
    Assert.assertEquals(i3, 9);
    Assert.assertTrue(i3 + 1 < foo.length());
    int i4 = foo.indexOf(' ', i3 + 1);
    Assert.assertEquals(i4, -1);
  }

  @Test(groups = "unit")
  public void testSplit() {
    Pattern p = Pattern.compile(":");
    ByteBufAsciiString[] strings;

    ByteBufAsciiString s = new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "boo:and:foo");

    strings = s.split(p);
    Assert.assertEquals(strings.length, 3);
    Assert.assertEquals(strings[0].toString(), "boo");
    Assert.assertEquals(strings[1].toString(), "and");
    Assert.assertEquals(strings[2].toString(), "foo");

    strings = s.split(p, 2);
    Assert.assertEquals(strings.length, 2);
    Assert.assertEquals(strings[0].toString(), "boo");
    Assert.assertEquals(strings[1].toString(), "and:foo");

    strings = s.split(p, 5);
    Assert.assertEquals(strings.length, 3);
    Assert.assertEquals(strings[0].toString(), "boo");
    Assert.assertEquals(strings[1].toString(), "and");
    Assert.assertEquals(strings[2].toString(), "foo");

    strings = s.split(p, -2);
    Assert.assertEquals(strings.length, 3);
    Assert.assertEquals(strings[0].toString(), "boo");
    Assert.assertEquals(strings[1].toString(), "and");
    Assert.assertEquals(strings[2].toString(), "foo");

    strings = s.split("x", 0);
    Assert.assertEquals(strings.length, 1);
    Assert.assertEquals(strings[0], s);

    p = Pattern.compile("o");

    strings = s.split(p);
    Assert.assertEquals(strings.length, 3);
    Assert.assertEquals(strings[0].toString(), "b");
    Assert.assertEquals(strings[1].toString(), "");
    Assert.assertEquals(strings[2].toString(), ":and:f");

    strings = s.split(p, 5);
    Assert.assertEquals(strings.length, 5);
    Assert.assertEquals(strings[0].toString(), "b");
    Assert.assertEquals(strings[1].toString(), "");
    Assert.assertEquals(strings[2].toString(), ":and:f");
    Assert.assertEquals(strings[3].toString(), "");
    Assert.assertEquals(strings[4].toString(), "");

    strings = s.split(p, -2);
    Assert.assertEquals(strings.length, 5);
    Assert.assertEquals(strings[0].toString(), "b");
    Assert.assertEquals(strings[1].toString(), "");
    Assert.assertEquals(strings[2].toString(), ":and:f");
    Assert.assertEquals(strings[3].toString(), "");
    Assert.assertEquals(strings[4].toString(), "");

    strings = s.split("o", 0);
    Assert.assertEquals(strings.length, 3);
    Assert.assertEquals(strings[0].toString(), "b");
    Assert.assertEquals(strings[1].toString(), "");
    Assert.assertEquals(strings[2].toString(), ":and:f");

    strings = s.split(':');
    Assert.assertEquals(strings.length, 3);
    Assert.assertEquals(strings[0].toString(), "boo");
    Assert.assertEquals(strings[1].toString(), "and");
    Assert.assertEquals(strings[2].toString(), "foo");

    strings = s.split('o');
    Assert.assertEquals(strings.length, 3);
    Assert.assertEquals(strings[0].toString(), "b");
    Assert.assertEquals(strings[1].toString(), "");
    Assert.assertEquals(strings[2].toString(), ":and:f");

    strings = s.split('x');
    Assert.assertEquals(strings.length, 1);
    Assert.assertEquals(strings[0], s);
  }

  @Test(groups = "unit")
  public void testEndsWith() {
    Assert.assertTrue(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "hello world").endsWith("world"));
    Assert.assertFalse(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "hello world!").endsWith("world"));
    Assert.assertTrue(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "hello world").endsWith(""));
    Assert.assertFalse(ByteBufAsciiString.EMPTY_STRING.endsWith("!"));

    Assert.assertTrue(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "hello world").matches(".*world"));
    Assert.assertThrows(
        NullPointerException.class,
        () -> new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "hello world")
            .regionMatches(true, 0, null, 0, 1));
    Assert.assertThrows(
        NullPointerException.class,
        () -> new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "hello world")
            .regionMatches(false, 0, null, 0, 1));
    Assert.assertFalse(
        new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "hello world").regionMatches(true, -1, "", 0, 1));
    Assert.assertFalse(
        new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "hello world").regionMatches(true, 7, "world", 0, 5));
    Assert.assertFalse(
        new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "hello world").regionMatches(true, 0, "", -1, 1));
    Assert.assertFalse(
        new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "hello world").regionMatches(true, 0, "world", 6, 5));
    Assert.assertTrue(
        new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "hello world").regionMatches(true, 6, "WORLD", 0, 5));
    Assert.assertTrue(
        new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "hello world").regionMatches(false, 6, "world", 0, 5));

    Assert.assertFalse(
        ByteBufAsciiString.regionMatches(
            new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "hello world"),
            true,
            -1,
            null,
            0,
            1));
    Assert.assertFalse(
        ByteBufAsciiString.regionMatchesAscii(
            new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "hello world"),
            true,
            -1,
            null,
            0,
            1));
  }

  @Test(groups = "unit")
  public void testStartsWith() {
    Assert.assertTrue(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "hello world").startsWith("hello"));
    Assert.assertFalse(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "hello world").startsWith("hello", -1));
    Assert.assertFalse(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "hello world!").startsWith("Hello"));
    Assert.assertTrue(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "hello world").startsWith("lo", 3));
    Assert.assertTrue(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "hello world").startsWith(""));
    Assert.assertFalse(ByteBufAsciiString.EMPTY_STRING.startsWith("!"));

    Assert.assertFalse(
        ByteBufAsciiString
            .startsWith(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "hello world!"), "Hello"));
    Assert.assertTrue(
        ByteBufAsciiString
            .startsWithIgnoreCase(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "hello world!"), "Hello"));
    Assert.assertFalse(
        ByteBufAsciiString
            .startsWith(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "hello world!"), "LO", 3));
    Assert.assertTrue(
        ByteBufAsciiString
            .startsWithIgnoreCase(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "hello world!"), "LO", 3));
    Assert.assertTrue(new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "hello world").matches("hello.*"));

  }

  @Test(groups = "unit")
  public void testConcat() {

    ByteBufAsciiString abc = new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "abc");
    ByteBufAsciiString def = new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "def");

    Assert.assertEquals(abc.concat(def).toString(), "abcdef");
    Assert.assertEquals(abc.concat("def").toString(), "abcdef");
    Assert.assertEquals(abc.concat("d", AsciiString.of("ef")).toString(), "abcdef");
    Assert.assertEquals(abc.toString(), "abc");
    Assert.assertSame(abc.concat(ByteBufAsciiString.EMPTY_STRING), abc);
    Assert.assertSame(abc.concat(""), abc);
    Assert.assertSame(abc.concat(), abc);
    Assert.assertSame(ByteBufAsciiString.EMPTY_STRING.concat(def), def);
    Assert.assertEquals(ByteBufAsciiString.EMPTY_STRING.concat("def"), def);
    Assert.assertSame(ByteBufAsciiString.EMPTY_STRING.concat(""), ByteBufAsciiString.EMPTY_STRING);
  }

  @Test(groups = "unit")
  public void testToAsciiString() {
    Assert.assertSame(
        new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "").toAsciiString(),
        AsciiString.EMPTY_STRING);
    Assert.assertEquals(
        new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "Hello World").toAsciiString(),
        AsciiString.of("Hello World"));
    Assert.assertEquals(
        new ByteBufAsciiString("Hello World".getBytes(), 0, 11, false).toAsciiString(),
        AsciiString.of("Hello World"));

    Assert.assertSame(
        new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "").toString(0, 0),
        AsciiString.EMPTY_STRING.toString());
    Assert.assertThrows(
        IndexOutOfBoundsException.class,
        () -> new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "").toString(1, 0));
  }

  @Test(groups = "unit")
  public void testCompareTo() {
    ByteBufAsciiString str = new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "Hello World");
    Assert.assertEquals(str.compareTo(str), 0);
    Assert.assertEquals(str.compareTo(str.toAsciiString()), 0);
    ByteBufAsciiString str2 = new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "Hello World!");
    Assert.assertEquals(str.compareTo(str2), -1);
    Assert.assertEquals(str2.compareTo(str), +1);
    ByteBufAsciiString str3 = new ByteBufAsciiString(UnpooledByteBufAllocator.DEFAULT, "Hello Worldz");
    Assert.assertEquals(str2.compareTo(str3), '!' - 'z');
    Assert.assertEquals(str3.compareTo(str2), 'z' - '!');
  }
}
