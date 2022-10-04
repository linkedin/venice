package com.linkedin.alpini.base.misc;

import java.util.Arrays;
import java.util.Date;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Greg Brandt (gbrandt@linkedin.com)
 */
public class TestDateUtils {
  @Test(groups = "unit")
  public void testParseDate() {
    Date date = DateUtils.parseRFC1123Date("Thu, 21 Jan 2010 17:47:00 EST");
    String str = DateUtils.getRFC1123Date(date);
    Assert.assertEquals(str, "Thu, 21 Jan 2010 22:47:00 GMT");
    date = DateUtils.parseRFC1123Date("  " + str);
    str = DateUtils.getRFC1123Date(date.getTime());
    Assert.assertEquals(str, "Thu, 21 Jan 2010 22:47:00 GMT");
    str = DateUtils.getRFC1036Date(date.getTime());
    Assert.assertEquals(str, "Thu, 21-Jan-10 22:47:00 GMT");
    try {
      DateUtils.parseRFC1123Date(str);
      Assert.fail();
    } catch (IllegalArgumentException ignored) {
      // expected
    }
    date = DateUtils.parseDate(str + "  ");
    str = DateUtils.getAnsiCDate(date.getTime());
    Assert.assertEquals(str, "Thu Jan 21 22:47:00 2010");
    date = DateUtils.parseDate("'" + str + "'");
    str = DateUtils.getRFC1123Date(date);
    Assert.assertEquals(str, "Thu, 21 Jan 2010 22:47:00 GMT");
    Assert.assertEquals(DateUtils.parseRFC1123Date("'" + str + "'"), date);
    try {
      // Reject trailing junk
      DateUtils.parseDate(str + " foo");
      Assert.fail();
    } catch (IllegalArgumentException ignored) {
      // expected
    }
  }

  @Test(groups = "unit")
  public void testEscapedSplitComma() throws Exception {
    String[] tokens;

    // Normal case
    tokens = DateUtils.escapedSplit("\\", ",", "+key:val,+hello:world");
    System.out.println(Arrays.toString(tokens));
    Assert.assertEquals(tokens.length, 2);
    Assert.assertEquals(tokens[0], "+key:val");
    Assert.assertEquals(tokens[1], "+hello:world");

    // Escaped case
    tokens = DateUtils.escapedSplit("\\", ",", "+ke\\,y:val,+hello:world");
    System.out.println(Arrays.toString(tokens));
    Assert.assertEquals(tokens.length, 2);
    Assert.assertEquals(tokens[0], "+ke\\,y:val");
    Assert.assertEquals(tokens[1], "+hello:world");

    // Split char at beginning of string
    tokens = DateUtils.escapedSplit("\\", ",", ",+key:val,+hello:world");
    System.out.println(Arrays.toString(tokens));
    Assert.assertEquals(tokens.length, 3);
    Assert.assertEquals(tokens[0], "");
    Assert.assertEquals(tokens[1], "+key:val");
    Assert.assertEquals(tokens[2], "+hello:world");

    // Split char at end of string
    tokens = DateUtils.escapedSplit("\\", ",", "+key:val,+hello:world,");
    System.out.println(Arrays.toString(tokens));
    Assert.assertEquals(tokens.length, 3);
    Assert.assertEquals(tokens[0], "+key:val");
    Assert.assertEquals(tokens[1], "+hello:world");
    Assert.assertEquals(tokens[2], "");

    // Escaped split char at beginning of string
    tokens = DateUtils.escapedSplit("\\", ",", "\\,+key:val,+hello:world");
    System.out.println(Arrays.toString(tokens));
    Assert.assertEquals(tokens.length, 2);
    Assert.assertEquals(tokens[0], "\\,+key:val");
    Assert.assertEquals(tokens[1], "+hello:world");

    // Escaped split char at end of string
    tokens = DateUtils.escapedSplit("\\", ",", "+key:val,+hello:world\\,");
    System.out.println(Arrays.toString(tokens));
    Assert.assertEquals(tokens.length, 2);
    Assert.assertEquals(tokens[0], "+key:val");
    Assert.assertEquals(tokens[1], "+hello:world\\,");

    // Escaped escape char before split char
    tokens = DateUtils.escapedSplit("\\", ",", "+key:val\\\\,+hello:world");
    System.out.println(Arrays.toString(tokens));
    Assert.assertEquals(tokens.length, 2);
    Assert.assertEquals(tokens[0], "+key:val\\\\");
    Assert.assertEquals(tokens[1], "+hello:world");
  }

  @Test(groups = "unit")
  public void testEscapedSplitFatArrow() throws Exception {
    String[] tokens;

    // Normal case
    tokens = DateUtils.escapedSplit("\\", "=>", "+key:val=>+hello:world");
    System.out.println(Arrays.toString(tokens));
    Assert.assertEquals(tokens.length, 2);
    Assert.assertEquals(tokens[0], "+key:val");
    Assert.assertEquals(tokens[1], "+hello:world");

    // Escaped case
    tokens = DateUtils.escapedSplit("\\", "=>", "+ke\\=>y:val=>+hello:world");
    System.out.println(Arrays.toString(tokens));
    Assert.assertEquals(tokens.length, 2);
    Assert.assertEquals(tokens[0], "+ke\\=>y:val");
    Assert.assertEquals(tokens[1], "+hello:world");

    // Split char at beginning of string
    tokens = DateUtils.escapedSplit("\\", "=>", "=>+key:val=>+hello:world");
    System.out.println(Arrays.toString(tokens));
    Assert.assertEquals(tokens.length, 3);
    Assert.assertEquals(tokens[0], "");
    Assert.assertEquals(tokens[1], "+key:val");
    Assert.assertEquals(tokens[2], "+hello:world");

    // Split char at end of string
    tokens = DateUtils.escapedSplit("\\", "=>", "+key:val=>+hello:world=>");
    System.out.println(Arrays.toString(tokens));
    Assert.assertEquals(tokens.length, 3);
    Assert.assertEquals(tokens[0], "+key:val");
    Assert.assertEquals(tokens[1], "+hello:world");
    Assert.assertEquals(tokens[2], "");

    // Escaped split char at beginning of string
    tokens = DateUtils.escapedSplit("\\", "=>", "\\=>+key:val=>+hello:world");
    System.out.println(Arrays.toString(tokens));
    Assert.assertEquals(tokens.length, 2);
    Assert.assertEquals(tokens[0], "\\=>+key:val");
    Assert.assertEquals(tokens[1], "+hello:world");

    // Escaped split char at end of string
    tokens = DateUtils.escapedSplit("\\", "=>", "+key:val=>+hello:world\\=>");
    System.out.println(Arrays.toString(tokens));
    Assert.assertEquals(tokens.length, 2);
    Assert.assertEquals(tokens[0], "+key:val");
    Assert.assertEquals(tokens[1], "+hello:world\\=>");
  }

  @Test(groups = "unit")
  public void testBadUsage() throws Exception {
    String[] tokens;

    // A split string that's longer than the target string
    checkFailure("\\", "abcd", "abc");

    // An empty split string
    checkFailure("\\", "", "abc");

    // A null split string
    checkFailure("\\", null, "abc");

    // A null target string
    checkFailure("\\", ",", null);

    // A null escape character
    checkFailure(null, ",", "a,b,c");

    // An empty escape character
    tokens = DateUtils.escapedSplit("", ",", "a,b,c");
    Assert.assertEquals(tokens.length, 3);
    Assert.assertEquals(tokens[0], "a");
    Assert.assertEquals(tokens[1], "b");
    Assert.assertEquals(tokens[2], "c");
  }

  private void checkFailure(String escape, String split, String target) {
    try {
      String[] tokens = DateUtils.escapedSplit(escape, split, target);
      Assert.fail();
    } catch (Exception e) {
      // Good
    }
  }
}
