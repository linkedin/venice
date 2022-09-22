package com.linkedin.alpini.base.misc;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class TestTimeValue {
  @Test(groups = "unit")
  public void testConstructorDefault() {
    TimeValue value = new TimeValue();
    Assert.assertNull(value.getUnit());
    Assert.assertEquals(value.getRawValue(), -1);
  }

  @Test(groups = "unit")
  public void testClone() {
    for (int i = 0; i < 10; i++) {
      TimeValue value1 =
          new TimeValue(ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE), TimeUnit.MILLISECONDS);
      TimeValue value2 = value1.clone();
      Assert.assertTrue(value1.equals(value2));
      Assert.assertEquals(value2.hashCode(), value1.hashCode());
      Assert.assertEquals(value1.compareTo(value2), 0);
      Assert.assertNotSame(value2, value1);
    }
  }

  @Test(groups = "unit")
  public void testConvertTo() {
    TimeValue test1 = new TimeValue(10101, TimeUnit.NANOSECONDS);
    TimeValue test2 = test1.convertTo(TimeUnit.MICROSECONDS);
    Assert.assertEquals(test2.getRawValue(), 10);

    Map<String, Object> resultOfTest2 = null;
    try {
      resultOfTest2 = SimpleJsonMapper.mapJSON(test2.toString());
    } catch (Exception e) {
      Assert.fail("Invalid JSON", e);
    }

    Assert.assertEquals(resultOfTest2.get("rawValue"), 10);
    Assert.assertEquals(resultOfTest2.get("unit"), "MICROSECONDS");
    Assert.assertEquals(test1.getRawValue(TimeUnit.MICROSECONDS), 10);

    // should do no conversion when input unit matches output unit
    TimeValue test3 = test1.convertTo(TimeUnit.NANOSECONDS);
    Assert.assertEquals(test3, test1);
  }

  @Test(groups = "unit")
  public void testDifference() {
    TimeValue test1 = new TimeValue(10101, TimeUnit.NANOSECONDS);
    TimeValue test2 = new TimeValue(9, TimeUnit.MICROSECONDS);
    TimeValue test3 = test1.difference(test2);
    Assert.assertEquals(test3.getRawValue(), 1);

    Map<String, Object> resultOfTest3 = null;
    try {
      resultOfTest3 = SimpleJsonMapper.mapJSON(test3.toString());
    } catch (Exception e) {
      Assert.fail("Invalid JSON", e);
    }

    Assert.assertEquals(resultOfTest3.get("rawValue"), 1);
    Assert.assertEquals(resultOfTest3.get("unit"), "MICROSECONDS");

    TimeValue test4 = new TimeValue(1000, TimeUnit.NANOSECONDS).difference(new TimeValue(1000, TimeUnit.NANOSECONDS));
    Assert.assertEquals(test4.getRawValue(), 0);
  }

  @Test(groups = "unit")
  public void testAdd() {
    TimeValue test1 = new TimeValue(100, TimeUnit.NANOSECONDS);
    TimeValue test2 = new TimeValue(10, TimeUnit.MICROSECONDS);
    TimeValue test3 = new TimeValue(10, TimeUnit.MICROSECONDS);
    TimeValue test4 = new TimeValue(200, TimeUnit.NANOSECONDS);

    Assert.assertEquals(test1.add(test2), test3);
    Assert.assertEquals(test1.add(test1), test4);
  }

  @Test(groups = "unit")
  public void testGetRawValue() {
    TimeValue test = new TimeValue(10, TimeUnit.NANOSECONDS);
    Assert.assertEquals(test.getRawValue(), 10);
  }

  @Test(groups = "unit")
  public void testParseHappyPath() throws Exception {
    String testString = "{\"rawValue\":10,\"unit\":\"NANOSECONDS\"}";
    TimeValue testParsed = TimeValue.parse(testString);
    Assert.assertEquals(testParsed.getRawValue(), 10);
    Assert.assertEquals(testParsed.getUnit(), TimeUnit.NANOSECONDS);
  }

  @Test(groups = "unit")
  public void testParseExceptions() {
    try {
      String testString = "{\"rawValue\":\"BOGUS\",\"unit\":\"NANOSECONDS\"}";
      TimeValue.parse(testString);
      Assert.fail("Expected TimeValueParseException.");
    } catch (IOException ex) {
      // Good
    }

    try {
      String testString = "{\"rawValue\":BOGUS,\"unit\":\"NANOSECONDS\"}";
      TimeValue.parse(testString);
      Assert.fail("Expected TimeValueParseException.");
    } catch (IOException ex) {
      // Good
    }

    try {
      String testString = "{\"rawValue\":10}";
      TimeValue.parse(testString);
      System.out.println("XXXX: " + TimeValue.parse(testString));
      Assert.fail("Expected TimeValueParseException.");
    } catch (IOException ex) {
      // Good
    }

    try {
      String testString = "{\"unit\":\"NANOSECONDS\"}";
      TimeValue.parse(testString);
      Assert.fail("Expected TimeValueParseException.");
    } catch (IOException ex) {
      // Good
    }
  }

  @Test(groups = "unit")
  public void toStringTest() {
    TimeValue test = new TimeValue(10, TimeUnit.NANOSECONDS);
    String testString = test.toString();

    Map<String, Object> resultOfTest = null;
    try {
      resultOfTest = SimpleJsonMapper.mapJSON(testString);
    } catch (Exception e) {
      Assert.fail("Invalid JSON", e);
    }

    Assert.assertEquals(resultOfTest.get("rawValue"), 10);
    Assert.assertEquals(resultOfTest.get("unit"), "NANOSECONDS");
  }

  @Test(groups = "unit")
  public void testEquals() {
    TimeValue test1 = new TimeValue(100, TimeUnit.NANOSECONDS);
    TimeValue test2 = new TimeValue(100, TimeUnit.NANOSECONDS);
    TimeValue test3 = new TimeValue(10, TimeUnit.NANOSECONDS);
    TimeValue test4 = new TimeValue(100, TimeUnit.MICROSECONDS);

    Assert.assertTrue(test1.equals(test2));
    Assert.assertFalse(test1.equals(test3));
    Assert.assertFalse(test1.equals(test4));
    Assert.assertFalse(test1.equals(null));
    Assert.assertFalse(test1.equals("BOGUS"));

    Assert.assertTrue(test1.compareTo(test2) == 0);
    Assert.assertTrue(test1.compareTo(test3) == 1);
    Assert.assertTrue(test1.compareTo(test4) == -1);

    Assert.assertEquals(test1.hashCode(), test2.hashCode());
  }

}
