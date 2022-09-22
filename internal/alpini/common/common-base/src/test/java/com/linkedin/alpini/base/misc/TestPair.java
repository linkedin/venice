package com.linkedin.alpini.base.misc;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Antony T Curtis <acurtis@linkedin.com>
 */
public class TestPair {
  @Test(groups = "unit")
  public void testConstructor() {
    Pair<Integer, String> pair = new Pair<Integer, String>(1, "string");
    Assert.assertEquals(pair.getFirst(), Integer.valueOf(1));
    Assert.assertEquals(pair.getSecond(), "string");
  }

  @Test(groups = "unit")
  public void testConstructorMake() {
    Pair<Integer, String> pair = Pair.make(1, "string");
    Assert.assertEquals(pair.getFirst(), Integer.valueOf(1));
    Assert.assertEquals(pair.getSecond(), "string");
  }

  @Test(groups = "unit")
  public void testConstructorMakeArray() {
    Pair<Integer, String>[] pair = Pair.array(Pair.make(1, "string"), Pair.make(2, "foobar"));
    Assert.assertEquals(pair.length, 2);
    Assert.assertEquals(pair[0].getFirst(), Integer.valueOf(1));
    Assert.assertEquals(pair[0].getSecond(), "string");
    Assert.assertEquals(pair[1].getFirst(), Integer.valueOf(2));
    Assert.assertEquals(pair[1].getSecond(), "foobar");
  }

  @Test(groups = "unit")
  public void testEquality() {
    Assert.assertEquals(Pair.make(1, "hello"), Pair.make(1, "hello"));
    Assert.assertNotSame(Pair.make(1, "hello"), Pair.make(1, "hello"));
    Assert.assertEquals(Pair.make(1, "hello").toString(), Pair.make(1, "hello").toString());

    Assert.assertNotEquals(Pair.make(1, "hello"), Pair.make(2, "hello"));
    Assert.assertNotEquals(Pair.make(1, "hello"), Pair.make(1, "foobar"));

    Assert.assertNotEquals((Object) Pair.make(1, "hello"), Pair.make(1, "foobar").toString());

    Assert.assertTrue(Pair.make(1, "hello").equals(Pair.make(1, "hello")));
    Assert.assertFalse(Pair.make(1, "hello").equals(Pair.make(1, "hello").toString()));
    Assert.assertFalse(Pair.make(1, "hello").equals(null));
    Assert.assertFalse(Pair.make(1, "hello").equals(null));
  }

  @Test(groups = "unit")
  public void testEqualityNull() {
    Assert.assertEquals(Pair.make(1, null), Pair.make(1, null));
    Assert.assertNotSame(Pair.make(1, null), Pair.make(1, null));
    Assert.assertEquals(Pair.make(1, null).toString(), Pair.make(1, null).toString());
  }

  @Test(groups = "unit")
  public void testSetOfPairs() {
    Set<Pair<Integer, String>> set = new HashSet<Pair<Integer, String>>();
    set.addAll(Arrays.asList(Pair.make(1, "hello"), Pair.make(2, "foobar")));

    Assert.assertTrue(set.contains(Pair.make(1, "hello")));
    Assert.assertTrue(set.contains(Pair.make(2, "foobar")));

    Assert.assertFalse(set.contains(Pair.make(2, "hello")));
    Assert.assertFalse(set.contains(Pair.make(1, "foobar")));
  }

}
