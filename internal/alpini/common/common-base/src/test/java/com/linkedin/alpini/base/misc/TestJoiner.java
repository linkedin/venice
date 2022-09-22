package com.linkedin.alpini.base.misc;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import javax.annotation.Nonnull;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Implementation of tests based upon Google Collections JoinerTest
 */
public class TestJoiner {
  private static final Joiner J = Joiner.on("-");

  // <Integer> needed to prevent warning :(
  private static final Iterable<Integer> ITERABLE_ = Collections.emptyList();
  private static final Iterable<Integer> ITERABLE_1 = Collections.singletonList(1);
  private static final Iterable<Integer> ITERABLE_12 = Arrays.asList(1, 2);
  private static final Iterable<Integer> ITERABLE_123 = Arrays.asList(1, 2, 3);
  private static final Iterable<Integer> ITERABLE_NULL = Collections.singletonList(null);
  private static final Iterable<Integer> ITERABLE_NULL_NULL = Arrays.asList((Integer) null, null);
  private static final Iterable<Integer> ITERABLE_NULL_1 = Arrays.asList(null, 1);
  private static final Iterable<Integer> ITERABLE_1_NULL = Arrays.asList(1, null);
  private static final Iterable<Integer> ITERABLE_1_NULL_2 = Arrays.asList(1, null, 2);
  private static final Iterable<Integer> ITERABLE_FOUR_NULLS = Arrays.asList((Integer) null, null, null, null);

  @Test(groups = "unit")
  public void testNoSpecialNullBehavior() {
    checkNoOutput(J, ITERABLE_);
    checkResult(J, ITERABLE_1, "1");
    checkResult(J, ITERABLE_12, "1-2");
    checkResult(J, ITERABLE_123, "1-2-3");

    try {
      J.join(ITERABLE_NULL);
      Assert.fail();
    } catch (NullPointerException expected) {
    }
    try {
      J.join(ITERABLE_1_NULL_2);
      Assert.fail();
    } catch (NullPointerException expected) {
    }

    try {
      J.join(ITERABLE_NULL.iterator());
      Assert.fail();
    } catch (NullPointerException expected) {
    }
    try {
      J.join(ITERABLE_1_NULL_2.iterator());
      Assert.fail();
    } catch (NullPointerException expected) {
    }
  }

  @Test(groups = "unit")
  public void testOnCharOverride() {
    Joiner onChar = Joiner.on('-');
    checkNoOutput(onChar, ITERABLE_);
    checkResult(onChar, ITERABLE_1, "1");
    checkResult(onChar, ITERABLE_12, "1-2");
    checkResult(onChar, ITERABLE_123, "1-2-3");
  }

  @Test(groups = "unit")
  public void testSkipNulls() {
    Joiner skipNulls = J.skipNulls();
    checkNoOutput(skipNulls, ITERABLE_);
    checkNoOutput(skipNulls, ITERABLE_NULL);
    checkNoOutput(skipNulls, ITERABLE_NULL_NULL);
    checkNoOutput(skipNulls, ITERABLE_FOUR_NULLS);
    checkResult(skipNulls, ITERABLE_1, "1");
    checkResult(skipNulls, ITERABLE_12, "1-2");
    checkResult(skipNulls, ITERABLE_123, "1-2-3");
    checkResult(skipNulls, ITERABLE_NULL_1, "1");
    checkResult(skipNulls, ITERABLE_1_NULL, "1");
    checkResult(skipNulls, ITERABLE_1_NULL_2, "1-2");
  }

  @Test(groups = "unit")
  public void testUseForNull() {
    Joiner zeroForNull = J.useForNull("0");
    checkNoOutput(zeroForNull, ITERABLE_);
    checkResult(zeroForNull, ITERABLE_1, "1");
    checkResult(zeroForNull, ITERABLE_12, "1-2");
    checkResult(zeroForNull, ITERABLE_123, "1-2-3");
    checkResult(zeroForNull, ITERABLE_NULL, "0");
    checkResult(zeroForNull, ITERABLE_NULL_NULL, "0-0");
    checkResult(zeroForNull, ITERABLE_NULL_1, "0-1");
    checkResult(zeroForNull, ITERABLE_1_NULL, "1-0");
    checkResult(zeroForNull, ITERABLE_1_NULL_2, "1-0-2");
    checkResult(zeroForNull, ITERABLE_FOUR_NULLS, "0-0-0-0");
  }

  private static void checkNoOutput(Joiner joiner, Iterable<Integer> set) {
    Assert.assertEquals(joiner.join(set), "");
    Assert.assertEquals(joiner.join(set.iterator()), "");

    Object[] array = CollectionUtil.stream(set).toArray(Integer[]::new);
    Assert.assertEquals(joiner.join(array), "");

    StringBuilder sb1FromIterable = new StringBuilder();
    Assert.assertSame(joiner.appendTo(sb1FromIterable, set), sb1FromIterable);
    Assert.assertEquals(sb1FromIterable.length(), 0);

    StringBuilder sb1FromIterator = new StringBuilder();
    Assert.assertSame(joiner.appendTo(sb1FromIterator, set), sb1FromIterator);
    Assert.assertEquals(sb1FromIterator.length(), 0);

    StringBuilder sb2 = new StringBuilder();
    Assert.assertSame(joiner.appendTo(sb2, array), sb2);
    Assert.assertEquals(sb2.length(), 0);

    try {
      joiner.appendTo(NASTY_APPENDABLE, set);
    } catch (IOException e) {
      throw new AssertionError(e);
    }

    try {
      joiner.appendTo(NASTY_APPENDABLE, set.iterator());
    } catch (IOException e) {
      throw new AssertionError(e);
    }

    try {
      joiner.appendTo(NASTY_APPENDABLE, array);
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  private static final Appendable NASTY_APPENDABLE = new Appendable() {
    @Override
    public Appendable append(CharSequence csq) throws IOException {
      throw new IOException();
    }

    @Override
    public Appendable append(CharSequence csq, int start, int end) throws IOException {
      throw new IOException();
    }

    @Override
    public Appendable append(char c) throws IOException {
      throw new IOException();
    }
  };

  private static void checkResult(Joiner joiner, Iterable<Integer> parts, String expected) {
    Assert.assertEquals(joiner.join(parts), expected);
    Assert.assertEquals(joiner.join(parts.iterator()), expected);

    StringBuilder sb1FromIterable = new StringBuilder().append('x');
    joiner.appendTo(sb1FromIterable, parts);
    Assert.assertEquals(sb1FromIterable.toString(), "x" + expected);

    StringBuilder sb1FromIterator = new StringBuilder().append('x');
    joiner.appendTo(sb1FromIterator, parts.iterator());
    Assert.assertEquals(sb1FromIterator.toString(), "x" + expected);

    Integer[] partsArray = CollectionUtil.stream(parts).toArray(Integer[]::new);
    Assert.assertEquals(joiner.join(partsArray), expected);

    StringBuilder sb2 = new StringBuilder().append('x');
    joiner.appendTo(sb2, partsArray);
    Assert.assertEquals(sb2.toString(), "x" + expected);

    int num = partsArray.length - 2;
    if (num >= 0) {
      Object[] rest = new Integer[num];
      for (int i = 0; i < num; i++) {
        rest[i] = partsArray[i + 2];
      }

      Assert.assertEquals(joiner.join(partsArray[0], partsArray[1], rest), expected);

      StringBuilder sb3 = new StringBuilder().append('x');
      joiner.appendTo(sb3, partsArray[0], partsArray[1], rest);
      Assert.assertEquals(sb3.toString(), "x" + expected);
    }
  }

  @Test(groups = "unit")
  public void test_useForNull_skipNulls() {
    Joiner j = Joiner.on("x").useForNull("y");
    try {
      j = j.skipNulls();
      Assert.fail();
    } catch (UnsupportedOperationException expected) {
    }
  }

  @Test(groups = "unit")
  public void test_skipNulls_useForNull() {
    Joiner j = Joiner.on("x").skipNulls();
    try {
      j = j.useForNull("y");
      Assert.fail();
    } catch (UnsupportedOperationException expected) {
    }
  }

  @Test(groups = "unit")
  public void test_useForNull_twice() {
    Joiner j = Joiner.on("x").useForNull("y");
    try {
      j = j.useForNull("y");
      Assert.fail();
    } catch (UnsupportedOperationException expected) {
    }
  }

  private static class DontStringMeBro implements CharSequence {
    @Override
    public int length() {
      return 3;
    }

    @Override
    public char charAt(int index) {
      return "foo".charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
      return "foo".subSequence(start, end);
    }

    @Override
    @Nonnull
    public String toString() {
      Assert.fail("shouldn't be invoked");
      return "Never gets here";
    }
  }

  @Test(groups = "unit") // StringBuilder.append in GWT invokes Object.toString(), unlike the JRE version.
  public void testDontConvertCharSequenceToString() {
    Assert.assertEquals(Joiner.on(",").join(new DontStringMeBro(), new DontStringMeBro()), "foo,foo");
    Assert.assertEquals(
        Joiner.on(",").useForNull("bar").join(new DontStringMeBro(), null, new DontStringMeBro()),
        "foo,bar,foo");
  }
}
