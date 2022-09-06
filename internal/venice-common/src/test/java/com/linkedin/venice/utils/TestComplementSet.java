package com.linkedin.venice.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * In this test, assume that all possible elements for the set is {0, 1, 2.... 9}
 */
public class TestComplementSet {
  private static final Set<Integer> universalSet = new HashSet<Integer>(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testAddAllForNonComplementSet(boolean addComplementSet) {
    ComplementSet<Integer> nonComplementSourceSet =
        ComplementSet.newSet(Collections.unmodifiableSet(new HashSet<Integer>(Arrays.asList(0, 1, 2, 3))));

    ComplementSet<Integer> newData;
    if (addComplementSet) {
      newData = ComplementSet.universalSet();
      newData.removeAll(ComplementSet.wrap(new HashSet<Integer>(Arrays.asList(0, 1, 2, 3, 6, 7, 8, 9))));
    } else {
      newData = ComplementSet.newSet(Collections.unmodifiableSet(new HashSet<Integer>(Arrays.asList(4, 5))));
    }

    // Add newData to non-complement set
    nonComplementSourceSet.addAll(newData);

    for (int i = 0; i <= 5; i++) {
      Assert.assertTrue(nonComplementSourceSet.contains(i));
    }

    for (int i = 6; i <= 9; i++) {
      Assert.assertFalse(nonComplementSourceSet.contains(i));
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testAddAllForComplementSet(boolean addComplementSet) {
    ComplementSet<Integer> complementSourceSet = ComplementSet.universalSet();
    complementSourceSet.removeAll(ComplementSet.wrap(new HashSet<Integer>(Arrays.asList(4, 5, 6, 7, 8, 9))));

    ComplementSet<Integer> newData;
    if (addComplementSet) {
      newData = ComplementSet.universalSet();
      newData.removeAll(ComplementSet.wrap(new HashSet<Integer>(Arrays.asList(0, 1, 2, 3, 6, 7, 8, 9))));
    } else {
      newData = ComplementSet.newSet(Collections.unmodifiableSet(new HashSet<Integer>(Arrays.asList(4, 5))));
    }

    // Add newData to non-complement set
    complementSourceSet.addAll(newData);

    for (int i = 0; i <= 5; i++) {
      Assert.assertTrue(complementSourceSet.contains(i));
    }

    for (int i = 6; i <= 9; i++) {
      Assert.assertFalse(complementSourceSet.contains(i));
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testRemoveAllForNonComplementSet(boolean removeComplementSet) {
    ComplementSet<Integer> nonComplementSourceSet =
        ComplementSet.newSet(Collections.unmodifiableSet(new HashSet<Integer>(Arrays.asList(0, 1, 2, 3, 4, 5))));

    ComplementSet<Integer> removedData;
    if (removeComplementSet) {
      removedData = ComplementSet.universalSet();
      removedData.removeAll(ComplementSet.wrap(new HashSet<Integer>(Arrays.asList(0, 1, 2, 3, 6, 7, 8, 9))));
    } else {
      removedData = ComplementSet.newSet(Collections.unmodifiableSet(new HashSet<Integer>(Arrays.asList(4, 5))));
    }

    // Add newData to non-complement set
    nonComplementSourceSet.removeAll(removedData);

    for (int i = 0; i <= 3; i++) {
      Assert.assertTrue(nonComplementSourceSet.contains(i));
    }

    for (int i = 4; i <= 9; i++) {
      Assert.assertFalse(nonComplementSourceSet.contains(i));
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testRemoveAllForComplementSet(boolean removeComplementSet) {
    ComplementSet<Integer> complementSourceSet = ComplementSet.universalSet();
    complementSourceSet.removeAll(ComplementSet.wrap(new HashSet<Integer>(Arrays.asList(6, 7, 8, 9))));

    ComplementSet<Integer> removedData;
    if (removeComplementSet) {
      removedData = ComplementSet.universalSet();
      removedData.removeAll(ComplementSet.wrap(new HashSet<Integer>(Arrays.asList(0, 1, 2, 3, 6, 7, 8, 9))));
    } else {
      removedData = ComplementSet.newSet(Collections.unmodifiableSet(new HashSet<Integer>(Arrays.asList(4, 5))));
    }

    // Add newData to non-complement set
    complementSourceSet.removeAll(removedData);

    for (int i = 0; i <= 3; i++) {
      Assert.assertTrue(complementSourceSet.contains(i));
    }

    for (int i = 4; i <= 9; i++) {
      Assert.assertFalse(complementSourceSet.contains(i));
    }
  }
}
