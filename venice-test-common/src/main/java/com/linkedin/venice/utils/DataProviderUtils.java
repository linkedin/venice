package com.linkedin.venice.utils;

import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.store.cache.backend.ObjectCacheConfig;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang.ArrayUtils;
import org.testng.annotations.DataProvider;
import org.testng.collections.Lists;


/**
 * This class gathers all common data provider patterns in test cases. In order to leverage this util class,
 * make sure your test case has "test" dependency on "venice-test-common" module.
 */
public class DataProviderUtils {

  public static final Object[] BOOLEAN = {false, true};

  /**
   * To use these data providers, add (dataProvider = "<provider_name>", dataProviderClass = DataProviderUtils.class)
   * into the @Test annotation.
   */

  @DataProvider(name = "True-and-False")
  public static Object[][] trueAndFalseProvider() {
    return new Object[][] {
        {false}, {true}
    };
  }

  @DataProvider(name = "Two-True-and-False")
  public static Object[][] twoBoolean() {
    return allPermutationGenerator(BOOLEAN, BOOLEAN);
  }

  @DataProvider(name = "Three-True-and-False")
  public static Object[][] threeBoolean() {
    return DataProviderUtils.allPermutationGenerator(DataProviderUtils.BOOLEAN, DataProviderUtils.BOOLEAN, DataProviderUtils.BOOLEAN);
  }

  @DataProvider(name = "Five-True-and-False")
  public static Object[][] fiveBoolean() {
    return DataProviderUtils.allPermutationGenerator(DataProviderUtils.BOOLEAN, DataProviderUtils.BOOLEAN, DataProviderUtils.BOOLEAN, DataProviderUtils.BOOLEAN, DataProviderUtils.BOOLEAN);
  }

  @DataProvider(name = "L/F-and-AmplificationFactor", parallel = false)
  public static Object[][] testLeaderFollowerAndAmplificationFactor() {
    return new Object[][]{
        {false, false},
        {true, true},
        {true, false}
    };
  }

  @DataProvider (name = "dv-client-config-provider")
  public static Object[][] daVinciConfigProvider() {
    DaVinciConfig defaultDaVinciConfig = new DaVinciConfig();

    DaVinciConfig cachingDaVinciConfig = new DaVinciConfig();
    cachingDaVinciConfig.enableHeapObjectCacheEnabled(true);
    cachingDaVinciConfig.setCacheConfig(new ObjectCacheConfig());

    return new Object[][] {{defaultDaVinciConfig}, {cachingDaVinciConfig}};
  }

  @DataProvider(name = "L/F-and-AmplificationFactor-and-ObjectCache", parallel = false)
  public static Object[][] lFAndAmplificationFactorAndObjectCacheConfigProvider() {
    List<Object[]> ampFactorCases = Lists.newArrayList();
    ampFactorCases.addAll(Arrays.asList(testLeaderFollowerAndAmplificationFactor()));
    List<Object[]> configCases = Lists.newArrayList();
    configCases.addAll(Arrays.asList(daVinciConfigProvider()));
    List<Object[]> resultingArray = Lists.newArrayList();

    for(Object[] ampFactorCase : ampFactorCases) {
      for(Object[] configCase : configCases) {
        resultingArray.add(ArrayUtils.addAll(ampFactorCase, configCase));
      }
    }

    return resultingArray.toArray(new Object[resultingArray.size()][]);
  }

  /**
   * Generate permutations to be fed to a DataProvider.
   * For two boolean's we'd pass in allPermutationGenerator(BOOLEAN, BOOLEAN)
   * @param parameterSets Sets of valid values for each parameter
   * @return the permutations that can be returned from a {@link DataProvider}
   */
  public static Object[][] allPermutationGenerator(Object[]... parameterSets) {
    PermutationIterator permutationIterator = new PermutationIterator(parameterSets);
    int totalPermutations = permutationIterator.size();
    Object[][] permutations = new Object[totalPermutations][];
    int i = 0;
    while (permutationIterator.hasNext()) {
      permutations[i] = permutationIterator.next();
      i++;
    }
    return permutations;
  }

  private static class PermutationIterator implements Iterator<Object[]> {
    private int totalPermutations;
    private Object[][] parameterSets;
    private int[] markers;
    private boolean valueRead = false;
    private boolean hasNext;

    public PermutationIterator(Object[]... parameterSets) {
      this.parameterSets = parameterSets;
      this.markers = new int[parameterSets.length];
      totalPermutations = 1;
      hasNext = true;
      for (int i = 0; i < parameterSets.length; i++) {
        markers[i] = 0;
        if (parameterSets[i] == null || parameterSets[i].length == 0) {
          throw new IllegalArgumentException("Argument type cannot be null or empty");
        }
        totalPermutations *= parameterSets[i].length;
      }
    }

    @Override
    public boolean hasNext() {
      if (!valueRead) {
        return hasNext;
      }

      int i = 0;
      for (; i < markers.length; i++) {
        if (markers[i] < parameterSets[i].length - 1) {
          markers[i]++;
          valueRead = false;
          break;
        } else {
          markers[i] = 0;
        }
      }

      hasNext = i != markers.length;
      return hasNext;
    }

    @Override
    public Object[] next() {
      valueRead = true;
      Object[] permutation = new Object[parameterSets.length];
      for (int i = 0; i < parameterSets.length; i++) {
        permutation[i] = parameterSets[i][markers[i]];
      }
      return permutation;
    }

    public int size() {
      return totalPermutations;
    }
  }
}
