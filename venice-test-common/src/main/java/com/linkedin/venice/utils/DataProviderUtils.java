package com.linkedin.venice.utils;

import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.store.cache.backend.ObjectCacheConfig;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.ArrayUtils;
import org.testng.annotations.DataProvider;
import org.testng.collections.Lists;


/**
 * This class gathers all common data provider patterns in test cases. In order to leverage this util class,
 * make sure your test case has "test" dependency on "venice-test-common" module.
 */
public class DataProviderUtils {

  /**
   * To use this data provider, add (dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class) into
   * @test annotation.
   */
  @DataProvider(name = "Two-True-and-False")
  public static Object[][] twoTrueAndFalseProvider() {
    return new Object[][]{
            {false, false},
            {false, true},
            {true, false},
            {true, true}
    };
  }

  /**
   * To use this data provider, add (dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class) into
   * @test annotation.
   */
  @DataProvider(name = "True-and-False")
  public static Object[][] trueAndFalseProvider() {
    return new Object[][] {
            {false}, {true}
    };
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
}
