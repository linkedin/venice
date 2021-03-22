package com.linkedin.venice.utils;

import org.testng.annotations.DataProvider;


/**
 * This class gathers all common data provider patterns in test cases. In order to leverage this util class,
 * make sure your test case has "test" dependency on "venice-test-common" module.
 */
public class DataProviderUtils {

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

  @DataProvider(name = "Two-True-and-False", parallel = false)
  public static Object[][] twoTrueAndFalseProvider() {
    return new Object[][]{
        {false, false},
        {false, true},
        {true, false},
        {true, true}
    };
  }
}
