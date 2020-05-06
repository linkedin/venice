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

}
