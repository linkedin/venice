package com.linkedin.venice.utils;

import java.util.Arrays;
import java.util.Optional;
import org.testng.IRetryAnalyzer;
import org.testng.ITestResult;

/**
 * Unfortunately, some of our tests are non-deterministic which makes them flaky.
 *
 * This TestNG class can be leveraged to make the framework retry those tests a
 * few times before giving up.
 *
 * It should be our goal to never use this class, and to work towards making our
 * tests deterministic enough to stop needing it. That being said, in the interest
 * of pragmatism, it is there to be used with parsimony.
 */
public class FlakyTestRetryAnalyzer implements IRetryAnalyzer  {
  private static final int MAX_ATTEMPTS = 3;

  private int currentAttempt = 1;

  /** retries are granted for each parameters */
  private Optional<Object[]> parameters = Optional.empty();

  /**
   * Currently, this class tries {@value #MAX_ATTEMPTS} times in total.
   *
   * In the future, we can leverage the {@link ITestResult} properties in order to
   * put some safeguards on retries. For example, bounding the total time that a
   * test can run, no matter the amount of retries.
   */
  @Override
  public boolean retry(ITestResult result) {
    Object[] incomingParameters = result.getParameters();
    if (parameters.isPresent()) {
       if (!Arrays.equals(incomingParameters, parameters.get())) {
         parameters = Optional.of(incomingParameters);
         currentAttempt = 1;
      }
    } else {
      parameters = Optional.of(incomingParameters);
    }
    if(currentAttempt < MAX_ATTEMPTS) {
      currentAttempt++;
      return true;
    }
    return false;
  }
}