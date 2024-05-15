package com.linkedin.venice.testng;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.ITestResult;
import org.testng.TestListenerAdapter;


public class VeniceTestListener extends TestListenerAdapter {
  private static final Logger LOGGER = LogManager.getLogger(VeniceTestListener.class);

  @Override
  public void onTestStart(ITestResult result) {
    super.onTestStart(result);

    /** N.B.: {@link Thread#interrupted()} not only retrieves, but also clears, the interrupt flag */
    boolean interruptCleared = Thread.interrupted();
    LOGGER.info(
        "\n\n######## TEST ######## {}({}){} - STARTED",
        result.getMethod().getQualifiedName(),
        result.getParameters(),
        interruptCleared ? " (interrupt cleared)" : "");
  }

  @Override
  public void onTestSuccess(ITestResult result) {
    super.onTestSuccess(result);
    LOGGER.info(
        "######## TEST ######## {}({}) - PASSED\n\n",
        result.getMethod().getQualifiedName(),
        result.getParameters());
  }

  @Override
  public void onTestFailure(ITestResult result) {
    super.onTestFailure(result);
    LOGGER.info(
        "######## TEST ######## {}({}) - FAILED\n\n",
        result.getMethod().getQualifiedName(),
        result.getParameters());
  }

  @Override
  public void onTestSkipped(ITestResult result) {
    super.onTestSkipped(result);
    LOGGER.info(
        "######## TEST ######## {}({}) - SKIPPED\n\n",
        result.getMethod().getQualifiedName(),
        result.getParameters());
  }

  @Override
  public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
    super.onTestFailedButWithinSuccessPercentage(result);
    LOGGER.info(
        "######## TEST ######## {}({}) - FLAKY\n\n",
        result.getMethod().getQualifiedName(),
        result.getParameters());
  }
}
