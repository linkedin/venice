package com.linkedin.venice.testng;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.ITestResult;
import org.testng.TestListenerAdapter;


public class VeniceTestListener extends TestListenerAdapter {
  private static final Logger LOGGER = LogManager.getLogger(VeniceTestListener.class);

  @Override
  public void onTestStart(ITestResult result) {
    LOGGER.info(
        "\n\n######## TEST ######## {}({}) - STARTED",
        result.getMethod().getQualifiedName(),
        result.getParameters());
  }

  @Override
  public void onTestSuccess(ITestResult result) {
    LOGGER.info(
        "######## TEST ######## {}({}) - PASSED\n\n",
        result.getMethod().getQualifiedName(),
        result.getParameters());
  }

  @Override
  public void onTestFailure(ITestResult result) {
    LOGGER.info(
        "######## TEST ######## {}({}) - FAILED\n\n",
        result.getMethod().getQualifiedName(),
        result.getParameters());
  }

  @Override
  public void onTestSkipped(ITestResult result) {
    LOGGER.info(
        "######## TEST ######## {}({}) - SKIPPED\n\n",
        result.getMethod().getQualifiedName(),
        result.getParameters());
  }

  @Override
  public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
    LOGGER.info(
        "######## TEST ######## {}({}) - FLAKY\n\n",
        result.getMethod().getQualifiedName(),
        result.getParameters());
  }
}
