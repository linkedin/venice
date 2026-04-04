package com.linkedin.venice.testng;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.IRetryAnalyzer;
import org.testng.ITestResult;


/**
 * Per-invocation retry analyzer for TestNG. When a test method with a data provider fails,
 * only the specific failed parameter combination is retried — not all combinations.
 *
 * <p>This is more precise than the Gradle test-retry plugin, which retries at the method level
 * and may re-run all data provider iterations when only one fails.
 *
 * <p>Each test invocation gets its own instance (TestNG creates a new IRetryAnalyzer per invocation),
 * so the retry count is per-invocation, not shared.
 */
public class VeniceTestRetryAnalyzer implements IRetryAnalyzer {
  private static final Logger LOGGER = LogManager.getLogger(VeniceTestRetryAnalyzer.class);
  private static final int MAX_RETRIES = 2;
  private final AtomicInteger retryCount = new AtomicInteger(0);

  @Override
  public boolean retry(ITestResult result) {
    int attempt = retryCount.incrementAndGet();
    if (attempt <= MAX_RETRIES) {
      LOGGER.info(
          "Retrying {}({}) — attempt {}/{}",
          result.getMethod().getQualifiedName(),
          result.getParameters(),
          attempt,
          MAX_RETRIES);
      return true;
    }
    return false;
  }
}
