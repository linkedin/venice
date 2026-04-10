package com.linkedin.venice.featurematrix.reporting;

import com.linkedin.venice.featurematrix.AbstractFeatureMatrixTest;
import com.linkedin.venice.featurematrix.model.TestCaseConfig;
import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;


/**
 * TestNG listener that captures 42-dimensional context for each test failure
 * and generates aggregated reports at the end of the test suite.
 *
 * Register in TestNG via:
 * - build.gradle: listeners << 'com.linkedin.venice.featurematrix.reporting.FeatureMatrixReportListener'
 * - @Listeners annotation on the test class
 */
public class FeatureMatrixReportListener implements ITestListener {
  private static final Logger LOGGER = LogManager.getLogger(FeatureMatrixReportListener.class);
  private static final String REPORT_DIR_PROPERTY = "feature.matrix.report.dir";

  private final FeatureMatrixReportAggregator aggregator = new FeatureMatrixReportAggregator();

  @Override
  public void onTestFailure(ITestResult result) {
    TestCaseConfig config = extractTestCaseConfig(result);
    if (config == null) {
      LOGGER.warn("Could not extract TestCaseConfig from test result");
      return;
    }

    String validationStep = inferValidationStep(result);
    String component = FailureReport.classifyComponent(validationStep);

    StringWriter sw = new StringWriter();
    if (result.getThrowable() != null) {
      result.getThrowable().printStackTrace(new PrintWriter(sw));
    }

    FailureReport failure = new FailureReport(
        config.getTestCaseId(),
        config.getDimensions(),
        validationStep,
        component,
        result.getThrowable() != null ? result.getThrowable().getMessage() : "Unknown error",
        sw.toString(),
        result.getEndMillis() - result.getStartMillis());

    aggregator.addFailure(failure);
    LOGGER.error(
        "Test FAILED: TC{} step={} component={} error={}",
        config.getTestCaseId(),
        validationStep,
        component,
        result.getThrowable() != null ? result.getThrowable().getMessage() : "Unknown");
  }

  @Override
  public void onFinish(ITestContext context) {
    int total = context.getAllTestMethods().length;
    int passed = context.getPassedTests().size();
    int failed = context.getFailedTests().size();
    int skipped = context.getSkippedTests().size();

    aggregator.recordCounts(total, passed, failed, skipped);

    String reportDirPath = System.getProperty(REPORT_DIR_PROPERTY, "build/reports");
    File reportDir = new File(reportDirPath);

    try {
      aggregator.generateHtmlReport(reportDir);
      aggregator.generateJsonReport(reportDir);
      LOGGER.info("Feature matrix reports generated in {}", reportDir.getAbsolutePath());
    } catch (Exception e) {
      LOGGER.error("Failed to generate feature matrix reports", e);
    }
  }

  @Override
  public void onTestStart(ITestResult result) {
    TestCaseConfig config = extractTestCaseConfig(result);
    if (config != null) {
      LOGGER.info("Starting test case TC{}", config.getTestCaseId());
    }
  }

  @Override
  public void onTestSuccess(ITestResult result) {
    // Success is tracked by TestNG context, not individually here
  }

  @Override
  public void onTestSkipped(ITestResult result) {
    // Skips are tracked by TestNG context
  }

  @Override
  public void onStart(ITestContext context) {
    LOGGER.info("Feature matrix test suite starting: {}", context.getName());
  }

  @Override
  public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
    // No-op for feature matrix tests
  }

  /**
   * Extracts TestCaseConfig from test method parameters or from the test instance
   * (for generated AbstractFeatureMatrixTest subclasses that have no parameters).
   */
  private TestCaseConfig extractTestCaseConfig(Object[] parameters) {
    if (parameters != null) {
      for (Object param: parameters) {
        if (param instanceof TestCaseConfig) {
          return (TestCaseConfig) param;
        }
      }
    }
    return null;
  }

  /**
   * Extracts TestCaseConfig from an ITestResult — checks parameters first,
   * then falls back to the test instance (for AbstractFeatureMatrixTest subclasses).
   */
  private TestCaseConfig extractTestCaseConfig(ITestResult result) {
    TestCaseConfig config = extractTestCaseConfig(result.getParameters());
    if (config != null) {
      return config;
    }
    Object instance = result.getInstance();
    if (instance instanceof AbstractFeatureMatrixTest) {
      return ((AbstractFeatureMatrixTest) instance).getConfig();
    }
    return null;
  }

  /**
   * Infers the validation step from the exception/assertion message.
   */
  private String inferValidationStep(ITestResult result) {
    Throwable t = result.getThrowable();
    if (t == null) {
      return "unknown";
    }

    String message = t.getMessage();
    if (message == null) {
      message = "";
    }
    String lowerMessage = message.toLowerCase();

    if (lowerMessage.contains("store creation") || lowerMessage.contains("create store")) {
      return "store_creation";
    } else if (lowerMessage.contains("batch push") || lowerMessage.contains("push job")) {
      return "batch_push";
    } else if (lowerMessage.contains("streaming") || lowerMessage.contains("rt write")) {
      return "streaming_write";
    } else if (lowerMessage.contains("incremental push")) {
      return "incremental_push";
    } else if (lowerMessage.contains("single get") && lowerMessage.contains("null")) {
      return "single_get_null";
    } else if (lowerMessage.contains("single get") && lowerMessage.contains("wrong")) {
      return "single_get_wrong_value";
    } else if (lowerMessage.contains("batch get") && lowerMessage.contains("missing")) {
      return "batch_get_missing_keys";
    } else if (lowerMessage.contains("read compute")) {
      return "read_compute";
    } else if (lowerMessage.contains("write compute") || lowerMessage.contains("partial update")) {
      return "write_compute";
    } else if (lowerMessage.contains("decompress")) {
      return "decompression";
    } else if (lowerMessage.contains("chunk")) {
      return "chunk_reassembly";
    }

    return "unknown";
  }
}
