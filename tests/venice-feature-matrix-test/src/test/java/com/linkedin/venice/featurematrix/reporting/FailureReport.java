package com.linkedin.venice.featurematrix.reporting;

import com.linkedin.venice.featurematrix.model.FeatureDimensions.DimensionId;
import java.util.Map;


/**
 * Captures per-failure data for the feature matrix report.
 * Each instance represents one failed test invocation with its full
 * 42-dimensional context, the validation step that failed, the likely
 * component, and the exception.
 */
public class FailureReport {
  private final int testCaseId;
  private final Map<DimensionId, String> dimensions;
  private final String validationStep;
  private final String likelyComponent;
  private final String errorMessage;
  private final String stackTrace;
  private final long durationMs;

  public FailureReport(
      int testCaseId,
      Map<DimensionId, String> dimensions,
      String validationStep,
      String likelyComponent,
      String errorMessage,
      String stackTrace,
      long durationMs) {
    this.testCaseId = testCaseId;
    this.dimensions = dimensions;
    this.validationStep = validationStep;
    this.likelyComponent = likelyComponent;
    this.errorMessage = errorMessage;
    this.stackTrace = stackTrace;
    this.durationMs = durationMs;
  }

  public int getTestCaseId() {
    return testCaseId;
  }

  public Map<DimensionId, String> getDimensions() {
    return dimensions;
  }

  public String getValidationStep() {
    return validationStep;
  }

  public String getLikelyComponent() {
    return likelyComponent;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public String getStackTrace() {
    return stackTrace;
  }

  public long getDurationMs() {
    return durationMs;
  }

  /**
   * Classifies the likely component based on the validation step that failed.
   */
  public static String classifyComponent(String validationStep) {
    if (validationStep == null) {
      return "Unknown";
    }
    switch (validationStep) {
      case "store_creation":
        return "Controller";
      case "batch_push":
        return "PushJob/Server";
      case "streaming_write":
      case "rt_ingestion":
        return "Server (ingestion)";
      case "incremental_push":
        return "Server/Controller";
      case "single_get_null":
        return "Server/Router";
      case "single_get_wrong_value":
        return "Server (data corruption)";
      case "batch_get_missing_keys":
        return "Router (scatter-gather)";
      case "read_compute":
        return "Server (compute engine)";
      case "write_compute":
        return "Server (AA/WC merge)";
      case "decompression":
        return "Router/Client";
      case "chunk_reassembly":
        return "Client/Server";
      default:
        return "Unknown";
    }
  }
}
