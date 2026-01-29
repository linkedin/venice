package com.linkedin.venice.integration.utils;

import org.apache.logging.log4j.Logger;


/**
 * Utility class for timing operations with consistent start/end logging.
 * This eliminates the repeated pattern of logging start time, end time, and duration
 * across Venice integration test wrapper classes.
 */
public class TimingUtils {
  /**
   * Times a Runnable operation with start and completion logging.
   *
   * @param logger the logger to use for timing messages
   * @param operationDescription description of the operation being timed (e.g., "Step 1: Shutting down routers")
   * @param operation the operation to execute and time
   * @return the duration in milliseconds
   */
  public static long timeOperation(Logger logger, String operationDescription, Runnable operation) {
    long startTime = System.currentTimeMillis();
    logger.info(operationDescription);

    try {
      operation.run();
    } finally {
      long duration = System.currentTimeMillis() - startTime;
      logger.info("Completed {} in {} ms", getCompletionDescription(operationDescription), duration);
    }

    return System.currentTimeMillis() - startTime;
  }

  /**
   * Times a Runnable operation with start and completion logging, returning the duration.
   *
   * @param logger the logger to use for timing messages
   * @param operationDescription description of the operation being timed (e.g., "Step 1: Shutting down routers")
   * @param operation the operation to execute and time
   * @return the duration in milliseconds
   */
  public static long timeOperationAndReturnDuration(Logger logger, String operationDescription, Runnable operation) {
    long startTime = System.currentTimeMillis();
    logger.info(operationDescription);

    try {
      operation.run();
    } finally {
      long duration = System.currentTimeMillis() - startTime;
      logger.info("Completed {} in {} ms", getCompletionDescription(operationDescription), duration);
    }

    return System.currentTimeMillis() - startTime;
  }

  /**
   * Converts an operation description to a completion description.
   * Examples:
   * - "Step 1: Shutting down routers" -> "shutdown of routers"
   * - "Shutting down 5 controllers in parallel" -> "shutdown of 5 controllers in parallel"
   * - "Step 3: Starting service" -> "startup of service"
   */
  private static String getCompletionDescription(String operationDescription) {
    String description = operationDescription;

    // Remove step prefixes like "Step 1: ", "Step 2: ", etc.
    description = description.replaceFirst("^Step \\d+: ", "");

    // Convert common operation verbs to completion forms
    description = description.replaceFirst("^Shutting down", "shutdown of");
    description = description.replaceFirst("^Starting", "startup of");
    description = description.replaceFirst("^Stopping", "shutdown of");
    description = description.replaceFirst("^Destroying", "destruction of");
    description = description.replaceFirst("^Creating", "creation of");
    description = description.replaceFirst("^Initializing", "initialization of");

    return description;
  }
}
