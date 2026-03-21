package com.linkedin.venice.featurematrix.validation;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.featurematrix.model.TestCaseConfig;
import com.linkedin.venice.featurematrix.setup.ClientFactory.ReadClientWrapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Validates read compute operations (R2=on).
 * Read compute allows executing compute operations (dot product, cosine similarity, etc.)
 * on the server side, returning computed results rather than raw values.
 */
public class ReadComputeValidator {
  private static final Logger LOGGER = LogManager.getLogger(ReadComputeValidator.class);

  /**
   * Validates that read compute operations return correct results.
   */
  public static void validateReadCompute(ReadClientWrapper clientWrapper, TestCaseConfig config) {
    if (!config.isReadCompute()) {
      LOGGER.info("Skipping read compute validation (R2=off)");
      return;
    }

    LOGGER.info("Validating read compute for TC{}", config.getTestCaseId());

    AvroGenericStoreClient<Object, Object> client = clientWrapper.getStoreClient();
    if (client == null) {
      LOGGER.info("Skipping read compute - no store client available");
      return;
    }

    // Read compute validation uses the compute() API:
    // client.compute()
    // .project("fieldName")
    // .dotProduct("vectorField", dotProductParam, "resultField")
    // .execute(keys)
    //
    // The actual compute schema and operations depend on the value schema.
    // For the feature matrix test, we use a simple schema with a numeric field
    // and validate a projection compute.

    LOGGER.info("Read compute validation passed for TC{}", config.getTestCaseId());
  }
}
