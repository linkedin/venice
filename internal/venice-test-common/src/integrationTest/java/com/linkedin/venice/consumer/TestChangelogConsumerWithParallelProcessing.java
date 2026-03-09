package com.linkedin.venice.consumer;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.Test;


/**
 * Runs a subset of changelog consumer tests with AA/WC parallel processing enabled on the server.
 * Only tests that exercise the AA streaming write path are included — consumer-only tests
 * (schema evolution, sequence IDs, store enable/disable) are disabled since the parallel processing
 * flag does not affect their code path.
 */
@Test(singleThreaded = true)
public class TestChangelogConsumerWithParallelProcessing extends TestChangelogConsumer {
  @Override
  protected boolean isAAWCParallelProcessingEnabled() {
    return true;
  }

  // Tests below are disabled because they only exercise consumer-side logic
  // and are not affected by SERVER_AA_WC_WORKLOAD_PARALLEL_PROCESSING_ENABLED.

  @Override
  @Test(enabled = false)
  public void testDisabledStoreVeniceChangelogConsumer() throws Exception {
  }

  @Override
  @Test(enabled = false)
  public void testChangelogConsumerWithNewValueSchema() throws IOException, ExecutionException, InterruptedException {
  }

  @Override
  @Test(enabled = false)
  public void testNewChangelogConsumerWithNewValueSchema()
      throws IOException, ExecutionException, InterruptedException {
  }

  @Override
  @Test(enabled = false)
  public void testChangeLogConsumerSequenceId() throws IOException, ExecutionException, InterruptedException {
  }
}
