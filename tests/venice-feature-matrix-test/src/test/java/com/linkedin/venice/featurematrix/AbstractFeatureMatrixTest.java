package com.linkedin.venice.featurematrix;

import com.linkedin.venice.featurematrix.model.TestCaseConfig;
import com.linkedin.venice.featurematrix.reporting.FeatureMatrixReportListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;


/**
 * Base class for generated per-TC feature matrix tests.
 *
 * Each generated subclass overrides {@link #getConfig()} to return its specific
 * {@link TestCaseConfig}. Gradle distributes subclasses across JVMs via
 * {@code forkEvery=1} + {@code maxParallelForks=4}.
 *
 * Test flow delegated to {@link TestCaseRunner#runTestCase}.
 */
@Listeners(FeatureMatrixReportListener.class)
@Test
public abstract class AbstractFeatureMatrixTest {
  private static final Logger LOGGER = LogManager.getLogger(AbstractFeatureMatrixTest.class);

  private FeatureMatrixClusterSetup clusterSetup;

  public abstract TestCaseConfig getConfig();

  @BeforeClass
  public void setupCluster() {
    TestCaseConfig config = getConfig();
    LOGGER.info("Setting up multi-region cluster for TC{}", config.getTestCaseId());
    clusterSetup = new FeatureMatrixClusterSetup(config);
    clusterSetup.create();
    LOGGER.info("Multi-region cluster setup complete for TC{}", config.getTestCaseId());
  }

  @Test(timeOut = 300_000)
  public void testFeatureCombination() throws Exception {
    TestCaseRunner.runTestCase(getConfig(), clusterSetup);
  }

  @AfterClass(alwaysRun = true)
  public void tearDownCluster() {
    TestCaseConfig config = getConfig();
    LOGGER.info("Tearing down cluster for TC{}", config.getTestCaseId());
    if (clusterSetup != null) {
      clusterSetup.tearDown();
    }
  }
}
