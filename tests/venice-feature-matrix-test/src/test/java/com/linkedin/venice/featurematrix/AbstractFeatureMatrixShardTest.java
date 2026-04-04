package com.linkedin.venice.featurematrix;

import com.linkedin.venice.featurematrix.model.TestCaseConfig;
import com.linkedin.venice.featurematrix.reporting.FeatureMatrixReportListener;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;


/**
 * Base class for sharded feature matrix tests.
 *
 * Each generated shard subclass provides a list of {@link TestCaseConfig}s.
 * TCs sharing the same cluster config key are run together on one cluster,
 * avoiding redundant cluster setup/teardown within a shard.
 *
 * Test execution delegated to {@link TestCaseRunner#runTestCase}.
 */
@Listeners(FeatureMatrixReportListener.class)
@Test
public abstract class AbstractFeatureMatrixShardTest {
  private static final Logger LOGGER = LogManager.getLogger(AbstractFeatureMatrixShardTest.class);

  public abstract List<TestCaseConfig> getConfigs();

  @Test(timeOut = 900_000)
  public void runShard() throws Exception {
    List<TestCaseConfig> configs = getConfigs();
    LOGGER.info("=== Starting shard with {} test cases ===", configs.size());

    // Group TCs by cluster config key to reuse clusters
    Map<String, List<TestCaseConfig>> groups = new LinkedHashMap<>();
    for (TestCaseConfig config: configs) {
      groups.computeIfAbsent(config.getClusterConfigKey(), k -> new ArrayList<>()).add(config);
    }

    LOGGER.info("Grouped into {} cluster configs", groups.size());

    int passCount = 0;
    List<String> failures = new ArrayList<>();

    for (Map.Entry<String, List<TestCaseConfig>> group: groups.entrySet()) {
      List<TestCaseConfig> groupConfigs = group.getValue();
      TestCaseConfig representative = groupConfigs.get(0);

      LOGGER
          .info("Setting up cluster for {} TCs (config key hash: {})", groupConfigs.size(), group.getKey().hashCode());

      FeatureMatrixClusterSetup clusterSetup = new FeatureMatrixClusterSetup(representative);
      try {
        clusterSetup.create();

        for (TestCaseConfig config: groupConfigs) {
          try {
            TestCaseRunner.runTestCase(config, clusterSetup);
            passCount++;
            LOGGER.info("=== TC{} PASSED ===", config.getTestCaseId());
          } catch (Exception e) {
            String msg = "TC" + config.getTestCaseId() + ": " + e.getMessage();
            failures.add(msg);
            LOGGER.error("=== TC{} FAILED: {} ===", config.getTestCaseId(), e.getMessage(), e);
          }
        }
      } finally {
        clusterSetup.tearDown();
      }
    }

    LOGGER.info("=== Shard complete: {} passed, {} failed ===", passCount, failures.size());
    if (!failures.isEmpty()) {
      Assert.fail("Shard had " + failures.size() + " failure(s):\n" + String.join("\n", failures));
    }
  }
}
