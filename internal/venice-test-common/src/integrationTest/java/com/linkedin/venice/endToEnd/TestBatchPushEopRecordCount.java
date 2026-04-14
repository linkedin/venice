package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.DEFAULT_USER_DATA_RECORD_COUNT;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SEND_CONTROL_MESSAGES_DIRECTLY;

import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.utils.KeyAndValueSchemas;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Verifies that per-partition record counts are correctly tracked and passed to broadcastEndOfPush.
 *
 * This test runs a batch push with SEND_CONTROL_MESSAGES_DIRECTLY=true and verifies:
 * 1. The push completes successfully (records are readable)
 * 2. Per-partition record counts sum to the total records pushed
 */
@Test(singleThreaded = true)
public class TestBatchPushEopRecordCount {
  private static final int TEST_TIMEOUT = 120 * Time.MS_PER_SECOND;
  private VeniceClusterWrapper veniceCluster;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    VeniceClusterCreateOptions options =
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1).numberOfServers(1).numberOfRouters(1).build();
    veniceCluster = ServiceFactory.getVeniceCluster(options);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testPerPartitionRecordCountsMatchTotalPushed() throws Exception {
    File inputDir = getTempDataDirectory();
    KeyAndValueSchemas schemas = new KeyAndValueSchemas(writeSimpleAvroFileWithStringToStringSchema(inputDir));
    String storeName = Utils.getUniqueString("eop-record-count-store");
    String inputDirPath = "file://" + inputDir.getAbsolutePath();

    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);
    props.setProperty(SEND_CONTROL_MESSAGES_DIRECTLY, "true");

    createStoreForJob(
        veniceCluster.getClusterName(),
        schemas.getKey().toString(),
        schemas.getValue().toString(),
        props,
        new UpdateStoreQueryParams()).close();

    // Run push job and capture per-partition counts via a subclass that exposes them
    VerifiableVenicePushJob pushJob = new VerifiableVenicePushJob("test-job", props);
    pushJob.run();
    Map<Integer, Long> perPartitionCounts = pushJob.getCapturedPerPartitionRecordCounts();

    // Verify per-partition counts are populated
    Assert.assertNotNull(perPartitionCounts, "Per-partition record counts should not be null");
    Assert.assertFalse(perPartitionCounts.isEmpty(), "Per-partition record counts should not be empty");

    // Verify sum matches total records
    long totalFromPartitions = perPartitionCounts.values().stream().mapToLong(Long::longValue).sum();
    Assert.assertEquals(
        totalFromPartitions,
        DEFAULT_USER_DATA_RECORD_COUNT,
        "Sum of per-partition record counts (" + perPartitionCounts + ") should equal total records pushed ("
            + DEFAULT_USER_DATA_RECORD_COUNT + ")");

    // Verify each partition has a non-negative count
    for (Map.Entry<Integer, Long> entry: perPartitionCounts.entrySet()) {
      Assert.assertTrue(
          entry.getValue() >= 0,
          "Partition " + entry.getKey() + " should have non-negative count, got " + entry.getValue());
    }

    // Verify push actually succeeded — data is readable
    veniceCluster.useControllerClient(controllerClient -> {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
        int currentVersion = controllerClient.getStore(storeName).getStore().getCurrentVersion();
        Assert.assertTrue(currentVersion > 0, "Store should have a current version");
      });
    });

    pushJob.close();
  }

  /**
   * A VenicePushJob subclass that captures per-partition record counts for test verification.
   */
  private static class VerifiableVenicePushJob extends VenicePushJob {
    private Map<Integer, Long> capturedCounts;

    VerifiableVenicePushJob(String jobId, Properties vanillaProps) {
      super(jobId, vanillaProps);
    }

    Map<Integer, Long> getCapturedPerPartitionRecordCounts() {
      return capturedCounts;
    }

    @Override
    public void run() {
      super.run();
      // Capture the counts after the job completes but before close
      try {
        java.lang.reflect.Method method = VenicePushJob.class.getDeclaredMethod("getPerPartitionRecordCounts");
        method.setAccessible(true);
        capturedCounts = (Map<Integer, Long>) method.invoke(this);
      } catch (Exception e) {
        throw new RuntimeException("Failed to capture per-partition record counts", e);
      }
    }
  }
}
