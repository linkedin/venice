package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.SERVER_BATCH_PUSH_RECORD_COUNT_VERIFICATION_ENABLED;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithCustomSize;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SEND_CONTROL_MESSAGES_DIRECTLY;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.KeyAndValueSchemas;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * End-to-end integration test for batch push record count verification.
 *
 * This test creates a cluster with {@code SERVER_BATCH_PUSH_RECORD_COUNT_VERIFICATION_ENABLED=true},
 * runs a batch push with {@code SEND_CONTROL_MESSAGES_DIRECTLY=true} (which embeds per-partition
 * record counts as PubSub headers on EOP), and verifies the push completes successfully.
 * If the record counts don't match, the server would throw a VeniceException and the push would fail.
 */
@Test(singleThreaded = true)
public class TestBatchPushRecordCountVerification {
  private static final int TEST_TIMEOUT = 120 * Time.MS_PER_SECOND;
  private VeniceClusterWrapper veniceCluster;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    VeniceClusterCreateOptions options =
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1).numberOfServers(0).numberOfRouters(0).build();
    veniceCluster = ServiceFactory.getVeniceCluster(options);

    Properties serverProperties = new Properties();
    serverProperties.setProperty(SERVER_BATCH_PUSH_RECORD_COUNT_VERIFICATION_ENABLED, "true");
    veniceCluster.addVeniceServer(new Properties(), serverProperties);

    veniceCluster.addVeniceRouter(new Properties());
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }

  /**
   * Tests that a basic batch push with record count verification enabled completes successfully.
   * The VPJ sends per-partition record counts on EOP, and the server verifies them.
   * If there's a mismatch, the server throws and the push fails.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testBatchPushWithRecordCountVerification() throws Exception {
    File inputDir = getTempDataDirectory();
    KeyAndValueSchemas schemas = new KeyAndValueSchemas(writeSimpleAvroFileWithStringToStringSchema(inputDir));
    String storeName = Utils.getUniqueString("record-count-verification-store");
    String inputDirPath = "file://" + inputDir.getAbsolutePath();

    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);
    props.setProperty(SEND_CONTROL_MESSAGES_DIRECTLY, "true");

    createStoreForJob(
        veniceCluster.getClusterName(),
        schemas.getKey().toString(),
        schemas.getValue().toString(),
        props,
        new UpdateStoreQueryParams()).close();

    // Run the push job - this will fail if record count verification detects a mismatch
    IntegrationTestPushUtils.runVPJ(props);

    // Wait for version to become current
    veniceCluster.useControllerClient(controllerClient -> {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
        int currentVersion = controllerClient.getStore(storeName).getStore().getCurrentVersion();
        Assert.assertTrue(currentVersion > 0, "Store " + storeName + " does not have a current version yet");
      });
    });

    // Verify data is readable (push succeeded with record count verification)
    try (AvroGenericStoreClient avroClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, true, () -> {
        for (int i = 1; i <= 100; i++) {
          Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "test_name_" + i);
        }
      });
    }
  }

  /**
   * Tests batch push with chunking enabled and record count verification.
   * Verifies that chunked records are counted correctly (only logical records, not chunk pieces).
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testChunkedBatchPushWithRecordCountVerification() throws Exception {
    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithCustomSize(inputDir, 10, 1024 * 1024, 1024 * 1024); // 10 records ~1MB
    String storeName = Utils.getUniqueString("chunked-record-count-store");
    String inputDirPath = "file://" + inputDir.getAbsolutePath();

    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);
    props.setProperty(SEND_CONTROL_MESSAGES_DIRECTLY, "true");

    createStoreForJob(
        veniceCluster.getClusterName(),
        recordSchema.getField("key").schema().toString(),
        recordSchema.getField("value").schema().toString(),
        props,
        new UpdateStoreQueryParams().setChunkingEnabled(true)).close();

    // Run the push - should succeed even with chunking because we count logical records, not chunks
    IntegrationTestPushUtils.runVPJ(props);

    veniceCluster.useControllerClient(controllerClient -> {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
        int currentVersion = controllerClient.getStore(storeName).getStore().getCurrentVersion();
        Assert.assertTrue(currentVersion > 0, "Store " + storeName + " does not have a current version yet");
      });
    });
  }
}
