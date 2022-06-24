package com.linkedin.venice.endToEnd;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.*;
import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.utils.TestPushUtils.*;
import static org.testng.Assert.*;


public class TestBackupVersionDatabaseOptimization {
  private static final Logger LOGGER = LogManager.getLogger(TestBackupVersionDatabaseOptimization.class);

  private VeniceClusterWrapper venice;

  @BeforeClass
  public void setUp() {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "true");
    extraProperties.setProperty(SERVER_OPTIMIZE_DATABASE_FOR_BACKUP_VERSION_ENABLED, "true");
    extraProperties.setProperty(SERVER_OPTIMIZE_DATABASE_SERVICE_SCHEDULE_INTERNAL_SECONDS, "1");
    extraProperties.setProperty(SERVER_OPTIMIZE_DATABASE_FOR_BACKUP_VERSION_NO_READ_THRESHOLD_SECONDS, "3");

    venice = ServiceFactory.getVeniceCluster(1, 2, 1, 2, 1000000, false, false, extraProperties);
  }

  @AfterClass (alwaysRun = true)
  public void tearDown() {
    if (venice != null) {
      venice.close();
    }
  }

  private void runH2V(Properties h2vProperties, int expectedVersionNumber, ControllerClient controllerClient) {
    long h2vStart = System.currentTimeMillis();
    String jobName = Utils.getUniqueString("push-job-" + expectedVersionNumber);
    try (VenicePushJob job = new VenicePushJob(jobName, h2vProperties)) {
      job.run();
      TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS,
          () -> controllerClient.getStore((String) h2vProperties.get(VenicePushJob.VENICE_STORE_NAME_PROP))
              .getStore().getCurrentVersion() == expectedVersionNumber);
      LOGGER.info("**TIME** H2V" + expectedVersionNumber + " takes " + (System.currentTimeMillis() - h2vStart));
    }
  }

  @Test
  public void verifyBackupVersionDatabaseOptimizationOccurs() throws IOException {
    String storeName = Utils.getUniqueString("backup-version-optimization-validation-store");
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir); // records 1-100
    Properties h2vProperties = defaultH2VProps(venice, inputDirPath, storeName);

    try (ControllerClient controllerClient = createStoreForJob(venice, recordSchema, h2vProperties);
        AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(venice.getRandomRouterURL()))) {
      //Do an H2V push
      runH2V(h2vProperties, 1, controllerClient);

      TestUtils.NonDeterministicAssertion validation = () -> {
        try {
          for (int i = 1; i < 100; i++) {
            String key = Integer.toString(i);
            Object value = client.get(key).get();
            assertNotNull(value, "Key " + i + " should not be missing!");
            assertEquals(value.toString(), "test_name_" + key);
          }
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      };

      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, validation);

      // Do another H2V push
      runH2V(h2vProperties, 2, controllerClient);

      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, validation);

      // Verify whether the backup version database optimization happens or not.
      VeniceServerWrapper serverWrapper = venice.getVeniceServers().get(0);
      MetricsRepository metricsRepository = serverWrapper.getMetricsRepository();
      Metric optimizationMetric = metricsRepository.getMetric(".BackupVersionOptimizationService--backup_version_database_optimization.OccurrenceRate");
      assertTrue(optimizationMetric.value() > 0, "Backup version database optimization should happen");
    }
  }

}
