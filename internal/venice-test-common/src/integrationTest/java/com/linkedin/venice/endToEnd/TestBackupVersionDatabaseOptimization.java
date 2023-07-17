package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_OPTIMIZE_DATABASE_FOR_BACKUP_VERSION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_OPTIMIZE_DATABASE_FOR_BACKUP_VERSION_NO_READ_THRESHOLD_SECONDS;
import static com.linkedin.venice.ConfigKeys.SERVER_OPTIMIZE_DATABASE_SERVICE_SCHEDULE_INTERNAL_SECONDS;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.DEFAULT_USER_DATA_RECORD_COUNT;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithUserSchema;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

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

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    if (venice != null) {
      venice.close();
    }
  }

  private void runVPJ(Properties vpjProperties, int expectedVersionNumber, ControllerClient controllerClient) {
    long vpjStart = System.currentTimeMillis();
    String jobName = Utils.getUniqueString("push-job-" + expectedVersionNumber);
    try (VenicePushJob job = new VenicePushJob(jobName, vpjProperties)) {
      job.run();
      TestUtils.waitForNonDeterministicCompletion(
          5,
          TimeUnit.SECONDS,
          () -> controllerClient.getStore((String) vpjProperties.get(VenicePushJob.VENICE_STORE_NAME_PROP))
              .getStore()
              .getCurrentVersion() == expectedVersionNumber);
      LOGGER.info("**TIME** VPJ" + expectedVersionNumber + " takes " + (System.currentTimeMillis() - vpjStart));
    }
  }

  @Test
  public void verifyBackupVersionDatabaseOptimizationOccurs() throws IOException {
    String storeName = Utils.getUniqueString("backup-version-optimization-validation-store");
    File inputDir = getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir); // records 1-100
    Properties vpjProperties = defaultVPJProps(venice, inputDirPath, storeName);

    try (ControllerClient controllerClient = createStoreForJob(venice.getClusterName(), recordSchema, vpjProperties);
        AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(venice.getRandomRouterURL()))) {
      // Do a VPJ push
      runVPJ(vpjProperties, 1, controllerClient);

      class ResultValidator implements TestUtils.NonDeterministicAssertion {
        private final int recordCount;

        public ResultValidator(int recordCount) {
          this.recordCount = recordCount;
        }

        @Override
        public void execute() {
          try {
            for (int i = 1; i < recordCount; i++) {
              String key = Integer.toString(i);
              Object value = client.get(key).get();
              assertNotNull(value, "Key " + i + " should not be missing!");
              assertEquals(value.toString(), "test_name_" + key);
            }
          } catch (Exception e) {
            throw new VeniceException(e);
          }
        }
      }

      TestUtils.waitForNonDeterministicAssertion(
          10,
          TimeUnit.SECONDS,
          true,
          new ResultValidator(DEFAULT_USER_DATA_RECORD_COUNT));

      // Do another VPJ push, with more records, otherwise the two datasets are indistinguishable
      int recordCountOf2ndRun = DEFAULT_USER_DATA_RECORD_COUNT * 2;
      File inputDir2 = getTempDataDirectory();
      String inputDirPath2 = "file://" + inputDir2.getAbsolutePath();
      writeSimpleAvroFileWithUserSchema(inputDir2, true, recordCountOf2ndRun);
      Properties vpjProperties2 = defaultVPJProps(venice, inputDirPath2, storeName);
      runVPJ(vpjProperties2, 2, controllerClient);

      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, new ResultValidator(recordCountOf2ndRun));

      // Verify whether the backup version database optimization happens or not.
      VeniceServerWrapper serverWrapper = venice.getVeniceServers().get(0);
      MetricsRepository metricsRepository = serverWrapper.getMetricsRepository();
      Metric optimizationMetric = metricsRepository
          .getMetric(".BackupVersionOptimizationService--backup_version_database_optimization.OccurrenceRate");
      Metric rocksdbMetric = metricsRepository.getMetric(".RocksDBMemoryStats--rocksdb.num-immutable-mem-table.Gauge");

      // N.B.: The optimization is performed by a periodic background task, so it cannot be expected to have already
      // completed as soon as we get here.
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        assertTrue(optimizationMetric.value() > 0, "Backup version database optimization should happen");
        /**
         * This assertion is used to make sure {@link com.linkedin.davinci.stats.RocksDBMemoryStats} won't crash after
         * reopening the backup version.
         */
        rocksdbMetric.value();
      });
    }
  }

}
