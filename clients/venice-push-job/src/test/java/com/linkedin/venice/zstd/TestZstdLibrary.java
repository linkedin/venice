package com.linkedin.venice.zstd;

import static com.linkedin.venice.hadoop.DefaultInputDataInfoProvider.COMPRESSION_DICTIONARY_SAMPLE_SIZE;
import static com.linkedin.venice.hadoop.DefaultInputDataInfoProvider.COMPRESSION_DICTIONARY_SIZE_LIMIT;
import static com.linkedin.venice.hadoop.DefaultInputDataInfoProvider.PATH_FILTER;
import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_KB;
import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_MB;

import com.github.luben.zstd.ZstdDictTrainer;
import com.github.luben.zstd.ZstdException;
import com.linkedin.venice.etl.ETLValueSchemaTransformation;
import com.linkedin.venice.hadoop.InputDataInfoProvider;
import com.linkedin.venice.hadoop.PushJobZstdConfig;
import com.linkedin.venice.hadoop.VeniceAvroRecordReader;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestZstdLibrary {
  private static final Logger LOGGER = LogManager.getLogger(TestZstdLibrary.class);

  private void runTest(int numOfFiles, int numOfRecordsPerFile, int dictSizeLimitInKB, int dictSampleSizeLimitInMB)
      throws Exception {
    FileSystem fs = FileSystem.get(new Configuration());
    File inputDir = Utils.getTempDataDirectory();
    try {
      TestWriteUtils.writeMultipleAvroFilesWithUserSchema(inputDir, numOfFiles, numOfRecordsPerFile);
      Properties props = new Properties();
      props.setProperty(COMPRESSION_DICTIONARY_SIZE_LIMIT, String.valueOf(dictSizeLimitInKB * BYTES_PER_KB));
      props.setProperty(COMPRESSION_DICTIONARY_SAMPLE_SIZE, String.valueOf(dictSampleSizeLimitInMB * BYTES_PER_MB));
      VeniceProperties vProps = new VeniceProperties(props);

      PushJobZstdConfig pushJobZstdConfig = new PushJobZstdConfig(vProps, numOfFiles);

      Path srcPath = new Path(inputDir.getAbsolutePath());
      FileStatus[] fileStatuses = fs.listStatus(srcPath, PATH_FILTER);
      LOGGER.info("Collect maximum of {} Bytes from {} files", pushJobZstdConfig.getMaxBytesPerFile(), numOfFiles);
      for (FileStatus fileStatus: fileStatuses) {
        VeniceAvroRecordReader recordReader = new VeniceAvroRecordReader(
            null,
            "key",
            "value",
            fs,
            fileStatus.getPath(),
            ETLValueSchemaTransformation.NONE);

        InputDataInfoProvider.loadZstdTrainingSamples(recordReader, pushJobZstdConfig);
      }
      LOGGER.info(
          "Collected {} Bytes from {} samples in {} files",
          pushJobZstdConfig.getFilledSize(),
          pushJobZstdConfig.getCollectedNumberOfSamples(),
          numOfFiles);
      // build dict
      Assert
          .assertTrue(pushJobZstdConfig.getZstdDictTrainer().trainSamples().length <= dictSizeLimitInKB * BYTES_PER_KB);
    } finally {
      fs.delete(new Path(inputDir.getAbsolutePath()), true);
    }
  }

  /**
   * Number of samples: 0 to 6 => ZstdException ("Src size is incorrect")
   * Known issue for the below crashes: check {@link PushJobZstdConfig#MINIMUM_NUMBER_OF_SAMPLES_REQUIRED_TO_BUILD_ZSTD_DICTIONARY}
   * Number of samples: 7 to 9 => SIGSEGV
   * Number of samples: 10 => SIGFPE
   * It should not fail for other cases
   */

  @Test
  public void testZstdWith0to6Samples() throws Exception {
    int numExceptions = 0;
    for (int i = 0; i < 7; i++) {
      try {
        LOGGER.info("Running test with {} samples in 1 File", i);
        runTest(1, i, 1, 1);
      } catch (Exception e) {
        if (e instanceof ZstdException && e.getMessage().equals("Src size is incorrect")) {
          LOGGER.info("Exception thrown for {} samples", i, e);
          numExceptions++;
        } else {
          throw e;
        }
      }
    }
    Assert.assertEquals(numExceptions, 7);
  }

  @Test
  public void testZstdWith11toNSamples() throws Exception {
    for (int i = 11; i < 100; i++) {
      LOGGER.info("Running test with {} samples in 1 File", i);
      runTest(1, i, 1, 200);
    }

    int numOfFiles = 1;
    int numOfRecordsPerFile = 1000000;
    LOGGER.info("Running test with {} samples in {} File", numOfRecordsPerFile, numOfFiles);
    runTest(numOfFiles, numOfRecordsPerFile, 1, 200);

    numOfFiles = 200;
    numOfRecordsPerFile = 100000;
    LOGGER.info("Running test with {} samples in {} Files", numOfRecordsPerFile, numOfFiles);
    runTest(numOfFiles, numOfRecordsPerFile, 1, 200);
  }

  /**
   * V2: A simple loop to test zstd with few different sample sizes by just
   * calling {@code addSample()} and {@code trainSamples()}
   */
  @Test
  public void testZstdWith11toNSamplesV2() throws Exception {
    ZstdDictTrainer zstdDictTrainer = new ZstdDictTrainer(200 * BYTES_PER_MB, 1 * BYTES_PER_KB);
    for (int i = 0; i < 11; i++) {
      if (!zstdDictTrainer.addSample(new byte[] { (byte) i })) {
        throw new Exception("SAMPLE FULL");
      }
    }
    for (int i = 11; i < 100000000; i++) {
      if (!zstdDictTrainer.addSample(new byte[] { (byte) i })) {
        throw new Exception("SAMPLE FULL");
      }
      if (i % 25000000 == 0) {
        LOGGER.info("Starting to train: number of samples {}", i);
        zstdDictTrainer.trainSamples();
        ByteBuffer dict = ByteBuffer.wrap(zstdDictTrainer.trainSamples());
        LOGGER.info("Dictionary size at i = {} is: {}", i, dict.limit());
      }
    }
  }
}
