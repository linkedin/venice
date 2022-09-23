package com.linkedin.venice.hadoop;

import static com.linkedin.venice.hadoop.VenicePushJob.COMPRESSION_METRIC_COLLECTION_ENABLED;
import static com.linkedin.venice.hadoop.VenicePushJob.COMPRESSION_STRATEGY;
import static com.linkedin.venice.hadoop.VenicePushJob.ETL_VALUE_SCHEMA_TRANSFORMATION;
import static com.linkedin.venice.hadoop.VenicePushJob.INCREMENTAL_PUSH;
import static com.linkedin.venice.hadoop.VenicePushJob.INPUT_PATH_LAST_MODIFIED_TIME;
import static com.linkedin.venice.hadoop.VenicePushJob.PATH_FILTER;
import static com.linkedin.venice.hadoop.VenicePushJob.PushJobSetting;
import static com.linkedin.venice.hadoop.VenicePushJob.StoreSetting;
import static com.linkedin.venice.hadoop.VenicePushJob.USE_MAPPER_TO_BUILD_DICTIONARY;
import static com.linkedin.venice.hadoop.VenicePushJob.VENICE_STORE_NAME_PROP;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.etl.ETLValueSchemaTransformation;
import com.linkedin.venice.schema.vson.VsonSchema;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Mapper only MR to Validate Schema and Build dictionary if needed
 *
 * Note
 * 1. processing all the files in this split are done sequentially and if it
 *    results in significant increase in the mapper time or resulting in timeouts,
 *    this needs to be revisited to be done via a thread pool.
 */
public class ValidateSchemaAndBuildDictMapper extends AbstractMapReduceTask
    implements Mapper<IntWritable, NullWritable, NullWritable, NullWritable> {
  private static final Logger LOGGER = LogManager.getLogger(ValidateSchemaAndBuildDictMapper.class);
  protected InputDataInfoProvider inputDataInfoProvider = null;
  protected InputDataInfoProvider.InputDataInfo inputDataInfo;
  protected PushJobSetting pushJobSetting = new PushJobSetting();
  protected StoreSetting storeSetting = new StoreSetting();
  protected boolean buildDictionary;
  protected boolean hasReportedFailure = false;
  private FileStatus[] fileStatuses = null;
  private FileSystem fileSystem = null;
  private long inputModificationTime;
  protected String inputDirectory;

  @Override
  public void map(
      IntWritable inputKey,
      NullWritable inputValue,
      OutputCollector<NullWritable, NullWritable> output,
      Reporter reporter) throws IOException {
    if (!hasReportedFailure) {
      if (process(inputKey, reporter)) {
        /** successfully processed a record */
        MRJobCounterHelper.incrMapperNumRecordsSuccessfullyProcessedCount(reporter, 1);
      } else {
        /** Failed processing: set this flag to skip further processing */
        hasReportedFailure = true;
      }
    }
  }

  /**
   * This function
   * 1. Processes a single input file
   *  1.1. validate this file's schema against the first file's schema
   *  1.2. Collect sample for dictionary from this file if enabled
   * 2. Builds dictionary from the collected samples if enabled
   *
   * @param inputIdx File Index or MAPPER_BUILD_DICTIONARY_KEY to build dictionary
   * @param reporter
   * @return true for successful processing, false for error
   */
  protected boolean process(IntWritable inputIdx, Reporter reporter) throws IOException {
    DefaultInputDataInfoProvider inputDataInfoProvider = (DefaultInputDataInfoProvider) this.inputDataInfoProvider;
    int fileIdx = inputIdx.get();
    if (fileIdx != VeniceFileInputSplit.MAPPER_BUILD_DICTIONARY_KEY) {
      // process input files
      LOGGER.info("Input File index to be processed is : {}", fileIdx);

      if (fileIdx >= fileStatuses.length) {
        MRJobCounterHelper.incrMapperInvalidInputIdxCount(reporter, 1);
        checkLastModificationTimeAndLogError("validating schema and building dictionary", reporter);
        return false;
      }

      FileStatus fileStatus = fileStatuses[fileIdx];
      LOGGER.info("Input File to be processed is : {}", fileStatus.getPath().toString());

      if (fileStatus.isDirectory()) {
        // Map-reduce job will fail if the input directory has sub-directory
        MRJobCounterHelper.incrMapperInvalidInputFileCount(reporter, 1);
        LOGGER.error(
            "Error while trying to validate schema: Input directory: {}  should not have sub directory: {}",
            fileStatus.getPath().getParent().getName(),
            fileStatus.getPath().getName());
        return false;
      }

      if (inputDataInfo.getSchemaInfo().isAvro()) {
        LOGGER.info("Detected Avro input format.");
        Pair<Schema, Schema> newSchema =
            inputDataInfoProvider.getAvroFileHeader(fileSystem, fileStatus.getPath(), buildDictionary);
        if (!newSchema.equals(inputDataInfo.getSchemaInfo().getAvroSchema())) {
          MRJobCounterHelper.incrMapperSchemaInconsistencyFailureCount(reporter, 1);
          LOGGER.error(
              "Error while trying to validate schema: Inconsistent file Avro schema found. File: {}. \n"
                  + "Expected file schema: {}.\n Real File schema: {}.",
              fileStatus.getPath().getName(),
              inputDataInfo.getSchemaInfo().getAvroSchema(),
              newSchema);
          return false;
        }
      } else {
        LOGGER.info("Detected Vson input format, will convert to Avro automatically.");
        Pair<VsonSchema, VsonSchema> newSchema =
            inputDataInfoProvider.getVsonFileHeader(fileSystem, fileStatus.getPath(), buildDictionary);
        if (!newSchema.equals(inputDataInfo.getSchemaInfo().getVsonSchema())) {
          MRJobCounterHelper.incrMapperSchemaInconsistencyFailureCount(reporter, 1);
          LOGGER.error(
              "Error while trying to validate schema: Inconsistent file vson schema found. File: {}. "
                  + "Expected file schema: {}. Real File schema: {}.",
              fileStatus.getPath().getName(),
              inputDataInfo.getSchemaInfo().getVsonSchema(),
              newSchema);
          return false;
        }
      }
    } else {
      // If dictionary building is enabled and if there are any input records:
      // build dictionary from the data collected so far
      if (buildDictionary) {
        if (inputDataInfo.hasRecords()) {
          LOGGER.info(
              "Creating ZSTD compression dictionary using {}  bytes of samples",
              inputDataInfoProvider.pushJobZstdConfig.getFilledSize());
          byte[] dict;
          try {
            dict = inputDataInfoProvider.getZstdDictTrainSamples();
            MRJobCounterHelper.incrMapperZstdDictTrainSuccessCount(reporter, 1);
          } catch (Exception e) {
            MRJobCounterHelper.incrMapperZstdDictTrainFailureCount(reporter, 1);
            LOGGER.error(
                "Training ZStd compression dictionary failed: Maybe the sample size is too small or "
                    + "the content is not suitable for creating dictionary :  ",
                e);
            return false;
          }
          LOGGER.info("Zstd compression dictionary size = {} bytes", dict.length);
        } else {
          LOGGER.info("No compression dictionary is generated as the input data doesn't contain any records");
        }
      } else {
        LOGGER.info(
            "No compression dictionary is generated with the strategy {} and compressionMetricCollectionEnabled is {}",
            storeSetting.compressionStrategy,
            (pushJobSetting.compressionMetricCollectionEnabled ? "Enabled" : "Disabled"));
      }
    }
    return true;
  }

  protected void checkLastModificationTimeAndLogError(Exception e, String errorString) throws IOException {
    checkLastModificationTimeAndLogError(e, errorString, null);
  }

  protected void checkLastModificationTimeAndLogError(String errorString, Reporter reporter) throws IOException {
    checkLastModificationTimeAndLogError(null, errorString, reporter);
  }

  protected void checkLastModificationTimeAndLogError(Exception exception, String errorString, Reporter reporter)
      throws IOException {
    long lastModificationTime = inputDataInfoProvider.getInputLastModificationTime(inputDirectory);
    if (lastModificationTime > inputModificationTime) {
      LOGGER.error(
          "Error while {}: Because Dataset changed during the push job. Rerun the job without dataset change.",
          errorString,
          exception);
      if (reporter != null) {
        MRJobCounterHelper.incrMapperErrorDataModifiedDuringPushJobCount(reporter, 1);
      }
    } else {
      LOGGER.error("Error while {}: Maybe because Dataset changed during the push job.", errorString, exception);
    }
  }

  protected void initInputData(JobConf job, VeniceProperties props) throws Exception {
    inputDataInfoProvider = new DefaultInputDataInfoProvider(storeSetting, pushJobSetting, props);
    try {
      inputDataInfo = inputDataInfoProvider.validateInputAndGetInfo(inputDirectory);
    } catch (Exception e) {
      /** Should not happen as this is already done in driver, unless there has
       * been some changes in the input path during that time, so check for that
       */
      hasReportedFailure = true;
      checkLastModificationTimeAndLogError(e, "validating schema");
      return;
    }

    if (buildDictionary) {
      inputDataInfoProvider.initZstdConfig(inputDataInfo.getNumInputFiles());
    }

    try {
      Configuration conf = new Configuration();
      fileSystem = FileSystem.get(conf);
      Path srcPath = new Path(inputDirectory);
      fileStatuses = fileSystem.listStatus(srcPath, PATH_FILTER);
    } catch (IOException e) {
      /** Should not happen as this is already done in driver, unless there has
       * been some changes in the input path during that time, so check for that
       */
      hasReportedFailure = true;
      checkLastModificationTimeAndLogError(e, "listing input files");
      return;
    }
  }

  @Override
  protected void configureTask(VeniceProperties props, JobConf job) {
    inputDirectory = props.getString(VenicePushJob.INPUT_PATH_PROP);

    pushJobSetting.storeName = props.getString(VENICE_STORE_NAME_PROP);
    pushJobSetting.isIncrementalPush = props.getBoolean(INCREMENTAL_PUSH);
    pushJobSetting.compressionMetricCollectionEnabled = props.getBoolean(COMPRESSION_METRIC_COLLECTION_ENABLED);
    pushJobSetting.useMapperToBuildDict = props.getBoolean(USE_MAPPER_TO_BUILD_DICTIONARY);
    pushJobSetting.etlValueSchemaTransformation = ETLValueSchemaTransformation
        .valueOf(props.getString(ETL_VALUE_SCHEMA_TRANSFORMATION, ETLValueSchemaTransformation.NONE.name()));
    storeSetting.compressionStrategy = CompressionStrategy.valueOf(props.getString(COMPRESSION_STRATEGY));
    // Getting the original modified time from driver
    inputModificationTime = props.getLong(INPUT_PATH_LAST_MODIFIED_TIME);

    buildDictionary = VenicePushJob.shouldBuildDictionary(pushJobSetting, storeSetting);

    try {
      initInputData(job, props);
    } catch (Exception e) {
      // Errors are printed and counters are incremented already
      hasReportedFailure = true;
    }
  }

  @Override
  public void close() {
    DefaultInputDataInfoProvider inputDataInfoProvider = (DefaultInputDataInfoProvider) this.inputDataInfoProvider;
    if (inputDataInfoProvider != null) {
      inputDataInfoProvider.close();
    }
  }
}
