package com.linkedin.venice.hadoop;

import static com.linkedin.venice.vpj.VenicePushJobConstants.COMPRESSION_METRIC_COLLECTION_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.COMPRESSION_STRATEGY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ETL_VALUE_SCHEMA_TRANSFORMATION;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INCREMENTAL_PUSH;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INPUT_PATH_LAST_MODIFIED_TIME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INPUT_PATH_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KEY_INPUT_FILE_DATA_SIZE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KEY_ZSTD_COMPRESSION_DICTIONARY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.MINIMUM_NUMBER_OF_SAMPLES_REQUIRED_TO_BUILD_ZSTD_DICTIONARY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PATH_FILTER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.USE_MAPPER_TO_BUILD_DICTIONARY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_STORE_NAME_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ZSTD_DICTIONARY_CREATION_REQUIRED;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.etl.ETLValueSchemaTransformation;
import com.linkedin.venice.hadoop.mapreduce.counter.MRJobCounterHelper;
import com.linkedin.venice.hadoop.mapreduce.engine.MapReduceEngineTaskConfigProvider;
import com.linkedin.venice.hadoop.output.avro.ValidateSchemaAndBuildDictMapperOutput;
import com.linkedin.venice.hadoop.task.datawriter.AbstractDataWriterTask;
import com.linkedin.venice.schema.vson.VsonSchema;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Mapper only MR to Validate Schema, Build compression dictionary if needed and persist
 * some data (total file size and compression dictionary) in HDFS to be used by the VPJ Driver
 *
 * Note: processing all the files in this split are done sequentially and if it
 * results in significant increase in the mapper time or resulting in timeouts,
 * this needs to be revisited to be done via a thread pool.
 *
 * TODO: Ideally, this class should not extend {@link AbstractDataWriterTask}. Soon, this MR job is going to be removed
 * and the handling will be moved to the VPJ driver.
 */
public class ValidateSchemaAndBuildDictMapper extends AbstractDataWriterTask
    implements Mapper<IntWritable, NullWritable, AvroWrapper<SpecificRecord>, NullWritable>, JobConfigurable {
  private static final Logger LOGGER = LogManager.getLogger(ValidateSchemaAndBuildDictMapper.class);
  private JobConf jobConf;
  protected DefaultInputDataInfoProvider inputDataInfoProvider = null;
  protected InputDataInfoProvider.InputDataInfo inputDataInfo;
  protected PushJobSetting pushJobSetting = new PushJobSetting();
  protected boolean isZstdDictCreationRequired;
  protected boolean hasReportedFailure = false;
  private FileStatus[] fileStatuses = null;
  private FileSystem fileSystem = null;
  private long inputModificationTime;
  protected String inputDirectory;
  protected Long inputFileDataSize = 0L;

  @Override
  public void map(
      IntWritable inputKey,
      NullWritable inputValue,
      OutputCollector<AvroWrapper<SpecificRecord>, NullWritable> output,
      Reporter reporter) throws IOException {
    if (!hasReportedFailure) {
      if (process(inputKey, output, reporter)) {
        /** successfully processed a record */
        MRJobCounterHelper.incrMapperNumRecordsSuccessfullyProcessedCount(reporter, 1);
      } else {
        /** Failed processing: set this flag to skip further processing */
        hasReportedFailure = true;
      }
    }
  }

  /**
   * This function does 1 of the below based on inputIdx:
   * 1. inputIdx is valid input file: Processes a single input file
   *  1.1. validate this file's schema against the first file's schema
   *  1.2. Calculates total file size
   *  1.3. Collect sample for dictionary from this file if enabled
   * 2. inputIdx is sentinel record: Builds dictionary and persists information
   *  2.1. persists total file size
   *  2.1. Builds and persists compression dictionary from the collected samples if enabled.
   *
   * @param inputIdx File Index or MAPPER_BUILD_DICTIONARY_KEY to build dictionary
   * @param reporter
   * @return true for successful processing, false for error
   */
  private boolean process(
      IntWritable inputIdx,
      OutputCollector<AvroWrapper<SpecificRecord>, NullWritable> output,
      Reporter reporter) throws IOException {
    int fileIdx = inputIdx.get();
    if (fileIdx != VeniceFileInputSplit.MAPPER_SENTINEL_KEY_TO_BUILD_DICTIONARY_AND_PERSIST_OUTPUT) {
      // Processes a single input file
      return processInput(fileIdx, reporter);
    } else {
      // Builds dictionary and persists information
      return buildDictionaryAndPersistOutput(output, reporter);
    }
  }

  /**
   * 1. validate this file's schema against the first file's schema
   * 2. Calculates total file size
   * 3. Collect sample for dictionary from this file if enabled
   */
  protected boolean processInput(int fileIdx, Reporter reporter) throws IOException {
    LOGGER.info("Processing input file index {} in directory {}", fileIdx, inputDirectory);
    if (fileIdx == fileStatuses.length) {
      MRJobCounterHelper.incrMapperInvalidInputIdxCount(reporter, 1);
      checkLastModificationTimeAndLogError("validating schema and building dictionary", reporter);
      return false;
    }

    FileStatus fileStatus = fileStatuses[fileIdx];
    LOGGER.info("Processing input file {}", fileStatus.getPath().toString());

    if (fileStatus.isDirectory()) {
      // Map-reduce job will fail if the input directory has sub-directory
      MRJobCounterHelper.incrMapperInvalidInputFileCount(reporter, 1);
      LOGGER.error(
          "Error while trying to validate schema: Input directory: {}  should not have sub directory: {}",
          fileStatus.getPath().getParent().getName(),
          fileStatus.getPath().getName());
      return false;
    }

    if (pushJobSetting.isAvro) {
      LOGGER.info("Detected Avro input format.");
      Pair<Schema, Schema> newSchema =
          inputDataInfoProvider.getAvroFileHeader(fileSystem, fileStatus.getPath(), isZstdDictCreationRequired);
      if (!newSchema.getFirst().equals(pushJobSetting.inputDataSchema)
          || !newSchema.getSecond().equals(pushJobSetting.valueSchema)) {
        MRJobCounterHelper.incrMapperSchemaInconsistencyFailureCount(reporter, 1);
        LOGGER.error(
            "Error while trying to validate schema: Inconsistent file Avro schema found. File: {}. \n"
                + "Expected file schema: {}.\n Real File schema: {}.",
            fileStatus.getPath().getName(),
            pushJobSetting.inputDataSchema,
            newSchema.getFirst());
        return false;
      }
    } else {
      LOGGER.info("Detected Vson input format, will convert to Avro automatically.");
      Pair<VsonSchema, VsonSchema> newSchema =
          inputDataInfoProvider.getVsonFileHeader(fileSystem, fileStatus.getPath(), isZstdDictCreationRequired);
      if (!newSchema.getFirst().equals(pushJobSetting.vsonInputKeySchema)
          || !newSchema.getSecond().equals(pushJobSetting.vsonInputValueSchema)) {
        MRJobCounterHelper.incrMapperSchemaInconsistencyFailureCount(reporter, 1);
        LOGGER.error(
            "Error while trying to validate schema: Inconsistent file vson schema found. File: {}. \n"
                + "Expected key schema: {}. Real key schema: {}.\n"
                + "Expected value schema: {}. Real value schema: {}.\n",
            fileStatus.getPath().getName(),
            pushJobSetting.vsonInputKeySchemaString,
            newSchema.getFirst().toString(),
            pushJobSetting.vsonInputValueSchemaString,
            newSchema.getSecond().toString());
        return false;
      }
    }
    inputFileDataSize += fileStatus.getLen();
    return true;
  }

  /**
   * 1. persists total file size
   * 2. Builds and persists compression dictionary from the collected samples if enabled.
   */
  private boolean buildDictionaryAndPersistOutput(
      OutputCollector<AvroWrapper<SpecificRecord>, NullWritable> output,
      Reporter reporter) throws IOException {
    // Post the processing of input files: Persist some data to HDFS to be used by the VPJ driver code
    // 1. Init the record
    ValidateSchemaAndBuildDictMapperOutput mapperOutputRecord = new ValidateSchemaAndBuildDictMapperOutput();

    // 2. Populate inputFileDataSize (Mandatory entry)
    mapperOutputRecord.put(KEY_INPUT_FILE_DATA_SIZE, inputFileDataSize);

    try {
      // 3. Populate zstd compression dictionary (optional entry): If dictionary building is enabled and
      // if there are any input records: build dictionary from the data collected so far and append it
      if (isZstdDictCreationRequired) {
        if (inputDataInfo.hasRecords()) {
          int collectedNumberOfSamples = inputDataInfoProvider.pushJobZstdConfig.getCollectedNumberOfSamples();
          int minNumberOfSamples = MINIMUM_NUMBER_OF_SAMPLES_REQUIRED_TO_BUILD_ZSTD_DICTIONARY;
          if (collectedNumberOfSamples < minNumberOfSamples) {
            /** check {@link MINIMUM_NUMBER_OF_SAMPLES_REQUIRED_TO_BUILD_ZSTD_DICTIONARY} */
            MRJobCounterHelper.incrMapperZstdDictTrainSkippedCount(reporter, 1);
            LOGGER.error(
                "Training ZSTD compression dictionary skipped: The sample size is too small. "
                    + "Collected number of samples: {}, Minimum number of required samples: {}",
                collectedNumberOfSamples,
                minNumberOfSamples);
            return false;
          } else {
            LOGGER.info(
                "Creating ZSTD compression dictionary using {} number of samples with {} bytes",
                collectedNumberOfSamples,
                inputDataInfoProvider.pushJobZstdConfig.getFilledSize());
            ByteBuffer compressionDictionary;
            try {
              compressionDictionary = ByteBuffer.wrap(inputDataInfoProvider.trainZstdDictionary());
              mapperOutputRecord.put(KEY_ZSTD_COMPRESSION_DICTIONARY, compressionDictionary);
              MRJobCounterHelper.incrMapperZstdDictTrainSuccessCount(reporter, 1);
              LOGGER.info("ZSTD compression dictionary size = {} bytes", compressionDictionary.remaining());
            } catch (Exception e) {
              MRJobCounterHelper.incrMapperZstdDictTrainFailureCount(reporter, 1);
              LOGGER.error(
                  "Training ZSTD compression dictionary failed: The content might not be suitable for creating dictionary. ",
                  e);
              return false;
            }
          }
        } else {
          if (pushJobSetting.storeCompressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
            LOGGER.info(
                "compression strategy is {} with no input records: dictionary will be generated from synthetic data or current version data for hybrid stores",
                pushJobSetting.storeCompressionStrategy);
          } else {
            LOGGER.info("No compression dictionary is generated as the input data doesn't contain any records");
          }
        }
      } else {
        LOGGER.info("No compression dictionary is generated");
      }
    } finally {
      // 4. collect(persist) the populated output so far
      output.collect(new AvroWrapper<>(mapperOutputRecord), NullWritable.get());
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

  protected void initInputData(VeniceProperties props) throws Exception {
    inputDataInfoProvider = new DefaultInputDataInfoProvider(pushJobSetting, props);
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

    if (isZstdDictCreationRequired) {
      inputDataInfoProvider.initZstdConfig(inputDataInfo.getNumInputFiles());
    }

    try {
      Path srcPath = new Path(inputDirectory);
      fileSystem = srcPath.getFileSystem(jobConf);
      fileStatuses = fileSystem.listStatus(srcPath, PATH_FILTER);
    } catch (IOException e) {
      /** Should not happen as this is already done in driver, unless there has
       * been some changes in the input path during that time, so check for that
       */
      hasReportedFailure = true;
      checkLastModificationTimeAndLogError(e, "listing input files");
    }
  }

  @Override
  protected void configureTask(VeniceProperties props) {
    inputDirectory = props.getString(INPUT_PATH_PROP);

    pushJobSetting.storeName = props.getString(VENICE_STORE_NAME_PROP);
    pushJobSetting.isIncrementalPush = props.getBoolean(INCREMENTAL_PUSH);
    pushJobSetting.etlValueSchemaTransformation = ETLValueSchemaTransformation
        .valueOf(props.getString(ETL_VALUE_SCHEMA_TRANSFORMATION, ETLValueSchemaTransformation.NONE.name()));
    pushJobSetting.storeCompressionStrategy = CompressionStrategy.valueOf(props.getString(COMPRESSION_STRATEGY));
    // Getting the original modified time from driver
    inputModificationTime = props.getLong(INPUT_PATH_LAST_MODIFIED_TIME);
    pushJobSetting.useMapperToBuildDict = props.getBoolean(USE_MAPPER_TO_BUILD_DICTIONARY);
    pushJobSetting.compressionMetricCollectionEnabled = props.getBoolean(COMPRESSION_METRIC_COLLECTION_ENABLED);
    isZstdDictCreationRequired = props.getBoolean(ZSTD_DICTIONARY_CREATION_REQUIRED);

    try {
      initInputData(props);
    } catch (Exception e) {
      // Errors are printed and counters are incremented already
      hasReportedFailure = true;
    }
  }

  @Override
  public void configure(JobConf job) {
    this.jobConf = job;
    super.configure(new MapReduceEngineTaskConfigProvider(job));
  }

  @Override
  public void close() {
    if (inputDataInfoProvider != null) {
      inputDataInfoProvider.close();
    }
  }
}
