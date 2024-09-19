package com.linkedin.venice.hadoop;

import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.FILE_KEY_SCHEMA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.FILE_VALUE_SCHEMA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.MINIMUM_NUMBER_OF_SAMPLES_REQUIRED_TO_BUILD_ZSTD_DICTIONARY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PATH_FILTER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_FIELD_PROP;

import com.linkedin.venice.compression.ZstdWithDictCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.exceptions.VeniceInconsistentSchemaException;
import com.linkedin.venice.hadoop.exceptions.VeniceSchemaFieldNotFoundException;
import com.linkedin.venice.hadoop.input.recordreader.VeniceRecordIterator;
import com.linkedin.venice.hadoop.input.recordreader.avro.HdfsAvroUtils;
import com.linkedin.venice.hadoop.input.recordreader.avro.VeniceAvroFileIterator;
import com.linkedin.venice.hadoop.input.recordreader.avro.VeniceAvroRecordReader;
import com.linkedin.venice.hadoop.input.recordreader.vson.VeniceVsonFileIterator;
import com.linkedin.venice.hadoop.input.recordreader.vson.VeniceVsonRecordReader;
import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import com.linkedin.venice.schema.vson.VsonSchema;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class DefaultInputDataInfoProvider implements InputDataInfoProvider {
  private static final Logger LOGGER = LogManager.getLogger(DefaultInputDataInfoProvider.class);

  /**
   * Config to control the thread pool size for HDFS operations.
   */
  private static final String HDFS_OPERATIONS_PARALLEL_THREAD_NUM = "hdfs.operations.parallel.thread.num";

  private final PushJobSetting pushJobSetting;
  protected PushJobZstdConfig pushJobZstdConfig;
  private final VeniceProperties props;
  /**
   * Thread pool for Hadoop File System operations: Lazy initialization as this
   * is not needed when {@link PushJobSetting#useMapperToBuildDict} is true
   */
  private final Lazy<ExecutorService> hdfsExecutorService;

  public DefaultInputDataInfoProvider(PushJobSetting pushJobSetting, VeniceProperties props) {
    this.pushJobSetting = pushJobSetting;
    this.props = props;
    this.hdfsExecutorService =
        Lazy.of(() -> Executors.newFixedThreadPool(props.getInt(HDFS_OPERATIONS_PARALLEL_THREAD_NUM, 20)));
  }

  /**
   * 1. Check whether it's Vson input or Avro input
   * 2. Check schema consistency;
   * 3. Populate key schema, value schema;
   * 4. Load samples for dictionary compression if enabled
   * @param inputUri
   * @return a {@link com.linkedin.venice.hadoop.InputDataInfoProvider.InputDataInfo} that contains input data information
   * @throws Exception
   */
  @Override
  public InputDataInfo validateInputAndGetInfo(String inputUri) throws Exception {
    long inputModificationTime = getInputLastModificationTime(inputUri);
    Path srcPath = new Path(inputUri);
    FileSystem fs = srcPath.getFileSystem(new Configuration());
    FileStatus[] fileStatuses = fs.listStatus(srcPath, PATH_FILTER);

    if (fileStatuses == null || fileStatuses.length == 0) {
      throw new RuntimeException("No data found at source path: " + srcPath);
    }

    if (pushJobSetting.isZstdDictCreationRequired && !pushJobSetting.useMapperToBuildDict) {
      initZstdConfig(fileStatuses.length);
    }

    // try reading the file via sequence file reader. It indicates Vson input if it is succeeded.
    Map<String, String> fileMetadata = getMetadataFromSequenceFile(fs, fileStatuses[0].getPath(), false);
    if (fileMetadata.containsKey(FILE_KEY_SCHEMA) && fileMetadata.containsKey(FILE_VALUE_SCHEMA)) {
      pushJobSetting.isAvro = false;
      pushJobSetting.vsonInputKeySchemaString = fileMetadata.get(FILE_KEY_SCHEMA);
      pushJobSetting.vsonInputKeySchema = VsonSchema.parse(pushJobSetting.vsonInputKeySchemaString);
      pushJobSetting.vsonInputValueSchemaString = fileMetadata.get(FILE_VALUE_SCHEMA);
      pushJobSetting.vsonInputValueSchema = VsonSchema.parse(pushJobSetting.vsonInputValueSchemaString);
    }
    // Check the first file type prior to check schema consistency to make sure a schema can be obtained from it.
    if (fileStatuses[0].isDirectory()) {
      throw new VeniceException(
          "Input directory: " + fileStatuses[0].getPath().getParent().getName() + " should not have sub directory: "
              + fileStatuses[0].getPath().getName());
    }

    final AtomicLong inputFileDataSize = new AtomicLong(0);
    if (pushJobSetting.isAvro) {
      LOGGER.info("Detected Avro input format.");
      pushJobSetting.keyField = props.getString(KEY_FIELD_PROP, DEFAULT_KEY_FIELD_PROP);
      pushJobSetting.valueField = props.getString(VALUE_FIELD_PROP, DEFAULT_VALUE_FIELD_PROP);

      Pair<Schema, Schema> fileAndOutputValueSchema;
      if (!pushJobSetting.useMapperToBuildDict) {
        fileAndOutputValueSchema = checkAvroSchemaConsistency(fs, fileStatuses, inputFileDataSize);
      } else {
        // ValidateSchemaAndBuildDictMapper will validate all file schemas and build the dictionary
        fileAndOutputValueSchema = getAvroFileHeader(fs, fileStatuses[0].getPath(), false);
      }

      pushJobSetting.inputDataSchema = fileAndOutputValueSchema.getFirst();
      pushJobSetting.valueSchema = fileAndOutputValueSchema.getSecond();

      pushJobSetting.inputDataSchemaString = pushJobSetting.inputDataSchema.toString();
      pushJobSetting.keySchema = extractAvroSubSchema(pushJobSetting.inputDataSchema, pushJobSetting.keyField);
    } else {
      LOGGER.info("Detected Vson input format, will convert to Avro automatically.");
      // key / value fields are optional for Vson input
      pushJobSetting.keyField = props.getString(KEY_FIELD_PROP, "");
      pushJobSetting.valueField = props.getString(VALUE_FIELD_PROP, "");

      Pair<VsonSchema, VsonSchema> vsonSchema;
      if (!pushJobSetting.useMapperToBuildDict) {
        vsonSchema = checkVsonSchemaConsistency(fs, fileStatuses, inputFileDataSize);
      } else {
        // ValidateSchemaAndBuildDictMapper will validate all file schemas and build the dictionary
        vsonSchema = getVsonFileHeader(fs, fileStatuses[0].getPath(), false);
      }

      VsonSchema vsonKeySchema = StringUtils.isEmpty(pushJobSetting.keyField)
          ? vsonSchema.getFirst()
          : vsonSchema.getFirst().recordSubtype(pushJobSetting.keyField);
      VsonSchema vsonValueSchema = StringUtils.isEmpty(pushJobSetting.valueField)
          ? vsonSchema.getSecond()
          : vsonSchema.getSecond().recordSubtype(pushJobSetting.valueField);

      pushJobSetting.keySchema = VsonAvroSchemaAdapter.parse(vsonKeySchema.toString());
      pushJobSetting.valueSchema = VsonAvroSchemaAdapter.parse(vsonValueSchema.toString());
    }

    pushJobSetting.keySchemaString = pushJobSetting.keySchema.toString();
    pushJobSetting.valueSchemaString = pushJobSetting.valueSchema.toString();

    return new InputDataInfo(
        inputFileDataSize.get(),
        fileStatuses.length,
        hasRecords(pushJobSetting.isAvro, fs, fileStatuses),
        inputModificationTime,
        !pushJobSetting.useMapperToBuildDict);
  }

  private boolean hasRecords(boolean isAvroFile, FileSystem fs, FileStatus[] fileStatusList) {
    for (FileStatus fileStatus: fileStatusList) {
      VeniceRecordIterator recordIterator = isAvroFile
          ? getVeniceAvroFileIterator(fs, fileStatus.getPath())
          : getVeniceVsonFileIterator(fs, fileStatus.getPath());
      if (recordIterator.next()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void initZstdConfig(int numFiles) {
    if (pushJobZstdConfig != null) {
      return;
    }
    pushJobZstdConfig = new PushJobZstdConfig(props, numFiles);
  }

  // Vson-based file store key / value schema string as separated properties in file header
  private Pair<VsonSchema, VsonSchema> checkVsonSchemaConsistency(
      FileSystem fs,
      FileStatus[] fileStatusList,
      AtomicLong inputFileDataSize) {
    Pair<VsonSchema, VsonSchema> vsonSchema = getVsonFileHeader(fs, fileStatusList[0].getPath(), false);
    parallelExecuteHDFSOperation(fileStatusList, "checkVsonSchemaConsistency", fileStatus -> {
      if (fileStatus.isDirectory()) {
        throw new VeniceException(
            "Input directory: " + fileStatus.getPath().getParent().getName() + " should not have sub directory: "
                + fileStatus.getPath().getName());
      }
      inputFileDataSize.addAndGet(fileStatus.getLen());
      Pair<VsonSchema, VsonSchema> newSchema =
          getVsonFileHeader(fs, fileStatus.getPath(), pushJobSetting.isZstdDictCreationRequired);
      if (!vsonSchema.getFirst().equals(newSchema.getFirst())
          || !vsonSchema.getSecond().equals(newSchema.getSecond())) {
        throw new VeniceInconsistentSchemaException(
            String.format(
                "Inconsistent file Vson schema found. File: %s.\n Expected key schema: %s.\n"
                    + "Expected value schema: %s.\n File key schema: %s.\n File value schema: %s.",
                fileStatus.getPath().getName(),
                vsonSchema.getFirst(),
                vsonSchema.getSecond(),
                newSchema.getFirst(),
                newSchema.getSecond()));
      }
    });
    return vsonSchema;
  }

  protected Pair<VsonSchema, VsonSchema> getVsonFileHeader(
      FileSystem fs,
      Path path,
      boolean isZstdDictCreationRequired) {
    Map<String, String> fileMetadata = getMetadataFromSequenceFile(fs, path, isZstdDictCreationRequired);
    if (!fileMetadata.containsKey(FILE_KEY_SCHEMA) || !fileMetadata.containsKey(FILE_VALUE_SCHEMA)) {
      throw new VeniceException("Can't find Vson schema from file: " + path.getName());
    }

    return new Pair<>(
        VsonSchema.parse(fileMetadata.get(FILE_KEY_SCHEMA)),
        VsonSchema.parse(fileMetadata.get(FILE_VALUE_SCHEMA)));
  }

  /**
   * This function is to execute HDFS operation in parallel
   */
  private void parallelExecuteHDFSOperation(
      final FileStatus[] fileStatusList,
      String operation,
      Consumer<FileStatus> fileStatusConsumer) {
    ExecutorService hdfsExecutorService = this.hdfsExecutorService.get();
    if (hdfsExecutorService.isShutdown()) {
      throw new VeniceException(
          "Unable to execute HDFS operations in parallel, the executor has already been shutdown");
    }
    int len = fileStatusList.length;
    CompletableFuture<Void>[] futures = new CompletableFuture[len];
    for (int cur = 0; cur < len; ++cur) {
      final int finalCur = cur;
      futures[cur] =
          CompletableFuture.runAsync(() -> fileStatusConsumer.accept(fileStatusList[finalCur]), hdfsExecutorService);
    }
    try {
      CompletableFuture.allOf(futures).get();
    } catch (Exception e) {
      if (e.getCause() instanceof VeniceException) {
        throw (VeniceException) e.getCause();
      }
      throw new VeniceException("Failed to execute " + operation + " in parallel", e);
    }
  }

  private Map<String, String> getMetadataFromSequenceFile(
      FileSystem fs,
      Path path,
      boolean isZstdDictCreationRequired) {
    VeniceVsonRecordReader recordReader = getVeniceVsonRecordReader(fs, path);
    if (isZstdDictCreationRequired) {
      try (VeniceVsonFileIterator fileIterator = new VeniceVsonFileIterator(fs, path, recordReader)) {
        InputDataInfoProvider.loadZstdTrainingSamples(fileIterator, pushJobZstdConfig);
      } catch (IOException e) {
        LOGGER.error(e);
      }
    }
    return recordReader.getMetadataMap();
  }

  private VeniceVsonRecordReader getVeniceVsonRecordReader(FileSystem fs, Path path) {
    String keyField = props.getString(KEY_FIELD_PROP, "");
    String valueField = props.getString(VALUE_FIELD_PROP, "");
    return new VeniceVsonRecordReader(keyField, valueField, fs, path);
  }

  private VeniceVsonFileIterator getVeniceVsonFileIterator(FileSystem fs, Path path) {
    return new VeniceVsonFileIterator(fs, path, getVeniceVsonRecordReader(fs, path));
  }

  @Override
  public byte[] trainZstdDictionary() {
    int collectedNumberOfSamples = pushJobZstdConfig.getCollectedNumberOfSamples();
    int minNumberOfSamples = MINIMUM_NUMBER_OF_SAMPLES_REQUIRED_TO_BUILD_ZSTD_DICTIONARY;
    if (collectedNumberOfSamples < minNumberOfSamples) {
      /** check {@link MINIMUM_NUMBER_OF_SAMPLES_REQUIRED_TO_BUILD_ZSTD_DICTIONARY} */
      LOGGER.warn(
          "Training ZSTD compression dictionary on store data skipped. The sample size is too small. "
              + "Collected number of samples: {}, Minimum number of required samples: {}. Will use synthetic data instead.",
          collectedNumberOfSamples,
          minNumberOfSamples);
      return ZstdWithDictCompressor.buildDictionaryOnSyntheticAvroData();
    }
    return pushJobZstdConfig.getZstdDictTrainer().trainSamples();
  }

  @Override
  public Schema extractAvroSubSchema(Schema origin, String fieldName) {
    Schema.Field field = origin.getField(fieldName);

    if (field == null) {
      throw new VeniceSchemaFieldNotFoundException(
          fieldName,
          "Could not find field: " + fieldName + " from " + origin.toString());
    }

    return field.schema();
  }

  @Override
  public long getInputLastModificationTime(String inputUri) throws IOException {
    Path srcPath = new Path(inputUri);
    FileSystem fs = srcPath.getFileSystem(new Configuration());
    try {
      return fs.getFileStatus(srcPath).getModificationTime();
    } catch (FileNotFoundException e) {
      throw new RuntimeException("No data found at source path: " + srcPath);
    }
  }

  // Avro-based file composes key and value schema as a whole
  private Pair<Schema, Schema> checkAvroSchemaConsistency(
      FileSystem fs,
      FileStatus[] fileStatusList,
      AtomicLong inputFileDataSize) {
    Pair<Schema, Schema> avroSchema = getAvroFileHeader(fs, fileStatusList[0].getPath(), false);
    parallelExecuteHDFSOperation(fileStatusList, "checkAvroSchemaConsistency", fileStatus -> {
      if (fileStatus.isDirectory()) {
        // Map-reduce job will fail if the input directory has sub-directory and 'recursive' is not specified.
        throw new VeniceException(
            "Input directory: " + fileStatus.getPath().getParent().getName() + " should not have sub directory: "
                + fileStatus.getPath().getName());
      }
      inputFileDataSize.addAndGet(fileStatus.getLen());
      Pair<Schema, Schema> newSchema =
          getAvroFileHeader(fs, fileStatus.getPath(), pushJobSetting.isZstdDictCreationRequired);
      if (!avroSchema.equals(newSchema)) {
        throw new VeniceInconsistentSchemaException(
            String.format(
                "Inconsistent file Avro schema found. File: %s.\n Expected file schema: %s.\n Real File schema: %s.",
                fileStatus.getPath().getName(),
                avroSchema,
                newSchema));
      }
    });
    return avroSchema;
  }

  protected Pair<Schema, Schema> getAvroFileHeader(FileSystem fs, Path path, boolean isZstdDictCreationRequired) {
    VeniceAvroRecordReader recordReader = getVeniceAvroRecordReader(fs, path);
    if (isZstdDictCreationRequired) {
      try (VeniceAvroFileIterator fileIterator = new VeniceAvroFileIterator(fs, path, recordReader)) {
        InputDataInfoProvider.loadZstdTrainingSamples(fileIterator, pushJobZstdConfig);
      } catch (IOException e) {
        LOGGER.error(e);
      }
    }
    return new Pair<>(recordReader.getDataSchema(), recordReader.getValueSchema());
  }

  private VeniceAvroRecordReader getVeniceAvroRecordReader(FileSystem fs, Path path) {
    String keyField = props.getString(KEY_FIELD_PROP, DEFAULT_KEY_FIELD_PROP);
    String valueField = props.getString(VALUE_FIELD_PROP, DEFAULT_VALUE_FIELD_PROP);
    return new VeniceAvroRecordReader(
        HdfsAvroUtils.getFileSchema(fs, path),
        keyField,
        valueField,
        pushJobSetting.etlValueSchemaTransformation,
        null);
  }

  private VeniceRecordIterator getVeniceAvroFileIterator(FileSystem fs, Path path) {
    return new VeniceAvroFileIterator(fs, path, getVeniceAvroRecordReader(fs, path));
  }

  @Override
  public void close() {
    shutdownHdfsExecutorService();
  }

  private void shutdownHdfsExecutorService() {
    if (!this.hdfsExecutorService.isPresent()) {
      LOGGER.warn("No HDFS executor service to shutdown");
      return;
    }

    ExecutorService hdfsExecutorService = this.hdfsExecutorService.get();
    hdfsExecutorService.shutdownNow();
    try {
      if (!hdfsExecutorService.awaitTermination(30, TimeUnit.SECONDS)) {
        LOGGER.warn(
            "Unable to shutdown the executor service used for HDFS operations. The job may hang with leaked resources.");
      }
    } catch (InterruptedException e) {
      LOGGER.error(e);
      Thread.currentThread().interrupt();
    }
  }
}
