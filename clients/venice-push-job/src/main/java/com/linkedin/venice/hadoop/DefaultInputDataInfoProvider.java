package com.linkedin.venice.hadoop;

import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_MB;

import com.github.luben.zstd.ZstdDictTrainer;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.exceptions.VeniceInconsistentSchemaException;
import com.linkedin.venice.hadoop.exceptions.VeniceSchemaFieldNotFoundException;
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
import org.apache.hadoop.fs.PathFilter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class DefaultInputDataInfoProvider implements InputDataInfoProvider {
  private static final Logger LOGGER = LogManager.getLogger(DefaultInputDataInfoProvider.class);
  /**
   * ignore hdfs files with prefix "_" and "."
   */
  public static final PathFilter PATH_FILTER = p -> !p.getName().startsWith("_") && !p.getName().startsWith(".");
  // Vson input configs
  // Vson files store key/value schema on file header. key / value fields are optional
  // and should be specified only when key / value schema is the partial of the files.
  public static final String FILE_KEY_SCHEMA = "key.schema";
  public static final String FILE_VALUE_SCHEMA = "value.schema";
  public static final String KEY_FIELD_PROP = "key.field";
  public static final String VALUE_FIELD_PROP = "value.field";

  /** Sample size to collect for building dictionary: Can be assigned a max of 2GB as {@link ZstdDictTrainer} in ZSTD library takes in sample size as int */
  public static final String COMPRESSION_DICTIONARY_SAMPLE_SIZE = "compression.dictionary.sample.size";
  public static final int DEFAULT_COMPRESSION_DICTIONARY_SAMPLE_SIZE = 200 * BYTES_PER_MB; // 200MB
  /** Maximum final dictionary size TODO add more details about the current limits */
  public static final String COMPRESSION_DICTIONARY_SIZE_LIMIT = "compression.dictionary.size.limit";

  /**
   * Config to control the thread pool size for HDFS operations.
   */
  public static final String HDFS_OPERATIONS_PARALLEL_THREAD_NUM = "hdfs.operations.parallel.thread.num";
  /**
   * Since the job is calculating the raw data file size, which is not accurate because of compression,
   * key/value schema and backend storage overhead, we are applying this factor to provide a more
   * reasonable estimation.
   *
   * TODO: for map-reduce job, we could come up with more accurate estimation.
   */
  public static final long INPUT_DATA_SIZE_FACTOR = 2;

  protected final VenicePushJob.StoreSetting storeSetting;
  protected final VenicePushJob.PushJobSetting pushJobSetting;
  protected PushJobZstdConfig pushJobZstdConfig;
  protected final VeniceProperties props;
  /**
   * Thread pool for Hadoop File System operations: Lazy initialization as this
   * is not needed when {@link VenicePushJob.PushJobSetting#useMapperToBuildDict} is true
   */
  protected final Lazy<ExecutorService> hdfsExecutorService;

  DefaultInputDataInfoProvider(
      VenicePushJob.StoreSetting storeSetting,
      VenicePushJob.PushJobSetting pushJobSetting,
      VeniceProperties props) {
    this.storeSetting = storeSetting;
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
    FileSystem fs = FileSystem.get(new Configuration());
    Path srcPath = new Path(inputUri);
    FileStatus[] fileStatuses = fs.listStatus(srcPath, PATH_FILTER);

    if (fileStatuses == null || fileStatuses.length == 0) {
      throw new RuntimeException("No data found at source path: " + srcPath);
    }

    if (!pushJobSetting.isIncrementalPush && !pushJobSetting.useMapperToBuildDict) {
      if (this.storeSetting.compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
        LOGGER.info("Zstd compression enabled for {}", pushJobSetting.storeName);
        initZstdConfig(fileStatuses.length);
      }
    }

    PushJobSchemaInfo pushJobSchemaInfo = new PushJobSchemaInfo();
    // try reading the file via sequence file reader. It indicates Vson input if it is succeeded.
    Map<String, String> fileMetadata = getMetadataFromSequenceFile(fs, fileStatuses[0].getPath(), false);
    if (fileMetadata.containsKey(FILE_KEY_SCHEMA) && fileMetadata.containsKey(FILE_VALUE_SCHEMA)) {
      pushJobSchemaInfo.setAvro(false);
      pushJobSchemaInfo.setVsonFileKeySchema(fileMetadata.get(FILE_KEY_SCHEMA));
      pushJobSchemaInfo.setVsonFileValueSchema(fileMetadata.get(FILE_VALUE_SCHEMA));
    }
    // Check the first file type prior to check schema consistency to make sure a schema can be obtained from it.
    if (fileStatuses[0].isDirectory()) {
      throw new VeniceException(
          "Input directory: " + fileStatuses[0].getPath().getParent().getName() + " should not have sub directory: "
              + fileStatuses[0].getPath().getName());
    }

    final AtomicLong inputFileDataSize = new AtomicLong(0);
    if (pushJobSchemaInfo.isAvro()) {
      LOGGER.info("Detected Avro input format.");
      pushJobSchemaInfo.setKeyField(props.getString(KEY_FIELD_PROP, DEFAULT_KEY_FIELD_PROP));
      pushJobSchemaInfo.setValueField(props.getString(VALUE_FIELD_PROP, DEFAULT_VALUE_FIELD_PROP));

      if (!pushJobSetting.useMapperToBuildDict) {
        pushJobSchemaInfo.setAvroSchema(checkAvroSchemaConsistency(fs, fileStatuses, inputFileDataSize));
      } else {
        pushJobSchemaInfo.setAvroSchema(getAvroFileHeader(fs, fileStatuses[0].getPath(), false));
      }

      Schema fileSchema = pushJobSchemaInfo.getAvroSchema().getFirst();
      Schema storeSchema = pushJobSchemaInfo.getAvroSchema().getSecond();

      pushJobSchemaInfo.setFileSchemaString(fileSchema.toString());
      pushJobSchemaInfo
          .setKeySchemaString(extractAvroSubSchema(storeSchema, pushJobSchemaInfo.getKeyField()).toString());
      pushJobSchemaInfo
          .setValueSchemaString(extractAvroSubSchema(storeSchema, pushJobSchemaInfo.getValueField()).toString());
    } else {
      LOGGER.info("Detected Vson input format, will convert to Avro automatically.");
      // key / value fields are optional for Vson input
      pushJobSchemaInfo.setKeyField(props.getString(KEY_FIELD_PROP, ""));
      pushJobSchemaInfo.setValueField(props.getString(VALUE_FIELD_PROP, ""));

      if (!pushJobSetting.useMapperToBuildDict) {
        pushJobSchemaInfo.setVsonSchema(checkVsonSchemaConsistency(fs, fileStatuses, inputFileDataSize));
      } else {
        pushJobSchemaInfo.setVsonSchema(getVsonFileHeader(fs, fileStatuses[0].getPath(), false));
      }

      VsonSchema vsonKeySchema = StringUtils.isEmpty(pushJobSchemaInfo.getKeyField())
          ? pushJobSchemaInfo.getVsonSchema().getFirst()
          : pushJobSchemaInfo.getVsonSchema().getFirst().recordSubtype(pushJobSchemaInfo.getKeyField());
      VsonSchema vsonValueSchema = StringUtils.isEmpty(pushJobSchemaInfo.getValueField())
          ? pushJobSchemaInfo.getVsonSchema().getSecond()
          : pushJobSchemaInfo.getVsonSchema().getSecond().recordSubtype(pushJobSchemaInfo.getValueField());

      pushJobSchemaInfo.setKeySchemaString(VsonAvroSchemaAdapter.parse(vsonKeySchema.toString()).toString());
      pushJobSchemaInfo.setValueSchemaString(VsonAvroSchemaAdapter.parse(vsonValueSchema.toString()).toString());
    }

    return new InputDataInfo(
        pushJobSchemaInfo,
        inputFileDataSize.get() * INPUT_DATA_SIZE_FACTOR,
        fileStatuses.length,
        hasRecords(pushJobSchemaInfo.isAvro(), fs, fileStatuses),
        inputModificationTime,
        !pushJobSetting.useMapperToBuildDict);
  }

  private boolean hasRecords(boolean isAvroFile, FileSystem fs, FileStatus[] fileStatusList) {
    for (FileStatus fileStatus: fileStatusList) {
      AbstractVeniceRecordReader recordReader = isAvroFile
          ? getVeniceAvroRecordReader(fs, fileStatus.getPath())
          : getVeniceVsonRecordReader(fs, fileStatus.getPath());
      if (recordReader.iterator().hasNext()) {
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
      Pair<VsonSchema, VsonSchema> newSchema = getVsonFileHeader(fs, fileStatus.getPath(), true);
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
    LOGGER.debug("path:{}", path.toUri().getPath());
    VeniceVsonRecordReader recordReader = getVeniceVsonRecordReader(fs, path);

    if (isZstdDictCreationRequired) {
      if (!pushJobSetting.isIncrementalPush) {
        if (!pushJobSetting.useMapperToBuildDict) {
          /** If dictionary compression is enabled for version, read the records to get training samples */
          if (storeSetting.compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
            InputDataInfoProvider.loadZstdTrainingSamples(recordReader, pushJobZstdConfig);
          }
        } else {
          /** If dictionary compression is enabled for version or compression metric collection is enabled,
           * read the records to get training samples
           */
          InputDataInfoProvider.loadZstdTrainingSamples(recordReader, pushJobZstdConfig);
        }
      }
    }
    return recordReader.getMetadataMap();
  }

  private VeniceVsonRecordReader getVeniceVsonRecordReader(FileSystem fs, Path path) {
    String keyField = props.getString(KEY_FIELD_PROP, "");
    String valueField = props.getString(VALUE_FIELD_PROP, "");
    return new VeniceVsonRecordReader(null, keyField, valueField, fs, path);
  }

  @Override
  public byte[] getZstdDictTrainSamples() {
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
    FileSystem fs = FileSystem.get(new Configuration());
    Path srcPath = new Path(inputUri);
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
      Pair<Schema, Schema> newSchema = getAvroFileHeader(fs, fileStatus.getPath(), true);
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
    LOGGER.debug("path:{}", path.toUri().getPath());
    VeniceAvroRecordReader recordReader = getVeniceAvroRecordReader(fs, path);

    if (isZstdDictCreationRequired) {
      if (!pushJobSetting.isIncrementalPush) {
        if (!pushJobSetting.useMapperToBuildDict) {
          /** If dictionary compression is enabled for version, read the records to get training samples */
          if (storeSetting.compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
            InputDataInfoProvider.loadZstdTrainingSamples(recordReader, pushJobZstdConfig);
          }
        } else {
          /** If dictionary compression is enabled for version or compression metric collection is enabled,
           * read the records to get training samples
           */
          InputDataInfoProvider.loadZstdTrainingSamples(recordReader, pushJobZstdConfig);
        }
      }
    }
    return new Pair<>(recordReader.getFileSchema(), recordReader.getStoreSchema());
  }

  private VeniceAvroRecordReader getVeniceAvroRecordReader(FileSystem fs, Path path) {
    String keyField = props.getString(KEY_FIELD_PROP, DEFAULT_KEY_FIELD_PROP);
    String valueField = props.getString(VALUE_FIELD_PROP, DEFAULT_VALUE_FIELD_PROP);
    return new VeniceAvroRecordReader(
        null,
        keyField,
        valueField,
        fs,
        path,
        pushJobSetting.etlValueSchemaTransformation);
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
