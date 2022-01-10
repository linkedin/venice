package com.linkedin.venice.hadoop;

import com.github.luben.zstd.ZstdDictTrainer;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.exceptions.VeniceInconsistentSchemaException;
import com.linkedin.venice.hadoop.exceptions.VeniceSchemaFieldNotFoundException;
import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import com.linkedin.venice.schema.vson.VsonSchema;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.Iterator;
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
  //Vson input configs
  //Vson files store key/value schema on file header. key / value fields are optional
  //and should be specified only when key / value schema is the partial of the files.
  public final static String FILE_KEY_SCHEMA = "key.schema";
  public final static String FILE_VALUE_SCHEMA = "value.schema";
  public static final String KEY_FIELD_PROP = "key.field";
  public static final String VALUE_FIELD_PROP = "value.field";
  public static final String COMPRESSION_DICTIONARY_SAMPLE_SIZE = "compression.dictionary.sample.size";
  public static final String COMPRESSION_DICTIONARY_SIZE_LIMIT = "compression.dictionary.size.limit";
  /**
   * Config to control the thread pool size for HDFS operations.
   */
  public static final String HDFS_OPERATIONS_PARALLEL_THREAD_NUM = "hdfs.operations.parallel.thread.num";
  /**
   * Since the job is calculating the raw data file size, which is not accurate because of compression,
   * key/value schema and backend storage overhead, we are applying this factor to provide a more
   * reasonable estimation.
   */
  /**
   * TODO: for map-reduce job, we could come up with more accurate estimation.
   */
  public static final long INPUT_DATA_SIZE_FACTOR = 2;

  protected final VenicePushJob.StoreSetting storeSetting;
  protected final VenicePushJob.PushJobSetting pushJobSetting;
  protected VenicePushJob.ZstdConfig zstdConfig;
  protected final VeniceProperties props;
  // Thread pool for Hadoop File System operations.
  protected final ExecutorService hdfsExecutorService;

  DefaultInputDataInfoProvider(
      VenicePushJob.StoreSetting storeSetting,
      VenicePushJob.PushJobSetting pushJobSetting,
      VeniceProperties props
  ) {
    this.storeSetting = storeSetting;
    this.pushJobSetting = pushJobSetting;
    this.props = props;
    this.hdfsExecutorService = Executors.newFixedThreadPool(props.getInt(HDFS_OPERATIONS_PARALLEL_THREAD_NUM, 20));
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
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path srcPath = new Path(inputUri);
    FileStatus[] fileStatuses = fs.listStatus(srcPath, PATH_FILTER);

    if (fileStatuses == null || fileStatuses.length == 0) {
      throw new RuntimeException("No data found at source path: " + srcPath);
    }

    if (this.storeSetting.compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
      LOGGER.info("Zstd compression enabled for " + pushJobSetting.storeName);
      initZstdConfig(fileStatuses.length);
    }

    VenicePushJob.SchemaInfo schemaInfo = new VenicePushJob.SchemaInfo();
    //try reading the file via sequence file reader. It indicates Vson input if it is succeeded.
    Map<String, String> fileMetadata = getMetadataFromSequenceFile(fs, fileStatuses[0].getPath(), false);
    if (fileMetadata.containsKey(FILE_KEY_SCHEMA) && fileMetadata.containsKey(FILE_VALUE_SCHEMA)) {
      schemaInfo.isAvro = false;
      schemaInfo.vsonFileKeySchema = fileMetadata.get(FILE_KEY_SCHEMA);
      schemaInfo.vsonFileValueSchema = fileMetadata.get(FILE_VALUE_SCHEMA);
    }
    // Check the first file type prior to check schema consistency to make sure a schema can be obtained from it.
    if (fileStatuses[0].isDirectory()) {
      throw new VeniceException(
          "Input directory: " + fileStatuses[0].getPath().getParent().getName() + " should not have sub directory: " + fileStatuses[0].getPath().getName());
    }

    final AtomicLong inputFileDataSize = new AtomicLong(0);
    if (schemaInfo.isAvro) {
      LOGGER.info("Detected Avro input format.");
      schemaInfo.keyField = props.getString(KEY_FIELD_PROP);
      schemaInfo.valueField = props.getString(VALUE_FIELD_PROP);

      Pair<Schema, Schema> avroSchema = checkAvroSchemaConsistency(fs, fileStatuses, inputFileDataSize);

      Schema fileSchema = avroSchema.getFirst();
      Schema storeSchema = avroSchema.getSecond();

      schemaInfo.fileSchemaString = fileSchema.toString();
      schemaInfo.keySchemaString = extractAvroSubSchema(storeSchema, schemaInfo.keyField).toString();
      schemaInfo.valueSchemaString = extractAvroSubSchema(storeSchema, schemaInfo.valueField).toString();
    } else {
      LOGGER.info("Detected Vson input format, will convert to Avro automatically.");
      //key / value fields are optional for Vson input
      schemaInfo.keyField = props.getString(KEY_FIELD_PROP, "");
      schemaInfo.valueField = props.getString(VALUE_FIELD_PROP, "");

      Pair<VsonSchema, VsonSchema> vsonSchemaPair = checkVsonSchemaConsistency(fs, fileStatuses, inputFileDataSize);

      VsonSchema vsonKeySchema = StringUtils.isEmpty(schemaInfo.keyField) ? vsonSchemaPair.getFirst()
          : vsonSchemaPair.getFirst().recordSubtype(schemaInfo.keyField);
      VsonSchema vsonValueSchema = StringUtils.isEmpty(schemaInfo.valueField) ? vsonSchemaPair.getSecond()
          : vsonSchemaPair.getSecond().recordSubtype(schemaInfo.valueField);

      schemaInfo.keySchemaString = VsonAvroSchemaAdapter.parse(vsonKeySchema.toString()).toString();
      schemaInfo.valueSchemaString = VsonAvroSchemaAdapter.parse(vsonValueSchema.toString()).toString();
    }

    // Since the job is calculating the raw data file size, which is not accurate because of compression, key/value schema and backend storage overhead,
    // we are applying this factor to provide a more reasonable estimation.
    return new InputDataInfo(
        schemaInfo,
        inputFileDataSize.get() * INPUT_DATA_SIZE_FACTOR,
        hasRecords(schemaInfo.isAvro, fs, fileStatuses)
    );
  }

  private boolean hasRecords(boolean isAvroFile, FileSystem fs, FileStatus[] fileStatusList) {
    for (FileStatus fileStatus : fileStatusList) {
      AbstractVeniceRecordReader recordReader = isAvroFile ?
          getVeniceAvroRecordReader(fs, fileStatus.getPath()) : getVeniceVsonRecordReader(fs, fileStatus.getPath());
      if (recordReader.iterator().hasNext()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void initZstdConfig(int numFiles) {
    if (zstdConfig != null) {
      return;
    }
    zstdConfig = new VenicePushJob.ZstdConfig();
    zstdConfig.dictSize = props.getInt(COMPRESSION_DICTIONARY_SIZE_LIMIT, VeniceWriter.DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES);
    zstdConfig.sampleSize = props.getInt(COMPRESSION_DICTIONARY_SAMPLE_SIZE, 200 * 1024 * 1024); // 200 MB samples
    zstdConfig.maxBytesPerFile = zstdConfig.sampleSize / numFiles;
    zstdConfig.zstdDictTrainer = new ZstdDictTrainer(zstdConfig.sampleSize, zstdConfig.dictSize);
  }


  //Vson-based file store key / value schema string as separated properties in file header
  private Pair<VsonSchema, VsonSchema> checkVsonSchemaConsistency(FileSystem fs, FileStatus[] fileStatusList,
      AtomicLong inputFileDataSize) {
    Pair<VsonSchema, VsonSchema> vsonSchema = getVsonFileHeader(fs, fileStatusList[0].getPath(), false);
    parallelExecuteHDFSOperation(fileStatusList, "checkVsonSchemaConsistency", fileStatus -> {
      if (fileStatus.isDirectory()) {
        throw new VeniceException(
            "Input directory: " + fileStatus.getPath().getParent().getName() + " should not have sub directory: " + fileStatus.getPath().getName());
      }
      inputFileDataSize.addAndGet(fileStatus.getLen());
      Pair<VsonSchema, VsonSchema> newSchema = getVsonFileHeader(fs, fileStatus.getPath(), true);
      if (!vsonSchema.getFirst().equals(newSchema.getFirst()) || !vsonSchema.getSecond().equals(newSchema.getSecond())) {
        throw new VeniceInconsistentSchemaException(String.format(
            "Inconsistent file Vson schema found. File: %s.\n Expected key schema: %s.\n"
                + "Expected value schema: %s.\n File key schema: %s.\n File value schema: %s.", fileStatus.getPath().getName(),
            vsonSchema.getFirst().toString(), vsonSchema.getSecond().toString(), newSchema.getFirst().toString(), newSchema.getSecond().toString()));
      }
    });

    return vsonSchema;
  }

  private Pair<VsonSchema, VsonSchema> getVsonFileHeader(FileSystem fs, Path path, boolean buildDictionary) {
    Map<String, String> fileMetadata = getMetadataFromSequenceFile(fs, path, buildDictionary);
    if (!fileMetadata.containsKey(FILE_KEY_SCHEMA) || !fileMetadata.containsKey(FILE_VALUE_SCHEMA)) {
      throw new VeniceException("Can't find Vson schema from file: " + path.getName());
    }

    return new Pair<>(VsonSchema.parse(fileMetadata.get(FILE_KEY_SCHEMA)),
        VsonSchema.parse(fileMetadata.get(FILE_VALUE_SCHEMA)));
  }

  /**
   * This function is to execute HDFS operation in parallel
   */
  private void parallelExecuteHDFSOperation(final FileStatus[] fileStatusList, String operation, Consumer<FileStatus> fileStatusConsumer) {
    if (hdfsExecutorService == null) {
      throw new VeniceException("Unable to execute HDFS operations in parallel, the executor is uninitialized");
    }
    if (hdfsExecutorService.isShutdown()) {
      throw new VeniceException("Unable to execute HDFS operations in parallel, the executor has already been shutdown");
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

  private Map<String, String> getMetadataFromSequenceFile(FileSystem fs, Path path, boolean buildDictionary) {
    LOGGER.debug("path:" + path.toUri().getPath());
    VeniceVsonRecordReader recordReader = getVeniceVsonRecordReader(fs, path);
    // If dictionary compression is enabled for version, read the records to get training samples
    if (buildDictionary && storeSetting.compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
      loadZstdTrainingSamples(recordReader);
    }
    return recordReader.getMetadataMap();
  }

  private VeniceVsonRecordReader getVeniceVsonRecordReader(FileSystem fs, Path path) {
    String keyField = props.getString(KEY_FIELD_PROP, "");
    String valueField = props.getString(VALUE_FIELD_PROP, "");
    return new VeniceVsonRecordReader(null, keyField, valueField, fs, path);
  }

  /**
   * This function loads training samples from an Avro file for building the Zstd dictionary.
   * @param recordReader The data accessor of input records.
   */
  @Override
  public void loadZstdTrainingSamples(AbstractVeniceRecordReader recordReader) {
    int fileSampleSize = 0;
    Iterator<Pair<byte[], byte[]>> it = recordReader.iterator();
    while (it.hasNext()) {
      Pair<byte[], byte[]> record = it.next();
      if (record == null) {
        continue;
      }

      byte[] data = record.getSecond();

      if (data == null || data.length == 0) {
        continue;
      }

      if (fileSampleSize + data.length > this.zstdConfig.maxBytesPerFile) {
        String perFileLimitErrorMsg = String.format("Read %s to build dictionary. Reached limit per file of %s.",
            ByteUtils.generateHumanReadableByteCountString(fileSampleSize),
            ByteUtils.generateHumanReadableByteCountString(this.zstdConfig.maxBytesPerFile));
        LOGGER.debug(perFileLimitErrorMsg);
        return;
      }

      // addSample returns false when the data read no longer fits in the 'sample' buffer limit
      if (!this.zstdConfig.zstdDictTrainer.addSample(data)) {
        String maxSamplesReadErrorMsg = String.format("Read %s to build dictionary. Reached sample limit of %s.",
            ByteUtils.generateHumanReadableByteCountString(fileSampleSize),
            ByteUtils.generateHumanReadableByteCountString(this.zstdConfig.sampleSize));
        LOGGER.debug(maxSamplesReadErrorMsg);
        return;
      }
      fileSampleSize += data.length;
    }

    LOGGER.debug(String.format("Read %s to build dictionary. Reached EOF.", ByteUtils.generateHumanReadableByteCountString(fileSampleSize)));
  }

  @Override
  public byte[] getZstdDictTrainSamples() {
    return zstdConfig.zstdDictTrainer.trainSamples();
  }

  @Override
  public Schema extractAvroSubSchema(Schema origin, String fieldName) {
    Schema.Field field = origin.getField(fieldName);

    if (field == null) {
      throw new VeniceSchemaFieldNotFoundException(fieldName, "Could not find field: " + fieldName + " from " + origin.toString());
    }

    return field.schema();
  }

  //Avro-based file composes key and value schema as a whole
  private Pair<Schema, Schema> checkAvroSchemaConsistency(FileSystem fs, FileStatus[] fileStatusList, AtomicLong inputFileDataSize) {
    Pair<Schema, Schema> avroSchema = getAvroFileHeader(fs, fileStatusList[0].getPath(), false);
    parallelExecuteHDFSOperation(fileStatusList, "checkAvroSchemaConsistency", fileStatus -> {
      if (fileStatus.isDirectory()) {
        // Map-reduce job will fail if the input directory has sub-directory and 'recursive' is not specified.
        throw new VeniceException("Input directory: " + fileStatus.getPath().getParent().getName() +
            " should not have sub directory: " + fileStatus.getPath().getName());
      }
      inputFileDataSize.addAndGet(fileStatus.getLen());
      Pair<Schema, Schema> newSchema = getAvroFileHeader(fs, fileStatus.getPath(), true);
      if (!avroSchema.equals(newSchema)) {
        throw new VeniceInconsistentSchemaException(String.format("Inconsistent file Avro schema found. File: %s.\n"
                + "Expected file schema: %s.\n Real File schema: %s.", fileStatus.getPath().getName(),
            avroSchema.toString(), newSchema.toString()));
      }
    });
    return avroSchema;
  }

  private Pair<Schema, Schema> getAvroFileHeader(FileSystem fs, Path path, boolean buildDictionary) {
    LOGGER.debug("path:" + path.toUri().getPath());
    VeniceAvroRecordReader recordReader = getVeniceAvroRecordReader(fs, path);
    // If dictionary compression is enabled for version, read the records to get training samples
    if (buildDictionary && storeSetting.compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
      loadZstdTrainingSamples(recordReader);
    }
    return new Pair<>(recordReader.getFileSchema(), recordReader.getStoreSchema());
  }

  private VeniceAvroRecordReader getVeniceAvroRecordReader(FileSystem fs, Path path) {
    String keyField = props.getString(KEY_FIELD_PROP);
    String valueField = props.getString(VALUE_FIELD_PROP);
    return new VeniceAvroRecordReader(null, keyField, valueField, fs, path, pushJobSetting.etlValueSchemaTransformation);
  }

  @Override
  public void close() {
    shutdownHdfsExecutorService();
  }

  private void shutdownHdfsExecutorService() {
    if (hdfsExecutorService == null) {
      LOGGER.warn("No HDFS executor service to shutdown");
      return;
    }
    hdfsExecutorService.shutdownNow();
    try {
      if (!hdfsExecutorService.awaitTermination(30, TimeUnit.SECONDS)) {
        LOGGER.warn("Unable to shutdown the executor service used for HDFS operations. "
            + "The job may hang with leaked resources.");
      }
    } catch (InterruptedException e) {
      LOGGER.error(e);
      Thread.currentThread().interrupt();
    }
  }
}
