package com.linkedin.venice.spark.datawriter.jobs;

import static com.linkedin.venice.ConfigKeys.KAFKA_CONFIG_PREFIX;
import static com.linkedin.venice.meta.Store.UNLIMITED_STORAGE_QUOTA;
import static com.linkedin.venice.spark.SparkConstants.CHUNKED_KEY_SUFFIX_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.KEY_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.MESSAGE_TYPE_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.OFFSET_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.RMD_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.RMD_VERSION_ID_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.SCHEMA_ID_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.SPARK_APP_NAME_CONFIG;
import static com.linkedin.venice.spark.SparkConstants.SPARK_DATA_WRITER_CONF_PREFIX;
import static com.linkedin.venice.spark.SparkConstants.SPARK_SESSION_CONF_PREFIX;
import static com.linkedin.venice.spark.SparkConstants.VALUE_COLUMN_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static org.apache.spark.sql.types.DataTypes.BinaryType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.etl.ETLValueSchemaTransformation;
import com.linkedin.venice.hadoop.PushJobSetting;
import com.linkedin.venice.hadoop.exceptions.VeniceInvalidInputException;
import com.linkedin.venice.jobs.ComputeJob;
import com.linkedin.venice.jobs.DataWriterComputeJob;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AbstractDataWriterSparkJobTest {
  @Test
  public void testConfigure() throws IOException {
    File inputDir = TestWriteUtils.getTempDataDirectory();
    Schema dataSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);

    PushJobSetting setting = getDefaultPushJobSetting(inputDir, dataSchema);
    String sparkAppNameOverride = "UPDATED_SPARK_APP_NAME";
    String dummyKafkaConfig = "dummy.kafka.config";
    String dummyKafkaConfigValue = "dummy.kafka.config.value";
    String dummyConfig = "some.dummy.config";
    String dummyConfigValue = "some.dummy.config.value";

    Properties properties = new Properties();
    properties.setProperty(SPARK_SESSION_CONF_PREFIX + SPARK_APP_NAME_CONFIG, sparkAppNameOverride);
    properties.setProperty(KAFKA_CONFIG_PREFIX + dummyKafkaConfig, dummyKafkaConfigValue);
    properties.setProperty(SPARK_DATA_WRITER_CONF_PREFIX + dummyConfig, dummyConfigValue);

    try (DataWriterSparkJob dataWriterSparkJob = new DataWriterSparkJob()) {
      dataWriterSparkJob.configure(new VeniceProperties(properties), setting);

      RuntimeConfig jobConf = dataWriterSparkJob.getSparkSession().conf();
      // Builder configs should get applied
      Assert.assertEquals(jobConf.get(SPARK_APP_NAME_CONFIG), sparkAppNameOverride);

      // Pass through properties should get applied without stripping the prefix
      Assert.assertEquals(jobConf.get(KAFKA_CONFIG_PREFIX + dummyKafkaConfig), dummyKafkaConfigValue);

      // Properties with SPARK_DATA_WRITER_CONF_PREFIX should get applied after stripping the prefix
      Assert.assertEquals(jobConf.get(dummyConfig), dummyConfigValue);
    }
  }

  @Test
  public void testValidateRmdSchema() {
    PushJobSetting pushJobSetting = new PushJobSetting();
    AbstractDataWriterSparkJob dataWriterSparkJob = spy(AbstractDataWriterSparkJob.class);
    dataWriterSparkJob.validateRmdSchema(pushJobSetting);

    final String rmdField = "rmd";
    Schema mockSchema = mock(Schema.class);
    Schema rmdSchema = Schema.create(Schema.Type.LONG);
    Schema.Field mockField = mock(Schema.Field.class);
    when(mockSchema.getField(eq(rmdField))).thenReturn(mockField);
    when(mockField.schema()).thenReturn(rmdSchema);
    pushJobSetting.rmdField = rmdField;
    pushJobSetting.inputDataSchema = mockSchema;

    dataWriterSparkJob.validateRmdSchema(pushJobSetting);
  }

  @Test(expectedExceptions = VeniceInvalidInputException.class, expectedExceptionsMessageRegExp = "The provided input rmd schema must be BinaryType.*")
  public void testValidateDataFrameWithInvalidRmdTypes() {
    AbstractDataWriterSparkJob dataWriterSparkJob = spy(AbstractDataWriterSparkJob.class);
    StructType inputStructType = new StructType(
        new StructField[] { new StructField(KEY_COLUMN_NAME, BinaryType, false, Metadata.empty()),
            new StructField(VALUE_COLUMN_NAME, BinaryType, true, Metadata.empty()),
            new StructField(RMD_COLUMN_NAME, StringType, true, Metadata.empty()) });

    Dataset<Row> mockDataset = mock(Dataset.class);
    when(mockDataset.schema()).thenReturn(inputStructType);
    dataWriterSparkJob.validateDataFrame(mockDataset);
  }

  @Test
  public void testValidateDataFrameWithValidRmdType() {
    AbstractDataWriterSparkJob dataWriterSparkJob = spy(AbstractDataWriterSparkJob.class);
    StructType inputWithBinaryRmdType = new StructType(
        new StructField[] { new StructField(KEY_COLUMN_NAME, BinaryType, false, Metadata.empty()),
            new StructField(VALUE_COLUMN_NAME, BinaryType, true, Metadata.empty()),
            new StructField(RMD_COLUMN_NAME, BinaryType, true, Metadata.empty()) });

    Dataset<Row> mockDataset = mock(Dataset.class);
    when(mockDataset.schema()).thenReturn(inputWithBinaryRmdType);
    dataWriterSparkJob.validateDataFrame(mockDataset);
    when(mockDataset.schema()).thenReturn(inputWithBinaryRmdType);
    dataWriterSparkJob.validateDataFrame(mockDataset);
  }

  @Test
  public void testValidateDataFrameWithChunkedKifColumns() {
    PushJobSetting kafkaSetting = new PushJobSetting();
    kafkaSetting.isSourceKafka = true;

    AbstractDataWriterSparkJob dataWriterSparkJob = spy(AbstractDataWriterSparkJob.class);
    when(dataWriterSparkJob.getPushJobSetting()).thenReturn(kafkaSetting);

    // Schema matching chunked KIF repush input: key, value, rmd + internal columns for chunk assembly
    StructType chunkedKifSchema = new StructType(
        new StructField[] { new StructField(KEY_COLUMN_NAME, BinaryType, false, Metadata.empty()),
            new StructField(VALUE_COLUMN_NAME, BinaryType, true, Metadata.empty()),
            new StructField(RMD_COLUMN_NAME, BinaryType, true, Metadata.empty()),
            new StructField(SCHEMA_ID_COLUMN_NAME, IntegerType, false, Metadata.empty()),
            new StructField(RMD_VERSION_ID_COLUMN_NAME, IntegerType, false, Metadata.empty()),
            new StructField(OFFSET_COLUMN_NAME, LongType, false, Metadata.empty()),
            new StructField(MESSAGE_TYPE_COLUMN_NAME, IntegerType, false, Metadata.empty()),
            new StructField(CHUNKED_KEY_SUFFIX_COLUMN_NAME, BinaryType, true, Metadata.empty()) });

    Dataset<Row> mockDataset = mock(Dataset.class);
    when(mockDataset.schema()).thenReturn(chunkedKifSchema);
    dataWriterSparkJob.validateDataFrame(mockDataset);
  }

  @Test(expectedExceptions = VeniceInvalidInputException.class, expectedExceptionsMessageRegExp = ".*must not have fields that start with an underscore.*__schema_id__.*")
  public void testValidateDataFrameRejectsInternalColumnsForNonKifJob() {
    PushJobSetting hdfsSetting = new PushJobSetting();
    hdfsSetting.isSourceKafka = false;

    AbstractDataWriterSparkJob dataWriterSparkJob = spy(AbstractDataWriterSparkJob.class);
    when(dataWriterSparkJob.getPushJobSetting()).thenReturn(hdfsSetting);

    // Same chunked KIF schema but on a non-KIF job — should be rejected
    StructType chunkedKifSchema = new StructType(
        new StructField[] { new StructField(KEY_COLUMN_NAME, BinaryType, false, Metadata.empty()),
            new StructField(VALUE_COLUMN_NAME, BinaryType, true, Metadata.empty()),
            new StructField(RMD_COLUMN_NAME, BinaryType, true, Metadata.empty()),
            new StructField(SCHEMA_ID_COLUMN_NAME, IntegerType, false, Metadata.empty()) });

    Dataset<Row> mockDataset = mock(Dataset.class);
    when(mockDataset.schema()).thenReturn(chunkedKifSchema);
    dataWriterSparkJob.validateDataFrame(mockDataset);
  }

  @Test(expectedExceptions = VeniceInvalidInputException.class, expectedExceptionsMessageRegExp = ".*must not have fields that start with an underscore.*_unknown_internal.*")
  public void testValidateDataFrameRejectsUnknownUnderscoreColumnsForKifJob() {
    PushJobSetting kafkaSetting = new PushJobSetting();
    kafkaSetting.isSourceKafka = true;

    AbstractDataWriterSparkJob dataWriterSparkJob = spy(AbstractDataWriterSparkJob.class);
    when(dataWriterSparkJob.getPushJobSetting()).thenReturn(kafkaSetting);

    // KIF job but with an unknown underscore column — should still be rejected
    StructType schemaWithUnknownInternalCol = new StructType(
        new StructField[] { new StructField(KEY_COLUMN_NAME, BinaryType, false, Metadata.empty()),
            new StructField(VALUE_COLUMN_NAME, BinaryType, true, Metadata.empty()),
            new StructField(RMD_COLUMN_NAME, BinaryType, true, Metadata.empty()),
            new StructField("_unknown_internal", StringType, true, Metadata.empty()) });

    Dataset<Row> mockDataset = mock(Dataset.class);
    when(mockDataset.schema()).thenReturn(schemaWithUnknownInternalCol);
    dataWriterSparkJob.validateDataFrame(mockDataset);
  }

  @Test
  public void testValidateDataFrameSchema() throws IOException {
    File inputDir = TestWriteUtils.getTempDataDirectory();
    Schema dataSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    PushJobSetting setting = getDefaultPushJobSetting(inputDir, dataSchema);
    setting.replicationMetadataSchemaString = "";
    Properties properties = new Properties();
    try (DataWriterComputeJob computeJob = new InvalidKeySchemaDataWriterSparkJob()) {
      computeJob.configure(new VeniceProperties(properties), setting);
      computeJob.runJob();
      Assert.assertEquals(computeJob.getStatus(), ComputeJob.Status.FAILED);
      Assert.assertNotNull(computeJob.getFailureReason());
      Assert.assertTrue(computeJob.getFailureReason() instanceof VeniceInvalidInputException);
    }

    try (DataWriterComputeJob computeJob = new InvalidValueSchemaDataWriterSparkJob()) {
      computeJob.configure(new VeniceProperties(properties), setting);
      computeJob.runJob();
      Assert.assertEquals(computeJob.getStatus(), ComputeJob.Status.FAILED);
      Assert.assertNotNull(computeJob.getFailureReason());
      Assert.assertTrue(computeJob.getFailureReason() instanceof VeniceInvalidInputException);
    }

    try (DataWriterComputeJob computeJob = new IncompleteFieldDataWriterSparkJob()) {
      computeJob.configure(new VeniceProperties(properties), setting);
      computeJob.runJob();
      Assert.assertEquals(computeJob.getStatus(), ComputeJob.Status.FAILED);
      Assert.assertNotNull(computeJob.getFailureReason());
      Assert.assertTrue(computeJob.getFailureReason() instanceof VeniceInvalidInputException);
    }

    try (DataWriterComputeJob computeJob = new MissingKeyFieldDataWriterSparkJob()) {
      computeJob.configure(new VeniceProperties(properties), setting);
      computeJob.runJob();
      Assert.assertEquals(computeJob.getStatus(), ComputeJob.Status.FAILED);
      Assert.assertNotNull(computeJob.getFailureReason());
      Assert.assertTrue(computeJob.getFailureReason() instanceof VeniceInvalidInputException);
    }

    try (DataWriterComputeJob computeJob = new MissingValueFieldDataWriterSparkJob()) {
      computeJob.configure(new VeniceProperties(properties), setting);
      computeJob.runJob();
      Assert.assertEquals(computeJob.getStatus(), ComputeJob.Status.FAILED);
      Assert.assertNotNull(computeJob.getFailureReason());
      Assert.assertTrue(computeJob.getFailureReason() instanceof VeniceInvalidInputException);
    }

    try (DataWriterComputeJob computeJob = new SchemaWithRestrictedFieldDataWriterSparkJob()) {
      computeJob.configure(new VeniceProperties(properties), setting);
      computeJob.runJob();
      Assert.assertEquals(computeJob.getStatus(), ComputeJob.Status.FAILED);
      Assert.assertNotNull(computeJob.getFailureReason());
      Assert.assertTrue(computeJob.getFailureReason() instanceof VeniceInvalidInputException);
    }
  }

  private PushJobSetting getDefaultPushJobSetting(File inputDir, Schema dataSchema) {
    PushJobSetting setting = new PushJobSetting();
    setting.storeName = Utils.getUniqueString("TEST_STORE");
    setting.jobId = Utils.getUniqueString("TEST_JOB");
    setting.inputURI = new Path(inputDir.toURI()).toString();
    setting.keyField = DEFAULT_KEY_FIELD_PROP;
    setting.valueField = DEFAULT_VALUE_FIELD_PROP;
    setting.topic = Version.composeKafkaTopic(setting.storeName, 7);
    setting.kafkaUrl = "localhost:9092";
    setting.partitionerClass = DefaultVenicePartitioner.class.getCanonicalName();
    setting.partitionerParams = null;
    setting.sslToKafka = false;
    setting.isDuplicateKeyAllowed = false;
    setting.chunkingEnabled = false;
    setting.rmdChunkingEnabled = false;
    setting.storeKeySchema = Schema.create(Schema.Type.STRING);
    setting.storeStorageQuota = UNLIMITED_STORAGE_QUOTA;
    setting.storeCompressionStrategy = CompressionStrategy.NO_OP;
    setting.sendControlMessagesDirectly = true;
    setting.isSourceETL = false;
    setting.isSourceKafka = false;
    setting.isAvro = true;
    setting.valueSchemaId = 1;
    setting.inputDataSchema = dataSchema;
    setting.inputDataSchemaString = setting.inputDataSchema.toString();
    setting.keySchema = dataSchema.getField(DEFAULT_KEY_FIELD_PROP).schema();
    setting.keySchemaString = setting.keySchema.toString();
    setting.valueSchema = dataSchema.getField(DEFAULT_VALUE_FIELD_PROP).schema();
    setting.valueSchemaString = setting.valueSchema.toString();
    setting.etlValueSchemaTransformation = ETLValueSchemaTransformation.NONE;
    setting.inputHasRecords = true;
    setting.inputFileDataSizeInBytes = 1000;
    setting.topicCompressionStrategy = setting.storeCompressionStrategy;
    return setting;
  }

  private static class InvalidKeySchemaDataWriterSparkJob extends AbstractDataWriterSparkJob {
    private static final StructType INVALID_SCHEMA = new StructType(
        new StructField[] { new StructField(KEY_COLUMN_NAME, StringType, false, Metadata.empty()),
            new StructField(VALUE_COLUMN_NAME, BinaryType, true, Metadata.empty()) });
    private static final RecordSerializer<String> serializer =
        FastSerializerDeserializerFactory.getFastAvroGenericSerializer(Schema.create(Schema.Type.STRING));

    @Override
    protected Dataset<Row> getUserInputDataFrame() {
      SparkSession spark = getSparkSession();
      JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
      RDD<Row> rowRDD = sparkContext.parallelize(Arrays.asList("1", "2", "3"))
          .map(item -> RowFactory.create(item, serializer.serialize(item)))
          .rdd();

      return spark.createDataFrame(rowRDD, INVALID_SCHEMA);
    }

    @Override
    protected Dataset<Row> getKafkaInputDataFrame() {
      return getSparkSession().emptyDataFrame();
    }
  }

  private static class InvalidValueSchemaDataWriterSparkJob extends AbstractDataWriterSparkJob {
    private static final StructType INVALID_SCHEMA = new StructType(
        new StructField[] { new StructField(KEY_COLUMN_NAME, BinaryType, false, Metadata.empty()),
            new StructField(VALUE_COLUMN_NAME, StringType, true, Metadata.empty()) });
    private static final RecordSerializer<String> serializer =
        FastSerializerDeserializerFactory.getFastAvroGenericSerializer(Schema.create(Schema.Type.STRING));

    @Override
    protected Dataset<Row> getUserInputDataFrame() {
      SparkSession spark = getSparkSession();
      JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
      RDD<Row> rowRDD = sparkContext.parallelize(Arrays.asList("1", "2", "3"))
          .map(item -> RowFactory.create(serializer.serialize(item), item))
          .rdd();

      return spark.createDataFrame(rowRDD, INVALID_SCHEMA);
    }

    @Override
    protected Dataset<Row> getKafkaInputDataFrame() {
      return getSparkSession().emptyDataFrame();
    }
  }

  private static class IncompleteFieldDataWriterSparkJob extends AbstractDataWriterSparkJob {
    private static final StructType INVALID_SCHEMA =
        new StructType(new StructField[] { new StructField(KEY_COLUMN_NAME, BinaryType, false, Metadata.empty()) });
    private static final RecordSerializer<String> serializer =
        FastSerializerDeserializerFactory.getFastAvroGenericSerializer(Schema.create(Schema.Type.STRING));

    @Override
    protected Dataset<Row> getUserInputDataFrame() {
      SparkSession spark = getSparkSession();
      JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
      RDD<Row> rowRDD = sparkContext.parallelize(Arrays.asList("1", "2", "3"))
          .map(item -> RowFactory.create((Object) serializer.serialize(item)))
          .rdd();

      return spark.createDataFrame(rowRDD, INVALID_SCHEMA);
    }

    @Override
    protected Dataset<Row> getKafkaInputDataFrame() {
      return getSparkSession().emptyDataFrame();
    }
  }

  private static class MissingKeyFieldDataWriterSparkJob extends AbstractDataWriterSparkJob {
    private static final StructType INVALID_SCHEMA = new StructType(
        new StructField[] { new StructField("DUMMY_FIELD", BinaryType, false, Metadata.empty()),
            new StructField(VALUE_COLUMN_NAME, BinaryType, true, Metadata.empty()) });
    private static final RecordSerializer<String> serializer =
        FastSerializerDeserializerFactory.getFastAvroGenericSerializer(Schema.create(Schema.Type.STRING));

    @Override
    protected Dataset<Row> getUserInputDataFrame() {
      SparkSession spark = getSparkSession();
      JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
      RDD<Row> rowRDD = sparkContext.parallelize(Arrays.asList("1", "2", "3"))
          .map(item -> RowFactory.create(serializer.serialize(item), serializer.serialize(item)))
          .rdd();

      return spark.createDataFrame(rowRDD, INVALID_SCHEMA);
    }

    @Override
    protected Dataset<Row> getKafkaInputDataFrame() {
      return getSparkSession().emptyDataFrame();
    }
  }

  private static class MissingValueFieldDataWriterSparkJob extends AbstractDataWriterSparkJob {
    private static final StructType INVALID_SCHEMA = new StructType(
        new StructField[] { new StructField(KEY_COLUMN_NAME, BinaryType, false, Metadata.empty()),
            new StructField("DUMMY_FIELD", BinaryType, true, Metadata.empty()) });
    private static final RecordSerializer<String> serializer =
        FastSerializerDeserializerFactory.getFastAvroGenericSerializer(Schema.create(Schema.Type.STRING));

    @Override
    protected Dataset<Row> getUserInputDataFrame() {
      SparkSession spark = getSparkSession();
      JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
      RDD<Row> rowRDD = sparkContext.parallelize(Arrays.asList("1", "2", "3"))
          .map(item -> RowFactory.create(serializer.serialize(item), serializer.serialize(item)))
          .rdd();

      return spark.createDataFrame(rowRDD, INVALID_SCHEMA);
    }

    @Override
    protected Dataset<Row> getKafkaInputDataFrame() {
      return getSparkSession().emptyDataFrame();
    }
  }

  private static class SchemaWithRestrictedFieldDataWriterSparkJob extends AbstractDataWriterSparkJob {
    private static final StructType INVALID_SCHEMA = new StructType(
        new StructField[] { new StructField(KEY_COLUMN_NAME, BinaryType, false, Metadata.empty()),
            new StructField(VALUE_COLUMN_NAME, BinaryType, true, Metadata.empty()),
            new StructField("_INTERNAL_FIELD", StringType, true, Metadata.empty()) });
    private static final RecordSerializer<String> serializer =
        FastSerializerDeserializerFactory.getFastAvroGenericSerializer(Schema.create(Schema.Type.STRING));

    @Override
    protected Dataset<Row> getUserInputDataFrame() {
      SparkSession spark = getSparkSession();
      JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
      RDD<Row> rowRDD = sparkContext.parallelize(Arrays.asList("1", "2", "3"))
          .map(item -> RowFactory.create(serializer.serialize(item), serializer.serialize(item), item))
          .rdd();

      return spark.createDataFrame(rowRDD, INVALID_SCHEMA);
    }

    @Override
    protected Dataset<Row> getKafkaInputDataFrame() {
      return getSparkSession().emptyDataFrame();
    }
  }
}
