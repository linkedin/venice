package com.linkedin.venice.spark.datawriter.jobs;

import static com.linkedin.venice.ConfigKeys.KAFKA_CONFIG_PREFIX;
import static com.linkedin.venice.meta.Store.UNLIMITED_STORAGE_QUOTA;
import static com.linkedin.venice.spark.SparkConstants.KEY_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.SPARK_APP_NAME_CONFIG;
import static com.linkedin.venice.spark.SparkConstants.SPARK_DATA_WRITER_CONF_PREFIX;
import static com.linkedin.venice.spark.SparkConstants.SPARK_SESSION_CONF_PREFIX;
import static com.linkedin.venice.spark.SparkConstants.VALUE_COLUMN_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static org.apache.spark.sql.types.DataTypes.BinaryType;
import static org.apache.spark.sql.types.DataTypes.StringType;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.etl.ETLValueSchemaTransformation;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.PushJobSetting;
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
  public void testValidateDataFrameSchema() throws IOException {
    File inputDir = TestWriteUtils.getTempDataDirectory();
    Schema dataSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir);
    PushJobSetting setting = getDefaultPushJobSetting(inputDir, dataSchema);
    Properties properties = new Properties();
    try (DataWriterComputeJob computeJob = new InvalidKeySchemaDataWriterSparkJob()) {
      Assert.assertThrows(VeniceException.class, () -> computeJob.configure(new VeniceProperties(properties), setting));
    }

    try (DataWriterComputeJob computeJob = new InvalidValueSchemaDataWriterSparkJob()) {
      Assert.assertThrows(VeniceException.class, () -> computeJob.configure(new VeniceProperties(properties), setting));
    }

    try (DataWriterComputeJob computeJob = new IncompleteFieldDataWriterSparkJob()) {
      Assert.assertThrows(VeniceException.class, () -> computeJob.configure(new VeniceProperties(properties), setting));
    }

    try (DataWriterComputeJob computeJob = new MissingKeyFieldDataWriterSparkJob()) {
      Assert.assertThrows(VeniceException.class, () -> computeJob.configure(new VeniceProperties(properties), setting));
    }

    try (DataWriterComputeJob computeJob = new MissingValueFieldDataWriterSparkJob()) {
      Assert.assertThrows(VeniceException.class, () -> computeJob.configure(new VeniceProperties(properties), setting));
    }

    try (DataWriterComputeJob computeJob = new SchemaWithRestrictedFieldDataWriterSparkJob()) {
      Assert.assertThrows(VeniceException.class, () -> computeJob.configure(new VeniceProperties(properties), setting));
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
  }
}
