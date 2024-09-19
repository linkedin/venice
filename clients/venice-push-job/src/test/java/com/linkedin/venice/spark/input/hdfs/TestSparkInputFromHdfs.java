package com.linkedin.venice.spark.input.hdfs;

import static com.linkedin.venice.spark.SparkConstants.DEFAULT_SCHEMA;
import static com.linkedin.venice.spark.input.hdfs.VeniceHdfsInputTable.INPUT_TABLE_NAME;
import static com.linkedin.venice.utils.TestWriteUtils.DEFAULT_USER_DATA_VALUE_PREFIX;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V2_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_TO_STRING_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.FILE_KEY_SCHEMA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.FILE_VALUE_SCHEMA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.GENERATE_PARTIAL_UPDATE_RECORD_FROM_INPUT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INPUT_PATH_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SCHEMA_STRING_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.UPDATE_SCHEMA_STRING_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VSON_PUSH;

import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import com.linkedin.venice.schema.vson.VsonAvroSerializer;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.utils.TestWriteUtils;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestSparkInputFromHdfs {
  private static final Schema AVRO_FILE_SCHEMA = STRING_TO_STRING_SCHEMA;
  private static final String VSON_STRING_SCHEMA = "\"string\"";

  @Test
  public void testHdfsInputSchema() {
    VeniceHdfsSource source = new VeniceHdfsSource();
    Assert.assertEquals(source.inferSchema(null), DEFAULT_SCHEMA);
  }

  @Test
  public void testAvroInputSource() throws IOException {
    File inputDir = TestWriteUtils.getTempDataDirectory();
    Map<String, String> config = getDefaultConfigs(inputDir);

    String file1Path = "string2string1.avro";
    writeAvroFile(inputDir, file1Path, 1, 100);
    String file2Path = "string2string2.avro";
    writeAvroFile(inputDir, file2Path, 101, 200);

    config.put(KEY_FIELD_PROP, DEFAULT_KEY_FIELD_PROP);
    config.put(VALUE_FIELD_PROP, DEFAULT_VALUE_FIELD_PROP);
    config.put(SCHEMA_STRING_PROP, AVRO_FILE_SCHEMA.toString());
    config.put(VSON_PUSH, String.valueOf(false));

    CaseInsensitiveStringMap caseInsensitiveConfig = new CaseInsensitiveStringMap(config);

    VeniceHdfsSource source = new VeniceHdfsSource();
    StructType schema = source.inferSchema(caseInsensitiveConfig);

    Table table = source.getTable(schema, source.inferPartitioning(caseInsensitiveConfig), config);
    Assert.assertTrue(table instanceof VeniceHdfsInputTable);
    Assert.assertTrue(table.capabilities().contains(TableCapability.BATCH_READ));
    Assert.assertEquals(table.schema(), DEFAULT_SCHEMA);
    // Spark docs mention that table's and source's partitioning should be the same
    Assert.assertEquals(table.partitioning(), source.inferPartitioning(caseInsensitiveConfig));
    Assert.assertEquals(table.name(), INPUT_TABLE_NAME);

    VeniceHdfsInputTable hdfsTable = (VeniceHdfsInputTable) table;
    ScanBuilder scanBuilder = hdfsTable.newScanBuilder(caseInsensitiveConfig);
    Assert.assertTrue(scanBuilder instanceof VeniceHdfsInputScanBuilder);

    Scan scan = scanBuilder.build();
    Assert.assertTrue(scan instanceof VeniceHdfsInputScan);
    Assert.assertEquals(scan.readSchema(), DEFAULT_SCHEMA);
    Assert.assertSame(scan.toBatch(), scan);

    VeniceHdfsInputScan hdfsScan = (VeniceHdfsInputScan) scan;
    InputPartition[] partitions = hdfsScan.planInputPartitions();
    Assert.assertEquals(partitions.length, 2);

    for (InputPartition partition: partitions) {
      Assert.assertTrue(partition instanceof VeniceHdfsInputPartition);

      PartitionReaderFactory readerFactory = hdfsScan.createReaderFactory();
      Assert.assertTrue(readerFactory instanceof VeniceHdfsInputPartitionReaderFactory);

      try (PartitionReader<InternalRow> reader = readerFactory.createReader(partition)) {
        Assert.assertTrue(reader instanceof VeniceHdfsInputPartitionReader);
        if (((VeniceHdfsInputPartition) partition).getFilePath().getName().equals(file1Path)) {
          verifyAvroData(reader, 1, 100);
        } else if (((VeniceHdfsInputPartition) partition).getFilePath().getName().equals(file2Path)) {
          verifyAvroData(reader, 101, 200);
        } else {
          Assert.fail("Unexpected partition: " + partition);
        }
      }
    }
  }

  @Test
  public void testAvroInputSourceForPartialUpdate() throws IOException {
    File inputDir = TestWriteUtils.getTempDataDirectory();
    Map<String, String> config = getDefaultConfigs(inputDir);

    writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);

    // Use a newer schema as the one used to generate the update-schema signalling that the input data is only partial
    Schema updateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(NAME_RECORD_V2_SCHEMA);

    config.put(KEY_FIELD_PROP, DEFAULT_KEY_FIELD_PROP);
    config.put(VALUE_FIELD_PROP, DEFAULT_VALUE_FIELD_PROP);
    config.put(SCHEMA_STRING_PROP, AVRO_FILE_SCHEMA.toString());
    config.put(GENERATE_PARTIAL_UPDATE_RECORD_FROM_INPUT, String.valueOf(true));
    config.put(UPDATE_SCHEMA_STRING_PROP, updateSchema.toString());
    config.put(VSON_PUSH, String.valueOf(false));

    CaseInsensitiveStringMap caseInsensitiveConfig = new CaseInsensitiveStringMap(config);

    VeniceHdfsSource source = new VeniceHdfsSource();
    StructType schema = source.inferSchema(caseInsensitiveConfig);

    Table table = source.getTable(schema, source.inferPartitioning(caseInsensitiveConfig), config);
    Assert.assertTrue(table instanceof VeniceHdfsInputTable);
    Assert.assertTrue(table.capabilities().contains(TableCapability.BATCH_READ));
    Assert.assertEquals(table.schema(), DEFAULT_SCHEMA);
    // Spark docs mention that table's and source's partitioning should be the same
    Assert.assertEquals(table.partitioning(), source.inferPartitioning(caseInsensitiveConfig));
    Assert.assertEquals(table.name(), INPUT_TABLE_NAME);

    VeniceHdfsInputTable hdfsTable = (VeniceHdfsInputTable) table;
    ScanBuilder scanBuilder = hdfsTable.newScanBuilder(caseInsensitiveConfig);
    Assert.assertTrue(scanBuilder instanceof VeniceHdfsInputScanBuilder);

    Scan scan = scanBuilder.build();
    Assert.assertTrue(scan instanceof VeniceHdfsInputScan);
    Assert.assertEquals(scan.readSchema(), DEFAULT_SCHEMA);
    Assert.assertSame(scan.toBatch(), scan);

    VeniceHdfsInputScan hdfsScan = (VeniceHdfsInputScan) scan;
    InputPartition[] partitions = hdfsScan.planInputPartitions();
    Assert.assertEquals(partitions.length, 1);

    for (InputPartition partition: partitions) {
      Assert.assertTrue(partition instanceof VeniceHdfsInputPartition);

      PartitionReaderFactory readerFactory = hdfsScan.createReaderFactory();
      Assert.assertTrue(readerFactory instanceof VeniceHdfsInputPartitionReaderFactory);

      try (PartitionReader<InternalRow> reader = readerFactory.createReader(partition)) {
        Assert.assertTrue(reader instanceof VeniceHdfsInputPartitionReader);
        verifyPartialUpdateAvroData(reader, updateSchema);
      }
    }
  }

  @Test
  public void testVsonInputSource() throws IOException {
    File inputDir = TestWriteUtils.getTempDataDirectory();
    Map<String, String> config = getDefaultConfigs(inputDir);

    String file1Path = "string2string1_vson";
    writeVsonFile(inputDir, file1Path, 1, 100);
    String file2Path = "string2string2_vson";
    writeVsonFile(inputDir, file2Path, 101, 200);

    config.put(FILE_KEY_SCHEMA, VSON_STRING_SCHEMA);
    config.put(FILE_VALUE_SCHEMA, VSON_STRING_SCHEMA);
    config.put(KEY_FIELD_PROP, "");
    config.put(VALUE_FIELD_PROP, "");
    config.put(VSON_PUSH, String.valueOf(true));

    CaseInsensitiveStringMap caseInsensitiveConfig = new CaseInsensitiveStringMap(config);

    VeniceHdfsSource source = new VeniceHdfsSource();
    StructType schema = source.inferSchema(caseInsensitiveConfig);

    Table table = source.getTable(schema, source.inferPartitioning(caseInsensitiveConfig), config);
    Assert.assertTrue(table instanceof VeniceHdfsInputTable);
    Assert.assertTrue(table.capabilities().contains(TableCapability.BATCH_READ));
    Assert.assertEquals(table.schema(), DEFAULT_SCHEMA);
    // Spark docs mention that table's and source's partitioning should be the same
    Assert.assertEquals(table.partitioning(), source.inferPartitioning(caseInsensitiveConfig));
    Assert.assertEquals(table.name(), INPUT_TABLE_NAME);

    VeniceHdfsInputTable hdfsTable = (VeniceHdfsInputTable) table;
    ScanBuilder scanBuilder = hdfsTable.newScanBuilder(caseInsensitiveConfig);
    Assert.assertTrue(scanBuilder instanceof VeniceHdfsInputScanBuilder);

    Scan scan = scanBuilder.build();
    Assert.assertTrue(scan instanceof VeniceHdfsInputScan);
    Assert.assertEquals(scan.readSchema(), DEFAULT_SCHEMA);
    Assert.assertSame(scan.toBatch(), scan);

    VeniceHdfsInputScan hdfsScan = (VeniceHdfsInputScan) scan;
    InputPartition[] partitions = hdfsScan.planInputPartitions();
    Assert.assertEquals(partitions.length, 2);

    for (InputPartition partition: partitions) {
      Assert.assertTrue(partition instanceof VeniceHdfsInputPartition);

      PartitionReaderFactory readerFactory = hdfsScan.createReaderFactory();
      Assert.assertTrue(readerFactory instanceof VeniceHdfsInputPartitionReaderFactory);

      try (PartitionReader<InternalRow> reader = readerFactory.createReader(partition)) {
        Assert.assertTrue(reader instanceof VeniceHdfsInputPartitionReader);
        if (((VeniceHdfsInputPartition) partition).getFilePath().getName().equals(file1Path)) {
          verifyVsonData(reader, 1, 100);
        } else if (((VeniceHdfsInputPartition) partition).getFilePath().getName().equals(file2Path)) {
          verifyVsonData(reader, 101, 200);
        } else {
          Assert.fail("Unexpected partition: " + partition);
        }
      }
    }
  }

  @Test
  public void testVeniceHdfsInputPartitionCanHandleSerDe() {
    Path filePath = new Path("test");
    VeniceHdfsInputPartition partition = new VeniceHdfsInputPartition(filePath);
    Assert.assertEquals(partition.getFilePath(), filePath);
    VeniceHdfsInputPartition deserialized = SerializationUtils.deserialize(SerializationUtils.serialize(partition));
    // Check if deserialized object has the same filePath since it is a transient field
    Assert.assertEquals(deserialized.getFilePath(), filePath);
  }

  private Map<String, String> getDefaultConfigs(File inputDir) {
    Map<String, String> config = new HashMap<>();
    config.put(INPUT_PATH_PROP, inputDir.getAbsolutePath());
    return config;
  }

  private void writeAvroFile(File inputDir, String fileName, int start, int end) throws IOException {
    File file = new File(inputDir, fileName);

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(AVRO_FILE_SCHEMA);
    try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(AVRO_FILE_SCHEMA, file);
      for (int i = start; i <= end; ++i) {
        GenericRecord user = new GenericData.Record(AVRO_FILE_SCHEMA);
        user.put(DEFAULT_KEY_FIELD_PROP, Integer.toString(i));
        user.put(DEFAULT_VALUE_FIELD_PROP, DEFAULT_USER_DATA_VALUE_PREFIX + i);
        dataFileWriter.append(user);
      }
    }
  }

  private void writeVsonFile(File parentDir, String fileName, int start, int end) throws IOException {
    SequenceFile.Metadata metadata = new SequenceFile.Metadata();
    metadata.set(new Text("key.schema"), new Text(VSON_STRING_SCHEMA));
    metadata.set(new Text("value.schema"), new Text(VSON_STRING_SCHEMA));

    VsonAvroSerializer stringSerializer = VsonAvroSerializer.fromSchemaStr(VSON_STRING_SCHEMA);

    try (SequenceFile.Writer writer = SequenceFile.createWriter(
        new Configuration(),
        SequenceFile.Writer.file(new Path(parentDir.toString(), fileName)),
        SequenceFile.Writer.keyClass(BytesWritable.class),
        SequenceFile.Writer.valueClass(BytesWritable.class),
        SequenceFile.Writer.metadata(metadata))) {
      for (int i = start; i <= end; i++) {
        writer.append(
            new BytesWritable(stringSerializer.toBytes(String.valueOf(i))),
            new BytesWritable(stringSerializer.toBytes(DEFAULT_USER_DATA_VALUE_PREFIX + i)));
      }
    }
  }

  private void verifyAvroData(PartitionReader<InternalRow> reader, int start, int end) throws IOException {
    Schema avroStringSchema = Schema.create(Schema.Type.STRING);
    RecordDeserializer<CharSequence> deserializer =
        FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(avroStringSchema, avroStringSchema);
    int currentIdx = start;
    while (reader.next()) {
      InternalRow row = reader.get();
      Assert.assertEquals(deserializer.deserialize(row.getBinary(0)).toString(), Integer.toString(currentIdx));
      Assert.assertEquals(
          deserializer.deserialize(row.getBinary(1)).toString(),
          DEFAULT_USER_DATA_VALUE_PREFIX + currentIdx);
      currentIdx++;
    }
    Assert.assertEquals(currentIdx, end + 1);
  }

  private void verifyVsonData(PartitionReader<InternalRow> reader, int start, int end) throws IOException {
    Schema vsonAvroStringSchema = VsonAvroSchemaAdapter.nullableUnion(Schema.create(Schema.Type.STRING));
    RecordDeserializer<CharSequence> deserializer =
        FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(vsonAvroStringSchema, vsonAvroStringSchema);
    int currentIdx = start;
    while (reader.next()) {
      InternalRow row = reader.get();
      Assert.assertEquals(deserializer.deserialize(row.getBinary(0)).toString(), Integer.toString(currentIdx));
      Assert.assertEquals(
          deserializer.deserialize(row.getBinary(1)).toString(),
          DEFAULT_USER_DATA_VALUE_PREFIX + currentIdx);
      currentIdx++;
    }
    Assert.assertEquals(currentIdx, end + 1);
  }

  private void verifyPartialUpdateAvroData(PartitionReader<InternalRow> reader, Schema updateSchema)
      throws IOException {
    Schema avroStringSchema = Schema.create(Schema.Type.STRING);
    RecordDeserializer<CharSequence> keyDeserializer =
        FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(avroStringSchema, avroStringSchema);
    RecordDeserializer<GenericRecord> valueDeserializer =
        FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(updateSchema, updateSchema);

    int currentIdx = 1;
    while (reader.next()) {
      InternalRow row = reader.get();
      Assert.assertEquals(keyDeserializer.deserialize(row.getBinary(0)).toString(), Integer.toString(currentIdx));
      GenericRecord value = valueDeserializer.deserialize(row.getBinary(1));
      Assert.assertEquals(value.get("firstName").toString(), "first_name_" + currentIdx);
      Assert.assertEquals(value.get("lastName").toString(), "last_name_" + currentIdx);

      // Fields not provided should have NoOp filled in
      Assert.assertEquals(((GenericRecord) value.get("age")).getSchema().getName(), "NoOp");
      currentIdx++;
    }
    Assert.assertEquals(currentIdx, 101);
  }
}
