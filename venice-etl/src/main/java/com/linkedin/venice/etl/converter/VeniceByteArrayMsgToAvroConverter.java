package com.linkedin.venice.etl.converter;

import com.linkedin.venice.etl.VeniceKafkaDecodedRecord;

import com.linkedin.venice.etl.schema.HDFSSchemaSource;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.converter.ToAvroConverterBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.venice.etl.source.VeniceKafkaSource.*;


/**
 * The store specific schema will be passed from the VeniceSource; this class convert all raw
 * key/value bytes to Store specific avro records.
 */
public class VeniceByteArrayMsgToAvroConverter extends ToAvroConverterBase<Schema, GenericRecord> {
  private static final Logger logger = LoggerFactory.getLogger(VeniceByteArrayMsgToAvroConverter.class);

  private String topicName;
  private String storeName;

  private HDFSSchemaSource hdfsSchemaSource;
  private Schema outputKeySchema;
  private Schema[] outputValueSchemas;
  private Schema outputSchema;
  private int valueSchemaNum;
  GenericDatumReader<Object> keyReader;
  GenericDatumReader<Object>[] valueReaders;

  @Override
  public Converter<Schema, Schema, GenericRecord, GenericRecord> init(WorkUnitState workUnit) {
    topicName = workUnit.getProp(TOPIC_NAME);
    logger.info("[converter] topic name = " + topicName);

    storeName = Version.parseStoreFromKafkaTopicName(topicName);

    // initialize the hdfsSchemaSource where we can get the key schema and all value schemas
    hdfsSchemaSource = createSchemaSource(workUnit);

    return this;
  }

  @Override
  public Schema convertSchema(Schema schemaIn, WorkUnitState workUnit) {
    // first, get schemas from HDFS files
    getSchemaFromHDFS();

    // build the output schema
    List<Schema.Field> outputSchemaFields = new ArrayList<>();
    for (Schema.Field field : VeniceKafkaDecodedRecord.SCHEMA$.getFields()) {
      if (field.name().equals(VENICE_ETL_KEY_FIELD)) {
        outputSchemaFields.add(new Schema.Field(field.name(), outputKeySchema, field.doc(), field.defaultValue(), field.order()));
      } else if (field.name().equals(VENICE_ETL_VALUE_FIELD)) {
        outputSchemaFields.add(new Schema.Field(field.name(), Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), outputValueSchemas[valueSchemaNum - 1])), field.doc(), field.defaultValue(), field.order()));
      } else if (!field.name().equals(VENICE_ETL_METADATA_FIELD)) {
        // any fields except key, value and metadata will be added using the original schemas, like the offset field and the DELETED_TS field
        outputSchemaFields.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue(), field.order()));
      }
    }
    outputSchema = Schema.createRecord(schemaIn.getName(), schemaIn.getDoc(), schemaIn.getNamespace(), schemaIn.isError());
    outputSchema.setFields(outputSchemaFields);


    // return the output schema with the latest value schema
    return outputSchema;
  }

  @Override
  public Iterable<GenericRecord> convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit) {
    ByteBuffer keyByteBuffer = (ByteBuffer) inputRecord.get(VENICE_ETL_KEY_FIELD);
    ByteBuffer valueByteBuffer = (ByteBuffer) inputRecord.get(VENICE_ETL_VALUE_FIELD);
    GenericRecord convertedRecord = new GenericData.Record(outputSchema);
    convertedRecord.put(VENICE_ETL_OFFSET_FIELD, (long) inputRecord.get(VENICE_ETL_OFFSET_FIELD));
    Map<String, String> metadata = (Map)inputRecord.get(VENICE_ETL_METADATA_FIELD);
    int schemaId = Integer.valueOf(metadata.get(VENICE_ETL_SCHEMAID_FIELD));
    try {
      DecoderFactory decoderFactory = new DecoderFactory();
      Decoder keyDecoder = decoderFactory.createBinaryDecoder(keyByteBuffer.array(), null);
      Object keyRecord = keyReader.read(null, keyDecoder);
      convertedRecord.put(VENICE_ETL_KEY_FIELD, keyRecord);

      if (inputRecord.get(VENICE_ETL_DELETED_TS_FIELD) == null) {
        // put message
        Decoder valueDecoder = decoderFactory.createBinaryDecoder(valueByteBuffer.array(), null);
        Object valueRecord = valueReaders[schemaId - 1].read(null, valueDecoder);
        convertedRecord.put(VENICE_ETL_VALUE_FIELD, valueRecord);
      } else {
        // delete message
        convertedRecord.put(VENICE_ETL_DELETED_TS_FIELD, (long) inputRecord.get(VENICE_ETL_DELETED_TS_FIELD));
      }

    } catch (IOException e) {
      // catch exception here so that the conversion for the rest of records won't fail
      logger.error("[converter] exception", e);
    }
    return new SingleRecordIterable<>(convertedRecord);
  }

  private HDFSSchemaSource createSchemaSource(WorkUnitState workUnit) {
    String schemaDir = workUnit.getProp(SCHEMA_DIRECTORY);
    if (schemaDir.endsWith("/")) {
      schemaDir = schemaDir.substring(0, schemaDir.length() - 1);
    }

    HDFSSchemaSource schemaSource;
    try {
      schemaSource = new HDFSSchemaSource(schemaDir);
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
      throw new VeniceException(e.getMessage(), e);
    }
    return schemaSource;
  }

  private void getSchemaFromHDFS() {
    outputKeySchema = hdfsSchemaSource.fetchKeySchema(storeName);
    keyReader = new GenericDatumReader<>(outputKeySchema);

    outputValueSchemas = hdfsSchemaSource.fetchValueSchemas(storeName);
    valueSchemaNum = outputValueSchemas.length;
    valueReaders = new GenericDatumReader[valueSchemaNum];
    for (int schemaId = 0; schemaId < valueSchemaNum; schemaId++) {
      valueReaders[schemaId] = new GenericDatumReader<>(outputValueSchemas[schemaId], outputValueSchemas[valueSchemaNum - 1]);
    }
  }

}
