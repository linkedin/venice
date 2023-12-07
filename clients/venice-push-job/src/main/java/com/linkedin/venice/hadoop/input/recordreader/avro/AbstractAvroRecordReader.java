package com.linkedin.venice.hadoop.input.recordreader.avro;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.etl.ETLUtils;
import com.linkedin.venice.etl.ETLValueSchemaTransformation;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.exceptions.VeniceSchemaFieldNotFoundException;
import com.linkedin.venice.hadoop.input.recordreader.AbstractVeniceRecordReader;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.util.LinkedList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;


/**
 * An abstraction for a record reader that reads records from input into Avro-serialized keys and values.
 * @param <INPUT_KEY> The format of the key as controlled by the input format
 * @param <INPUT_VALUE> The format of the value as controlled by the input format
 */
public abstract class AbstractAvroRecordReader<INPUT_KEY, INPUT_VALUE>
    extends AbstractVeniceRecordReader<INPUT_KEY, INPUT_VALUE> {
  private final Schema fileSchema;
  private final Schema outputSchema;

  private final int keyFieldPos;
  private final Schema keySchema;
  private final int valueFieldPos;
  private final Schema valueSchema;

  private boolean generatePartialUpdateRecordFromInput;

  /**
   * This constructor is used when data is read from HDFS.
   * @param fileSchema Schema of the avro file
   * @param keyFieldStr Field name of the key field
   * @param valueFieldStr Field name of the value field
   * @param etlValueSchemaTransformation The type of transformation that was applied to this schema during ETL. When source data set is not an ETL job, use NONE.
   */
  public AbstractAvroRecordReader(
      Schema fileSchema,
      String keyFieldStr,
      String valueFieldStr,
      ETLValueSchemaTransformation etlValueSchemaTransformation,
      Schema updateSchema) {
    this.fileSchema = fileSchema;
    Schema.Field keyField = getField(fileSchema, keyFieldStr);
    keyFieldPos = keyField.pos();
    keySchema = keyField.schema();

    if (!etlValueSchemaTransformation.equals(ETLValueSchemaTransformation.NONE)) {
      List<Schema.Field> outputSchemaFields = new LinkedList<>();

      for (Schema.Field fileField: fileSchema.getFields()) {
        Schema fieldSchema = fileField.schema();
        // In our ETL jobs, when we see a "delete" record, we set the value as "null". To allow the value field to be
        // set as "null", we make the schema of the value field as a union schema of "null" and the original value
        // schema. To push back to Venice from ETL data, we strip the schema of the value field of the union type,
        // leaving just the original value schema thus passing the schema validation.
        if (fileField.name().equals(valueFieldStr)) {
          fieldSchema = ETLUtils.getValueSchemaFromETLValueSchema(fieldSchema, etlValueSchemaTransformation);
        }
        outputSchemaFields.add(AvroCompatibilityHelper.newField(fileField).setSchema(fieldSchema).build());
      }

      outputSchema = Schema
          .createRecord(fileSchema.getName(), fileSchema.getDoc(), fileSchema.getNamespace(), fileSchema.isError());
      outputSchema.setFields(outputSchemaFields);
    } else {
      outputSchema = fileSchema;
    }

    Schema.Field outputValueField = getField(outputSchema, valueFieldStr);
    valueFieldPos = outputValueField.pos();

    this.generatePartialUpdateRecordFromInput = updateSchema != null;
    if (generatePartialUpdateRecordFromInput) {
      this.valueSchema = updateSchema;
    } else {
      valueSchema = outputValueField.schema();
    }

    configure(keySchema, valueSchema);
  }

  private static Schema.Field getField(Schema fileSchema, String fieldName) {
    Schema.Field field = fileSchema.getField(fieldName);

    if (field == null) {
      throw new VeniceSchemaFieldNotFoundException(
          fieldName,
          "Could not find field: " + fieldName + " from " + fileSchema);
    }

    return field;
  }

  public Schema getDatasetSchema() {
    return fileSchema;
  }

  protected abstract IndexedRecord getRecordDatum(INPUT_KEY inputKey, INPUT_VALUE inputValue);

  @Override
  public Object getAvroKey(INPUT_KEY inputKey, INPUT_VALUE inputValue) {
    Object keyDatum = getRecordDatum(inputKey, inputValue).get(keyFieldPos);

    if (keyDatum == null) {
      // Invalid data
      // Theoretically it should not happen since all the avro records are sharing the same schema in the same file
      throw new VeniceException("Encountered record with null key");
    }

    return keyDatum;
  }

  @Override
  public Object getAvroValue(INPUT_KEY inputKey, INPUT_VALUE inputValue) {
    Object valueObject = getRecordDatum(inputKey, inputValue).get(valueFieldPos);
    if (!generatePartialUpdateRecordFromInput) {
      return valueObject;
    }
    if (!(valueObject instanceof IndexedRecord)) {
      throw new VeniceException("Retrieved record is not a Avro indexed record");
    }
    UpdateBuilder updateBuilder = new UpdateBuilderImpl(valueSchema);
    IndexedRecord indexedRecordValue = (IndexedRecord) valueObject;
    for (Schema.Field field: indexedRecordValue.getSchema().getFields()) {
      updateBuilder.setNewFieldValue(field.name(), indexedRecordValue.get(field.pos()));
    }
    return updateBuilder.build();
  }
}
