package com.linkedin.venice.hadoop.input.recordreader.avro;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroSchemaUtil;
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
  private final Schema dataSchema;

  private final int keyFieldPos;
  private final int valueFieldPos;
  private int rmdFieldPos;
  private final Schema valueSchema;

  private final boolean generatePartialUpdateRecordFromInput;

  /**
   * This constructor is used when data is read from HDFS.
   * @param dataSchema Schema of the avro file
   * @param keyFieldStr Field name of the key field
   * @param valueFieldStr Field name of the value field
   * @param etlValueSchemaTransformation The type of transformation that was applied to this schema during ETL. When source data set is not an ETL job, use NONE.
   */
  public AbstractAvroRecordReader(
      Schema dataSchema,
      String keyFieldStr,
      String valueFieldStr,
      String rmdFieldStr,
      ETLValueSchemaTransformation etlValueSchemaTransformation,
      Schema updateSchema) {
    this.dataSchema = dataSchema;
    Schema.Field keyField = getField(dataSchema, keyFieldStr);
    keyFieldPos = keyField.pos();
    Schema keySchema = keyField.schema();

    Schema rmdSchema = null;
    // The timestamp field is optional
    if (!rmdFieldStr.isEmpty()) {
      try {
        Schema.Field rmdField = getField(dataSchema, rmdFieldStr);
        rmdSchema = rmdField.schema();
        rmdFieldPos = rmdField.pos();
      } catch (VeniceSchemaFieldNotFoundException e) {
        rmdFieldPos = -1;
      }
    } else {
      rmdFieldPos = -1;
    }

    Schema outputSchema;
    if (!etlValueSchemaTransformation.equals(ETLValueSchemaTransformation.NONE)) {
      List<Schema.Field> outputSchemaFields = new LinkedList<>();

      for (Schema.Field fileField: dataSchema.getFields()) {
        Schema fieldSchema = fileField.schema();
        // In our ETL jobs, when we see a "delete" record, we set the value as "null". To allow the value field to be
        // set as "null", we make the schema of the value field as a union schema of "null" and the original value
        // schema. To push back to Venice from ETL data, we strip the schema of the value field of the union type,
        // leaving just the original value schema thus passing the schema validation.
        if (fileField.name().equals(valueFieldStr)) {
          fieldSchema = ETLUtils.getValueSchemaFromETLValueSchema(fieldSchema, etlValueSchemaTransformation);
        }
        // Since we are stripping the schema of the value field of the "null" type, we need to handle the case where the
        // default value of the schema is null and the schema is not nullable. In this case, we create a schema without
        // the "null" default type.
        if (fileField.hasDefaultValue() && AvroCompatibilityHelper.getGenericDefaultValue(fileField) == null
            && !AvroSchemaUtil.isNullAValidDefaultForSchema(fieldSchema)) {
          outputSchemaFields.add(
              AvroCompatibilityHelper.newField(null)
                  .setName(fileField.name())
                  .setSchema(fieldSchema)
                  .setDoc(fileField.doc())
                  .setOrder(fileField.order())
                  .build());
        } else {
          outputSchemaFields.add(AvroCompatibilityHelper.newField(fileField).setSchema(fieldSchema).build());
        }
      }

      outputSchema = Schema
          .createRecord(dataSchema.getName(), dataSchema.getDoc(), dataSchema.getNamespace(), dataSchema.isError());
      outputSchema.setFields(outputSchemaFields);
    } else {
      outputSchema = dataSchema;
    }

    Schema.Field outputValueField = getField(outputSchema, valueFieldStr);
    valueFieldPos = outputValueField.pos();

    this.generatePartialUpdateRecordFromInput = updateSchema != null;
    if (generatePartialUpdateRecordFromInput) {
      valueSchema = updateSchema;
    } else {
      valueSchema = outputValueField.schema();
    }

    if (rmdSchema != null) {
      configure(keySchema, valueSchema, rmdSchema);
    } else {
      configure(keySchema, valueSchema);
    }
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

  public Schema getDataSchema() {
    return dataSchema;
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
  public Object getRmdValue(INPUT_KEY inputKey, INPUT_VALUE inputValue) {
    if (rmdFieldPos == -1) {
      return null;
    }

    return getRecordDatum(inputKey, inputValue).get(rmdFieldPos);
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
