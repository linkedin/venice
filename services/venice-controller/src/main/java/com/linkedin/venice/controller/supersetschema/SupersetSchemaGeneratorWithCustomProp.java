package com.linkedin.venice.controller.supersetschema;

import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.AvroSupersetSchemaUtils;
import java.util.Collection;
import org.apache.avro.Schema;


/**
 * This class would copy the specified {@link #customProp} from the latest value schema to the generated
 * superset schema and in the meantime, the compare method in this impl will consider this extra property as well.
 */
public class SupersetSchemaGeneratorWithCustomProp implements SupersetSchemaGenerator {
  private final String customProp;

  public SupersetSchemaGeneratorWithCustomProp(String customProp) {
    this.customProp = customProp;
  }

  @Override
  public SchemaEntry generateSupersetSchemaFromSchemas(Collection<SchemaEntry> schemas) {
    if (schemas.isEmpty()) {
      throw new IllegalArgumentException("Empty schema collection is unexpected");
    }
    SchemaEntry supersetSchemaEntry = AvroSchemaUtils.generateSupersetSchemaFromAllValueSchemas(schemas);

    // Find the latest value schema
    SchemaEntry latestValueSchemaEntry = null;
    for (SchemaEntry se: schemas) {
      if (latestValueSchemaEntry == null) {
        latestValueSchemaEntry = se;
      } else {
        if (se.getId() > latestValueSchemaEntry.getId()) {
          latestValueSchemaEntry = se;
        }
      }
    }
    /**
     * Check whether the latest value schema contains {@link #customProp} or not.
     */
    String customPropInLatestValueSchema = latestValueSchemaEntry.getSchema().getProp(customProp);
    if (customPropInLatestValueSchema != null && supersetSchemaEntry.getSchema().getProp(customProp) == null) {
      /**
       * The 'supersetSchemaEntry' can contain a different custom prop value than the latest value schema, and
       * custom prop value is not mutable.
       */
      Schema newSupersetSchema = supersetSchemaEntry.clone().getSchema();
      // Not empty, then copy it to the superset schema
      newSupersetSchema.addProp(customProp, customPropInLatestValueSchema);
      // Check whether this new schema exists or not
      for (SchemaEntry se: schemas) {
        if (compareSchema(se.getSchema(), newSupersetSchema)) {
          return se;
        }
      }
      return new SchemaEntry(latestValueSchemaEntry.getId() + 1, newSupersetSchema);
    } else {
      return supersetSchemaEntry;
    }
  }

  @Override
  public boolean compareSchema(Schema s1, Schema s2) {
    if (!AvroSchemaUtils.compareSchemaIgnoreFieldOrder(s1, s2)) {
      return false;
    }
    // Check custom prop
    String customPropFromS1 = s1.getProp(customProp);
    String customPropFromS2 = s2.getProp(customProp);

    return (customPropFromS1 == null && customPropFromS2 == null)
        || (customPropFromS1 != null && customPropFromS1.equals(customPropFromS2));
  }

  @Override
  public Schema generateSupersetSchema(Schema existingSchema, Schema newSchema) {
    Schema supersetSchema = AvroSupersetSchemaUtils.generateSupersetSchema(existingSchema, newSchema);
    String customPropInNewSchema = newSchema.getProp(customProp);
    if (customPropInNewSchema != null && supersetSchema.getProp(customProp) == null) {
      Schema newSupersetSchema = AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(supersetSchema.toString());
      newSupersetSchema.addProp(customProp, customPropInNewSchema);
      return newSupersetSchema;
    }
    return supersetSchema;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "(customProp: " + customProp + ")";
  }
}
