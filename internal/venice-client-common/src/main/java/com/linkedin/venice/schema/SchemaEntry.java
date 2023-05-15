package com.linkedin.venice.schema;

import static com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType.BACKWARD;
import static com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType.FORWARD;
import static com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType.FULL;
import static com.linkedin.venice.schema.avro.SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE;
import static com.linkedin.venice.schema.avro.SchemaCompatibility.checkReaderWriterCompatibility;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelperCommon;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.schema.avro.SchemaCompatibility;
import java.util.Arrays;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * {@link SchemaEntry} is composed of a schema and its corresponding id.
 * Internally, this class will maintain a parsed {@link org.apache.avro.Schema}
 */
public class SchemaEntry {
  // To ensure we don't accidentally use different compatibility type for schema creation in Venice.
  public static final DirectionalSchemaCompatibilityType DEFAULT_SCHEMA_CREATION_COMPATIBILITY_TYPE =
      DirectionalSchemaCompatibilityType.FULL;
  private static final Logger LOGGER = LogManager.getLogger(SchemaEntry.class);

  private final int id;
  private Schema schema;
  private boolean failedParsing = false;
  private String schemaStr;
  private String canonicalSchemaStr = null;

  /**
   * Primary constructor taking a literal id and schema.
   */
  public SchemaEntry(int id, String schemaStr) {
    if (schemaStr == null) {
      throw new IllegalArgumentException("The schemaStr parameter cannot be null!");
    }

    this.id = id;
    try {
      this.schema = AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(schemaStr);
    } catch (Exception e) {
      if ((e instanceof AvroTypeException)
          && (AvroCompatibilityHelperCommon.getRuntimeAvroVersion().laterThan(AvroVersion.AVRO_1_8))) {
        this.schema = Schema.create(Schema.Type.NULL);
        this.failedParsing = true;
        LOGGER.warn(
            "Avro 1.9 and newer version enforces stricter schema validation during parsing, will treat failed value schema as deprecated old value schema and ignore it. Error trace: ",
            e);
      } else {
        LOGGER.error("Failed to parse schema: {} with exception: ", schemaStr, e);
        throw new SchemaParseException(e);
      }
    }
    this.schemaStr = schemaStr;
  }

  public SchemaEntry(int id, Schema schema) {
    if (schema == null) {
      throw new IllegalArgumentException("The schema parameter cannot be null!");
    }

    this.id = id;
    this.schema = schema;
    this.schemaStr = null;
  }

  public SchemaEntry(int id, byte[] bytes) {
    this(id, new String(bytes));
  }

  public String getSchemaStr() {
    if (schemaStr == null) {
      schemaStr = schema.toString();
    }
    return schemaStr;
  }

  public String getCanonicalSchemaStr() {
    if (canonicalSchemaStr == null) {
      canonicalSchemaStr = AvroCompatibilityHelper.toParsingForm(schema);
    }
    return canonicalSchemaStr;
  }

  /** @return the id */
  public int getId() {
    return id;
  }

  public Schema getSchema() {
    return schema;
  }

  @Override
  public int hashCode() {
    return schema.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    SchemaEntry other = (SchemaEntry) obj;
    return schema.equals(other.getSchema());
  }

  public SchemaEntry clone() {
    return new SchemaEntry(id, schema.toString());
  }

  @Override
  public String toString() {
    return this.toString(false);
  }

  public String toString(boolean pretty) {
    return id + "\t" + schema.toString(pretty) + "\t" + failedParsing;
  }

  public byte[] getSchemaBytes() {
    return schema.toString().getBytes();
  }

  /**
   * This function will check whether a new schema is compatible with this one according to the passed in
   * {@param expectedCompatibilityType}.
   *
   * Right now, this function is using the util function provided by avro-1.7+ to check compatibility.
   * We need to remove the util class manually copied when venice is able to use avro-1.7+
   *
   * @return true if compatible, false otherwise
   */
  public boolean isNewSchemaCompatible(
      final SchemaEntry newSchemaEntry,
      final DirectionalSchemaCompatibilityType expectedCompatibilityType) {

    if (Arrays.asList(BACKWARD, FULL).contains(expectedCompatibilityType)) {
      SchemaCompatibility.SchemaPairCompatibility backwardCompatibility = checkReaderWriterCompatibility(
          /** reader */
          newSchemaEntry.schema,
          /** writer */
          this.schema);
      if (backwardCompatibility.getType() == INCOMPATIBLE) {
        LOGGER.info(
            "New schema (id {}) is not backward compatible with (i.e.: cannot read data written by)"
                + " existing schema (id {}), Full message:\n{}",
            newSchemaEntry.getId(),
            this.id,
            backwardCompatibility.getDescription());
        return false;
      }
    }

    if (Arrays.asList(FORWARD, FULL).contains(expectedCompatibilityType)) {
      SchemaCompatibility.SchemaPairCompatibility forwardCompatibility = checkReaderWriterCompatibility(
          /** reader */
          this.schema,
          /** writer */
          newSchemaEntry.schema);
      if (forwardCompatibility.getType() == INCOMPATIBLE) {
        LOGGER.info(
            "New schema id ({}) is not forward compatible with (i.e.: cannot have its written data read by)"
                + " existing schema id ({}), Full message:\n{}",
            newSchemaEntry.getId(),
            this.id,
            forwardCompatibility.getDescription());
        return false;
      }
    }

    return true;
  }
}
