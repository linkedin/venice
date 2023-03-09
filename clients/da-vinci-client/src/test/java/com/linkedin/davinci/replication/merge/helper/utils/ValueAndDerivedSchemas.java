package com.linkedin.davinci.replication.merge.helper.utils;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;


public class ValueAndDerivedSchemas {
  private final Schema valueSchema;

  private final Schema updateSchema;
  private final Schema rmdSchema;

  private final String storeName;
  private final int valueSchemaId;
  private static final int updateSchemaProtocolVersion = 1;
  private final int rmdSchemaProtocolVersion = RmdSchemaGenerator.getLatestVersion();

  public ValueAndDerivedSchemas(String storeName, int valueSchemaId, String valueSchemaFilePath) {
    this.storeName = storeName;
    this.valueSchemaId = valueSchemaId;
    this.valueSchema = loadSchemaFile(valueSchemaFilePath);
    this.updateSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);
    this.rmdSchema = RmdSchemaGenerator.generateMetadataSchema(valueSchema, RmdSchemaGenerator.getLatestVersion());
  }

  public String getStoreName() {
    return storeName;
  }

  public int getValueSchemaId() {
    return valueSchemaId;
  }

  public int getUpdateSchemaProtocolVersion() {
    return updateSchemaProtocolVersion;
  }

  public int getRmdSchemaProtocolVersion() {
    return rmdSchemaProtocolVersion;
  }

  public Schema getValueSchema() {
    return valueSchema;
  }

  public SchemaEntry getValueSchemaEntry() {
    return new SchemaEntry(valueSchemaId, valueSchema);
  }

  public Schema getUpdateSchema() {
    return updateSchema;
  }

  public DerivedSchemaEntry getUpdateSchemaEntry() {
    return new DerivedSchemaEntry(valueSchemaId, updateSchemaProtocolVersion, updateSchema);
  }

  public Schema getRmdSchema() {
    return rmdSchema;
  }

  public RmdSchemaEntry getRmdSchemaEntry() {
    return new RmdSchemaEntry(valueSchemaId, rmdSchemaProtocolVersion, rmdSchema);
  }

  private String loadSchemaFileAsString(String filePath) {
    try {
      return IOUtils.toString(
          Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResourceAsStream(filePath)),
          StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new VeniceException(e);
    }
  }

  private Schema loadSchemaFile(String filePath) {
    return AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(loadSchemaFileAsString(filePath));
  }
}
