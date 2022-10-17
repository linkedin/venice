package com.linkedin.davinci.replication.merge.helper.utils;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;


public class ValueAndRmdSchema {
  private final Schema valueSchema;
  private final Schema rmdSchema;

  public ValueAndRmdSchema(String valueSchemaFilePath, int rmdVersionId) {
    this.valueSchema = loadSchemaFile(valueSchemaFilePath);
    this.rmdSchema = RmdSchemaGenerator.generateMetadataSchema(valueSchema, rmdVersionId);
  }

  public ValueAndRmdSchema(String valueSchemaFilePath) {
    this(valueSchemaFilePath, RmdSchemaGenerator.getLatestVersion());
  }

  public Schema getValueSchema() {
    return valueSchema;
  }

  public Schema getRmdSchema() {
    return rmdSchema;
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
