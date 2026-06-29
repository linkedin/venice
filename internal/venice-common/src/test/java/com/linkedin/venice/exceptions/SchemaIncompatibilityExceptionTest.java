package com.linkedin.venice.exceptions;

import com.linkedin.venice.schema.SchemaEntry;
import org.apache.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SchemaIncompatibilityExceptionTest {
  @Test
  public void testException() {
    SchemaEntry oldSchema = new SchemaEntry(1, "{\"type\":\"record\",\"name\":\"R\",\"fields\":[]}");
    SchemaEntry newSchema = new SchemaEntry(2, "\"string\"");

    SchemaIncompatibilityException e = new SchemaIncompatibilityException(oldSchema, newSchema);

    Assert.assertEquals(e.getErrorType(), ErrorType.INVALID_SCHEMA);
    Assert.assertEquals(
        e.getHttpStatusCode(),
        HttpStatus.SC_BAD_REQUEST,
        "An incompatible client-supplied schema must surface as HTTP 400, not 500");
  }
}
