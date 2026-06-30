package com.linkedin.venice.hadoop.utils;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.PushJobSetting;
import com.linkedin.venice.hadoop.exceptions.VeniceSchemaFieldNotFoundException;
import com.linkedin.venice.spark.utils.RmdPushUtils;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class TestRmdPushUtils {
  @Test
  public void testGetInputRmdSchema() {
    final String rmdField = "rmd";
    Schema mockSchema = mock(Schema.class);
    Schema rmdSchema = Schema.create(Schema.Type.LONG);
    Schema.Field mockField = mock(Schema.Field.class);
    when(mockSchema.getField(eq(rmdField))).thenReturn(mockField);
    when(mockField.schema()).thenReturn(rmdSchema);
    PushJobSetting pushJobSetting = new PushJobSetting();
    pushJobSetting.rmdField = rmdField;
    pushJobSetting.inputDataSchema = mockSchema;
    assertEquals(RmdPushUtils.getInputRmdSchema(pushJobSetting), rmdSchema);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetInputRmdSchemaWithNoRmdField() {
    PushJobSetting pushJobSetting = new PushJobSetting();
    pushJobSetting.inputDataSchema = mock(Schema.class);
    RmdPushUtils.getInputRmdSchema(pushJobSetting);
  }

  @Test(expectedExceptions = VeniceSchemaFieldNotFoundException.class)
  public void testGetInputRmdSchemaWhenRmdFieldNotInInputSchema() {
    // timestamp.field is configured, but the named field is absent from the top level of the input record schema
    // (e.g. it is nested inside the value). getField returns null, so the call must fail with a clear error, not an
    // NPE.
    final String rmdField = "createdTimeEpoch";
    Schema mockSchema = mock(Schema.class);
    when(mockSchema.getField(eq(rmdField))).thenReturn(null);
    PushJobSetting pushJobSetting = new PushJobSetting();
    pushJobSetting.rmdField = rmdField;
    pushJobSetting.inputDataSchema = mockSchema;
    RmdPushUtils.getInputRmdSchema(pushJobSetting);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testGetInputRmdSchemaWhenInputDataSchemaIsNull() {
    // timestamp.field is configured but the input record schema was never populated (e.g. a VSON input). The
    // configured-but-unavailable schema must fail with a clear error, not an NPE.
    PushJobSetting pushJobSetting = new PushJobSetting();
    pushJobSetting.rmdField = "createdTimeEpoch";
    pushJobSetting.inputDataSchema = null;
    RmdPushUtils.getInputRmdSchema(pushJobSetting);
  }

  @Test
  public void testContainsLogicalTimestamp() {
    final String rmdField = "rmd";
    Schema mockSchema = mock(Schema.class);
    Schema rmdSchema = Schema.create(Schema.Type.LONG);
    Schema.Field mockField = mock(Schema.Field.class);
    when(mockSchema.getField(eq(rmdField))).thenReturn(mockField);
    when(mockField.schema()).thenReturn(rmdSchema);

    PushJobSetting pushJobSetting = new PushJobSetting();
    pushJobSetting.rmdField = rmdField;
    pushJobSetting.inputDataSchema = mockSchema;

    assertTrue(RmdPushUtils.containsLogicalTimestamp(pushJobSetting));

    when(mockSchema.getField(eq(rmdField))).thenReturn(mockField);
    when(mockField.schema()).thenReturn(Schema.create(Schema.Type.BYTES));
    assertFalse(RmdPushUtils.containsLogicalTimestamp(pushJobSetting));

    pushJobSetting.rmdField = null;
    assertFalse(RmdPushUtils.containsLogicalTimestamp(pushJobSetting));
  }

  @Test
  public void testRmdFieldPresent() {
    PushJobSetting pushJobSetting = new PushJobSetting();
    assertFalse(RmdPushUtils.rmdFieldPresent(pushJobSetting));

    pushJobSetting.rmdField = "";
    assertFalse(RmdPushUtils.rmdFieldPresent(pushJobSetting));

    pushJobSetting.rmdField = "rmd";
    assertTrue(RmdPushUtils.rmdFieldPresent(pushJobSetting));
  }
}
