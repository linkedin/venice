package com.linkedin.venice.hadoop.utils;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.hadoop.PushJobSetting;
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
