package com.linkedin.venice.spark.datawriter.recordprocessor;

import static com.linkedin.venice.spark.SparkConstants.KEY_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.RMD_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.VALUE_COLUMN_NAME;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.spark.utils.RmdPushUtils;
import org.apache.avro.Schema;
import org.apache.spark.sql.Row;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SparkLogicalTimestampProcessorTest {
  private static final String RMD_SCHEMA_STR = "{\"type\":\"record\",\"name\":\"venice_replication_metadata\","
      + "\"namespace\":\"com.linkedin.venice\",\"fields\":[{\"name\":\"replication_checkpoint_vector\","
      + "\"type\":{\"type\":\"array\",\"items\":\"long\"},\"default\":[]},{\"name\":\"timestamp\","
      + "\"type\":\"long\",\"default\":0}]}";
  private static final byte[] KEY_BYTES = new byte[] { 1, 2 };
  private static final byte[] RMD_BYTES = new byte[] { 1, 2, 3, 4 };
  private static final byte[] VALUE_BYTES = new byte[] { 1, 2, 3 };

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidRmdSchemaString() {
    new SparkLogicalTimestampProcessor(true, "");
  }

  @Test
  public void testPassThroughRmd() throws Exception {
    SparkLogicalTimestampProcessor processor = new SparkLogicalTimestampProcessor(false, "");
    Row mockRow = mock(Row.class);
    when(mockRow.getAs(eq(VALUE_COLUMN_NAME))).thenReturn(VALUE_BYTES);
    when(mockRow.getAs(eq(KEY_COLUMN_NAME))).thenReturn(KEY_BYTES);
    when(mockRow.getAs(eq(RMD_COLUMN_NAME))).thenReturn(RMD_BYTES);

    Row processedRow = processor.call(mockRow);
    Assert.assertEquals(processedRow.getAs(KEY_COLUMN_NAME), KEY_BYTES);
    Assert.assertEquals(processedRow.getAs(RMD_COLUMN_NAME), RMD_BYTES);
    Assert.assertEquals(processedRow.getAs(VALUE_COLUMN_NAME), VALUE_BYTES);
  }

  @Test
  public void testConvertLogicalTimestampToRmd() throws Exception {
    long timestamp = 123L;
    SparkLogicalTimestampProcessor processor = new SparkLogicalTimestampProcessor(true, RMD_SCHEMA_STR);
    Schema rmdSchema = AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(RMD_SCHEMA_STR);
    byte[] expectedRmd = RmdSchemaGenerator.generateRecordLevelTimestampMetadata(rmdSchema, 123L);

    Row mockRow = mock(Row.class);
    when(mockRow.getAs(eq(VALUE_COLUMN_NAME))).thenReturn(VALUE_BYTES);
    when(mockRow.getAs(eq(KEY_COLUMN_NAME))).thenReturn(KEY_BYTES);
    when(mockRow.getAs(eq(RMD_COLUMN_NAME)))
        .thenReturn(RmdPushUtils.getSerializerForLogicalTimestamp().serialize(timestamp));

    Row processedRow = processor.call(mockRow);
    Assert.assertEquals(processedRow.getAs(KEY_COLUMN_NAME), KEY_BYTES);
    Assert.assertEquals(processedRow.getAs(RMD_COLUMN_NAME), expectedRmd);
    Assert.assertEquals(processedRow.getAs(VALUE_COLUMN_NAME), VALUE_BYTES);
  }
}
