package com.linkedin.venice.spark.datawriter.task;

import static com.linkedin.venice.spark.SparkConstants.DEFAULT_SCHEMA;
import static com.linkedin.venice.spark.SparkConstants.DEFAULT_SCHEMA_WITH_SCHEMA_ID;
import static org.apache.spark.sql.types.DataTypes.BinaryType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;

import com.linkedin.venice.spark.SparkConstants;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class CountingIteratorTest {
  private SparkSession spark;
  private LongAccumulator recordCounter;
  private LongAccumulator byteCounter;

  @BeforeClass
  public void setUp() {
    spark = SparkSession.builder()
        .appName("CountingIteratorTest")
        .master(SparkConstants.DEFAULT_SPARK_CLUSTER)
        .getOrCreate();
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  private void resetCounters() {
    recordCounter = spark.sparkContext().longAccumulator("test_records");
    byteCounter = spark.sparkContext().longAccumulator("test_bytes");
  }

  private Row createRow(byte[] key, byte[] value, byte[] rmd) {
    return new GenericRowWithSchema(new Object[] { key, value, rmd }, DEFAULT_SCHEMA);
  }

  private CountingIterator createCountingIterator(Iterator<Row> delegate) {
    return new CountingIterator(delegate, recordCounter, byteCounter, DEFAULT_SCHEMA);
  }

  @Test
  public void testEmptyIterator() {
    resetCounters();
    Iterator<Row> empty = Collections.emptyIterator();
    CountingIterator counting = new CountingIterator(empty, recordCounter, byteCounter);

    Assert.assertFalse(counting.hasNext());
    Assert.assertEquals((long) recordCounter.value(), 0L);
    Assert.assertEquals((long) byteCounter.value(), 0L);
  }

  @Test
  public void testSingleRecord() {
    resetCounters();
    byte[] key = new byte[10];
    byte[] value = new byte[100];
    byte[] rmd = new byte[20];
    Row row = createRow(key, value, rmd);

    CountingIterator counting = createCountingIterator(Collections.singletonList(row).iterator());

    Assert.assertTrue(counting.hasNext());
    Row result = counting.next();
    Assert.assertSame(result, row);
    Assert.assertFalse(counting.hasNext());

    Assert.assertEquals((long) recordCounter.value(), 1L);
    Assert.assertEquals((long) byteCounter.value(), 130L); // 10 + 100 + 20
  }

  @Test
  public void testNullValueField() {
    resetCounters();
    byte[] key = new byte[10];
    byte[] rmd = new byte[20];
    Row row = createRow(key, null, rmd);

    CountingIterator counting = createCountingIterator(Collections.singletonList(row).iterator());
    counting.next();

    Assert.assertEquals((long) recordCounter.value(), 1L);
    Assert.assertEquals((long) byteCounter.value(), 30L); // 10 + 0 + 20
  }

  @Test
  public void testNullRmdField() {
    resetCounters();
    byte[] key = new byte[10];
    byte[] value = new byte[100];
    Row row = createRow(key, value, null);

    CountingIterator counting = createCountingIterator(Collections.singletonList(row).iterator());
    counting.next();

    Assert.assertEquals((long) recordCounter.value(), 1L);
    Assert.assertEquals((long) byteCounter.value(), 110L); // 10 + 100 + 0
  }

  @Test
  public void testBothNullableFieldsNull() {
    resetCounters();
    byte[] key = new byte[10];
    Row row = createRow(key, null, null);

    CountingIterator counting = createCountingIterator(Collections.singletonList(row).iterator());
    counting.next();

    Assert.assertEquals((long) recordCounter.value(), 1L);
    Assert.assertEquals((long) byteCounter.value(), 10L); // key only
  }

  @Test
  public void testMultipleRecords() {
    resetCounters();
    Row row1 = createRow(new byte[10], new byte[100], new byte[20]); // 130
    Row row2 = createRow(new byte[5], new byte[50], new byte[10]); // 65
    Row row3 = createRow(new byte[20], new byte[200], null); // 220

    CountingIterator counting = createCountingIterator(Arrays.asList(row1, row2, row3).iterator());

    counting.next();
    counting.next();
    counting.next();

    Assert.assertEquals((long) recordCounter.value(), 3L);
    Assert.assertEquals((long) byteCounter.value(), 415L); // 130 + 65 + 220
  }

  @Test
  public void testIteratorOrder() {
    resetCounters();
    Row row1 = createRow(new byte[] { 1 }, new byte[] { 10 }, null);
    Row row2 = createRow(new byte[] { 2 }, new byte[] { 20 }, null);
    Row row3 = createRow(new byte[] { 3 }, new byte[] { 30 }, null);

    CountingIterator counting = createCountingIterator(Arrays.asList(row1, row2, row3).iterator());

    Assert.assertSame(counting.next(), row1);
    Assert.assertSame(counting.next(), row2);
    Assert.assertSame(counting.next(), row3);
  }

  @Test
  public void testNoSchemaReturnZeroBytes() {
    resetCounters();
    Row row = createRow(new byte[10], new byte[100], new byte[20]);

    // No-schema constructor: byte size should be 0, but record count should still work
    CountingIterator counting =
        new CountingIterator(Collections.singletonList(row).iterator(), recordCounter, byteCounter);
    counting.next();

    Assert.assertEquals((long) recordCounter.value(), 1L);
    Assert.assertEquals((long) byteCounter.value(), 0L);
  }

  @Test(expectedExceptions = NoSuchElementException.class)
  public void testIteratorExhaustion() {
    resetCounters();
    Row row = createRow(new byte[1], null, null);
    CountingIterator counting = createCountingIterator(Collections.singletonList(row).iterator());

    counting.next(); // OK
    counting.next(); // should throw
  }

  @Test
  public void testComputeByteSizeAllFieldsPresent() {
    resetCounters();
    CountingIterator iter = createCountingIterator(Collections.emptyIterator());
    Row row = createRow(new byte[10], new byte[100], new byte[20]);

    Assert.assertEquals(iter.computeByteSize(row), 130L);
  }

  @Test
  public void testComputeByteSizeNullValue() {
    resetCounters();
    CountingIterator iter = createCountingIterator(Collections.emptyIterator());
    Row row = createRow(new byte[10], null, new byte[20]);

    Assert.assertEquals(iter.computeByteSize(row), 30L);
  }

  @Test
  public void testComputeByteSizeNullRmd() {
    resetCounters();
    CountingIterator iter = createCountingIterator(Collections.emptyIterator());
    Row row = createRow(new byte[10], new byte[100], null);

    Assert.assertEquals(iter.computeByteSize(row), 110L);
  }

  @Test
  public void testComputeByteSizeAllNullable() {
    resetCounters();
    CountingIterator iter = createCountingIterator(Collections.emptyIterator());
    Row row = createRow(new byte[5], null, null);

    Assert.assertEquals(iter.computeByteSize(row), 5L);
  }

  @Test
  public void testComputeByteSizeWithSchemaIdSchema() {
    // DEFAULT_SCHEMA_WITH_SCHEMA_ID has key, value, rmd plus schema_id and rmd_version_id
    resetCounters();
    CountingIterator iter =
        new CountingIterator(Collections.emptyIterator(), recordCounter, byteCounter, DEFAULT_SCHEMA_WITH_SCHEMA_ID);

    Row row = new GenericRowWithSchema(
        new Object[] { new byte[10], new byte[100], new byte[20], 1, 1 },
        DEFAULT_SCHEMA_WITH_SCHEMA_ID);

    // Should still compute key + value + rmd, ignoring schema_id and rmd_version_id
    Assert.assertEquals(iter.computeByteSize(row), 130L);
  }

  @Test
  public void testComputeByteSizeNoKeyColumn() {
    // Schema without key/value/rmd columns — should return 0
    resetCounters();
    StructType noKeySchema = new StructType(
        new StructField[] { new StructField("foo", IntegerType, false, Metadata.empty()),
            new StructField("bar", BinaryType, true, Metadata.empty()) });

    CountingIterator iter = new CountingIterator(Collections.emptyIterator(), recordCounter, byteCounter, noKeySchema);

    Row row = new GenericRowWithSchema(new Object[] { 42, new byte[100] }, noKeySchema);

    Assert.assertEquals(iter.computeByteSize(row), 0L);
  }

  @Test
  public void testComputeByteSizeSchemaWithKeyValueButNoRmd() {
    // Schema with key and value but no rmd column — should compute key + value only
    resetCounters();
    StructType keyValueOnly = new StructType(
        new StructField[] { new StructField("key", BinaryType, false, Metadata.empty()),
            new StructField("value", BinaryType, true, Metadata.empty()) });

    CountingIterator iter = new CountingIterator(Collections.emptyIterator(), recordCounter, byteCounter, keyValueOnly);

    Row row = new GenericRowWithSchema(new Object[] { new byte[10], new byte[100] }, keyValueOnly);

    Assert.assertEquals(iter.computeByteSize(row), 110L);
  }

  @Test
  public void testComputeByteSizeLargeValues() {
    resetCounters();
    CountingIterator iter = createCountingIterator(Collections.emptyIterator());

    byte[] largeKey = new byte[1024];
    byte[] largeValue = new byte[10 * 1024 * 1024]; // 10 MB
    byte[] largeRmd = new byte[512 * 1024]; // 512 KB
    Row row = createRow(largeKey, largeValue, largeRmd);

    long expected = 1024L + (10L * 1024 * 1024) + (512L * 1024);
    Assert.assertEquals(iter.computeByteSize(row), expected);
  }
}
