package com.linkedin.venice.spark.datawriter.task;

import static com.linkedin.venice.spark.SparkConstants.DEFAULT_SCHEMA;

import com.linkedin.venice.spark.SparkConstants;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
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

  @Test
  public void testEmptyIterator() {
    resetCounters();
    Iterator<Row> empty = Collections.emptyIterator();
    CountingIterator counting = new CountingIterator(empty, recordCounter, byteCounter);

    Assert.assertFalse(counting.hasNext());
    Assert.assertEquals(recordCounter.value(), 0L);
    Assert.assertEquals(byteCounter.value(), 0L);
  }

  @Test
  public void testSingleRecord() {
    resetCounters();
    byte[] key = new byte[10];
    byte[] value = new byte[100];
    byte[] rmd = new byte[20];
    Row row = createRow(key, value, rmd);

    CountingIterator counting =
        new CountingIterator(Collections.singletonList(row).iterator(), recordCounter, byteCounter);

    Assert.assertTrue(counting.hasNext());
    Row result = counting.next();
    Assert.assertSame(result, row);
    Assert.assertFalse(counting.hasNext());

    Assert.assertEquals(recordCounter.value(), 1L);
    Assert.assertEquals(byteCounter.value(), 130L); // 10 + 100 + 20
  }

  @Test
  public void testNullValueField() {
    resetCounters();
    byte[] key = new byte[10];
    byte[] rmd = new byte[20];
    Row row = createRow(key, null, rmd);

    CountingIterator counting =
        new CountingIterator(Collections.singletonList(row).iterator(), recordCounter, byteCounter);
    counting.next();

    Assert.assertEquals(recordCounter.value(), 1L);
    Assert.assertEquals(byteCounter.value(), 30L); // 10 + 0 + 20
  }

  @Test
  public void testNullRmdField() {
    resetCounters();
    byte[] key = new byte[10];
    byte[] value = new byte[100];
    Row row = createRow(key, value, null);

    CountingIterator counting =
        new CountingIterator(Collections.singletonList(row).iterator(), recordCounter, byteCounter);
    counting.next();

    Assert.assertEquals(recordCounter.value(), 1L);
    Assert.assertEquals(byteCounter.value(), 110L); // 10 + 100 + 0
  }

  @Test
  public void testBothNullableFieldsNull() {
    resetCounters();
    byte[] key = new byte[10];
    Row row = createRow(key, null, null);

    CountingIterator counting =
        new CountingIterator(Collections.singletonList(row).iterator(), recordCounter, byteCounter);
    counting.next();

    Assert.assertEquals(recordCounter.value(), 1L);
    Assert.assertEquals(byteCounter.value(), 10L); // key only
  }

  @Test
  public void testMultipleRecords() {
    resetCounters();
    Row row1 = createRow(new byte[10], new byte[100], new byte[20]); // 130
    Row row2 = createRow(new byte[5], new byte[50], new byte[10]); // 65
    Row row3 = createRow(new byte[20], new byte[200], null); // 220

    CountingIterator counting =
        new CountingIterator(Arrays.asList(row1, row2, row3).iterator(), recordCounter, byteCounter);

    counting.next();
    counting.next();
    counting.next();

    Assert.assertEquals(recordCounter.value(), 3L);
    Assert.assertEquals(byteCounter.value(), 415L); // 130 + 65 + 220
  }

  @Test
  public void testByteSizeAccuracy() {
    resetCounters();
    // Known sizes: key=10, value=1024, rmd=256
    byte[] key = new byte[10];
    byte[] value = new byte[1024];
    byte[] rmd = new byte[256];
    Row row = createRow(key, value, rmd);

    CountingIterator counting =
        new CountingIterator(Collections.singletonList(row).iterator(), recordCounter, byteCounter);
    counting.next();

    Assert.assertEquals(byteCounter.value(), 1290L); // 10 + 1024 + 256
  }

  @Test
  public void testIteratorOrder() {
    resetCounters();
    Row row1 = createRow(new byte[] { 1 }, new byte[] { 10 }, null);
    Row row2 = createRow(new byte[] { 2 }, new byte[] { 20 }, null);
    Row row3 = createRow(new byte[] { 3 }, new byte[] { 30 }, null);

    CountingIterator counting =
        new CountingIterator(Arrays.asList(row1, row2, row3).iterator(), recordCounter, byteCounter);

    Assert.assertSame(counting.next(), row1);
    Assert.assertSame(counting.next(), row2);
    Assert.assertSame(counting.next(), row3);
  }

  @Test(expectedExceptions = NoSuchElementException.class)
  public void testIteratorExhaustion() {
    resetCounters();
    Row row = createRow(new byte[1], null, null);
    CountingIterator counting =
        new CountingIterator(Collections.singletonList(row).iterator(), recordCounter, byteCounter);

    counting.next(); // OK
    counting.next(); // should throw
  }
}
