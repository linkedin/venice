package com.linkedin.venice.spark.utils;

import static com.linkedin.venice.spark.SparkConstants.KEY_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.OFFSET_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.POSITION_COLUMN_NAME;
import static org.apache.spark.sql.types.DataTypes.BinaryType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.testng.annotations.Test;


public class PubSubPositionRowComparatorTest {
  private static final StructType SCHEMA = new StructType(
      new StructField[] { new StructField(KEY_COLUMN_NAME, BinaryType, false, null),
          new StructField(OFFSET_COLUMN_NAME, LongType, false, null),
          new StructField(POSITION_COLUMN_NAME, BinaryType, true, null) });

  @Test
  public void testPositionsAreDeserializedOncePerRow() {
    int numRows = 64;
    List<Row> rows = new ArrayList<>();
    for (int seq = 0; seq < numRows; seq++) {
      rows.add(row(-1, OfflineOrderedTestPosition.positionBytes(0, seq)));
    }
    Collections.shuffle(rows, new Random(42));
    PubSubPositionRowComparator comparator = OfflineOrderedTestPosition.newRowComparator();

    OfflineOrderedTestPosition.DESERIALIZE_COUNT.set(0);
    comparator.sortLatestFirst(rows);
    assertEquals(OfflineOrderedTestPosition.DESERIALIZE_COUNT.get(), numRows);
    for (int i = 0; i < numRows; i++) {
      assertEquals(
          rows.get(i).getAs(POSITION_COLUMN_NAME),
          OfflineOrderedTestPosition.positionBytes(0, numRows - 1 - i));
    }

    Collections.shuffle(rows, new Random(7));
    OfflineOrderedTestPosition.DESERIALIZE_COUNT.set(0);
    Row latest = comparator.selectLatest(rows);
    assertEquals(OfflineOrderedTestPosition.DESERIALIZE_COUNT.get(), numRows);
    assertEquals(latest.getAs(POSITION_COLUMN_NAME), OfflineOrderedTestPosition.positionBytes(0, numRows - 1));
  }

  @Test
  public void testNumericOffsetsSkipDeserializationAndEmptyInput() {
    Row older = row(10, OfflineOrderedTestPosition.positionBytes(0, 99));
    Row newer = row(11, OfflineOrderedTestPosition.positionBytes(0, 1));
    List<Row> rows = new ArrayList<>();
    rows.add(older);
    rows.add(newer);
    PubSubPositionRowComparator comparator = OfflineOrderedTestPosition.newRowComparator();

    OfflineOrderedTestPosition.DESERIALIZE_COUNT.set(0);
    comparator.sortLatestFirst(rows);
    assertSame(rows.get(0), newer);
    assertSame(comparator.selectLatest(rows), newer);
    assertEquals(OfflineOrderedTestPosition.DESERIALIZE_COUNT.get(), 0);

    assertNull(comparator.selectLatest(new ArrayList<>()));
  }

  private static Row row(long offset, byte[] position) {
    return new GenericRowWithSchema(new Object[] { new byte[] { 1 }, offset, position }, SCHEMA);
  }
}
