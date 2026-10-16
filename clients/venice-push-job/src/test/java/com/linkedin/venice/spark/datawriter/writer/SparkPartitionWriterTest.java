package com.linkedin.venice.spark.datawriter.writer;

import static com.linkedin.venice.spark.SparkConstants.DEFAULT_SCHEMA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PARTITION_COUNT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_SCHEMA_ID_PROP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.spark.datawriter.task.DataWriterAccumulators;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.util.LongAccumulator;
import org.mockito.InOrder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class SparkPartitionWriterTest {
  private TaskContext previousTaskContext;
  private DataWriterAccumulators accumulators;
  private AbstractVeniceWriter<byte[], byte[], byte[]> veniceWriter;

  @BeforeMethod
  public void setUp() {
    previousTaskContext = TaskContext.get();
    TaskContext taskContext = mock(TaskContext.class);
    when(taskContext.getLocalProperties()).thenReturn(new Properties());
    TaskContext$.MODULE$.setTaskContext(taskContext);

    SparkSession session = mock(SparkSession.class);
    SparkContext sparkContext = mock(SparkContext.class);
    when(session.sparkContext()).thenReturn(sparkContext);
    when(sparkContext.longAccumulator(anyString())).thenAnswer(invocation -> new LongAccumulator());
    accumulators = new DataWriterAccumulators(session);

    veniceWriter = mock(AbstractVeniceWriter.class);
    PubSubProduceResult produceResult = mock(PubSubProduceResult.class);
    doAnswer(invocation -> {
      invocation.getArgument(3, PubSubProducerCallback.class).onCompletion(produceResult, null);
      return null;
    }).when(veniceWriter).put(any(), any(), eq(1), any(), isNull());
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() {
    TaskContext$.MODULE$.setTaskContext(previousTaskContext);
  }

  @DataProvider(name = "rowProcessingFailures")
  public Object[][] rowProcessingFailures() {
    return new Object[][] { { new IllegalStateException("Iterator failed") },
        { new AssertionError("Iterator failed") } };
  }

  @Test(dataProvider = "rowProcessingFailures")
  public void testProcessRowsFailureFlushesAndClosesWithoutEndingSegments(Throwable failure) throws IOException {
    Iterator<Row> rows = mock(Iterator.class);
    when(rows.hasNext()).thenReturn(true, true, true).thenThrow(failure);
    when(rows.next()).thenReturn(row(1), row(2), row(3));
    SparkPartitionWriter partitionWriter = createPartitionWriter();

    Throwable thrown = expectThrows(Throwable.class, () -> {
      try (SparkPartitionWriter writer = partitionWriter) {
        writer.processRows(rows);
      }
    });

    assertSame(thrown, failure);
    assertEquals(thrown.getSuppressed().length, 0);
    assertEquals(partitionWriter.getRecordCount(), 2L);
    verifyWritesAndClose(2, false);
  }

  @Test
  public void testProcessRowsSuccessFlushesAndClosesWithEndingSegments() throws IOException {
    try (SparkPartitionWriter writer = createPartitionWriter()) {
      writer.processRows(Arrays.asList(row(1), row(2), row(3)).iterator());
      assertEquals(writer.getRecordCount(), 3L);
    }

    verifyWritesAndClose(3, true);
  }

  private SparkPartitionWriter createPartitionWriter() {
    Properties properties = new Properties();
    properties.setProperty(PARTITION_COUNT, "1");
    properties.setProperty(VALUE_SCHEMA_ID_PROP, "1");
    return new SparkPartitionWriter(properties, accumulators) {
      @Override
      protected AbstractVeniceWriter<byte[], byte[], byte[]> createBasicVeniceWriter() {
        return veniceWriter;
      }
    };
  }

  private Row row(int key) {
    return new GenericRowWithSchema(new Object[] { new byte[] { (byte) key }, new byte[] { 1 }, null }, DEFAULT_SCHEMA);
  }

  private void verifyWritesAndClose(int recordCount, boolean endSegments) throws IOException {
    assertEquals(accumulators.outputRecordCounter.value().longValue(), (long) recordCount);
    assertEquals(accumulators.partitionWriterCloseCounter.value().longValue(), 1L);
    InOrder inOrder = inOrder(veniceWriter);
    inOrder.verify(veniceWriter, times(recordCount)).put(any(), any(), eq(1), any(), isNull());
    inOrder.verify(veniceWriter).flush();
    inOrder.verify(veniceWriter).close(endSegments);
    inOrder.verifyNoMoreInteractions();
  }
}
