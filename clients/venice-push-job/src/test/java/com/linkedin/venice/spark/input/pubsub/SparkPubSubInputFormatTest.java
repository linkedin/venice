package com.linkedin.venice.spark.input.pubsub;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.vpj.VenicePushJobConstants;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import com.linkedin.venice.vpj.pubsub.input.PubSubSplitPlanner;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SparkPubSubInputFormatTest {
  private VeniceProperties props;

  @BeforeMethod
  public void setUp() {
    Properties p = new Properties();
    p.setProperty(VenicePushJobConstants.KAFKA_INPUT_TOPIC, "test-topic");
    props = new VeniceProperties(p);
  }

  @Test
  public void testPlanInputPartitionsEmpty() {
    PubSubSplitPlanner planner = mock(PubSubSplitPlanner.class);
    when(planner.plan(props)).thenReturn(Collections.emptyList());

    Supplier<PubSubSplitPlanner> supplier = () -> planner;
    SparkPubSubInputFormat format = new SparkPubSubInputFormat(props, supplier);

    InputPartition[] partitions = format.planInputPartitions();

    assertNotNull(partitions);
    assertEquals(partitions.length, 0);
    verify(planner).plan(props);
    verifyNoMoreInteractions(planner);
  }

  @Test
  public void testPlanInputPartitionsNonEmpty() {
    PubSubPartitionSplit s1 = mock(PubSubPartitionSplit.class);
    PubSubPartitionSplit s2 = mock(PubSubPartitionSplit.class);
    List<PubSubPartitionSplit> planned = Arrays.asList(s1, s2);

    PubSubSplitPlanner planner = mock(PubSubSplitPlanner.class);
    when(planner.plan(props)).thenReturn(planned);

    Supplier<PubSubSplitPlanner> supplier = () -> planner;
    SparkPubSubInputFormat format = new SparkPubSubInputFormat(props, supplier);

    InputPartition[] partitions = format.planInputPartitions();

    assertNotNull(partitions);
    assertEquals(partitions.length, 2);
    assertTrue(partitions[0] instanceof SparkPubSubInputPartition);
    assertTrue(partitions[1] instanceof SparkPubSubInputPartition);
    verify(planner).plan(props);
    verifyNoMoreInteractions(planner);
  }

  @Test
  public void testCreateReaderFactory() {
    SparkPubSubInputFormat format = new SparkPubSubInputFormat(props);
    PartitionReaderFactory factory = format.createReaderFactory();
    assertNotNull(factory);
    assertTrue(factory instanceof SparkPubSubPartitionReaderFactory);
  }

  @Test
  public void testReadSchemaReturnsNull() {
    SparkPubSubInputFormat format = new SparkPubSubInputFormat(props);
    assertNull(format.readSchema());
  }
}
