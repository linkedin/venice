package com.linkedin.venice.status;

import static com.linkedin.venice.status.PushJobDetailsStatus.COMPLETED;
import static com.linkedin.venice.status.PushJobDetailsStatus.ERROR;
import static com.linkedin.venice.status.PushJobDetailsStatus.KILLED;
import static com.linkedin.venice.status.PushJobDetailsStatus.isFailed;
import static com.linkedin.venice.status.PushJobDetailsStatus.isSucceeded;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.dimensions.VeniceDimensionTestFixture;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.utils.CollectionUtils;
import com.linkedin.venice.utils.VeniceEnumValueTest;
import java.util.Map;
import org.testng.annotations.Test;


public class TestPushJobDetailsStatus extends VeniceEnumValueTest<PushJobDetailsStatus> {
  public TestPushJobDetailsStatus() {
    super(PushJobDetailsStatus.class);
  }

  @Override
  protected Map<Integer, PushJobDetailsStatus> expectedMapping() {
    return CollectionUtils.<Integer, PushJobDetailsStatus>mapBuilder()
        .put(0, PushJobDetailsStatus.STARTED)
        .put(1, PushJobDetailsStatus.COMPLETED)
        .put(2, PushJobDetailsStatus.ERROR)
        .put(3, PushJobDetailsStatus.NOT_CREATED)
        .put(4, PushJobDetailsStatus.UNKNOWN)
        .put(5, PushJobDetailsStatus.TOPIC_CREATED)
        .put(6, PushJobDetailsStatus.DATA_WRITER_COMPLETED)
        .put(7, PushJobDetailsStatus.KILLED)
        .put(8, PushJobDetailsStatus.END_OF_PUSH_RECEIVED)
        .put(9, PushJobDetailsStatus.START_OF_INCREMENTAL_PUSH_RECEIVED)
        .put(10, PushJobDetailsStatus.END_OF_INCREMENTAL_PUSH_RECEIVED)
        .build();
  }

  @Test
  public void testDimensionInterface() {
    Map<PushJobDetailsStatus, String> expectedValues = CollectionUtils.<PushJobDetailsStatus, String>mapBuilder()
        .put(PushJobDetailsStatus.STARTED, "started")
        .put(PushJobDetailsStatus.COMPLETED, "completed")
        .put(PushJobDetailsStatus.ERROR, "error")
        .put(PushJobDetailsStatus.NOT_CREATED, "not_created")
        .put(PushJobDetailsStatus.UNKNOWN, "unknown")
        .put(PushJobDetailsStatus.TOPIC_CREATED, "topic_created")
        .put(PushJobDetailsStatus.DATA_WRITER_COMPLETED, "data_writer_completed")
        .put(PushJobDetailsStatus.KILLED, "killed")
        .put(PushJobDetailsStatus.END_OF_PUSH_RECEIVED, "end_of_push_received")
        .put(PushJobDetailsStatus.START_OF_INCREMENTAL_PUSH_RECEIVED, "start_of_incremental_push_received")
        .put(PushJobDetailsStatus.END_OF_INCREMENTAL_PUSH_RECEIVED, "end_of_incremental_push_received")
        .build();
    new VeniceDimensionTestFixture<>(
        PushJobDetailsStatus.class,
        VeniceMetricsDimensions.VENICE_PUSH_JOB_EXECUTION_STATE,
        expectedValues).assertAll();
  }

  @Test
  public void testIsFailedOrIsSuccess() {
    for (PushJobDetailsStatus status: PushJobDetailsStatus.values()) {
      if (status == COMPLETED) {
        assertTrue(isSucceeded(status));
        assertFalse(isFailed(status));
      } else if (status == ERROR || status == KILLED) {
        assertTrue(isFailed(status));
        assertFalse(isSucceeded(status));
      } else {
        assertFalse(isSucceeded(status));
        assertFalse(isFailed(status));
      }
    }
  }

  @Test
  public void testIsTerminal() {
    for (PushJobDetailsStatus status: PushJobDetailsStatus.values()) {
      if (status == COMPLETED || status == ERROR || status == KILLED) {
        assertTrue(PushJobDetailsStatus.isTerminal(status.getValue()));
      } else {
        assertFalse(PushJobDetailsStatus.isTerminal(status.getValue()));
      }
    }
  }
}
