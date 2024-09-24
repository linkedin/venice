package com.linkedin.venice.status;

import static com.linkedin.venice.status.PushJobDetailsStatus.COMPLETED;
import static com.linkedin.venice.status.PushJobDetailsStatus.ERROR;
import static com.linkedin.venice.status.PushJobDetailsStatus.KILLED;
import static com.linkedin.venice.status.PushJobDetailsStatus.isFailed;
import static com.linkedin.venice.status.PushJobDetailsStatus.isSucceeded;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.alpini.base.misc.CollectionUtil;
import com.linkedin.venice.utils.VeniceEnumValueTest;
import java.util.Map;
import org.testng.annotations.Test;


public class TestPushJobDetailsStatus extends VeniceEnumValueTest<PushJobDetailsStatus> {
  public TestPushJobDetailsStatus() {
    super(PushJobDetailsStatus.class);
  }

  @Override
  protected Map<Integer, PushJobDetailsStatus> expectedMapping() {
    return CollectionUtil.<Integer, PushJobDetailsStatus>mapBuilder()
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
