package com.linkedin.venice.status;

import static com.linkedin.venice.status.PushJobDetailsStatus.COMPLETED;
import static com.linkedin.venice.status.PushJobDetailsStatus.ERROR;
import static com.linkedin.venice.status.PushJobDetailsStatus.KILLED;
import static com.linkedin.venice.status.PushJobDetailsStatus.isFailed;
import static com.linkedin.venice.status.PushJobDetailsStatus.isSucceeded;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;


public class TestPushJobDetailsStatus {
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
