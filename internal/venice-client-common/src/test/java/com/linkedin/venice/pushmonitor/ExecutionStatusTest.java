package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.ERROR;

import org.testng.Assert;
import org.testng.annotations.Test;


public class ExecutionStatusTest {
  @Test
  public void testErrorExecutionStatus() {
    for (ExecutionStatus status: ExecutionStatus.values()) {
      if (status == ERROR) {
        Assert.assertTrue(ExecutionStatus.isError(status));
      } else {
        Assert.assertFalse(ExecutionStatus.isError(status));
      }
    }
  }
}
