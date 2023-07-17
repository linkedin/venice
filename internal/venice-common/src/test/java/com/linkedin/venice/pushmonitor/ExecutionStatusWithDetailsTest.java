package com.linkedin.venice.pushmonitor;

import org.testng.Assert;
import org.testng.annotations.Test;


public class ExecutionStatusWithDetailsTest {
  @Test
  public void testExecutionStatusWithDetailsGetter() {
    ExecutionStatusWithDetails executionStatusWithDetails =
        new ExecutionStatusWithDetails(ExecutionStatus.ERROR, "dummyString", false);
    Assert.assertEquals(executionStatusWithDetails.getStatus(), ExecutionStatus.ERROR);
    Assert.assertEquals(executionStatusWithDetails.getDetails(), "dummyString");
  }
}
