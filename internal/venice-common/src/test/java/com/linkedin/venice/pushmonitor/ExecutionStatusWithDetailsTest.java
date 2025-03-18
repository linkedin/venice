package com.linkedin.venice.pushmonitor;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;


public class ExecutionStatusWithDetailsTest {
  @Test
  public void testExecutionStatusWithDetailsGetter() {
    ExecutionStatusWithDetails executionStatusWithDetails =
        new ExecutionStatusWithDetails(ExecutionStatus.ERROR, "dummyString", false);
    assertEquals(executionStatusWithDetails.getStatus(), ExecutionStatus.ERROR);
    assertEquals(executionStatusWithDetails.getDetails(), "dummyString");
  }

  @Test
  public void testToStringMethod() {
    ExecutionStatusWithDetails instance =
        new ExecutionStatusWithDetails(ExecutionStatus.COMPLETED, "Success", false, 1627890000000L);
    String expected =
        "ExecutionStatusWithDetails{status=COMPLETED, details='Success', noDaVinciStatusReport=false, statusUpdateTimestamp=1627890000000}";
    assertEquals(instance.toString(), expected, "Unexpected toString output");
  }
}
