package com.linkedin.venice.utils;

import org.apache.logging.log4j.Level;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestForkedProcessLogProcessing {
  private static final String ERROR_LOG_WITH_NEW_LINE = "01:23:45 ERROR THIS IS LOG MESSAGE | NEWLINE MESSAGE|";
  private static final String ERROR_LOG_WITH_NEW_LINE_PROCESS_EXPECTED =
      "01:23:45 THIS IS LOG MESSAGE \n NEWLINE MESSAGE";
  private static final String NO_LEVEL_LOG_WITH_NEW_LINE = "01:23:45 THIS IS LOG MESSAGE | NEWLINE MESSAGE|";
  private static final String NO_LEVEL_LOG_WITH_NEW_LINE_PROCESS_EXPECTED =
      "01:23:45 THIS IS LOG MESSAGE \n NEWLINE MESSAGE";

  @Test
  public void testLogLine() {
    ForkedJavaProcess.LogInfo logInfo = new ForkedJavaProcess.LogInfo();

    ForkedJavaProcess.processAndExtractLevelFromForkedProcessLog(logInfo, ERROR_LOG_WITH_NEW_LINE);
    Assert.assertEquals(logInfo.getLevel(), Level.ERROR);
    Assert.assertEquals(logInfo.getLog(), ERROR_LOG_WITH_NEW_LINE_PROCESS_EXPECTED);

    ForkedJavaProcess.processAndExtractLevelFromForkedProcessLog(logInfo, NO_LEVEL_LOG_WITH_NEW_LINE);
    Assert.assertEquals(logInfo.getLevel(), Level.INFO);
    Assert.assertEquals(logInfo.getLog(), NO_LEVEL_LOG_WITH_NEW_LINE_PROCESS_EXPECTED);
  }
}
