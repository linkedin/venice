package com.linkedin.davinci.ingestion;

import com.linkedin.venice.utils.ForkedJavaProcess;

import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.davinci.ingestion.IngestionUtils.*;


public class IngestionUtilsTest {
  private static final int SERVICE_PORT = 27015;
  private static final int MAX_ATTEMPT = 100;

  @Test
  public void testReleaseTargetPortBinding() throws Exception {
    ForkedJavaProcess forkedIngestionServiceProcess = ForkedJavaProcess.exec(IngestionService.class, String.valueOf(SERVICE_PORT));
    waitPortBinding(SERVICE_PORT, MAX_ATTEMPT);
    Assert.assertEquals(forkedIngestionServiceProcess.isAlive(), true);

    long processId = Long.parseLong(constructStringFromInputStream(executeShellCommand(new String[]{"lsof", "-t", "-i", ":" + SERVICE_PORT})));
    Assert.assertEquals(processId, forkedIngestionServiceProcess.getPid());

    String fullProcessName = constructStringFromInputStream(executeShellCommand(new String[]{"ps", "-p",
        String.valueOf(processId), "-o", "command"}));
    Assert.assertEquals(fullProcessName.contains(IngestionService.class.getName()), true);

    IngestionUtils.releaseTargetPortBinding(SERVICE_PORT);
    forkedIngestionServiceProcess = ForkedJavaProcess.exec(IngestionService.class, String.valueOf(SERVICE_PORT));
    Assert.assertEquals(forkedIngestionServiceProcess.isAlive(), true);
    forkedIngestionServiceProcess.destroy();
  }
}
