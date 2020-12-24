package com.linkedin.davinci.ingestion;

import com.linkedin.venice.utils.ForkedJavaProcess;

import com.linkedin.venice.utils.Utils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.davinci.ingestion.IngestionUtils.*;


public class IngestionUtilsTest {
  private static final int MAX_ATTEMPT = 100;

  @Test
  public void testReleaseTargetPortBinding() throws Exception {
    int servicePort = Utils.getFreePort();
    ForkedJavaProcess forkedIngestionServiceProcess = ForkedJavaProcess.exec(IngestionService.class, String.valueOf(servicePort));
    waitPortBinding(servicePort, MAX_ATTEMPT);
    Assert.assertEquals(forkedIngestionServiceProcess.isAlive(), true);

    long processId = Long.parseLong(constructStringFromInputStream(executeShellCommand(new String[]{"lsof", "-t", "-i", ":" + servicePort})));
    Assert.assertEquals(processId, forkedIngestionServiceProcess.getPid());

    String fullProcessName = constructStringFromInputStream(executeShellCommand(new String[]{"ps", "-p",
        String.valueOf(processId), "-o", "command"}));
    Assert.assertEquals(fullProcessName.contains(IngestionService.class.getName()), true);

    IngestionUtils.releaseTargetPortBinding(servicePort);
    forkedIngestionServiceProcess = ForkedJavaProcess.exec(IngestionService.class, String.valueOf(servicePort));
    Assert.assertEquals(forkedIngestionServiceProcess.isAlive(), true);
    forkedIngestionServiceProcess.destroy();
  }
}
