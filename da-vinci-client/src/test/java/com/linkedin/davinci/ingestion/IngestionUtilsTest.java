package com.linkedin.davinci.ingestion;

import com.linkedin.venice.utils.ForkedJavaProcess;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.concurrent.TimeUnit;
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
    long processId = Long.parseLong(executeShellCommand("/usr/sbin/lsof -t -i :" + servicePort).split("\n")[0]);
    Assert.assertEquals(processId, forkedIngestionServiceProcess.getPid());
    String fullProcessName = executeShellCommand("ps -p " + processId + " -o command");
    Assert.assertEquals(fullProcessName.contains(IngestionService.class.getName()), true);
    IngestionUtils.releaseTargetPortBinding(servicePort);
    TestUtils.waitForNonDeterministicAssertion(5000, TimeUnit.MILLISECONDS, () -> {
      Assert.assertEquals(executeShellCommand("/usr/sbin/lsof -t -i :" + servicePort), "");
    });
  }
}
