package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.ingestion.isolated.IsolatedIngestionServer;
import com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils;
import com.linkedin.venice.utils.ForkedJavaProcess;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.*;


public class IsolatedIngestionUtilsTest {
  private static final int MAX_ATTEMPT = 100;

  @Test
  public void testReleaseTargetPortBinding() throws Exception {
    int servicePort = Utils.getFreePort();
    ForkedJavaProcess forkedIngestionServiceProcess = ForkedJavaProcess.exec(IsolatedIngestionServer.class, String.valueOf(servicePort));
    waitPortBinding(servicePort, MAX_ATTEMPT);
    Assert.assertEquals(forkedIngestionServiceProcess.isAlive(), true);
    long processId = Long.parseLong(executeShellCommand("/usr/sbin/lsof -t -i :" + servicePort).split("\n")[0]);
    Assert.assertEquals(processId, forkedIngestionServiceProcess.pid());
    String fullProcessName = executeShellCommand("ps -p " + processId + " -o command");
    Assert.assertEquals(fullProcessName.contains(IsolatedIngestionServer.class.getName()), true);
    IsolatedIngestionUtils.releaseTargetPortBinding(servicePort);
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(executeShellCommand("/usr/sbin/lsof -t -i :" + servicePort), "");
    });
  }
}
