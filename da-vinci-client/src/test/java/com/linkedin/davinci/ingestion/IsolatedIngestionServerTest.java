package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.ingestion.isolated.IsolatedIngestionServer;
import com.linkedin.davinci.ingestion.main.MainIngestionRequestClient;
import com.linkedin.venice.utils.ForkedJavaProcess;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


public class IsolatedIngestionServerTest {
  @Test
  public void testShutdownAfterHeartbeatTimeout() throws IOException {
    int servicePort = Utils.getFreePort();
    Process isolatedIngestionService = ForkedJavaProcess.exec(
        IsolatedIngestionServer.class,
        Arrays.asList(String.valueOf(servicePort), String.valueOf(TimeUnit.SECONDS.toMillis(10))),
        new ArrayList<>(),
        Optional.empty()
    );

    MainIngestionRequestClient client = new MainIngestionRequestClient(servicePort);
    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.SECONDS, () -> {
      Assert.assertTrue(client.sendHeartbeatRequest());
    });
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Assert.assertFalse(isolatedIngestionService.isAlive());
    });
  }
}
