package com.linkedin.davinci.ingestion;

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


public class IngestionServiceTest {
  @Test
  public void testShutdownAfterHeartbeatTimeout() throws IOException {
    int servicePort = Utils.getFreePort();
    Process isolatedIngestionService = ForkedJavaProcess.exec(
        IngestionService.class,
        Arrays.asList(String.valueOf(servicePort), String.valueOf(TimeUnit.SECONDS.toMillis(10))),
        new ArrayList<>(),
        Optional.empty()
    );

    IngestionRequestClient client = new IngestionRequestClient(servicePort);
    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(true, client.sendHeartbeatRequest());
    });
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(isolatedIngestionService.isAlive(), false);
    });
  }
}
