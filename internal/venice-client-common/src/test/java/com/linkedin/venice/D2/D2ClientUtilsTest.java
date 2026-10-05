package com.linkedin.venice.D2;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


public class D2ClientUtilsTest {
  @AfterMethod(alwaysRun = true)
  public void clearInterrupt() {
    // Never leak an interrupt flag onto the shared TestNG worker thread.
    Thread.interrupted();
  }

  @Test
  public void testClose() {
    // Should not throw
    D2ClientUtils.shutdownClient(null);
  }

  /**
   * An interrupted D2 request must fail with the interrupt flag still set, so retry loops above it (for example D2
   * leader controller discovery) can tell a cancelled caller apart from an unreachable service.
   */
  @Test(timeOut = 30_000)
  public void testSendD2GetRequestKeepsTheInterruptFlag() {
    D2Client d2Client = Mockito.mock(D2Client.class);
    Mockito.doReturn(new CompletableFuture<RestResponse>()).when(d2Client).restRequest(Mockito.any(RestRequest.class));

    Thread.currentThread().interrupt();
    VeniceException e = Assert.expectThrows(
        VeniceException.class,
        () -> D2ClientUtils.sendD2GetRequest("d2://VeniceController/leader_controller", d2Client));

    Assert.assertTrue(e.getCause() instanceof InterruptedException, "Unexpected cause: " + e.getCause());
    Assert.assertTrue(Thread.currentThread().isInterrupted(), "The interrupt flag must still be set for the caller");
  }

  @Test(timeOut = 30_000)
  public void testStartClientKeepsTheInterruptFlag() {
    // The mock never calls back, so the start would wait for the whole timeout unless the wait is interrupted.
    D2Client d2Client = Mockito.mock(D2Client.class);

    Thread.currentThread().interrupt();
    VeniceException e = Assert.expectThrows(VeniceException.class, () -> D2ClientUtils.startClient(d2Client, 60_000));

    Assert.assertTrue(e.getCause() instanceof InterruptedException, "Unexpected cause: " + e.getCause());
    Assert.assertTrue(Thread.currentThread().isInterrupted(), "The interrupt flag must still be set for the caller");
  }
}
