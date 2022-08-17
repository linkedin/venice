package com.linkedin.venice.fastclient;

import static org.testng.Assert.*;

import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.fastclient.transport.R2TransportClient;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.Test;


public class R2TransportClientTest {
  @Test
  public void testUnreachableHost() {
    Client r2Client = ClientTestUtils.getR2Client();
    R2TransportClient r2TransportClient = new R2TransportClient(r2Client);

    String fakeUrl = "https://fake.host/test_path";
    try {
      CompletableFuture future = r2TransportClient.get(fakeUrl);
      future.get();
    } catch (Exception e) {
      /**
       * TODO: in the main code path, we may need to extract whether the root cause is `UnknownHostException` to detect the unavailable host.
       */
      assertTrue(
          e instanceof ExecutionException && e.getCause() instanceof VeniceClientException,
          "VeniceClientException is expected here");
    } finally {
      r2Client.shutdown(null);
    }
  }
}
