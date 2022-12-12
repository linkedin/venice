package com.linkedin.venice.fastclient;

import static org.testng.Assert.assertTrue;

import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.fastclient.transport.R2TransportClient;
import com.linkedin.venice.fastclient.utils.ClientTestUtils;
import com.linkedin.venice.utils.DataProviderUtils;
import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class R2TransportClientTest {
  public static final Object[] HTTP_VERSIONS = { ClientTestUtils.ClientType.HTTP_1_1_BASED_R2_CLIENT,
      ClientTestUtils.ClientType.HTTP_2_BASED_R2_CLIENT, ClientTestUtils.ClientType.HTTP_2_BASED_HTTPCLIENT5 };

  @DataProvider(name = "Http-versions")
  public static Object[][] httpVersions() {
    return DataProviderUtils.allPermutationGenerator(HTTP_VERSIONS);
  }

  @Test(dataProvider = "Http-versions", expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "Exception caught and handled as expected")
  public void testUnreachableHost(ClientTestUtils.ClientType clientType) throws Exception {
    Client r2Client = ClientTestUtils.getR2Client(clientType);
    R2TransportClient r2TransportClient = new R2TransportClient(r2Client);

    String fakeUrl = "https://fake.host/test_path";
    try {
      CompletableFuture future = r2TransportClient.get(fakeUrl);
      future.get();
    } catch (Exception e) {
      /**
       * TODO: in the main code path, we may need to extract whether the root cause is `UnknownHostException` to detect the unavailable host.
       */
      switch (clientType) {
        case HTTP_1_1_BASED_R2_CLIENT:
        case HTTP_2_BASED_R2_CLIENT:
          assertTrue(
              e instanceof ExecutionException && e.getCause() instanceof VeniceClientException
                  && e.getCause().getCause() instanceof RemoteInvocationException
                  && e.getCause().getCause().getCause() instanceof UnknownHostException,
              "Incorrect exceptions thrown");
          break;
        case HTTP_2_BASED_HTTPCLIENT5:
          assertTrue(
              e instanceof ExecutionException && e.getCause() instanceof VeniceClientException
                  && e.getCause().getCause() instanceof UnknownHostException,
              "Incorrect exceptions thrown");
          break;
        default:
          throw new VeniceClientException("unhandled client type");
      }
      throw new VeniceClientException("Exception caught and handled as expected");
    } finally {
      r2Client.shutdown(null);
    }
  }
}
