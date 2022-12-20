package com.linkedin.venice.fastclient.transport;

import static com.linkedin.venice.fastclient.utils.ClientTestUtils.FASTCLIENT_HTTP_VARIANTS;
import static org.testng.Assert.assertTrue;

import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.fastclient.utils.ClientTestUtils;
import com.linkedin.venice.utils.DataProviderUtils;
import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class R2TransportClientTest {
  @DataProvider(name = "fastClientHTTPVariantsAndGetOrPut")
  public static Object[][] httpVersionsAndGetOrPut() {
    return DataProviderUtils.allPermutationGenerator(FASTCLIENT_HTTP_VARIANTS, DataProviderUtils.BOOLEAN);
  }

  @Test(dataProvider = "fastClientHTTPVariantsAndGetOrPut", expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "Exception caught and handled as expected")
  public void testUnreachableHost(ClientTestUtils.FastClientHTTPVariant fastClientHTTPVariant, boolean isGet)
      throws Exception {
    Client r2Client = ClientTestUtils.getR2Client(fastClientHTTPVariant);
    R2TransportClient r2TransportClient = new R2TransportClient(r2Client);

    String fakeUrl = "https://fake.host/test_path";
    try {
      CompletableFuture future;
      if (isGet) {
        future = r2TransportClient.get(fakeUrl);
      } else {
        future = r2TransportClient.post(fakeUrl, "".getBytes());
      }
      future.get();
    } catch (Exception e) {
      /**
       * TODO: in the main code path, we may need to extract whether the root cause is `UnknownHostException` to detect the unavailable host.
       */
      switch (fastClientHTTPVariant) {
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
      // Throwing an exception here and expect this in the test as a way to validate
      // that the test actually threw the right exceptions as shown above
      throw new VeniceClientException("Exception caught and handled as expected");
    } finally {
      r2Client.shutdown(null);
    }
  }

  @Test(expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "'streamPost' is not supported.")
  public void testStreamPost() throws Exception {
    Client r2Client = ClientTestUtils.getR2Client(ClientTestUtils.FastClientHTTPVariant.HTTP_2_BASED_HTTPCLIENT5);
    R2TransportClient r2TransportClient = new R2TransportClient(r2Client);

    r2TransportClient.streamPost(null, null, null, null, 0);
  }
}
