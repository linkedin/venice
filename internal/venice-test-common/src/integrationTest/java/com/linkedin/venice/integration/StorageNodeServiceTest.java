package com.linkedin.venice.integration;

import com.linkedin.venice.httpclient.HttpClientUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.testng.Assert;
import org.testng.annotations.Test;


public class StorageNodeServiceTest {
  @Test
  public void storageServerRespondsToRequests() throws ExecutionException, InterruptedException, IOException {
    Utils.thisIsLocalhost();
    try (
        CloseableHttpAsyncClient client =
            HttpClientUtils.getMinimalHttpClient(1, 1, Optional.of(SslUtils.getVeniceLocalSslFactory()));
        VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster(1, 1, 0, 1, 100, true, false)) {

      client.start();
      VeniceServerWrapper sslServer = venice.getVeniceServers().get(0);
      // This should work, talking ssl to an ssl storage node
      HttpGet httpsRequest = new HttpGet("https://" + sslServer.getAddress() + "/health");
      HttpResponse httpsResponse = client.execute(httpsRequest, null).get();
      String content = IOUtils.toString(httpsResponse.getEntity().getContent());
      Assert.assertEquals(httpsResponse.getStatusLine().getStatusCode(), 200);
      Assert.assertEquals(content, "OK");

      // This should not work, talking non-ssl to an ssl storage node
      HttpGet httpRequest = new HttpGet("http://" + sslServer.getAddress() + "/health");
      HttpResponse httpResponse = client.execute(httpRequest, null).get();
      Assert.assertEquals(httpResponse.getStatusLine().getStatusCode(), 403);
      Assert.assertEquals(IOUtils.toString(httpResponse.getEntity().getContent()), "SSL Required");
    }
  }
}
