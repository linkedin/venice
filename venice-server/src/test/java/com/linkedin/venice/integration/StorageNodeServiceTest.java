package com.linkedin.venice.integration;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.utils.SslUtils;
import java.io.IOException;
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
    VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster();

    //Note: Router in the cluster wont be able to talk to this server because it only talks ssl
    VeniceServerWrapper sslServer = ServiceFactory.getVeniceServer(venice.getClusterName(), venice.getKafka(), false, false, true);
    CloseableHttpAsyncClient client = SslUtils.getSslClient();
    client.start();

    // This should work, talking ssl to an ssl storage node
    HttpGet httpsRequest = new HttpGet("https://" + sslServer.getAddress() + "/health");
    HttpResponse httpsResponse = client.execute(httpsRequest, null).get();
    Assert.assertEquals(httpsResponse.getStatusLine().getStatusCode(), 200);
    Assert.assertEquals(IOUtils.toString(httpsResponse.getEntity().getContent()), "OK");

    // This should not work, talking non-ssl to an ssl storage node
    HttpGet httpRequest = new HttpGet("http://" + sslServer.getAddress() + "/health");
    HttpResponse httpResponse = client.execute(httpRequest, null).get();
    Assert.assertEquals(httpResponse.getStatusLine().getStatusCode(), 403);
    Assert.assertEquals(IOUtils.toString(httpResponse.getEntity().getContent()), "SSL Required");

    client.close();
    sslServer.close();
    venice.close();
  }
}
