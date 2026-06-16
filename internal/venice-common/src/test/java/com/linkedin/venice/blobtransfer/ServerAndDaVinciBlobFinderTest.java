package com.linkedin.venice.blobtransfer;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ServerAndDaVinciBlobFinderTest {
  private static final String STORE_NAME = "testStore";
  private static final int VERSION = 1;
  private static final int PARTITION = 0;

  @Test
  public void testDiscoverBlobPeersReturnsDaVinciPeersBeforeServerPeers() {
    BlobFinder daVinciBlobFinder = mock(BlobFinder.class);
    BlobPeersDiscoveryResponse daVinciResponse = new BlobPeersDiscoveryResponse();
    daVinciResponse.setDiscoveryResult(Collections.singletonList("dvc-host"));
    doReturn(daVinciResponse).when(daVinciBlobFinder).discoverBlobPeers(anyString(), anyInt(), anyInt());

    BlobFinder serverBlobFinder = mock(BlobFinder.class);
    BlobPeersDiscoveryResponse serverResponse = new BlobPeersDiscoveryResponse();
    serverResponse.setDiscoveryResult(Collections.singletonList("server-host"));
    doReturn(serverResponse).when(serverBlobFinder).discoverBlobPeers(anyString(), anyInt(), anyInt());

    ServerAndDaVinciBlobFinder finder = new ServerAndDaVinciBlobFinder(daVinciBlobFinder, serverBlobFinder);

    BlobPeersDiscoveryResponse response = finder.discoverBlobPeers(STORE_NAME, VERSION, PARTITION);

    Assert.assertFalse(response.isError());
    Assert.assertEquals(response.getDiscoveryResult(), Arrays.asList("dvc-host", "server-host"));
    verify(daVinciBlobFinder).discoverBlobPeers(STORE_NAME, VERSION, PARTITION);
    verify(serverBlobFinder).discoverBlobPeers(STORE_NAME, VERSION, PARTITION);
  }

  @Test
  public void testDiscoverBlobPeersUsesServerPeersWhenDaVinciDiscoveryHasNoPeers() {
    BlobFinder daVinciBlobFinder = mock(BlobFinder.class);
    BlobPeersDiscoveryResponse daVinciResponse = new BlobPeersDiscoveryResponse();
    daVinciResponse.setDiscoveryResult(Collections.emptyList());
    doReturn(daVinciResponse).when(daVinciBlobFinder).discoverBlobPeers(anyString(), anyInt(), anyInt());

    BlobFinder serverBlobFinder = mock(BlobFinder.class);
    BlobPeersDiscoveryResponse serverResponse = new BlobPeersDiscoveryResponse();
    serverResponse.setDiscoveryResult(Collections.singletonList("server-host"));
    doReturn(serverResponse).when(serverBlobFinder).discoverBlobPeers(anyString(), anyInt(), anyInt());
    ServerAndDaVinciBlobFinder finder = new ServerAndDaVinciBlobFinder(daVinciBlobFinder, serverBlobFinder);

    BlobPeersDiscoveryResponse response = finder.discoverBlobPeers(STORE_NAME, VERSION, PARTITION);

    Assert.assertFalse(response.isError());
    Assert.assertEquals(response.getDiscoveryResult(), Collections.singletonList("server-host"));
  }

  @Test
  public void testDiscoverBlobPeersUsesDaVinciPeersWhenServerDiscoveryErrors() {
    BlobFinder daVinciBlobFinder = mock(BlobFinder.class);
    BlobPeersDiscoveryResponse daVinciResponse = new BlobPeersDiscoveryResponse();
    daVinciResponse.setDiscoveryResult(Collections.singletonList("dvc-host"));
    doReturn(daVinciResponse).when(daVinciBlobFinder).discoverBlobPeers(anyString(), anyInt(), anyInt());

    BlobFinder serverBlobFinder = mock(BlobFinder.class);
    BlobPeersDiscoveryResponse serverResponse = new BlobPeersDiscoveryResponse();
    serverResponse.setError(true);
    serverResponse.setErrorMessage("server discovery failed");
    doReturn(serverResponse).when(serverBlobFinder).discoverBlobPeers(anyString(), anyInt(), anyInt());

    ServerAndDaVinciBlobFinder finder = new ServerAndDaVinciBlobFinder(daVinciBlobFinder, serverBlobFinder);

    BlobPeersDiscoveryResponse response = finder.discoverBlobPeers(STORE_NAME, VERSION, PARTITION);

    Assert.assertFalse(response.isError());
    Assert.assertEquals(response.getDiscoveryResult(), Collections.singletonList("dvc-host"));
  }

  @Test
  public void testCloseClosesBothFinders() throws Exception {
    BlobFinder daVinciBlobFinder = mock(BlobFinder.class);
    BlobFinder serverBlobFinder = mock(BlobFinder.class);

    new ServerAndDaVinciBlobFinder(daVinciBlobFinder, serverBlobFinder).close();

    verify(daVinciBlobFinder).close();
    verify(serverBlobFinder).close();
  }
}
