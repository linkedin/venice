package com.linkedin.venice.blobtransfer;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
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
    Assert.assertEquals(response.getServerHostNames(), Collections.singleton("server-host"));
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
    Assert.assertEquals(response.getServerHostNames(), Collections.singleton("server-host"));
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
    Assert.assertTrue(response.getServerHostNames().isEmpty());
  }

  @Test
  public void testServerHostNamesUseManagerHostNormalization() {
    BlobFinder daVinciBlobFinder = mock(BlobFinder.class);
    BlobPeersDiscoveryResponse daVinciResponse = new BlobPeersDiscoveryResponse();
    daVinciResponse.setDiscoveryResult(Collections.singletonList("dvc-host_1234"));
    doReturn(daVinciResponse).when(daVinciBlobFinder).discoverBlobPeers(anyString(), anyInt(), anyInt());

    BlobFinder serverBlobFinder = mock(BlobFinder.class);
    BlobPeersDiscoveryResponse serverResponse = new BlobPeersDiscoveryResponse();
    serverResponse.setDiscoveryResult(Arrays.asList("server-a_5678", "server-b_5678"));
    doReturn(serverResponse).when(serverBlobFinder).discoverBlobPeers(anyString(), anyInt(), anyInt());

    BlobPeersDiscoveryResponse response = new ServerAndDaVinciBlobFinder(daVinciBlobFinder, serverBlobFinder)
        .discoverBlobPeers(STORE_NAME, VERSION, PARTITION);

    Assert.assertEquals(response.getServerHostNames(), new HashSet<>(Arrays.asList("server-a", "server-b")));
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
