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
    serverResponse.setDiscoveryResult(Collections.singletonList("server-host_1234"));
    doReturn(serverResponse).when(serverBlobFinder).discoverBlobPeers(anyString(), anyInt(), anyInt());

    ServerAndDaVinciBlobFinder finder = new ServerAndDaVinciBlobFinder(daVinciBlobFinder, serverBlobFinder);

    BlobPeersDiscoveryResponse response = finder.discoverBlobPeers(STORE_NAME, VERSION, PARTITION);

    Assert.assertFalse(response.isError());
    Assert.assertEquals(response.getDiscoveryResult(), Arrays.asList("dvc-host", "server-host_1234"));
    Assert.assertEquals(response.getServerHostNames(), Collections.singleton("server-host"));
    Assert.assertTrue(response.isSourceAware());
    verify(daVinciBlobFinder).discoverBlobPeers(STORE_NAME, VERSION, PARTITION);
    verify(serverBlobFinder).discoverBlobPeers(STORE_NAME, VERSION, PARTITION);
  }

  @Test
  public void testOverlappingNormalizedHostsRemainDaVinciSource() {
    BlobFinder daVinciBlobFinder = mock(BlobFinder.class);
    BlobPeersDiscoveryResponse daVinciResponse = new BlobPeersDiscoveryResponse();
    daVinciResponse.setDiscoveryResult(Collections.singletonList("shared-host_1111"));
    doReturn(daVinciResponse).when(daVinciBlobFinder).discoverBlobPeers(anyString(), anyInt(), anyInt());

    BlobFinder serverBlobFinder = mock(BlobFinder.class);
    BlobPeersDiscoveryResponse serverResponse = new BlobPeersDiscoveryResponse();
    serverResponse.setDiscoveryResult(Collections.singletonList("shared-host_2222"));
    doReturn(serverResponse).when(serverBlobFinder).discoverBlobPeers(anyString(), anyInt(), anyInt());

    ServerAndDaVinciBlobFinder finder = new ServerAndDaVinciBlobFinder(daVinciBlobFinder, serverBlobFinder);

    BlobPeersDiscoveryResponse response = finder.discoverBlobPeers(STORE_NAME, VERSION, PARTITION);

    Assert.assertFalse(response.isError());
    Assert.assertEquals(response.getDiscoveryResult(), Arrays.asList("shared-host_1111", "shared-host_2222"));
    Assert.assertTrue(response.getServerHostNames().isEmpty());
    Assert.assertTrue(response.isSourceAware());
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

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testServerHostNamesAreImmutable() {
    BlobPeersDiscoveryResponse response = new BlobPeersDiscoveryResponse();
    response.setServerHostNames(Collections.singleton("server-host"));

    response.getServerHostNames().add("other-server");
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
