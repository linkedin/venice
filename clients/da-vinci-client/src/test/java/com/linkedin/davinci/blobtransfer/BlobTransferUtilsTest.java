package com.linkedin.davinci.blobtransfer;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.exceptions.VeniceException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class BlobTransferUtilsTest {
  @Test
  public void testIsBlobTransferManagerEnabled() {
    VeniceServerConfig serverConfig = Mockito.mock(VeniceServerConfig.class);
    boolean isIsolatedIngestionDisabled = false;
    Mockito.when(serverConfig.isBlobTransferManagerEnabled()).thenReturn(true);
    Mockito.when(serverConfig.isBlobTransferSslEnabled()).thenReturn(true);
    Mockito.when(serverConfig.isBlobTransferAclEnabled()).thenReturn(true);

    // Case 1: test when all conditions are met
    boolean result = BlobTransferUtils.isBlobTransferManagerEnabled(serverConfig, isIsolatedIngestionDisabled);
    Assert.assertTrue(result);

    // Case 2: isolatedIngestionEnabled is true
    boolean isIsolatedIngestionEnabled = true;
    Assert.expectThrows(
        VeniceException.class,
        () -> BlobTransferUtils.isBlobTransferManagerEnabled(serverConfig, isIsolatedIngestionEnabled));

    // Case 3: assert when blob transfer manager is enabled but SSL and ACL are not enabled, and it is not isolated
    // ingestion
    Mockito.when(serverConfig.isBlobTransferSslEnabled()).thenReturn(false);
    Mockito.when(serverConfig.isBlobTransferAclEnabled()).thenReturn(false);
    Assert.expectThrows(
        VeniceException.class,
        () -> BlobTransferUtils.isBlobTransferManagerEnabled(serverConfig, isIsolatedIngestionDisabled));
  }
}
