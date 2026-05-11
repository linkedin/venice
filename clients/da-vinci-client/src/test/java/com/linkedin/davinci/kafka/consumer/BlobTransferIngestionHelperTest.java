package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;

import com.linkedin.davinci.blobtransfer.BlobTransferManager;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.pubsub.PubSubContext;
import java.util.Collections;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Unit test for the version-status gate added to {@link BlobTransferIngestionHelper#shouldStartBlobTransfer}.
 * Blob-transfer fast-bootstrap must run for the store's current version (and on cold start when no
 * current version is set yet) but must be suppressed for any non-current version — including
 * future-slot versions during target-region push + deferred swap.
 */
public class BlobTransferIngestionHelperTest {
  private static final String STORE_NAME = "testStore";
  private static final int SUBSCRIBING_VERSION = 1;
  private static final int PARTITION = 0;
  private static final String REPLICA_ID = STORE_NAME + "_v" + SUBSCRIBING_VERSION + "-" + PARTITION;
  private static final String KAFKA_VERSION_TOPIC = STORE_NAME + "_v" + SUBSCRIBING_VERSION;

  /**
   * Truth table for the version-status gate. The version being subscribed to is fixed at
   * SUBSCRIBING_VERSION (= 1) by the test setup; the parameter is the store's current version.
   *
   * <pre>
   *   storeCurrentVersion       | expectFastBootstrapAllowed | reason
   *   --------------------------+----------------------------+--------------------------------
   *   NON_EXISTING_VERSION (0)  | true                       | cold start: no current set
   *   SUBSCRIBING_VERSION (1)   | true                       | subscribing == current
   *   2                         | false                      | subscribing < current (stale)
   *   99                        | false                      | subscribing < current (future)
   * </pre>
   *
   * Note: this test only verifies the early-return path (the gate). When the gate returns false,
   * shouldStartBlobTransfer returns false without consulting the store config or replica lag, so
   * we can assert false directly. The current==subscribing and cold-start cases fall through to
   * downstream checks and are covered by other tests.
   */
  @DataProvider(name = "blobTransferGateCases")
  public Object[][] blobTransferGateCases() {
    return new Object[][] { { 2, "version 1 < current 2 should suppress blob transfer" },
        { 99, "version 1 < current 99 should suppress blob transfer" } };
  }

  @Test(dataProvider = "blobTransferGateCases")
  public void testShouldStartBlobTransferSuppressedForNonCurrentVersion(int storeCurrentVersion, String description) {
    BlobTransferManager blobTransferManager = mock(BlobTransferManager.class);
    StorageService storageService = mock(StorageService.class);
    StorageMetadataService storageMetadataService = mock(StorageMetadataService.class);
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);

    BlobTransferIngestionHelper helper = new BlobTransferIngestionHelper(
        blobTransferManager,
        storageService,
        storageMetadataService,
        serverConfig,
        Collections.emptySet());

    Store store = mock(Store.class);
    when(store.getName()).thenReturn(STORE_NAME);
    when(store.getCurrentVersion()).thenReturn(storeCurrentVersion);
    when(store.isBlobTransferEnabled()).thenReturn(true);

    PubSubContext pubSubContext = mock(PubSubContext.class);

    boolean shouldStart = helper.shouldStartBlobTransfer(
        null,
        store,
        STORE_NAME,
        SUBSCRIBING_VERSION,
        PARTITION,
        REPLICA_ID,
        true,
        false,
        KAFKA_VERSION_TOPIC,
        pubSubContext);

    assertFalse(shouldStart, description);
  }
}
