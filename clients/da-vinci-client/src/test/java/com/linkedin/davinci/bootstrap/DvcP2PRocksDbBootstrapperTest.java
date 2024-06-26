package com.linkedin.davinci.bootstrap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.DaVinciBackend;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.ingestion.IngestionBackend;
import com.linkedin.venice.blobtransfer.BlobFinder;
import com.linkedin.venice.blobtransfer.BlobTransferManager;
import com.linkedin.venice.blobtransfer.NettyP2PBlobTransferManager;
import com.linkedin.venice.blobtransfer.client.NettyFileTransferClient;
import com.linkedin.venice.blobtransfer.server.P2PBlobTransferService;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactoryTestUtils;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.exceptions.VeniceBootstrapException;
import com.linkedin.venice.utils.DataProviderUtils;
import java.io.InputStream;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class DvcP2PRocksDbBootstrapperTest {
  private DaVinciBackend backend;
  private VeniceServerConfig serverConfig;
  private VeniceStoreVersionConfig veniceStoreVersionConfig;
  private P2PBlobTransferService blobTransferService;
  private NettyFileTransferClient fileTransferClient;
  private TransportClient transportClient;
  private BlobFinder blobFinder;
  private BlobTransferManager p2pBlobTransferManager;
  private IngestionBackend ingestionBackend;

  private final String storeName = "store";
  private final int versionNumber = 1;
  private final int partitionId = 1;
  private final String baseDirector = "/tmp";

  @BeforeMethod
  public void setUp() throws Exception {
    backend = mock(DaVinciBackend.class);
    serverConfig = mock(VeniceServerConfig.class);
    veniceStoreVersionConfig = mock(VeniceStoreVersionConfig.class);
    blobTransferService = mock(P2PBlobTransferService.class);
    fileTransferClient = mock(NettyFileTransferClient.class);
    transportClient = mock(TransportClient.class);
    blobFinder = mock(BlobFinder.class);
    p2pBlobTransferManager = mock(NettyP2PBlobTransferManager.class);
    ingestionBackend = mock(IngestionBackend.class);

    when(serverConfig.getDvcP2pBlobTransferPort()).thenReturn(1234);
    when(serverConfig.getDvcP2pFileTransferPort()).thenReturn(5678);
    when(serverConfig.getRocksDBPath()).thenReturn(baseDirector);
    when(backend.getClientConfig()).thenReturn(mock(ClientConfig.class));
    when(backend.getClientConfig().getVeniceURL()).thenReturn("http://localhost:8080");
    when(backend.getIngestionBackend()).thenReturn(ingestionBackend);

    ClientConfig storeClientConfig = ClientConfig.defaultGenericClientConfig(storeName);
    ClientFactoryTestUtils.setUnitTestMode();
    ClientFactoryTestUtils.registerTransportClient(storeClientConfig, transportClient);

    CompletionStage<InputStream> mockCompletionStage = mock(CompletionStage.class);
    when(p2pBlobTransferManager.get(storeName, versionNumber, partitionId)).thenReturn(mockCompletionStage);
    doNothing().when(p2pBlobTransferManager).start();

    doAnswer(invocation -> {
      BiConsumer<InputStream, Throwable> callback = invocation.getArgument(0);
      callback.accept(mock(InputStream.class), null); // Simulate a successful completion
      return null;
    }).when(mockCompletionStage).whenComplete(any());
  }

  @Test(dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testBootstrapOptionsBasedOnSettings(boolean isBlobTransferEnabled, boolean isHybridStore)
      throws VeniceBootstrapException {

    DvcP2PRocksDbBootstrapper dvcP2PRocksDbBootstrapper = new DvcP2PRocksDbBootstrapper(
        backend,
        serverConfig,
        veniceStoreVersionConfig,
        isBlobTransferEnabled,
        isHybridStore,
        p2pBlobTransferManager);

    DvcP2PRocksDbBootstrapper spyBootstrapper = spy(dvcP2PRocksDbBootstrapper);
    spyBootstrapper.bootstrapDatabase(storeName, versionNumber, partitionId);

    if (!isBlobTransferEnabled || isHybridStore) {
      verify(spyBootstrapper, times(1)).bootstrapFromKafka(partitionId);
      verify(spyBootstrapper, never()).bootstrapFromBlobs(storeName, versionNumber, partitionId);
    } else {
      verify(spyBootstrapper, times(1)).bootstrapFromBlobs(storeName, versionNumber, partitionId);
    }
  }

  @Test
  public void testBootstrapFromBlobs_Success() throws Exception {
    DvcP2PRocksDbBootstrapper dvcP2PRocksDbBootstrapper = new DvcP2PRocksDbBootstrapper(
        backend,
        serverConfig,
        veniceStoreVersionConfig,
        true,
        false,
        p2pBlobTransferManager);

    RocksDB mockRocksDB = mock(RocksDB.class);
    RocksIterator mockIterator = mock(RocksIterator.class);

    DvcP2PRocksDbBootstrapper spyBootstrapper = spy(dvcP2PRocksDbBootstrapper);
    doReturn(mockRocksDB).when(spyBootstrapper).openRocksDB(any(Options.class), anyString());

    when(mockRocksDB.newIterator()).thenReturn(mockIterator);
    doNothing().when(mockIterator).seekToFirst();
    when(mockIterator.isValid()).thenReturn(true);

    spyBootstrapper.bootstrapDatabase(storeName, versionNumber, partitionId);

    verify(p2pBlobTransferManager).start();
    verify(p2pBlobTransferManager).get(storeName, versionNumber, partitionId);
    verify(p2pBlobTransferManager).close();
  }

  @Test
  public void testBootstrapFromBlobs_StartException() throws Exception {
    doThrow(new VeniceBootstrapException("Exception")).when(p2pBlobTransferManager).start();

    DvcP2PRocksDbBootstrapper dvcP2PRocksDbBootstrapper = new DvcP2PRocksDbBootstrapper(
        backend,
        serverConfig,
        veniceStoreVersionConfig,
        true,
        false,
        p2pBlobTransferManager);

    DvcP2PRocksDbBootstrapper spyBootstrapper = spy(dvcP2PRocksDbBootstrapper);
    spyBootstrapper.bootstrapDatabase(storeName, versionNumber, partitionId);
    verify(spyBootstrapper, times(1)).bootstrapFromKafka(partitionId);
  }

  @Test
  public void testBootstrapFromBlobs_GetException() throws Exception {
    when(p2pBlobTransferManager.get(storeName, versionNumber, partitionId))
        .thenThrow(new RuntimeException("Exception"));

    DvcP2PRocksDbBootstrapper dvcP2PRocksDbBootstrapper = new DvcP2PRocksDbBootstrapper(
        backend,
        serverConfig,
        veniceStoreVersionConfig,
        true,
        false,
        p2pBlobTransferManager);

    DvcP2PRocksDbBootstrapper spyBootstrapper = spy(dvcP2PRocksDbBootstrapper);
    doNothing().when(spyBootstrapper).bootstrapFromKafka(partitionId);

    spyBootstrapper.bootstrapFromBlobs(storeName, versionNumber, partitionId);

    verify(p2pBlobTransferManager).start();
    verify(p2pBlobTransferManager).get(storeName, versionNumber, partitionId);
    verify(spyBootstrapper).bootstrapFromKafka(partitionId);
  }
}
