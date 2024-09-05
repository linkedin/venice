package com.linkedin.venice.blobtransfer;

import static com.linkedin.venice.client.store.ClientFactory.getTransportClient;

import com.linkedin.venice.blobtransfer.client.NettyFileTransferClient;
import com.linkedin.venice.blobtransfer.server.P2PBlobTransferService;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.client.store.AvroGenericStoreClientImpl;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class BlobTransferUtil {
  private static final Logger LOGGER = LogManager.getLogger(BlobTransferUtil.class);

  /**
   * Get a P2P blob transfer manager for DaVinci Client and start it.
   * @param p2pTransferPort, the port used by the P2P transfer service and client
   * @param baseDir, the base directory of the underlying storage
   * @param clientConfig, the client config to start up a transport client
   * @return the blob transfer manager
   * @throws Exception
   */
  public static BlobTransferManager<Void> getP2PBlobTransferManagerForDVCAndStart(
      int p2pTransferPort,
      String baseDir,
      ClientConfig clientConfig) {
    return getP2PBlobTransferManagerForDVCAndStart(p2pTransferPort, p2pTransferPort, baseDir, clientConfig);
  }

  public static BlobTransferManager<Void> getP2PBlobTransferManagerForDVCAndStart(
      int p2pTransferServerPort,
      int p2pTransferClientPort,
      String baseDir,
      ClientConfig clientConfig) {
    try {
      AbstractAvroStoreClient storeClient =
          new AvroGenericStoreClientImpl<>(getTransportClient(clientConfig), false, clientConfig);
      BlobTransferManager<Void> manager = new NettyP2PBlobTransferManager(
          new P2PBlobTransferService(p2pTransferServerPort, baseDir),
          new NettyFileTransferClient(p2pTransferClientPort, baseDir),
          new DaVinciBlobFinder(storeClient));
      manager.start();
      return manager;
    } catch (Exception e) {
      // swallow the exception and continue the consumption via pubsub system
      LOGGER.warn("Failed to start up the P2P blob transfer manager for DaVinci Client", e);
      return null;
    }
  }

  /**
   * Get a P2P blob transfer manager for Service and start it.
   * @param p2pTransferServerPort the port used by the P2P transfer service
   * @param p2pTransferClientPort the port used by the P2P transfer client
   * @param baseDir the base directory of the underlying storage
   * @param customizedViewFuture the future of the customized view repository
   * @return the blob transfer manager
   */
  public static BlobTransferManager<Void> getP2PBlobTransferManagerForServiceAndStart(
      int p2pTransferServerPort,
      int p2pTransferClientPort,
      String baseDir,
      CompletableFuture<HelixCustomizedViewOfflinePushRepository> customizedViewFuture) {
    try {
      BlobTransferManager<Void> manager = new NettyP2PBlobTransferManager(
          new P2PBlobTransferService(p2pTransferServerPort, baseDir),
          new NettyFileTransferClient(p2pTransferClientPort, baseDir),
          new ServerBlobFinder(customizedViewFuture));
      manager.start();
      return manager;
    } catch (Exception e) {
      // swallow the exception and continue the consumption via pubsub system
      LOGGER.warn("Failed to start up the P2P blob transfer manager for service", e);
      return null;
    }
  }
}
