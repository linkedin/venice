package com.linkedin.venice.blobtransfer;

import com.linkedin.venice.blobtransfer.client.NettyFileTransferClient;
import com.linkedin.venice.blobtransfer.server.P2PBlobTransferService;
import com.linkedin.venice.client.store.AvroGenericStoreClientImpl;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class BlobTransferUtil {
  private static final Logger LOGGER = LogManager.getLogger(BlobTransferUtil.class);

  /**
   * Get a P2P blob transfer manager and start it.
   * @param p2pTransferPort, the port used by the P2P transfer service and client
   * @param baseDir, the base directory of the underlying storage
   * @param clientConfig, the client config to start up a transport client
   * @return
   * @throws Exception
   */
  public static BlobTransferManager<Void> getP2PBlobTransferManagerAndStart(
      int p2pTransferPort,
      String baseDir,
      ClientConfig clientConfig) {
    return getP2PBlobTransferManagerAndStart(p2pTransferPort, p2pTransferPort, baseDir, clientConfig);
  }

  public static BlobTransferManager<Void> getP2PBlobTransferManagerAndStart(
      int p2pTransferServerPort,
      int p2pTransferClientPort,
      String baseDir,
      ClientConfig clientConfig) {
    try {
      AvroGenericStoreClientImpl storeClient =
          (AvroGenericStoreClientImpl) ClientFactory.getAndStartGenericAvroClient(clientConfig);
      BlobTransferManager<Void> manager = new NettyP2PBlobTransferManager(
          new P2PBlobTransferService(p2pTransferServerPort, baseDir),
          new NettyFileTransferClient(p2pTransferClientPort, baseDir),
          new DaVinciBlobFinder(storeClient));
      manager.start();
      return manager;
    } catch (Exception e) {
      // swallow the exception and continue the consumption via pubsub system
      LOGGER.warn("Failed to start up the P2P blob transfer manager", e);
      return null;
    }
  }
}
