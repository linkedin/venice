package com.linkedin.venice.blobtransfer;

import com.linkedin.venice.blobtransfer.client.NettyFileTransferClient;
import com.linkedin.venice.blobtransfer.server.P2PBlobTransferService;
import com.linkedin.venice.exceptions.VenicePeersNotFoundException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CompletionStage;


/**
 * Netty based P2P blob transfer manager implementation.
 * Upon start, it starts the blob transfer service and the client, so it can receive requests from peers to serve
 * blobs and in the meanwhile, it can make requests to other peers to fetch blobs.
 */
public class NettyP2PBlobTransferManager implements P2PBlobTransferManager<Void> {
  private final P2PBlobTransferService blobTransferService;
  // netty client is responsible to make requests against other peers for blob fetching
  protected final NettyFileTransferClient nettyClient;
  // peer finder is responsible to find the peers that have the requested blob
  protected final BlobFinder peerFinder;

  public NettyP2PBlobTransferManager(
      P2PBlobTransferService blobTransferService,
      NettyFileTransferClient nettyClient,
      BlobFinder peerFinder) {
    this.blobTransferService = blobTransferService;
    this.nettyClient = nettyClient;
    this.peerFinder = peerFinder;
  }

  @Override
  public void start() throws Exception {
    blobTransferService.start();
  }

  @Override
  public CompletionStage<InputStream> get(String storeName, int version, int partition)
      throws VenicePeersNotFoundException {
    CompletionStage<InputStream> inputStream;
    BlobPeersDiscoveryResponse response = peerFinder.discoverBlobPeers(storeName, version, partition);
    if (response == null || response.isError()) {
      throw new VenicePeersNotFoundException("Failed to obtain the peers for the requested blob");
    }
    List<String> discoverPeers = response.getDiscoveryResult();
    if (discoverPeers == null || discoverPeers.isEmpty()) {
      throw new VenicePeersNotFoundException("No peers found for the requested blob");
    }
    try {
      // TODO: add some retry logic or strategy to choose the peers differently in case of failure
      // instanceName comes as a format of <hostName>_<applicationPort>
      String chosenHost = discoverPeers.get(0).split("_")[0];
      inputStream = nettyClient.get(chosenHost, storeName, version, partition);
    } catch (Exception e) {
      throw new VenicePeersNotFoundException("The connection to peers failed", e);
    }

    return inputStream;
  }

  @Override
  public void close() throws Exception {
    blobTransferService.close();
    nettyClient.close();
    peerFinder.close();
  }
}
