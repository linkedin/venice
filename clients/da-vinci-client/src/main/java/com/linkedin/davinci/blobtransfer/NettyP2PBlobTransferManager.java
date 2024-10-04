package com.linkedin.davinci.blobtransfer;

import com.linkedin.davinci.blobtransfer.client.NettyFileTransferClient;
import com.linkedin.davinci.blobtransfer.server.P2PBlobTransferService;
import com.linkedin.venice.blobtransfer.BlobFinder;
import com.linkedin.venice.blobtransfer.BlobPeersDiscoveryResponse;
import com.linkedin.venice.exceptions.VenicePeersNotFoundException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Netty based P2P blob transfer manager implementation.
 * Upon start, it starts the blob transfer service and the client, so it can receive requests from peers to serve
 * blobs and in the meanwhile, it can make requests to other peers to fetch blobs.
 */
public class NettyP2PBlobTransferManager implements P2PBlobTransferManager<Void> {
  private static final Logger LOGGER = LogManager.getLogger(NettyP2PBlobTransferManager.class);
  protected static final int MAX_RETRIES_FOR_BLOB_TRANSFER_PER_HOST = 3;
  private static final int MAX_DURATION_OF_BLOB_TRANSFER_IN_HOUR = 5;
  private static final Duration maxDuration = Duration.ofHours(MAX_DURATION_OF_BLOB_TRANSFER_IN_HOUR);
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
    BlobPeersDiscoveryResponse response = peerFinder.discoverBlobPeers(storeName, version, partition);
    if (response == null || response.isError()) {
      throw new VenicePeersNotFoundException("Failed to obtain the peers for the requested blob");
    }
    List<String> discoverPeers = response.getDiscoveryResult();
    if (discoverPeers == null || discoverPeers.isEmpty()) {
      throw new VenicePeersNotFoundException("No peers found for the requested blob");
    }
    LOGGER
        .info("Discovered peers {} for store {} version {} partition {}", discoverPeers, storeName, version, partition);

    Instant startTime = Instant.now();
    for (String peer: discoverPeers) {
      int retryCount = 0;
      while (retryCount < MAX_RETRIES_FOR_BLOB_TRANSFER_PER_HOST) {

        if (Duration.between(startTime, Instant.now()).compareTo(maxDuration) > 0) {
          throw new VenicePeersNotFoundException("No valid peers found within the maximum duration for blob transfer.");
        }

        try {
          // instanceName comes as a format of <hostName>_<applicationPort>
          String chosenHost = peer.split("_")[0];
          LOGGER.info("Attempt {} to connect to host: {}", retryCount + 1, chosenHost);
          CompletionStage<InputStream> inputStreamStage = nettyClient.get(chosenHost, storeName, version, partition);
          CompletableFuture<InputStream> inputStreamFuture = inputStreamStage.toCompletableFuture();

          InputStream inputStream = inputStreamFuture.get(30, TimeUnit.MINUTES);

          LOGGER.info(
              "Successfully fetched blob from peer {} for store {} partition {} version {} in {} seconds",
              peer,
              storeName,
              partition,
              version,
              Duration.between(startTime, Instant.now()).getSeconds());
          return CompletableFuture.completedFuture(inputStream);
        } catch (Exception e) {
          LOGGER.warn(
              "Get error when connect to peer: {} for store {} partition {}, retrying {}/{}",
              peer,
              storeName,
              partition,
              retryCount + 1,
              MAX_RETRIES_FOR_BLOB_TRANSFER_PER_HOST,
              e);
          retryCount++;
        }
      }
      LOGGER.warn(
          "Failed to connect to peer {} for partition {} store {} version {} after {} attempts, moving to next possible host.",
          peer,
          partition,
          storeName,
          version,
          MAX_RETRIES_FOR_BLOB_TRANSFER_PER_HOST);
    }
    throw new VenicePeersNotFoundException(
        "No valid peers found for the requested blob after trying all possible hosts.");
  }

  @Override
  public void close() throws Exception {
    blobTransferService.close();
    nettyClient.close();
    peerFinder.close();
  }
}
