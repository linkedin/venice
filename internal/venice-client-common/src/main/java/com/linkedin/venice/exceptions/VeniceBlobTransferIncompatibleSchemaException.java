package com.linkedin.venice.exceptions;

/**
 * Thrown when a P2P blob-transfer peer rejects the requester's schema-version
 * advertisement (the server compares the requester's compiled-in protocol versions
 * for {@code PartitionState} / {@code StoreVersionState} against its own and refuses
 * the transfer up front). Used to fail over to the next peer or to Kafka bootstrap
 * without waiting for the per-host receive timeout.
 *
 * <p>Both sides advertise their {@code currentProtocolVersion}; the policy is exact
 * equality, since blob transfer is the fast path and any binary skew between peers
 * is bounded to rolling-deploy windows.
 */
public class VeniceBlobTransferIncompatibleSchemaException extends VeniceException {
  private final String peerHost;

  public VeniceBlobTransferIncompatibleSchemaException(String peerHost, String message) {
    super(message);
    this.peerHost = peerHost;
  }

  public String getPeerHost() {
    return peerHost;
  }
}
