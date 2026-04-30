package com.linkedin.venice.exceptions;

/**
 * Thrown when a P2P blob-transfer client receives a metadata response from a peer
 * whose serialization protocol version for {@code PartitionState} or
 * {@code StoreVersionState} does not match the local binary's compiled-in
 * {@code currentProtocolVersion}. Used to fail fast at HTTP header-parse time,
 * before any body is consumed, so the transfer can fall over to the next peer or
 * to Kafka bootstrap without waiting for the per-host receive timeout.
 *
 * <p>Both sides advertise their {@code currentProtocolVersion}; the policy is exact
 * equality, since blob transfer is the fast path and any binary skew between peers
 * is bounded to rolling-deploy windows.
 */
public class VeniceBlobTransferIncompatibleSchemaException extends VeniceException {
  /** Sentinel meaning the peer did not provide a value for this protocol. */
  public static final int VERSION_UNKNOWN = -1;

  private final String peerHost;
  private final int peerPartitionStateVersion;
  private final int peerStoreVersionStateVersion;
  private final int localPartitionStateVersion;
  private final int localStoreVersionStateVersion;

  public VeniceBlobTransferIncompatibleSchemaException(
      String peerHost,
      int peerPartitionStateVersion,
      int peerStoreVersionStateVersion,
      int localPartitionStateVersion,
      int localStoreVersionStateVersion) {
    super(
        buildMessage(
            peerHost,
            peerPartitionStateVersion,
            peerStoreVersionStateVersion,
            localPartitionStateVersion,
            localStoreVersionStateVersion));
    this.peerHost = peerHost;
    this.peerPartitionStateVersion = peerPartitionStateVersion;
    this.peerStoreVersionStateVersion = peerStoreVersionStateVersion;
    this.localPartitionStateVersion = localPartitionStateVersion;
    this.localStoreVersionStateVersion = localStoreVersionStateVersion;
  }

  private static String buildMessage(
      String peerHost,
      int peerPartitionStateVersion,
      int peerStoreVersionStateVersion,
      int localPartitionStateVersion,
      int localStoreVersionStateVersion) {
    return "Peer " + peerHost + " serialized blob-transfer metadata with protocol versions" + " PartitionState="
        + render(peerPartitionStateVersion) + ", StoreVersionState=" + render(peerStoreVersionStateVersion)
        + "; local binary has PartitionState=" + localPartitionStateVersion + ", StoreVersionState="
        + localStoreVersionStateVersion;
  }

  private static String render(int v) {
    return v == VERSION_UNKNOWN ? "<unknown>" : Integer.toString(v);
  }

  public String getPeerHost() {
    return peerHost;
  }

  public int getPeerPartitionStateVersion() {
    return peerPartitionStateVersion;
  }

  public int getPeerStoreVersionStateVersion() {
    return peerStoreVersionStateVersion;
  }

  public int getLocalPartitionStateVersion() {
    return localPartitionStateVersion;
  }

  public int getLocalStoreVersionStateVersion() {
    return localStoreVersionStateVersion;
  }
}
