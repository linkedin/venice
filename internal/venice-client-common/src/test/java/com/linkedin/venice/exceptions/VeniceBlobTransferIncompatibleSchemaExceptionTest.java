package com.linkedin.venice.exceptions;

import org.testng.Assert;
import org.testng.annotations.Test;


public class VeniceBlobTransferIncompatibleSchemaExceptionTest {
  @Test
  public void testMessageAndPeerHostArePreserved() {
    String peerHost = "peer-host:1234";
    String message = "Blob transfer schema version mismatch: requester PartitionState=4, "
        + "StoreVersionState=5; local PartitionState=6, StoreVersionState=7";
    VeniceBlobTransferIncompatibleSchemaException e =
        new VeniceBlobTransferIncompatibleSchemaException(peerHost, message);

    Assert.assertEquals(e.getPeerHost(), peerHost);
    Assert.assertEquals(e.getMessage(), message);
  }
}
