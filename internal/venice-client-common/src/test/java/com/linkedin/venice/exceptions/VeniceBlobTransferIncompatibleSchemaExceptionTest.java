package com.linkedin.venice.exceptions;

import org.testng.Assert;
import org.testng.annotations.Test;


public class VeniceBlobTransferIncompatibleSchemaExceptionTest {
  @Test
  public void testKnownVersionsOnBothSides() {
    String peerHost = "peer-host:1234";
    VeniceBlobTransferIncompatibleSchemaException e =
        new VeniceBlobTransferIncompatibleSchemaException(peerHost, 4, 5, 6, 7);

    Assert.assertEquals(e.getPeerHost(), peerHost);
    Assert.assertEquals(e.getPeerPartitionStateVersion(), 4);
    Assert.assertEquals(e.getPeerStoreVersionStateVersion(), 5);
    Assert.assertEquals(e.getLocalPartitionStateVersion(), 6);
    Assert.assertEquals(e.getLocalStoreVersionStateVersion(), 7);

    String message = e.getMessage();
    Assert.assertTrue(message.contains(peerHost), "message should mention peer host: " + message);
    Assert.assertTrue(message.contains("PartitionState=4"), "message should mention peer PartitionState: " + message);
    Assert.assertTrue(
        message.contains("StoreVersionState=5"),
        "message should mention peer StoreVersionState: " + message);
    Assert.assertTrue(message.contains("PartitionState=6"), "message should mention local PartitionState: " + message);
    Assert.assertTrue(
        message.contains("StoreVersionState=7"),
        "message should mention local StoreVersionState: " + message);
    Assert.assertFalse(message.contains("<unknown>"), "all versions are known, message should not contain <unknown>");
  }

  @Test
  public void testPeerVersionsUnknownRendersUnknown() {
    String peerHost = "older-peer:5678";
    VeniceBlobTransferIncompatibleSchemaException e = new VeniceBlobTransferIncompatibleSchemaException(
        peerHost,
        VeniceBlobTransferIncompatibleSchemaException.VERSION_UNKNOWN,
        VeniceBlobTransferIncompatibleSchemaException.VERSION_UNKNOWN,
        3,
        9);

    Assert
        .assertEquals(e.getPeerPartitionStateVersion(), VeniceBlobTransferIncompatibleSchemaException.VERSION_UNKNOWN);
    Assert.assertEquals(
        e.getPeerStoreVersionStateVersion(),
        VeniceBlobTransferIncompatibleSchemaException.VERSION_UNKNOWN);
    Assert.assertEquals(e.getLocalPartitionStateVersion(), 3);
    Assert.assertEquals(e.getLocalStoreVersionStateVersion(), 9);

    String message = e.getMessage();
    Assert.assertTrue(
        message.contains("PartitionState=<unknown>"),
        "unknown peer PartitionState should render as <unknown>: " + message);
    Assert.assertTrue(
        message.contains("StoreVersionState=<unknown>"),
        "unknown peer StoreVersionState should render as <unknown>: " + message);
    Assert.assertTrue(message.contains("PartitionState=3"), "local PartitionState should be rendered: " + message);
    Assert
        .assertTrue(message.contains("StoreVersionState=9"), "local StoreVersionState should be rendered: " + message);
  }

  @Test
  public void testVersionUnknownSentinel() {
    Assert.assertEquals(VeniceBlobTransferIncompatibleSchemaException.VERSION_UNKNOWN, -1);
  }
}
