package com.linkedin.davinci.blobtransfer;

import com.linkedin.davinci.blobtransfer.server.BlobTransferAdmissionController;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestBlobTransferAdmissionController {
  @Test
  public void testClientCapacityComputation() {
    // 25% of 15 -> floor(3.75) = 3
    Assert.assertEquals(new BlobTransferAdmissionController(15, 25).getMaxClientTransfers(), 3);
    // 25% of 4 -> floor(1.0) = 1
    Assert.assertEquals(new BlobTransferAdmissionController(4, 25).getMaxClientTransfers(), 1);
    // 25% of 2 -> floor(0.5) = 0, but a positive percentage floors at 1
    Assert.assertEquals(new BlobTransferAdmissionController(2, 25).getMaxClientTransfers(), 1);
    // 0% explicitly disables client-origin capacity
    Assert.assertEquals(new BlobTransferAdmissionController(15, 0).getMaxClientTransfers(), 0);
    // 50% (the max allowed) of 15 -> floor(7.5) = 7
    Assert.assertEquals(new BlobTransferAdmissionController(15, 50).getMaxClientTransfers(), 7);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testRejectsNonPositiveBudget() {
    new BlobTransferAdmissionController(0, 25);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testRejectsOutOfRangePercent() {
    // 51 is just above the 50% ceiling, so it must be rejected
    new BlobTransferAdmissionController(15, 51);
  }

  @Test
  public void testClientAdmittedUpToCap() {
    BlobTransferAdmissionController controller = new BlobTransferAdmissionController(15, 25); // clientCap = 3
    for (int i = 0; i < 3; i++) {
      Assert.assertTrue(controller.tryAdmitClient());
    }
    Assert.assertEquals(controller.getClientInFlight(), 3);
    // 4th client rejected once the reservation is full.
    Assert.assertFalse(controller.tryAdmitClient());
  }

  @Test
  public void testReleaseFreesClientSlot() {
    BlobTransferAdmissionController controller = new BlobTransferAdmissionController(4, 25); // clientCap = 1
    Assert.assertTrue(controller.tryAdmitClient());
    Assert.assertFalse(controller.tryAdmitClient());
    controller.releaseClient();
    Assert.assertEquals(controller.getClientInFlight(), 0);
    Assert.assertTrue(controller.tryAdmitClient());
  }

  @Test
  public void testReleaseUnderflowIsClamped() {
    BlobTransferAdmissionController controller = new BlobTransferAdmissionController(15, 25);
    // Releasing with nothing in flight must not throw or go negative.
    controller.releaseClient();
    Assert.assertEquals(controller.getClientInFlight(), 0);
  }

  @Test
  public void testZeroPercentRejectsAllClients() {
    BlobTransferAdmissionController controller = new BlobTransferAdmissionController(15, 0);
    Assert.assertFalse(controller.tryAdmitClient());
    Assert.assertEquals(controller.getClientInFlight(), 0);
  }
}
