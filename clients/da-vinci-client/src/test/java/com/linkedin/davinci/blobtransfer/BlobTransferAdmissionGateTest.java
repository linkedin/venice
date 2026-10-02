package com.linkedin.davinci.blobtransfer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.Test;


/**
 * The gate is driven through its supplier constructor so the decisions can be exercised without allocating real
 * memory, which is the only way to cover the rejecting branches deterministically. The tests that do allocate exist
 * to prove the supplier stands in for something real.
 */
public class BlobTransferAdmissionGateTest {
  private static final String REPLICA_ID = "store_v1-0";
  private static final long ONE_GB = 1024L * 1024 * 1024;
  private static final long THRESHOLD_BYTES = 64L * 1024 * 1024;

  private static BlobTransferAdmissionGate gate(AtomicLong usedDirectMemory, long thresholdBytes) {
    return new BlobTransferAdmissionGate(thresholdBytes, true, usedDirectMemory::get);
  }

  @Test
  public void testAcceptsWhileUsageIsBelowTheThreshold() {
    assertTrue(gate(new AtomicLong(THRESHOLD_BYTES - 1), THRESHOLD_BYTES).canAcceptNewTransfer(REPLICA_ID));
  }

  @Test
  public void testDeclinesOnceUsageReachesTheThreshold() {
    // At the threshold, not merely past it: the point is to stop before the ceiling is exceeded, not after.
    assertFalse(gate(new AtomicLong(THRESHOLD_BYTES), THRESHOLD_BYTES).canAcceptNewTransfer(REPLICA_ID));
  }

  @Test
  public void testDeclinesWellPastTheThreshold() {
    assertFalse(gate(new AtomicLong(THRESHOLD_BYTES * 4), THRESHOLD_BYTES).canAcceptNewTransfer(REPLICA_ID));
  }

  @Test
  public void testTheDecisionFollowsUsageRatherThanBeingFixedAtConstruction() {
    // Each admission must re-read, or a host that recovered would stay locked out for the rest of its life.
    AtomicLong usedDirectMemory = new AtomicLong(THRESHOLD_BYTES * 2);
    BlobTransferAdmissionGate gate = gate(usedDirectMemory, THRESHOLD_BYTES);
    assertFalse(gate.canAcceptNewTransfer(REPLICA_ID));
    usedDirectMemory.set(0);
    assertTrue(gate.canAcceptNewTransfer(REPLICA_ID));
  }

  @Test
  public void testZeroThresholdLeavesTheGateOff() {
    // The shipped default, so merging this cannot change behaviour until an operator opts in.
    assertTrue(gate(new AtomicLong(Long.MAX_VALUE), 0).canAcceptNewTransfer(REPLICA_ID));
  }

  @Test
  public void testNegativeThresholdLeavesTheGateOff() {
    assertTrue(gate(new AtomicLong(Long.MAX_VALUE), -1).canAcceptNewTransfer(REPLICA_ID));
  }

  @Test
  public void testGateIsInertWithoutADedicatedAllocator() {
    // Without one, the only reading available describes every Netty user in the process, so comparing it against a
    // blob transfer ceiling would decline transfers for memory blob transfer never allocated.
    BlobTransferAdmissionGate gate = new BlobTransferAdmissionGate(THRESHOLD_BYTES, false, () -> Long.MAX_VALUE);
    assertTrue(gate.canAcceptNewTransfer(REPLICA_ID));
  }

  @Test
  public void testThresholdAtOrAboveTheProcessLimitIsRejected() {
    // Blob transfer holds a small fraction of the process limit, so a ceiling at or above it can never be reached and
    // the gate would be armed while protecting nothing.
    IllegalArgumentException atLimit =
        expectThrows(IllegalArgumentException.class, () -> BlobTransferAdmissionGate.validateThreshold(ONE_GB, ONE_GB));
    assertTrue(atLimit.getMessage().contains("MaxDirectMemorySize"), atLimit.getMessage());
    expectThrows(IllegalArgumentException.class, () -> BlobTransferAdmissionGate.validateThreshold(3 * ONE_GB, ONE_GB));
  }

  @Test
  public void testThresholdBelowTheProcessLimitIsAccepted() {
    assertEquals(BlobTransferAdmissionGate.validateThreshold(THRESHOLD_BYTES, ONE_GB), THRESHOLD_BYTES);
  }

  @Test
  public void testValidationIsSkippedWhenTheProcessLimitIsUnknown() {
    // Nothing to judge the ceiling against, and refusing to start would be worse than accepting an unverified one.
    assertEquals(
        BlobTransferAdmissionGate.validateThreshold(3 * ONE_GB, BlobTransferAdmissionGate.MAX_DIRECT_MEMORY_UNKNOWN),
        3 * ONE_GB);
  }

  @Test
  public void testDisabledThresholdSkipsValidation() {
    assertEquals(BlobTransferAdmissionGate.validateThreshold(0, ONE_GB), 0);
    assertEquals(BlobTransferAdmissionGate.validateThreshold(-1, ONE_GB), -1);
  }

  @Test
  public void testTheReadingTracksRealAllocationsOnADedicatedAllocator() {
    // The suppliers above stand in for this: a dedicated allocator's usedDirectMemory() has to move with buffers that
    // are actually held, or every test here would be measuring nothing.
    PooledByteBufAllocator allocator = new PooledByteBufAllocator(true, 2, 2, 8192, 4);
    BlobTransferAdmissionGate gate = new BlobTransferAdmissionGate(allocator, 8L * 1024 * 1024);
    assertTrue(gate.canAcceptNewTransfer(REPLICA_ID));

    ByteBuf held = allocator.directBuffer(16 * 1024 * 1024);
    try {
      assertFalse(gate.canAcceptNewTransfer(REPLICA_ID), "a held buffer above the ceiling must decline");
    } finally {
      held.release();
    }
    assertTrue(gate.canAcceptNewTransfer(REPLICA_ID), "releasing must let transfers back in");
  }

  @Test
  public void testTheGateIsInertOnTheSharedDefaultAllocator() {
    // Constructed the way production does when the dedicated allocator is disabled.
    BlobTransferAdmissionGate gate = new BlobTransferAdmissionGate(PooledByteBufAllocator.DEFAULT, 1024 * 1024);
    ByteBuf held = PooledByteBufAllocator.DEFAULT.directBuffer(16 * 1024 * 1024);
    try {
      assertTrue(gate.canAcceptNewTransfer(REPLICA_ID));
    } finally {
      held.release();
    }
  }
}
