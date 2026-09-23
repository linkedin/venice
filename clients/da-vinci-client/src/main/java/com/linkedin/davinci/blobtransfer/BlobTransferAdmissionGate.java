package com.linkedin.davinci.blobtransfer;

import com.linkedin.venice.ConfigKeys;
import com.sun.management.HotSpotDiagnosticMXBean;
import io.netty.buffer.ByteBufAllocator;
import java.lang.management.ManagementFactory;
import java.util.function.LongSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Decides whether the receiver should start another blob transfer, by comparing the direct memory blob transfer is
 * currently holding against a configured ceiling.
 * <p>
 * The reading is the one #3019 made attributable: {@code usedDirectMemory()} on blob transfer's own allocator. It
 * counts oversized allocations while they are outstanding, so it reflects transfers in progress rather than only
 * what the pool has retained, which is what makes it usable at admission time.
 * <p>
 * Declining is cheap: the replica falls back to bootstrapping from the version topic, which is slower but complete,
 * so this errs towards declining rather than towards admitting.
 * <p>
 * What it bounds, and what it does not: this caps blob transfer's own contribution to direct memory. It cannot stop
 * an allocation failure that another consumer drove the process into, because it does not read the process total.
 * Size the ceiling from what blob transfer itself holds -- a receiver charges roughly 1MB per streaming transfer --
 * not from {@code -XX:MaxDirectMemorySize}.
 * <p>
 * The check reads a live counter rather than reserving against a budget. A transfer's memory is charged as it
 * streams, so several transfers admitted at once can all pass before any of them has grown, and usage can overshoot
 * the ceiling. The overshoot is bounded by how many transfers can start at once, and each one is small relative to
 * any sane ceiling, so it is accepted rather than tracked.
 */
public class BlobTransferAdmissionGate {
  private static final Logger LOGGER = LogManager.getLogger(BlobTransferAdmissionGate.class);

  /** Reported by {@link #maxDirectMemoryBytes()} when the limit cannot be established. */
  static final long MAX_DIRECT_MEMORY_UNKNOWN = 0L;

  private final long thresholdBytes;
  /**
   * Whether the reading describes blob transfer alone. False when blob transfer shares
   * {@link io.netty.buffer.PooledByteBufAllocator#DEFAULT} with the rest of the process, in which case a total that
   * includes the read path must not be compared against a blob transfer ceiling, and the gate stands down.
   */
  private final boolean allocatorIsDedicated;
  private final LongSupplier usedDirectMemorySupplier;

  /**
   * @param allocator the receiver's allocator, read through
   *        {@link BlobTransferPooledByteBufAllocator#usedDirectMemoryBytes}.
   * @param thresholdBytes how much direct memory blob transfer may hold before new transfers are declined, or zero
   *        to leave the gate off.
   */
  public BlobTransferAdmissionGate(ByteBufAllocator allocator, long thresholdBytes) {
    this(
        validateThreshold(thresholdBytes, maxDirectMemoryBytes()),
        BlobTransferPooledByteBufAllocator.isDedicated(allocator),
        () -> BlobTransferPooledByteBufAllocator.usedDirectMemoryBytes(allocator));
  }

  /**
   * Takes the reading as a supplier so a test can drive the decision without live memory pressure.
   */
  BlobTransferAdmissionGate(long thresholdBytes, boolean allocatorIsDedicated, LongSupplier usedDirectMemorySupplier) {
    this.thresholdBytes = thresholdBytes;
    this.allocatorIsDedicated = allocatorIsDedicated;
    this.usedDirectMemorySupplier = usedDirectMemorySupplier;
  }

  /**
   * @return true when another transfer may start. A false return has already logged why.
   */
  public boolean canAcceptNewTransfer(String replicaId) {
    // The two ways the gate is off: no ceiling was configured, or one was but blob transfer shares the process-wide
    // allocator, so the only reading available covers every Netty user rather than blob transfer. Either way there
    // is nothing to enforce, and the host behaves exactly as it did before this gate existed.
    if (thresholdBytes <= 0 || !allocatorIsDedicated) {
      return true;
    }
    long usedDirectMemory = usedDirectMemorySupplier.getAsLong();
    if (usedDirectMemory < thresholdBytes) {
      return true;
    }
    LOGGER.info(
        "Declining blob transfer for replica {}: blob transfer holds {} B of direct memory, ceiling {} B.",
        replicaId,
        usedDirectMemory,
        thresholdBytes);
    return false;
  }

  /**
   * Rejects a ceiling that could never decline a transfer, which would otherwise be silent: it looks like the gate is
   * armed while offering no protection. Blob transfer holds far less than the process limit, so a ceiling at or above
   * that limit is unreachable by definition.
   *
   * @param maxDirectMemoryBytes the process-wide limit to validate against, or {@link #MAX_DIRECT_MEMORY_UNKNOWN}
   *        when it could not be established, in which case the ceiling cannot be judged unreachable.
   *        Passed in rather than read here so both branches are reachable in a test.
   */
  static long validateThreshold(long thresholdBytes, long maxDirectMemoryBytes) {
    if (thresholdBytes <= 0) {
      return thresholdBytes;
    }
    if (maxDirectMemoryBytes != MAX_DIRECT_MEMORY_UNKNOWN && thresholdBytes >= maxDirectMemoryBytes) {
      throw new IllegalArgumentException(
          String.format(
              "%s of %d B is at or above this JVM's -XX:MaxDirectMemorySize of %d B, so an allocation would fail "
                  + "before the ceiling was ever reached and the check would never decline a transfer. This ceiling "
                  + "applies to blob transfer's own usage, which is a small fraction of the process limit, so size it "
                  + "from what blob transfer holds rather than from the limit.",
              ConfigKeys.BLOB_TRANSFER_RECEIVER_DIRECT_MEMORY_THROTTLE_THRESHOLD_BYTES,
              thresholdBytes,
              maxDirectMemoryBytes));
    }
    return thresholdBytes;
  }

  /**
   * @return this process's {@code -XX:MaxDirectMemorySize}, or {@link #MAX_DIRECT_MEMORY_UNKNOWN} when it cannot be
   *         read. It varies by deployment rather than being a fixed figure -- venice-server's base container sets 1G
   *         and only some fabrics raise it -- and a DaVinci Client inherits whatever its host application was given,
   *         so it is read at startup rather than assumed.
   *         <p>
   *         The JVM publishes it neither as a metric nor through the Java SE API, so this reads the HotSpot
   *         diagnostic bean. A JVM that does not offer that bean, or that was never given the flag, yields
   *         {@link #MAX_DIRECT_MEMORY_UNKNOWN}, and the ceiling is then accepted unvalidated rather than compared
   *         against a guessed limit.
   */
  private static long maxDirectMemoryBytes() {
    try {
      String value = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class)
          .getVMOption("MaxDirectMemorySize")
          .getValue();
      long bytes = Long.parseLong(value.trim());
      return bytes > 0 ? bytes : MAX_DIRECT_MEMORY_UNKNOWN;
    } catch (Exception | LinkageError e) {
      LOGGER.warn("Could not read MaxDirectMemorySize, so the configured ceiling is accepted without validation.", e);
      return MAX_DIRECT_MEMORY_UNKNOWN;
    }
  }
}
